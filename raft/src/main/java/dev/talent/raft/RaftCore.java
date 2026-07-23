package dev.talent.raft;

import dev.talent.raft.RaftEffect.AppendRequest;
import dev.talent.raft.RaftEffect.AppendResponse;
import dev.talent.raft.RaftEffect.Error;
import dev.talent.raft.RaftEffect.PersistentState;
import dev.talent.raft.RaftEffect.Send;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Deterministic Raft state machine.
 *
 * <p>This class deliberately has no locks. Its owner must deliver every event
 * from one serialized event loop.</p>
 */
public final class RaftCore {
    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    public record Status(
            long nodeId,
            Role role,
            long term,
            Long leaderId,
            long commitIndex,
            long appliedIndex,
            long lastLogIndex,
            ClusterConfiguration configuration) {}

    private final long id;
    private final long electionTimeoutMillis;
    private final long heartbeatIntervalMillis;

    private Role role = Role.FOLLOWER;
    private long currentTerm;
    private Long votedFor;
    private Long leaderId;
    private long logicalNow;
    private long electionDeadline;
    private long heartbeatDeadline;

    private ClusterConfiguration configuration;
    private final List<RaftLogEntry> log = new ArrayList<>();
    private long snapshotIndex;
    private long snapshotTerm;
    private RaftSnapshot latestSnapshot;
    private long commitIndex;
    private long appliedIndex;

    private final Set<Long> votes = new HashSet<>();
    private final Map<Long, Long> nextIndex = new HashMap<>();
    private final Map<Long, Long> matchIndex = new HashMap<>();
    private final Map<Long, Long> proposalByIndex = new LinkedHashMap<>();
    private final Map<Long, ReadRound> readRounds = new LinkedHashMap<>();
    private long nextReadContext = 1;

    private Long membershipCorrelation;
    private Set<Long> targetVoters;
    private Set<Long> targetLearners;
    private boolean jointEntryAppended;

    public RaftCore(
            long id,
            ClusterConfiguration initialConfiguration,
            long electionTimeoutMillis,
            long heartbeatIntervalMillis) {
        if (id <= 0) {
            throw new IllegalArgumentException("node id must be positive");
        }
        if (electionTimeoutMillis <= heartbeatIntervalMillis || heartbeatIntervalMillis <= 0) {
            throw new IllegalArgumentException("election timeout must exceed heartbeat interval");
        }
        this.id = id;
        this.configuration = Objects.requireNonNull(initialConfiguration, "initialConfiguration");
        this.electionTimeoutMillis = electionTimeoutMillis;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
        log.add(RaftLogEntry.noop(0, 0));
        resetElectionDeadline();
    }

    public RaftCore(
            long id,
            long electionTimeoutMillis,
            long heartbeatIntervalMillis,
            PersistentState restored,
            RaftSnapshot snapshot) {
        this(id, restored.configuration(), electionTimeoutMillis, heartbeatIntervalMillis);
        currentTerm = restored.term();
        votedFor = restored.votedFor();
        commitIndex = restored.commitIndex();
        snapshotIndex = restored.snapshotIndex();
        snapshotTerm = restored.snapshotTerm();
        latestSnapshot = snapshot;
        log.clear();
        log.addAll(restored.log());
        if (log.isEmpty()
                || log.getFirst().index() != snapshotIndex
                || log.getFirst().term() != snapshotTerm) {
            throw new IllegalArgumentException("persisted log does not match snapshot boundary");
        }
        appliedIndex = snapshotIndex;
        resetElectionDeadline();
    }

    /**
     * Emits entries committed after the restored snapshot. Call once on the
     * owning event loop after the state-machine snapshot has been restored.
     */
    public List<RaftEffect> bootstrap() {
        List<RaftEffect> effects = new ArrayList<>();
        applyCommitted(effects);
        return List.copyOf(effects);
    }

    public List<RaftEffect> handle(RaftEvent event) {
        Objects.requireNonNull(event, "event");
        List<RaftEffect> effects = new ArrayList<>();
        switch (event) {
            case RaftEvent.Tick tick -> onTick(tick, effects);
            case RaftEvent.Propose proposal -> onPropose(proposal, effects);
            case RaftEvent.Read read -> onRead(read, effects);
            case RaftEvent.ChangeMembership change -> onMembershipChange(change, effects);
            case RaftEvent.RequestVote request -> onRequestVote(request, effects);
            case RaftEvent.RequestVoteResult result -> onRequestVoteResult(result, effects);
            case RaftEvent.AppendEntries append -> onAppendEntries(append, effects);
            case RaftEvent.AppendEntriesResult result -> onAppendEntriesResult(result, effects);
            case RaftEvent.InstallSnapshot install -> onInstallSnapshot(install, effects);
            case RaftEvent.SnapshotInstalled installed -> onSnapshotInstalled(installed, effects);
            case RaftEvent.InstallSnapshotResult result -> onInstallSnapshotResult(result, effects);
            case RaftEvent.SnapshotCreated created -> onSnapshotCreated(created, effects);
        }
        return List.copyOf(effects);
    }

    public Status status() {
        return new Status(
                id, role, currentTerm, leaderId, commitIndex, appliedIndex, lastIndex(), configuration);
    }

    private void onTick(RaftEvent.Tick tick, List<RaftEffect> effects) {
        if (tick.nowMillis() < logicalNow) {
            throw new IllegalArgumentException("logical time cannot move backwards");
        }
        logicalNow = tick.nowMillis();
        if (role == Role.LEADER) {
            if (logicalNow >= heartbeatDeadline) {
                heartbeatDeadline = logicalNow + heartbeatIntervalMillis;
                replicateAll(0, effects);
            }
        } else if (logicalNow >= electionDeadline && configuration.voters().contains(id)) {
            beginElection(effects);
        }
    }

    private void onPropose(RaftEvent.Propose proposal, List<RaftEffect> effects) {
        if (role != Role.LEADER) {
            effects.add(new RaftEffect.Rejected(proposal.correlationId(), Error.NOT_LEADER, leaderId));
            return;
        }
        RaftLogEntry entry =
                RaftLogEntry.command(lastIndex() + 1, currentTerm, proposal.command());
        log.add(entry);
        proposalByIndex.put(entry.index(), proposal.correlationId());
        acknowledgeSelf();
        persist(effects);
        advanceCommit(effects);
        replicateAll(0, effects);
    }

    private void onRead(RaftEvent.Read read, List<RaftEffect> effects) {
        if (role != Role.LEADER) {
            effects.add(new RaftEffect.Rejected(read.correlationId(), Error.NOT_LEADER, leaderId));
            return;
        }
        long context = nextReadContext++;
        ReadRound round = new ReadRound(read.correlationId(), commitIndex);
        round.acknowledgements.add(id);
        readRounds.put(context, round);
        if (configuration.hasElectionQuorum(round.acknowledgements)) {
            completeReads(effects);
        } else {
            replicateAll(context, effects);
        }
    }

    private void onMembershipChange(
            RaftEvent.ChangeMembership change, List<RaftEffect> effects) {
        if (role != Role.LEADER) {
            effects.add(new RaftEffect.Rejected(change.correlationId(), Error.NOT_LEADER, leaderId));
            return;
        }
        if (membershipCorrelation != null || configuration.joint()) {
            effects.add(new RaftEffect.Rejected(
                    change.correlationId(), Error.MEMBERSHIP_CHANGE_IN_PROGRESS, leaderId));
            return;
        }
        if (change.voters().isEmpty()) {
            effects.add(new RaftEffect.Rejected(
                    change.correlationId(), Error.INVALID_CONFIGURATION, leaderId));
            return;
        }
        try {
            ClusterConfiguration.stable(change.voters(), change.learners());
        } catch (IllegalArgumentException invalid) {
            effects.add(new RaftEffect.Rejected(
                    change.correlationId(), Error.INVALID_CONFIGURATION, leaderId));
            return;
        }

        membershipCorrelation = change.correlationId();
        targetVoters = Set.copyOf(change.voters());
        targetLearners = Set.copyOf(change.learners());
        jointEntryAppended = false;

        Set<Long> additions = new HashSet<>(targetVoters);
        additions.removeAll(configuration.voters());
        Set<Long> missingLearners = new HashSet<>(additions);
        missingLearners.removeAll(configuration.learners());
        if (!missingLearners.isEmpty()) {
            Set<Long> stagedLearners = new HashSet<>(configuration.learners());
            stagedLearners.addAll(missingLearners);
            appendConfiguration(
                    RaftLogEntry.Kind.STABLE_CONFIG,
                    configuration.voters(),
                    configuration.voters(),
                    stagedLearners,
                    effects);
        } else {
            maybeAppendJoint(effects);
        }
        advanceCommit(effects);
        replicateAll(0, effects);
    }

    private void onRequestVote(RaftEvent.RequestVote request, List<RaftEffect> effects) {
        if (request.term() > currentTerm) {
            stepDown(request.term(), null, effects);
        }
        boolean upToDate = request.lastLogTerm() > lastTerm()
                || request.lastLogTerm() == lastTerm() && request.lastLogIndex() >= lastIndex();
        boolean eligible = configuration.voters().contains(request.candidateId());
        boolean grant = request.term() == currentTerm
                && eligible
                && upToDate
                && (votedFor == null || votedFor == request.candidateId());
        if (grant) {
            votedFor = request.candidateId();
            resetElectionDeadline();
            persist(effects);
        }
        effects.add(new Send(
                request.from(), new RaftEffect.VoteResponse(currentTerm, grant)));
    }

    private void onRequestVoteResult(
            RaftEvent.RequestVoteResult result, List<RaftEffect> effects) {
        if (result.term() > currentTerm) {
            stepDown(result.term(), null, effects);
            return;
        }
        if (role != Role.CANDIDATE || result.term() != currentTerm || !result.granted()) {
            return;
        }
        votes.add(result.from());
        if (configuration.hasElectionQuorum(votes)) {
            becomeLeader(effects);
        }
    }

    private void onAppendEntries(RaftEvent.AppendEntries request, List<RaftEffect> effects) {
        if (request.term() > currentTerm) {
            stepDown(request.term(), request.leaderId(), effects);
        } else if (request.term() == currentTerm && role != Role.FOLLOWER) {
            stepDown(currentTerm, request.leaderId(), effects);
        }
        if (request.term() < currentTerm) {
            rejectAppend(request, 0, lastIndex() + 1, effects);
            return;
        }

        leaderId = request.leaderId();
        resetElectionDeadline();
        if (request.previousLogIndex() < snapshotIndex) {
            rejectAppend(request, 0, snapshotIndex + 1, effects);
            return;
        }
        if (request.previousLogIndex() > lastIndex()) {
            rejectAppend(request, 0, lastIndex() + 1, effects);
            return;
        }
        long localPreviousTerm = termAt(request.previousLogIndex());
        if (localPreviousTerm != request.previousLogTerm()) {
            long conflictIndex = firstIndexOfTerm(localPreviousTerm, request.previousLogIndex());
            rejectAppend(request, localPreviousTerm, conflictIndex, effects);
            return;
        }

        boolean changed = mergeEntries(request.entries());
        if (changed) {
            persist(effects);
        }
        if (request.leaderCommit() > commitIndex) {
            commitIndex = Math.min(request.leaderCommit(), lastIndex());
            persist(effects);
            applyCommitted(effects);
        }
        effects.add(new Send(request.from(), new AppendResponse(
                currentTerm, true, lastIndex(), 0, 0, request.readContext())));
    }

    private void onAppendEntriesResult(
            RaftEvent.AppendEntriesResult result, List<RaftEffect> effects) {
        if (result.term() > currentTerm) {
            stepDown(result.term(), null, effects);
            return;
        }
        if (role != Role.LEADER || result.term() != currentTerm) {
            return;
        }
        if (result.success()) {
            matchIndex.merge(result.from(), result.matchIndex(), Math::max);
            nextIndex.put(result.from(), matchIndex.get(result.from()) + 1);
            if (result.readContext() != 0) {
                ReadRound round = readRounds.get(result.readContext());
                if (round != null) {
                    round.acknowledgements.add(result.from());
                }
            }
            advanceCommit(effects);
            maybeAppendJoint(effects);
            completeReads(effects);
            if (nextIndex.get(result.from()) <= lastIndex()) {
                replicateTo(result.from(), 0, effects);
            }
        } else {
            long next = result.conflictIndex() > 0
                    ? result.conflictIndex()
                    : Math.max(snapshotIndex + 1, nextIndex.getOrDefault(result.from(), 1L) - 1);
            if (result.conflictTerm() > 0) {
                long lastConflict = lastIndexOfTerm(result.conflictTerm());
                if (lastConflict >= snapshotIndex) {
                    next = lastConflict + 1;
                }
            }
            nextIndex.put(result.from(), Math.max(1, next));
            replicateTo(result.from(), result.readContext(), effects);
        }
    }

    private void onInstallSnapshot(
            RaftEvent.InstallSnapshot request, List<RaftEffect> effects) {
        if (request.term() > currentTerm) {
            stepDown(request.term(), request.leaderId(), effects);
        } else if (request.term() == currentTerm && role != Role.FOLLOWER) {
            stepDown(currentTerm, request.leaderId(), effects);
        }
        if (request.term() < currentTerm) {
            effects.add(new Send(request.from(),
                    new RaftEffect.SnapshotResponse(currentTerm, false, snapshotIndex)));
            return;
        }
        leaderId = request.leaderId();
        resetElectionDeadline();
        if (request.snapshot().lastIncludedIndex() <= snapshotIndex) {
            effects.add(new Send(request.from(),
                    new RaftEffect.SnapshotResponse(currentTerm, true, snapshotIndex)));
            return;
        }
        effects.add(new RaftEffect.RestoreSnapshot(
                request.from(), request.term(), request.snapshot()));
    }

    private void onSnapshotInstalled(
            RaftEvent.SnapshotInstalled installed, List<RaftEffect> effects) {
        if (!installed.success()) {
            effects.add(new Send(installed.from(),
                    new RaftEffect.SnapshotResponse(currentTerm, false, snapshotIndex)));
            return;
        }
        RaftSnapshot snapshot = installed.snapshot();
        installSnapshotState(snapshot);
        persist(effects);
        effects.add(new Send(installed.from(), new RaftEffect.SnapshotResponse(
                currentTerm, true, snapshot.lastIncludedIndex())));
    }

    private void onInstallSnapshotResult(
            RaftEvent.InstallSnapshotResult result, List<RaftEffect> effects) {
        if (result.term() > currentTerm) {
            stepDown(result.term(), null, effects);
            return;
        }
        if (role != Role.LEADER || result.term() != currentTerm || !result.installed()) {
            return;
        }
        matchIndex.merge(result.from(), result.lastIncludedIndex(), Math::max);
        nextIndex.put(result.from(), matchIndex.get(result.from()) + 1);
        replicateTo(result.from(), 0, effects);
    }

    private void onSnapshotCreated(
            RaftEvent.SnapshotCreated created, List<RaftEffect> effects) {
        RaftSnapshot snapshot = created.snapshot();
        if (snapshot.lastIncludedIndex() <= snapshotIndex
                || snapshot.lastIncludedIndex() > appliedIndex) {
            return;
        }
        compactLog(snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm());
        snapshotIndex = snapshot.lastIncludedIndex();
        snapshotTerm = snapshot.lastIncludedTerm();
        latestSnapshot = snapshot;
        effects.add(new RaftEffect.SaveSnapshot(snapshot));
        persist(effects);
    }

    private void beginElection(List<RaftEffect> effects) {
        role = Role.CANDIDATE;
        leaderId = null;
        currentTerm++;
        votedFor = id;
        votes.clear();
        votes.add(id);
        resetElectionDeadline();
        persist(effects);
        effects.add(new RaftEffect.RoleChanged(role, currentTerm, null));

        if (configuration.hasElectionQuorum(votes)) {
            becomeLeader(effects);
            return;
        }
        for (long peer : configuration.voters()) {
            if (peer != id) {
                effects.add(new Send(peer, new RaftEffect.VoteRequest(
                        currentTerm, id, lastIndex(), lastTerm())));
            }
        }
    }

    private void becomeLeader(List<RaftEffect> effects) {
        role = Role.LEADER;
        leaderId = id;
        votes.clear();
        nextIndex.clear();
        matchIndex.clear();
        for (long peer : configuration.replicationTargets()) {
            nextIndex.put(peer, lastIndex() + 1);
            matchIndex.put(peer, peer == id ? lastIndex() : snapshotIndex);
        }
        RaftLogEntry noop = RaftLogEntry.noop(lastIndex() + 1, currentTerm);
        log.add(noop);
        acknowledgeSelf();
        persist(effects);
        effects.add(new RaftEffect.RoleChanged(role, currentTerm, id));
        heartbeatDeadline = logicalNow;
        advanceCommit(effects);
        replicateAll(0, effects);
    }

    private void stepDown(long term, Long newLeader, List<RaftEffect> effects) {
        boolean hardStateChanged = term > currentTerm;
        if (hardStateChanged) {
            currentTerm = term;
            votedFor = null;
        }
        boolean roleChanged = role != Role.FOLLOWER || !Objects.equals(leaderId, newLeader);
        role = Role.FOLLOWER;
        leaderId = newLeader;
        votes.clear();
        resetElectionDeadline();
        rejectPending(effects);
        if (hardStateChanged) {
            persist(effects);
        }
        if (roleChanged || hardStateChanged) {
            effects.add(new RaftEffect.RoleChanged(role, currentTerm, leaderId));
        }
    }

    private void rejectPending(List<RaftEffect> effects) {
        for (long correlation : proposalByIndex.values()) {
            effects.add(new RaftEffect.Rejected(correlation, Error.NOT_LEADER, leaderId));
        }
        proposalByIndex.clear();
        for (ReadRound read : readRounds.values()) {
            effects.add(new RaftEffect.Rejected(read.correlationId, Error.NOT_LEADER, leaderId));
        }
        readRounds.clear();
        if (membershipCorrelation != null) {
            effects.add(new RaftEffect.Rejected(
                    membershipCorrelation, Error.NOT_LEADER, leaderId));
            clearMembershipChange();
        }
    }

    private void advanceCommit(List<RaftEffect> effects) {
        if (role != Role.LEADER) {
            return;
        }
        boolean progressed;
        do {
            progressed = false;
            for (long candidate = lastIndex(); candidate > commitIndex; candidate--) {
                if (termAt(candidate) == currentTerm
                        && configuration.hasCommitQuorum(candidate, matchIndex)) {
                    commitIndex = candidate;
                    persist(effects);
                    applyCommitted(effects);
                    progressed = true;
                    break;
                }
            }
        } while (progressed && commitIndex < lastIndex());
        completeReads(effects);
    }

    private void applyCommitted(List<RaftEffect> effects) {
        while (appliedIndex < commitIndex) {
            appliedIndex++;
            RaftLogEntry entry = entryAt(appliedIndex);
            switch (entry.kind()) {
                case COMMAND -> effects.add(
                        new RaftEffect.Apply(entry.index(), entry.term(), entry.command()));
                case JOINT_CONFIG -> {
                    configuration = ClusterConfiguration.joint(
                            entry.oldVoters(), entry.newVoters(), entry.learners());
                    initializeReplicationMaps();
                    persist(effects);
                    if (role == Role.LEADER) {
                        appendConfiguration(
                                RaftLogEntry.Kind.STABLE_CONFIG,
                                entry.newVoters(),
                                entry.newVoters(),
                                targetLearners == null ? entry.learners() : targetLearners,
                                effects);
                    }
                    effects.add(new RaftEffect.AdvanceApplied(entry.index(), entry.term()));
                }
                case STABLE_CONFIG -> {
                    configuration = ClusterConfiguration.stable(
                            entry.newVoters(), entry.learners());
                    initializeReplicationMaps();
                    persist(effects);
                    if (membershipCorrelation != null) {
                        if (targetVoters.equals(configuration.voters())) {
                            effects.add(new RaftEffect.ProposalCommitted(
                                    membershipCorrelation, entry.index()));
                            clearMembershipChange();
                            if (!configuration.voters().contains(id) && role == Role.LEADER) {
                                stepDown(currentTerm, null, effects);
                            }
                        } else {
                            maybeAppendJoint(effects);
                        }
                    }
                    effects.add(new RaftEffect.AdvanceApplied(entry.index(), entry.term()));
                }
                case NOOP ->
                        effects.add(new RaftEffect.AdvanceApplied(entry.index(), entry.term()));
            }
            Long correlation = proposalByIndex.remove(entry.index());
            if (correlation != null) {
                effects.add(new RaftEffect.ProposalCommitted(correlation, entry.index()));
            }
        }
    }

    private void maybeAppendJoint(List<RaftEffect> effects) {
        if (role != Role.LEADER || membershipCorrelation == null || jointEntryAppended
                || targetVoters == null || configuration.joint()) {
            return;
        }
        Set<Long> additions = new HashSet<>(targetVoters);
        additions.removeAll(configuration.voters());
        boolean caughtUp = additions.stream()
                .allMatch(node -> matchIndex.getOrDefault(node, 0L) >= commitIndex);
        if (!caughtUp) {
            return;
        }
        Set<Long> finalLearners = new HashSet<>(targetLearners);
        finalLearners.removeAll(targetVoters);
        jointEntryAppended = true;
        appendConfiguration(
                RaftLogEntry.Kind.JOINT_CONFIG,
                configuration.voters(),
                targetVoters,
                finalLearners,
                effects);
        advanceCommit(effects);
        replicateAll(0, effects);
    }

    private void appendConfiguration(
            RaftLogEntry.Kind kind,
            Set<Long> oldVoters,
            Set<Long> newVoters,
            Set<Long> learners,
            List<RaftEffect> effects) {
        RaftLogEntry entry = new RaftLogEntry(
                lastIndex() + 1,
                currentTerm,
                kind,
                null,
                oldVoters,
                newVoters,
                learners);
        log.add(entry);
        acknowledgeSelf();
        persist(effects);
    }

    private void replicateAll(long readContext, List<RaftEffect> effects) {
        if (role != Role.LEADER) {
            return;
        }
        for (long peer : configuration.replicationTargets()) {
            if (peer != id) {
                replicateTo(peer, readContext, effects);
            }
        }
    }

    private void replicateTo(long peer, long readContext, List<RaftEffect> effects) {
        long next = nextIndex.getOrDefault(peer, lastIndex() + 1);
        if (next <= snapshotIndex && latestSnapshot != null) {
            effects.add(new Send(peer,
                    new RaftEffect.SnapshotRequest(currentTerm, id, latestSnapshot)));
            return;
        }
        next = Math.max(snapshotIndex + 1, Math.min(next, lastIndex() + 1));
        long previous = next - 1;
        List<RaftLogEntry> entries = new ArrayList<>();
        for (long index = next; index <= lastIndex(); index++) {
            entries.add(entryAt(index));
        }
        effects.add(new Send(peer, new AppendRequest(
                currentTerm,
                id,
                previous,
                termAt(previous),
                entries,
                commitIndex,
                readContext)));
    }

    private void completeReads(List<RaftEffect> effects) {
        var iterator = readRounds.entrySet().iterator();
        while (iterator.hasNext()) {
            var item = iterator.next();
            ReadRound round = item.getValue();
            if (!round.quorumConfirmed
                    && configuration.hasElectionQuorum(round.acknowledgements)) {
                round.quorumConfirmed = true;
                round.readIndex = Math.max(round.readIndex, commitIndex);
            }
            if (round.quorumConfirmed && appliedIndex >= round.readIndex) {
                effects.add(new RaftEffect.ReadReady(round.correlationId, round.readIndex));
                iterator.remove();
            }
        }
    }

    private boolean mergeEntries(List<RaftLogEntry> incoming) {
        boolean changed = false;
        for (RaftLogEntry entry : incoming) {
            if (entry.index() <= snapshotIndex) {
                continue;
            }
            if (entry.index() <= lastIndex()) {
                RaftLogEntry local = entryAt(entry.index());
                if (!local.sameContent(entry)) {
                    truncateFrom(entry.index());
                    log.add(entry);
                    changed = true;
                }
            } else {
                if (entry.index() != lastIndex() + 1) {
                    throw new IllegalStateException("non-contiguous append");
                }
                log.add(entry);
                changed = true;
            }
        }
        return changed;
    }

    private void truncateFrom(long index) {
        int offset = Math.toIntExact(index - snapshotIndex);
        log.subList(offset, log.size()).clear();
        proposalByIndex.keySet().removeIf(candidate -> candidate >= index);
    }

    private void installSnapshotState(RaftSnapshot snapshot) {
        if (snapshot.lastIncludedIndex() < lastIndex()
                && snapshot.lastIncludedIndex() >= snapshotIndex
                && termAt(snapshot.lastIncludedIndex()) == snapshot.lastIncludedTerm()) {
            compactLog(snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm());
        } else {
            log.clear();
            log.add(RaftLogEntry.noop(
                    snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm()));
        }
        snapshotIndex = snapshot.lastIncludedIndex();
        snapshotTerm = snapshot.lastIncludedTerm();
        latestSnapshot = snapshot;
        configuration = snapshot.configuration();
        commitIndex = Math.max(commitIndex, snapshotIndex);
        appliedIndex = Math.max(appliedIndex, snapshotIndex);
        initializeReplicationMaps();
    }

    private void compactLog(long throughIndex, long throughTerm) {
        List<RaftLogEntry> suffix = new ArrayList<>();
        for (long index = throughIndex + 1; index <= lastIndex(); index++) {
            suffix.add(entryAt(index));
        }
        log.clear();
        log.add(RaftLogEntry.noop(throughIndex, throughTerm));
        log.addAll(suffix);
    }

    private void initializeReplicationMaps() {
        Set<Long> targets = configuration.replicationTargets();
        nextIndex.keySet().retainAll(targets);
        matchIndex.keySet().retainAll(targets);
        for (long peer : targets) {
            nextIndex.putIfAbsent(peer, lastIndex() + 1);
            matchIndex.putIfAbsent(peer, peer == id ? lastIndex() : snapshotIndex);
        }
        acknowledgeSelf();
    }

    private void acknowledgeSelf() {
        nextIndex.put(id, lastIndex() + 1);
        matchIndex.put(id, lastIndex());
    }

    private void rejectAppend(
            RaftEvent.AppendEntries request,
            long conflictTerm,
            long conflictIndex,
            List<RaftEffect> effects) {
        effects.add(new Send(request.from(), new AppendResponse(
                currentTerm,
                false,
                0,
                conflictTerm,
                conflictIndex,
                request.readContext())));
    }

    private long firstIndexOfTerm(long term, long from) {
        long index = from;
        while (index > snapshotIndex && termAt(index - 1) == term) {
            index--;
        }
        return index;
    }

    private long lastIndexOfTerm(long term) {
        for (long index = lastIndex(); index >= snapshotIndex; index--) {
            if (termAt(index) == term) {
                return index;
            }
        }
        return -1;
    }

    private RaftLogEntry entryAt(long index) {
        if (index < snapshotIndex || index > lastIndex()) {
            throw new IllegalArgumentException("log index outside retained range: " + index);
        }
        return log.get(Math.toIntExact(index - snapshotIndex));
    }

    private long termAt(long index) {
        return entryAt(index).term();
    }

    private long lastIndex() {
        return snapshotIndex + log.size() - 1L;
    }

    private long lastTerm() {
        return log.getLast().term();
    }

    private void resetElectionDeadline() {
        long spread = Math.max(1, electionTimeoutMillis / 2);
        long jitter = Math.floorMod(id * 97 + currentTerm * 31, spread);
        electionDeadline = logicalNow + electionTimeoutMillis + jitter;
    }

    private void persist(List<RaftEffect> effects) {
        effects.add(new RaftEffect.Persist(new PersistentState(
                currentTerm,
                votedFor,
                commitIndex,
                snapshotIndex,
                snapshotTerm,
                configuration,
                log)));
    }

    private void clearMembershipChange() {
        membershipCorrelation = null;
        targetVoters = null;
        targetLearners = null;
        jointEntryAppended = false;
    }

    private static final class ReadRound {
        private final long correlationId;
        private long readIndex;
        private final Set<Long> acknowledgements = new HashSet<>();
        private boolean quorumConfirmed;

        private ReadRound(long correlationId, long readIndex) {
            this.correlationId = correlationId;
            this.readIndex = readIndex;
        }
    }
}
