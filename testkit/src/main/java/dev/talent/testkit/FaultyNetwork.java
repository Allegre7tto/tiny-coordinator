package dev.talent.testkit;

import dev.talent.raft.RaftEffect;
import dev.talent.raft.RaftEvent;
import dev.talent.runtime.RaftRuntime;
import dev.talent.runtime.RaftTransport;

import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

public final class FaultyNetwork {
    private record Link(long from, long to) {}
    private record Deferred(
            long source,
            long target,
            RaftEffect.Rpc request,
            CompletableFuture<RaftEffect.Rpc> result) {}

    private final Map<Long, RaftRuntime> nodes = new ConcurrentHashMap<>();
    private final Set<Link> blocked = ConcurrentHashMap.newKeySet();
    private final Set<Link> delayed = ConcurrentHashMap.newKeySet();
    private final Set<Link> duplicateNext = ConcurrentHashMap.newKeySet();
    private final List<Deferred> deferred = Collections.synchronizedList(new ArrayList<>());

    public RaftTransport transportFor(long source) {
        return (target, request) -> send(source, target, request);
    }

    public void register(long id, RaftRuntime runtime) {
        nodes.put(id, runtime);
    }

    public void partition(Set<Long> left, Set<Long> right) {
        for (long from : left) {
            for (long to : right) {
                blocked.add(new Link(from, to));
                blocked.add(new Link(to, from));
            }
        }
    }

    public void heal() {
        blocked.clear();
    }

    public void delay(long from, long to) {
        delayed.add(new Link(from, to));
    }

    public void stopDelaying(long from, long to) {
        delayed.remove(new Link(from, to));
    }

    public void duplicateNext(long from, long to) {
        duplicateNext.add(new Link(from, to));
    }

    public int delayedCount() {
        return deferred.size();
    }

    public void deliverDelayed(boolean reverseOrder) {
        List<Deferred> deliveries;
        synchronized (deferred) {
            deliveries = new ArrayList<>(deferred);
            deferred.clear();
        }
        if (reverseOrder) {
            Collections.reverse(deliveries);
        }
        deliveries.forEach(this::deliver);
    }

    private CompletionStage<RaftEffect.Rpc> send(
            long source, long target, RaftEffect.Rpc request) {
        if (blocked.contains(new Link(source, target))) {
            return CompletableFuture.failedFuture(new IllegalStateException("partitioned link"));
        }
        if (delayed.contains(new Link(source, target))) {
            CompletableFuture<RaftEffect.Rpc> result = new CompletableFuture<>();
            deferred.add(new Deferred(source, target, request, result));
            return result;
        }
        return deliver(source, target, request);
    }

    private CompletionStage<RaftEffect.Rpc> deliver(
            long source, long target, RaftEffect.Rpc request) {
        RaftRuntime targetNode = nodes.get(target);
        if (targetNode == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("unknown node"));
        }
        CompletionStage<RaftEffect.Rpc> response =
                targetNode.receive(source, toEvent(source, request));
        if (duplicateNext.remove(new Link(source, target))) {
            targetNode.receive(source, toEvent(source, request));
        }
        return response;
    }

    private void deliver(Deferred item) {
        deliver(item.source(), item.target(), item.request()).whenComplete((response, problem) -> {
            if (problem != null) {
                item.result().completeExceptionally(problem);
            } else {
                item.result().complete(response);
            }
        });
    }

    private static RaftEvent toEvent(long source, RaftEffect.Rpc rpc) {
        return switch (rpc) {
            case RaftEffect.VoteRequest vote -> new RaftEvent.RequestVote(
                    source,
                    vote.term(),
                    vote.candidateId(),
                    vote.lastLogIndex(),
                    vote.lastLogTerm());
            case RaftEffect.AppendRequest append -> new RaftEvent.AppendEntries(
                    source,
                    append.term(),
                    append.leaderId(),
                    append.previousLogIndex(),
                    append.previousLogTerm(),
                    append.entries(),
                    append.leaderCommit(),
                    append.readContext());
            case RaftEffect.SnapshotRequest snapshot -> new RaftEvent.InstallSnapshot(
                    source, snapshot.term(), snapshot.leaderId(), snapshot.snapshot());
            default -> throw new IllegalArgumentException("not a request: " + rpc);
        };
    }
}
