package dev.talent.server.state;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/** MVCC storage; callers serialize mutations through CoordinatorStateMachine. */
public final class MvccStore {
    public record Put(KvRecord current, KvRecord previous) {}
    public record Delete(KvRecord tombstone, KvRecord previous) {}
    public record Range(List<KvRecord> values, long count, boolean more) {
        public Range {
            values = List.copyOf(values);
        }
    }

    private final NavigableMap<ByteKey, List<KvRecord>> histories = new TreeMap<>();
    private long revision;
    private long compactedRevision;

    public long revision() {
        return revision;
    }

    public long compactedRevision() {
        return compactedRevision;
    }

    public long beginMutation() {
        return ++revision;
    }

    public Put put(ByteKey key, byte[] value, long leaseId, long atRevision) {
        requireCurrentMutation(atRevision);
        List<KvRecord> history = histories.computeIfAbsent(key, ignored -> new ArrayList<>());
        KvRecord previous = latestLive(history);
        long createRevision = previous == null ? atRevision : previous.createRevision();
        long version = previous == null ? 1 : previous.version() + 1;
        KvRecord current = new KvRecord(
                key, value, createRevision, atRevision, version, leaseId, false);
        history.add(current);
        return new Put(current, previous);
    }

    public Delete delete(ByteKey key, long atRevision) {
        requireCurrentMutation(atRevision);
        List<KvRecord> history = histories.get(key);
        KvRecord previous = history == null ? null : latestLive(history);
        if (previous == null) {
            return null;
        }
        KvRecord tombstone = new KvRecord(
                key,
                new byte[0],
                previous.createRevision(),
                atRevision,
                previous.version() + 1,
                0,
                true);
        history.add(tombstone);
        return new Delete(tombstone, previous);
    }

    public KvRecord current(ByteKey key) {
        List<KvRecord> history = histories.get(key);
        return history == null ? null : latestLive(history);
    }

    public Range range(
            ByteKey start, ByteKey endExclusive, long atRevision, long limit, boolean keysOnly) {
        long target = atRevision == 0 ? revision : atRevision;
        if (atRevision != 0 && target <= compactedRevision) {
            throw new CompactedException(compactedRevision);
        }
        if (target > revision) {
            throw new IllegalArgumentException("requested revision is in the future");
        }
        NavigableMap<ByteKey, List<KvRecord>> selected = endExclusive == null
                ? histories.subMap(start, true, start, true)
                : histories.subMap(start, true, endExclusive, false);
        List<KvRecord> all = new ArrayList<>();
        for (List<KvRecord> history : selected.values()) {
            KvRecord value = at(history, target);
            if (value != null && !value.tombstone()) {
                all.add(keysOnly ? value.withoutValue() : value);
            }
        }
        long count = all.size();
        boolean more = limit > 0 && count > limit;
        if (more) {
            all = new ArrayList<>(all.subList(0, Math.toIntExact(limit)));
        }
        return new Range(all, count, more);
    }

    public List<ByteKey> liveKeys(ByteKey start, ByteKey endExclusive) {
        NavigableMap<ByteKey, List<KvRecord>> selected = endExclusive == null
                ? histories.subMap(start, true, start, true)
                : histories.subMap(start, true, endExclusive, false);
        return selected.entrySet().stream()
                .filter(entry -> latestLive(entry.getValue()) != null)
                .map(java.util.Map.Entry::getKey)
                .toList();
    }

    /**
     * Removes obsolete versions while retaining the last version at or before
     * the compact point as the anchor for all later reads.
     */
    public int compact(long throughRevision) {
        if (throughRevision <= compactedRevision || throughRevision > revision) {
            throw new IllegalArgumentException("invalid compaction revision " + throughRevision);
        }
        int removed = 0;
        var iterator = histories.entrySet().iterator();
        while (iterator.hasNext()) {
            List<KvRecord> history = iterator.next().getValue();
            int anchor = -1;
            for (int i = 0; i < history.size(); i++) {
                if (history.get(i).modificationRevision() <= throughRevision) {
                    anchor = i;
                } else {
                    break;
                }
            }
            if (anchor > 0) {
                removed += anchor;
                history.subList(0, anchor).clear();
            }
            if (history.size() == 1
                    && history.getFirst().tombstone()
                    && history.getFirst().modificationRevision() <= throughRevision) {
                iterator.remove();
                removed++;
            }
        }
        compactedRevision = throughRevision;
        return removed;
    }

    public NavigableMap<ByteKey, List<KvRecord>> copyHistories() {
        NavigableMap<ByteKey, List<KvRecord>> copy = new TreeMap<>();
        histories.forEach((key, history) -> copy.put(key, List.copyOf(history)));
        return Collections.unmodifiableNavigableMap(copy);
    }

    public void restore(
            long restoredRevision,
            long restoredCompactedRevision,
            NavigableMap<ByteKey, List<KvRecord>> restoredHistories) {
        if (restoredRevision < 0
                || restoredCompactedRevision < 0
                || restoredCompactedRevision > restoredRevision) {
            throw new IllegalArgumentException("invalid restored MVCC revisions");
        }
        histories.clear();
        restoredHistories.forEach((key, history) -> {
            if (history.isEmpty()) {
                throw new IllegalArgumentException("empty MVCC history");
            }
            histories.put(key, new ArrayList<>(history));
        });
        revision = restoredRevision;
        compactedRevision = restoredCompactedRevision;
    }

    private void requireCurrentMutation(long atRevision) {
        if (atRevision != revision) {
            throw new IllegalArgumentException(
                    "mutation revision " + atRevision + " is not current " + revision);
        }
    }

    private static KvRecord latestLive(List<KvRecord> history) {
        if (history.isEmpty()) {
            return null;
        }
        KvRecord latest = history.getLast();
        return latest.tombstone() ? null : latest;
    }

    private static KvRecord at(List<KvRecord> history, long revision) {
        for (int i = history.size() - 1; i >= 0; i--) {
            KvRecord candidate = history.get(i);
            if (candidate.modificationRevision() <= revision) {
                return candidate;
            }
        }
        return null;
    }
}
