package dev.talent.server.state;

import java.util.List;

public sealed interface StateMachineResult {
    long revision();

    record Put(KvRecord current, KvRecord previous, long revision)
            implements StateMachineResult {}

    record Delete(List<KvRecord> previous, long revision) implements StateMachineResult {
        public Delete {
            previous = List.copyOf(previous);
        }
    }

    record Range(MvccStore.Range range, long revision) implements StateMachineResult {}

    record Txn(boolean succeeded, List<StateMachineResult> operations, long revision)
            implements StateMachineResult {
        public Txn {
            operations = List.copyOf(operations);
        }
    }

    record Compact(long compactedRevision, int removed, long revision)
            implements StateMachineResult {}

    record Lease(long id, long ttlSeconds, long revision) implements StateMachineResult {}

    record Failure(String message, long revision) implements StateMachineResult {}
}
