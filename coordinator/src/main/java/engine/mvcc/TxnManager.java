package engine.mvcc;

import com.google.protobuf.ByteString;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * 事务管理器。支持 If/Then/Else 原子操作。
 */
@ApplicationScoped
public class TxnManager {

    private static final Logger LOG = Logger.getLogger(TxnManager.class);

    @Inject MvccStore mvccStore;

    // ── Compare ──────────────────────────────────────────────────────────────

    public enum CompareTarget { KEY, MOD_REVISION, CREATE_REVISION, VERSION, VALUE }
    public enum CompareResult { EQUAL, GREATER, LESS }

    public record Compare(CompareTarget target, CompareResult result, String key, ByteString value) {}

    // ── Op ───────────────────────────────────────────────────────────────────

    public enum OpType { PUT, DELETE, RANGE }

    public record Op(OpType type, String key, ByteString value, String rangeEnd, long lease) {}

    // ── Request / Response ───────────────────────────────────────────────────

    public record TxnRequest(List<Compare> compares, List<Op> successOps, List<Op> failureOps) {}
    public record TxnResponse(boolean succeeded, List<Long> revisions) {}

    // ── Execute ──────────────────────────────────────────────────────────────

    public TxnResponse execute(TxnRequest request) {
        boolean conditionMet = evaluateCompares(request.compares());
        List<Op> ops = conditionMet ? request.successOps() : request.failureOps();

        List<Long> revisions = new ArrayList<>();
        for (Op op : ops) {
            long rev = executeOp(op);
            revisions.add(rev);
        }

        LOG.debugf("Txn: succeeded=%b, ops=%d", conditionMet, ops.size());
        return new TxnResponse(conditionMet, revisions);
    }

    private boolean evaluateCompares(List<Compare> compares) {
        for (Compare compare : compares) {
            if (!evaluateCompare(compare)) return false;
        }
        return true;
    }

    private boolean evaluateCompare(Compare compare) {
        var kvOpt = mvccStore.get(compare.key());

        return switch (compare.target()) {
            case KEY -> {
                boolean exists = kvOpt.isPresent();
                yield compare.result() == CompareResult.EQUAL ? exists : !exists;
            }
            case VALUE -> {
                if (kvOpt.isEmpty()) yield compare.result() != CompareResult.EQUAL;
                yield compareValues(kvOpt.get().value(), compare.value(), compare.result());
            }
            case MOD_REVISION -> {
                if (kvOpt.isEmpty()) yield compare.result() != CompareResult.EQUAL;
                yield compareNumbers(kvOpt.get().modRevision(), parseLong(compare.value()), compare.result());
            }
            case CREATE_REVISION -> {
                if (kvOpt.isEmpty()) yield compare.result() != CompareResult.EQUAL;
                yield compareNumbers(kvOpt.get().createRevision(), parseLong(compare.value()), compare.result());
            }
            case VERSION -> {
                if (kvOpt.isEmpty()) yield compare.result() != CompareResult.EQUAL;
                yield compareNumbers(kvOpt.get().version(), parseLong(compare.value()), compare.result());
            }
        };
    }

    private boolean compareValues(ByteString actual, ByteString expected, CompareResult result) {
        int cmp = actual.equals(expected) ? 0 : (actual.size() > expected.size() ? 1 : -1);
        return switch (result) {
            case EQUAL -> cmp == 0;
            case GREATER -> cmp > 0;
            case LESS -> cmp < 0;
        };
    }

    private boolean compareNumbers(long actual, long expected, CompareResult result) {
        return switch (result) {
            case EQUAL -> actual == expected;
            case GREATER -> actual > expected;
            case LESS -> actual < expected;
        };
    }

    private long parseLong(ByteString bs) {
        return Long.parseLong(bs.toStringUtf8());
    }

    private long executeOp(Op op) {
        return switch (op.type()) {
            case PUT -> mvccStore.put(op.key(), op.value(), op.lease());
            case DELETE -> {
                if (op.rangeEnd().isEmpty()) yield mvccStore.delete(op.key());
                else yield mvccStore.deleteRange(op.key(), op.rangeEnd());
            }
            case RANGE -> mvccStore.currentRevision();
        };
    }
}
