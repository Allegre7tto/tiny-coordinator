package engine.mvcc;

import engine.coordinator.v1.CoordinatorOuterClass.*;
import engine.coordinator.LeaseManager;

import com.google.protobuf.ByteString;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.List;

@ApplicationScoped
public class TxnManager {

    private static final Logger LOG = Logger.getLogger(TxnManager.class);

    @Inject MvccStore mvccStore;
    @Inject LeaseManager leaseManager;



    public TxnResponse applyTxn(TxnRequest req) {
        boolean conditionMet = evaluateCompares(req.getCompareList());
        var ops = conditionMet ? req.getSuccessList() : req.getFailureList();

        TxnResponse.Builder builder = TxnResponse.newBuilder().setSucceeded(conditionMet);

        for (var op : ops) {
            OperationResponse.Builder opr = OperationResponse.newBuilder();
            if (op.hasPut()) {
                PutRequest pr = op.getPut();
                long rev = mvccStore.currentRevision();
                mvccStore.putAtRevision(pr.getKey(), pr.getValue(), rev, pr.getLease());
                if (pr.getLease() != 0) leaseManager.attach(pr.getLease(), pr.getKey());
                opr.setRevision(rev);
                opr.setPut(PutResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder().setRevision(rev)));
            } else if (op.hasDelete()) {
                DeleteRequest dr = op.getDelete();
                int deleted;
                if (dr.getRangeend().isEmpty()) {
                    deleted = mvccStore.deleteAtRevision(dr.getKey(), mvccStore.currentRevision()) > 0 ? 1 : 0;
                } else {
                    deleted = mvccStore.deleteRangeAtRevision(dr.getKey(), dr.getRangeend(), mvccStore.currentRevision());
                }
                opr.setRevision(mvccStore.currentRevision());
                opr.setDelete(DeleteResponse.newBuilder()
                    .setDeleted(deleted)
                    .setHeader(ResponseHeader.newBuilder().setRevision(mvccStore.currentRevision())));
            } else if (op.hasRange()) {
                GetRequest gr = op.getRange();
                var rangeResult = mvccStore.range(gr.getKey(), gr.getRangeend(), 0, gr.getLimit());
                GetResponse.Builder gb = GetResponse.newBuilder();
                for (var e : rangeResult.entries()) {
                    gb.addKvs(KeyValue.newBuilder()
                        .setKey(e.key())
                        .setValue(e.kv().value())
                        .setCreaterev(e.kv().createRevision())
                        .setModrev(e.kv().modRevision())
                        .setVersion(e.kv().version())
                        .setLease(e.kv().lease()));
                }
                gb.setCount(rangeResult.entries().size());
                gb.setMore(rangeResult.more());
                gb.setHeader(ResponseHeader.newBuilder().setRevision(mvccStore.currentRevision()));
                opr.setRange(gb.build());
                opr.setRevision(mvccStore.currentRevision());
            }
            builder.addResponses(opr.build());
        }

        return builder.build();
    }

    private boolean evaluateCompares(List<engine.coordinator.v1.CoordinatorOuterClass.Compare> compares) {
        for (var compare : compares) {
            if (!evaluateCompare(compare)) return false;
        }
        return true;
    }

    private boolean evaluateCompare(engine.coordinator.v1.CoordinatorOuterClass.Compare compare) {
        var kvOpt = mvccStore.get(compare.getKey());

        return switch (compare.getTarget()) {
            case KEY -> {
                boolean exists = kvOpt.isPresent();
                yield compare.getResult() == engine.coordinator.v1.CoordinatorOuterClass.Compare.CompareResult.EQUAL ? exists : !exists;
            }
            case VALUE -> {
                if (kvOpt.isEmpty()) yield compare.getResult() != engine.coordinator.v1.CoordinatorOuterClass.Compare.CompareResult.EQUAL;
                int cmp = MvccStore.CMP.compare(kvOpt.get().value(), compare.getValue());
                yield matchCmp(compare.getResult(), cmp);
            }
            case MOD_REVISION -> {
                if (kvOpt.isEmpty()) yield compare.getResult() != engine.coordinator.v1.CoordinatorOuterClass.Compare.CompareResult.EQUAL;
                long v = kvOpt.get().modRevision();
                long e = decodeInt64(compare.getValue());
                yield matchNum(compare.getResult(), v, e);
            }
            case CREATE_REVISION -> {
                if (kvOpt.isEmpty()) yield compare.getResult() != engine.coordinator.v1.CoordinatorOuterClass.Compare.CompareResult.EQUAL;
                long v = kvOpt.get().createRevision();
                long e = decodeInt64(compare.getValue());
                yield matchNum(compare.getResult(), v, e);
            }
            case VERSION -> {
                if (kvOpt.isEmpty()) yield compare.getResult() != engine.coordinator.v1.CoordinatorOuterClass.Compare.CompareResult.EQUAL;
                long v = kvOpt.get().version();
                long e = decodeInt64(compare.getValue());
                yield matchNum(compare.getResult(), v, e);
            }
            case UNRECOGNIZED -> false;
        };
    }

    private boolean matchCmp(engine.coordinator.v1.CoordinatorOuterClass.Compare.CompareResult r, int cmp) {
        return switch (r) {
            case EQUAL -> cmp == 0;
            case GREATER -> cmp > 0;
            case LESS -> cmp < 0;
            case UNRECOGNIZED -> false;
        };
    }

    private boolean matchNum(engine.coordinator.v1.CoordinatorOuterClass.Compare.CompareResult r, long actual, long expected) {
        return switch (r) {
            case EQUAL -> actual == expected;
            case GREATER -> actual > expected;
            case LESS -> actual < expected;
            case UNRECOGNIZED -> false;
        };
    }

    private long decodeInt64(ByteString bs) {
        try {
            var cis = com.google.protobuf.CodedInputStream.newInstance(bs.toByteArray());
            return cis.readInt64();
        } catch (IOException e) {
            return 0;
        }
    }
}
