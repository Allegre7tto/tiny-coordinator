package engine.client;

import engine.jni.v1.Jni.*;

import java.nio.ByteBuffer;

import com.google.protobuf.InvalidProtocolBufferException;

public class RaftLib implements AutoCloseable {

    static {
        System.loadLibrary("jni");
    }

    private final long ptr;

    public RaftLib(long id, String dir, String[] peers, int port) {
        long p = init(id, dir, peers, port);
        if (p == 0) {
            throw new RuntimeException("Failed to initialize Raft engine");
        }
        this.ptr = p;
    }

    public long propose(byte[] data) {
        ProposeRequest req = ProposeRequest.newBuilder()
            .setData(com.google.protobuf.ByteString.copyFrom(data))
            .build();
        ByteBuffer buf = ByteBuffer.allocateDirect(req.getSerializedSize());
        try { req.writeTo(com.google.protobuf.CodedOutputStream.newInstance(buf)); } catch (java.io.IOException e) { return -1; }
        return propose(ptr, buf);
    }

    public CommittedEntry recv() {
        ByteBuffer buf = ByteBuffer.allocateDirect(4 * 1024 * 1024);
        int n = recv(ptr, buf);
        if (n <= 0) return null;
        byte[] data = new byte[n];
        buf.position(0);
        buf.get(data);
        try { return CommittedEntry.parseFrom(data); } catch (InvalidProtocolBufferException e) { return null; }
    }

    public void snap(long index, byte[] data) {
        SnapshotSave msg = SnapshotSave.newBuilder()
            .setIndex(index)
            .setData(com.google.protobuf.ByteString.copyFrom(data))
            .build();
        ByteBuffer buf = ByteBuffer.allocateDirect(msg.getSerializedSize());
        try { msg.writeTo(com.google.protobuf.CodedOutputStream.newInstance(buf)); } catch (java.io.IOException e) { return; }
        snap(ptr, buf);
    }

    public SnapshotLoad load() {
        ByteBuffer buf = ByteBuffer.allocateDirect(256 * 1024 * 1024);
        int n = load(ptr, buf);
        if (n <= 0) return SnapshotLoad.getDefaultInstance();
        byte[] data = new byte[n];
        buf.position(0);
        buf.get(data);
        try { return SnapshotLoad.parseFrom(data); } catch (InvalidProtocolBufferException e) { return SnapshotLoad.getDefaultInstance(); }
    }

    public NodeStatus status() {
        ByteBuffer buf = ByteBuffer.allocateDirect(256);
        int n = status(ptr, buf);
        if (n < 0) {
            return NodeStatus.getDefaultInstance();
        }
        byte[] data = new byte[n];
        buf.position(0);
        buf.get(data);
        try {
            return NodeStatus.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            return NodeStatus.getDefaultInstance();
        }
    }

    public boolean isLeader()   { return status().getIsleader(); }
    public long getTerm()       { return status().getTerm(); }
    public long getCommitIndex(){ return status().getCommitidx(); }
    public long getAppliedIndex(){ return status().getApplyidx(); }
    public long getLeaderId()   { return status().getLeaderid(); }

    @Override
    public void close() {
        if (ptr != 0) {
            stop(ptr);
        }
    }

    private static native long init(long id, String dir, String[] peers, int port);
    private static native long propose(long ptr, ByteBuffer buf);
    private static native int recv(long ptr, ByteBuffer buf);
    private static native void snap(long ptr, ByteBuffer buf);
    private static native int load(long ptr, ByteBuffer buf);
    private static native int status(long ptr, ByteBuffer buf);
    private static native void stop(long ptr);
}
