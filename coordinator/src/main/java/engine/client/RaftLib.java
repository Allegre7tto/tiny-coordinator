package engine.client;

import java.nio.ByteBuffer;

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

    public long propose(ByteBuffer buf) { return propose(ptr, buf); }

    public int recv(ByteBuffer buf) { return recv(ptr, buf); }

    public void snap(long index, ByteBuffer buf) { snap(ptr, index, buf); }

    public int load(ByteBuffer buf) { return load(ptr, buf); }

    @Override
    public void close() {
        if (ptr != 0) {
            stop(ptr);
        }
    }

    private static native long init(long id, String dir, String[] peers, int port);

    private static native long propose(long ptr, ByteBuffer buf);

    private static native int recv(long ptr, ByteBuffer buf);

    private static native void snap(long ptr, long index, ByteBuffer buf);

    private static native int load(long ptr, ByteBuffer buf);

    private static native void stop(long ptr);
}
