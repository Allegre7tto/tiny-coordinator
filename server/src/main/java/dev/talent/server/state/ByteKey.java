package dev.talent.server.state;

import java.util.Arrays;

public final class ByteKey implements Comparable<ByteKey> {
    private final byte[] bytes;

    private ByteKey(byte[] bytes) {
        this.bytes = bytes;
    }

    public static ByteKey of(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException("key cannot be empty");
        }
        return new ByteKey(bytes.clone());
    }

    public byte[] bytes() {
        return bytes.clone();
    }

    @Override
    public int compareTo(ByteKey other) {
        return Arrays.compareUnsigned(bytes, other.bytes);
    }

    @Override
    public boolean equals(Object value) {
        return value instanceof ByteKey other && Arrays.equals(bytes, other.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return java.util.HexFormat.of().formatHex(bytes);
    }
}
