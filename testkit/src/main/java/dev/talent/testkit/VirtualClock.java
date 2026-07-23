package dev.talent.testkit;

public final class VirtualClock {
    private long millis;

    public long now() {
        return millis;
    }

    public long advance(long deltaMillis) {
        if (deltaMillis < 0) {
            throw new IllegalArgumentException("time cannot move backwards");
        }
        return millis += deltaMillis;
    }
}
