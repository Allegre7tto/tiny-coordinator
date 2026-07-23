package dev.talent.runtime;

public interface ReplicatedStateMachine {
    /**
     * Applies one committed opaque command. Implementations must be
     * deterministic and must not block on watch delivery.
     */
    void apply(long index, long term, byte[] command);

    /**
     * Advances the Raft apply position for a committed non-command entry such
     * as a leader no-op or membership configuration.
     */
    default void advance(long index, long term) {}

    /** Restores an opaque coordinator snapshot before later entries are replayed. */
    void restore(byte[] snapshot);
}
