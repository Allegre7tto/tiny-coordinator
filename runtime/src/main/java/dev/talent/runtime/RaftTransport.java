package dev.talent.runtime;

import dev.talent.raft.RaftEffect;
import java.util.concurrent.CompletionStage;

@FunctionalInterface
public interface RaftTransport {
    CompletionStage<RaftEffect.Rpc> request(long target, RaftEffect.Rpc request);
}
