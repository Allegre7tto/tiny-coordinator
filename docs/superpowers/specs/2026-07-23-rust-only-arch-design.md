# Rust-Only Architecture

## Motivation
Remove Java coordinator entirely. Single Rust workspace with all components.

## Final Crate Structure

```
core/
  Cargo.toml          # workspace: ["proto", "raft", "coordinator"]
  proto/              # prost/tonic generation: raft.proto only
  raft/               # pure Raft (renamed from engine)
  coordinator/        # all business logic + entry point
    src/
      main.rs         # startup: RaftNode + tonic + axum
      lib.rs          # re-exports
      mvcc.rs         # BTreeMap<Vec<u8>, VersionedKey> MVCC store
      txn.rs          # Txn compare+evaluate+execute
      compact.rs      # MVCC compaction
      watch.rs        # Watch subscription + history replay
      lease.rs        # Lease lifecycle + expiry
      snapshot.rs     # StateMachineSnapshot serde (custom binary)
      state_machine.rs# StoreState: StoreState::apply() + encode_envelope
      api.rs          # axum HTTP routes
```

## Removed

- `coordinator/` (Java Quarkus project)
- `core/jni/` (JNI bridge)
- `proto/jni.proto`
- `proto/coordinator.proto`
- `Dockerfile.java` → replaced with `Dockerfile`
- `Makefile` → cargo-only

## Communication

- **Raft node-to-node**: tonic gRPC (`raft.proto`, port 7000)
- **External API**: axum HTTP JSON (port 9001)
- **No protobuf for external API** — plain JSON via axum

## Data Flow

```
HTTP client → axum handler → propose(encoded envelope) → Raft → commit
  → committed callback → mpsc channel → StoreState::apply() → mvcc/txn/lease/compact
  → Watch notify
  → Snapshot every 10k commits
```

## State Machine

- `CommandEnvelope` encoding: `[optype:4][padding:8][payload:var]`
- `StoreState::apply()` decodes envelope, dispatches to handler
- `StoreState` owns `MvccStore`, `WatchManager`, `LeaseManager`
- Snapshot triggered every 10k commits
- Snapshot persistent format: custom binary LE

## Key Design Changes from Java

1. No coordinator.proto — HTTP JSON API instead of gRPC
2. No JNI — all Rust, channel-based communication
3. RaftNode exposes `Arc<AtomicBool>/Arc<AtomicU64>` for leader/term/live status
4. Watch uses tokio `mpsc::UnboundedSender` for streaming responses
5. Lease expiry runs as background tokio task, proposes OP_LEASE_EXPIRE via Raft
