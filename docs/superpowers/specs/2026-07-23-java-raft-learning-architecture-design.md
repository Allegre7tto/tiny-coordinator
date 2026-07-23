# Java Raft Learning Architecture

**Date:** 2026-07-23

## Goals

The repository remains a learning-oriented, self-implemented Raft system. The
refactor favors explicit state transitions, deterministic tests, and clear
module boundaries over preserving the current API or package layout.

The implementation must:

- keep Raft independent from KV, transaction, watch, lease, and compaction
  semantics;
- serialize every Raft state transition through one event loop;
- apply every committed entry exactly once and in log order on every node;
- support snapshots, linearizable reads, and dynamic voter membership through
  joint consensus;
- make the coordinator state machine deterministic and snapshot-safe;
- use Quarkus-managed lifecycle and scheduling facilities at the application
  boundary;
- provide deterministic fault-injection tests for the Raft algorithm.

Compatibility with the current external and internal APIs is not required.

## Maven Modules

The repository is a Maven reactor with short, responsibility-based module
names:

```text
tiny-coordinator/
  pom.xml
  proto/
  raft/
  runtime/
  server/
  testkit/
```

The dependency direction is:

```text
server  -> runtime -> raft
   |          |
   +--------> proto <--------+
                              |
testkit ----------------------+
```

- `proto` owns the internal Raft RPC schema and the public coordinator API.
- `raft` is the deterministic Raft algorithm. It has no Quarkus, gRPC,
  filesystem, wall-clock, or business-state dependency.
- `runtime` owns the Raft actor, peer transport, WAL, snapshots, and lifecycle.
- `server` owns MVCC, transactions, watches, leases, compaction, public gRPC
  services, and health checks.
- `testkit` owns virtual time, a controllable network, restartable nodes, and
  cluster assertions.

Generated protocol classes exist in exactly one module. The duplicate Raft
schema and build-time copying of schemas are removed.

## Raft Core

`raft` is modeled as a pure event-driven state machine. Java 21 sealed
interfaces and records replace untyped `Object` payloads, wildcard futures,
and unchecked casts.

`RaftEvent` includes:

- election and heartbeat timer events carrying monotonic time;
- client proposals;
- vote, append, and snapshot requests and responses;
- ReadIndex requests and heartbeat acknowledgements;
- membership-change requests;
- persistence and state-machine completion acknowledgements.

Handling an event mutates state synchronously and returns typed `RaftEffect`
values:

- send an RPC;
- persist hard state or append/truncate log records;
- persist or install a snapshot;
- apply a committed entry;
- complete or reject a client operation;
- reset a logical timer.

No callback, transport thread, scheduler thread, or application thread may
call Raft state methods directly.

Log entries have explicit kinds:

- `COMMAND` for opaque coordinator bytes;
- `NOOP` for the leader's current-term commitment barrier;
- `JOINT_CONFIG` for the old/new voter union phase;
- `STABLE_CONFIG` for the final voter set.

The core advances commit immediately after a local append, so a one-node
cluster commits without waiting for a nonexistent peer response.

## Runtime and Concurrency

`runtime` contains one mailbox and one actor for each Raft node. Timer ticks,
incoming RPCs, outbound RPC completions, proposals, reads, snapshot results,
and shutdown signals are all mailbox messages. Only the actor thread invokes
the core.

Peer calls use asynchronous generated gRPC stubs. Completion callbacks only
enqueue typed responses. Transport failures remain failures and can never be
converted into default success messages.

Quarkus owns the application executor and scheduler. Scheduled methods only
enqueue timer or lease-expiration events and use non-overlapping execution.
They never mutate Raft or coordinator state. CDI shutdown closes peer channels,
stops accepting work, drains or fails pending requests, and joins the actor.

Client correlation and state-machine application are independent:

- every committed command is always emitted to the coordinator on every node;
- a local pending request only determines whether an apply result is delivered
  to a waiting caller;
- timeout, cancellation, leadership loss, or restart cannot suppress apply.

The proposal is registered before it can become committed, removing the
append-before-pending-registration race.

## Persistence

The runtime uses an append-oriented WAL instead of rewriting an entire Java
object graph. Records are length-delimited and checksummed. Record kinds cover
hard state, log append, log truncation, and snapshot metadata. Required records
are flushed before the corresponding success effect is exposed.

Snapshot files contain:

- last included Raft index and term;
- stable or joint membership state;
- opaque coordinator snapshot bytes;
- a format version and checksum.

Snapshot installation is a distinct protocol path. Received snapshot bytes
are persisted, handed to the coordinator for restoration, acknowledged, and
only then allowed to replace the compacted Raft prefix. A snapshot is never
decoded as a command envelope. Corruption and incompatible formats fail
startup explicitly instead of being swallowed.

## Linearizable Reads

Public reads are linearizable by default. A leader starts a ReadIndex round
with a unique context, confirms leadership with a quorum in its current term,
records the current commit index, and completes the read only after the local
state machine has applied through that index.

Followers return a typed `NotLeader` result containing a best-effort leader
hint. A separate explicitly named serializable/local read mode may bypass
ReadIndex when stale reads are acceptable.

## Membership Changes

Membership changes support voter addition and removal with joint consensus.
Only one change may be active at a time.

Adding a node follows these stages:

1. register it as a non-voting learner;
2. replicate the current log or snapshot until it is sufficiently caught up;
3. commit `JOINT_CONFIG(oldVoters, newVoters)`;
4. commit `STABLE_CONFIG(newVoters)`.

Removing a voter uses the same two committed configuration entries. During
the joint phase, elections and commitment require a majority of both the old
and new voter sets. Replication targets the union. If the current leader is
removed, it steps down after the stable configuration commits.

Learners do not vote and are not counted in quorum calculations. Membership
state is part of Raft persistence and snapshots.

## Coordinator State Machine

`server` has one `CoordinatorStateMachine.apply()` entry point. It decodes an
opaque replicated command and serially updates MVCC, transaction, lease, and
compaction state. Watch subscription bookkeeping is local, while watch events
are derived only from committed state transitions.

Commands use a protobuf `oneof` rather than a manual operation prefix plus
unchecked payload conventions. Values inside business commands are still
opaque byte strings.

Raft index and MVCC revision are distinct:

- every committed Raft entry advances the applied Raft index;
- an operation that changes KV state advances MVCC revision once;
- a multi-key transaction uses one MVCC revision;
- read-only transactions, lease metadata changes without key deletion,
  no-ops, and membership entries do not consume an MVCC revision;
- lease revoke or expiry consumes one revision when it deletes one or more
  keys.

Transaction comparisons use one documented integer encoding consistently.
Malformed commands produce deterministic errors and do not partially mutate
state.

## MVCC and Compaction

Each key version preserves create revision, modification revision, logical
version, value, tombstone state, and lease association.

Deletion captures the previous KV before writing a tombstone, allowing DELETE
watch events and `prevKv` responses without dereferencing a missing latest
value.

Compaction removes obsolete history while retaining the anchor version needed
to answer the current state at and after the compacted revision. It never
deletes a live value merely because its last modification predates the
compaction point. Requests for unavailable historical revisions return
`Compacted`.

Snapshots restore original create revisions, modification revisions, logical
versions, tombstones needed by the retained history, compacted revision, and
current MVCC revision rather than replaying values through ordinary `put`.

## Leases

Lease commands are deterministic replicated data:

- the leader chooses the lease ID and absolute expiry instant before proposing;
- grant and keep-alive log entries contain those values;
- followers never call the wall clock while applying a lease command.

Lease-to-key and key-to-lease indexes are updated atomically. Replacing,
moving, or deleting a key detaches its former lease. Revoke and expiry delete
all attached keys through the state machine. Expiration scanning runs only on
the leader and proposes an expiry command; it does not directly mutate local
MVCC state.

Lease and attachment state is included in coordinator snapshots.

## Watches

Committed KV mutations produce immutable watch-event batches. The state
machine releases its mutation lock before handing a batch to `WatchManager`.
Delivery therefore cannot block Raft apply.

Watch subscriptions support:

- PUT and DELETE events;
- previous KV values when requested;
- replay from a retained revision;
- an explicit compacted-revision error;
- bounded per-watch queues and slow-consumer cancellation;
- an explicit cancellation handle and terminal error propagation.

Watch events are not replicated separately and subscriptions are not stored in
snapshots.

## API and Errors

The public gRPC API may change. Request fields are either implemented or
removed; no accepted field is silently ignored. Range responses report
accurate `count` and `more` values, and keys-only behavior is explicit.

Internal failures use typed results such as:

- `NotLeader` with an optional leader hint;
- `Unavailable`;
- `Timeout`;
- `Compacted`;
- `InvalidArgument`;
- `Conflict`;
- `CorruptStorage`.

The server maps these results consistently to gRPC status codes. Health is
split into:

- liveness: the process and required actors are running;
- readiness: storage is loaded and the node can either serve the requested
  consistency level or return an actionable leader redirect.

The Java client uses generated async stubs, propagates stream errors, and
returns cancellable watch handles.

## Testing

`raft` unit tests are deterministic and drive events directly. They cover:

- election and term transitions;
- vote restrictions;
- log conflict repair;
- current-term commitment rules;
- one-node commitment;
- stale and failed RPC responses;
- snapshot compaction and installation;
- ReadIndex quorum and applied-index barriers;
- joint and stable configuration quorum calculations.

`testkit` supplies a virtual monotonic clock and a network that can drop,
delay, duplicate, and reorder messages. Nodes can be partitioned, crashed,
restored from disk, and rejoined. Cluster tests continuously assert:

- election safety;
- log matching;
- leader completeness;
- state-machine safety;
- committed entries never roll back;
- joint configuration decisions satisfy both majorities.

Scenario tests cover minority and majority partitions, leader loss during
proposal and ReadIndex, divergent follower repair, WAL replay, snapshot
transfer, learner catch-up, leader removal, and failures between joint and
stable configuration commits.

`server` tests cover MVCC histories, anchor-preserving compaction, transaction
revision semantics, lease attachment cleanup and expiry, snapshot round trips,
watch replay/backpressure/cancellation, and gRPC error mapping. Quarkus
integration tests cover single-node and multi-node public API behavior.

## Removed Structure

The old single `coordinator` module, duplicate schemas, manual scheduler and
thread lifecycle, untyped task envelopes, placeholder health test, and unused
classes/dependencies are removed as their responsibilities move into the five
modules above.
