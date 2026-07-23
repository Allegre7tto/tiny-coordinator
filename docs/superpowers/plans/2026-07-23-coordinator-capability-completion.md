# Coordinator Capability Completion — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix core correctness: byte keys, Txn/Compact through Raft, leader awareness, lease atomicity, complete snapshot. Moderate restructuring without changing packages.

**Architecture:** Replace String keys with protobuf ByteString (unsigned lexicographic order via custom Comparator), use TreeMap for ordered key store, expose Raft node status from Rust via channels, add ReadIndex barrier for linearizable reads, wrap all Raft-bound commands in unified StateMachineCommand with proposal_id.

**Tech Stack:** Rust (Tokio, tonic/prost, jni crate), Java 21 (Quarkus 3.36, Maven, protobuf-java, Micrometer).

---

## File Structure Plan

```
Created:
  coordinator/src/main/java/engine/mvcc/ByteStringComparator.java   — unsigned lexicographic byte comparator
  coordinator/src/main/java/engine/mvcc/DedupWindow.java            — dedup: (clientId, requestId) → result

Modified:
  coordinator/src/main/java/engine/mvcc/MvccStore.java             — String → ByteString keys, TreeMap, single revision
  coordinator/src/main/java/engine/mvcc/VersionedKeyValue.java     — String → ByteString keys
  coordinator/src/main/java/engine/mvcc/TxnManager.java            — ByteString keys, execute() merged into apply path
  coordinator/src/main/java/engine/mvcc/CompactManager.java        — compact() triggers Raft propose, not local
  coordinator/src/main/java/engine/coordinator/StateMachineDriver.java — proposal_id, full Txn/Compact/Lease apply
  coordinator/src/main/java/engine/coordinator/KvStore.java        — ByteString keys, snapshot includes all state
  coordinator/src/main/java/engine/coordinator/CoordinatorGrpcService.java — leader guard, Txn/Compact via Raft, NOT_LEADER
  coordinator/src/main/java/engine/coordinator/LeaseManager.java   — leader-only expiry, atomic revoke+delete
  coordinator/src/main/java/engine/coordinator/WatchManager.java   — ByteString keys, prevKv
  coordinator/src/main/java/engine/client/RaftLib.java            — new methods: isLeader, getTerm, getCommitIndex, getAppliedIndex, readIndex
  coordinator/src/main/java/engine/health/CoordinatorHealthCheck.java — true readiness
  core/engine/src/raftcore.rs                                     — expose isleader/term/commitidx/applyidx, ReadIndex
  core/engine/src/node.rs                                          — channel for node status queries, readindex
  core/jni/src/lib.rs                                              — new JNI: isLeader, getTerm, getCommitIndex, getAppliedIndex, readIndex
```

---

## Phase 1: Byte Key Foundation

### Task 1: ByteString keys everywhere — MvccStore core

**Files:**
- Create: `coordinator/src/main/java/engine/mvcc/ByteStringComparator.java`
- Modify: `coordinator/src/main/java/engine/mvcc/MvccStore.java`
- Modify: `coordinator/src/main/java/engine/mvcc/VersionedKeyValue.java`

**Rationale:** Proto `bytes key` must stay bytes. Convert all `String key` parameters and fields to `com.google.protobuf.ByteString`. Use `TreeMap<ByteString, VersionedKeyValue>` with a `ByteStringComparator` implementing unsigned lexicographic byte comparison (compare each byte as `(b1 & 0xFF) - (b2 & 0xFF)`). Replace `ConcurrentHashMap` with `TreeMap` guarded by `ReentrantReadWriteLock` (apply is already serialized).

- [ ] **Step 1: Write ByteStringComparator**

```java
package engine.mvcc;

import com.google.protobuf.ByteString;
import java.util.Comparator;

public final class ByteStringComparator implements Comparator<ByteString> {
    public static final ByteStringComparator INSTANCE = new ByteStringComparator();

    private ByteStringComparator() {}

    @Override
    public int compare(ByteString a, ByteString b) {
        int mlen = Math.min(a.size(), b.size());
        for (int i = 0; i < mlen; i++) {
            int cmp = (a.byteAt(i) & 0xFF) - (b.byteAt(i) & 0xFF);
            if (cmp != 0) return cmp;
        }
        return a.size() - b.size();
    }
}
```

- [ ] **Step 2: Write VersionedKeyValueTest for ByteString keys**

Add to `coordinator/src/test/java/engine/mvcc/VersionedKeyValueTest.java`:

```java
@Test
void testBinaryKeyNotCorrupted() {
    byte[] raw = new byte[] { (byte) 0x00, (byte) 0xFF, (byte) 0x80 };
    ByteString key = ByteString.copyFrom(raw);
    VersionedKeyValue vkv = new VersionedKeyValue(key);
    vkv.put(ByteString.copyFromUtf8("val"), 1, 0);
    assertArrayEquals(raw, vkv.key().toByteArray());
}

@Test
void testUnsignedLexicographicOrdering() {
    ByteString a = ByteString.copyFrom(new byte[] { (byte) 0xFF });
    ByteString b = ByteString.copyFrom(new byte[] { (byte) 0x00 });
    assertTrue(ByteStringComparator.INSTANCE.compare(a, b) > 0);
}

@Test
void testPrefixOrdering() {
    ByteString a = ByteString.copyFromUtf8("abc");
    ByteString b = ByteString.copyFromUtf8("abcd");
    assertTrue(ByteStringComparator.INSTANCE.compare(a, b) < 0);
    assertTrue(ByteStringComparator.INSTANCE.compare(b, a) > 0);
}
```

Run: `cd coordinator && ./mvnw test -Dtest=VersionedKeyValueTest -pl .`
Expected: new tests FAIL (old ones may still pass on String)

- [ ] **Step 3: Change VersionedKeyValue to use ByteString keys**

In `coordinator/src/main/java/engine/mvcc/VersionedKeyValue.java`:
- Change `private final String key` to `private final ByteString key`
- Change constructor `public VersionedKeyValue(String key)` to `public VersionedKeyValue(ByteString key)`
- Change `public String key()` to `public ByteString key()`
- All internal logic stays — only the type changes

```java
public class VersionedKeyValue {
    private final ByteString key;
    private final TreeMap<Long, KvEntry> versions;

    public record KvEntry(
        ByteString value,
        long createRevision,
        long modRevision,
        long version,
        long lease
    ) {
        public boolean isTombstone() { return value.isEmpty(); }
    }

    public VersionedKeyValue(ByteString key) {
        this.key = key;
        this.versions = new TreeMap<>();
    }

    public ByteString key() { return key; }

    // put, tombstone, getAtRevision, latest, getVersionRange, getVersionsAfter, compact, versionCount
    // — all unchanged except key type
}
```

Run: `cd coordinator && ./mvnw test -Dtest=VersionedKeyValueTest -pl .`
Expected: all tests PASS

- [ ] **Step 4: Change MvccStore to use ByteString + TreeMap**

In `coordinator/src/main/java/engine/mvcc/MvccStore.java`:
- Replace `ConcurrentHashMap<String, VersionedKeyValue> store` with `TreeMap<ByteString, VersionedKeyValue> store`
- Add import for `ByteStringComparator`
- Change all method signatures: `String key` → `ByteString key`, `String startKey`/`endKey` → `ByteString startKey`/`endKey`
- Use `ByteString.EMPTY` instead of `""` for empty range ends
- Range queries use `store.subMap(startKey, true, endKey, false)` or iterate via `tailMap`/`headMap`

Key method signature changes:

```java
public long put(ByteString key, ByteString value, long lease)
public long delete(ByteString key)
public int deleteRange(ByteString startKey, ByteString endKey)
public Optional<VersionedKeyValue.KvEntry> get(ByteString key)
public Optional<VersionedKeyValue.KvEntry> get(ByteString key, long revision)
public record RangeEntry(ByteString key, VersionedKeyValue.KvEntry kv) {}
public RangeResult range(ByteString startKey, ByteString endKey, long revision, long limit)
public WatchEvent — key field becomes ByteString
```

The store field becomes:
```java
private final TreeMap<ByteString, VersionedKeyValue> store =
    new TreeMap<>(ByteStringComparator.INSTANCE);
```

The `range()` method now uses subMap for efficient ordered traversal:
```java
public RangeResult range(ByteString startKey, ByteString endKey, long revision, long limit) {
    rwLock.readLock().lock();
    try {
        if (revision > 0 && revision < compactRevision) {
            throw new IllegalStateException("revision " + revision + " has been compacted");
        }
        long queryRevision = (revision > 0) ? revision : currentRevision.get();
        List<RangeEntry> entries = new ArrayList<>();

        var submap = endKey.isEmpty()
            ? store.tailMap(startKey, true)
            : store.subMap(startKey, true, endKey, false);

        for (var e : submap.entrySet()) {
            VersionedKeyValue.KvEntry kv = e.getValue().getAtRevision(queryRevision);
            if (kv != null) {
                entries.add(new RangeEntry(e.getKey(), kv));
                if (limit > 0 && entries.size() >= limit) {
                    return new RangeResult(entries, queryRevision, true);
                }
            }
        }
        return new RangeResult(entries, queryRevision, false);
    } finally {
        rwLock.readLock().unlock();
    }
}
```

The `deleteRange()` now single-revision (requiring a new method):
```java
public int deleteRange(ByteString startKey, ByteString endKey) {
    rwLock.writeLock().lock();
    try {
        long rev = currentRevision.incrementAndGet();
        int count = 0;
        var submap = endKey.isEmpty()
            ? store.tailMap(startKey, true)
            : store.subMap(startKey, true, endKey, false);
        for (var key : new ArrayList<>(submap.keySet())) {
            VersionedKeyValue vkv = store.get(key);
            if (vkv != null && vkv.latest() != null) {
                vkv.tombstone(rev);
                notifyWatchers(EventType.DELETE, key, rev, vkv.latest());
                count++;
            }
        }
        return count;
    } finally {
        rwLock.writeLock().unlock();
    }
}
```

Run: `cd coordinator && ./mvnw test -Dtest=MvccStoreTest -pl .`
Expected: compilation errors (callers still use String) — fix test file to use ByteString

- [ ] **Step 5: Update MvccStoreTest to use ByteString**

In `coordinator/src/test/java/engine/mvcc/MvccStoreTest.java`:
- Replace all `ByteString.copyFromUtf8("...")` key usages (they already use ByteString for keys? Let's check...)
- Actually the tests already pass ByteString for keys via `put(key, value)`. Just need to update any String literals.
- `assertEquals("key1", entry.key())` becomes `assertEquals(ByteString.copyFromUtf8("key1"), entry.key())`

Run: `cd coordinator && ./mvnw test -Dtest=MvccStoreTest -pl .`
Expected: PASS (all 15+ tests)

- [ ] **Step 6: Commit**

```bash
git add coordinator/src/main/java/engine/mvcc/ByteStringComparator.java \
        coordinator/src/main/java/engine/mvcc/MvccStore.java \
        coordinator/src/main/java/engine/mvcc/VersionedKeyValue.java \
        coordinator/src/test/java/engine/mvcc/VersionedKeyValueTest.java \
        coordinator/src/test/java/engine/mvcc/MvccStoreTest.java
git commit -m "feat: replace String keys with ByteString throughout MVCC, add unsigned lexicographic comparator, use TreeMap for ordered key store"
```

- [ ] **Step 7: Propagate ByteString to TxnManager**

In `coordinator/src/main/java/engine/mvcc/TxnManager.java`:
- Change `Compare.key`, `Op.key`, `Op.rangeEnd` from `String` to `ByteString`
- Remove `parseLong(ByteString)` — instead, revision/version values are decoded from the proto field as `int64` directly
- Add `CompareResult.NOT_EQUAL` to the enum
- Fix `compareValues` to use unsigned lexicographic comparison

```java
public enum CompareResult { EQUAL, GREATER, LESS, NOT_EQUAL }

private boolean compareValues(ByteString actual, ByteString expected, CompareResult result) {
    int cmp = ByteStringComparator.INSTANCE.compare(actual, expected);
    return switch (result) {
        case EQUAL -> cmp == 0;
        case NOT_EQUAL -> cmp != 0;
        case GREATER -> cmp > 0;
        case LESS -> cmp < 0;
    };
}
```

For revision/version comparisons, get the numeric value directly from proto (the value field in Compare proto should be parsed as a protobuf varint, not a UTF-8 string):
```java
private long decodeInt64(ByteString bs) {
    com.google.protobuf.CodedInputStream cis = com.google.protobuf.CodedInputStream.newInstance(bs.toByteArray());
    try { return cis.readInt64(); } catch (java.io.IOException e) { return 0; }
}
```

Update `CoordinatorGrpcService.buildTxnRequest()` accordingly — pass `compare.getKey()` (already ByteString) directly.

Run: `cd coordinator && ./mvnw test -Dtest=TxnManagerTest -pl .`
Expected: PASS after fixing key types

- [ ] **Step 8: Commit**

```bash
git add coordinator/src/main/java/engine/mvcc/TxnManager.java \
        coordinator/src/main/java/engine/coordinator/CoordinatorGrpcService.java \
        coordinator/src/test/java/engine/mvcc/TxnManagerTest.java
git commit -m "feat: propagate ByteString keys to TxnManager, add NOT_EQUAL, fix int64 decode"
```

### Task 2: Propagate ByteString to coordinator layer

**Files:**
- Modify: `coordinator/src/main/java/engine/coordinator/KvStore.java`
- Modify: `coordinator/src/main/java/engine/coordinator/WatchManager.java`
- Modify: `coordinator/src/main/java/engine/coordinator/LeaseManager.java`

- [ ] **Step 1: Update KvStore to use ByteString**

In `KvStore.java`:
- Remove `toStringUtf8()` conversions — `req.getKey()` already returns ByteString
- `WatchEvent` record: change `String key` to `ByteString key`
- `toProto(BytesKey key, ...)` → `toProto(ByteString key, ...)` — use `key` directly instead of `ByteString.copyFromUtf8(key)`
- `applyPut`: pass `req.getKey()` directly
- `applyDelete`: pass `req.getKey()` and `req.getRangeEnd()` directly; empty check uses `rangeEnd.isEmpty()` (ByteString has `isEmpty()`)
- `get(GetRequest)`: remove `.toStringUtf8()` calls
- `snapshot()` and `restore()`: keys are already ByteString in proto

The key change is in `toProto`:
```java
private KeyValue toProto(ByteString key, VersionedKeyValue.KvEntry mv) {
    return KeyValue.newBuilder()
        .setKey(key)  // No conversion — already ByteString
        .setValue(mv.value())
        .setCreateRevision(mv.createRevision())
        .setModRevision(mv.modRevision())
        .setVersion(mv.version())
        .setLease(mv.lease())
        .build();
}
```

- [ ] **Step 2: Update WatchManager to use ByteString**

In `WatchManager.java`:
- Change `WatchEntry` record's `String key` and `String rangeEnd` to `ByteString key` and `ByteString rangeEnd`
- Update `register()` signature
- Update `matches()` to use `ByteStringComparator.INSTANCE.compare()` instead of `String.compareTo()`
- Update `CoordinatorGrpcService.watch()`: pass `cr.getKey()` directly (already ByteString)
- Remove `ByteString.copyFromUtf8(key)` conversions

```java
private boolean matches(WatchEntry w, ByteString key) {
    if (w.rangeEnd() == null || w.rangeEnd().isEmpty())
        return key.equals(w.key());
    return ByteStringComparator.INSTANCE.compare(key, w.key()) >= 0
        && ByteStringComparator.INSTANCE.compare(key, w.rangeEnd()) < 0;
}
```

- [ ] **Step 3: Update LeaseManager to use ByteString**

In `LeaseManager.java`:
- Change `Set<String> keys` in `Lease` record to `Set<ByteString> keys`
- Change `attach(long id, String key)` → `attach(long id, ByteString key)`
- Change `detach(long id, String key)` → `detach(long id, ByteString key)`
- Change `keysOf(long id)` return type to `Set<ByteString>`

- [ ] **Step 4: Verify compilation**

Run: `cd coordinator && ./mvnw compile -pl .`
Expected: compile PASS (no String key usages remain)

- [ ] **Step 5: Run all tests**

Run: `cd coordinator && ./mvnw test -pl .`
Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
git add coordinator/src/main/java/engine/coordinator/KvStore.java \
        coordinator/src/main/java/engine/coordinator/WatchManager.java \
        coordinator/src/main/java/engine/coordinator/LeaseManager.java \
        coordinator/src/main/java/engine/coordinator/CoordinatorGrpcService.java
git commit -m "feat: propagate ByteString keys to KvStore, WatchManager, LeaseManager"
```

---

## Phase 2: Rust Node Status & Leader Awareness

### Task 3: Expose Raft node status to Java via JNI

**Files:**
- Modify: `core/engine/src/raftcore.rs`
- Modify: `core/engine/src/node.rs`
- Modify: `core/jni/src/lib.rs`
- Modify: `coordinator/src/main/java/engine/client/RaftLib.java`

**Rationale:** `isleader` and `getterm` are currently stubs returning `false`/`0`. RaftCore already has `isleader()`, `getterm()`, `commitidx`, `applyidx` fields. Expose these through channels to the JNI layer in a single compact status query.

- [ ] **Step 1: Add status query channel in Rust**

In `core/engine/src/node.rs`, add a `NodeStatus` struct and a oneshot channel for status queries. Rename fields for compactness (`isleader` → `isleader`, keep existing).

Add after existing types:
```rust
#[derive(Debug, Clone, Copy)]
pub struct NodeStatus {
    pub isleader: bool,
    pub term: u64,
    pub commitidx: u64,
    pub applyidx: u64,
    pub leaderid: u64,
}

pub type StatusReq = oneshot::Sender<NodeStatus>;
```

In `RaftNode` struct, add `statustx: mpsc::UnboundedSender<StatusReq>`. In the `RaftNode::start` function:
- Create `(statustx, mut statusrx)`
- Add branch in the `select!` for `statusrx.recv()`:
```rust
Some(respond) = statusrx.recv() => {
    respond.send(NodeStatus {
        isleader: raft.isleader(),
        term: raft.term,
        commitidx: raft.commitidx,
        applyidx: raft.applyidx,
        leaderid: 0, // placeholder: RaftCore doesn't track leader_id for non-leaders yet
    }).ok();
}
```

Update `RaftNode` constructor to pass `statustx` out.

In `core/engine/src/raftcore.rs`, make `isleader()`, `term`, `commitidx`, `applyidx` accessible (they already are within the crate). Add `leaderid` field if not present (when follower, track the leader's ID from AppendEntries RPCs). In `onappendentries`, set `self.leaderid = args.leader_id`.

Also rename methods for compactness:
- `getme()` is fine
- `getpeercount()` is fine
- Already have `isleader()`, `getterm()` — keep these names
- Add `pub leaderid: u64` field, initialize to 0

- [ ] **Step 2: Add JNI functions**

In `core/jni/src/lib.rs`, add three new JNI exports:

```rust
#[export_name = "Java_engine_client_RaftLib_status"]
pub extern "system" fn status<'local>(env: JNIEnv<'local>, _cls: JClass<'local>, ptr: jlong, buf: JObject<'local>) -> jint {
    let b = JByteBuffer::from(buf);
    let addr = env.get_direct_buffer_address(&b).unwrap();
    let cap = env.get_direct_buffer_capacity(&b).unwrap();
    if cap < 41 { return -2 }  // 1 + 8 + 8 + 8 + 8 + 8
    let (tx, rx) = oneshot::channel();
    let s = s!(ptr);
    s.status_tx.send(tx).ok();  // need to add status_tx to S struct
    let st = s.rt.block_on(async { rx.await.unwrap_or(NodeStatus { isleader: false, term: 0, commitidx: 0, applyidx: 0, leaderid: 0 }) });
    let dst = unsafe { std::slice::from_raw_parts_mut(addr, cap) };
    dst[0] = st.isleader as u8;
    dst[1..9].copy_from_slice(&st.term.to_le_bytes());
    dst[9..17].copy_from_slice(&st.commitidx.to_le_bytes());
    dst[17..25].copy_from_slice(&st.applyidx.to_le_bytes());
    dst[25..33].copy_from_slice(&st.leaderid.to_le_bytes());
    33
}
```

Update `struct S` to include `status_tx`:
```rust
struct S { rt: tokio::runtime::Runtime, ptx: tmpsc::UnboundedSender<ProposeReq>, stx: tmpsc::UnboundedSender<SnapSaveReq>, ltx: tmpsc::UnboundedSender<SnapLoadReq>, status_tx: tmpsc::UnboundedSender<StatusReq>, crx: Mutex<mpsc::Receiver<E>> }
```

- [ ] **Step 3: Add Java native methods in RaftLib**

```java
public native int status(ByteBuffer buf);
// layout: 1 byte isleader + 8 bytes term (LE) + 8 bytes commitidx (LE) + 8 bytes applyidx (LE) + 8 bytes leaderid (LE) = 33 bytes

public boolean isLeader() {
    ByteBuffer buf = ByteBuffer.allocateDirect(33);
    int n = status(buf);
    if (n < 33) return false;
    return buf.get(0) != 0;
}

public long getTerm() {
    ByteBuffer buf = ByteBuffer.allocateDirect(33);
    int n = status(buf);
    if (n < 33) return 0;
    return buf.getLong(1);
}

public long getCommitIndex() {
    ByteBuffer buf = ByteBuffer.allocateDirect(33);
    int n = status(buf);
    if (n < 33) return 0;
    return buf.getLong(9);
}

public long getAppliedIndex() {
    ByteBuffer buf = ByteBuffer.allocateDirect(33);
    int n = status(buf);
    if (n < 33) return 0;
    return buf.getLong(17);
}

public long getLeaderId() {
    ByteBuffer buf = ByteBuffer.allocateDirect(33);
    int n = status(buf);
    if (n < 33) return 0;
    return buf.getLong(25);
}
```

- [ ] **Step 4: Build Rust and verify**

```bash
cd core && cargo build --release
```

Expected: compile PASS

- [ ] **Step 5: Commit**

```bash
git add core/engine/src/raftcore.rs core/engine/src/node.rs core/jni/src/lib.rs \
        coordinator/src/main/java/engine/client/RaftLib.java
git commit -m "feat: expose Raft node status (isleader/term/indices/leaderid) from Rust to Java via JNI"
```

### Task 4: Leader-aware gRPC routing

**Files:**
- Modify: `coordinator/src/main/java/engine/coordinator/CoordinatorGrpcService.java`

- [ ] **Step 1: Add leader guard to write gRPC handlers**

In `put()`, `delete()`, `leaseGrant()`, `leaseRevoke()`, `txn()`, `compact()` — before proposing, check if current node is leader:

```java
private void requireLeader(StreamObserver<?> resp) throws StatusException {
    if (!driver.isLeader()) {
        long leaderId = driver.getLeaderId();
        String leaderHint = (leaderId != 0) ? "leader_id=" + leaderId : "";
        throw io.grpc.Status.UNAVAILABLE
            .withDescription("NOT_LEADER " + leaderHint)
            .asException();
    }
}
```

Each write method gets a try-catch for `StatusException`:
```java
@Override
public void put(PutRequest req, StreamObserver<PutResponse> resp) {
    try {
        requireLeader(resp);
        long revision = driver.propose(StateMachineDriver.OP_PUT, req)
                .get(10, TimeUnit.SECONDS);
        resp.onNext(PutResponse.newBuilder()
                .setHeader(ResponseHeader.newBuilder()
                    .setRevision(revision)
                    .setRaftTerm(driver.getTerm()))
                .build());
        resp.onCompleted();
    } catch (StatusException e) {
        resp.onError(e);
    } catch (Exception e) {
        LOG.errorf(e, "Put failed");
        resp.onError(io.grpc.Status.UNAVAILABLE.withCause(e).asRuntimeException());
    }
}
```

- [ ] **Step 2: Add isLeader/getLeaderId/getTerm to StateMachineDriver**

```java
public boolean isLeader() { return raftLib.isLeader(); }
public long getLeaderId() { return raftLib.getLeaderId(); }
public long getTerm() { return raftLib.getTerm(); }
public long getCommitIndex() { return raftLib.getCommitIndex(); }
public long getAppliedIndex() { return raftLib.getAppliedIndex(); }
```

- [ ] **Step 3: Verify compilation**

```bash
cd coordinator && ./mvnw compile -pl .
```

- [ ] **Step 4: Commit**

```bash
git add coordinator/src/main/java/engine/coordinator/CoordinatorGrpcService.java \
        coordinator/src/main/java/engine/coordinator/StateMachineDriver.java
git commit -m "feat: add leader guard to gRPC handlers, return NOT_LEADER with leader hint"
```

---

## Phase 3: StateMachineCommand & Proposal Correctness

### Task 5: StateMachineCommand wrapper + proposal_id

**Files:**
- Modify: `coordinator/src/main/java/engine/coordinator/StateMachineDriver.java`

**Rationale:** Current code uses Raft log index directly as the correlation key for pending proposals — this is racy because the propose returns an index, but by the time it's inserted in `pendingProposals`, an apply could have already happened for a different entry at that index. Fix: generate a unique `proposal_id` (AtomicLong), store it in the command payload, and match on proposal_id not index.

- [ ] **Step 1: Add proposal_id to command encoding**

Change propose() to embed a proposal_id:

```java
private final AtomicLong nextProposalId = new AtomicLong(1);
private final Map<Long, CompletableFuture<Long>> pendingProposals = new ConcurrentHashMap<>();

public CompletableFuture<Long> propose(byte opType, com.google.protobuf.Message msg) {
    byte[] payload = msg.toByteArray();
    long proposalId = nextProposalId.getAndIncrement();

    // Wire format: [opType:1][proposal_id:8 LE][payload:N]
    byte[] data = new byte[9 + payload.length];
    data[0] = opType;
    // pack proposal_id as little-endian
    data[1] = (byte)(proposalId);
    data[2] = (byte)(proposalId >>> 8);
    data[3] = (byte)(proposalId >>> 16);
    data[4] = (byte)(proposalId >>> 24);
    data[5] = (byte)(proposalId >>> 32);
    data[6] = (byte)(proposalId >>> 40);
    data[7] = (byte)(proposalId >>> 48);
    data[8] = (byte)(proposalId >>> 56);
    System.arraycopy(payload, 0, data, 9, payload.length);

    ByteBuffer buf = ByteBuffer.allocateDirect(data.length);
    buf.put(data);

    long encoded = raftLib.propose(buf);
    long statusCode = encoded >> 48;

    CompletableFuture<Long> future = new CompletableFuture<>();
    if (statusCode != 0) {
        future.completeExceptionally(new RuntimeException("propose failed: status=" + statusCode));
        return future;
    }

    pendingProposals.put(proposalId, future);
    future.orTimeout(10, TimeUnit.SECONDS)
          .whenComplete((rev, ex) -> pendingProposals.remove(proposalId));
    return future;
}
```

- [ ] **Step 2: Update applyEntry to extract proposal_id**

```java
private void applyEntry(long index, long term, byte[] data) {
    if (data.length == 0) {
        lastApplied.set(index);
        return;
    }

    byte opType = data[0];
    long proposalId = data.length >= 9 ? ((data[1] & 0xFFL) | ((data[2] & 0xFFL) << 8) | ((data[3] & 0xFFL) << 16) | ((data[4] & 0xFFL) << 24) | ((data[5] & 0xFFL) << 32) | ((data[6] & 0xFFL) << 40) | ((data[7] & 0xFFL) << 48) | ((data[8] & 0xFFL) << 56)) : 0;
    byte[] payload = new byte[data.length - 9];
    System.arraycopy(data, 9, payload, 0, payload.length);

    try {
        switch (opType) {
            case OP_PUT -> { ... }
            case OP_DELETE -> { ... }
            case OP_LEASE_GRANT -> { ... }
            case OP_LEASE_REVOKE -> { ... }
            case OP_TXN -> { ... }
            case OP_COMPACT -> { ... }
        }
    } catch (InvalidProtocolBufferException e) {
        LOG.errorf(e, "Failed to parse entry at index %d", index);
    }

    lastApplied.set(index);

    CompletableFuture<Long> pending = pendingProposals.remove(proposalId);
    if (pending != null) pending.complete(kvStore.revision());

    if (index - lastSnapshotIndex >= SNAPSHOT_INTERVAL) {
        saveSnapshot(index);
    }
}
```

Note: old data format (1-byte opType, no proposal_id) still partially works — we just won't complete the future for those entries. Backward compatibility isn't needed since we're in early dev.

- [ ] **Step 4: Commit**

```bash
git add coordinator/src/main/java/engine/coordinator/StateMachineDriver.java
git commit -m "fix: add proposal_id to command encoding to fix async correlation race"
```

---

## Phase 4: Replicate Txn & Compact through Raft

### Task 6: Txn and Compact go through Raft

**Files:**
- Modify: `coordinator/src/main/java/engine/coordinator/CoordinatorGrpcService.java` — Txn/Compact methods
- Modify: `coordinator/src/main/java/engine/coordinator/StateMachineDriver.java` — apply handlers for OP_TXN, OP_COMPACT
- Modify: `coordinator/src/main/java/engine/mvcc/TxnManager.java` — add applyTxn() for serialized apply
- Modify: `coordinator/src/main/java/engine/mvcc/CompactManager.java` — add applyCompact() for serialized apply

**Rationale:** Txn and Compact currently execute locally without Raft replication, making cluster state divergent. They must be proposed through Raft and deterministically applied.

- [ ] **Step 1: Update CoordinatorGrpcService — Txn through Raft**

Change `txn()` to propose OP_TXN:

```java
@Override
public void txn(TxnRequest req, StreamObserver<TxnResponse> resp) {
    try {
        requireLeader(resp);
        byte[] result = driver.proposeTxn(req)
                .get(10, TimeUnit.SECONDS);
        TxnResponse txnResp = TxnResponse.parseFrom(result);
        resp.onNext(txnResp);
        resp.onCompleted();
    } catch (StatusException e) {
        resp.onError(e);
    } catch (Exception e) {
        LOG.errorf(e, "Txn failed");
        resp.onError(io.grpc.Status.INTERNAL.withCause(e).asRuntimeException());
    }
}
```

- [ ] **Step 2: Add proposeTxn to StateMachineDriver**

Propose returns the serialized TxnResponse from apply:

```java
public CompletableFuture<byte[]> proposeTxn(TxnRequest req) {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    long proposalId = nextProposalId.getAndIncrement();
    byte[] payload = req.toByteArray();
    byte[] data = new byte[9 + payload.length];
    data[0] = OP_TXN;
    packProposalId(data, 1, proposalId);
    System.arraycopy(payload, 0, data, 9, payload.length);
    ByteBuffer buf = ByteBuffer.allocateDirect(data.length);
    buf.put(data);
    long encoded = raftLib.propose(buf);
    long statusCode = encoded >> 48;
    if (statusCode != 0) {
        future.completeExceptionally(new RuntimeException("propose failed"));
        return future;
    }
    txnPending.put(proposalId, future);  // separate map for typed results
    future.orTimeout(10, TimeUnit.SECONDS).whenComplete((r, ex) -> txnPending.remove(proposalId));
    return future;
}
```

Add field: `private final Map<Long, CompletableFuture<byte[]>> txnPending = new ConcurrentHashMap<>();`

- [ ] **Step 3: Implement applyTxn in StateMachineDriver**

The OP_TXN apply handler:

```java
case OP_TXN -> {
    TxnRequest req = TxnRequest.parseFrom(payload);
    TxnResponse resp = txnManager.applyTxn(req);
    byte[] result = resp.toByteArray();
    txnPending.remove(proposalId)?.complete(result);
}
```

- [ ] **Step 4: Add applyTxn to TxnManager**

Move the txn execution logic into a new `applyTxn(TxnRequest)` method that:
1. Evaluates compares
2. Executes Then/Else ops IN ORDER
3. All mutations in the txn share ONE revision
4. Returns the TxnResponse

```java
public TxnResponse applyTxn(TxnRequest req) {
    boolean conditionMet = evaluateCompares(req.getCompareList());
    List<RequestOp> ops = conditionMet ? req.getSuccessList() : req.getFailureList();

    TxnResponse.Builder builder = TxnResponse.newBuilder().setSucceeded(conditionMet);

    for (RequestOp op : ops) {
        OperationResponse.Builder opr = OperationResponse.newBuilder();
        if (op.hasPut()) {
            PutRequest pr = op.getPut();
            long rev = mvccStore.put(pr.getKey(), pr.getValue(), pr.getLease());
            if (pr.getLease() != 0) leaseManager.attach(pr.getLease(), pr.getKey());
            opr.setRevision(rev);
            opr.setPut(PutResponse.newBuilder()
                .setHeader(header(rev)));
        } else if (op.hasDelete()) {
            DeleteRequest dr = op.getDelete();
            int deleted;
            if (dr.getRangeEnd().isEmpty()) {
                deleted = mvccStore.delete(dr.getKey()) > 0 ? 1 : 0;
            } else {
                deleted = mvccStore.deleteRange(dr.getKey(), dr.getRangeEnd());
            }
            opr.setRevision(mvccStore.currentRevision());
            opr.setDelete(DeleteResponse.newBuilder()
                .setDeleted(deleted)
                .setHeader(header(mvccStore.currentRevision())));
        } else if (op.hasRange()) {
            GetRequest gr = op.getRange();
            // Range is a read — included in response but no mutation
            MvccStore.RangeResult rangeResult = mvccStore.range(
                gr.getKey(), gr.getRangeEnd(), 0, gr.getLimit());
            GetResponse.Builder gb = GetResponse.newBuilder();
            for (var e : rangeResult.entries()) {
                gb.addKvs(toProto(e.key(), e.kv()));
            }
            gb.setCount(rangeResult.entries().size());
            gb.setMore(rangeResult.more());
            gb.setHeader(header(mvccStore.currentRevision()));
            opr.setRange(gb.build());
            opr.setRevision(mvccStore.currentRevision());
        }
        builder.addResponses(opr.build());
    }

    return builder.build();
}
```

Note: for the Txn, ALL mutations happen at the latest revision (which is just `currentRevision+1`). The caller must ensure revision is bumped once per command. Since `put()` and `delete()` both increment revision, we need a different approach.

Actually, the single-revision requirement means we should NOT auto-increment revision in `put()`/`delete()`/`deleteRange()`. Instead, the driver bumps revision once per command, and the store uses that revision. Let's handle this properly in Task 7.

For now, keep existing behavior (each op increments revision) — we'll fix in the next task.

- [ ] **Step 5: Compact through Raft**

Similar to Txn: `compact()` gRPC handler proposes OP_COMPACT. In `applyEntry`, the handler:

```java
case OP_COMPACT -> {
    CompactRequest req = CompactRequest.parseFrom(payload);
    CompactResponse resp = compactManager.applyCompact(req);
    compactPending.remove(proposalId)?.complete(resp.toByteArray());
}
```

Add `applyCompact` to CompactManager:
```java
public CompactResponse applyCompact(CompactRequest req) {
    long rev = req.getRevision();
    if (rev < mvccStore.compactRevision()) {
        throw new IllegalStateException("cannot compact: already at " + mvccStore.compactRevision());
    }
    if (rev > mvccStore.currentRevision()) {
        throw new IllegalStateException("cannot compact: beyond current revision");
    }
    int removed = mvccStore.compact(rev);
    return CompactResponse.newBuilder()
        .setRevision(rev)
        .setRemovedCount(removed)
        .build();
}
```

Remove the local CompactManager.compact() call from the gRPC handler.

- [ ] **Step 6: Remove local execution paths**

Delete the old `transact()` call in gRPC handler. Delete the old `compact()` call in gRPC handler.

- [ ] **Step 7: Build and test**

```bash
cd coordinator && ./mvnw compile -pl .
```

Fix any compilation errors from the method signature changes.

- [ ] **Step 8: Commit**

```bash
git add coordinator/src/main/java/engine/coordinator/CoordinatorGrpcService.java \
        coordinator/src/main/java/engine/coordinator/StateMachineDriver.java \
        coordinator/src/main/java/engine/mvcc/TxnManager.java \
        coordinator/src/main/java/engine/mvcc/CompactManager.java
git commit -m "feat: route Txn and Compact through Raft for deterministic replication"
```

---

## Phase 5: Single Revision per Command

### Task 7: Revision allocated once per apply, not per op

**Files:**
- Modify: `coordinator/src/main/java/engine/mvcc/MvccStore.java`
- Modify: `coordinator/src/main/java/engine/coordinator/StateMachineDriver.java`

**Rationale:** Currently `put()` and `delete()` each call `currentRevision.incrementAndGet()`, so `deleteRange` gets N revisions for N keys. A single replicated command must produce ONE revision. Fix: the apply loop allocates one revision, passes it as a parameter.

- [ ] **Step 1: Change MvccStore to accept revision parameter**

Add overloads that take an explicit revision:
```java
public long putAtRevision(ByteString key, ByteString value, long revision, long lease)
public long deleteAtRevision(ByteString key, long revision)
public int deleteRangeAtRevision(ByteString startKey, ByteString endKey, long revision)
```

The old `put(String key, ByteString value)` increments revision itself — keep for compatibility but mark as internal.

Implementation of `putAtRevision`:
```java
public long putAtRevision(ByteString key, ByteString value, long revision, long lease) {
    rwLock.writeLock().lock();
    try {
        VersionedKeyValue vkv = store.computeIfAbsent(key, VersionedKeyValue::new);
        vkv.put(value, revision, lease);
        notifyWatchers(EventType.PUT, key, revision, vkv.latest());
        return revision;
    } finally {
        rwLock.writeLock().unlock();
    }
}
```

Same pattern for `deleteAtRevision`, `deleteRangeAtRevision`.

- [ ] **Step 2: Bump revision once in the driver's apply**

In `applyEntry`, before the switch:
```java
long rev = mvccStore.currentRevision() + 1;
mvccStore.setCurrentRevision(rev);
```

Then all apply handlers use `rev` instead of letting the store auto-increment.

Update apply handlers:
```java
case OP_PUT -> {
    PutRequest req = PutRequest.parseFrom(payload);
    kvStore.applyPut(req, rev);
    if (req.getLease() != 0) leaseManager.attach(req.getLease(), req.getKey());
}
case OP_DELETE -> {
    DeleteRequest req = DeleteRequest.parseFrom(payload);
    kvStore.applyDelete(req, rev);
}
```

Update KvStore methods:
```java
public void applyPut(PutRequest req, long rev) {
    mvccStore.putAtRevision(req.getKey(), req.getValue(), rev, req.getLease());
}
public void applyDelete(DeleteRequest req, long rev) {
    if (req.getRangeEnd().isEmpty()) {
        mvccStore.deleteAtRevision(req.getKey(), rev);
    } else {
        mvccStore.deleteRangeAtRevision(req.getKey(), req.getRangeEnd(), rev);
    }
}
```

For `applyTxn`, the single revision is already set by the driver before calling.

- [ ] **Step 3: Remove public auto-rev methods on MvccStore**

Remove `put(ByteString, ByteString)` and `delete(String)` (the auto-incrementing ones). Keep only the `xxxAtRevision` variants.

- [ ] **Step 4: Run tests**

```bash
cd coordinator && ./mvnw test -pl .
```

Fix compilation errors in tests — tests should use `putAtRevision(key, value, rev, 0)`.

- [ ] **Step 5: Commit**

```bash
git add coordinator/src/main/java/engine/mvcc/MvccStore.java \
        coordinator/src/main/java/engine/coordinator/StateMachineDriver.java \
        coordinator/src/main/java/engine/coordinator/KvStore.java \
        coordinator/src/main/java/engine/mvcc/TxnManager.java \
        coordinator/src/test/java/engine/mvcc/MvccStoreTest.java
git commit -m "fix: allocate one revision per apply command, not per internal op"
```

---

## Phase 6: Lease Completeness

### Task 8: Atomic lease revoke + leader-only expiry + server-generated ID

**Files:**
- Modify: `coordinator/src/main/java/engine/coordinator/LeaseManager.java`
- Modify: `coordinator/src/main/java/engine/coordinator/CoordinatorGrpcService.java`
- Modify: `coordinator/src/main/java/engine/coordinator/StateMachineDriver.java`

**Rationale:** Lease revoke and key deletion must happen atomically in one Raft command. Expiry check must only run on the leader. Lease ID generation needed when `id=0`.

- [ ] **Step 1: Add LeaseExpire op type and atomic revoke**

Add `OP_LEASE_EXPIRE = 0x07` and `OP_LEASE_RENEW = 0x08` to `StateMachineDriver`.

Define an internal message for lease expire in a proto-compatible way. For simplicity, define a Java record that serializes to bytes:
```java
record LeaseExpireCommand(long leaseId, List<ByteString> keys) {}
```

But actually, let's just encode the leaseId and key list directly. Or better yet, create a new proto message — but that requires modifying `coordinator.proto`. For now, use a deterministic byte encoding:

```
[op_type:1][proposal_id:8][lease_id:8 LE][key_count:4 LE][key1_len:4 LE][key1_bytes][key2_len:4 LE][key2_bytes]...
```

This is getting complex. Simpler approach: create a `LeaseExpireRequest` proto message. Add to `coordinator.proto`:
```protobuf
message LeaseExpireRequest {
  int64 id = 1;
}
```

And a `LeaseRenewRequest`:
```protobuf
message LeaseRenewRequest {
  int64 id = 1;
}
```

Update the proto, regenerate Java code.

Wait — the build process regenerates proto code. Let me check if there's a Maven plugin for this. The pom.xml should have protobuf-maven-plugin or similar.

Let me simplify: don't add new proto messages. Instead, use `LeaseRevokeRequest` for revoke (already exists) and encode the lease_id for expire/renew directly in the raw bytes. Since expire and renew don't need the full proto structure.

Actually, let's just use an `int64` field directly for these commands:
```java
// For OP_LEASE_RENEW: payload is 8 bytes (leaseId LE)
// For OP_LEASE_EXPIRE: payload is 8 bytes (leaseId LE)
```

- [ ] **Step 2: Leader-only expiry check**

In `LeaseManager.checkExpiry()`:
```java
@Scheduled(every = "1s")
void checkExpiry() {
    if (!driver.isLeader()) return;
    Instant now = Instant.now();
    List<Long> expiredIds = new ArrayList<>();
    leases.forEach((id, lease) -> {
        if (now.isAfter(lease.deadline())) expiredIds.add(id);
    });
    for (long id : expiredIds) {
        try {
            // Propose OP_LEASE_EXPIRE — atomic revoke + key deletion
            driver.proposeLeaseExpire(id);
        } catch (Exception e) {
            LOG.warnf("Failed to propose lease expire for id=%d", id, e);
        }
    }
}
```

`proposeLeaseExpire` in driver:
```java
public void proposeLeaseExpire(long leaseId) {
    // The expire is handled when apply runs — the lease and its keys are deleted atomically
    byte[] data = new byte[9 + 8];
    data[0] = OP_LEASE_EXPIRE;
    packProposalId(data, 1, nextProposalId.getAndIncrement());
    // Pack leaseId as LE
    data[9] = (byte)(leaseId);
    data[10] = (byte)(leaseId >>> 8);
    // ... etc.
    ByteBuffer buf = ByteBuffer.allocateDirect(data.length);
    buf.put(data);
    raftLib.propose(buf);
}
```

- [ ] **Step 3: OP_LEASE_EXPIRE apply handler — atomic delete**

```java
case OP_LEASE_EXPIRE -> {
    long leaseId = decodeInt64LE(payload, 0);
    Set<ByteString> keys = leaseManager.keysOf(leaseId);
    for (ByteString key : keys) {
        kvStore.applyDelete(DeleteRequest.newBuilder().setKey(key).build(), rev);
    }
    leaseManager.applyRevoke(leaseId);
}
```

- [ ] **Step 4: OP_LEASE_RENEW for KeepAlive through Raft**

Change `leaseKeepAlive` gRPC handler to propose OP_LEASE_RENEW:
```java
@Override
public StreamObserver<LeaseKeepAliveRequest> leaseKeepAlive(
        StreamObserver<LeaseKeepAliveResponse> resp) {
    return new StreamObserver<>() {
        @Override
        public void onNext(LeaseKeepAliveRequest req) {
            try {
                driver.propose(StateMachineDriver.OP_LEASE_RENEW, req).get(10, TimeUnit.SECONDS);
                long ttl = leaseManager.remaining(req.getId());
                resp.onNext(LeaseKeepAliveResponse.newBuilder()
                        .setId(req.getId()).setTtl(ttl).build());
            } catch (Exception e) {
                resp.onError(io.grpc.Status.NOT_FOUND
                        .withDescription(e.getMessage()).asRuntimeException());
            }
        }
        ...
    };
}
```

Apply handler:
```java
case OP_LEASE_RENEW -> {
    LeaseKeepAliveRequest req = LeaseKeepAliveRequest.parseFrom(payload);
    leaseManager.renewOnApply(req.getId());
}
```

Add `renewOnApply` to LeaseManager:
```java
public void renewOnApply(long id) {
    Lease lease = leases.get(id);
    if (lease == null) throw new IllegalArgumentException("lease not found: " + id);
    Instant newDeadline = Instant.now().plusSeconds(lease.ttlSeconds());
    leases.put(id, lease.withDeadline(newDeadline));
}
```

- [ ] **Step 5: Server-generated Lease ID**

In `leaseGrant()` gRPC handler:
```java
@Override
public void leaseGrant(LeaseGrantRequest req, StreamObserver<LeaseGrantResponse> resp) {
    try {
        requireLeader(resp);
        long leaseId = req.getId();
        if (leaseId == 0) {
            leaseId = leaseManager.generateId();
            req = req.toBuilder().setId(leaseId).build();
        }
        driver.propose(StateMachineDriver.OP_LEASE_GRANT, req).get(10, TimeUnit.SECONDS);
        resp.onNext(LeaseGrantResponse.newBuilder()
                .setId(leaseId).setTtl(req.getTtl())
                .setHeader(header()).build());
        resp.onCompleted();
    } catch (StatusException e) {
        resp.onError(e);
    } catch (Exception e) {
        resp.onNext(LeaseGrantResponse.newBuilder().setError(e.getMessage()).build());
        resp.onCompleted();
    }
}
```

Add to LeaseManager:
```java
private final AtomicLong nextLeaseId = new AtomicLong(1);
public long generateId() { return nextLeaseId.getAndIncrement(); }
```

- [ ] **Step 6: Commit**

```bash
git add coordinator/src/main/java/engine/coordinator/LeaseManager.java \
        coordinator/src/main/java/engine/coordinator/CoordinatorGrpcService.java \
        coordinator/src/main/java/engine/coordinator/StateMachineDriver.java
git commit -m "feat: atomic lease revoke+delete, leader-only expiry, server-generated ids, renew through Raft"
```

---

## Phase 7: Snapshot Completeness

### Task 9: Complete snapshot with lease/dedup/compact state

**Files:**
- Modify: `coordinator/src/main/java/engine/coordinator/KvStore.java`
- Modify: `coordinator/src/main/java/engine/coordinator/StateMachineDriver.java`
- Modify: `coordinator/src/main/java/engine/mvcc/MvccStore.java`

**Rationale:** Snapshot must include lease state, lease-key bindings, compact revision, and dedup state so restart recovers complete cluster state.

- [ ] **Step 1: Define Snapshot proto**

Add to `coordinator.proto`:

```protobuf
message LeaseState {
  int64  id         = 1;
  int64  ttl_seconds = 2;
  int64  deadline_millis = 3;
  repeated bytes keys = 4;
}

message StateMachineSnapshot {
  int64  last_applied_index = 1;
  int64  mvcc_revision       = 2;
  int64  compact_revision    = 3;
  repeated KeyValue kvs      = 4;
  repeated LeaseState leases = 5;
  // Dedup state omitted for now — will add in Task 10
}
```

Regenerate Java proto code:
```bash
cd coordinator && ./mvnw generate-sources -pl .
```

- [ ] **Step 2: Update snapshot() to include leases**

In `KvStore.snapshot()`:
```java
public byte[] snapshot(LeaseManager leaseManager) {
    StateMachineSnapshot.Builder snap = StateMachineSnapshot.newBuilder()
        .setLastAppliedIndex(lastApplied)
        .setMvccRevision(mvccStore.currentRevision())
        .setCompactRevision(mvccStore.compactRevision());

    for (var entry : mvccStore.snapshotEntries()) {
        snap.addKvs(toProto(entry.key(), entry.kv()));
    }
    for (var lease : leaseManager.allLeases()) {
        LeaseState.Builder lb = LeaseState.newBuilder()
            .setId(lease.id())
            .setTtlSeconds(lease.ttl())
            .setDeadlineMillis(lease.deadline().toEpochMilli());
        for (var key : lease.keys()) lb.addKeys(key);
        snap.addLeases(lb.build());
    }

    return snap.build().toByteArray();
}
```

Need to access `StateMachineDriver.lastApplied` and add `allLeases()` to LeaseManager.

- [ ] **Step 3: Update restore() to restore leases**

In `KvStore.restore()`:
```java
public void restore(byte[] data, LeaseManager leaseManager, StateMachineDriver driver) {
    StateMachineSnapshot snap = StateMachineSnapshot.parseFrom(data);

    // Restore KV
    List<MvccStore.RangeEntry> entries = new ArrayList<>();
    for (KeyValue kv : snap.getKvsList()) {
        entries.add(new MvccStore.RangeEntry(kv.getKey(), new VersionedKeyValue.KvEntry(
            kv.getValue(), kv.getCreateRevision(), kv.getModRevision(),
            kv.getVersion(), kv.getLease())));
    }
    mvccStore.restoreFromEntries(entries, snap.getMvccRevision());
    mvccStore.setCompactRevision(snap.getCompactRevision());

    // Restore leases
    for (LeaseState ls : snap.getLeasesList()) {
        leaseManager.restoreLease(
            ls.getId(), ls.getTtlSeconds(),
            Instant.ofEpochMilli(ls.getDeadlineMillis()),
            new HashSet<>(ls.getKeysList().stream().map(ByteString.class::cast).toList()));
    }

    driver.setLastApplied(snap.getLastAppliedIndex());
}
```

- [ ] **Step 4: Fix async snapshot generation**

In `StateMachineDriver.saveSnapshot()`:
```java
private void saveSnapshot(long index) {
    // Capture state synchronously
    byte[] snapData = kvStore.snapshot(leaseManager);
    ByteBuffer buf = ByteBuffer.allocateDirect(snapData.length);
    buf.put(snapData);
    raftLib.snap(index, buf);
    lastSnapshotIndex = index;
    LOG.infof("Snapshot saved at index=%d size=%d", index, snapData.length);
}
```

Remove the `CompletableFuture.runAsync()` wrapper — snapshot must capture state atomically while the write lock is held or implicit ordering of the apply thread ensures consistency.

- [ ] **Step 5: Update loadSnapshot to restore all state**

```java
private void loadSnapshot() {
    ByteBuffer buf = ByteBuffer.allocateDirect(MAX_SNAP_SIZE);
    int size = raftLib.load(buf);
    if (size <= 0) {
        LOG.info("No existing snapshot, starting fresh");
        return;
    }
    byte[] data = new byte[size];
    buf.position(0);
    buf.get(data);
    kvStore.restore(data, leaseManager, this);
    LOG.infof("Loaded snapshot size=%d", size);
}
```

- [ ] **Step 6: Add compact revision restore support**

In `MvccStore`:
```java
public void setCompactRevision(long rev) { compactRevision = rev; }
```

- [ ] **Step 7: Add lease restore to LeaseManager**

```java
public record LeaseSnapshot(long id, long ttlSeconds, Instant deadline, Set<ByteString> keys) {}

public void restoreLease(long id, long ttlSeconds, Instant deadline, Set<ByteString> keys) {
    leases.put(id, new Lease(id, ttlSeconds, deadline, ConcurrentHashMap.newKeySet()));
    leases.get(id).keys().addAll(keys);
}

public List<LeaseSnapshot> allLeases() {
    List<LeaseSnapshot> result = new ArrayList<>();
    leases.forEach((id, l) -> result.add(new LeaseSnapshot(id, l.ttlSeconds(), l.deadline(), Set.copyOf(l.keys()))));
    return result;
}
```

- [ ] **Step 8: Compile and fix proto references**

```bash
cd coordinator && ./mvnw compile -pl .
```

Fix any import issues for the new proto types.

- [ ] **Step 9: Commit**

```bash
git add proto/coordinator.proto \
        coordinator/src/main/java/engine/coordinator/KvStore.java \
        coordinator/src/main/java/engine/coordinator/StateMachineDriver.java \
        coordinator/src/main/java/engine/coordinator/LeaseManager.java \
        coordinator/src/main/java/engine/mvcc/MvccStore.java
git commit -m "feat: complete snapshot with lease state, compact revision; synchronous snapshot capture"
```

---

## Phase 8: Structure Cleanup

### Task 10: Reorganize apply logic and fix method co-location

**Files:**
- Modify: `coordinator/src/main/java/engine/coordinator/StateMachineDriver.java`
- Modify: `coordinator/src/main/java/engine/coordinator/KvStore.java`
- Modify: `coordinator/src/main/java/engine/mvcc/TxnManager.java`

**Goal:** `StateMachineDriver` should be thin — dispatch only. Move apply-specific logic to the respective managers. Make the driver delegate to `KvStore`, `LeaseManager`, `TxnManager` rather than implementing business logic itself.

- [ ] **Step 1: Move apply logic to KvStore**

Add `apply(byte opType, byte[] payload, long rev)` to KvStore:
```java
public void apply(byte opType, byte[] payload, long rev, LeaseManager leaseManager) throws Exception {
    switch (opType) {
        case StateMachineDriver.OP_PUT -> {
            PutRequest req = PutRequest.parseFrom(payload);
            applyPut(req, rev);
            if (req.getLease() != 0) leaseManager.attach(req.getLease(), req.getKey());
        }
        case StateMachineDriver.OP_DELETE -> applyDelete(DeleteRequest.parseFrom(payload), rev);
        case StateMachineDriver.OP_LEASE_GRANT -> {
            LeaseGrantRequest req = LeaseGrantRequest.parseFrom(payload);
            leaseManager.applyGrant(req.getId(), req.getTtl(), rev);
        }
        case StateMachineDriver.OP_LEASE_REVOKE ->
            leaseManager.applyRevoke(LeaseRevokeRequest.parseFrom(payload).getId());
        case StateMachineDriver.OP_LEASE_EXPIRE -> {
            long leaseId = decodeInt64LE(payload, 0);
            Set<ByteString> keys = leaseManager.keysOf(leaseId);
            for (ByteString key : keys) {
                applyDelete(DeleteRequest.newBuilder().setKey(key).build(), rev);
            }
            leaseManager.applyRevoke(leaseId);
        }
        case StateMachineDriver.OP_LEASE_RENEW -> {
            LeaseKeepAliveRequest req = LeaseKeepAliveRequest.parseFrom(payload);
            leaseManager.renewOnApply(req.getId());
        }
        default -> LOG.warnf("Unknown op type in KvStore.apply: 0x%02x", opType);
    }
}
```

Let `StateMachineDriver.applyEntry` call:
```java
switch (opType) {
    case OP_TXN -> {
        TxnRequest req = TxnRequest.parseFrom(payload);
        byte[] result = txnManager.applyTxn(req).toByteArray();
        txnPending.remove(proposalId)?.complete(result);
    }
    case OP_COMPACT -> {
        CompactRequest req = CompactRequest.parseFrom(payload);
        byte[] result = compactManager.applyCompact(req).toByteArray();
        compactPending.remove(proposalId)?.complete(result);
    }
    default -> kvStore.apply(opType, payload, rev, leaseManager);
}
```

- [ ] **Step 2: Remove stale PLAN.md**

```bash
git rm PLAN.md
```

- [ ] **Step 3: Verify all tests pass**

```bash
cd core && cargo test
cd coordinator && ./mvnw test -pl .
```

Fix any compilation errors from the reorganization.

- [ ] **Step 4: Final commit**

```bash
git add coordinator/src/main/java/engine/coordinator/StateMachineDriver.java \
        coordinator/src/main/java/engine/coordinator/KvStore.java \
        coordinator/src/main/java/engine/mvcc/TxnManager.java
git commit -m "refactor: thin StateMachineDriver delegate to KvStore; remove stale PLAN.md"
git rm PLAN.md
git commit -m "chore: remove stale PLAN.md"
```

---

## Spec Coverage Verification

| Design Doc Section | Task(s) | Status |
|---|---|---|
| 4.1 Unified replicated commands & deterministic apply | Task 5, 6, 7 | proposal_id, Txn/Compact via Raft, single revision |
| 4.2 MVCC & revision | Task 7 | Single revision per command |
| 4.3 Raw byte keys & ordered range | Task 1, 2 | ByteString keys, TreeMap ordering |
| 4.4 Atomic Txn/CAS | Task 6 | Txn via Raft, same revision |
| 4.5 Read consistency & leader awareness | Task 3, 4 | Node status, leader guard, NOT_LEADER |
| 4.6 Recoverable Watch | Task 2 (bytes only) | prevKv, compact checks deferred to follow-up |
| 4.7 Complete Lease lifecycle | Task 8 | Atomic revoke, leader-only expiry, server IDs, renew via Raft |
| 4.8 Snapshot & crash recovery | Task 9 | Complete snapshot, synchronous capture |
| 4.9 JNI protocol safety | Task 3, 5 | Status query, proposal encoding |
| 4.10 Cluster visibility | Task 4 | ResponseHeader with term, NOT_LEADER with hint |
| 4.11 Observability | — | Deferred to follow-up |

**Scope not covered (deferred):**
- Watch: prevKv, compacted error, progress notification, bounded queue
- Metrics (Micrometer)
- ReadIndex for linearizable reads (partial — leader guard ensures reads go to leader)
- MemberList with actual members
- Dedup state (client_id/request_id)
- JNI max message size checks on Rust side
- Unsigned lexicographic comparison in Rust (byte comparison is Rust default, but JNI side needs explicit)

