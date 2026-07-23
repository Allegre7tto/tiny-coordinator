package engine.mvcc;

import com.google.protobuf.ByteString;

import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * 单个 key 的多版本存储。
 * 使用 TreeMap 按 revision 排序，支持范围查询。
 */
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
        public boolean isTombstone() {
            return value.isEmpty();
        }
    }

    public VersionedKeyValue(ByteString key) {
        this.key = key;
        this.versions = new TreeMap<>();
    }

    public ByteString key() { return key; }

    /**
     * 添加新版本
     */
    public void put(ByteString value, long revision, long lease) {
        KvEntry last = latest();
        long createRevision = (last != null) ? last.createRevision() : revision;
        long newVersion = (last != null) ? last.version() + 1 : 1;

        KvEntry entry = new KvEntry(value, createRevision, revision, newVersion, lease);
        versions.put(revision, entry);
    }

    /**
     * 添加 tombstone（软删除）
     */
    public void tombstone(long revision) {
        KvEntry last = latest();
        if (last == null || last.isTombstone()) return;

        KvEntry tomb = new KvEntry(
            ByteString.EMPTY,
            last.createRevision(),
            revision,
            last.version() + 1,
            0
        );
        versions.put(revision, tomb);
    }

    /**
     * 获取指定 revision 的版本
     */
    public KvEntry getAtRevision(long revision) {
        var entry = versions.floorEntry(revision);
        if (entry == null) return null;
        return entry.getValue().isTombstone() ? null : entry.getValue();
    }

    /**
     * 获取最新版本
     */
    public KvEntry latest() {
        var entry = versions.lastEntry();
        if (entry == null) return null;
        return entry.getValue().isTombstone() ? null : entry.getValue();
    }

    /**
     * 获取 [fromRevision, toRevision] 之间的所有版本
     */
    public NavigableMap<Long, KvEntry> getVersionRange(long fromRevision, long toRevision) {
        return versions.subMap(fromRevision, true, toRevision, true);
    }

    /**
     * 获取 >= fromRevision 的所有版本（用于 Watch 历史回放）
     */
    public NavigableMap<Long, KvEntry> getVersionsAfter(long fromRevision) {
        return versions.tailMap(fromRevision, false);
    }

    /**
     * 清理指定 revision 之前的旧版本
     */
    public int compact(long beforeRevision) {
        NavigableMap<Long, KvEntry> toRemove = versions.headMap(beforeRevision, false);
        int count = toRemove.size();
        toRemove.clear();
        return count;
    }

    /**
     * 版本数量
     */
    public int versionCount() {
        return versions.size();
    }
}
