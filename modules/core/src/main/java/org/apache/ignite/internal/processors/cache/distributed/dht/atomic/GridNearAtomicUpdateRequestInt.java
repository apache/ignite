package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.util.List;
import java.util.UUID;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.jetbrains.annotations.Nullable;

public interface GridNearAtomicUpdateRequestInt {
    /**
     * @return Mapped node ID.
     */
    UUID nodeId();

    /**
     * @param nodeId Node ID.
     */
    void nodeId(UUID nodeId);

    /**
     * @return Subject ID.
     */
    UUID subjectId();

    /**
     * @return Task name hash.
     */
    int taskNameHash();

    /**
     * @return Future version.
     */
    GridCacheVersion futureVersion();

    /**
     * @return Flag indicating whether this is fast-map udpate.
     */
    boolean fastMap();

    /**
     * @return Update version for fast-map request.
     */
    GridCacheVersion updateVersion();

    /**
     * @return Topology locked flag.
     */
    boolean topologyLocked();

    /**
     * @return {@code True} if request sent from client node.
     */
    boolean clientRequest();

    /**
     * @return Cache write synchronization mode.
     */
    CacheWriteSynchronizationMode writeSynchronizationMode();

    /**
     * @return Expiry policy.
     */
    ExpiryPolicy expiry();

    /**
     * @return Return value flag.
     */
    boolean returnValue();

    /**
     * @return Filter.
     */
    @Nullable CacheEntryPredicate[] filter();

    /**
     * @return Skip write-through to a persistent storage.
     */
    boolean skipStore();

    /**
     * @return Keep binary flag.
     */
    boolean keepBinary();

    /**
     * @return Update operation.
     */
    GridCacheOperation operation();

    /**
     * @return Optional arguments for entry processor.
     */
    @Nullable Object[] invokeArguments();

    /**
     * @return Flag indicating whether this request contains primary keys.
     */
    boolean hasPrimary();

    /**
     * @param res Response.
     * @return {@code True} if current response was {@code null}.
     */
    boolean onResponse(GridNearAtomicUpdateResponse res);

    /**
     * @return Response.
     */
    @Nullable GridNearAtomicUpdateResponse response();

    /**
     * @param key Key to add.
     * @param val Optional update value.
     * @param conflictTtl Conflict TTL (optional).
     * @param conflictExpireTime Conflict expire time (optional).
     * @param conflictVer Conflict version (optional).
     * @param primary If given key is primary on this mapping.
     */
    void addUpdateEntry(KeyCacheObject key,
        @Nullable Object val,
        long conflictTtl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean primary);

    /**
     * @return Keys for this update request.
     */
    List<KeyCacheObject> keys();

    /**
     * @return Values for this update request.
     */
    List<?> values();

    /**
     * @param idx Key index.
     * @return Value.
     */
    @SuppressWarnings("unchecked") CacheObject value(int idx);

    /**
     * @param idx Key index.
     * @return Entry processor.
     */
    @SuppressWarnings("unchecked") EntryProcessor<Object, Object, Object> entryProcessor(int idx);

    /**
     * @param idx Index to get.
     * @return Write value - either value, or transform closure.
     */
    CacheObject writeValue(int idx);

    /**
     * @return Message ID.
     */
    public long messageId();

    /**
     * Gets topology version or -1 in case of topology version is not required for this message.
     *
     * @return Topology version.
     */
    AffinityTopologyVersion topologyVersion();

    /**
     * @return Conflict versions.
     */
    @Nullable List<GridCacheVersion> conflictVersions();

    /**
     * @param idx Index.
     * @return Conflict version.
     */
    @Nullable GridCacheVersion conflictVersion(int idx);

    /**
     * @param idx Index.
     * @return Conflict TTL.
     */
    long conflictTtl(int idx);

    /**
     * @param idx Index.
     * @return Conflict expire time.
     */
    long conflictExpireTime(int idx);

    /**
     * Cleanup values.
     *
     * @param clearKeys If {@code true} clears keys.
     */
    void cleanup(boolean clearKeys);
}
