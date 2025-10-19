/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.CacheWriteSynchronizationModeMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public abstract class GridDhtAtomicAbstractUpdateRequest extends GridCacheIdMessage implements GridCacheDeployable {
    /** Skip store flag bit mask. */
    protected static final int DHT_ATOMIC_SKIP_STORE_FLAG_MASK = 0x01;

    /** Keep binary flag. */
    protected static final int DHT_ATOMIC_KEEP_BINARY_FLAG_MASK = 0x02;

    /** Near cache key flag. */
    protected static final int DHT_ATOMIC_NEAR_FLAG_MASK = 0x04;

    /** */
    static final int DHT_ATOMIC_HAS_RESULT_MASK = 0x08;

    /** */
    private static final int DHT_ATOMIC_REPLY_WITHOUT_DELAY = 0x10;

    /** */
    protected static final int DHT_ATOMIC_OBSOLETE_NEAR_KEY_FLAG_MASK = 0x20;

    /** Flag indicating transformation operation was performed. */
    protected static final int DHT_ATOMIC_TRANSFORM_OP_FLAG_MASK = 0x40;

    /** Flag indicating recovery on read repair. */
    protected static final int DHT_ATOMIC_READ_REPAIR_RECOVERY_FLAG_MASK = 0x80;

    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** Future ID on primary. */
    @Order(value = 4, method = "futureId")
    protected long futId;

    /** Write version. */
    @Order(value = 5, method = "writeVersion")
    protected GridCacheVersion writeVer;

    /** Write synchronization mode wrapper message. */
    @Order(value = 6, method = "writeSynchronizationModeMessage")
    protected CacheWriteSynchronizationModeMessage syncModeMsg;

    /** Topology version. */
    @Order(value = 7, method = "topologyVersion")
    protected AffinityTopologyVersion topVer;

    /** Task name hash. */
    @Order(8)
    protected int taskNameHash;

    /** Node ID. */
    protected UUID nodeId;

    /** On response flag. Access should be synced on future. */
    private boolean onRes;

    /** */
    @Order(9)
    private UUID nearNodeId;

    /** */
    @Order(value = 10, method = "nearFutureId")
    private long nearFutId;

    /** Additional flags. */
    @Order(11)
    protected byte flags;

    /**
     * Empty constructor.
     */
    protected GridDhtAtomicAbstractUpdateRequest() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cacheId Cache ID.
     * @param nodeId Node ID.
     */
    protected GridDhtAtomicAbstractUpdateRequest(int cacheId,
        UUID nodeId,
        long futId,
        GridCacheVersion writeVer,
        CacheWriteSynchronizationMode syncMode,
        @NotNull AffinityTopologyVersion topVer,
        int taskNameHash,
        boolean addDepInfo,
        boolean keepBinary,
        boolean skipStore,
        boolean readRepairRecovery
    ) {
        assert topVer.topologyVersion() > 0 : topVer;

        this.cacheId = cacheId;
        this.nodeId = nodeId;
        this.futId = futId;
        this.writeVer = writeVer;
        this.syncModeMsg = new CacheWriteSynchronizationModeMessage(syncMode);
        this.topVer = topVer;
        this.taskNameHash = taskNameHash;
        this.addDepInfo = addDepInfo;

        if (skipStore)
            setFlag(true, DHT_ATOMIC_SKIP_STORE_FLAG_MASK);
        if (keepBinary)
            setFlag(true, DHT_ATOMIC_KEEP_BINARY_FLAG_MASK);
        if (readRepairRecovery)
            setFlag(true, DHT_ATOMIC_READ_REPAIR_RECOVERY_FLAG_MASK);
    }

    /** {@inheritDoc} */
    @Override public final AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @param topVer New topology version.
     */
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /**
     * @param nearNodeId Near node ID.
     * @param nearFutId Future ID on near node.
     */
    void nearReplyInfo(UUID nearNodeId, long nearFutId) {
        assert nearNodeId != null;

        this.nearNodeId = nearNodeId;
        this.nearFutId = nearFutId;
    }

    /**
     * @return {@code True} if backups should reply immediately.
     */
    boolean replyWithoutDelay() {
        return isFlag(DHT_ATOMIC_REPLY_WITHOUT_DELAY);
    }

    /**
     * @param replyWithoutDelay {@code True} if backups should reply immediately.
     */
    void replyWithoutDelay(boolean replyWithoutDelay) {
        setFlag(replyWithoutDelay, DHT_ATOMIC_REPLY_WITHOUT_DELAY);
    }

    /**
     * @param res Result flag.
     */
    void hasResult(boolean res) {
        setFlag(res, DHT_ATOMIC_HAS_RESULT_MASK);
    }

    /**
     * @return Result flag.
     */
    private boolean hasResult() {
        return isFlag(DHT_ATOMIC_HAS_RESULT_MASK);
    }

    /**
     * @return Near node ID.
     */
    public UUID nearNodeId() {
        return nearNodeId;
    }

    /**
     * @param nearNodeId New near node id.
     */
    public void nearNodeId(UUID nearNodeId) {
        this.nearNodeId = nearNodeId;
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Flags.
     */
    public final byte flags() {
        return flags;
    }

    /**
     * @param flags New additional flags.
     */
    public void flags(byte flags) {
        this.flags = flags;
    }

    /**
     * @return Keep binary flag.
     */
    public final boolean keepBinary() {
        return isFlag(DHT_ATOMIC_KEEP_BINARY_FLAG_MASK);
    }

    /**
     * @return Recovery on Read Repair flag.
     */
    public final boolean readRepairRecovery() {
        return isFlag(DHT_ATOMIC_READ_REPAIR_RECOVERY_FLAG_MASK);
    }

    /**
     * @return Skip write-through to a persistent storage.
     */
    public final boolean skipStore() {
        return isFlag(DHT_ATOMIC_SKIP_STORE_FLAG_MASK);
    }

    /**
     * @return {@code True} if transformation operation was performed.
     */
    public final boolean transformOperation() {
        return isFlag(DHT_ATOMIC_TRANSFORM_OP_FLAG_MASK);
    }

    /**
     * @return {@code True} if on response flag changed.
     */
    public boolean onResponse() {
        return !onRes && (onRes = true);
    }

    /**
     * @return {@code True} if response was received.
     */
    boolean hasResponse() {
        return onRes;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /**
     * @return Force transform backups flag.
     */
    public abstract boolean forceTransformBackups();

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext ctx) {
        return ctx.atomicMessageLogger();
    }

    /**
     * @param key Key to add.
     * @param val Value, {@code null} if should be removed.
     * @param entryProcessor Entry processor.
     * @param ttl TTL (optional).
     * @param conflictExpireTime Conflict expire time (optional).
     * @param conflictVer Conflict version (optional).
     * @param addPrevVal If {@code true} adds previous value.
     * @param prevVal Previous value.
     * @param updateCntr Update counter.
     * @param cacheOp Corresponding cache operation.
     */
    public abstract void addWriteValue(KeyCacheObject key,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean addPrevVal,
        @Nullable CacheObject prevVal,
        long updateCntr,
        GridCacheOperation cacheOp);

    /**
     * @param key Key to add.
     * @param val Value, {@code null} if should be removed.
     * @param entryProcessor Entry processor.
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    public abstract void addNearWriteValue(KeyCacheObject key,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long expireTime);

    /**
     * Cleanup values not needed after message was sent.
     */
    protected abstract void cleanup();

    /**
     * @return Task name.
     */
    public final int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @param taskNameHash New task name hash.
     */
    public void taskNameHash(int taskNameHash) {
        this.taskNameHash = taskNameHash;
    }

    /**
     * @return Future ID on primary node.
     */
    public final long futureId() {
        return futId;
    }

    /**
     * @param futId New future ID on primary node.
     */
    public void futureId(long futId) {
        this.futId = futId;
    }

    /**
     * @return Future ID on near node.
     */
    public final long nearFutureId() {
        return nearFutId;
    }

    /**
     * @param nearFutId New near future id.
     */
    public void nearFutureId(long nearFutId) {
        this.nearFutId = nearFutId;
    }

    /**
     * @return Write version.
     */
    public final GridCacheVersion writeVersion() {
        return writeVer;
    }

    /**
     * @param writeVer New write version.
     */
    public void writeVersion(GridCacheVersion writeVer) {
        this.writeVer = writeVer;
    }

    /**
     * @return The write mode synchronization wrapper message.
     */
    public final CacheWriteSynchronizationModeMessage writeSynchronizationModeMessage() {
        return syncModeMsg;
    }

    /**
     * @param writeSyncModeMsg Write syncronization mode wrapper message.
     */
    public void writeSynchronizationModeMessage(CacheWriteSynchronizationModeMessage writeSyncModeMsg) {
        this.syncModeMsg = writeSyncModeMsg;
    }

    /**
     * @return Keys size.
     */
    public abstract int size();

    /**
     * @return Keys size.
     */
    public abstract int nearSize();

    /**
     * @param idx Key index.
     * @return Key.
     */
    public abstract KeyCacheObject key(int idx);

    /**
     * @return Obsolete near cache keys size.
     */
    public abstract int obsoleteNearKeysSize();

    /**
     * @param idx Obsolete near cache key index.
     * @return Obsolete near cache key.
     */
    public abstract KeyCacheObject obsoleteNearKey(int idx);

    /**
     * @param updCntr Update counter.
     * @return Update counter.
     */
    public abstract Long updateCounter(int updCntr);

    /**
     * @param idx Near key index.
     * @return Key.
     */
    public abstract KeyCacheObject nearKey(int idx);

    /**
     * @param idx Key index.
     * @return Value.
     */
    @Nullable public abstract CacheObject value(int idx);

    /**
     * @param idx Key index.
     * @return Value.
     */
    @Nullable public abstract CacheObject previousValue(int idx);

    /**
     * @param idx Key index.
     * @return Entry processor.
     */
    @Nullable public abstract EntryProcessor<Object, Object, Object> entryProcessor(int idx);

    /**
     * @param idx Near key index.
     * @return Value.
     */
    @Nullable public abstract CacheObject nearValue(int idx);

    /**
     * @param idx Key index.
     * @return Transform closure.
     */
    @Nullable public abstract EntryProcessor<Object, Object, Object> nearEntryProcessor(int idx);

    /**
     * @param idx Index.
     * @return Conflict version.
     */
    @Nullable public abstract GridCacheVersion conflictVersion(int idx);

    /**
     * @param idx Index.
     * @return TTL.
     */
    public abstract long ttl(int idx);

    /**
     * @param idx Index.
     * @return TTL for near cache update.
     */
    public abstract long nearTtl(int idx);

    /**
     * @param idx Index.
     * @return Conflict expire time.
     */
    public abstract long conflictExpireTime(int idx);

    /**
     * @param idx Index.
     * @return Expire time for near cache update.
     */
    public abstract long nearExpireTime(int idx);

    /**
     * @return Optional arguments for entry processor.
     */
    @Nullable public abstract Object[] invokeArguments();

    /**
     * Sets flag mask.
     *
     * @param flag Set or clear.
     * @param mask Mask.
     */
    protected final void setFlag(boolean flag, int mask) {
        flags = flag ? (byte)(flags | mask) : (byte)(flags & ~mask);
    }

    /**
     * Reags flag mask.
     *
     * @param mask Mask to read.
     * @return Flag value.
     */
    final boolean isFlag(int mask) {
        return (flags & mask) != 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder flags = new StringBuilder();

        if (skipStore())
            appendFlag(flags, "skipStore");
        if (keepBinary())
            appendFlag(flags, "keepBinary");
        if (isFlag(DHT_ATOMIC_NEAR_FLAG_MASK))
            appendFlag(flags, "near");
        if (hasResult())
            appendFlag(flags, "hasRes");
        if (replyWithoutDelay())
            appendFlag(flags, "resNoDelay");

        return S.toString(GridDhtAtomicAbstractUpdateRequest.class, this,
            "flags", flags.toString());
    }
}
