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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public abstract class GridNearAtomicAbstractUpdateRequest extends GridCacheIdMessage implements GridCacheDeployable {
    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** . */
    private static final int NEED_PRIMARY_RES_FLAG_MASK = 0x01;

    /** Topology locked flag. Set if atomic update is performed inside TX or explicit lock. */
    private static final int TOP_LOCKED_FLAG_MASK = 0x02;

    /** Skip write-through to a persistent storage. */
    private static final int SKIP_STORE_FLAG_MASK = 0x04;

    /** Keep binary flag. */
    private static final int KEEP_BINARY_FLAG_MASK = 0x08;

    /** Return value flag. */
    private static final int RET_VAL_FLAG_MASK = 0x10;

    /** Recovery value flag. */
    private static final int RECOVERY_FLAG_MASK = 0x20;

    /** */
    private static final int NEAR_CACHE_FLAG_MASK = 0x40;

    /** */
    private static final int AFFINITY_MAPPING_FLAG_MASK = 0x80;

    /** Target node ID. */
    @GridDirectTransient
    protected UUID nodeId;

    /** Future version. */
    protected long futId;

    /** Topology version. */
    protected AffinityTopologyVersion topVer;

    /** Write synchronization mode. */
    protected CacheWriteSynchronizationMode syncMode;

    /** Update operation. */
    protected GridCacheOperation op;

    /** Subject ID. */
    protected UUID subjId;

    /** Task name hash. */
    protected int taskNameHash;

    /** Compressed boolean flags. Make sure 'toString' is updated when add new flag. */
    @GridToStringExclude
    protected byte flags;

    /** */
    @GridDirectTransient
    private GridNearAtomicUpdateResponse res;

    /**
     *
     */
    public GridNearAtomicAbstractUpdateRequest() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cacheId Cache ID.
     * @param nodeId Node ID.
     * @param futId Future ID.
     * @param topVer Topology version.
     * @param syncMode Synchronization mode.
     * @param op Cache update operation.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param flags Flags.
     * @param addDepInfo Deployment info flag.
     */
    protected GridNearAtomicAbstractUpdateRequest(
        int cacheId,
        UUID nodeId,
        long futId,
        @NotNull AffinityTopologyVersion topVer,
        CacheWriteSynchronizationMode syncMode,
        GridCacheOperation op,
        @Nullable UUID subjId,
        int taskNameHash,
        byte flags,
        boolean addDepInfo
    ) {
        this.cacheId = cacheId;
        this.nodeId = nodeId;
        this.futId = futId;
        this.topVer = topVer;
        this.syncMode = syncMode;
        this.op = op;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.flags = flags;
        this.addDepInfo = addDepInfo;
    }

    /**
     * @param nearCache {@code True} if near cache enabled on originating node.
     * @param topLocked Topology locked flag.
     * @param retval Return value required flag.
     * @param affMapping {@code True} if originating node detected that rebalancing finished and
     *    expects that update is mapped using current affinity.
     * @param needPrimaryRes {@code True} if near node waits for primary response.
     * @param skipStore Skip write-through to a CacheStore flag.
     * @param keepBinary Keep binary flag.
     * @param recovery Recovery mode flag.
     * @return Flags.
     */
    static byte flags(
        boolean nearCache,
        boolean topLocked,
        boolean retval,
        boolean affMapping,
        boolean needPrimaryRes,
        boolean skipStore,
        boolean keepBinary,
        boolean recovery) {
        byte flags = 0;

        if (nearCache)
            flags |= NEAR_CACHE_FLAG_MASK;

        if (topLocked)
            flags |= TOP_LOCKED_FLAG_MASK;

        if (retval)
            flags |= RET_VAL_FLAG_MASK;

        if (affMapping)
            flags |= AFFINITY_MAPPING_FLAG_MASK;

        if (needPrimaryRes)
            flags |= NEED_PRIMARY_RES_FLAG_MASK;

        if (skipStore)
            flags |= SKIP_STORE_FLAG_MASK;

        if (keepBinary)
            flags |= KEEP_BINARY_FLAG_MASK;

        if (recovery)
            flags |= RECOVERY_FLAG_MASK;

        return flags;
    }

    /**
     * @return {@code True} if originating node detected that rebalancing finished and
     *    expects that update is mapped using current affinity.
     */
    boolean affinityMapping() {
        return isFlag(AFFINITY_MAPPING_FLAG_MASK);
    }

    /**
     * @return {@code True} if near cache is enabled on node initiated operation.
     */
    public boolean nearCache() {
        return isFlag(NEAR_CACHE_FLAG_MASK);
    }

    /** {@inheritDoc} */
    @Override public final AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public final int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /** {@inheritDoc} */
    @Override public final boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public final IgniteLogger messageLogger(GridCacheSharedContext ctx) {
        return ctx.atomicMessageLogger();
    }

    /**
     * @return {@code True} if near node is able to initialize update mapping locally.
     */
    boolean initMappingLocally() {
        return !needPrimaryResponse() && fullSync();
    }

    /**
     * @return {@code True} if near node waits for primary response.
     */
    boolean needPrimaryResponse() {
        return isFlag(NEED_PRIMARY_RES_FLAG_MASK);
    }

    /**
     * @param needRes {@code True} if near node waits for primary response.
     */
    void needPrimaryResponse(boolean needRes) {
        setFlag(needRes, NEED_PRIMARY_RES_FLAG_MASK);
    }

    /**
     * @return {@code True} if update is processed in {@link CacheWriteSynchronizationMode#FULL_SYNC} mode.
     */
    boolean fullSync() {
        assert syncMode != null;

        return syncMode == CacheWriteSynchronizationMode.FULL_SYNC;
    }

    /**
     * @return Task name hash code.
     */
    public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @return Update opreation.
     */
    public GridCacheOperation operation() {
        return op;
    }

    /**
     * @return Subject ID.
     */
    public UUID subjectId() {
        return subjId;
    }

    /**
     * @return Target node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Near node future ID.
     */
    public long futureId() {
        return futId;
    }

    /**
     * @return Write synchronization mode.
     */
    public final CacheWriteSynchronizationMode writeSynchronizationMode() {
        return syncMode;
    }

    /**
     * @param res Response.
     * @return {@code True} if current response was {@code null}.
     */
    public boolean onResponse(GridNearAtomicUpdateResponse res) {
        if (this.res == null) {
            this.res = res;

            return true;
        }

        return false;
    }

    /**
     *
     */
    void resetResponse() {
        this.res = null;
    }

    /**
     * @return Response.
     */
    @Nullable public GridNearAtomicUpdateResponse response() {
        return res;
    }

    /**
     * @return {@code True} if received notification about primary fail.
     */
    boolean nodeFailedResponse() {
        return res != null && res.nodeLeftResponse();
    }

    /**
     * @return Topology locked flag.
     */
    final boolean topologyLocked() {
        return isFlag(TOP_LOCKED_FLAG_MASK);
    }

    /**
     * @param val {@code True} if topology is locked on near node.
     */
    private void topologyLocked(boolean val) {
        setFlag(val, TOP_LOCKED_FLAG_MASK);
    }

    /**
     * @return Return value flag.
     */
    public final boolean returnValue() {
        return isFlag(RET_VAL_FLAG_MASK);
    }

    /**
     * @param val Return value flag.
     */
    public final void returnValue(boolean val) {
        setFlag(val, RET_VAL_FLAG_MASK);
    }

    /**
     * @return Skip write-through to a persistent storage.
     */
    public final boolean skipStore() {
        return isFlag(SKIP_STORE_FLAG_MASK);
    }

    /**
     * @param val Skip store flag.
     */
    public void skipStore(boolean val) {
        setFlag(val, SKIP_STORE_FLAG_MASK);
    }

    /**
     * @return Keep binary flag.
     */
    public final boolean keepBinary() {
        return isFlag(KEEP_BINARY_FLAG_MASK);
    }

    /**
     * @param val Keep binary flag.
     */
    public void keepBinary(boolean val) {
        setFlag(val, KEEP_BINARY_FLAG_MASK);
    }

    /**
     * @return Recovery flag.
     */
    public final boolean recovery() {
        return isFlag(RECOVERY_FLAG_MASK);
    }

    /**
     * @param val Recovery flag.
     */
    public void recovery(boolean val) {
        setFlag(val, RECOVERY_FLAG_MASK);
    }

    /**
     * Sets flag mask.
     *
     * @param flag Set or clear.
     * @param mask Mask.
     */
    private void setFlag(boolean flag, int mask) {
        flags = flag ? (byte)(flags | mask) : (byte)(flags & ~mask);
    }

    /**
     * Reads flag mask.
     *
     * @param mask Mask to read.
     * @return Flag value.
     */
    private boolean isFlag(int mask) {
        return (flags & mask) != 0;
    }

    /**
     * @return Expiry policy.
     */
    public abstract ExpiryPolicy expiry();

    /**
     * @return Filter.
     */
    @Nullable public abstract CacheEntryPredicate[] filter();

    /**
     * @return Optional arguments for entry processor.
     */
    @Nullable public abstract Object[] invokeArguments();

    /**
     * @param key Key to add.
     * @param val Optional update value.
     * @param conflictTtl Conflict TTL (optional).
     * @param conflictExpireTime Conflict expire time (optional).
     * @param conflictVer Conflict version (optional).
     */
    abstract void addUpdateEntry(KeyCacheObject key,
        @Nullable Object val,
        long conflictTtl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer);

    /**
     * @return Keys for this update request.
     */
    public abstract List<KeyCacheObject> keys();

    /**
     * @return Values for this update request.
     */
    public abstract List<?> values();

    /**
     * @param idx Key index.
     * @return Value.
     */
    public abstract CacheObject value(int idx);

    /**
     * @param idx Key index.
     * @return Entry processor.
     */
    public abstract EntryProcessor<Object, Object, Object> entryProcessor(int idx);

    /**
     * @param idx Index to get.
     * @return Write value - either value, or transform closure.
     */
    public abstract CacheObject writeValue(int idx);

    /**
     * @return Conflict versions.
     */
    @Nullable public abstract List<GridCacheVersion> conflictVersions();

    /**
     * @param idx Index.
     * @return Conflict version.
     */
    @Nullable public abstract GridCacheVersion conflictVersion(int idx);

    /**
     * @param idx Index.
     * @return Conflict TTL.
     */
    public abstract long conflictTtl(int idx);

    /**
     * @param idx Index.
     * @return Conflict expire time.
     */
    public abstract long conflictExpireTime(int idx);

    /**
     * Cleanup values.
     *
     * @param clearKeys If {@code true} clears keys.
     */
    public abstract void cleanup(boolean clearKeys);

    /**
     * @return Keys size.
     */
    public abstract int size();

    /**
     * @param idx Key index.
     * @return Key.
     */
    public abstract KeyCacheObject key(int idx);

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 11;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 4:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeLong("futId", futId))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeByte("op", op != null ? (byte)op.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeByte("syncMode", syncMode != null ? (byte)syncMode.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeAffinityTopologyVersion("topVer", topVer))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 4:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                futId = reader.readLong("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                byte opOrd;

                opOrd = reader.readByte("op");

                if (!reader.isLastRead())
                    return false;

                op = GridCacheOperation.fromOrdinal(opOrd);

                reader.incrementState();

            case 7:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                byte syncModeOrd;

                syncModeOrd = reader.readByte("syncMode");

                if (!reader.isLastRead())
                    return false;

                syncMode = CacheWriteSynchronizationMode.fromOrdinal(syncModeOrd);

                reader.incrementState();

            case 9:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                topVer = reader.readAffinityTopologyVersion("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearAtomicAbstractUpdateRequest.class);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder flags = new StringBuilder();

        if (needPrimaryResponse())
            appendFlag(flags, "needRes");
        if (topologyLocked())
            appendFlag(flags, "topLock");
        if (skipStore())
            appendFlag(flags, "skipStore");
        if (keepBinary())
            appendFlag(flags, "keepBinary");
        if (returnValue())
            appendFlag(flags, "retVal");

        return S.toString(GridNearAtomicAbstractUpdateRequest.class, this,
            "flags", flags.toString());
    }
}
