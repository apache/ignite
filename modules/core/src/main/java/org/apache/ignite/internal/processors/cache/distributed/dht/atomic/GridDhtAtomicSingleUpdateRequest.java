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

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.KEEP_BINARY_FLAG_MASK;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.SKIP_STORE_FLAG_MASK;

/**
 *
 */
public class GridDhtAtomicSingleUpdateRequest extends GridDhtAtomicAbstractUpdateRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** Near cache key flag. */
    private static final int NEAR_FLAG_MASK = 0x80;

    /** Future version. */
    protected GridCacheVersion futVer;

    /** Write version. */
    protected GridCacheVersion writeVer;

    /** Write synchronization mode. */
    protected CacheWriteSynchronizationMode syncMode;

    /** Topology version. */
    protected AffinityTopologyVersion topVer;

    /** Subject ID. */
    protected UUID subjId;

    /** Task name hash. */
    protected int taskNameHash;

    /** Additional flags. */
    protected byte flags;

    /** Key to update. */
    @GridToStringInclude
    protected KeyCacheObject key;

    /** Value to update. */
    @GridToStringInclude
    protected CacheObject val;

    /** Previous value. */
    @GridToStringInclude
    protected CacheObject prevVal;

    /** Partition. */
    protected long updateCntr;

    /** */
    protected int partId;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridDhtAtomicSingleUpdateRequest() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cacheId Cache ID.
     * @param nodeId Node ID.
     * @param futVer Future version.
     * @param writeVer Write version for cache values.
     * @param syncMode Cache write synchronization mode.
     * @param topVer Topology version.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param addDepInfo Deployment info.
     * @param keepBinary Keep binary flag.
     * @param skipStore Skip store flag.
     */
    GridDhtAtomicSingleUpdateRequest(
        int cacheId,
        UUID nodeId,
        GridCacheVersion futVer,
        GridCacheVersion writeVer,
        CacheWriteSynchronizationMode syncMode,
        @NotNull AffinityTopologyVersion topVer,
        UUID subjId,
        int taskNameHash,
        boolean addDepInfo,
        boolean keepBinary,
        boolean skipStore
    ) {
        super(cacheId, nodeId);
        this.futVer = futVer;
        this.writeVer = writeVer;
        this.syncMode = syncMode;
        this.topVer = topVer;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.addDepInfo = addDepInfo;

        if (skipStore)
            setFlag(true, SKIP_STORE_FLAG_MASK);
        if (keepBinary)
            setFlag(true, KEEP_BINARY_FLAG_MASK);
    }

    /**
     * @param key Key to add.
     * @param val Value, {@code null} if should be removed.
     * @param entryProcessor Entry processor.
     * @param ttl TTL (optional).
     * @param conflictExpireTime Conflict expire time (optional).
     * @param conflictVer Conflict version (optional).
     * @param addPrevVal If {@code true} adds previous value.
     * @param partId Partition.
     * @param prevVal Previous value.
     * @param updateCntr Update counter.
     */
    @Override public void addWriteValue(KeyCacheObject key,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean addPrevVal,
        int partId,
        @Nullable CacheObject prevVal,
        long updateCntr
    ) {
        assert entryProcessor == null;
        assert ttl <= 0 : ttl;
        assert conflictExpireTime <= 0 : conflictExpireTime;
        assert conflictVer == null : conflictVer;

        near(false);

        this.key = key;
        this.partId = partId;
        this.val = val;

        if (addPrevVal)
            this.prevVal = prevVal;

        this.updateCntr = updateCntr;
    }

    /**
     * @param key Key to add.
     * @param val Value, {@code null} if should be removed.
     * @param entryProcessor Entry processor.
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    @Override public void addNearWriteValue(KeyCacheObject key,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long expireTime) {
        assert entryProcessor == null;
        assert ttl <= 0 : ttl;

        near(true);

        this.key = key;
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public boolean forceTransformBackups() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return key != null ? near() ? 0 : 1 : 0;
    }

    /** {@inheritDoc} */
    @Override public int nearSize() {
        return key != null ? near() ? 1 : 0 : 0;
    }

    /** {@inheritDoc} */
    @Override public boolean hasKey(KeyCacheObject key) {
        return !near() && F.eq(this.key, key);
    }

    /** {@inheritDoc} */
    @Override public boolean skipStore() {
        return isFlag(SKIP_STORE_FLAG_MASK);
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject key(int idx) {
        assert idx == 0 : idx;

        return near() ? null : key;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override public int partitionId(int idx) {
        assert idx == 0 : idx;

        return partId;
    }

    /** {@inheritDoc} */
    @Override public Long updateCounter(int updCntr) {
        assert updCntr == 0 : updCntr;

        return updateCntr;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject nearKey(int idx) {
        assert idx == 0 : idx;

        return near() ? key : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public CacheObject value(int idx) {
        assert idx == 0 : idx;

        return near() ? null : val;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion futureVersion() {
        return futVer;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion writeVersion() {
        return writeVer;
    }

    /** {@inheritDoc} */
    @Override public int taskNameHash() {
        return taskNameHash;
    }

    /** {@inheritDoc} */
    @Override public UUID subjectId() {
        return subjId;
    }

    /** {@inheritDoc} */
    @Override public CacheWriteSynchronizationMode writeSynchronizationMode() {
        return syncMode;
    }

    /** {@inheritDoc} */
    @Override @Nullable public CacheObject previousValue(int idx) {
        assert idx == 0 : idx;

        return prevVal;
    }

    /** {@inheritDoc} */
    @Override @Nullable public CacheObject nearValue(int idx) {
        assert idx == 0 : idx;

        return near() ? val : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public EntryProcessor<Object, Object, Object> entryProcessor(int idx) {
        assert idx == 0 : idx;

        return null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public EntryProcessor<Object, Object, Object> nearEntryProcessor(int idx) {
        assert idx == 0 : idx;

        return null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public GridCacheVersion conflictVersion(int idx) {
        assert idx == 0 : idx;

        return null;
    }

    /** {@inheritDoc} */
    @Override public long ttl(int idx) {
        assert idx == 0 : idx;

        return CU.TTL_NOT_CHANGED;
    }

    /** {@inheritDoc} */
    @Override public long nearTtl(int idx) {
        assert idx == 0 : idx;

        return CU.TTL_NOT_CHANGED;
    }

    /** {@inheritDoc} */
    @Override public long conflictExpireTime(int idx) {
        assert idx == 0 : idx;

        return CU.EXPIRE_TIME_CALCULATE;
    }

    /** {@inheritDoc} */
    @Override public long nearExpireTime(int idx) {
        assert idx == 0 : idx;

        return CU.EXPIRE_TIME_CALCULATE;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Object[] invokeArguments() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean keepBinary() {
        return isFlag(KEEP_BINARY_FLAG_MASK);
    }

    /**
     *
     */
    private boolean near() {
        return isFlag(NEAR_FLAG_MASK);
    }

    /**
     *
     */
    private void near(boolean near) {
        setFlag(near, NEAR_FLAG_MASK);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        prepareMarshalObject(key, cctx);

        prepareMarshalObject(val, cctx);

        prepareMarshalObject(prevVal, cctx);

    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        finishUnmarshalObject(key, cctx, ldr);

        finishUnmarshalObject(val, cctx, ldr);

        finishUnmarshalObject(prevVal, cctx, ldr);

        key.partition(partId);
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
            case 3:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMessage("futVer", futVer))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMessage("key", key))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeInt("partId", partId))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeMessage("prevVal", prevVal))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeByte("syncMode", syncMode != null ? (byte)syncMode.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeMessage("topVer", topVer))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeLong("updateCntr", updateCntr))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeMessage("val", val))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeMessage("writeVer", writeVer))
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
            case 3:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                futVer = reader.readMessage("futVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                key = reader.readMessage("key");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                partId = reader.readInt("partId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                prevVal = reader.readMessage("prevVal");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                byte syncModeOrd;

                syncModeOrd = reader.readByte("syncMode");

                if (!reader.isLastRead())
                    return false;

                syncMode = CacheWriteSynchronizationMode.fromOrdinal(syncModeOrd);

                reader.incrementState();

            case 10:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                updateCntr = reader.readLong("updateCntr");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                val = reader.readMessage("val");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                writeVer = reader.readMessage("writeVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtAtomicSingleUpdateRequest.class);
    }

    /**
     * @param obj CacheObject to marshal
     * @param ctx context
     * @throws IgniteCheckedException if error
     */
    private void prepareMarshalObject(CacheObject obj, GridCacheContext ctx) throws IgniteCheckedException {
        if (obj != null)
            obj.prepareMarshal(ctx.cacheObjectContext());
    }

    /**
     * @param obj CacheObject un to marshal
     * @param ctx context
     * @param ldr class loader
     * @throws IgniteCheckedException if error
     */
    private void finishUnmarshalObject(@Nullable CacheObject obj, GridCacheContext ctx,
        ClassLoader ldr) throws IgniteCheckedException {
        if (obj != null)
            obj.finishUnmarshal(ctx.cacheObjectContext(), ldr);
    }

    /**
     * Cleanup values not needed after message was sent.
     */
    @Override protected void cleanup() {
        val = null;
        prevVal = null;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -36;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 15;
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
     * Reags flag mask.
     *
     * @param mask Mask to read.
     * @return Flag value.
     */
    private boolean isFlag(int mask) {
        return (flags & mask) != 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicSingleUpdateRequest.class, this, "super", super.toString());
    }
}
