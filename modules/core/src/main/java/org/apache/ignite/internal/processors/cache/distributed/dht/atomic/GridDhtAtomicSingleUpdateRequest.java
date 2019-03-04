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
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
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

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;

/**
 *
 */
public class GridDhtAtomicSingleUpdateRequest extends GridDhtAtomicAbstractUpdateRequest {
    /** */
    private static final long serialVersionUID = 0L;

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
     * @param futId Future ID.
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
        long futId,
        GridCacheVersion writeVer,
        CacheWriteSynchronizationMode syncMode,
        @NotNull AffinityTopologyVersion topVer,
        UUID subjId,
        int taskNameHash,
        boolean addDepInfo,
        boolean keepBinary,
        boolean skipStore
    ) {
        super(cacheId,
            nodeId,
            futId,
            writeVer,
            syncMode,
            topVer,
            subjId,
            taskNameHash,
            addDepInfo,
            keepBinary,
            skipStore);
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
    @Override public void addWriteValue(KeyCacheObject key,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean addPrevVal,
        @Nullable CacheObject prevVal,
        long updateCntr,
        GridCacheOperation cacheOp) {
        assert entryProcessor == null;
        assert ttl <= 0 : ttl;
        assert conflictExpireTime <= 0 : conflictExpireTime;
        assert conflictVer == null : conflictVer;
        assert key.partition() >= 0 : key;

        assert this.key == null;

        this.key = key;
        this.val = val;

        if (addPrevVal)
            this.prevVal = prevVal;

        this.updateCntr = updateCntr;

        if (cacheOp == TRANSFORM)
            setFlag(true, DHT_ATOMIC_TRANSFORM_OP_FLAG_MASK);
    }

    /**
     * @return {@code True} if near cache update request.
     */
    private boolean near() {
        return isFlag(DHT_ATOMIC_NEAR_FLAG_MASK);
    }

    /**
     * @param near Near cache update flag.
     */
    private void near(boolean near) {
        setFlag(near, DHT_ATOMIC_NEAR_FLAG_MASK);
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
        assert key.partition() >= 0 : key;

        if (this.key != null) {
            setFlag(true, DHT_ATOMIC_OBSOLETE_NEAR_KEY_FLAG_MASK);

            return;
        }

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
    @Override public KeyCacheObject key(int idx) {
        assert idx == 0 : idx;

        return near() ? null : key;
    }

    /** {@inheritDoc} */
    @Override public int obsoleteNearKeysSize() {
        return isFlag(DHT_ATOMIC_OBSOLETE_NEAR_KEY_FLAG_MASK) ? 1 : 0;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject obsoleteNearKey(int idx) {
        assert obsoleteNearKeysSize() == 1 && idx == 0 : idx;

        return key;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        int p = key.partition();

        assert p >= 0;

        return p;
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
            case 13:
                if (!writer.writeMessage("key", key))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeMessage("prevVal", prevVal))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeLong("updateCntr", updateCntr))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeMessage("val", val))
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
            case 13:
                key = reader.readMessage("key");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 14:
                prevVal = reader.readMessage("prevVal");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                updateCntr = reader.readLong("updateCntr");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                val = reader.readMessage("val");

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
    @Override public short directType() {
        return -36;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 17;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicSingleUpdateRequest.class, this, "super", super.toString());
    }
}
