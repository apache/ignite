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
import java.util.Collections;
import java.util.List;
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
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;

/**
 *
 */
public class GridNearAtomicSingleUpdateRequest extends GridNearAtomicAbstractSingleUpdateRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** Key to update. */
    @GridToStringInclude
    protected KeyCacheObject key;

    /** Value to update. */
    protected CacheObject val;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridNearAtomicSingleUpdateRequest() {
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
    GridNearAtomicSingleUpdateRequest(
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
        super(cacheId,
            nodeId,
            futId,
            topVer,
            syncMode,
            op,
            subjId,
            taskNameHash,
            flags,
            addDepInfo
        );
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        assert key != null;

        return key.partition();
    }

    /**
     * @param key Key to add.
     * @param val Optional update value.
     * @param conflictTtl Conflict TTL (optional).
     * @param conflictExpireTime Conflict expire time (optional).
     * @param conflictVer Conflict version (optional).
     */
    @Override public void addUpdateEntry(KeyCacheObject key,
        @Nullable Object val,
        long conflictTtl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer) {
        assert op != TRANSFORM;
        assert val != null || op == DELETE;
        assert conflictTtl < 0 : conflictTtl;
        assert conflictExpireTime < 0 : conflictExpireTime;
        assert conflictVer == null : conflictVer;

        this.key = key;

        if (val != null) {
            assert val instanceof CacheObject : val;

            this.val = (CacheObject)val;
        }
    }

    /** {@inheritDoc} */
    @Override public int size() {
        assert key != null;

        return key == null ? 0 : 1;
    }

    /** {@inheritDoc} */
    @Override public List<KeyCacheObject> keys() {
        return Collections.singletonList(key);
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject key(int idx) {
        assert idx == 0 : idx;

        return key;
    }

    /** {@inheritDoc} */
    @Override public List<?> values() {
        return Collections.singletonList(val);
    }

    /** {@inheritDoc} */
    @Override public CacheObject value(int idx) {
        assert idx == 0 : idx;

        return val;
    }

    /** {@inheritDoc} */
    @Override public EntryProcessor<Object, Object, Object> entryProcessor(int idx) {
        assert idx == 0 : idx;

        return null;
    }

    /** {@inheritDoc} */
    @Override public CacheObject writeValue(int idx) {
        assert idx == 0 : idx;

        return val;
    }

    /** {@inheritDoc} */
    @Nullable @Override public List<GridCacheVersion> conflictVersions() {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheVersion conflictVersion(int idx) {
        assert idx == 0 : idx;

        return null;
    }

    /** {@inheritDoc} */
    @Override public long conflictTtl(int idx) {
        assert idx == 0 : idx;

        return CU.TTL_NOT_CHANGED;
    }

    /** {@inheritDoc} */
    @Override public long conflictExpireTime(int idx) {
        assert idx == 0 : idx;

        return CU.EXPIRE_TIME_CALCULATE;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        prepareMarshalCacheObject(key, cctx);

        if (val != null)
            prepareMarshalCacheObject(val, cctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        key.finishUnmarshal(cctx.cacheObjectContext(), ldr);

        if (val != null)
            val.finishUnmarshal(cctx.cacheObjectContext(), ldr);
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
            case 11:
                if (!writer.writeMessage("key", key))
                    return false;

                writer.incrementState();

            case 12:
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
            case 11:
                key = reader.readMessage("key");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                val = reader.readMessage("val");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearAtomicSingleUpdateRequest.class);
    }

    /** {@inheritDoc} */
    @Override public void cleanup(boolean clearKey) {
        val = null;

        if (clearKey)
            key = null;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 125;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 13;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicSingleUpdateRequest.class, this, "parent", super.toString());
    }
}
