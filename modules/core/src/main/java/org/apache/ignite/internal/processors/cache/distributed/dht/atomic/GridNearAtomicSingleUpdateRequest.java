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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.IgniteExternalizableExpiryPolicy;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;

/**
 *
 */
public class GridNearAtomicSingleUpdateRequest extends GridNearAtomicAbstractUpdateRequest {
    /** Key to update. */
    @GridToStringInclude
    private KeyCacheObject key;

    /** Value to update. */
    private CacheObject val;

    /** Partition of key. */
    private int partId;

    /** Entry processors. */
    @GridDirectTransient
    private EntryProcessor<Object, Object, Object> entryProcessor;

    /** Entry processors bytes. */
    private byte[] entryProcessorBytes;

    /** Conflict version. */
    private GridCacheVersion conflictVer;

    /** Conflict TTL. */
    private long conflictTtl = CU.TTL_NOT_CHANGED;

    /** Conflict expire time. */
    private long conflictExpireTime = CU.EXPIRE_TIME_CALCULATE;

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
     * @param futVer Future version.
     * @param fastMap Fast map scheme flag.
     * @param updateVer Update version set if fast map is performed.
     * @param topVer Topology version.
     * @param topLocked Topology locked flag.
     * @param syncMode Synchronization mode.
     * @param op Cache update operation.
     * @param retval Return value required flag.
     * @param expiryPlc Expiry policy.
     * @param invokeArgs Optional arguments for entry processor.
     * @param filter Optional filter for atomic check.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param skipStore Skip write-through to a persistent storage.
     * @param keepBinary Keep binary flag.
     * @param clientReq Client node request flag.
     * @param addDepInfo Deployment info flag.
     */
    public GridNearAtomicSingleUpdateRequest(
        int cacheId,
        UUID nodeId,
        GridCacheVersion futVer,
        boolean fastMap,
        @Nullable GridCacheVersion updateVer,
        @NotNull AffinityTopologyVersion topVer,
        boolean topLocked,
        CacheWriteSynchronizationMode syncMode,
        GridCacheOperation op,
        boolean retval,
        @Nullable ExpiryPolicy expiryPlc,
        @Nullable Object[] invokeArgs,
        @Nullable CacheEntryPredicate[] filter,
        @Nullable UUID subjId,
        int taskNameHash,
        boolean skipStore,
        boolean keepBinary,
        boolean clientReq,
        boolean addDepInfo
    ) {
        super(
                cacheId,
                nodeId,
                futVer,
                fastMap,
                updateVer,
                topVer,
                topLocked,
                syncMode,
                op,
                retval,
                expiryPlc,
                invokeArgs,
                filter,
                subjId,
                taskNameHash,
                skipStore,
                keepBinary,
                clientReq,
                addDepInfo
        );

    }

    /**
     * @param key Key to add.
     * @param val Optional update value.
     * @param conflictTtl Conflict TTL (optional).
     * @param conflictExpireTime Conflict expire time (optional).
     * @param conflictVer Conflict version (optional).
     * @param primary If given key is primary on this mapping.
     */
    public void addUpdateEntry(KeyCacheObject key,
       @Nullable Object val,
       long conflictTtl,
       long conflictExpireTime,
       @Nullable GridCacheVersion conflictVer,
       boolean primary) {
        EntryProcessor<Object, Object, Object> entryProcessor = null;

        if (op == TRANSFORM) {
            assert val instanceof EntryProcessor : val;

            entryProcessor = (EntryProcessor<Object, Object, Object>)val;
        }

        assert val != null || op == DELETE;

        this.key = key;
        partId = key.partition();

        if (entryProcessor != null)
            this.entryProcessor = entryProcessor;
        else if (val != null) {
            assert val instanceof CacheObject : val;

            this.val = (CacheObject)val;
        }

        hasPrimary |= primary;

        this.conflictVer = conflictVer;

        if (conflictTtl >= 0)
            this.conflictTtl = conflictTtl;

        if (conflictExpireTime >= 0)
            this.conflictExpireTime = conflictExpireTime;
    }

    /** {@inheritDoc} */
    @Override public List<KeyCacheObject> keys() {
        return Collections.singletonList(key);
    }

    /** {@inheritDoc} */
    @Override public List<?> values() {
        return Collections.singletonList(op == TRANSFORM ? entryProcessor : val);
    }

    /** {@inheritDoc} */
    @Override public CacheObject value(int idx) {
        assert idx == 0 : idx;

        return val;
    }

    /** {@inheritDoc} */
    @Override public EntryProcessor<Object, Object, Object> entryProcessor(int idx) {
        assert idx == 0 : idx;

        return entryProcessor;
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

        return conflictVer;
    }

    /** {@inheritDoc} */
    @Override public long conflictTtl(int idx) {
        assert idx == 0 : idx;

        return conflictTtl;
    }

    /** {@inheritDoc} */
    @Override public long conflictExpireTime(int idx) {
        assert idx == 0 : idx;

        return conflictExpireTime;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        prepareMarshalCacheObject(key, cctx);

        if (filter != null) {
            boolean hasFilter = false;

            for (CacheEntryPredicate p : filter) {
                if (p != null) {
                    hasFilter = true;

                    p.prepareMarshal(cctx);
                }
            }

            if (!hasFilter)
                filter = null;
        }

        if (expiryPlc != null && expiryPlcBytes == null)
            expiryPlcBytes = CU.marshal(cctx, new IgniteExternalizableExpiryPolicy(expiryPlc));

        if (op == TRANSFORM) {
            // force addition of deployment info for entry processors if P2P is enabled globally.
            if (!addDepInfo && ctx.deploymentEnabled())
                addDepInfo = true;

            if (entryProcessor != null && entryProcessorBytes == null)
                entryProcessorBytes = CU.marshal(cctx, entryProcessor);

            if (invokeArgsBytes == null)
                invokeArgsBytes = marshalInvokeArguments(invokeArgs, cctx);
        }
        else
            prepareMarshalCacheObject(val, cctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        key.finishUnmarshal(cctx.cacheObjectContext(), ldr);

        if (op == TRANSFORM) {
            if (entryProcessorBytes != null && entryProcessor == null)
                entryProcessor = ctx.marshaller().unmarshal(
                    entryProcessorBytes,
                    U.resolveClassLoader(ldr, ctx.gridConfig())
                );

            if (invokeArgs == null)
                invokeArgs = unmarshalInvokeArguments(invokeArgsBytes, ctx, ldr);
        }
        else
            if (val != null)
                val.finishUnmarshal(cctx.cacheObjectContext(), ldr);

        if (filter != null) {
            for (CacheEntryPredicate p : filter) {
                if (p != null)
                    p.finishUnmarshal(cctx, ldr);
            }
        }

        if (expiryPlcBytes != null && expiryPlc == null)
            expiryPlc = ctx.marshaller().unmarshal(expiryPlcBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));

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
            case 20:
                if (!writer.writeLong("conflictExpireTime", conflictExpireTime))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeLong("conflictTtl", conflictTtl))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeMessage("conflictVer", conflictVer))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeByteArray("entryProcessorBytes", entryProcessorBytes))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeMessage("key", key))
                    return false;

                writer.incrementState();

            case 25:
                if (!writer.writeInt("partId", partId))
                    return false;

                writer.incrementState();

            case 26:
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
            case 20:
                conflictExpireTime = reader.readLong("conflictExpireTime");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                conflictTtl = reader.readLong("conflictTtl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                conflictVer = reader.readMessage("conflictVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                entryProcessorBytes = reader.readByteArray("entryProcessorBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                key = reader.readMessage("key");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 25:
                partId = reader.readInt("partId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 26:
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
        entryProcessor = null;
        invokeArgs = null;
        invokeArgsBytes = null;

        if (clearKey)
            key = null;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 125;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 27;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicSingleUpdateRequest.class, this, "filter", Arrays.toString(filter),
                "parent", super.toString());
    }
}
