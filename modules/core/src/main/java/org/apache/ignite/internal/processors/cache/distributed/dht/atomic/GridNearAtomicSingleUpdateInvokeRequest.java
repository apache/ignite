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
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;

/**
 *
 */
public class GridNearAtomicSingleUpdateInvokeRequest extends GridNearAtomicSingleUpdateRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** Optional arguments for entry processor. */
    @GridDirectTransient
    private Object[] invokeArgs;

    /** Entry processor arguments bytes. */
    private byte[][] invokeArgsBytes;

    /** Entry processors. */
    @GridDirectTransient
    private EntryProcessor<Object, Object, Object> entryProcessor;

    /** Entry processors bytes. */
    private byte[] entryProcessorBytes;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridNearAtomicSingleUpdateInvokeRequest() {
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
     * @param invokeArgs Optional arguments for entry processor.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param skipStore Skip write-through to a persistent storage.
     * @param keepBinary Keep binary flag.
     * @param clientReq Client node request flag.
     * @param addDepInfo Deployment info flag.
     */
    GridNearAtomicSingleUpdateInvokeRequest(
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
        @Nullable Object[] invokeArgs,
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
            subjId,
            taskNameHash,
            skipStore,
            keepBinary,
            clientReq,
            addDepInfo
        );
        this.invokeArgs = invokeArgs;

        assert op == TRANSFORM : op;
    }

    /**
     * @param key Key to add.
     * @param val Optional update value.
     * @param conflictTtl Conflict TTL (optional).
     * @param conflictExpireTime Conflict expire time (optional).
     * @param conflictVer Conflict version (optional).
     * @param primary If given key is primary on this mapping.
     */
    @Override public void addUpdateEntry(KeyCacheObject key,
        @Nullable Object val,
        long conflictTtl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean primary) {
        assert conflictTtl < 0 : conflictTtl;
        assert conflictExpireTime < 0 : conflictExpireTime;
        assert conflictVer == null : conflictVer;
        assert val instanceof EntryProcessor : val;

        entryProcessor = (EntryProcessor<Object, Object, Object>)val;

        this.key = key;
        partId = key.partition();

        hasPrimary(hasPrimary() | primary);
    }

    /** {@inheritDoc} */
    @Override public List<?> values() {
        return Collections.singletonList(entryProcessor);
    }

    /** {@inheritDoc} */
    @Override public CacheObject value(int idx) {
        assert idx == 0 : idx;

        return null;
    }

    /** {@inheritDoc} */
    @Override public EntryProcessor<Object, Object, Object> entryProcessor(int idx) {
        assert idx == 0 : idx;

        return entryProcessor;
    }

    /** {@inheritDoc} */
    @Override public CacheObject writeValue(int idx) {
        assert idx == 0 : idx;

        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object[] invokeArguments() {
        return invokeArgs;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        // force addition of deployment info for entry processors if P2P is enabled globally.
        if (!addDepInfo && ctx.deploymentEnabled())
            addDepInfo = true;

        if (entryProcessor != null && entryProcessorBytes == null) {
            if (addDepInfo)
                prepareObject(entryProcessor, cctx);

            entryProcessorBytes = CU.marshal(cctx, entryProcessor);
        }

        if (invokeArgsBytes == null)
            invokeArgsBytes = marshalInvokeArguments(invokeArgs, cctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (entryProcessorBytes != null && entryProcessor == null)
            entryProcessor = U.unmarshal(ctx, entryProcessorBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));

        if (invokeArgs == null)
            invokeArgs = unmarshalInvokeArguments(invokeArgsBytes, ctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public void cleanup(boolean clearKey) {
        super.cleanup(clearKey);

        entryProcessor = null;
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
            case 14:
                if (!writer.writeByteArray("entryProcessorBytes", entryProcessorBytes))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeObjectArray("invokeArgsBytes", invokeArgsBytes, MessageCollectionItemType.BYTE_ARR))
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
            case 14:
                entryProcessorBytes = reader.readByteArray("entryProcessorBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                invokeArgsBytes = reader.readObjectArray("invokeArgsBytes", MessageCollectionItemType.BYTE_ARR, byte[].class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearAtomicSingleUpdateInvokeRequest.class);
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 16;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 126;
    }
}
