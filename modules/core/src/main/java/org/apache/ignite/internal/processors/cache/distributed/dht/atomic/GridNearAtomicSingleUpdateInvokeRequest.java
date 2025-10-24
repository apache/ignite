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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.marshaller.Marshallers.jdk;

/**
 *
 */
public final class GridNearAtomicSingleUpdateInvokeRequest extends GridNearAtomicSingleUpdateRequest {
    /** Optional arguments for entry processor. */
    private @Nullable Object[] invokeArgs;

    /** Number of entry processor arguments. */
    @Order(value = 12, method = "invokeArgumentsCount")
    private int invokeArgsCnt;

    /** Entry processor arguments bytes. */
    @Order(value = 13, method = "invokeArgumentsBytes")
    private @Nullable List<byte[]> invokeArgsBytes;

    /** Entry processor. */
    private EntryProcessor<Object, Object, Object> entryProc;

    /** Entry processors bytes. */
    @Order(value = 14, method = "entryProcessorBytes")
    private byte[] entryProcBytes;

    /**
     * Empty constructor.
     */
    public GridNearAtomicSingleUpdateInvokeRequest() {
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
     * @param invokeArgs Optional arguments for entry processor.
     * @param taskNameHash Task name hash code.
     * @param flags Flags.
     * @param addDepInfo Deployment info flag.
     */
    GridNearAtomicSingleUpdateInvokeRequest(
        int cacheId,
        UUID nodeId,
        long futId,
        @NotNull AffinityTopologyVersion topVer,
        CacheWriteSynchronizationMode syncMode,
        GridCacheOperation op,
        @Nullable Object[] invokeArgs,
        int taskNameHash,
        byte flags,
        boolean addDepInfo
    ) {
        super(
            cacheId,
            nodeId,
            futId,
            topVer,
            syncMode,
            op,
            taskNameHash,
            flags,
            addDepInfo
        );

        assert op == TRANSFORM : op;

        this.invokeArgs = invokeArgs;

        if (!F.isEmpty(invokeArgs))
            invokeArgsCnt = invokeArgs.length;
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
        assert conflictTtl < 0 : conflictTtl;
        assert conflictExpireTime < 0 : conflictExpireTime;
        assert conflictVer == null : conflictVer;
        assert val instanceof EntryProcessor : val;

        entryProc = (EntryProcessor<Object, Object, Object>)val;

        this.key = key;
    }

    /** {@inheritDoc} */
    @Override public List<?> values() {
        return Collections.singletonList(entryProc);
    }

    /** {@inheritDoc} */
    @Override public CacheObject value(int idx) {
        assert idx == 0 : idx;

        return null;
    }

    /** {@inheritDoc} */
    @Override public EntryProcessor<Object, Object, Object> entryProcessor(int idx) {
        assert idx == 0 : idx;

        return entryProc;
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

    /** @return Number of entry processor arguments. */
    public int invokeArgumentsCount() {
        return invokeArgsCnt;
    }

    /** @param invokeArgsCnt Number of entry processor arguments. */
    public void invokeArgumentsCount(int invokeArgsCnt) {
        this.invokeArgsCnt = invokeArgsCnt;
    }

    /** @return Entry processors arguments bytes. */
    public @Nullable List<byte[]> invokeArgumentsBytes() {
        return invokeArgsBytes;
    }

    /** @param invokeArgsBytes Entry processors arguments bytes. */
    public void invokeArgumentsBytes(@Nullable List<byte[]> invokeArgsBytes) {
        this.invokeArgsBytes = invokeArgsBytes;
    }

    /** @return Entry processors bytes. */
    public byte[] entryProcessorBytes() {
        return entryProcBytes;
    }

    /** @param entryProcBytes Entry processors bytes. */
    public void entryProcessorBytes(byte[] entryProcBytes) {
        this.entryProcBytes = entryProcBytes;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId);

        // force addition of deployment info for entry processors if P2P is enabled globally.
        if (!addDepInfo && ctx.deploymentEnabled())
            addDepInfo = true;

        if (entryProc != null) {
            if (addDepInfo)
                prepareObject(entryProc, cctx);

            entryProcBytes = U.marshal(jdk(), entryProc);
        }
        else
            entryProcBytes = null;

        if (!F.isEmpty(invokeArgs)) {
            assert invokeArgsCnt == invokeArgs.length;

            invokeArgsBytes = new ArrayList<>(invokeArgsCnt);

            for (int i = 0; i < invokeArgsCnt; ++i)
                invokeArgsBytes.add(U.marshal(jdk(), invokeArgs[i]));
        }
        else
            invokeArgsBytes = null;
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (entryProcBytes != null) {
            entryProc = U.unmarshal(jdk(), entryProcBytes, U.gridClassLoader());

            entryProcBytes = null;
        }
        else
            entryProc = null;

        if (invokeArgsCnt == 0) {
            assert invokeArgsBytes == null;

            invokeArgs = null;
        }
        else {
            assert invokeArgsBytes != null && invokeArgsBytes.size() == invokeArgsCnt;

            invokeArgs = new Object[invokeArgsCnt];

            for (int i = 0; i < invokeArgsCnt; ++i)
                invokeArgs[i] = U.unmarshal(jdk(), invokeArgsBytes.get(i), U.gridClassLoader());

            invokeArgsBytes = null;
        }
    }

    /** {@inheritDoc} */
    @Override public void cleanup(boolean clearKey) {
        super.cleanup(clearKey);

        entryProc = null;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 126;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicSingleUpdateRequest.class, this, super.toString());
    }
}
