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
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.IgniteExternalizableExpiryPolicy;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;

/**
 * Lite DHT cache update request sent from near node to primary node.
 */
public class GridNearAtomicFullUpdateRequest extends GridNearAtomicAbstractUpdateRequest {
    /** Keys to update. */
    @Order(10)
    @GridToStringInclude
    List<KeyCacheObject> keys;

    /** Values to update. */
    @Order(11)
    List<CacheObject> vals;

    /** Entry processors. */
    private List<EntryProcessor<Object, Object, Object>> entryProcessors;

    /** Entry processors bytes. */
    @Order(12)
    @Nullable List<byte[]> entryProcessorsBytes;

    /** Conflict versions. */
    @Order(13)
    @Nullable List<GridCacheVersion> conflictVers;

    /** Conflict TTLs. */
    @Order(14)
    GridLongList conflictTtls;

    /** Conflict expire times. */
    @Order(15)
    GridLongList conflictExpireTimes;

    /** Optional arguments for entry processor. */
    private @Nullable Object[] invokeArgs;

    /** Entry processor arguments bytes. */
    @Order(16)
    @Nullable List<byte[]> invokeArgsBytes;

    /** Expiry policy. */
    private @Nullable ExpiryPolicy expiryPlc;

    /** Expiry policy bytes. */
    @Order(17)
    @Nullable byte[] expiryPlcBytes;

    /** Filter. */
    @Order(18)
    @Nullable CacheEntryPredicate[] filter;

    /** Maximum possible size of inner collections. */
    private int initSize;

    /**
     * Empty constructor.
     */
    public GridNearAtomicFullUpdateRequest() {
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
     * @param expiryPlc Expiry policy.
     * @param invokeArgs Optional arguments for entry processor.
     * @param filter Optional filter for atomic check.
     * @param taskNameHash Task name hash code.
     * @param flags Flags.
     * @param addDepInfo Deployment info flag.
     * @param maxEntryCnt Maximum entries count.
     */
    GridNearAtomicFullUpdateRequest(
        int cacheId,
        UUID nodeId,
        long futId,
        @NotNull AffinityTopologyVersion topVer,
        CacheWriteSynchronizationMode syncMode,
        GridCacheOperation op,
        @Nullable ExpiryPolicy expiryPlc,
        @Nullable Object[] invokeArgs,
        @Nullable CacheEntryPredicate[] filter,
        int taskNameHash,
        short flags,
        boolean addDepInfo,
        int maxEntryCnt
    ) {
        super(cacheId,
            nodeId,
            futId,
            topVer,
            syncMode,
            op,
            taskNameHash,
            flags,
            addDepInfo);

        this.expiryPlc = expiryPlc;
        this.invokeArgs = invokeArgs;
        this.filter = filter;

        // By default, ArrayList expands to array of 10 elements on first add. We cannot guess how many entries
        // will be added to request because of unknown affinity distribution. However, we DO KNOW how many keys
        // participate in request. As such, we know upper bound of all collections in request. If this bound is lower
        // than 10, we use it.
        initSize = Math.min(maxEntryCnt, 10);

        keys = new ArrayList<>(initSize);
    }

    /** {@inheritDoc} */
    @Override public void addUpdateEntry(KeyCacheObject key,
        @Nullable Object val,
        long conflictTtl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer) {
        EntryProcessor<Object, Object, Object> entryProc = null;

        if (operation() == TRANSFORM) {
            assert val instanceof EntryProcessor : val;

            entryProc = (EntryProcessor<Object, Object, Object>)val;
        }

        assert val != null || operation() == DELETE;

        keys.add(key);

        if (entryProc != null) {
            if (entryProcessors == null)
                entryProcessors = new ArrayList<>(initSize);

            entryProcessors.add(entryProc);
        }
        else if (val != null) {
            assert val instanceof CacheObject : val;

            if (vals == null)
                vals = new ArrayList<>(initSize);

            vals.add((CacheObject)val);
        }

        // In case there is no conflict, do not create the list.
        if (conflictVer != null) {
            if (conflictVers == null) {
                conflictVers = new ArrayList<>(initSize);

                for (int i = 0; i < keys.size() - 1; i++)
                    conflictVers.add(null);
            }

            conflictVers.add(conflictVer);
        }
        else if (conflictVers != null)
            conflictVers.add(null);

        if (conflictTtl >= 0) {
            if (conflictTtls == null) {
                conflictTtls = new GridLongList(keys.size());

                for (int i = 0; i < keys.size() - 1; i++)
                    conflictTtls.add(CU.TTL_NOT_CHANGED);
            }

            conflictTtls.add(conflictTtl);
        }

        if (conflictExpireTime >= 0) {
            if (conflictExpireTimes == null) {
                conflictExpireTimes = new GridLongList(keys.size());

                for (int i = 0; i < keys.size() - 1; i++)
                    conflictExpireTimes.add(CU.EXPIRE_TIME_CALCULATE);
            }

            conflictExpireTimes.add(conflictExpireTime);
        }
    }

    /** {@inheritDoc} */
    @Override public List<KeyCacheObject> keys() {
        return keys;
    }

    /**
     * @param keys Keys to update.
     */
    public void keys(List<KeyCacheObject> keys) {
        this.keys = keys;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        assert keys != null;

        return keys != null ? keys.size() : 0;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject key(int idx) {
        return keys.get(idx);
    }

    /** {@inheritDoc} */
    @Override public List<?> values() {
        return operation() == TRANSFORM ? entryProcessors : vals;
    }

    /** {@inheritDoc} */
    @Override public CacheObject value(int idx) {
        assert operation() == UPDATE : operation();

        return vals.get(idx);
    }

    /** {@inheritDoc} */
    @Override public EntryProcessor<Object, Object, Object> entryProcessor(int idx) {
        assert operation() == TRANSFORM : operation();

        return entryProcessors.get(idx);
    }

    /** {@inheritDoc} */
    @Override public CacheObject writeValue(int idx) {
        if (vals != null)
            return vals.get(idx);

        return null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public List<GridCacheVersion> conflictVersions() {
        return conflictVers;
    }

    /**
     * @param conflictVers Conflict versions.
     */
    public void conflictVersions(@Nullable List<GridCacheVersion> conflictVers) {
        this.conflictVers = conflictVers;
    }

    /** {@inheritDoc} */
    @Override @Nullable public GridCacheVersion conflictVersion(int idx) {
        if (conflictVers != null) {
            assert idx >= 0 && idx < conflictVers.size();

            return conflictVers.get(idx);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public long conflictTtl(int idx) {
        if (conflictTtls != null) {
            assert idx >= 0 && idx < conflictTtls.size();

            return conflictTtls.get(idx);
        }

        return CU.TTL_NOT_CHANGED;
    }

    /** {@inheritDoc} */
    @Override public long conflictExpireTime(int idx) {
        if (conflictExpireTimes != null) {
            assert idx >= 0 && idx < conflictExpireTimes.size();

            return conflictExpireTimes.get(idx);
        }

        return CU.EXPIRE_TIME_CALCULATE;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Object[] invokeArguments() {
        return invokeArgs;
    }

    /** {@inheritDoc} */
    @Override @Nullable public CacheEntryPredicate[] filter() {
        return filter;
    }

    /**
     * @param filter Filter.
     */
    public void filter(@Nullable CacheEntryPredicate[] filter) {
        this.filter = filter;
    }

    /** {@inheritDoc} */
    @Override public ExpiryPolicy expiry() {
        return expiryPlc;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId);

        if (expiryPlc != null && expiryPlcBytes == null)
            expiryPlcBytes = CU.marshal(cctx, new IgniteExternalizableExpiryPolicy(expiryPlc));

        prepareMarshalCacheObjects(keys, cctx);

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

        if (operation() == TRANSFORM) {
            // force addition of deployment info for entry processors if P2P is enabled globally.
            if (!addDepInfo && ctx.deploymentEnabled())
                addDepInfo = true;

            if (entryProcessorsBytes == null)
                entryProcessorsBytes = marshalCollection(entryProcessors, cctx);

            if (!F.isEmpty(invokeArgs) && invokeArgsBytes == null)
                invokeArgsBytes = Arrays.asList(marshalInvokeArguments(invokeArgs, cctx));
        }
        else
            prepareMarshalCacheObjects(vals, cctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId);

        if (expiryPlcBytes != null && expiryPlc == null)
            expiryPlc = U.unmarshal(ctx, expiryPlcBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));

        finishUnmarshalCacheObjects(keys, cctx, ldr);

        if (filter != null) {
            for (CacheEntryPredicate p : filter) {
                if (p != null)
                    p.finishUnmarshal(cctx, ldr);
            }
        }

        if (operation() == TRANSFORM) {
            if (entryProcessors == null)
                entryProcessors = unmarshalCollection(entryProcessorsBytes, ctx, ldr);

            if (invokeArgsBytes != null && invokeArgs == null)
                invokeArgs = unmarshalInvokeArguments(invokeArgsBytes.toArray(new byte[invokeArgsBytes.size()][]), ctx, ldr);
        }
        else
            finishUnmarshalCacheObjects(vals, cctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        assert !F.isEmpty(keys);

        return keys.get(0).partition();
    }

    /**
     * @return Values to update.
     */
    public List<CacheObject> valuesToUpdate() {
        return vals;
    }

    /**
     * @param vals Values to update.
     */
    public void valuesToUpdate(List<CacheObject> vals) {
        this.vals = vals;
    }

    /**
     * @return Entry processors bytes.
     */
    public @Nullable List<byte[]> entryProcessorsBytes() {
        return entryProcessorsBytes;
    }

    /**
     * @param entryProcessorsBytes Entry processors bytes.
     */
    public void entryProcessorsBytes(@Nullable List<byte[]> entryProcessorsBytes) {
        this.entryProcessorsBytes = entryProcessorsBytes;
    }

    /**
     * @return Conflict TTLs.
     */
    public GridLongList conflictTtls() {
        return conflictTtls;
    }

    /**
     * @param conflictTtls Conflict TTLs.
     */
    public void conflictTtls(GridLongList conflictTtls) {
        this.conflictTtls = conflictTtls;
    }

    /**
     * @return Conflict expire times.
     */
    public GridLongList conflictExpireTimes() {
        return conflictExpireTimes;
    }

    /**
     * @param conflictExpireTimes Conflict expire times.
     */
    public void conflictExpireTimes(GridLongList conflictExpireTimes) {
        this.conflictExpireTimes = conflictExpireTimes;
    }

    /**
     * @return Entry processor arguments bytes.
     */
    public @Nullable List<byte[]> invokeArgumentsBytes() {
        return invokeArgsBytes;
    }

    /**
     * @param invokeArgsBytes Entry processor arguments bytes.
     */
    public void invokeArgumentsBytes(@Nullable List<byte[]> invokeArgsBytes) {
        this.invokeArgsBytes = invokeArgsBytes;
    }

    /**
     * @return Expiry policy bytes.
     */
    public @Nullable byte[] expiryPolicyBytes() {
        return expiryPlcBytes;
    }

    /**
     * @param expiryPlcBytes Expiry policy bytes.
     */
    public void expiryPolicyBytes(@Nullable byte[] expiryPlcBytes) {
        this.expiryPlcBytes = expiryPlcBytes;
    }

    /** {@inheritDoc} */
    @Override public void cleanup(boolean clearKeys) {
        vals = null;
        entryProcessors = null;
        entryProcessorsBytes = null;
        invokeArgs = null;
        invokeArgsBytes = null;

        if (clearKeys)
            keys = null;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 40;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicFullUpdateRequest.class, this,
            "filter", Arrays.toString(filter),
            "parent", super.toString());
    }
}
