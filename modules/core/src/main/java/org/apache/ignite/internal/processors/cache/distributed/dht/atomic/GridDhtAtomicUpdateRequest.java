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
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Lite dht cache backup update request.
 */
public class GridDhtAtomicUpdateRequest extends GridDhtAtomicAbstractUpdateRequest {
    /** Keys to update. */
    @GridToStringInclude
    @Order(11)
    List<KeyCacheObject> keys;

    /** Values to update. */
    @GridToStringInclude
    @Order(12)
    List<CacheObject> vals;

    /** Previous values. */
    @GridToStringInclude
    @Order(13)
    List<CacheObject> prevVals;

    /** Conflict versions. */
    @Order(14)
    List<GridCacheVersion> conflictVers;

    /** TTLs. */
    @Order(15)
    GridLongList ttls;

    /** Conflict expire time. */
    @Order(16)
    GridLongList conflictExpireTimes;

    /** Near TTLs. */
    @Order(17)
    GridLongList nearTtls;

    /** Near expire times. */
    @Order(18)
    GridLongList nearExpireTimes;

    /** Near cache keys to update. */
    @GridToStringInclude
    @Order(19)
    List<KeyCacheObject> nearKeys;

    /** Values to update. */
    @GridToStringInclude
    @Order(20)
    List<CacheObject> nearVals;

    /** Obsolete near values. */
    @GridToStringInclude
    @Order(21)
    List<Integer> obsoleteIndexes;

    /** Force transform backups flag. */
    @Order(22)
    boolean forceTransformBackups;

    /** Entry processors. */
    private List<EntryProcessor<Object, Object, Object>> entryProcessors;

    /** Entry processors bytes. */
    @Order(23)
    List<byte[]> entryProcessorsBytes;

    /** Near entry processors. */
    private List<EntryProcessor<Object, Object, Object>> nearEntryProcessors;

    /** Near entry processors bytes. */
    @Order(24)
    List<byte[]> nearEntryProcessorsBytes;

    /** Optional arguments for entry processor. */
    private Object[] invokeArgs;

    /** Entry processor arguments bytes. */
    @Order(25)
    List<byte[]> invokeArgsBytes;

    /** Partition. */
    @Order(26)
    GridLongList updateCntrs;

    /**
     * Empty constructor.
     */
    public GridDhtAtomicUpdateRequest() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cacheId Cache ID.
     * @param nodeId Node ID.
     * @param futId Future ID.
     * @param writeVer Write version for cache values.
     * @param invokeArgs Optional arguments for entry processor.
     * @param topVer Topology version.
     * @param keepBinary Keep binary flag.
     * @param skipStore Skip store flag.
     * @param forceTransformBackups Force transform backups flag.
     * @param taskNameHash Task name hash code.
     * @param addDepInfo Deployment info.
     * @param readRepairRecovery Recovery on Read Repair flag.
     */
    public GridDhtAtomicUpdateRequest(
        int cacheId,
        UUID nodeId,
        long futId,
        GridCacheVersion writeVer,
        @NotNull AffinityTopologyVersion topVer,
        int taskNameHash,
        Object[] invokeArgs,
        boolean addDepInfo,
        boolean keepBinary,
        boolean skipStore,
        boolean forceTransformBackups,
        boolean readRepairRecovery
    ) {
        super(cacheId,
            nodeId,
            futId,
            writeVer,
            topVer,
            taskNameHash,
            addDepInfo,
            keepBinary,
            skipStore,
            readRepairRecovery);

        assert invokeArgs == null || forceTransformBackups;

        this.forceTransformBackups = forceTransformBackups;
        this.invokeArgs = invokeArgs;

        keys = new ArrayList<>();

        if (forceTransformBackups) {
            entryProcessors = new ArrayList<>();
            entryProcessorsBytes = new ArrayList<>();
        }
        else
            vals = new ArrayList<>();
    }

    /** {@inheritDoc} */
    @Override public void addWriteValue(KeyCacheObject key,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProc,
        long ttl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean addPrevVal,
        @Nullable CacheObject prevVal,
        long updateCntr,
        GridCacheOperation cacheOp) {
        assert key.partition() >= 0 : key;

        keys.add(key);

        if (forceTransformBackups) {
            assert entryProc != null;

            entryProcessors.add(entryProc);
        }
        else
            vals.add(val);

        if (addPrevVal) {
            if (prevVals == null)
                prevVals = new ArrayList<>();

            prevVals.add(prevVal);
        }

        if (updateCntrs == null)
            updateCntrs = new GridLongList();

        updateCntrs.add(updateCntr);

        // In case there is no conflict, do not create the list.
        if (conflictVer != null) {
            if (conflictVers == null) {
                conflictVers = new ArrayList<>();

                for (int i = 0; i < keys.size() - 1; i++)
                    conflictVers.add(null);
            }

            conflictVers.add(conflictVer);
        }
        else if (conflictVers != null)
            conflictVers.add(null);

        if (ttl >= 0 && ttls == null) {
            ttls = new GridLongList(keys.size());

            for (int i = 0; i < keys.size() - 1; i++)
                ttls.add(CU.TTL_NOT_CHANGED);
        }

        if (ttls != null)
            ttls.add(ttl);

        if (conflictExpireTime >= 0 && conflictExpireTimes == null) {
            conflictExpireTimes = new GridLongList(keys.size());

            for (int i = 0; i < keys.size() - 1; i++)
                conflictExpireTimes.add(CU.EXPIRE_TIME_CALCULATE);
        }

        if (conflictExpireTimes != null)
            conflictExpireTimes.add(conflictExpireTime);
    }

    /** {@inheritDoc} */
    @Override public void addNearWriteValue(KeyCacheObject key,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProc,
        long ttl,
        long expireTime) {
        assert key.partition() >= 0 : key;

        int idx = keys == null ? -1 : keys.indexOf(key);

        if (idx > -1) {
            if (obsoleteIndexes == null)
                obsoleteIndexes = new ArrayList<>();

            obsoleteIndexes.add(idx);

            return;
        }

        if (nearKeys == null) {
            nearKeys = new ArrayList<>();

            if (forceTransformBackups) {
                nearEntryProcessors = new ArrayList<>();
                nearEntryProcessorsBytes = new ArrayList<>();
            }
            else
                nearVals = new ArrayList<>();
        }

        nearKeys.add(key);

        if (forceTransformBackups) {
            assert entryProc != null;

            nearEntryProcessors.add(entryProc);
        }
        else
            nearVals.add(val);

        if (ttl >= 0 && nearTtls == null) {
            nearTtls = new GridLongList(nearKeys.size());

            for (int i = 0; i < nearKeys.size() - 1; i++)
                nearTtls.add(CU.TTL_NOT_CHANGED);
        }

        if (nearTtls != null)
            nearTtls.add(ttl);

        if (expireTime >= 0 && nearExpireTimes == null) {
            nearExpireTimes = new GridLongList(nearKeys.size());

            for (int i = 0; i < nearKeys.size() - 1; i++)
                nearExpireTimes.add(CU.EXPIRE_TIME_CALCULATE);
        }

        if (nearExpireTimes != null)
            nearExpireTimes.add(expireTime);
    }

    /** {@inheritDoc} */
    @Override public boolean forceTransformBackups() {
        return forceTransformBackups;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return keys.size();
    }

    /** {@inheritDoc} */
    @Override public int nearSize() {
        return nearKeys != null ? nearKeys.size() : 0;
    }

    /** {@inheritDoc} */
    @Override public int obsoleteNearKeysSize() {
        return obsoleteIndexes != null ? obsoleteIndexes.size() : 0;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject obsoleteNearKey(int idx) {
        return keys.get(obsoleteIndexes.get(idx));
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject key(int idx) {
        return keys.get(idx);
    }

    /** {@inheritDoc} */
    @Override public Long updateCounter(int updCntr) {
        if (updateCntrs != null && updCntr < updateCntrs.size())
            return updateCntrs.get(updCntr);

        return null;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject nearKey(int idx) {
        return nearKeys.get(idx);
    }

    /** {@inheritDoc} */
    @Override @Nullable public CacheObject value(int idx) {
        if (vals != null)
            return vals.get(idx);

        return null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public CacheObject previousValue(int idx) {
        if (prevVals != null)
            return prevVals.get(idx);

        return null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public EntryProcessor<Object, Object, Object> entryProcessor(int idx) {
        return entryProcessors == null ? null : entryProcessors.get(idx);
    }

    /** {@inheritDoc} */
    @Override @Nullable public CacheObject nearValue(int idx) {
        if (nearVals != null)
            return nearVals.get(idx);

        return null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public EntryProcessor<Object, Object, Object> nearEntryProcessor(int idx) {
        return nearEntryProcessors == null ? null : nearEntryProcessors.get(idx);
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
    @Override public long ttl(int idx) {
        if (ttls != null) {
            assert idx >= 0 && idx < ttls.size();

            return ttls.get(idx);
        }

        return CU.TTL_NOT_CHANGED;
    }

    /** {@inheritDoc} */
    @Override public long nearTtl(int idx) {
        if (nearTtls != null) {
            assert idx >= 0 && idx < nearTtls.size();

            return nearTtls.get(idx);
        }

        return CU.TTL_NOT_CHANGED;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        assert !F.isEmpty(keys) || !F.isEmpty(nearKeys);

        int p = !keys.isEmpty() ? keys.get(0).partition() : nearKeys.get(0).partition();

        assert p >= 0;

        return p;
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
    @Override public long nearExpireTime(int idx) {
        if (nearExpireTimes != null) {
            assert idx >= 0 && idx < nearExpireTimes.size();

            return nearExpireTimes.get(idx);
        }

        return CU.EXPIRE_TIME_CALCULATE;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Object[] invokeArguments() {
        return invokeArgs;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId);

        prepareMarshalCacheObjects(keys, cctx);

        prepareMarshalCacheObjects(vals, cctx);

        prepareMarshalCacheObjects(nearKeys, cctx);

        prepareMarshalCacheObjects(nearVals, cctx);

        prepareMarshalCacheObjects(prevVals, cctx);

        if (forceTransformBackups) {
            // force addition of deployment info for entry processors if P2P is enabled globally.
            if (!addDepInfo && ctx.deploymentEnabled())
                addDepInfo = true;

            if (!F.isEmpty(invokeArgs) && invokeArgsBytes == null)
                invokeArgsBytes = Arrays.asList(marshalInvokeArguments(invokeArgs, cctx));

            if (entryProcessorsBytes == null)
                entryProcessorsBytes = marshalCollection(entryProcessors, cctx);

            if (nearEntryProcessorsBytes == null)
                nearEntryProcessorsBytes = marshalCollection(nearEntryProcessors, cctx);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId);

        finishUnmarshalCacheObjects(keys, cctx, ldr);

        finishUnmarshalCacheObjects(vals, cctx, ldr);

        finishUnmarshalCacheObjects(nearKeys, cctx, ldr);

        finishUnmarshalCacheObjects(nearVals, cctx, ldr);

        finishUnmarshalCacheObjects(prevVals, cctx, ldr);

        if (forceTransformBackups) {
            if (entryProcessors == null)
                entryProcessors = unmarshalCollection(entryProcessorsBytes, ctx, ldr);

            if (invokeArgsBytes != null && invokeArgs == null)
                invokeArgs = unmarshalInvokeArguments(invokeArgsBytes.toArray(new byte[invokeArgsBytes.size()][]), ctx, ldr);

            if (nearEntryProcessors == null)
                nearEntryProcessors = unmarshalCollection(nearEntryProcessorsBytes, ctx, ldr);
        }
    }

    /** {@inheritDoc} */
    @Override protected void cleanup() {
        nearVals = null;
        prevVals = null;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 38;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicUpdateRequest.class, this, "super", super.toString());
    }
}
