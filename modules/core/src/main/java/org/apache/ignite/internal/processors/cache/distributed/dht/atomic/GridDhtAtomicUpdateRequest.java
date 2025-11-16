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
    private List<KeyCacheObject> keys;

    /** Values to update. */
    @GridToStringInclude
    @Order(12)
    private List<CacheObject> vals;

    /** Previous values. */
    @GridToStringInclude
    @Order(value = 13, method = "previousValues")
    private List<CacheObject> prevVals;

    /** Conflict versions. */
    @Order(value = 14, method = "conflictVersions")
    private List<GridCacheVersion> conflictVers;

    /** TTLs. */
    @Order(15)
    private GridLongList ttls;

    /** Conflict expire time. */
    @Order(16)
    private GridLongList conflictExpireTimes;

    /** Near TTLs. */
    @Order(17)
    private GridLongList nearTtls;

    /** Near expire times. */
    @Order(18)
    private GridLongList nearExpireTimes;

    /** Near cache keys to update. */
    @GridToStringInclude
    @Order(19)
    private List<KeyCacheObject> nearKeys;

    /** Values to update. */
    @GridToStringInclude
    @Order(value = 20, method = "nearValues")
    private List<CacheObject> nearVals;

    /** Obsolete near values. */
    @GridToStringInclude
    @Order(21)
    private List<Integer> obsoleteIndexes;

    /** Force transform backups flag. */
    @Order(22)
    private boolean forceTransformBackups;

    /** Entry processors. */
    private List<EntryProcessor<Object, Object, Object>> entryProcessors;

    /** Entry processors bytes. */
    @Order(23)
    private List<byte[]> entryProcessorsBytes;

    /** Near entry processors. */
    private List<EntryProcessor<Object, Object, Object>> nearEntryProcessors;

    /** Near entry processors bytes. */
    @Order(24)
    private List<byte[]> nearEntryProcessorsBytes;

    /** Optional arguments for entry processor. */
    private Object[] invokeArgs;

    /** Entry processor arguments bytes. */
    @Order(25)
    private byte[][] invokeArgsBytes;

    /** Partition. */
    @Order(value = 26, method = "updateCounters")
    private GridLongList updateCntrs;

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

        if (ttl >= 0) {
            if (ttls == null) {
                ttls = new GridLongList(keys.size());

                for (int i = 0; i < keys.size() - 1; i++)
                    ttls.add(CU.TTL_NOT_CHANGED);
            }
        }

        if (ttls != null)
            ttls.add(ttl);

        if (conflictExpireTime >= 0) {
            if (conflictExpireTimes == null) {
                conflictExpireTimes = new GridLongList(keys.size());

                for (int i = 0; i < keys.size() - 1; i++)
                    conflictExpireTimes.add(CU.EXPIRE_TIME_CALCULATE);
            }
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

        if (ttl >= 0) {
            if (nearTtls == null) {
                nearTtls = new GridLongList(nearKeys.size());

                for (int i = 0; i < nearKeys.size() - 1; i++)
                    nearTtls.add(CU.TTL_NOT_CHANGED);
            }
        }

        if (nearTtls != null)
            nearTtls.add(ttl);

        if (expireTime >= 0) {
            if (nearExpireTimes == null) {
                nearExpireTimes = new GridLongList(nearKeys.size());

                for (int i = 0; i < nearKeys.size() - 1; i++)
                    nearExpireTimes.add(CU.EXPIRE_TIME_CALCULATE);
            }
        }

        if (nearExpireTimes != null)
            nearExpireTimes.add(expireTime);
    }

    /** {@inheritDoc} */
    @Override public boolean forceTransformBackups() {
        return forceTransformBackups;
    }

    /**
     * @param forceTransformBackups New force transform backups flag.
     */
    public void forceTransformBackups(boolean forceTransformBackups) {
        this.forceTransformBackups = forceTransformBackups;
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

    /**
     * @return Keys to update.
     */
    public List<KeyCacheObject> keys() {
        return keys;
    }

    /**
     * @param keys New keys to update.
     */
    public void keys(List<KeyCacheObject> keys) {
        this.keys = keys;
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

    /**
     * @return Near cache keys to update.
     */
    public List<KeyCacheObject> nearKeys() {
        return nearKeys;
    }

    /**
     * @param nearKeys New near cache keys to update.
     */
    public void nearKeys(List<KeyCacheObject> nearKeys) {
        this.nearKeys = nearKeys;
    }

    /** {@inheritDoc} */
    @Override @Nullable public CacheObject value(int idx) {
        if (vals != null)
            return vals.get(idx);

        return null;
    }

    /**
     * @return Values to update.
     */
    public List<CacheObject> vals() {
        return vals;
    }

    /**
     * @param vals New values to update.
     */
    public void vals(List<CacheObject> vals) {
        this.vals = vals;
    }

    /** {@inheritDoc} */
    @Override @Nullable public CacheObject previousValue(int idx) {
        if (prevVals != null)
            return prevVals.get(idx);

        return null;
    }

    /**
     * @return Previous values.
     */
    public List<CacheObject> previousValues() {
        return prevVals;
    }

    /**
     * @param prevVals New previous values.
     */
    public void previousValues(List<CacheObject> prevVals) {
        this.prevVals = prevVals;
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

    /**
     * @return Values to update.
     */
    public List<CacheObject> nearValues() {
        return nearVals;
    }

    /**
     * @param nearVals New values to update.
     */
    public void nearValues(List<CacheObject> nearVals) {
        this.nearVals = nearVals;
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

    /**
     * @return Conflict versions.
     */
    public List<GridCacheVersion> conflictVersions() {
        return conflictVers;
    }

    /**
     * @param conflictVers New conflict versions.
     */
    public void conflictVersions(List<GridCacheVersion> conflictVers) {
        this.conflictVers = conflictVers;
    }

    /** {@inheritDoc} */
    @Override public long ttl(int idx) {
        if (ttls != null) {
            assert idx >= 0 && idx < ttls.size();

            return ttls.get(idx);
        }

        return CU.TTL_NOT_CHANGED;
    }

    /**
     * @return TTLs.
     */
    public GridLongList ttls() {
        return ttls;
    }

    /**
     * @param ttls New TTLs.
     */
    public void ttls(GridLongList ttls) {
        this.ttls = ttls;
    }

    /** {@inheritDoc} */
    @Override public long nearTtl(int idx) {
        if (nearTtls != null) {
            assert idx >= 0 && idx < nearTtls.size();

            return nearTtls.get(idx);
        }

        return CU.TTL_NOT_CHANGED;
    }

    /**
     * @return Near TTLs.
     */
    public GridLongList nearTtls() {
        return nearTtls;
    }

    /**
     * @param nearTtls New near TTLs.
     */
    public void nearTtls(GridLongList nearTtls) {
        this.nearTtls = nearTtls;
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

    /**
     * @return Conflict expire times.
     */
    public GridLongList conflictExpireTimes() {
        return conflictExpireTimes;
    }

    /**
     * @param conflictExpireTimes New conflict expire times.
     */
    public void conflictExpireTimes(GridLongList conflictExpireTimes) {
        this.conflictExpireTimes = conflictExpireTimes;
    }

    /** {@inheritDoc} */
    @Override public long nearExpireTime(int idx) {
        if (nearExpireTimes != null) {
            assert idx >= 0 && idx < nearExpireTimes.size();

            return nearExpireTimes.get(idx);
        }

        return CU.EXPIRE_TIME_CALCULATE;
    }

    /**
     * @return Near expire times.
     */
    public GridLongList nearExpireTimes() {
        return nearExpireTimes;
    }

    /**
     * @param nearExpireTimes New near expire times.
     */
    public void nearExpireTimes(GridLongList nearExpireTimes) {
        this.nearExpireTimes = nearExpireTimes;
    }

    /**
     * @return Obsolete near values.
     */
    public List<Integer> obsoleteIndexes() {
        return obsoleteIndexes;
    }

    /**
     * @param obsoleteIndexes New obsolete near values.
     */
    public void obsoleteIndexes(List<Integer> obsoleteIndexes) {
        this.obsoleteIndexes = obsoleteIndexes;
    }

    /**
     * @return Serialized entry processors.
     */
    public List<byte[]> entryProcessorsBytes() {
        return entryProcessorsBytes;
    }

    /**
     * @param entryProcessorsBytes New entry processors.
     */
    public void entryProcessorsBytes(List<byte[]> entryProcessorsBytes) {
        this.entryProcessorsBytes = entryProcessorsBytes;
    }

    /**
     * @return Serialized near entry processors.
     */
    public List<byte[]> nearEntryProcessorsBytes() {
        return Collections.unmodifiableList(nearEntryProcessorsBytes);
    }

    /**
     * @param nearEntryProcessorsBytes New serialized near entry processors.
     */
    public void nearEntryProcessorsBytes(List<byte[]> nearEntryProcessorsBytes) {
        this.nearEntryProcessorsBytes = nearEntryProcessorsBytes;
    }

    /**
     * @return Serialized optional entry processor arguments.
     */
    public byte[][] invokeArgsBytes() {
        return invokeArgsBytes;
    }

    /**
     * @param invokeArgsBytes New serialized optional entry processor arguments.
     */
    public void invokeArgsBytes(byte[][] invokeArgsBytes) {
        this.invokeArgsBytes = invokeArgsBytes;
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

            if (invokeArgsBytes == null)
                invokeArgsBytes = marshalInvokeArguments(invokeArgs, cctx);

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

            if (invokeArgs == null)
                invokeArgs = unmarshalInvokeArguments(invokeArgsBytes, ctx, ldr);

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

    /**
     * @return Partition update counters.
     */
    public GridLongList updateCounters() {
        return updateCntrs;
    }

    /**
     * @param updateCntrs New partition update counters.
     */
    public void updateCounters(GridLongList updateCntrs) {
        this.updateCntrs = updateCntrs;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicUpdateRequest.class, this, "super", super.toString());
    }
}
