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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Near cache prepare response.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class GridNearTxPrepareResponse extends GridDistributedTxPrepareResponse {
    /** Versions that are less than lock version ({@link #version()}). */
    @GridToStringInclude
    @Order(9)
    private Collection<GridCacheVersion> pending;

    /** Future ID.  */
    @Order(value = 10, method = "futureId")
    private IgniteUuid futId;

    /** Mini future ID. */
    @Order(11)
    private int miniId;

    /** DHT version. */
    @Order(value = 12, method = "dhtVersion")
    private GridCacheVersion dhtVer;

    /** Write version. */
    @Order(value = 13, method = "writeVersion")
    private GridCacheVersion writeVer;

    /** Map of owned values to set on near node. */
    @GridToStringInclude
    @Order(value = 14, method = "ownedValues")
    private Map<IgniteTxKey, CacheVersionedValue> ownedVals;

    /** Cache return value. */
    @Order(value = 15, method = "returnValue")
    private GridCacheReturn retVal;

    /** Keys that did not pass the filter. */
    @Order(16)
    private Collection<IgniteTxKey> filterFailedKeys;

    /** Topology version, which is set when client node should remap lock request. */
    @Order(value = 17, method = "clientRemapVersion")
    private AffinityTopologyVersion clientRemapVer;

    /** One-phase commit on primary flag. */
    @Order(18)
    private boolean onePhaseCommit;

    /**
     * Empty constructor.
     */
    public GridNearTxPrepareResponse() {
        // No-op.
    }

    /**
     * @param part Partition.
     * @param xid Xid version.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param dhtVer DHT version.
     * @param writeVer Write version.
     * @param retVal Return value.
     * @param err Error.
     * @param clientRemapVer Not {@code null} if client node should remap transaction.
     * @param addDepInfo Deployment info flag.
     */
    public GridNearTxPrepareResponse(
        int part,
        GridCacheVersion xid,
        IgniteUuid futId,
        int miniId,
        GridCacheVersion dhtVer,
        GridCacheVersion writeVer,
        GridCacheReturn retVal,
        Throwable err,
        AffinityTopologyVersion clientRemapVer,
        boolean onePhaseCommit,
        boolean addDepInfo
    ) {
        super(part, xid, err, addDepInfo);

        assert futId != null;
        assert dhtVer != null;

        this.futId = futId;
        this.miniId = miniId;
        this.dhtVer = dhtVer;
        this.writeVer = writeVer;
        this.retVal = retVal;
        this.clientRemapVer = clientRemapVer;
        this.onePhaseCommit = onePhaseCommit;
    }

    /**
     * @return One-phase commit on primary flag.
     */
    public boolean onePhaseCommit() {
        return onePhaseCommit;
    }

    /**
     * @param onePhaseCommit New one-phase commit on primary flag.
     */
    public void onePhaseCommit(boolean onePhaseCommit) {
        this.onePhaseCommit = onePhaseCommit;
    }

    /**
     * @return Topology version, which is set when client node should remap lock request.
     */
    @Nullable public AffinityTopologyVersion clientRemapVersion() {
        return clientRemapVer;
    }

    /**
     * @param clientRemapVer New topology version, which is set when client node should remap lock request.
     */
    public void clientRemapVersion(AffinityTopologyVersion clientRemapVer) {
        this.clientRemapVer = clientRemapVer;
    }

    /**
     * @return Versions that are less than lock version ({@link #version()}).
     */
    public Collection<GridCacheVersion> pending() {
        return pending;
    }

    /**
     * @param pending New versions that are less than lock version ({@link #version()}).
     */
    public void pending(Collection<GridCacheVersion> pending) {
        this.pending = pending;
    }

    /**
     * @return Mini future ID.
     */
    public int miniId() {
        return miniId;
    }

    /**
     * @param miniId New mini future ID.
     */
    public void miniId(int miniId) {
        this.miniId = miniId;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @param futId New future ID.
     */
    public void futureId(IgniteUuid futId) {
        this.futId = futId;
    }

    /**
     * @return DHT version.
     */
    public GridCacheVersion dhtVersion() {
        return dhtVer;
    }

    /**
     * @param dhtVer New DHT version.
     */
    public void dhtVersion(GridCacheVersion dhtVer) {
        this.dhtVer = dhtVer;
    }

    /**
     * @return Write version.
     */
    public GridCacheVersion writeVersion() {
        return writeVer;
    }

    /**
     * @param writeVer New write version.
     */
    public void writeVersion(GridCacheVersion writeVer) {
        this.writeVer = writeVer;
    }

    /**
     * Adds owned value.
     *
     * @param key Key.
     * @param ver DHT version.
     * @param val Value.
     */
    public void addOwnedValue(IgniteTxKey key, GridCacheVersion ver, CacheObject val) {
        if (val == null)
            return;

        if (ownedVals == null)
            ownedVals = new HashMap<>();

        CacheVersionedValue oVal = new CacheVersionedValue(val, ver);

        ownedVals.put(key, oVal);
    }

    /**
     * @return Map of owned values to set on near node.
     */
    public Map<IgniteTxKey, CacheVersionedValue> ownedValues() {
        return ownedVals;
    }

    /**
     * @param ownedVals New map of owned values to set on near node.
     */
    public void ownedValues(Map<IgniteTxKey, CacheVersionedValue> ownedVals) {
        this.ownedVals = ownedVals;
    }

    /**
     * @return Cache return value.
     */
    public GridCacheReturn returnValue() {
        return retVal;
    }

    /**
     * @param retVal New cache return value.
     */
    public void returnValue(GridCacheReturn retVal) {
        this.retVal = retVal;
    }

    /**
     * @param filterFailedKeys Keys that did not pass the filter.
     */
    public void filterFailedKeys(Collection<IgniteTxKey> filterFailedKeys) {
        this.filterFailedKeys = filterFailedKeys;
    }

    /**
     * @return New keys that did not pass the filter.
     */
    public Collection<IgniteTxKey> filterFailedKeys() {
        return filterFailedKeys;
    }

    /**
     * @param key Key.
     * @return {@code True} if response has owned value for given key.
     */
    public boolean hasOwnedValue(IgniteTxKey key) {
        return F.mapContainsKey(ownedVals, key);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (ownedVals != null) {
            for (Map.Entry<IgniteTxKey, CacheVersionedValue> entry : ownedVals.entrySet()) {
                GridCacheContext<?, ?> cacheCtx = ctx.cacheContext(entry.getKey().cacheId());

                entry.getKey().prepareMarshal(cacheCtx);

                entry.getValue().prepareMarshal(cacheCtx.cacheObjectContext());
            }
        }

        if (retVal != null && retVal.cacheId() != 0) {
            GridCacheContext<?, ?> cctx = ctx.cacheContext(retVal.cacheId());

            assert cctx != null : retVal.cacheId();

            retVal.prepareMarshal(cctx);
        }

        if (filterFailedKeys != null) {
            for (IgniteTxKey key : filterFailedKeys) {
                GridCacheContext<?, ?> cctx = ctx.cacheContext(key.cacheId());

                key.prepareMarshal(cctx);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (ownedVals != null) {
            for (Map.Entry<IgniteTxKey, CacheVersionedValue> e : ownedVals.entrySet()) {
                IgniteTxKey key = e.getKey();

                GridCacheContext<?, ?> cctx = ctx.cacheContext(key.cacheId());

                key.finishUnmarshal(cctx, ldr);

                e.getValue().finishUnmarshal(cctx, ldr);
            }
        }

        if (retVal != null && retVal.cacheId() != 0) {
            GridCacheContext<?, ?> cctx = ctx.cacheContext(retVal.cacheId());

            assert cctx != null : retVal.cacheId();

            retVal.finishUnmarshal(cctx, ldr);
        }

        if (filterFailedKeys != null) {
            for (IgniteTxKey key : filterFailedKeys) {
                GridCacheContext<?, ?> cctx = ctx.cacheContext(key.cacheId());

                key.finishUnmarshal(cctx, ldr);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 56;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxPrepareResponse.class, this, "super", super.toString());
    }
}
