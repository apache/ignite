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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * DHT prepare request.
 */
public class GridDhtTxPrepareRequest extends GridDistributedTxPrepareRequest {
    /** Max order. */
    @Order(20)
    private UUID nearNodeId;

    /** Future ID. */
    @Order(value = 21, method = "futureId")
    private IgniteUuid futId;

    /** Mini future ID. */
    @Order(22)
    private int miniId;

    /** Topology version. */
    @Order(value = 23, method = "topologyVersion")
    private AffinityTopologyVersion topVer;

    /** Invalidate near entries flags. */
    @Order(24)
    private BitSet invalidateNearEntries;

    /** Near writes. */
    @Order(25)
    @GridToStringInclude
    private Collection<IgniteTxEntry> nearWrites;

    /** Owned versions by key. */
    @GridToStringInclude
    private Map<IgniteTxKey, GridCacheVersion> owned;

    /** Owned keys. */
    @Order(26)
    private Collection<IgniteTxKey> ownedKeys;

    /** Owned values. */
    @Order(value = 27, method = "ownedValues")
    private Collection<GridCacheVersion> ownedVals;

    /** */
    @Order(value = 28, method = "updateCounters")
    private Collection<PartitionUpdateCountersMessage> updCntrs;

    /** Near transaction ID. */
    @Order(value = 29, method = "nearXidVersion")
    private GridCacheVersion nearXidVer;

    /** Task name hash. */
    @Order(30)
    private int taskNameHash;

    /** Preload keys. */
    @Order(31)
    private BitSet preloadKeys;

    /** */
    private List<IgniteTxKey> nearWritesCacheMissed;

    /** {@code True} if remote tx should skip adding itself to completed versions map on finish. */
    @Order(value = 32, method = "skipCompletedVersion")
    private boolean skipCompletedVers;

    /** Transaction label. */
    @Order(value = 33, method = "txLabel")
    @GridToStringInclude
    @Nullable private String txLbl;

    /**
     * Empty constructor.
     */
    public GridDhtTxPrepareRequest() {
        // No-op.
    }

    /**
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param topVer Topology version.
     * @param tx Transaction.
     * @param timeout Transaction timeout.
     * @param dhtWrites DHT writes.
     * @param nearWrites Near writes.
     * @param txNodes Transaction nodes mapping.
     * @param nearXidVer Near transaction ID.
     * @param last {@code True} if this is last prepare request for node.
     * @param addDepInfo Deployment info flag.
     * @param storeWriteThrough Cache store write through flag.
     * @param retVal Need return value flag
     * @param updCntrs Update counters for Tx.
     */
    public GridDhtTxPrepareRequest(
        IgniteUuid futId,
        int miniId,
        AffinityTopologyVersion topVer,
        GridDhtTxLocalAdapter tx,
        long timeout,
        Collection<IgniteTxEntry> dhtWrites,
        Collection<IgniteTxEntry> nearWrites,
        Map<UUID, Collection<UUID>> txNodes,
        GridCacheVersion nearXidVer,
        boolean last,
        boolean onePhaseCommit,
        int taskNameHash,
        boolean addDepInfo,
        boolean storeWriteThrough,
        boolean retVal,
        Collection<PartitionUpdateCountersMessage> updCntrs) {
        super(tx,
            timeout,
            null,
            dhtWrites,
            txNodes,
            retVal,
            last,
            onePhaseCommit,
            addDepInfo);

        assert futId != null;
        assert miniId != 0;

        this.topVer = topVer;
        this.futId = futId;
        this.nearWrites = nearWrites;
        this.miniId = miniId;
        this.nearXidVer = nearXidVer;
        this.taskNameHash = taskNameHash;
        this.updCntrs = updCntrs;

        storeWriteThrough(storeWriteThrough);
        needReturnValue(retVal);

        invalidateNearEntries = new BitSet(dhtWrites == null ? 0 : dhtWrites.size());

        nearNodeId = tx.nearNodeId();

        skipCompletedVers = tx.xidVersion() == tx.nearXidVersion();

        txLbl = tx.label();
    }

    /**
     * @return Update counters list.
     */
    public Collection<PartitionUpdateCountersMessage> updateCounters() {
        return updCntrs;
    }

    /**
     * @param updCntrs Update counters list.
     */
    public void updateCounters(Collection<PartitionUpdateCountersMessage> updCntrs) {
        this.updCntrs = updCntrs;
    }

    /**
     * @return Near cache writes for which cache was not found (possible if client near cache was closed).
     */
    @Nullable public List<IgniteTxKey> nearWritesCacheMissed() {
        return nearWritesCacheMissed;
    }

    /**
     * @return Near transaction ID.
     */
    public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /**
     * @param nearXidVer Near transaction ID.
     */
    public void nearXidVersion(GridCacheVersion nearXidVer) {
        this.nearXidVer = nearXidVer;
    }

    /**
     * @return Near node ID.
     */
    public UUID nearNodeId() {
        return nearNodeId;
    }

    /**
     * @param nodeId Near node ID.
     */
    public void nearNodeId(UUID nodeId) {
        nearNodeId = nodeId;
    }

    /**
     * @return Invalidate near entries flags.
     */
    public BitSet invalidateNearEntries() {
        return invalidateNearEntries;
    }

    /**
     * @param invalidateNearEntries Invalidate near entries flags.
     */
    public void invalidateNearEntries(BitSet invalidateNearEntries) {
        this.invalidateNearEntries = invalidateNearEntries;
    }

    /**
     * @return Task name hash.
     */
    public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @param taskNameHash Task name hash.
     */
    public void taskNameHash(int taskNameHash) {
        this.taskNameHash = taskNameHash;
    }

    /**
     * @return Near writes.
     */
    public Collection<IgniteTxEntry> nearWrites() {
        return nearWrites == null ? Collections.emptyList() : nearWrites;
    }

    /**
     * @param nearWrites Near writes.
     */
    public void nearWrites(Collection<IgniteTxEntry> nearWrites) {
        this.nearWrites = nearWrites;
    }

    /**
     * @param idx Entry index to set invalidation flag.
     * @param invalidate Invalidation flag value.
     */
    void invalidateNearEntry(int idx, boolean invalidate) {
        invalidateNearEntries.set(idx, invalidate);
    }

    /**
     * @param idx Index to get invalidation flag value.
     * @return Invalidation flag value.
     */
    public boolean invalidateNearEntry(int idx) {
        return invalidateNearEntries.get(idx);
    }

    /**
     * Marks last added key for preloading.
     *
     * @param idx Key index.
     */
    void markKeyForPreload(int idx) {
        if (preloadKeys == null)
            preloadKeys = new BitSet();

        preloadKeys.set(idx, true);
    }

    /**
     * Checks whether entry info should be sent to primary node from backup.
     *
     * @param idx Index.
     * @return {@code True} if value should be sent, {@code false} otherwise.
     */
    public boolean needPreloadKey(int idx) {
        return preloadKeys != null && preloadKeys.get(idx);
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @param futId Future ID.
     */
    public void futureId(IgniteUuid futId) {
        this.futId = futId;
    }

    /**
     * @return Mini future ID.
     */
    public int miniId() {
        return miniId;
    }

    /**
     * @param miniId Mini future ID.
     */
    public void miniId(int miniId) {
        this.miniId = miniId;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @param topVer Topology version.
     */
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /**
     * @return Owned keys.
     */
    public Collection<IgniteTxKey> ownedKeys() {
        return ownedKeys;
    }

    /**
     * @param ownedKeys Owned keys.
     */
    public void ownedKeys(Collection<IgniteTxKey> ownedKeys) {
        this.ownedKeys = ownedKeys;
    }

    /**
     * @return Owned values.
     */
    public Collection<GridCacheVersion> ownedValues() {
        return ownedVals;
    }

    /**
     * @param ownedVals Owned values.
     */
    public void ownedValues(Collection<GridCacheVersion> ownedVals) {
        this.ownedVals = ownedVals;
    }

    /**
     * @return Preload keys.
     */
    public BitSet preloadKeys() {
        return preloadKeys;
    }

    /**
     * @param preloadKeys Preload keys.
     */
    public void preloadKeys(BitSet preloadKeys) {
        this.preloadKeys = preloadKeys;
    }

    /**
     * Sets owner and its mapped version.
     *
     * @param key Key.
     * @param ownerMapped Owner mapped version.
     */
    public void owned(IgniteTxKey key, GridCacheVersion ownerMapped) {
        if (owned == null)
            owned = new GridLeanMap<>(3);

        owned.put(key, ownerMapped);
    }

    /**
     * @return Owned versions map.
     */
    public Map<IgniteTxKey, GridCacheVersion> owned() {
        return owned;
    }

    /**
     * @return {@code True} if remote tx should skip adding itself to completed versions map on finish.
     */
    public boolean skipCompletedVersion() {
        return skipCompletedVers;
    }

    /**
     * @param skipCompletedVers {@code True} if remote tx should skip adding itself to completed versions map on finish.
     */
    public void skipCompletedVersion(boolean skipCompletedVers) {
        this.skipCompletedVers = skipCompletedVers;
    }

    /**
     * @return Transaction label.
     */
    @Nullable public String txLabel() {
        return txLbl;
    }

    /**
     * @param txLbl Transaction label.
     */
    public void txLabel(String txLbl) {
        this.txLbl = txLbl;
    }

    /**
     * {@inheritDoc}
     *
     * @param ctx
     */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (owned != null && ownedKeys == null) {
            ownedKeys = owned.keySet();

            ownedVals = owned.values();

            for (IgniteTxKey key: ownedKeys) {
                GridCacheContext<?, ?> cctx = ctx.cacheContext(key.cacheId());

                key.prepareMarshal(cctx);

                if (addDepInfo)
                    prepareObject(key, cctx);
            }
        }

        if (nearWrites != null)
            marshalTx(nearWrites, ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (ownedKeys != null) {
            assert ownedKeys.size() == ownedVals.size();

            owned = U.newHashMap(ownedKeys.size());

            Iterator<IgniteTxKey> keyIter = ownedKeys.iterator();

            Iterator<GridCacheVersion> valIter = ownedVals.iterator();

            while (keyIter.hasNext()) {
                IgniteTxKey key = keyIter.next();

                GridCacheContext<?, ?> cacheCtx = ctx.cacheContext(key.cacheId());

                if (cacheCtx != null) {
                    key.finishUnmarshal(cacheCtx, ldr);

                    owned.put(key, valIter.next());
                }
            }
        }

        if (nearWrites != null) {
            for (Iterator<IgniteTxEntry> it = nearWrites.iterator(); it.hasNext();) {
                IgniteTxEntry e = it.next();

                GridCacheContext<?, ?> cacheCtx = ctx.cacheContext(e.cacheId());

                if (cacheCtx == null) {
                    it.remove();

                    if (nearWritesCacheMissed == null)
                        nearWritesCacheMissed = new ArrayList<>();

                    nearWritesCacheMissed.add(e.txKey());
                }
                else {
                    e.context(cacheCtx);

                    e.unmarshal(ctx, true, ldr);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 34;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return U.safeAbs(version().hashCode());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxPrepareRequest.class, this, "super", super.toString());
    }
}
