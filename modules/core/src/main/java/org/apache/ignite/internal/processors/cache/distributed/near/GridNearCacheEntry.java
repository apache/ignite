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

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheLockCandidates;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMvcc;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;

/**
 * Near cache entry.
 */
@SuppressWarnings({"NonPrivateFieldAccessedInSynchronizedContext", "TooBroadScope"})
public class GridNearCacheEntry extends GridDistributedCacheEntry {
    /** */
    private static final int NEAR_SIZE_OVERHEAD = 36 + 16;

    /** Topology version at the moment when value was initialized from primary node. */
    private volatile AffinityTopologyVersion topVer = AffinityTopologyVersion.NONE;

    /** DHT version which caused the last update. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private GridCacheVersion dhtVer;

    /** Partition. */
    private int part;

    /** */
    private short evictReservations;

    /**
     * @param ctx Cache context.
     * @param key Cache key.
     * @param hash Key hash value.
     * @param val Entry value.
     */
    public GridNearCacheEntry(
        GridCacheContext ctx,
        KeyCacheObject key,
        int hash,
        CacheObject val
    ) {
        super(ctx, key, hash, val);

        part = ctx.affinity().partition(key);
    }

    /** {@inheritDoc} */
    @Override public int memorySize() throws IgniteCheckedException {
        return super.memorySize() + NEAR_SIZE_OVERHEAD;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return part;
    }

    /** {@inheritDoc} */
    @Override public boolean isNear() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean valid(AffinityTopologyVersion topVer) {
        assert topVer.topologyVersion() > 0 : "Topology version is invalid: " + topVer;

        AffinityTopologyVersion topVer0 = this.topVer;

        if (topVer0.equals(topVer))
            return true;

        if (topVer0.equals(AffinityTopologyVersion.NONE) || topVer.compareTo(topVer0) < 0)
            return false;

        try {
            if (cctx.affinity().primaryChanged(partition(), topVer0, topVer)) {
                this.topVer = AffinityTopologyVersion.NONE;

                return false;
            }

            if (cctx.affinity().backupByPartition(cctx.localNode(), part, topVer)) {
                this.topVer = AffinityTopologyVersion.NONE;

                return false;
            }

            this.topVer = topVer;

            return true;
        }
        catch (IllegalStateException ignore) {
            // Do not have affinity history.
            this.topVer = AffinityTopologyVersion.NONE;

            return false;
        }
    }

    /**
     * @param topVer Topology version.
     * @throws GridCacheEntryRemovedException If this entry is obsolete.
     */
    public void initializeFromDht(AffinityTopologyVersion topVer) throws GridCacheEntryRemovedException {
        GridDhtCacheEntry entry = cctx.near().dht().peekExx(key);

        if (entry != null) {
            GridCacheEntryInfo e = entry.info();

            if (e != null) {
                GridCacheVersion enqueueVer = null;

                try {
                    synchronized (this) {
                        checkObsolete();

                        if (isNew() || !valid(topVer)) {
                            // Version does not change for load ops.
                            update(e.value(), e.expireTime(), e.ttl(), e.isNew() ? ver : e.version(), true);

                            if (cctx.deferredDelete() && !isNew() && !isInternal()) {
                                boolean deleted = val == null;

                                if (deleted != deletedUnlocked()) {
                                    deletedUnlocked(deleted);

                                    if (deleted)
                                        enqueueVer = e.version();
                                }
                            }

                            ClusterNode primaryNode = cctx.affinity().primaryByKey(key, topVer);

                            if (primaryNode == null)
                                this.topVer = AffinityTopologyVersion.NONE;
                            else
                                recordNodeId(primaryNode.id(), topVer);

                            dhtVer = e.isNew() || e.isDeleted() ? null : e.version();
                        }
                    }
                }
                finally {
                    if (enqueueVer != null)
                        cctx.onDeferredDelete(this, enqueueVer);
                }
            }
        }
    }

    /**
     * This method should be called only when lock is owned on this entry.
     *
     * @param val Value.
     * @param ver Version.
     * @param dhtVer DHT version.
     * @param primaryNodeId Primary node ID.
     * @param topVer Topology version.
     * @return {@code True} if reset was done.
     * @throws GridCacheEntryRemovedException If obsolete.
     */
    public boolean resetFromPrimary(CacheObject val,
        GridCacheVersion ver,
        GridCacheVersion dhtVer,
        UUID primaryNodeId,
        AffinityTopologyVersion topVer)
        throws GridCacheEntryRemovedException
    {
        assert dhtVer != null;

        cctx.versions().onReceived(primaryNodeId, dhtVer);

        synchronized (this) {
            checkObsolete();

            primaryNode(primaryNodeId, topVer);

            if (!F.eq(this.dhtVer, dhtVer)) {
                value(val);

                this.ver = ver;
                this.dhtVer = dhtVer;

                return true;
            }
        }

        return false;
    }

    /**
     * This method should be called only when lock is owned on this entry.
     *
     * @param dhtVer DHT version.
     * @param val Value associated with version.
     * @param expireTime Expire time.
     * @param ttl Time to live.
     * @param primaryNodeId Primary node ID.
     * @param topVer Topology version.
     */
    public void updateOrEvict(GridCacheVersion dhtVer,
        @Nullable CacheObject val,
        long expireTime,
        long ttl,
        UUID primaryNodeId,
        AffinityTopologyVersion topVer)
    {
        assert dhtVer != null;

        cctx.versions().onReceived(primaryNodeId, dhtVer);

        synchronized (this) {
            if (!obsolete()) {
                // Don't set DHT version to null until we get a match from DHT remote transaction.
                if (F.eq(this.dhtVer, dhtVer))
                    this.dhtVer = null;

                // If we are here, then we already tried to evict this entry.
                // If cannot evict, then update.
                if (this.dhtVer == null) {
                    if (!markObsolete(dhtVer)) {
                        value(val);

                        ttlAndExpireTimeExtras((int) ttl, expireTime);

                        primaryNode(primaryNodeId, topVer);
                    }
                }
            }
        }
    }

    /**
     * @return DHT version for this entry.
     * @throws GridCacheEntryRemovedException If obsolete.
     */
    @Nullable public synchronized GridCacheVersion dhtVersion() throws GridCacheEntryRemovedException {
        checkObsolete();

        return dhtVer;
    }

    /**
     * @return Tuple with version and value of this entry.
     * @throws GridCacheEntryRemovedException If entry has been removed.
     */
    @Nullable public synchronized IgniteBiTuple<GridCacheVersion, CacheObject> versionedValue()
        throws GridCacheEntryRemovedException {
        checkObsolete();

        if (dhtVer == null)
            return null;
        else {
            CacheObject val0 = valueBytesUnlocked();

            return F.t(dhtVer, val0);
        }
    }

    /** {@inheritDoc} */
    @Override protected void recordNodeId(UUID primaryNodeId, AffinityTopologyVersion topVer) {
        assert Thread.holdsLock(this);

        assert topVer.compareTo(cctx.affinity().affinityTopologyVersion()) <= 0 : "Affinity not ready [" +
            "topVer=" + topVer +
            ", readyVer=" + cctx.affinity().affinityTopologyVersion() +
            ", cache=" + cctx.name() + ']';

        primaryNode(primaryNodeId, topVer);
    }

    /**
     * @param dhtVer DHT version to record.
     * @return {@code False} if given version is lower then existing version.
     */
    public final boolean recordDhtVersion(GridCacheVersion dhtVer) {
        assert dhtVer != null;
        assert Thread.holdsLock(this);

        if (this.dhtVer == null || this.dhtVer.compareTo(dhtVer) <= 0) {
            this.dhtVer = dhtVer;

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override protected Object readThrough(IgniteInternalTx tx, KeyCacheObject key, boolean reload,
        UUID subjId, String taskName) throws IgniteCheckedException {
        return cctx.near().loadAsync(tx,
            F.asList(key),
            /*force primary*/false,
            subjId,
            taskName,
            true,
            null,
            false,
            /*skip store*/false,
            /*can remap*/true,
            false
        ).get().get(keyValue(false));
    }

    /**
     * @param tx Transaction.
     * @param primaryNodeId Primary node ID.
     * @param val New value.
     * @param ver Version to use.
     * @param dhtVer DHT version received from remote node.
     * @param ttl Time to live.
     * @param expireTime Expiration time.
     * @param evt Event flag.
     * @param topVer Topology version.
     * @param subjId Subject ID.
     * @return {@code True} if initial value was set.
     * @throws IgniteCheckedException In case of error.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    public boolean loadedValue(@Nullable IgniteInternalTx tx,
        UUID primaryNodeId,
        CacheObject val,
        GridCacheVersion ver,
        GridCacheVersion dhtVer,
        long ttl,
        long expireTime,
        boolean evt,
        boolean keepBinary,
        AffinityTopologyVersion topVer,
        UUID subjId)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        assert dhtVer != null;

        GridCacheVersion enqueueVer = null;

        try {
            synchronized (this) {
                checkObsolete();

                if (cctx.cache().configuration().isStatisticsEnabled())
                    cctx.cache().metrics0().onRead(false);

                boolean ret = false;

                CacheObject old = this.val;
                boolean hasVal = hasValueUnlocked();

                if (this.dhtVer == null || this.dhtVer.compareTo(dhtVer) < 0) {
                    primaryNode(primaryNodeId, topVer);

                    update(val, expireTime, ttl, ver, true);

                    if (cctx.deferredDelete() && !isInternal()) {
                        boolean deleted = val == null;

                        if (deleted != deletedUnlocked()) {
                            deletedUnlocked(deleted);

                            if (deleted)
                                enqueueVer = ver;
                        }
                    }

                    this.dhtVer = dhtVer;

                    ret = true;
                }

                if (evt && cctx.events().isRecordable(EVT_CACHE_OBJECT_READ))
                    cctx.events().addEvent(
                        partition(),
                        key,
                        tx,
                        null,
                        EVT_CACHE_OBJECT_READ,
                        val,
                        val != null,
                        old,
                        hasVal,
                        subjId,
                        null,
                        null,
                        keepBinary);

                return ret;
            }
        }
        finally {
            if (enqueueVer != null)
                cctx.onDeferredDelete(this, enqueueVer);
        }
    }

    /** {@inheritDoc} */
    @Override protected void updateIndex(CacheObject val, long expireTime,
        GridCacheVersion ver, CacheObject old) throws IgniteCheckedException {
        // No-op: queries are disabled for near cache.
    }

    /** {@inheritDoc} */
    @Override protected void clearIndex(CacheObject val) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvccCandidate addLocal(
        long threadId,
        GridCacheVersion ver,
        AffinityTopologyVersion topVer,
        long timeout,
        boolean reenter,
        boolean tx,
        boolean implicitSingle,
        boolean read) throws GridCacheEntryRemovedException {
        return addNearLocal(
            null,
            threadId,
            ver,
            topVer,
            timeout,
            reenter,
            tx,
            implicitSingle,
            read
        );
    }

    /**
     * Add near local candidate.
     *
     * @param dhtNodeId DHT node ID.
     * @param threadId Owning thread ID.
     * @param ver Lock version.
     * @param topVer Topology version.
     * @param timeout Timeout to acquire lock.
     * @param reenter Reentry flag.
     * @param tx Transaction flag.
     * @param implicitSingle Implicit flag.
     * @param read Read lock flag.
     * @return New candidate.
     * @throws GridCacheEntryRemovedException If entry has been removed.
     */
    @Nullable GridCacheMvccCandidate addNearLocal(
        @Nullable UUID dhtNodeId,
        long threadId,
        GridCacheVersion ver,
        AffinityTopologyVersion topVer,
        long timeout,
        boolean reenter,
        boolean tx,
        boolean implicitSingle,
        boolean read)
        throws GridCacheEntryRemovedException {
        CacheLockCandidates prev;
        CacheLockCandidates owner = null;
        GridCacheMvccCandidate cand;

        CacheObject val;

        UUID locId = cctx.nodeId();

        synchronized (this) {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc == null) {
                mvcc = new GridCacheMvcc(cctx);

                mvccExtras(mvcc);
            }

            GridCacheMvccCandidate c = mvcc.localCandidate(locId, threadId);

            if (c != null)
                return reenter ? c.reenter() : null;

            prev = mvcc.allOwners();

            boolean emptyBefore = mvcc.isEmpty();

            // Lock could not be acquired.
            if (timeout < 0 && !emptyBefore)
                return null;

            // Local lock for near cache is a local lock.
            cand = mvcc.addNearLocal(
                this,
                locId,
                dhtNodeId,
                threadId,
                ver,
                tx,
                implicitSingle,
                read);

            cand.topologyVersion(topVer);

            boolean emptyAfter = mvcc.isEmpty();

            checkCallbacks(emptyBefore, emptyAfter);

            val = this.val;

            if (emptyAfter)
                mvccExtras(null);
            else
                owner = mvcc.allOwners();
        }

        // This call must be outside of synchronization.
        checkOwnerChanged(prev, owner, val);

        return cand;
    }

    /**
     * @param ver Version to set DHT node ID for.
     * @param dhtNodeId DHT node ID.
     * @return {@code true} if candidate was found.
     * @throws GridCacheEntryRemovedException If entry is removed.
     */
    @Nullable public synchronized GridCacheMvccCandidate dhtNodeId(GridCacheVersion ver, UUID dhtNodeId)
        throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc mvcc = mvccExtras();

        GridCacheMvccCandidate cand = mvcc == null ? null : mvcc.candidate(ver);

        if (cand == null)
            return null;

        cand.otherNodeId(dhtNodeId);

        return cand;
    }

    /**
     * Unlocks local lock.
     *
     * @return Removed candidate, or <tt>null</tt> if thread still holds the lock.
     */
    @Nullable @Override public GridCacheMvccCandidate removeLock() {
        CacheLockCandidates prev = null;
        CacheLockCandidates owner = null;

        CacheObject val;

        UUID locId = cctx.nodeId();

        GridCacheMvccCandidate cand = null;

        synchronized (this) {
            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc != null) {
                prev = mvcc.allOwners();

                boolean emptyBefore = mvcc.isEmpty();

                cand = mvcc.localCandidate(locId, Thread.currentThread().getId());

                assert cand == null || cand.nearLocal();

                if (cand != null && cand.owner()) {
                    // If a reentry, then release reentry. Otherwise, remove lock.
                    GridCacheMvccCandidate reentry = cand.unenter();

                    if (reentry != null) {
                        assert reentry.reentry();

                        return reentry;
                    }

                    mvcc.remove(cand.version());

                    owner = mvcc.allOwners();
                }
                else
                    return null;

                boolean emptyAfter = mvcc.isEmpty();

                checkCallbacks(emptyBefore, emptyAfter);

                if (emptyAfter)
                    mvccExtras(null);
            }

            val = this.val;
        }

        assert cand != null;
        assert owner != prev;

        if (log.isDebugEnabled())
            log.debug("Released local candidate from entry [owner=" + owner + ", prev=" + prev +
                ", entry=" + this + ']');

        cctx.mvcc().removeExplicitLock(cand);

        checkThreadChain(cand);

        // This call must be outside of synchronization.
        checkOwnerChanged(prev, owner, val);

        return cand;
    }

    /** {@inheritDoc} */
    @Override protected void onInvalidate() {
        topVer = AffinityTopologyVersion.NONE;
        dhtVer = null;
    }

    /**
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    synchronized void reserveEviction() throws GridCacheEntryRemovedException {
        checkObsolete();

        evictReservations++;
    }

    /**
     *
     */
    synchronized void releaseEviction() {
        assert evictReservations > 0 : this;
        assert !obsolete() : this;

        evictReservations--;
    }

    /** {@inheritDoc} */
    @Override protected boolean evictionDisabled() {
        assert Thread.holdsLock(this);

        return evictReservations > 0;
    }

    /**
     * @param nodeId Primary node ID.
     * @param topVer Topology version.
     */
    private void primaryNode(UUID nodeId, AffinityTopologyVersion topVer) {
        assert Thread.holdsLock(this);
        assert nodeId != null;

        ClusterNode primary = null;

        try {
            primary = cctx.affinity().primaryByPartition(part, topVer);
        }
        catch (IllegalStateException ignore) {
            // Do not have affinity history.
        }

        if (primary == null || !nodeId.equals(primary.id())) {
            this.topVer = AffinityTopologyVersion.NONE;

            return;
        }

        if (topVer.compareTo(this.topVer) > 0)
            this.topVer = topVer;
    }

    /** {@inheritDoc} */
    @Override public synchronized String toString() {
        return S.toString(GridNearCacheEntry.class, this, "super", super.toString());
    }
}
