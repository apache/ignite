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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheLockCandidates;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMultiTxFuture;
import org.apache.ignite.internal.processors.cache.GridCacheMvcc;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockCancelledException;
import org.apache.ignite.internal.processors.cache.extras.GridCacheObsoleteEntryExtras;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Replicated cache entry.
 */
@SuppressWarnings({"TooBroadScope", "NonPrivateFieldAccessedInSynchronizedContext"})
public class GridDhtCacheEntry extends GridDistributedCacheEntry {
    /** Size overhead. */
    private static final int DHT_SIZE_OVERHEAD = 16;

    /** Gets node value from reader ID. */
    private static final IgniteClosure<ReaderId, UUID> R2N = new C1<ReaderId, UUID>() {
        @Override public UUID apply(ReaderId e) {
            return e.nodeId();
        }
    };

    /** Reader clients. */
    @GridToStringInclude
    private volatile ReaderId[] rdrs = ReaderId.EMPTY_ARRAY;

    /** Local partition. */
    private final GridDhtLocalPartition locPart;

    /**
     * @param ctx Cache context.
     * @param topVer Topology version at the time of creation (if negative, then latest topology is assumed).
     * @param key Cache key.
     * @param hash Key hash value.
     * @param val Entry value.
     */
    public GridDhtCacheEntry(
        GridCacheContext ctx,
        AffinityTopologyVersion topVer,
        KeyCacheObject key,
        int hash,
        CacheObject val
    ) {
        super(ctx, key, hash, val);

        // Record this entry with partition.
        int p = cctx.affinity().partition(key);

        locPart = ctx.topology().localPartition(p, topVer, true);

        assert locPart != null;
    }

    /** {@inheritDoc} */
    @Override public int memorySize() throws IgniteCheckedException {
        int rdrsOverhead;

        synchronized (this) {
            rdrsOverhead = ReaderId.READER_ID_SIZE * rdrs.length;
        }

        return super.memorySize() + DHT_SIZE_OVERHEAD + rdrsOverhead;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return locPart.id();
    }

    /** {@inheritDoc} */
    @Override public boolean isDht() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean partitionValid() {
        return locPart.valid();
    }

    /** {@inheritDoc} */
    @Override public void onMarkedObsolete() {
        assert !Thread.holdsLock(this);

        // Remove this entry from partition mapping.
        cctx.dht().topology().onRemoved(this);
    }

    /**
     * @param nearVer Near version.
     * @param rmv If {@code true}, then add to removed list if not found.
     * @return Local candidate by near version.
     * @throws GridCacheEntryRemovedException If removed.
     */
    @Nullable synchronized GridCacheMvccCandidate localCandidateByNearVersion(GridCacheVersion nearVer,
        boolean rmv) throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc mvcc = mvccExtras();

        if (mvcc != null) {
            for (GridCacheMvccCandidate c : mvcc.localCandidatesNoCopy(false)) {
                GridCacheVersion ver = c.otherVersion();

                if (ver != null && ver.equals(nearVer))
                    return c;
            }
        }

        if (rmv)
            addRemoved(nearVer);

        return null;
    }

    /**
     * Add local candidate.
     *
     * @param nearNodeId Near node ID.
     * @param nearVer Near version.
     * @param topVer Topology version.
     * @param threadId Owning thread ID.
     * @param ver Lock version.
     * @param serOrder Version for serializable transactions ordering.
     * @param timeout Timeout to acquire lock.
     * @param reenter Reentry flag.
     * @param tx Tx flag.
     * @param implicitSingle Implicit flag.
     * @param read Read lock flag.
     * @return New candidate.
     * @throws GridCacheEntryRemovedException If entry has been removed.
     * @throws GridDistributedLockCancelledException If lock was cancelled.
     */
    @Nullable GridCacheMvccCandidate addDhtLocal(
        UUID nearNodeId,
        GridCacheVersion nearVer,
        AffinityTopologyVersion topVer,
        long threadId,
        GridCacheVersion ver,
        @Nullable GridCacheVersion serOrder,
        long timeout,
        boolean reenter,
        boolean tx,
        boolean implicitSingle,
        boolean read)
        throws GridCacheEntryRemovedException, GridDistributedLockCancelledException {
        assert !reenter || serOrder == null;

        GridCacheMvccCandidate cand;
        CacheLockCandidates prev;
        CacheLockCandidates owner;

        CacheObject val;

        synchronized (this) {
            // Check removed locks prior to obsolete flag.
            checkRemoved(ver);
            checkRemoved(nearVer);

            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc == null) {
                mvcc = new GridCacheMvcc(cctx);

                mvccExtras(mvcc);
            }

            prev = mvcc.allOwners();

            boolean emptyBefore = mvcc.isEmpty();

            cand = mvcc.addLocal(
                this,
                nearNodeId,
                nearVer,
                threadId,
                ver,
                timeout,
                serOrder,
                reenter,
                tx,
                implicitSingle,
                /*dht-local*/true,
                read
            );

            if (cand == null)
                return null;

            cand.topologyVersion(topVer);

            owner = mvcc.allOwners();

            if (owner != null)
                cand.ownerVersion(owner.candidate(0).version());

            boolean emptyAfter = mvcc.isEmpty();

            checkCallbacks(emptyBefore, emptyAfter);

            val = this.val;

            if (mvcc.isEmpty())
                mvccExtras(null);
        }

        // Don't link reentries.
        if (!cand.reentry())
            // Link with other candidates in the same thread.
            cctx.mvcc().addNext(cctx, cand);

        checkOwnerChanged(prev, owner, val);

        return cand;
    }

    /** {@inheritDoc} */
    @Override public boolean tmLock(IgniteInternalTx tx,
        long timeout,
        @Nullable GridCacheVersion serOrder,
        GridCacheVersion serReadVer,
        boolean read
    ) throws GridCacheEntryRemovedException, GridDistributedLockCancelledException {
        if (tx.local()) {
            GridDhtTxLocalAdapter dhtTx = (GridDhtTxLocalAdapter)tx;

            // Null is returned if timeout is negative and there is other lock owner.
            return addDhtLocal(
                dhtTx.nearNodeId(),
                dhtTx.nearXidVersion(),
                tx.topologyVersion(),
                tx.threadId(),
                tx.xidVersion(),
                serOrder,
                timeout,
                /*reenter*/false,
                /*tx*/true,
                tx.implicitSingle(),
                read) != null;
        }

        try {
            addRemote(
                tx.nodeId(),
                tx.otherNodeId(),
                tx.threadId(),
                tx.xidVersion(),
                /*tx*/true,
                tx.implicit(),
                null);

            return true;
        }
        catch (GridDistributedLockCancelledException ignored) {
            if (log.isDebugEnabled())
                log.debug("Attempted to enter tx lock for cancelled ID (will ignore): " + tx);

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvccCandidate removeLock() {
        GridCacheMvccCandidate ret = super.removeLock();

        locPart.onUnlock();

        return ret;
    }

    /** {@inheritDoc} */
    @Override public boolean removeLock(GridCacheVersion ver) throws GridCacheEntryRemovedException {
        boolean ret = super.removeLock(ver);

        locPart.onUnlock();

        return ret;
    }

    /** {@inheritDoc} */
    @Override public void onUnlock() {
        locPart.onUnlock();
    }

    /**
     * @param topVer Topology version.
     * @return Tuple with version and value of this entry, or {@code null} if entry is new.
     * @throws GridCacheEntryRemovedException If entry has been removed.
     */
    @SuppressWarnings({"NonPrivateFieldAccessedInSynchronizedContext"})
    @Nullable public synchronized IgniteBiTuple<GridCacheVersion, CacheObject> versionedValue(
        AffinityTopologyVersion topVer)
        throws GridCacheEntryRemovedException {
        if (isNew() || !valid(AffinityTopologyVersion.NONE) || deletedUnlocked())
            return null;
        else {
            CacheObject val0 = valueBytesUnlocked();

            return F.t(ver, val0);
        }
    }

    /**
     * @return Readers.
     * @throws GridCacheEntryRemovedException If removed.
     */
    public Collection<UUID> readers() throws GridCacheEntryRemovedException {
        return F.viewReadOnly(checkReaders(), R2N);
    }

    /**
     * @param nodeId Node ID.
     * @return reader ID.
     */
    @Nullable public ReaderId readerId(UUID nodeId) {
        ReaderId[] rdrs = this.rdrs;

        for (ReaderId reader : rdrs) {
            if (reader.nodeId().equals(nodeId))
                return reader;
        }

        return null;
    }

    /**
     * @param nodeId Reader to add.
     * @param msgId Message ID.
     * @param topVer Topology version.
     * @return Future for all relevant transactions that were active at the time of adding reader,
     *      or {@code null} if reader was added
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    @SuppressWarnings("unchecked")
    @Nullable public IgniteInternalFuture<Boolean> addReader(UUID nodeId, long msgId, AffinityTopologyVersion topVer)
        throws GridCacheEntryRemovedException {
        // Don't add local node as reader.
        if (cctx.nodeId().equals(nodeId))
            return null;

        ClusterNode node = cctx.discovery().node(nodeId);

        if (node == null) {
            if (log.isDebugEnabled())
                log.debug("Ignoring near reader because node left the grid: " + nodeId);

            return null;
        }

        // If remote node has no near cache, don't add it.
        if (!cctx.discovery().cacheNearNode(node, cacheName())) {
            if (log.isDebugEnabled())
                log.debug("Ignoring near reader because near cache is disabled: " + nodeId);

            return null;
        }

        // If remote node is (primary?) or back up, don't add it as a reader.
        if (cctx.affinity().partitionBelongs(node, partition(), topVer)) {
            if (log.isDebugEnabled())
                log.debug("Ignoring near reader because remote node is affinity node [locNodeId=" + cctx.localNodeId()
                    + ", rmtNodeId=" + nodeId + ", key=" + key + ']');

            return null;
        }

        boolean ret = false;

        GridCacheMultiTxFuture txFut = null;

        Collection<GridCacheMvccCandidate> cands = null;

        ReaderId reader;

        synchronized (this) {
            checkObsolete();

            reader = readerId(nodeId);

            if (reader == null) {
                reader = new ReaderId(nodeId, msgId);

                ReaderId[] rdrs = Arrays.copyOf(this.rdrs, this.rdrs.length + 1);

                rdrs[rdrs.length - 1] = reader;

                // Seal.
                this.rdrs = rdrs;

                // No transactions in ATOMIC cache.
                if (!cctx.atomic()) {
                    txFut = reader.getOrCreateTxFuture(cctx);

                    cands = localCandidates();

                    ret = true;
                }
            }
            else {
                txFut = reader.txFuture();

                long id = reader.messageId();

                if (id < msgId)
                    reader.messageId(msgId);
            }
        }

        if (ret) {
            assert txFut != null;

            if (!F.isEmpty(cands)) {
                for (GridCacheMvccCandidate c : cands) {
                    IgniteInternalTx tx = cctx.tm().tx(c.version());

                    if (tx != null && tx.local())
                        txFut.addTx(tx);
                }
            }

            txFut.init();

            if (!txFut.isDone()) {
                final ReaderId reader0 = reader;

                txFut.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> f) {
                        cctx.kernalContext().closure().runLocalSafe(new GridPlainRunnable() {
                            @Override public void run() {
                                synchronized (this) {
                                    // Release memory.
                                    reader0.resetTxFuture();
                                }
                            }
                        });
                    }
                });
            }
            else {
                synchronized (this) {
                    // Release memory.
                    reader.resetTxFuture();
                }

                txFut = null;
            }
        }

        return txFut;
    }

    /**
     * @param nodeId Reader to remove.
     * @param msgId Message ID.
     * @return {@code True} if reader was removed as a result of this operation.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    @SuppressWarnings("unchecked")
    public synchronized boolean removeReader(UUID nodeId, long msgId) throws GridCacheEntryRemovedException {
        checkObsolete();

        ReaderId[] rdrs = this.rdrs;

        int readerIdx = -1;

        for (int i = 0; i < rdrs.length; i++) {
            if (rdrs[i].nodeId().equals(nodeId)) {
                readerIdx = i;

                break;
            }
        }

        if (readerIdx == -1 || (rdrs[readerIdx].messageId() > msgId && msgId >= 0))
            return false;

        if (rdrs.length == 1)
            this.rdrs = ReaderId.EMPTY_ARRAY;
        else {
            ReaderId[] newRdrs = Arrays.copyOf(rdrs, rdrs.length - 1);

            System.arraycopy(rdrs, readerIdx + 1, newRdrs, readerIdx, rdrs.length - readerIdx - 1);

            // Seal.
            this.rdrs = newRdrs;
        }

        return true;
    }

    /**
     * Clears all readers (usually when partition becomes invalid and ready for eviction).
     */
    @SuppressWarnings("unchecked")
    @Override public synchronized void clearReaders() {
        rdrs = ReaderId.EMPTY_ARRAY;
    }

    /** {@inheritDoc} */
    @Override public synchronized void clearReader(UUID nodeId) throws GridCacheEntryRemovedException {
        removeReader(nodeId, -1);
    }

    /**
     * Marks entry as obsolete and, if possible or required, removes it
     * from swap storage.
     *
     * @param ver Obsolete version.
     * @param swap If {@code true} then remove from swap.
     * @return {@code True} if entry was not being used, passed the filter and could be removed.
     * @throws IgniteCheckedException If failed to remove from swap.
     */
    public boolean clearInternal(
        GridCacheVersion ver,
        boolean swap,
        GridCacheObsoleteEntryExtras extras
    ) throws IgniteCheckedException {
        boolean rmv = false;

        try {
            synchronized (this) {
                CacheObject prev = saveValueForIndexUnlocked();

                // Call markObsolete0 to avoid recursive calls to clear if
                // we are clearing dht local partition (onMarkedObsolete should not be called).
                if (!markObsolete0(ver, false, extras)) {
                    if (log.isDebugEnabled())
                        log.debug("Entry could not be marked obsolete (it is still used or has readers): " + this);

                    return false;
                }

                rdrs = ReaderId.EMPTY_ARRAY;

                if (log.isDebugEnabled())
                    log.debug("Entry has been marked obsolete: " + this);

                if (log.isTraceEnabled()) {
                    log.trace("clearInternal [key=" + key +
                        ", entry=" + System.identityHashCode(this) +
                        ", prev=" + prev +
                        ", ptr=" + offHeapPointer() +
                        ']');
                }

                clearIndex(prev);

                // Give to GC.
                update(null, 0L, 0L, ver, true);

                if (swap) {
                    releaseSwap();

                    if (log.isDebugEnabled())
                        log.debug("Entry has been cleared from swap storage: " + this);
                }

                if (cctx.store().isLocal())
                    cctx.store().remove(null, key);

                rmv = true;

                return true;
            }
        }
        finally {
            if (rmv)
                cctx.cache().removeEntry(this); // Clear cache.
        }
    }

    /**
     * @return Collection of readers after check.
     * @throws GridCacheEntryRemovedException If removed.
     */
    public synchronized Collection<ReaderId> checkReaders() throws GridCacheEntryRemovedException {
        return checkReadersLocked();
    }

    /**
     * @return Collection of readers after check.
     * @throws GridCacheEntryRemovedException If removed.
     */
    @SuppressWarnings({"unchecked", "ManualArrayToCollectionCopy"})
    protected Collection<ReaderId> checkReadersLocked() throws GridCacheEntryRemovedException {
        assert Thread.holdsLock(this);

        checkObsolete();

        ReaderId[] rdrs = this.rdrs;

        if (rdrs.length == 0)
            return Collections.emptySet();

        List<ReaderId> newRdrs = null;

        for (int i = 0; i < rdrs.length; i++) {
            ClusterNode node = cctx.discovery().getAlive(rdrs[i].nodeId());

            if (node == null || !cctx.discovery().cacheNode(node, cacheName())) {
                // Node has left and if new list has already been created, just skip.
                // Otherwise, create new list and add alive nodes.
                if (newRdrs == null) {
                    newRdrs = new ArrayList<>(rdrs.length);

                    for (int k = 0; k < i; k++)
                        newRdrs.add(rdrs[k]);
                }
            }
            // If node is still alive and no failed nodes
            // found yet, simply go to next iteration.
            else if (newRdrs != null)
                // Some of the nodes has left. Add to list.
                newRdrs.add(rdrs[i]);
        }

        if (newRdrs != null) {
            rdrs = newRdrs.toArray(new ReaderId[newRdrs.size()]);

            this.rdrs = rdrs;
        }

        return Arrays.asList(rdrs);
    }

    /** {@inheritDoc} */
    @Override protected synchronized boolean hasReaders() throws GridCacheEntryRemovedException {
        checkReadersLocked();

        return rdrs.length > 0;
    }

    /**
     * Sets mappings into entry.
     *
     * @param ver Version.
     * @return Candidate, if one existed for the version, or {@code null} if candidate was not found.
     * @throws GridCacheEntryRemovedException If removed.
     */
    @Nullable public synchronized GridCacheMvccCandidate mappings(
        GridCacheVersion ver,
        Collection<ClusterNode> dhtNodeIds,
        Collection<ClusterNode> nearNodeIds
    ) throws GridCacheEntryRemovedException {
        checkObsolete();

        GridCacheMvcc mvcc = mvccExtras();

        GridCacheMvccCandidate cand = mvcc == null ? null : mvcc.candidate(ver);

        if (cand != null)
            cand.mappedNodeIds(dhtNodeIds, nearNodeIds);

        return cand;
    }

    /**
     * @param ver Version.
     * @param mappedNode Mapped node to remove.
     */
    public synchronized void removeMapping(GridCacheVersion ver, ClusterNode mappedNode) {
        GridCacheMvcc mvcc = mvccExtras();

        GridCacheMvccCandidate cand = mvcc == null ? null : mvcc.candidate(ver);

        if (cand != null)
            cand.removeMappedNode(mappedNode);
    }

    /**
     * @return Cache name.
     */
    protected String cacheName() {
        return cctx.dht().near().name();
    }

    /** {@inheritDoc} */
    @Override public synchronized String toString() {
        return S.toString(GridDhtCacheEntry.class, this, "super", super.toString());
    }

    /** {@inheritDoc} */
    @Override protected void incrementMapPublicSize() {
        locPart.incrementPublicSize(this);
    }

    /** {@inheritDoc} */
    @Override protected void decrementMapPublicSize() {
        locPart.decrementPublicSize(this);
    }

    /**
     * Reader ID.
     */
    private static class ReaderId {
        /** */
        private static final ReaderId[] EMPTY_ARRAY = new ReaderId[0];

        /** Reader ID size. */
        private static final int READER_ID_SIZE = 24;

        /** Node ID. */
        private UUID nodeId;

        /** Message ID. */
        private long msgId;

        /** Transaction future. */
        private GridCacheMultiTxFuture txFut;

        /**
         * @param nodeId Node ID.
         * @param msgId Message ID.
         */
        ReaderId(UUID nodeId, long msgId) {
            this.nodeId = nodeId;
            this.msgId = msgId;
        }

        /**
         * @return Node ID.
         */
        UUID nodeId() {
            return nodeId;
        }

        /**
         * @return Message ID.
         */
        long messageId() {
            return msgId;
        }

        /**
         * @param msgId Message ID.
         */
        void messageId(long msgId) {
            this.msgId = msgId;
        }

        /**
         * @param cctx Cache context.
         * @return Transaction future.
         */
        GridCacheMultiTxFuture getOrCreateTxFuture(GridCacheContext cctx) {
            if (txFut == null)
                txFut = new GridCacheMultiTxFuture<>(cctx);

            return txFut;
        }

        /**
         * @return Transaction future.
         */
        GridCacheMultiTxFuture txFuture() {
            return txFut;
        }

        /**
         * Sets multi-transaction future to {@code null}.
         *
         * @return Previous transaction future.
         */
        GridCacheMultiTxFuture resetTxFuture() {
            GridCacheMultiTxFuture txFut = this.txFut;

            this.txFut = null;

            return txFut;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof ReaderId))
                return false;

            ReaderId readerId = (ReaderId)o;

            return msgId == readerId.msgId && nodeId.equals(readerId.nodeId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = nodeId.hashCode();

            res = 31 * res + (int)(msgId ^ (msgId >>> 32));

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ReaderId.class, this);
        }
    }
}
