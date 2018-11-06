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
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
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
@SuppressWarnings({"TooBroadScope"})
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
    @GridToStringExclude
    private final GridDhtLocalPartition locPart;

    /**
     * @param ctx Cache context.
     * @param topVer Topology version at the time of creation (if negative, then latest topology is assumed).
     * @param key Cache key.
     */
    public GridDhtCacheEntry(
        GridCacheContext ctx,
        AffinityTopologyVersion topVer,
        KeyCacheObject key
    ) {
        super(ctx, key);

        // Record this entry with partition.
        int p = cctx.affinity().partition(key);

        locPart = ctx.topology().localPartition(p, topVer, true, true);

        assert locPart != null : p;
    }

    /** {@inheritDoc} */
    @Override protected long nextPartitionCounter(AffinityTopologyVersion topVer,
        boolean primary,
        @Nullable Long primaryCntr) {
        return locPart.nextUpdateCounter(cctx.cacheId(), topVer, primary, primaryCntr);
    }

    /** {@inheritDoc} */
    @Override public int memorySize() throws IgniteCheckedException {
        int rdrsOverhead;

        lockEntry();

        try {
            rdrsOverhead = ReaderId.READER_ID_SIZE * rdrs.length;
        }
        finally {
            unlockEntry();
        }

        return super.memorySize() + DHT_SIZE_OVERHEAD + rdrsOverhead;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return locPart.id();
    }

    /** {@inheritDoc} */
    @Override protected GridDhtLocalPartition localPartition() {
        return locPart;
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
        assert !lockedByCurrentThread();

        // Remove this entry from partition mapping.
        cctx.topology().onRemoved(this);
    }

    /**
     * @param nearVer Near version.
     * @param rmv If {@code true}, then add to removed list if not found.
     * @return Local candidate by near version.
     * @throws GridCacheEntryRemovedException If removed.
     */
    @Nullable GridCacheMvccCandidate localCandidateByNearVersion(
        GridCacheVersion nearVer,
        boolean rmv
    ) throws GridCacheEntryRemovedException {
        lockEntry();

        try {
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
        finally {
            unlockEntry();
        }
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

        lockEntry();

        try {
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
        finally {
            unlockEntry();
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
    @Nullable public IgniteBiTuple<GridCacheVersion, CacheObject> versionedValue(
        AffinityTopologyVersion topVer)
        throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            if (isNew() || !valid(AffinityTopologyVersion.NONE) || deletedUnlocked())
                return null;
            else {
                CacheObject val0 = this.val;

                return F.t(ver, val0);
            }
        }
        finally {
            unlockEntry();
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

        lockEntry();

        try {
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
        finally {
            unlockEntry();
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
                                lockEntry();

                                try {
                                    // Release memory.
                                    reader0.resetTxFuture();
                                }
                                finally {
                                    unlockEntry();
                                }
                            }
                        });
                    }
                });
            }
            else {
                lockEntry();

                try {
                    // Release memory.
                    reader.resetTxFuture();
                }
                finally {
                    unlockEntry();
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
    public boolean removeReader(UUID nodeId, long msgId) throws GridCacheEntryRemovedException {
        lockEntry();

        try {
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
        finally {
            unlockEntry();
        }
    }

    /**
     * Clears all readers (usually when partition becomes invalid and ready for eviction).
     */
    @SuppressWarnings("unchecked")
    @Override public void clearReaders() {
        lockEntry();

        try {
            rdrs = ReaderId.EMPTY_ARRAY;
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override public void clearReader(UUID nodeId) throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            removeReader(nodeId, -1);
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * Marks entry as obsolete and, if possible or required, removes it
     * from swap storage.
     *
     * @param ver Obsolete version.
     * @return {@code True} if entry was not being used, passed the filter and could be removed.
     * @throws IgniteCheckedException If failed to remove from swap.
     */
    public boolean clearInternal(
        GridCacheVersion ver,
        GridCacheObsoleteEntryExtras extras
    ) throws IgniteCheckedException {
        boolean rmv = false;

        lockEntry();

        try {
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
                    ']');
            }

            removeValue();

            // Give to GC.
            update(null, 0L, 0L, ver, true);

            if (cctx.store().isLocal())
                cctx.store().remove(null, key);

            rmv = true;

            return true;
        }
        finally {
            unlockEntry();

            if (rmv)
                cctx.cache().removeEntry(this); // Clear cache.
        }
    }

    /**
     * @return Collection of readers after check.
     * @throws GridCacheEntryRemovedException If removed.
     */
    public Collection<ReaderId> checkReaders() throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            return checkReadersLocked();
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * @return Readers.
     */
    @Nullable public ReaderId[] readersLocked() {
        assert lockedByCurrentThread();

        return this.rdrs;
    }

    /**
     * @return Collection of readers after check.
     * @throws GridCacheEntryRemovedException If removed.
     */
    @SuppressWarnings({"unchecked", "ManualArrayToCollectionCopy"})
    protected Collection<ReaderId> checkReadersLocked() throws GridCacheEntryRemovedException {
        assert lockedByCurrentThread();

        checkObsolete();

        ReaderId[] rdrs = this.rdrs;

        if (rdrs.length == 0)
            return Collections.emptySet();

        List<ReaderId> newRdrs = null;

        for (int i = 0; i < rdrs.length; i++) {
            ClusterNode node = cctx.discovery().getAlive(rdrs[i].nodeId());

            if (node == null) {
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
    @Override protected boolean hasReaders() throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkReadersLocked();

            return rdrs.length > 0;
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * Sets mappings into entry.
     *
     * @param ver Version.
     * @return Candidate, if one existed for the version, or {@code null} if candidate was not found.
     * @throws GridCacheEntryRemovedException If removed.
     */
    @Nullable public GridCacheMvccCandidate mappings(
        GridCacheVersion ver,
        Collection<ClusterNode> dhtNodeIds,
        Collection<ClusterNode> nearNodeIds
    ) throws GridCacheEntryRemovedException {
        lockEntry();

        try {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            GridCacheMvccCandidate cand = mvcc == null ? null : mvcc.candidate(ver);

            if (cand != null)
                cand.mappedNodeIds(dhtNodeIds, nearNodeIds);

            return cand;
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * @param ver Version.
     * @param mappedNode Mapped node to remove.
     */
    public void removeMapping(GridCacheVersion ver, ClusterNode mappedNode) {
        lockEntry();

        try {
            GridCacheMvcc mvcc = mvccExtras();

            GridCacheMvccCandidate cand = mvcc == null ? null : mvcc.candidate(ver);

            if (cand != null)
                cand.removeMappedNode(mappedNode);
        }
        finally {
            unlockEntry();
        }
    }

    /**
     * @return Cache name.
     */
    protected final String cacheName() {
        return cctx.name();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        lockEntry();

        try {
            return S.toString(GridDhtCacheEntry.class, this,
                "part", locPart.id(),
                "super", super.toString());
        }
        finally {
            unlockEntry();
        }
    }

    /** {@inheritDoc} */
    @Override protected void incrementMapPublicSize() {
        locPart.incrementPublicSize(null, this);
    }

    /** {@inheritDoc} */
    @Override protected void decrementMapPublicSize() {
        locPart.decrementPublicSize(null, this);
    }

    /**
     * Reader ID.
     */
    public static class ReaderId {
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
         * @param readers Readers array.
         * @param nodeId Node ID to check.
         * @return {@code True} if node ID found in readers array.
         */
        public static boolean contains(@Nullable ReaderId[] readers, UUID nodeId) {
            if (readers == null)
                return false;

            for (int i = 0; i < readers.length; i++) {
                if (nodeId.equals(readers[i].nodeId))
                    return true;
            }

            return false;
        }

        /**
         * @return Node ID.
         */
        public UUID nodeId() {
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
