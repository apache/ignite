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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheMvcc;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_LOCKED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_UNLOCKED;

/**
 * Entry for distributed (replicated/partitioned) cache.
 */
@SuppressWarnings({"NonPrivateFieldAccessedInSynchronizedContext", "TooBroadScope"})
public class GridDistributedCacheEntry extends GridCacheMapEntry {
    /** Remote candidates snapshot. */
    private volatile List<GridCacheMvccCandidate> rmts = Collections.emptyList();

    /**
     * @param ctx Cache context.
     * @param key Cache key.
     * @param hash Key hash value.
     * @param val Entry value.
     * @param next Next entry in the linked list.
     * @param hdrId Cache map header ID.
     */
    public GridDistributedCacheEntry(GridCacheContext ctx,
        KeyCacheObject key,
        int hash,
        CacheObject val,
        GridCacheMapEntry next,
        int hdrId)
    {
        super(ctx, key, hash, val, next, hdrId);
    }

    /**
     *
     */
    protected void refreshRemotes() {
        GridCacheMvcc mvcc = mvccExtras();

        rmts = mvcc == null ? Collections.<GridCacheMvccCandidate>emptyList() : mvcc.remoteCandidates();
    }

    /**
     * Add local candidate.
     *
     * @param threadId Owning thread ID.
     * @param ver Lock version.
     * @param topVer Topology version.
     * @param timeout Timeout to acquire lock.
     * @param reenter Reentry flag.
     * @param tx Transaction flag.
     * @param implicitSingle Implicit flag.
     * @return New candidate.
     * @throws GridCacheEntryRemovedException If entry has been removed.
     */
    @Nullable public GridCacheMvccCandidate addLocal(
        long threadId,
        GridCacheVersion ver,
        AffinityTopologyVersion topVer,
        long timeout,
        boolean reenter,
        boolean tx,
        boolean implicitSingle) throws GridCacheEntryRemovedException {
        GridCacheMvccCandidate cand;
        GridCacheMvccCandidate prev;
        GridCacheMvccCandidate owner;

        CacheObject val;

        synchronized (this) {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc == null) {
                mvcc = new GridCacheMvcc(cctx);

                mvccExtras(mvcc);
            }

            prev = mvcc.anyOwner();

            boolean emptyBefore = mvcc.isEmpty();

            cand = mvcc.addLocal(this, threadId, ver, timeout, reenter, tx, implicitSingle);

            if (cand != null)
                cand.topologyVersion(topVer);

            owner = mvcc.anyOwner();

            boolean emptyAfter = mvcc.isEmpty();

            checkCallbacks(emptyBefore, emptyAfter);

            val = this.val;

            if (emptyAfter)
                mvccExtras(null);
        }

        // Don't link reentries.
        if (cand != null && !cand.reentry())
            // Link with other candidates in the same thread.
            cctx.mvcc().addNext(cctx, cand);

        checkOwnerChanged(prev, owner, val);

        return cand;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheMvccCandidate> remoteMvccSnapshot(GridCacheVersion... exclude) {
        Collection<GridCacheMvccCandidate> rmts = this.rmts;

        if (rmts.isEmpty() || F.isEmpty(exclude))
            return rmts;

        Collection<GridCacheMvccCandidate> cands = new ArrayList<>(rmts.size());

        for (GridCacheMvccCandidate c : rmts) {
            assert !c.reentry();

            // Don't include reentries.
            if (!U.containsObjectArray(exclude, c.version()))
                cands.add(c);
        }

        return cands;
    }

    /**
     * Adds new lock candidate.
     *
     * @param nodeId Node ID.
     * @param otherNodeId Other node ID.
     * @param threadId Thread ID.
     * @param ver Lock version.
     * @param timeout Lock acquire timeout.
     * @param tx Transaction flag.
     * @param implicitSingle Implicit flag.
     * @param owned Owned candidate version.
     * @throws GridDistributedLockCancelledException If lock has been canceled.
     * @throws GridCacheEntryRemovedException If this entry is obsolete.
     */
    public void addRemote(
        UUID nodeId,
        @Nullable UUID otherNodeId,
        long threadId,
        GridCacheVersion ver,
        long timeout,
        boolean tx,
        boolean implicitSingle,
        @Nullable GridCacheVersion owned
    ) throws GridDistributedLockCancelledException, GridCacheEntryRemovedException {
        GridCacheMvccCandidate prev;
        GridCacheMvccCandidate owner;

        CacheObject val;

        synchronized (this) {
            // Check removed locks prior to obsolete flag.
            checkRemoved(ver);

            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc == null) {
                mvcc = new GridCacheMvcc(cctx);

                mvccExtras(mvcc);
            }

            prev = mvcc.anyOwner();

            boolean emptyBefore = mvcc.isEmpty();

            mvcc.addRemote(
                this,
                nodeId,
                otherNodeId,
                threadId,
                ver,
                timeout,
                tx,
                implicitSingle,
                /*near-local*/false
            );

            if (owned != null)
                mvcc.markOwned(ver, owned);

            owner = mvcc.anyOwner();

            boolean emptyAfter = mvcc.isEmpty();

            checkCallbacks(emptyBefore, emptyAfter);

            val = this.val;

            refreshRemotes();

            if (emptyAfter)
                mvccExtras(null);
        }

        // This call must be outside of synchronization.
        checkOwnerChanged(prev, owner, val);
    }

    /**
     * Adds new lock candidate.
     *
     * @param cand Remote lock candidate.
     * @throws GridDistributedLockCancelledException If lock has been canceled.
     * @throws GridCacheEntryRemovedException If this entry is obsolete.
     */
    public void addRemote(GridCacheMvccCandidate cand) throws GridDistributedLockCancelledException,
        GridCacheEntryRemovedException {

        CacheObject val;

        GridCacheMvccCandidate prev;
        GridCacheMvccCandidate owner;

        synchronized (this) {
            cand.parent(this);

            // Check removed locks prior to obsolete flag.
            checkRemoved(cand.version());

            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc == null) {
                mvcc = new GridCacheMvcc(cctx);

                mvccExtras(mvcc);
            }

            boolean emptyBefore = mvcc.isEmpty();

            prev = mvcc.anyOwner();

            mvcc.addRemote(cand);

            owner = mvcc.anyOwner();

            boolean emptyAfter = mvcc.isEmpty();

            checkCallbacks(emptyBefore, emptyAfter);

            val = this.val;

            refreshRemotes();

            if (emptyAfter)
                mvccExtras(null);
        }

        // This call must be outside of synchronization.
        checkOwnerChanged(prev, owner, val);
    }

    /**
     * Removes all lock candidates for node.
     *
     * @param nodeId ID of node to remove locks from.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    public void removeExplicitNodeLocks(UUID nodeId) throws GridCacheEntryRemovedException {
        GridCacheMvccCandidate prev = null;
        GridCacheMvccCandidate owner = null;

        CacheObject val = null;

        synchronized (this) {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc != null) {
                prev = mvcc.anyOwner();

                boolean emptyBefore = mvcc.isEmpty();

                owner = mvcc.removeExplicitNodeCandidates(nodeId);

                boolean emptyAfter = mvcc.isEmpty();

                checkCallbacks(emptyBefore, emptyAfter);

                val = this.val;

                refreshRemotes();

                if (emptyAfter) {
                    mvccExtras(null);

                    onUnlock();
                }
            }
        }

        // This call must be outside of synchronization.
        checkOwnerChanged(prev, owner, val);
    }

    /**
     * Unlocks local lock.
     *
     * @return Removed candidate, or <tt>null</tt> if thread still holds the lock.
     */
    @Nullable public GridCacheMvccCandidate removeLock() {
        GridCacheMvccCandidate prev = null;
        GridCacheMvccCandidate owner = null;

        CacheObject val;

        synchronized (this) {
            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc != null) {
                prev = mvcc.anyOwner();

                boolean emptyBefore = mvcc.isEmpty();

                owner = mvcc.releaseLocal();

                boolean emptyAfter = mvcc.isEmpty();

                checkCallbacks(emptyBefore, emptyAfter);

                if (emptyAfter)
                    mvccExtras(null);
            }

            val = this.val;
        }

        if (log.isDebugEnabled())
            log.debug("Released local candidate from entry [owner=" + owner + ", prev=" + prev +
                ", entry=" + this + ']');

        if (prev != null && owner != prev)
            checkThreadChain(prev);

        // This call must be outside of synchronization.
        checkOwnerChanged(prev, owner, val);

        return owner != prev ? prev : null;
    }

    /** {@inheritDoc} */
    @Override public boolean removeLock(GridCacheVersion ver) throws GridCacheEntryRemovedException {
        GridCacheMvccCandidate prev = null;
        GridCacheMvccCandidate owner = null;

        GridCacheMvccCandidate doomed;

        CacheObject val;

        synchronized (this) {
            GridCacheMvcc mvcc = mvccExtras();

            doomed = mvcc == null ? null : mvcc.candidate(ver);

            if (doomed == null || doomed.dhtLocal() || (!doomed.local() && !doomed.nearLocal()))
                addRemoved(ver);

            GridCacheVersion obsoleteVer = obsoleteVersionExtras();

            if (obsoleteVer != null && !obsoleteVer.equals(ver))
                checkObsolete();

            if (doomed != null) {
                assert mvcc != null;

                prev = mvcc.anyOwner();

                boolean emptyBefore = mvcc.isEmpty();

                owner = mvcc.remove(doomed.version());

                boolean emptyAfter = mvcc.isEmpty();

                if (!doomed.local())
                    refreshRemotes();

                checkCallbacks(emptyBefore, emptyAfter);

                if (emptyAfter)
                    mvccExtras(null);
            }

            val = this.val;
        }

        if (log.isDebugEnabled())
            log.debug("Removed lock candidate from entry [doomed=" + doomed + ", owner=" + owner + ", prev=" + prev +
                ", entry=" + this + ']');

        if (doomed != null && doomed.nearLocal())
            cctx.mvcc().removeExplicitLock(doomed);

        if (doomed != null)
            checkThreadChain(doomed);

        // This call must be outside of synchronization.
        checkOwnerChanged(prev, owner, val);

        return doomed != null;
    }

    /**
     *
     * @param ver Lock version.
     * @throws GridDistributedLockCancelledException If lock is cancelled.
     */
    protected void checkRemoved(GridCacheVersion ver) throws GridDistributedLockCancelledException {
        assert Thread.holdsLock(this);

        GridCacheVersion obsoleteVer = obsoleteVersionExtras();

        if ((obsoleteVer != null && obsoleteVer.equals(ver)) || cctx.mvcc().isRemoved(cctx, ver))
            throw new GridDistributedLockCancelledException("Lock has been cancelled [key=" + key +
                ", ver=" + ver + ']');
    }

    /**
     * @param ver Lock version.
     * @return {@code True} if removed.
     */
    public boolean addRemoved(GridCacheVersion ver) {
        assert Thread.holdsLock(this);

        return cctx.mvcc().addRemoved(cctx, ver);
    }

    /**
     *
     * @param ver Version of candidate to acquire lock for.
     * @return Owner.
     * @throws GridCacheEntryRemovedException If entry is removed.
     */
    @Nullable public GridCacheMvccCandidate readyLock(GridCacheVersion ver)
        throws GridCacheEntryRemovedException {
        GridCacheMvccCandidate prev = null;
        GridCacheMvccCandidate owner = null;

        CacheObject val;

        synchronized (this) {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc != null) {
                prev = mvcc.anyOwner();

                boolean emptyBefore = mvcc.isEmpty();

                owner = mvcc.readyLocal(ver);

                assert owner == null || owner.owner() : "Owner flag not set for owner: " + owner;

                boolean emptyAfter = mvcc.isEmpty();

                checkCallbacks(emptyBefore, emptyAfter);

                if (emptyAfter)
                    mvccExtras(null);
            }

            val = this.val;
        }

        // This call must be made outside of synchronization.
        checkOwnerChanged(prev, owner, val);

        return owner;
    }

    /**
     * Notifies mvcc that near local lock is ready to be acquired.
     *
     * @param ver Lock version.
     * @param mapped Mapped dht lock version.
     * @param committed Committed versions.
     * @param rolledBack Rolled back versions.
     * @param pending Pending locks on dht node with version less then mapped.
     * @return Current lock owner.
     *
     * @throws GridCacheEntryRemovedException If entry is removed.
     */
    @Nullable public GridCacheMvccCandidate readyNearLock(GridCacheVersion ver, GridCacheVersion mapped,
        Collection<GridCacheVersion> committed,
        Collection<GridCacheVersion> rolledBack,
        Collection<GridCacheVersion> pending) throws GridCacheEntryRemovedException {
        GridCacheMvccCandidate prev = null;
        GridCacheMvccCandidate owner = null;

        CacheObject val;

        synchronized (this) {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc != null) {
                prev = mvcc.anyOwner();

                boolean emptyBefore = mvcc.isEmpty();

                owner = mvcc.readyNearLocal(ver, mapped, committed, rolledBack, pending);

                assert owner == null || owner.owner() : "Owner flag is not set for owner: " + owner;

                boolean emptyAfter = mvcc.isEmpty();

                checkCallbacks(emptyBefore, emptyAfter);

                if (emptyAfter)
                    mvccExtras(null);
            }

            val = this.val;
        }

        // This call must be made outside of synchronization.
        checkOwnerChanged(prev, owner, val);

        return owner;
    }

    /**
     * Reorders completed versions.
     *
     * @param baseVer Base version for reordering.
     * @param committedVers Completed versions.
     * @param rolledbackVers Rolled back versions.
     * @throws GridCacheEntryRemovedException If entry has been removed.
     */
    public void orderCompleted(GridCacheVersion baseVer, Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers)
        throws GridCacheEntryRemovedException {
        if (!F.isEmpty(committedVers) || !F.isEmpty(rolledbackVers)) {
            GridCacheMvccCandidate prev = null;
            GridCacheMvccCandidate owner = null;

            CacheObject val;

            synchronized (this) {
                checkObsolete();

                GridCacheMvcc mvcc = mvccExtras();

                if (mvcc != null) {
                    prev = mvcc.anyOwner();

                    boolean emptyBefore = mvcc.isEmpty();

                    owner = mvcc.orderCompleted(baseVer, committedVers, rolledbackVers);

                    boolean emptyAfter = mvcc.isEmpty();

                    checkCallbacks(emptyBefore, emptyAfter);

                    if (emptyAfter)
                        mvccExtras(null);
                }

                val = this.val;
            }

            // This call must be made outside of synchronization.
            checkOwnerChanged(prev, owner, val);
        }
    }

    /**
     *
     * @param lockVer Done version.
     * @param baseVer Base version.
     * @param committedVers Completed versions for reordering.
     * @param rolledbackVers Rolled back versions for reordering.
     * @param sysInvalidate Flag indicating if this entry is done from invalidated transaction (in case of tx
     *      salvage). In this case all locks before salvaged lock will marked as used and corresponding
     *      transactions will be invalidated.
     * @throws GridCacheEntryRemovedException If entry has been removed.
     * @return Owner.
     */
    @Nullable public GridCacheMvccCandidate doneRemote(
        GridCacheVersion lockVer,
        GridCacheVersion baseVer,
        Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers,
        boolean sysInvalidate) throws GridCacheEntryRemovedException {
        return doneRemote(lockVer, baseVer, Collections.<GridCacheVersion>emptySet(), committedVers,
            rolledbackVers, sysInvalidate);
    }

    /**
     *
     * @param lockVer Done version.
     * @param baseVer Base version.
     * @param pendingVers Pending versions that are less than lock version.
     * @param committedVers Completed versions for reordering.
     * @param rolledbackVers Rolled back versions for reordering.
     * @param sysInvalidate Flag indicating if this entry is done from invalidated transaction (in case of tx
     *      salvage). In this case all locks before salvaged lock will marked as used and corresponding
     *      transactions will be invalidated.
     * @throws GridCacheEntryRemovedException If entry has been removed.
     * @return Owner.
     */
    @Nullable public GridCacheMvccCandidate doneRemote(
        GridCacheVersion lockVer,
        GridCacheVersion baseVer,
        @Nullable Collection<GridCacheVersion> pendingVers,
        Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers,
        boolean sysInvalidate) throws GridCacheEntryRemovedException {
        GridCacheMvccCandidate prev = null;
        GridCacheMvccCandidate owner = null;

        CacheObject val;

        synchronized (this) {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc != null) {
                prev = mvcc.anyOwner();

                boolean emptyBefore = mvcc.isEmpty();

                // Order completed versions.
                if (!F.isEmpty(committedVers) || !F.isEmpty(rolledbackVers)) {
                    mvcc.orderCompleted(lockVer, committedVers, rolledbackVers);

                    if (!baseVer.equals(lockVer))
                        mvcc.orderCompleted(baseVer, committedVers, rolledbackVers);
                }

                if (sysInvalidate && baseVer != null)
                    mvcc.salvageRemote(baseVer);

                owner = mvcc.doneRemote(lockVer, maskNull(pendingVers), maskNull(committedVers),
                    maskNull(rolledbackVers));

                boolean emptyAfter = mvcc.isEmpty();

                checkCallbacks(emptyBefore, emptyAfter);

                if (emptyAfter)
                    mvccExtras(null);
            }

            val = this.val;
        }

        // This call must be made outside of synchronization.
        checkOwnerChanged(prev, owner, val);

        return owner;
    }

    /**
     * Rechecks if lock should be reassigned.
     *
     * @return Current owner.
     */
    @Nullable public GridCacheMvccCandidate recheck() {
        GridCacheMvccCandidate prev = null;
        GridCacheMvccCandidate owner = null;

        CacheObject val;

        synchronized (this) {
            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc != null) {
                prev = mvcc.anyOwner();

                boolean emptyBefore = mvcc.isEmpty();

                owner = mvcc.recheck();

                boolean emptyAfter = mvcc.isEmpty();

                checkCallbacks(emptyBefore, emptyAfter);

                if (emptyAfter)
                    mvccExtras(null);
            }

            val = this.val;
        }

        // This call must be made outside of synchronization.
        checkOwnerChanged(prev, owner, val);

        return owner;
    }

    /** {@inheritDoc} */
    @Override public boolean tmLock(IgniteInternalTx tx, long timeout)
        throws GridCacheEntryRemovedException, GridDistributedLockCancelledException {
        if (tx.local())
            // Null is returned if timeout is negative and there is other lock owner.
            return addLocal(
                tx.threadId(),
                tx.xidVersion(),
                tx.topologyVersion(),
                timeout,
                false,
                true,
                tx.implicitSingle()) != null;

        try {
            addRemote(
                tx.nodeId(),
                tx.otherNodeId(),
                tx.threadId(),
                tx.xidVersion(),
                tx.timeout(),
                true,
                tx.implicitSingle(),
                tx.ownedVersion(txKey())
            );

            return true;
        }
        catch (GridDistributedLockCancelledException ignored) {
            if (log.isDebugEnabled())
                log.debug("Attempted to enter tx lock for cancelled ID (will ignore): " + tx);

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public void txUnlock(IgniteInternalTx tx) throws GridCacheEntryRemovedException {
        removeLock(tx.xidVersion());
    }

    /**
     * @param emptyBefore Empty flag before operation.
     * @param emptyAfter Empty flag after operation.
     */
    protected void checkCallbacks(boolean emptyBefore, boolean emptyAfter) {
        assert Thread.holdsLock(this);

        if (emptyBefore != emptyAfter) {
            if (emptyBefore)
                cctx.mvcc().callback().onLocked(this);

            if (emptyAfter)
                cctx.mvcc().callback().onFreed(this);
        }
    }

    /**
     * @param prev Previous owner.
     * @param owner Current owner.
     * @param val Entry value.
     */
    protected void checkOwnerChanged(GridCacheMvccCandidate prev, GridCacheMvccCandidate owner, CacheObject val) {
        assert !Thread.holdsLock(this);

        if (owner != prev) {
            cctx.mvcc().callback().onOwnerChanged(this, prev, owner);

            if (owner != null && owner.local())
                checkThreadChain(owner);

            if (prev != null && cctx.events().isRecordable(EVT_CACHE_OBJECT_UNLOCKED)) {
                boolean hasVal = hasValue();

                // Event notification.
                cctx.events().addEvent(partition(), key, prev.nodeId(), prev, EVT_CACHE_OBJECT_UNLOCKED, val, hasVal,
                    val, hasVal, null, null, null);
            }

            if (owner != null && cctx.events().isRecordable(EVT_CACHE_OBJECT_LOCKED)) {
                boolean hasVal = hasValue();

                // Event notification.
                cctx.events().addEvent(partition(), key, owner.nodeId(), owner, EVT_CACHE_OBJECT_LOCKED, val, hasVal,
                    val, hasVal, null, null, null);
            }
        }
    }

    /**
     * @param owner Starting candidate in the chain.
     */
    protected void checkThreadChain(GridCacheMvccCandidate owner) {
        assert !Thread.holdsLock(this);

        assert owner != null;
        assert owner.owner() || owner.used() : "Neither owner or used flags are set on ready local candidate: " +
            owner;

        if (owner.local() && owner.next() != null) {
            for (GridCacheMvccCandidate cand = owner.next(); cand != null; cand = cand.next()) {
                assert cand.local() : "Remote candidate cannot be part of thread chain: " + cand;

                // Allow next lock in the thread to proceed.
                if (!cand.used()) {
                    GridCacheContext cctx0 = cand.parent().context();

                    GridDistributedCacheEntry e =
                        (GridDistributedCacheEntry)cctx0.cache().peekEx(cand.key());

                    if (e != null)
                        e.recheck();

                    break;
                }
            }
        }
    }

    /**
     * @param col Collection to mask.
     * @return Empty collection if argument is null.
     */
    private Collection<GridCacheVersion> maskNull(Collection<GridCacheVersion> col) {
        return col == null ? Collections.<GridCacheVersion>emptyList() : col;
    }

    /** {@inheritDoc} */
    @Override public synchronized String toString() {
        return S.toString(GridDistributedCacheEntry.class, this, super.toString());
    }
}
