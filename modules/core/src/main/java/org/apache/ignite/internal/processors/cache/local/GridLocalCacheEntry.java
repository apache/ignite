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

package org.apache.ignite.internal.processors.cache.local;

import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.jetbrains.annotations.*;

import static org.apache.ignite.events.EventType.*;

/**
 * Cache entry for local caches.
 */
@SuppressWarnings({"NonPrivateFieldAccessedInSynchronizedContext", "TooBroadScope"})
public class GridLocalCacheEntry extends GridCacheMapEntry {
    /** Off-heap value pointer. */
    private long valPtr;

    /**
     * @param ctx  Cache registry.
     * @param key  Cache key.
     * @param hash Key hash value.
     * @param val Entry value.
     * @param next Next entry in the linked list.
     * @param hdrId Header id.
     */
    public GridLocalCacheEntry(GridCacheContext ctx,
        KeyCacheObject key,
        int hash,
        CacheObject val,
        GridCacheMapEntry next,
        int hdrId)
    {
        super(ctx, key, hash, val, next, hdrId);
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal() {
        return true;
    }

    /**
     * Add local candidate.
     *
     * @param threadId Owning thread ID.
     * @param ver Lock version.
     * @param timeout Timeout to acquire lock.
     * @param reenter Reentry flag.
     * @param tx Transaction flag.
     * @param implicitSingle Implicit transaction flag.
     * @return New candidate.
     * @throws GridCacheEntryRemovedException If entry has been removed.
     */
    @Nullable public GridCacheMvccCandidate addLocal(
        long threadId,
        GridCacheVersion ver,
        long timeout,
        boolean reenter,
        boolean tx,
        boolean implicitSingle) throws GridCacheEntryRemovedException {
        GridCacheMvccCandidate prev;
        GridCacheMvccCandidate cand;
        GridCacheMvccCandidate owner;

        CacheObject val;
        boolean hasVal;

        synchronized (this) {
            checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc == null) {
                mvcc = new GridCacheMvcc(cctx);

                mvccExtras(mvcc);
            }

            prev = mvcc.localOwner();

            cand = mvcc.addLocal(
                this,
                threadId,
                ver,
                timeout,
                reenter,
                tx,
                implicitSingle
            );

            owner = mvcc.localOwner();

            val = this.val;

            hasVal = hasValueUnlocked();

            if (mvcc.isEmpty())
                mvccExtras(null);
        }

        if (cand != null) {
            if (!cand.reentry())
                cctx.mvcc().addNext(cctx, cand);

            // Event notification.
            if (cctx.events().isRecordable(EVT_CACHE_OBJECT_LOCKED))
                cctx.events().addEvent(partition(), key, cand.nodeId(), cand, EVT_CACHE_OBJECT_LOCKED, val, hasVal,
                    val, hasVal, null, null, null);
        }

        checkOwnerChanged(prev, owner);

        return cand;
    }

    /**
     *
     * @param cand Candidate.
     * @return Current owner.
     */
    @Nullable public GridCacheMvccCandidate readyLocal(GridCacheMvccCandidate cand) {
        GridCacheMvccCandidate prev = null;
        GridCacheMvccCandidate owner = null;

        synchronized (this) {
            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc != null) {
                prev = mvcc.localOwner();

                owner = mvcc.readyLocal(cand);

                if (mvcc.isEmpty())
                    mvccExtras(null);
            }
        }

        checkOwnerChanged(prev, owner);

        return owner;
    }

    /**
     *
     * @param ver Candidate version.
     * @return Current owner.
     */
    @Nullable public GridCacheMvccCandidate readyLocal(GridCacheVersion ver) {
        GridCacheMvccCandidate prev = null;
        GridCacheMvccCandidate owner = null;

        synchronized (this) {
            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc != null) {
                prev = mvcc.localOwner();

                owner = mvcc.readyLocal(ver);

                if (mvcc.isEmpty())
                    mvccExtras(null);
            }
        }

        checkOwnerChanged(prev, owner);

        return owner;
    }

    /** {@inheritDoc} */
    @Override public boolean tmLock(IgniteInternalTx tx, long timeout) throws GridCacheEntryRemovedException {
        GridCacheMvccCandidate cand = addLocal(
            tx.threadId(),
            tx.xidVersion(),
            timeout,
            /*reenter*/false,
            /*tx*/true,
            tx.implicitSingle()
        );

        if (cand != null) {
            readyLocal(cand);

            return true;
        }

        return false;
    }

    /**
     * Rechecks if lock should be reassigned.
     *
     * @return Current owner.
     */
    @Nullable public GridCacheMvccCandidate recheck() {
        GridCacheMvccCandidate prev = null;
        GridCacheMvccCandidate owner = null;

        synchronized (this) {
            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc != null) {
                prev = mvcc.localOwner();

                owner = mvcc.recheck();

                if (mvcc.isEmpty())
                    mvccExtras(null);
            }
        }

        checkOwnerChanged(prev, owner);

        return owner;
    }

    /**
     * @param prev Previous owner.
     * @param owner Current owner.
     */
    private void checkOwnerChanged(GridCacheMvccCandidate prev, GridCacheMvccCandidate owner) {
        assert !Thread.holdsLock(this);

        if (owner != prev) {
            cctx.mvcc().callback().onOwnerChanged(this, prev, owner);

            if (owner != null)
                checkThreadChain(owner);
        }
    }

    /**
     * @param owner Starting candidate in the chain.
     */
    private void checkThreadChain(GridCacheMvccCandidate owner) {
        assert !Thread.holdsLock(this);

        assert owner != null;
        assert owner.owner() || owner.used() : "Neither owner or used flags are set on ready local candidate: " +
            owner;

        if (owner.next() != null) {
            for (GridCacheMvccCandidate cand = owner.next(); cand != null; cand = cand.next()) {
                assert cand.local();

                // Allow next lock in the thread to proceed.
                if (!cand.used()) {
                    GridCacheContext cctx0 = cand.parent().context();

                    GridLocalCacheEntry e = (GridLocalCacheEntry)cctx0.cache().peekEx(cand.key());

                    // At this point candidate may have been removed and entry destroyed,
                    // so we check for null.
                    if (e != null)
                        e.recheck();

                    break;
                }
            }
        }
    }

    /**
     * Unlocks lock if it is currently owned.
     *
     * @param tx Transaction to unlock.
     */
    @Override public void txUnlock(IgniteInternalTx tx) throws GridCacheEntryRemovedException {
        removeLock(tx.xidVersion());
    }

    /**
     * Releases local lock.
     */
    void releaseLocal() {
        releaseLocal(Thread.currentThread().getId());
    }

    /**
     * Releases local lock.
     *
     * @param threadId Thread ID.
     */
    void releaseLocal(long threadId) {
        GridCacheMvccCandidate prev = null;
        GridCacheMvccCandidate owner = null;

        CacheObject val;
        boolean hasVal;

        synchronized (this) {
            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc != null) {
                prev = mvcc.localOwner();

                owner = mvcc.releaseLocal(threadId);

                if (mvcc.isEmpty())
                    mvccExtras(null);
            }

            val = this.val;
            hasVal = hasValueUnlocked();
        }

        if (prev != null && owner != prev) {
            checkThreadChain(prev);

            // Event notification.
            if (cctx.events().isRecordable(EVT_CACHE_OBJECT_UNLOCKED))
                cctx.events().addEvent(partition(), key, prev.nodeId(), prev, EVT_CACHE_OBJECT_UNLOCKED, val, hasVal,
                    val, hasVal, null, null, null);
        }

        checkOwnerChanged(prev, owner);
    }

    /**
     * Removes candidate regardless if it is owner or not.
     *
     * @param cand Candidate to remove.
     * @throws GridCacheEntryRemovedException If the entry was removed by version other
     *      than one passed in.
     */
    void removeLock(GridCacheMvccCandidate cand) throws GridCacheEntryRemovedException {
        removeLock(cand.version());
    }

    /** {@inheritDoc} */
    @Override public boolean removeLock(GridCacheVersion ver) throws GridCacheEntryRemovedException {
        GridCacheMvccCandidate prev = null;
        GridCacheMvccCandidate owner = null;

        GridCacheMvccCandidate doomed;

        CacheObject val;
        boolean hasVal;

        synchronized (this) {
            GridCacheVersion obsoleteVer = obsoleteVersionExtras();

            if (obsoleteVer != null && !obsoleteVer.equals(ver))
                checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            doomed = mvcc == null ? null : mvcc.candidate(ver);

            if (doomed != null) {
                prev = mvcc.localOwner();

                owner = mvcc.remove(ver);

                if (mvcc.isEmpty())
                    mvccExtras(null);
            }

            val = this.val;
            hasVal = hasValueUnlocked();
        }

        if (doomed != null) {
            checkThreadChain(doomed);

            // Event notification.
            if (cctx.events().isRecordable(EVT_CACHE_OBJECT_UNLOCKED))
                cctx.events().addEvent(partition(), key, doomed.nodeId(), doomed, EVT_CACHE_OBJECT_UNLOCKED,
                    val, hasVal, val, hasVal, null, null, null);
        }

        checkOwnerChanged(prev, owner);

        return doomed != null;
    }

    /** {@inheritDoc} */
    @Override protected boolean hasOffHeapPointer() {
        return valPtr != 0;
    }

    /** {@inheritDoc} */
    @Override protected long offHeapPointer() {
        return valPtr;
    }

    /** {@inheritDoc} */
    @Override protected void offHeapPointer(long valPtr) {
        this.valPtr = valPtr;
    }
}
