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

import org.apache.ignite.internal.processors.cache.CacheLockCandidates;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheMvcc;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Cache entry for local caches.
 */
@SuppressWarnings({"TooBroadScope"})
public class GridLocalCacheEntry extends GridCacheMapEntry {
    /**
     * @param ctx  Cache registry.
     * @param key  Cache key.
     */
    GridLocalCacheEntry(
        GridCacheContext ctx,
        KeyCacheObject key
    ) {
        super(ctx, key);
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
     * @param serOrder Version for serializable transactions ordering.
     * @param serReadVer Optional read entry version for optimistic serializable transaction.
     * @param timeout Timeout to acquire lock.
     * @param reenter Reentry flag.
     * @param tx Transaction flag.
     * @param implicitSingle Implicit transaction flag.
     * @param read Read lock flag.
     * @return New candidate.
     * @throws GridCacheEntryRemovedException If entry has been removed.
     */
    @Nullable GridCacheMvccCandidate addLocal(
        long threadId,
        GridCacheVersion ver,
        @Nullable GridCacheVersion serOrder,
        @Nullable GridCacheVersion serReadVer,
        long timeout,
        boolean reenter,
        boolean tx,
        boolean implicitSingle,
        boolean read
    ) throws GridCacheEntryRemovedException {
        assert serReadVer == null || serOrder != null;

        CacheObject val;
        GridCacheMvccCandidate cand;
        CacheLockCandidates prev;
        CacheLockCandidates owner = null;

        lockEntry();

        try {
            checkObsolete();

            if (serReadVer != null) {
                if (!checkSerializableReadVersion(serReadVer))
                    return null;
            }

            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc == null) {
                mvcc = new GridCacheMvcc(cctx);

                mvccExtras(mvcc);
            }

            prev = mvcc.localOwners();

            cand = mvcc.addLocal(
                this,
                /*nearNodeId*/null,
                /*nearVer*/null,
                threadId,
                ver,
                timeout,
                serOrder,
                reenter,
                tx,
                implicitSingle,
                /*dht-local*/false,
                read
            );

            if (mvcc.isEmpty())
                mvccExtras(null);
            else
                owner = mvcc.localOwners();

            val = this.val;
        }
        finally {
            unlockEntry();
        }

        if (cand != null && !cand.reentry())
            cctx.mvcc().addNext(cctx, cand);

        checkOwnerChanged(prev, owner, val);

        return cand;
    }

    /**
     * @param cand Candidate.
     */
    void readyLocal(GridCacheMvccCandidate cand) {
        CacheObject val;
        CacheLockCandidates prev = null;
        CacheLockCandidates owner = null;

        lockEntry();

        try {
            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc != null) {
                prev = mvcc.localOwners();

                owner = mvcc.readyLocal(cand);

                if (mvcc.isEmpty())
                    mvccExtras(null);
            }

            val = this.val;
        }
        finally {
            unlockEntry();
        }

        checkOwnerChanged(prev, owner, val);
    }

    /** {@inheritDoc} */
    @Override public boolean tmLock(IgniteInternalTx tx,
        long timeout,
        @Nullable GridCacheVersion serOrder,
        GridCacheVersion serReadVer,
        boolean read)
        throws GridCacheEntryRemovedException {
        GridCacheMvccCandidate cand = addLocal(
            tx.threadId(),
            tx.xidVersion(),
            serOrder,
            serReadVer,
            timeout,
            /*reenter*/false,
            /*tx*/true,
            tx.implicitSingle(),
            read
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
     * @param ver Thread chain version.
     *
     * @return {@code True} if thread chain processing must be stopped.
     */
    public boolean recheck(GridCacheVersion ver) {
        CacheObject val;
        CacheLockCandidates prev = null;
        CacheLockCandidates owner = null;

        lockEntry();

        try {
            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc != null) {
                prev = mvcc.allOwners();

                owner = mvcc.recheck();

                if (mvcc.isEmpty())
                    mvccExtras(null);
            }

            val = this.val;
        }
        finally {
            unlockEntry();
        }

        boolean lockedByThreadChainVer = owner != null && owner.hasCandidate(ver);

        // If locked by the thread chain version no need to do recursive thread chain scans for the same chain.
        // This call must be made outside of synchronization.
        checkOwnerChanged(prev, owner, val, lockedByThreadChainVer);

        return !lockedByThreadChainVer;
    }

    /** {@inheritDoc} */
    @Override protected void checkThreadChain(GridCacheMvccCandidate owner) {
        assert !lockedByCurrentThread();

        assert owner != null;
        assert owner.owner() || owner.used() : "Neither owner or used flags are set on ready local candidate: " +
            owner;

        if (owner.next() != null) {
            for (GridCacheMvccCandidate cand = owner.next(); cand != null; cand = cand.next()) {
                assert cand.local();

                // Allow next lock in the thread to proceed.
                if (!cand.used()) {
                    GridCacheContext cctx0 = cand.parent().context();

                    GridLocalCacheEntry e = (GridLocalCacheEntry)cctx0.cache().peekEx(cand.parent().key());

                    // At this point candidate may have been removed and entry destroyed, so we check for null.
                    if (e == null || e.recheck(owner.version()))
                        break;
                }
            }
        }
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
    private void releaseLocal(long threadId) {
        CacheObject val;
        CacheLockCandidates prev = null;
        CacheLockCandidates owner = null;

        lockEntry();

        try {
            GridCacheMvcc mvcc = mvccExtras();

            if (mvcc != null) {
                prev = mvcc.localOwners();

                mvcc.releaseLocal(threadId);

                if (mvcc.isEmpty())
                    mvccExtras(null);
                else
                    owner = mvcc.allOwners();
            }

            val = this.val;
        }
        finally {
            unlockEntry();
        }

        if (prev != null) {
            for (int i = 0; i < prev.size(); i++) {
                GridCacheMvccCandidate cand = prev.candidate(i);

                boolean unlocked = owner == null || !owner.hasCandidate(cand.version());

                if (unlocked)
                    checkThreadChain(cand);
            }
        }

        checkOwnerChanged(prev, owner, val);
    }

    /** {@inheritDoc} */
    @Override public boolean removeLock(GridCacheVersion ver) throws GridCacheEntryRemovedException {
        CacheObject val;
        CacheLockCandidates prev = null;
        CacheLockCandidates owner = null;

        GridCacheMvccCandidate doomed;

        GridCacheVersion deferredDelVer;

        lockEntry();

        try {
            GridCacheVersion obsoleteVer = obsoleteVersionExtras();

            if (obsoleteVer != null && !obsoleteVer.equals(ver))
                checkObsolete();

            GridCacheMvcc mvcc = mvccExtras();

            doomed = mvcc == null ? null : mvcc.candidate(ver);

            if (doomed != null) {
                prev = mvcc.allOwners();

                mvcc.remove(ver);

                if (mvcc.isEmpty())
                    mvccExtras(null);
                else
                    owner = mvcc.allOwners();
            }

            val = this.val;

            deferredDelVer = this.ver;
        }
        finally {
            unlockEntry();
        }

        if (val == null) {
            boolean deferred = cctx.deferredDelete() && !detached() && !isInternal();

            if (deferred) {
                if (deferredDelVer != null)
                    cctx.onDeferredDelete(this, deferredDelVer);
            }
        }

        if (doomed != null)
            checkThreadChain(doomed);

        checkOwnerChanged(prev, owner, val);

        return doomed != null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        lockEntry();

        try {
            return S.toString(GridLocalCacheEntry.class, this, super.toString());
        }
        finally {
            unlockEntry();
        }
    }
}
