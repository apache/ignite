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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCoordinator;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCoordinatorVersion;
import org.apache.ignite.internal.processors.cache.mvcc.MvccResponseListener;
import org.apache.ignite.internal.processors.cache.mvcc.TxMvccInfo;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public abstract class GridNearOptimisticTxPrepareFutureAdapter extends GridNearTxPrepareFutureAdapter {
    /** */
    private static final AtomicIntegerFieldUpdater<MvccVersionFuture> LOCK_CNT_UPD =
        AtomicIntegerFieldUpdater.newUpdater(MvccVersionFuture.class, "lockCnt");

    /** */
    @GridToStringExclude
    protected KeyLockFuture keyLockFut;

    /** */
    @GridToStringExclude
    protected MvccVersionFuture mvccVerFut;

    /**
     * @param cctx Context.
     * @param tx Transaction.
     */
    protected GridNearOptimisticTxPrepareFutureAdapter(GridCacheSharedContext cctx, GridNearTxLocal tx) {
        super(cctx, tx);

        assert tx.optimistic() : tx;
    }

    /** {@inheritDoc} */
    @Override public final void prepare() {
        // Obtain the topology version to use.
        long threadId = Thread.currentThread().getId();

        AffinityTopologyVersion topVer = cctx.mvcc().lastExplicitLockTopologyVersion(threadId);

        // If there is another system transaction in progress, use it's topology version to prevent deadlock.
        if (topVer == null && tx.system()) {
            topVer = cctx.tm().lockedTopologyVersion(threadId, tx);

            if (topVer == null)
                topVer = tx.topologyVersionSnapshot();
        }

        if (topVer != null) {
            tx.topologyVersion(topVer);

            cctx.mvcc().addFuture(this);

            prepare0(false, true);

            return;
        }

        prepareOnTopology(false, null);
    }

    /**
     * Acquires topology read lock.
     *
     * @return Topology ready future.
     */
    protected final GridDhtTopologyFuture topologyReadLock() {
        return tx.txState().topologyReadLock(cctx, this);
    }

    /**
     * Releases topology read lock.
     */
    protected final void topologyReadUnlock() {
        tx.txState().topologyReadUnlock(cctx);
    }

    /**
     * @param remap Remap flag.
     * @param c Optional closure to run after map.
     */
    protected final void prepareOnTopology(final boolean remap, @Nullable final Runnable c) {
        GridDhtTopologyFuture topFut = topologyReadLock();

        AffinityTopologyVersion topVer = null;

        try {
            if (topFut == null) {
                assert isDone();

                return;
            }

            if (topFut.isDone()) {
                topVer = topFut.topologyVersion();

                if (remap)
                    tx.onRemap(topVer);
                else
                    tx.topologyVersion(topVer);

                if (!remap)
                    cctx.mvcc().addFuture(this);
            }
        }
        finally {
            topologyReadUnlock();
        }

        if (topVer != null) {
            IgniteCheckedException err = tx.txState().validateTopology(
                cctx,
                tx.writeMap().isEmpty(),
                topFut);

            if (err != null) {
                onDone(err);

                return;
            }

            prepare0(remap, false);

            if (c != null)
                c.run();
        }
        else {
            topFut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                @Override public void apply(final IgniteInternalFuture<AffinityTopologyVersion> fut) {
                    cctx.kernalContext().closure().runLocalSafe(new GridPlainRunnable() {
                        @Override public void run() {
                            try {
                                fut.get();

                                prepareOnTopology(remap, c);
                            }
                            catch (IgniteCheckedException e) {
                                onDone(e);
                            }
                            finally {
                                cctx.txContextReset();
                            }
                        }
                    });
                }
            });
        }
    }

    /**
     * @param remap Remap flag.
     * @param topLocked {@code True} if thread already acquired lock preventing topology change.
     */
    protected abstract void prepare0(boolean remap, boolean topLocked);

    /**
     * @param mvccCrd
     * @param lockCnt
     * @param remap
     */
    final void initMvccVersionFuture(MvccCoordinator mvccCrd, int lockCnt, boolean remap) {
        if (!remap) {
            mvccVerFut = new MvccVersionFuture();

            mvccVerFut.init(mvccCrd, lockCnt);

            if (keyLockFut != null)
                keyLockFut.listen(mvccVerFut);

            add(mvccVerFut);
        }
        else {
            assert mvccVerFut != null;

            mvccVerFut.init(mvccCrd, lockCnt);
        }
    }

    /**
     * Keys lock future.
     */
    protected static class KeyLockFuture extends GridFutureAdapter<Void> {
        /** */
        @GridToStringInclude
        protected Collection<IgniteTxKey> lockKeys = new GridConcurrentHashSet<>();

        /** */
        private volatile boolean allKeysAdded;

        /**
         * @param key Key to track for locking.
         */
        protected void addLockKey(IgniteTxKey key) {
            assert !allKeysAdded;

            lockKeys.add(key);
        }

        /**
         * @param key Locked keys.
         */
        protected void onKeyLocked(IgniteTxKey key) {
            lockKeys.remove(key);

            checkLocks();
        }

        /**
         * Moves future to the ready state.
         */
        protected void onAllKeysAdded() {
            allKeysAdded = true;

            checkLocks();
        }

        /**
         * @return {@code True} if all locks are owned.
         */
        private boolean checkLocks() {
            boolean locked = lockKeys.isEmpty();

            if (locked && allKeysAdded) {
                if (log.isDebugEnabled())
                    log.debug("All locks are acquired for near prepare future: " + this);

                onDone((Void)null);
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Still waiting for locks [fut=" + this + ", keys=" + lockKeys + ']');
            }

            return locked;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(KeyLockFuture.class, this, super.toString());
        }
    }

    /**
     *
     */
    class MvccVersionFuture extends GridFutureAdapter implements MvccResponseListener,
        IgniteInClosure<IgniteInternalFuture<Void>> {
        /** */
        MvccCoordinator crd;

        /** */
        volatile int lockCnt;

        @Override public void apply(IgniteInternalFuture<Void> keyLockFut) {
            try {
                keyLockFut.get();

                onLockReceived();
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("MvccVersionFuture ignores key lock future failure: " + e);
            }
        }

        /**
         * @param crd Mvcc coordinator.
         * @param lockCnt Expected number of lock responses.
         */
        void init(MvccCoordinator crd, int lockCnt) {
            assert crd != null;
            assert lockCnt > 0;

            this.crd = crd;
            this.lockCnt = lockCnt;

            assert !isDone();
        }

        /**
         *
         */
        void onLockReceived() {
            int remaining = LOCK_CNT_UPD.decrementAndGet(this);

            assert remaining >= 0 : remaining;

            if (remaining == 0) {
                // TODO IGNTIE-3478: add method to do not create one more future in requestTxCounter.
                if (cctx.localNodeId().equals(crd.nodeId()))
                    onMvccResponse(crd.nodeId(), cctx.coordinators().requestTxCounterOnCoordinator(tx));
                else
                    cctx.coordinators().requestTxCounter(crd, this, tx.nearXidVersion());
            }
        }

        /** {@inheritDoc} */
        @Override public void onMvccResponse(UUID crdId, MvccCoordinatorVersion res) {
            tx.mvccInfo(new TxMvccInfo(crdId, res));

            onDone();
        }

        /** {@inheritDoc} */
        @Override public void onMvccError(IgniteCheckedException e) {
            if (e instanceof ClusterTopologyCheckedException) {
                IgniteInternalFuture<?> fut = cctx.nextAffinityReadyFuture(tx.topologyVersion());

                ((ClusterTopologyCheckedException)e).retryReadyFuture(fut);
            }

            ERR_UPD.compareAndSet(GridNearOptimisticTxPrepareFutureAdapter.this, null, e);

            onDone();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "MvccVersionFuture [crd=" + crd.nodeId() +
                ", lockCnt=" + lockCnt +
                ", done=" + isDone() + ']';
        }
    }
}
