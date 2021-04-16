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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.tracing.MTC.TraceSurroundings;
import static org.apache.ignite.internal.processors.tracing.SpanType.TX_NEAR_PREPARE;

/**
 *
 */
public abstract class GridNearOptimisticTxPrepareFutureAdapter extends GridNearTxPrepareFutureAdapter {
    /** */
    private static final long serialVersionUID = 7460376140787916619L;

    /** Tracing span. */
    protected Span span;

    /** */
    @GridToStringExclude
    protected KeyLockFuture keyLockFut;

    /**
     * @param cctx Context.
     * @param tx Transaction.
     */
    protected GridNearOptimisticTxPrepareFutureAdapter(GridCacheSharedContext cctx, GridNearTxLocal tx) {
        super(cctx, tx);

        assert tx.optimistic() : tx;

        if (tx.timeout() > 0) {
            // Init keyLockFut to make sure it is created when {@link #onNearTxLocalTimeout} is called.
            for (IgniteTxEntry e : tx.writeEntries()) {
                if (e.context().isNear() || e.context().isLocal()) {
                    keyLockFut = new KeyLockFuture();
                    break;
                }
            }

            if (tx.serializable() && keyLockFut == null) {
                for (IgniteTxEntry e : tx.readEntries()) {
                    if (e.context().isNear() || e.context().isLocal()) {
                        keyLockFut = new KeyLockFuture();
                        break;
                    }
                }
            }

            if (keyLockFut != null)
                add((IgniteInternalFuture)keyLockFut);
        }
    }

    /** {@inheritDoc} */
    @Override public final void onNearTxLocalTimeout() {
        try (TraceSurroundings ignored = MTC.support(span)) {
            if (keyLockFut != null && !keyLockFut.isDone()) {
                ERR_UPD.compareAndSet(this, null, new IgniteTxTimeoutCheckedException(
                    "Failed to acquire lock within provided timeout for transaction [timeout=" + tx.timeout() +
                        ", tx=" + tx + ']'));

                keyLockFut.onDone();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public final void prepare() {
        try (TraceSurroundings ignored =
                 MTC.supportContinual(span = cctx.kernalContext().tracing().create(TX_NEAR_PREPARE, MTC.span()))) {
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
                if ((topVer = topFut.topologyVersion()) == null && topFut.error() != null) {
                    onDone(topFut.error()); // Prevent stack overflow if topFut has error.

                    return;
                }

                if (remap)
                    tx.onRemap(topVer, true);
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

            if (tx.isRollbackOnly()) {
                onDone(new IgniteTxRollbackCheckedException(
                    "Failed to prepare the transaction, due to the transaction is marked as rolled back " +
                        "[tx=" + CU.txString(tx) + ']'));

                return;
            }

            prepare0(remap, false);

            if (c != null)
                c.run();
        }
        else {
            cctx.time().waitAsync(topFut, tx.remainingTime(), (e, timedOut) -> {
                if (errorOrTimeoutOnTopologyVersion(e, timedOut))
                    return;

                try {
                    if (tx.isRollbackOnly()) {
                        onDone(new IgniteTxRollbackCheckedException(
                            "Failed to prepare the transaction, due to the transaction is marked as rolled back " +
                                "[tx=" + CU.txString(tx) + ']'));

                        return;
                    }

                    prepareOnTopology(remap, c);
                }
                finally {
                    cctx.txContextReset();
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
     * @param e Exception.
     * @param timedOut {@code True} if timed out.
     */
    protected boolean errorOrTimeoutOnTopologyVersion(IgniteCheckedException e, boolean timedOut) {
        if (e != null || timedOut) {
            if (timedOut)
                e = tx.timeoutException();

            ERR_UPD.compareAndSet(this, null, e);

            onDone(e);

            return true;
        }

        return false;
    }

    /**
     * Keys lock future.
     */
    protected static class KeyLockFuture extends GridFutureAdapter<Void> {
        /** */
        @GridToStringInclude
        protected Collection<IgniteTxKey> lockKeys = new GridConcurrentHashSet<>();

        /** */
        protected volatile boolean allKeysAdded;

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

        /** */
        private void checkLocks() {
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
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(KeyLockFuture.class, this, super.toString());
        }
    }
}
