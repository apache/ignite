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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.typedef.CI1;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public abstract class GridNearOptimisticTxPrepareFutureAdapter extends GridNearTxPrepareFutureAdapter {
    /**
     * @param cctx Context.
     * @param tx Transaction.
     */
    public GridNearOptimisticTxPrepareFutureAdapter(GridCacheSharedContext cctx, GridNearTxLocal tx) {
        super(cctx, tx);

        assert tx.optimistic() : tx;
    }

    /** {@inheritDoc} */
    @Override public final void prepare() {
        // Obtain the topology version to use.
        AffinityTopologyVersion topVer = cctx.mvcc().lastExplicitLockTopologyVersion(Thread.currentThread().getId());

        // If there is another system transaction in progress, use it's topology version to prevent deadlock.
        if (topVer == null && tx != null && tx.system()) {
            IgniteInternalTx tx0 = cctx.tm().anyActiveThreadTx(tx);

            if (tx0 != null)
                topVer = tx0.topologyVersionSnapshot();
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
            IgniteCheckedException err = tx.txState().validateTopology(cctx, topFut);

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
}
