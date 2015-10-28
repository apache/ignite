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
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.U;
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
        if (tx.activeCacheIds().isEmpty())
            return cctx.exchange().lastTopologyFuture();

        GridCacheContext<?, ?> nonLocCtx = null;

        for (int cacheId : tx.activeCacheIds()) {
            GridCacheContext<?, ?> cacheCtx = cctx.cacheContext(cacheId);

            if (!cacheCtx.isLocal()) {
                nonLocCtx = cacheCtx;

                break;
            }
        }

        if (nonLocCtx == null)
            return cctx.exchange().lastTopologyFuture();

        nonLocCtx.topology().readLock();

        if (nonLocCtx.topology().stopping()) {
            onDone(new IgniteCheckedException("Failed to perform cache operation (cache is stopped): " +
                nonLocCtx.name()));

            return null;
        }

        return nonLocCtx.topology().topologyVersionFuture();
    }

    /**
     * Releases topology read lock.
     */
    protected final void topologyReadUnlock() {
        if (!tx.activeCacheIds().isEmpty()) {
            GridCacheContext<?, ?> nonLocCtx = null;

            for (int cacheId : tx.activeCacheIds()) {
                GridCacheContext<?, ?> cacheCtx = cctx.cacheContext(cacheId);

                if (!cacheCtx.isLocal()) {
                    nonLocCtx = cacheCtx;

                    break;
                }
            }

            if (nonLocCtx != null)
                nonLocCtx.topology().readUnlock();
        }
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
            StringBuilder invalidCaches = null;

            for (Integer cacheId : tx.activeCacheIds()) {
                GridCacheContext ctx = cctx.cacheContext(cacheId);

                assert ctx != null : cacheId;

                Throwable err = topFut.validateCache(ctx);

                if (err != null) {
                    if (invalidCaches != null)
                        invalidCaches.append(", ");
                    else
                        invalidCaches = new StringBuilder();

                    invalidCaches.append(U.maskName(ctx.name()));
                }
            }

            if (invalidCaches != null) {
                onDone(new IgniteCheckedException("Failed to perform cache operation (cache topology is not valid): " +
                    invalidCaches.toString()));

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
