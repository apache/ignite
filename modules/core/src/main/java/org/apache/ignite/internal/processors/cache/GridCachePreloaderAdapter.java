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

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicAbstractUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander.RebalanceFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloaderAssignments;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_REBALANCE_BATCHES_PREFETCH_COUNT;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_REBALANCE_BATCH_SIZE;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_REBALANCE_THROTTLE;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_REBALANCE_TIMEOUT;

/**
 * Adapter for preloading which always assumes that preloading finished.
 */
public class GridCachePreloaderAdapter implements GridCachePreloader {
    /** */
    protected final CacheGroupContext grp;

    /** */
    protected final GridCacheSharedContext ctx;

    /** Logger. */
    protected final IgniteLogger log;

    /** Start future (always completed by default). */
    private final IgniteInternalFuture finFut;

    /**
     * @param grp Cache group.
     */
    public GridCachePreloaderAdapter(CacheGroupContext grp) {
        assert grp != null;

        this.grp = grp;

        ctx = grp.shared();

        log = ctx.logger(getClass());

        finFut = new GridFinishedFuture();
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> forceRebalance() {
        return new GridFinishedFuture<>(true);
    }

    /** {@inheritDoc} */
    @Override public boolean needForceKeys() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void onReconnected() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Object> startFuture() {
        return finFut;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> syncFuture() {
        return finFut;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> rebalanceFuture() {
        return finFut;
    }

    /** {@inheritDoc} */
    @Override public void handleSupplyMessage(UUID id, GridDhtPartitionSupplyMessage s) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void handleDemandMessage(int idx, UUID id, GridDhtPartitionDemandMessage d) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public GridDhtFuture<Object> request(GridCacheContext ctx, Collection<KeyCacheObject> keys,
        AffinityTopologyVersion topVer) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridDhtFuture<Object> request(GridCacheContext ctx, GridNearAtomicAbstractUpdateRequest req,
        AffinityTopologyVersion topVer) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void onInitialExchangeComplete(@Nullable Throwable err) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean rebalanceRequired(GridDhtPartitionsExchangeFuture exchFut) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public GridDhtPreloaderAssignments generateAssignments(GridDhtPartitionExchangeId exchId,
                                                                     GridDhtPartitionsExchangeFuture exchFut) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public RebalanceFuture addAssignments(GridDhtPreloaderAssignments assignments,
        boolean forcePreload,
        long rebalanceId,
        RebalanceFuture next,
        @Nullable GridCompoundFuture<Boolean, Boolean> forcedRebFut,
        GridCompoundFuture<Boolean, Boolean> compatibleRebFut) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void onTopologyChanged(GridDhtPartitionsExchangeFuture lastFut) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void dumpDebugInfo() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void pause() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void resume() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public long timeout() {
        return grp.shared().gridConfig().getRebalanceTimeout() == DFLT_REBALANCE_TIMEOUT ?
            grp.config().getRebalanceTimeout() : grp.shared().gridConfig().getRebalanceTimeout();
    }

    /** {@inheritDoc} */
    @Override public long batchesPrefetchCount() {
        return grp.shared().gridConfig().getRebalanceBatchesPrefetchCount() == DFLT_REBALANCE_BATCHES_PREFETCH_COUNT ?
            grp.config().getRebalanceBatchesPrefetchCount() : grp.shared().gridConfig().getRebalanceBatchesPrefetchCount();
    }

    /** {@inheritDoc} */
    @Override public long throttle() {
        return grp.shared().gridConfig().getRebalanceThrottle() == DFLT_REBALANCE_THROTTLE ?
            grp.config().getRebalanceThrottle() : grp.shared().gridConfig().getRebalanceThrottle();
    }

    /** {@inheritDoc} */
    @Override public int batchSize() {
        return grp.shared().gridConfig().getRebalanceBatchSize() == DFLT_REBALANCE_BATCH_SIZE ?
            grp.config().getRebalanceBatchSize() : grp.shared().gridConfig().getRebalanceBatchSize();
    }
}
