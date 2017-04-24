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
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicAbstractUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessageV2;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloaderAssignments;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

/**
 * Adapter for preloading which always assumes that preloading finished.
 */
public class GridCachePreloaderAdapter implements GridCachePreloader {
    /** Cache context. */
    protected final GridCacheContext<?, ?> cctx;

    /** Logger. */
    protected final IgniteLogger log;

    /** Affinity. */
    protected final AffinityFunction aff;

    /** Start future (always completed by default). */
    private final IgniteInternalFuture finFut;

    /** Preload predicate. */
    protected IgnitePredicate<GridCacheEntryInfo> preloadPred;

    /**
     * @param cctx Cache context.
     */
    public GridCachePreloaderAdapter(GridCacheContext<?, ?> cctx) {
        assert cctx != null;

        this.cctx = cctx;

        log = cctx.logger(getClass());
        aff = cctx.config().getAffinity();

        finFut = new GridFinishedFuture();
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
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
    @Override public void preloadPredicate(IgnitePredicate<GridCacheEntryInfo> preloadPred) {
        this.preloadPred = preloadPred;
    }

    /** {@inheritDoc} */
    @Override public IgnitePredicate<GridCacheEntryInfo> preloadPredicate() {
        return preloadPred;
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
    @Override public void unwindUndeploys() {
        cctx.deploy().unwind(cctx);
    }

    /** {@inheritDoc} */
    @Override public void handleSupplyMessage(int idx, UUID id, GridDhtPartitionSupplyMessageV2 s) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void handleDemandMessage(int idx, UUID id, GridDhtPartitionDemandMessage d) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Object> request(Collection<KeyCacheObject> keys,
        AffinityTopologyVersion topVer) {
        return new GridFinishedFuture<>();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Object> request(GridNearAtomicAbstractUpdateRequest req,
        AffinityTopologyVersion topVer) {
        return new GridFinishedFuture<>();
    }

    /** {@inheritDoc} */
    @Override public void onInitialExchangeComplete(@Nullable Throwable err) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public GridDhtPreloaderAssignments assign(GridDhtPartitionsExchangeFuture exchFut) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Runnable addAssignments(GridDhtPreloaderAssignments assignments,
        boolean forcePreload,
        int cnt,
        Runnable next,
        @Nullable GridFutureAdapter<Boolean> forcedRebFut) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void evictPartitionAsync(GridDhtLocalPartition part) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onTopologyChanged(GridDhtPartitionsExchangeFuture lastFut) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void dumpDebugInfo() {
        // No-op.
    }
}
