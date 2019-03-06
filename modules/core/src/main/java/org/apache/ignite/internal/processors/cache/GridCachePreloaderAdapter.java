/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloaderAssignments;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

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

    /** Preload predicate. */
    protected IgnitePredicate<GridCacheEntryInfo> preloadPred;

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
        grp.unwindUndeploys();
    }

    /** {@inheritDoc} */
    @Override public void handleSupplyMessage(int idx, UUID id, GridDhtPartitionSupplyMessage s) {
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
    @Override public boolean rebalanceRequired(AffinityTopologyVersion rebTopVer,
        GridDhtPartitionsExchangeFuture exchFut) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public GridDhtPreloaderAssignments generateAssignments(GridDhtPartitionExchangeId exchId,
                                                                     GridDhtPartitionsExchangeFuture exchFut) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Runnable addAssignments(GridDhtPreloaderAssignments assignments,
        boolean forcePreload,
        long rebalanceId,
        Runnable next,
        @Nullable GridCompoundFuture<Boolean, Boolean> forcedRebFut) {
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
}
