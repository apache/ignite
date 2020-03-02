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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicAbstractUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.ForceRebalanceExchangeTask;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloaderAssignments;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Cache preloader that is responsible for loading cache entries either from remote
 * nodes (for distributed cache) or anywhere else at cache startup.
 */
public interface GridCachePreloader {
    /**
     * Starts preloading.
     *
     * @throws IgniteCheckedException If start failed.
     */
    public void start() throws IgniteCheckedException;

    /**
     * Kernal stop callback.
     */
    public void onKernalStop();

    /**
     * Client reconnected callback.
     */
    public void onReconnected();

    /**
     * Callback by exchange manager when initial partition exchange is complete.
     *
     * @param err Error, if any happened on initial exchange.
     */
    public void onInitialExchangeComplete(@Nullable Throwable err);

    /**
     * @param rebTopVer Previous rebalance topology version or {@code NONE} if there is no info.
     * @param exchFut Completed exchange future.
     * @return {@code True} if rebalance should be started (previous will be interrupted).
     */
    public boolean rebalanceRequired(AffinityTopologyVersion rebTopVer, GridDhtPartitionsExchangeFuture exchFut);

    /**
     * @param exchId Exchange ID.
     * @param exchFut Completed exchange future. Can be {@code null} if forced or reassigned generation occurs.
     * @return Partition assignments which will be requested from supplier nodes.
     */
    @Nullable public GridDhtPreloaderAssignments generateAssignments(GridDhtPartitionExchangeId exchId,
                                                                     @Nullable GridDhtPartitionsExchangeFuture exchFut);

    /**
     * Adds assignments to preloader.
     *
     * @param assignments Assignments to add.
     * @param forcePreload {@code True} if preload requested by {@link ForceRebalanceExchangeTask}.
     * @param rebalanceId Rebalance id created by exchange thread.
     * @param next Runnable responsible for cache rebalancing chain.
     * @param forcedRebFut External future for forced rebalance.
     * @return Rebalancing runnable.
     */
    public Runnable addAssignments(GridDhtPreloaderAssignments assignments,
        boolean forcePreload,
        long rebalanceId,
        Runnable next,
        @Nullable GridCompoundFuture<Boolean, Boolean> forcedRebFut);

    /**
     * @return Future which will complete when preloader is safe to use.
     */
    public IgniteInternalFuture<Object> startFuture();

    /**
     * @return Future which will complete when initial preloading is finished.
     */
    public IgniteInternalFuture<?> syncFuture();

    /**
     * @return Future which will complete when preloading finishes on current topology.
     *
     * Future result is {@code true} in case rebalancing successfully finished at current topology.
     * Future result is {@code false} in case rebalancing cancelled or finished with missed partitions and will be
     * restarted at current or pending topology.
     *
     */
    public IgniteInternalFuture<Boolean> rebalanceFuture();

    /**
     * @return {@code true} if there is no need to force keys preloading
     *      (e.g. rebalancing has been completed).
     */
    public boolean needForceKeys();

    /**
     * Requests that preloader sends the request for the key.
     *
     * @param cctx Cache context.
     * @param keys Keys to request.
     * @param topVer Topology version, {@code -1} if not required.
     * @return Future to complete when all keys are preloaded.
     */
    public GridDhtFuture<Object> request(GridCacheContext cctx,
        Collection<KeyCacheObject> keys,
        AffinityTopologyVersion topVer);

    /**
     * Requests that preloader sends the request for the key.
     *
     * @param cctx Cache context.
     * @param req Message with keys to request.
     * @param topVer Topology version, {@code -1} if not required.
     * @return Future to complete when all keys are preloaded.
     */
    public GridDhtFuture<Object> request(GridCacheContext cctx,
        GridNearAtomicAbstractUpdateRequest req,
        AffinityTopologyVersion topVer);

    /**
     * Force Rebalance process.
     */
    public IgniteInternalFuture<Boolean> forceRebalance();

    /**
     * Handles Supply message.
     *
     * @param id Node Id.
     * @param s Supply message.
     */
    public void handleSupplyMessage(UUID id, final GridDhtPartitionSupplyMessage s);

    /**
     * Handles Demand message.
     *
     * @param idx Index.
     * @param id Node Id.
     * @param d Demand message.
     */
    public void handleDemandMessage(int idx, UUID id, GridDhtPartitionDemandMessage d);

    /**
     * @param lastFut Last future.
     */
    public void onTopologyChanged(GridDhtPartitionsExchangeFuture lastFut);

    /**
     * Dumps debug information.
     */
    public void dumpDebugInfo();

    /**
     *  Pause preloader.
     */
    public void pause();

    /**
     * Resume preloader.
     */
    public void resume();

    /**
     * Rebalance timeout for supply and demand messages in milliseconds.
     * <p>
     * The {@link IgniteConfiguration#getRebalanceTimeout()} will be used by default. If an Ignite's configuration value
     * is not provided than the {@link CacheConfiguration#getRebalanceTimeout()} will be used instead.
     *
     * @return Rebalance message timeout in milliseconds.
     */
    public long timeout();

    /**
     * The number of batches generated by supply node at rebalancing procedure start.
     * <p>
     * The {@link IgniteConfiguration#getRebalanceBatchesPrefetchCount()} will be used by default. If an Ignite's
     * configuration value is not provided than the {@link CacheConfiguration#getRebalanceBatchesPrefetchCount()}
     * will be used instead.
     *
     * @return The number of batches prefetch count.
     */
    public long batchesPrefetchCount();

    /**
     * Time in milliseconds to wait between rebalance messages to avoid overloading of CPU or network.
     * <p>
     * The {@link IgniteConfiguration#getRebalanceThrottle()} will be used by default. If an Ignite's
     * configuration value is not provided than the {@link CacheConfiguration#getRebalanceThrottle()}
     * will be used instead.
     *
     * @return Time in milliseconds to wait between rebalance messages, {@code 0} to disable throttling.
     */
    public long throttle();

    /**
     * The supply message size in bytes to be loaded within a single rebalance batch.
     * <p>
     * The {@link IgniteConfiguration#getRebalanceBatchSize()} will be used by default. If an Ignite's
     * configuration value is not provided than the {@link CacheConfiguration#getRebalanceBatchSize()}
     * will be used instead.
     *
     * @return Rebalance message size in bytes.
     */
    public int batchSize();
}
