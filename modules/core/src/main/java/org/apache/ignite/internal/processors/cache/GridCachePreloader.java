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
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessageV2;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloaderAssignments;
import org.apache.ignite.lang.IgnitePredicate;
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
     * Stops preloading.
     */
    public void stop();

    /**
     * Kernal start callback.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void onKernalStart() throws IgniteCheckedException;

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
     * Callback by exchange manager when new exchange future is added to worker.
     */
    public void onExchangeFutureAdded();

    /**
     * Updates last exchange future.
     *
     * @param lastFut Last future.
     */
    public void updateLastExchangeFuture(GridDhtPartitionsExchangeFuture lastFut);

    /**
     * @param exchFut Exchange future to assign.
     * @return Assignments.
     */
    public GridDhtPreloaderAssignments assign(GridDhtPartitionsExchangeFuture exchFut);

    /**
     * Adds assignments to preloader.
     *
     * @param assignments Assignments to add.
     * @param forcePreload Force preload flag.
     * @param caches Rebalancing of these caches will be finished before this started.
     * @param cnt Counter.
     */
    public Callable<Boolean> addAssignments(GridDhtPreloaderAssignments assignments, boolean forcePreload,
        Collection<String> caches, int cnt);

    /**
     * @param p Preload predicate.
     */
    public void preloadPredicate(IgnitePredicate<GridCacheEntryInfo> p);

    /**
     * @return Preload predicate. If not {@code null}, will evaluate each preloaded entry during
     *      send and receive, and if predicate evaluates to {@code false}, entry will be skipped.
     */
    public IgnitePredicate<GridCacheEntryInfo> preloadPredicate();

    /**
     * @return Future which will complete when preloader is safe to use.
     */
    public IgniteInternalFuture<Object> startFuture();

    /**
     * @return Future which will complete when preloading is finished.
     */
    public IgniteInternalFuture<?> syncFuture();

    /**
     * @return Future which will complete when preloading finishes on current topology.
     *
     * Future result is {@code true} in case rebalancing successfully finished at current topology.
     * Future result is {@code false} in case rebalancing cancelled or finished with missed partitions and will be
     * restarted at current or pending topology.
     *
     * Note that topology change creates new futures and finishes previous.
     */
    public IgniteInternalFuture<Boolean> rebalanceFuture();

    /**
     * Requests that preloader sends the request for the key.
     *
     * @param keys Keys to request.
     * @param topVer Topology version, {@code -1} if not required.
     * @return Future to complete when all keys are preloaded.
     */
    public IgniteInternalFuture<Object> request(Collection<KeyCacheObject> keys, AffinityTopologyVersion topVer);

    /**
     * Force preload process.
     */
    public void forcePreload();

    /**
     * Unwinds undeploys.
     */
    public void unwindUndeploys();

    /**
     * Handles Supply message.
     *
     * @param idx Index.
     * @param id Node Id.
     * @param s Supply message.
     */
    public void handleSupplyMessage(int idx, UUID id, final GridDhtPartitionSupplyMessageV2 s);

    /**
     * Handles Demand message.
     *
     * @param idx Index.
     * @param id Node Id.
     * @param d Demand message.
     */
    public void handleDemandMessage(int idx, UUID id, GridDhtPartitionDemandMessage d);

    /**
     * Evicts partition asynchronously.
     *
     * @param part Partition.
     */
    public void evictPartitionAsync(GridDhtLocalPartition part);

    /**
     * Handles new topology.
     *
     * @param topVer Topology version.
     */
    public void onTopologyChanged(AffinityTopologyVersion topVer);

    /**
     * Dumps debug information.
     */
    public void dumpDebugInfo();
}
