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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

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
     */
    public void addAssignments(GridDhtPreloaderAssignments assignments, boolean forcePreload);

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
}
