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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloaderAssignments;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_THREAD_DUMP_ON_EXCHANGE_TIMEOUT;

/**
 * Exchange future thread. All exchanges happen only by one thread and next
 * exchange will not start until previous one completes.
 */
public class CachePartitionExchangeWorker<K, V> extends GridWorker {
    /** Cache context. */
    private final GridCacheSharedContext<K, V> cctx;

    /** Exchange manager. */
    private final GridCachePartitionExchangeManager<K, V> exchMgr;

    /** Future queue. */
    private final LinkedBlockingDeque<GridDhtPartitionsExchangeFuture> futQ =
        new LinkedBlockingDeque<>();

    /** Busy flag used as performance optimization to stop current preloading. */
    private volatile boolean busy;

    /**
     * Constructor.
     *
     * @param exchMgr Exchange manager.
     * @param log Logger.
     */
    public CachePartitionExchangeWorker(GridCachePartitionExchangeManager<K, V> exchMgr, IgniteLogger log) {
        super(exchMgr.context().igniteInstanceName(), "partition-exchanger", log);

        this.cctx = exchMgr.context();

        this.exchMgr = exchMgr;
    }

    /**
     * Add first exchange future.
     *
     * @param fut Future.
     */
    public void addFirstFuture(GridDhtPartitionsExchangeFuture fut) {
        futQ.addFirst(fut);
    }

    /**
     * @param exchFut Exchange future.
     */
    void addFuture(GridDhtPartitionsExchangeFuture exchFut) {
        assert exchFut != null;

        if (!exchFut.dummy() || (exchangeQueueIsEmpty() && !busy))
            futQ.offer(exchFut);

        if (log.isDebugEnabled())
            log.debug("Added exchange future to exchange worker: " + exchFut);
    }

    /**
     * Dump debug info.
     */
    public void dumpFuturesDebugInfo() {
        U.warn(log, "Pending exchange futures:");

        for (GridDhtPartitionsExchangeFuture fut : futQ)
            U.warn(log, ">>> " + fut);
    }

    /**
     * @return {@code True} iif exchange queue is empty.
     */
    public boolean exchangeQueueIsEmpty() {
        return futQ.isEmpty();
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
        long timeout = cctx.gridConfig().getNetworkTimeout();

        int cnt = 0;

        while (!isCancelled()) {
            GridDhtPartitionsExchangeFuture exchFut = null;

            cnt++;

            try {
                boolean preloadFinished = true;

                for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                    preloadFinished &= cacheCtx.preloader() != null && cacheCtx.preloader().syncFuture().isDone();

                    if (!preloadFinished)
                        break;
                }

                // If not first preloading and no more topology events present.
                if (!cctx.kernalContext().clientNode() && exchangeQueueIsEmpty() && preloadFinished)
                    timeout = cctx.gridConfig().getNetworkTimeout();

                // After workers line up and before preloading starts we initialize all futures.
                if (log.isDebugEnabled()) {
                    Collection<IgniteInternalFuture> unfinished = new HashSet<>();

                    for (GridDhtPartitionsExchangeFuture fut : exchMgr.exchangeFutures()) {
                        if (!fut.isDone())
                            unfinished.add(fut);
                    }

                    log.debug("Before waiting for exchange futures [futs" + unfinished + ", worker=" + this + ']');
                }

                // Take next exchange future.
                if (isCancelled())
                    Thread.currentThread().interrupt();

                exchFut = futQ.poll(timeout, MILLISECONDS);

                if (exchFut == null)
                    continue; // Main while loop.

                busy = true;

                Map<Integer, GridDhtPreloaderAssignments> assignsMap = null;

                boolean dummyReassign = exchFut.dummyReassign();
                boolean forcePreload = exchFut.forcePreload();

                try {
                    if (isCancelled())
                        break;

                    if (!exchFut.dummy() && !exchFut.forcePreload()) {
                        exchMgr.lastTopologyFuture(exchFut);

                        exchFut.init();

                        int dumpedObjects = 0;

                        while (true) {
                            try {
                                exchFut.get(2 * cctx.gridConfig().getNetworkTimeout(), TimeUnit.MILLISECONDS);

                                break;
                            }
                            catch (IgniteFutureTimeoutCheckedException ignored) {
                                U.warn(log, "Failed to wait for partition map exchange [" +
                                    "topVer=" + exchFut.topologyVersion() +
                                    ", node=" + cctx.localNodeId() + "]. " +
                                    "Dumping pending objects that might be the cause: ");

                                if (dumpedObjects < GridDhtPartitionsExchangeFuture.DUMP_PENDING_OBJECTS_THRESHOLD) {
                                    try {
                                        exchMgr.dumpDebugInfo(exchFut.topologyVersion());
                                    }
                                    catch (Exception e) {
                                        U.error(log, "Failed to dump debug information: " + e, e);
                                    }

                                    if (IgniteSystemProperties.getBoolean(IGNITE_THREAD_DUMP_ON_EXCHANGE_TIMEOUT, false))
                                        U.dumpThreads(log);

                                    dumpedObjects++;
                                }
                            }
                        }


                        if (log.isDebugEnabled())
                            log.debug("After waiting for exchange future [exchFut=" + exchFut + ", worker=" +
                                this + ']');

                        boolean changed = false;

                        // Just pick first worker to do this, so we don't
                        // invoke topology callback more than once for the
                        // same event.
                        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                            if (cacheCtx.isLocal())
                                continue;

                            changed |= cacheCtx.topology().afterExchange(exchFut);
                        }

                        if (!cctx.kernalContext().clientNode() && changed && exchangeQueueIsEmpty())
                            exchMgr.refreshPartitions();
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Got dummy exchange (will reassign)");

                        if (!dummyReassign) {
                            timeout = 0; // Force refresh.

                            continue;
                        }
                    }

                    if (!exchFut.skipPreload()) {
                        assignsMap = new HashMap<>();

                        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                            long delay = cacheCtx.config().getRebalanceDelay();

                            GridDhtPreloaderAssignments assigns = null;

                            // Don't delay for dummy reassigns to avoid infinite recursion.
                            if (delay == 0 || forcePreload)
                                assigns = cacheCtx.preloader().assign(exchFut);

                            assignsMap.put(cacheCtx.cacheId(), assigns);
                        }
                    }
                }
                finally {
                    // Must flip busy flag before assignments are given to demand workers.
                    busy = false;
                }

                if (assignsMap != null) {
                    int size = assignsMap.size();

                    NavigableMap<Integer, List<Integer>> orderMap = new TreeMap<>();

                    for (Map.Entry<Integer, GridDhtPreloaderAssignments> e : assignsMap.entrySet()) {
                        int cacheId = e.getKey();

                        GridCacheContext<K, V> cacheCtx = cctx.cacheContext(cacheId);

                        int order = cacheCtx.config().getRebalanceOrder();

                        if (orderMap.get(order) == null)
                            orderMap.put(order, new ArrayList<Integer>(size));

                        orderMap.get(order).add(cacheId);
                    }

                    Runnable r = null;

                    List<String> rebList = new LinkedList<>();

                    boolean assignsCancelled = false;

                    for (Integer order : orderMap.descendingKeySet()) {
                        for (Integer cacheId : orderMap.get(order)) {
                            GridCacheContext<K, V> cacheCtx = cctx.cacheContext(cacheId);

                            GridDhtPreloaderAssignments assigns = assignsMap.get(cacheId);

                            if (assigns != null)
                                assignsCancelled |= assigns.cancelled();

                            // Cancels previous rebalance future (in case it's not done yet).
                            // Sends previous rebalance stopped event (if necessary).
                            // Creates new rebalance future.
                            // Sends current rebalance started event (if necessary).
                            // Finishes cache sync future (on empty assignments).
                            Runnable cur = cacheCtx.preloader().addAssignments(assigns,
                                forcePreload,
                                cnt,
                                r,
                                exchFut.forcedRebalanceFuture());

                            if (cur != null) {
                                rebList.add(U.maskName(cacheCtx.name()));

                                r = cur;
                            }
                        }
                    }

                    if (assignsCancelled) { // Pending exchange.
                        U.log(log, "Skipping rebalancing (obsolete exchange ID) " +
                            "[top=" + exchFut.topologyVersion() + ", evt=" + exchFut.discoveryEvent().name() +
                            ", node=" + exchFut.discoveryEvent().eventNode().id() + ']');
                    }
                    else if (r != null) {
                        Collections.reverse(rebList);

                        U.log(log, "Rebalancing scheduled [order=" + rebList + "]");

                        if (exchangeQueueIsEmpty()) {
                            U.log(log, "Rebalancing started " +
                                "[top=" + exchFut.topologyVersion() + ", evt=" + exchFut.discoveryEvent().name() +
                                ", node=" + exchFut.discoveryEvent().eventNode().id() + ']');

                            r.run(); // Starts rebalancing routine.
                        }
                        else
                            U.log(log, "Skipping rebalancing (obsolete exchange ID) " +
                                "[top=" + exchFut.topologyVersion() + ", evt=" + exchFut.discoveryEvent().name() +
                                ", node=" + exchFut.discoveryEvent().eventNode().id() + ']');
                    }
                    else
                        U.log(log, "Skipping rebalancing (nothing scheduled) " +
                            "[top=" + exchFut.topologyVersion() + ", evt=" + exchFut.discoveryEvent().name() +
                            ", node=" + exchFut.discoveryEvent().eventNode().id() + ']');
                }
            }
            catch (IgniteInterruptedCheckedException e) {
                throw e;
            }
            catch (IgniteClientDisconnectedCheckedException ignored) {
                return;
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to wait for completion of partition map exchange " +
                    "(preloading will not start): " + exchFut, e);
            }
        }
    }
}
