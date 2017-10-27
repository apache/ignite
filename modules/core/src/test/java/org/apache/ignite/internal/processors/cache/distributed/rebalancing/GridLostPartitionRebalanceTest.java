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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST;

/**
 * Tests EVT_CACHE_REBALANCE_PART_DATA_LOST events.
 */
public class GridLostPartitionRebalanceTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "test";

    /** Latch. */
    private static CountDownLatch latch;

    /** Count. */
    private static AtomicInteger cnt;

    /** Failed flag. */
    private static boolean failed;

    /** Backups. */
    private int backups;

    /** Expected events. */
    private int expEvts;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setNodeFilter(NODE_FILTER);
        ccfg.setBackups(backups);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        cfg.setIncludeEventTypes(EVT_CACHE_REBALANCE_PART_DATA_LOST);

        Map<IgnitePredicate<? extends Event>, int[]> listeners = new HashMap<>();

        listeners.put(new Listener(), new int[]{EVT_CACHE_REBALANCE_PART_DATA_LOST});

        cfg.setLocalEventListeners(listeners);

        cfg.setClientMode(gridName.contains("client"));

        final Map<String, Object> attrs = new HashMap<>();

        attrs.put("node.name", gridName);

        cfg.setUserAttributes(attrs);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** Filter. */
    private static final IgnitePredicate NODE_FILTER = new IgnitePredicate<ClusterNode>() {
        /** */
        private static final long serialVersionUID = 0L;

        @Override public boolean apply(ClusterNode node) {
            return !"basenode".equals(node.attribute("node.name"));
        }
    };

    /**
     * @throws Exception If failed.
     */
    public void testPartDataLostEvent1Backup() throws Exception {
        expEvts = 3;
        backups = 1;

        checkEvents();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartDataLostEventNoBackups() throws Exception {
        expEvts = 4;
        backups = 0;

        checkEvents();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkEvents() throws Exception {
        latch = new CountDownLatch(expEvts);
        cnt = new AtomicInteger(0);

        List<Ignite> srvrs = new ArrayList<>();

        // Client router. It always up, so client is guaranteed to get
        // event.
        srvrs.add(startGrid("basenode"));

        Ignite client = startGrid("client");

        srvrs.add(startGrid("server-1"));
        srvrs.add(startGrid("server-2"));
        srvrs.add(startGrid("server-3"));

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache = client.cache(CACHE_NAME);

        for (int i = 0; i < 10_000; i++)
            cache.put(i, i);

        // Stop node with 0 partition.
        Set<ClusterNode> nodes = new HashSet<>(client.affinity(CACHE_NAME).mapPartitionToPrimaryAndBackups(0));

        final List<String> stopped = stopAffinityNodes(srvrs, nodes);

        // Check that all nodes (and clients) got notified.
        assert latch.await(15, TimeUnit.SECONDS) : latch.getCount();

        // Check that exchange was not finished when event received.
        assertFalse("Exchange was finished when event received.", failed);

        U.sleep(4_000);

        assertEquals("Fired more events than expected", expEvts, cnt.get());

        startNodes(stopped);

        assertEquals("Fired unexpected events", expEvts, cnt.get());
    }

    /**
     * @param nodeNames Node names.
     */
    private void startNodes(List<String> nodeNames) throws Exception {
        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (final String nodeName : nodeNames) {
            IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    startGrid(nodeName);

                    return null;
                }
            });

            futs.add(fut);
        }

        for (IgniteInternalFuture<?> fut : futs)
            fut.get();
    }

    /**
     * @param srvrs Servers.
     * @param nodes Nodes.
     */
    @NotNull private List<String> stopAffinityNodes(List<Ignite> srvrs, Set<ClusterNode> nodes) throws IgniteCheckedException {
        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        final List<String> stopped = new ArrayList<>();

        for (final Ignite srv : srvrs) {
            final ClusterNode node = srv.cluster().localNode();

            if (nodes.contains(node)) {
                IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        srv.close();

                        System.out.println(">> Stopped " + srv.name() + " " + node.id());

                        stopped.add(srv.name());

                        return null;
                    }
                });

                futs.add(fut);
            }
        }

        for (IgniteInternalFuture<?> fut : futs)
            fut.get();

        return stopped;
    }

    /**
     *
     */
    private static class Listener implements IgnitePredicate<CacheRebalancingEvent> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Ignite. */
        @SuppressWarnings("unused")
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public boolean apply(CacheRebalancingEvent evt) {
            int part = evt.partition();

            // AtomicBoolean because new owner will produce two events.
            if (part == 0 && CACHE_NAME.equals(evt.cacheName())) {
                System.out.println(">> Received event for 0 partition. [node=" + ignite.name() + ", evt=" + evt
                    + ", thread=" + Thread.currentThread().getName() + ']');

                latch.countDown();

                cnt.incrementAndGet();

                if (exchangeCompleted(ignite))
                    failed = true;
            }

            return true;
        }
    }

    /**
     * @param ignite Ignite.
     * @return {@code True} if exchange finished.
     */
    private static boolean exchangeCompleted(Ignite ignite) {
        GridKernalContext ctx = ((IgniteKernal)ignite).context();

        List<GridDhtPartitionsExchangeFuture> futs = ctx.cache().context().exchange().exchangeFutures();

        for (GridDhtPartitionsExchangeFuture fut : futs) {
            if (!fut.isDone())
                return false;
        }

        return true;
    }
}
