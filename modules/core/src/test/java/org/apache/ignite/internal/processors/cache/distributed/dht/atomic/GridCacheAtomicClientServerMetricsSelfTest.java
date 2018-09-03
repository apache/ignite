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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;

public class GridCacheAtomicClientServerMetricsSelfTest extends GridCommonAbstractTest {
    /** Number of nodes. */
    private final static int GRID_CNT = 3;

    /** Server node index. */
    private static final int SERVER_NODE = 0;

    /** Client node index. */
    private static final int CLIENT_NODE = 2;

    /** */
    private static TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    private int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(GRID_CNT - 1);

        Ignition.setClientMode(true);

        startGrid(CLIENT_NODE);

        Ignition.setClientMode(false);
    }

    /** {@inheritDoc} */
    protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * Creates a new Cache Configuration for the given cache mode.
     *
     * @param mode Cache mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> getCacheConfiguration(CacheMode mode) {
        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>();

        cacheCfg.setCacheMode(mode);
        cacheCfg.setAtomicityMode(ATOMIC);
        cacheCfg.setStatisticsEnabled(true);
        cacheCfg.setName("metrics");

        return cacheCfg;
    }

    /**
     * Wait for {@link EventType#EVT_NODE_METRICS_UPDATED} event will be receieved.
     */
    private void awaitMetricsUpdate() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(GRID_CNT * 2);

        IgnitePredicate<Event> lsnr = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event event) {
                latch.countDown();
                return true;
            }
        };

        for (int i = 0; i < gridCount(); ++i)
            grid(i).events().localListen(lsnr, EVT_NODE_METRICS_UPDATED);

        latch.await();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedGetAvgTime() throws Exception {
        testGetAvgTime(PARTITIONED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedGetAvgTime() throws Exception {
        testGetAvgTime(REPLICATED);
    }

    /**
     * @param mode Cache mode.
     * @throws Exception If failed.
     */
    private void testGetAvgTime(CacheMode mode) throws Exception {
        IgniteCache<Integer, Integer> cache = null;

        try {
            cache = grid(CLIENT_NODE).getOrCreateCache(getCacheConfiguration(mode));

            ThreadLocalRandom rand = ThreadLocalRandom.current();

            final int numOfKeys = 500;
            for (int i = 0; i < numOfKeys; ++i)
                cache.put(i, rand.nextInt(12_000_000));

            for (int i = 0; i < numOfKeys; ++i)
                cache.get(i);

            awaitMetricsUpdate();

            ClusterGroup clientGroup = grid(CLIENT_NODE).cluster().forClients();
            ClusterGroup serverGroup = grid(SERVER_NODE).cluster().forServers();

            CacheMetrics clientMetrics = cache.metrics(clientGroup);
            CacheMetrics serverMetrics = cache.metrics(serverGroup);

            assertEquals(clientMetrics.getAveragePutTime(), 0.0, 0.0);
            assertEquals(clientMetrics.getAverageGetTime(), 0.0, 0.0);

            assertTrue(serverMetrics.getAveragePutTime() > 0.0);
            assertTrue(serverMetrics.getAverageGetTime() > 0.0);
        }
        finally {
            if (cache != null)
                cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedGetAndPutAvgTime() throws Exception {
        testGetAndPutAvgTime(PARTITIONED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedGetAndPutAvgTime() throws Exception {
        testGetAndPutAvgTime(REPLICATED);
    }

    /**
     * @param mode Cache mode.
     * @throws Exception If failed.
     */
    private void testGetAndPutAvgTime(CacheMode mode) throws Exception {
        IgniteCache<Integer, Integer> cache = null;

        try {
            cache = grid(CLIENT_NODE).getOrCreateCache(getCacheConfiguration(mode));

            ThreadLocalRandom rand = ThreadLocalRandom.current();

            final int numOfKeys = 500;
            for (int i = 0; i < numOfKeys; ++i)
                cache.getAndPut(i, rand.nextInt(12_000_000));

            awaitMetricsUpdate();

            ClusterGroup clientGroup = grid(CLIENT_NODE).cluster().forClients();
            ClusterGroup serverGroup = grid(SERVER_NODE).cluster().forServers();

            CacheMetrics clientMetrics = cache.metrics(clientGroup);
            CacheMetrics serverMetrics = cache.metrics(serverGroup);

            assertEquals(clientMetrics.getAveragePutTime(), 0.0, 0.0);
            assertEquals(clientMetrics.getAverageGetTime(), 0.0, 0.0);

            assertTrue(serverMetrics.getAveragePutTime() > 0.0);
            assertTrue(serverMetrics.getAverageGetTime() > 0.0);
        }
        finally {
            if (cache != null)
                cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedRemoveAvgTime() throws Exception {
        testRemoveAvgTime(PARTITIONED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedRemoveAvgTime() throws Exception {
        testRemoveAvgTime(REPLICATED);
    }

    /**
     * @param mode Cache mode.
     * @throws Exception If failed.
     */
    private void testRemoveAvgTime(CacheMode mode) throws Exception {
        IgniteCache<Integer, Integer> cache = null;

        try {
            cache = grid(CLIENT_NODE).getOrCreateCache(getCacheConfiguration(mode));

            ThreadLocalRandom rand = ThreadLocalRandom.current();

            final int numOfKeys = 500;
            for (int i = 0; i < numOfKeys; ++i)
                cache.put(i, rand.nextInt(12_000_000));

            for (int i = 0; i < numOfKeys; ++i)
                cache.remove(i);

            awaitMetricsUpdate();

            ClusterGroup clientGroup = grid(CLIENT_NODE).cluster().forClients();
            ClusterGroup serverGroup = grid(SERVER_NODE).cluster().forServers();

            CacheMetrics clientMetrics = cache.metrics(clientGroup);
            CacheMetrics serverMetrics = cache.metrics(serverGroup);

            assertEquals(clientMetrics.getAveragePutTime(), 0.0, 0.0);
            assertEquals(clientMetrics.getAverageRemoveTime(), 0.0, 0.0);

            assertTrue(serverMetrics.getAveragePutTime() > 0.0);
            assertTrue(serverMetrics.getAverageRemoveTime() > 0.0);
        }
        finally {
            if (cache != null)
                cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedGetAllAvgTime() throws Exception {
        testGetAllAvgTime(PARTITIONED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedGetAllAvgTime() throws Exception {
        testGetAllAvgTime(REPLICATED);
    }

    /**
     * @param mode Cache mode.
     * @throws Exception If failed.
     */
    private void testGetAllAvgTime(CacheMode mode) throws Exception {
        IgniteCache<Integer, Integer> cache = null;

        try {
            cache = grid(CLIENT_NODE).getOrCreateCache(getCacheConfiguration(mode));

            ThreadLocalRandom rand = ThreadLocalRandom.current();

            Set<Integer> keys = new TreeSet<>();

            final int numOfKeys = 500;
            for (int i = 0; i < numOfKeys; ++i) {
                cache.put(i, rand.nextInt(12_000_000));

                keys.add(i);
            }

            cache.getAll(keys);

            awaitMetricsUpdate();

            ClusterGroup clientGroup = grid(CLIENT_NODE).cluster().forClients();
            ClusterGroup serverGroup = grid(SERVER_NODE).cluster().forServers();

            CacheMetrics clientMetrics = cache.metrics(clientGroup);
            CacheMetrics serverMetrics = cache.metrics(serverGroup);

            assertEquals(clientMetrics.getAveragePutTime(), 0.0, 0.0);
            assertEquals(clientMetrics.getAverageGetTime(), 0.0, 0.0);

            assertTrue(serverMetrics.getAveragePutTime() > 0.0);
            assertTrue(serverMetrics.getAverageGetTime() > 0.0);
        }
        finally {
            if (cache != null)
                cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedPutAllAvgTime() throws Exception {
        testPutAllAvgTime(PARTITIONED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedPutAllAvgTime() throws Exception {
        testPutAllAvgTime(REPLICATED);
    }

    /**
     * @param mode Cache mode.
     * @throws Exception If failed.
     */
    private void testPutAllAvgTime(CacheMode mode) throws Exception {
        IgniteCache<Integer, Integer> cache = null;

        try {
            cache = grid(CLIENT_NODE).getOrCreateCache(getCacheConfiguration(mode));

            ThreadLocalRandom rand = ThreadLocalRandom.current();

            Map<Integer, Integer> values = new TreeMap<>();

            final int numOfKeys = 500;
            for (int i = 0; i < numOfKeys; ++i)
                values .put(i, rand.nextInt(12_000_000));

            cache.putAll(values);

            awaitMetricsUpdate();

            ClusterGroup clientGroup = grid(CLIENT_NODE).cluster().forClients();
            ClusterGroup serverGroup = grid(SERVER_NODE).cluster().forServers();

            CacheMetrics clientMetrics = cache.metrics(clientGroup);
            CacheMetrics serverMetrics = cache.metrics(serverGroup);

            assertEquals(clientMetrics.getAveragePutTime(), 0.0, 0.0);

            assertTrue(serverMetrics.getAveragePutTime() > 0.0);
        }
        finally {
            if (cache != null)
                cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedRemoveAllAvgTime() throws Exception {
        testRemoveAllAvgTime(PARTITIONED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedRemoveAllAvgTime() throws Exception {
        testRemoveAllAvgTime(REPLICATED);
    }

    /**
     * @param mode Cache mode.
     * @throws Exception If failed.
     */
    private void testRemoveAllAvgTime(CacheMode mode) throws Exception {
        IgniteCache<Integer, Integer> cache = null;

        try {
            cache = grid(CLIENT_NODE).getOrCreateCache(getCacheConfiguration(mode));

            ThreadLocalRandom rand = ThreadLocalRandom.current();

            Set<Integer> keys = new TreeSet<>();

            final int numOfKeys = 500;
            for (int i = 0; i < numOfKeys; ++i) {
                cache.put(i, rand.nextInt(12_000_000));

                keys.add(i);
            }

            cache.removeAll(keys);

            awaitMetricsUpdate();

            ClusterGroup clientGroup = grid(CLIENT_NODE).cluster().forClients();
            ClusterGroup serverGroup = grid(SERVER_NODE).cluster().forServers();

            CacheMetrics clientMetrics = cache.metrics(clientGroup);
            CacheMetrics serverMetrics = cache.metrics(serverGroup);

            assertEquals(clientMetrics.getAveragePutTime(), 0.0, 0.0);
            assertEquals(clientMetrics.getAverageRemoveTime(), 0.0, 0.0);

            assertTrue(serverMetrics.getAveragePutTime() > 0.0);
            assertTrue(serverMetrics.getAverageRemoveTime() > 0.0);
        }
        finally {
            if (cache != null)
                cache.destroy();
        }
    }
}
