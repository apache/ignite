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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;

/**
 * Test for customer scenario.
 */
public class IgniteCacheClientReconnectTest extends GridCommonAbstractTest {
    /** */
    private static final int SRV_CNT = 3;

    /** */
    private static final int CLIENTS_CNT = 3;

    /** */
    private static final int CACHES = 10;

    /** */
    private static final int PARTITIONS_CNT = 32;

    /** */
    private static final long TEST_TIME = 60_000;

    /** */
    private boolean forceServerMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        if (!cfg.isClientMode()) {
            CacheConfiguration[] ccfgs = new CacheConfiguration[CACHES];

            for (int i = 0; i < CACHES; i++) {
                CacheConfiguration ccfg = new CacheConfiguration();

                ccfg.setCacheMode(PARTITIONED);
                ccfg.setAtomicityMode(TRANSACTIONAL);
                ccfg.setBackups(1);
                ccfg.setName("cache-" + i);
                ccfg.setWriteSynchronizationMode(FULL_SYNC);
                ccfg.setAffinity(new RendezvousAffinityFunction(PARTITIONS_CNT, null));

                ccfgs[i] = ccfg;
            }

            cfg.setCacheConfiguration(ccfgs);
        }
        else
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(forceServerMode);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIME + 60_000;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * If setting IGNITE_EXCHANGE_HISTORY_SIZE is set to small value
     * it is possible that bunch of clients simultaneous start (amount > IGNITE_EXCHANGE_HISTORY_SIZE)
     * may result in ExchangeFuture for some client being flushed from exchange history.
     *
     * In that case client attempts to reconnect until success.
     *
     * After that it should get correct information about topology version and affinity distribution.
     *
     * @throws Exception If failed
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_EXCHANGE_HISTORY_SIZE, value = "1")
    public void testClientReconnectOnExchangeHistoryExhaustion() throws Exception {
        startGrids(SRV_CNT);

        startClientGridsMultiThreaded(SRV_CNT, CLIENTS_CNT);

        waitForTopology(SRV_CNT + CLIENTS_CNT);

        awaitPartitionMapExchange();

        verifyPartitionToNodeMappings();

        verifyAffinityTopologyVersions();

        verifyCacheOperationsOnClients();
    }

    /**
     * Verifies that in case of exchange history exhaustion
     * (refer to javadoc at {@link #testClientReconnectOnExchangeHistoryExhaustion()} for more info about it)
     * clients with forceServerMode=true flag don't try to reconnect to the cluster and stop.
     *
     * @throws Exception If failed
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_EXCHANGE_HISTORY_SIZE, value = "1")
    public void testClientInForceServerModeStopsOnExchangeHistoryExhaustion() throws Exception {
        startGrids(SRV_CNT);

        forceServerMode = true;

        int clientNodes = 24;

        try {
            startClientGridsMultiThreaded(SRV_CNT, clientNodes);
        }
        catch (IgniteCheckedException e) {
            //Ignored: it is expected to get exception here
        }

        awaitPartitionMapExchange();

        int topSize = G.allGrids().size();

        assertTrue("Actual size: " + topSize, topSize < SRV_CNT + clientNodes);
    }

    /**
     * Verifies basic cache operations from all clients.
     */
    private void verifyCacheOperationsOnClients() {
        for (int i = SRV_CNT; i < SRV_CNT + CLIENTS_CNT; i++) {
            IgniteEx cl = grid(i);

            if (!forceServerMode)
                assertTrue(cl.localNode().isClient());

            for (int j = 0; j < CACHES; j++) {
                IgniteCache<Object, Object> cache = cl.cache("cache-" + j);

                String keyPrefix = "cl-" + i + "_key-";

                for (int k = 0; k < 1_000; k++)
                    cache.put(keyPrefix + k, "val-" + k);

                for (int k = 999; k >= 0; k--)
                    assertEquals("val-" + k, cache.get(keyPrefix + k));
            }
        }
    }

    /**
     * Verifies that affinity mappings are the same on clients and servers.
     */
    private void verifyPartitionToNodeMappings() {
        IgniteEx refSrv = grid(0);
        String cacheName;

        for (int i = 0; i < CACHES; i++) {
            cacheName = "cache-" + i;

            Affinity<Object> refAffinity = refSrv.affinity(cacheName);

            for (int j = 0; j < PARTITIONS_CNT; j++) {
                ClusterNode refAffNode = refAffinity.mapPartitionToNode(j);

                assertNotNull("Affinity node for " + j + " partition is null", refAffNode);

                for (int k = SRV_CNT; k < SRV_CNT + CLIENTS_CNT; k++) {
                    ClusterNode clAffNode = grid(k).affinity(cacheName).mapPartitionToNode(j);

                    assertNotNull("Affinity node for " + k + " client and " + j + " partition is null", clAffNode);

                    assertEquals("Affinity node for "
                        + k
                        + " client and "
                        + j
                        + " partition is different on client",
                        refAffNode.id(),
                        clAffNode.id());
                }
            }
        }
    }

    /**
     * Verifies {@link AffinityTopologyVersion}s: one obtained from coordinator and all from each client node.
     */
    private void verifyAffinityTopologyVersions() {
        IgniteEx srv = grid(0);

        AffinityTopologyVersion srvTopVer = srv.context().discovery().topologyVersionEx();

        for (int i = SRV_CNT; i < SRV_CNT + CLIENTS_CNT; i++) {
            AffinityTopologyVersion clntTopVer = grid(i).context().discovery().topologyVersionEx();

            assertTrue(clntTopVer.equals(srvTopVer));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnect() throws Exception {
        startGrids(SRV_CNT);

        final AtomicBoolean stop = new AtomicBoolean(false);

        final AtomicInteger idx = new AtomicInteger(SRV_CNT);

        final CountDownLatch latch = new CountDownLatch(2);

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Ignite ignite = startClientGrid(idx.getAndIncrement());

                latch.countDown();

                assertTrue(ignite.cluster().localNode().isClient());

                while (!stop.get())
                    putGet(ignite);

                return null;
            }
        }, 2, "client-thread");

        try {
            assertTrue(latch.await(10_000, MILLISECONDS));

            long end = System.currentTimeMillis() + TEST_TIME;

            int clientIdx = idx.getAndIncrement();

            int cnt = 0;

            while (System.currentTimeMillis() < end) {
                log.info("Iteration: " + cnt++);

                try (Ignite ignite = startClientGrid(clientIdx)) {
                    assertTrue(ignite.cluster().localNode().isClient());

                    assertEquals(6, ignite.cluster().nodes().size());

                    putGet(ignite);
                }
            }

            stop.set(true);

            fut.get();
        }
        finally {
            stop.set(true);
        }
    }

    /**
     * Verifies that new node ID generated by client on disconnect replaces old ID only on RECONNECTED event.
     *
     * Old node ID is still available on DISCONNECTED event.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientIdUpdateOnReconnect() throws Exception {
        startGrid(0);

        IgniteEx clNode = startClientGrid(1);
        UUID oldNodeId = clNode.localNode().id();

        awaitPartitionMapExchange();

        stopGrid(0);

        AtomicReference<UUID> idOnDisconnect = new AtomicReference<>();
        AtomicReference<UUID> idOnReconnect = new AtomicReference<>();

        CountDownLatch disconnectedLatch = new CountDownLatch(1);
        CountDownLatch reconnectedLatch = new CountDownLatch(1);

        clNode.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event event) {
                switch (event.type()) {
                    case EVT_CLIENT_NODE_DISCONNECTED: {
                        idOnDisconnect.set(event.node().id());

                        disconnectedLatch.countDown();

                        break;
                    }

                    case EVT_CLIENT_NODE_RECONNECTED: {
                        idOnReconnect.set(event.node().id());

                        reconnectedLatch.countDown();

                        break;
                    }
                }

                return true;
            }
        }, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED);

        assertTrue(GridTestUtils.waitForCondition(() -> disconnectedLatch.getCount() == 0, 10_000));
        assertEquals(oldNodeId, idOnDisconnect.get());

        startGrid(0);

        assertTrue(GridTestUtils.waitForCondition(() -> reconnectedLatch.getCount() == 0, 10_000));
        assertEquals(grid(1).localNode().id(), idOnReconnect.get());
    }

    /**
     * @param ignite Ignite.
     */
    private void putGet(Ignite ignite) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < CACHES; i++) {
            IgniteCache<Object, Object> cache = ignite.cache("cache-" + i);

            assertNotNull(cache);

            Integer key = rnd.nextInt(0, 100_000);

            cache.put(key, key);

            assertEquals(key, cache.get(key));
        }
    }
}
