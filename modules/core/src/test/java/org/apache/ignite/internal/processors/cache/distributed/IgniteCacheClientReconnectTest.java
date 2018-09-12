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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test for customer scenario.
 */
public class IgniteCacheClientReconnectTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

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
    private boolean client;

    /** */
    private boolean forceServerMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        if (!client) {
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

        cfg.setClientMode(client);

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
    public void testClientReconnectOnExchangeHistoryExhaustion() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_EXCHANGE_HISTORY_SIZE, "1");

        try {
            startGrids(SRV_CNT);

            client = true;

            startGridsMultiThreaded(SRV_CNT, CLIENTS_CNT);

            waitForTopology(SRV_CNT + CLIENTS_CNT);

            awaitPartitionMapExchange();

            verifyPartitionToNodeMappings();

            verifyAffinityTopologyVersions();

            verifyCacheOperationsOnClients();
        }
        finally {
            System.clearProperty(IgniteSystemProperties.IGNITE_EXCHANGE_HISTORY_SIZE);
        }
    }

    /**
     * Verifies that in case of exchange history exhaustion
     * (refer to javadoc at {@link #testClientReconnectOnExchangeHistoryExhaustion()} for more info about it)
     * clients with forceServerMode=true flag don't try to reconnect to the cluster and stop.
     *
     * @throws Exception If failed
     */
    public void testClientInForceServerModeStopsOnExchangeHistoryExhaustion() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_EXCHANGE_HISTORY_SIZE, "1");

        try {
            startGrids(SRV_CNT);

            client = true;

            forceServerMode = true;

            int clientNodes = 10;

            try {
                startGridsMultiThreaded(SRV_CNT, clientNodes);
            }
            catch (IgniteCheckedException e) {
                //Ignored: it is expected to get exception here
            }

            awaitPartitionMapExchange();

            int topSize = G.allGrids().size();

            assertTrue("Actual size: " + topSize, topSize < SRV_CNT + clientNodes);

        }
        finally {
            System.clearProperty(IgniteSystemProperties.IGNITE_EXCHANGE_HISTORY_SIZE);
        }
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
    public void testClientReconnect() throws Exception {
        startGrids(SRV_CNT);

        client = true;

        final AtomicBoolean stop = new AtomicBoolean(false);

        final AtomicInteger idx = new AtomicInteger(SRV_CNT);

        final CountDownLatch latch = new CountDownLatch(2);

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Ignite ignite = startGrid(idx.getAndIncrement());

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

                try (Ignite ignite = startGrid(clientIdx)) {
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