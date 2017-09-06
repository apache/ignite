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

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 *
 */
@SuppressWarnings("unchecked")
public class CacheStartOnJoinTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Iteration. */
    private static final int ITERATIONS = 3;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi testSpi = new TcpDiscoverySpi() {
            /** */
            private boolean delay = true;

            @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg, long timeout) throws IOException, IgniteCheckedException {
                super.writeToSocket(sock, out, msg, timeout);
            }

            @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
                if (getTestIgniteInstanceName(0).equals(ignite.name())) {
                    if (msg instanceof TcpDiscoveryJoinRequestMessage) {
                        TcpDiscoveryJoinRequestMessage msg0 = (TcpDiscoveryJoinRequestMessage)msg;

                        if (msg0.client() && delay) {
                            log.info("Delay join processing: " + msg0);

                            delay = false;

                            doSleep(5000);
                        }
                    }
                }

                super.startMessageProcess(msg);
            }
        };

        testSpi.setIpFinder(ipFinder);
        testSpi.setJoinTimeout(60_000);

        cfg.setDiscoverySpi(testSpi);

        MemoryConfiguration memCfg = new MemoryConfiguration();
        memCfg.setPageSize(1024);
        memCfg.setDefaultMemoryPolicySize(50 * 1024 * 1024);

        cfg.setMemoryConfiguration(memCfg);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60 * 1000L;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(IgniteSystemProperties.IGNITE_START_CACHES_ON_JOIN, "true");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        System.clearProperty(IgniteSystemProperties.IGNITE_START_CACHES_ON_JOIN);

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentClientsStart1() throws Exception {
        concurrentClientsStart(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentClientsStart2() throws Exception {
        concurrentClientsStart(true);
    }

    /**
     * @param createCache If {@code true} concurrently calls getOrCreateCaches.
     * @throws Exception If failed.
     */
    private void concurrentClientsStart(boolean createCache) throws Exception {
        for (int i = 0; i < ITERATIONS; i++) {
            try {
                log.info("Iteration: " + (i + 1) + '/' + ITERATIONS);

                doTest(createCache);
            }
            finally {
                stopAllGrids(true);
            }
        }
    }

    /**
     * @param createCache If {@code true} concurrently calls getOrCreateCaches.
     * @throws Exception If failed.
     */
    private void doTest(final boolean createCache) throws Exception {
        client = false;

        final int CLIENTS = 5;
        final int SRVS = 4;

        Ignite srv = startGrids(SRVS);

        srv.getOrCreateCaches(cacheConfigurations());

        final CyclicBarrier b = new CyclicBarrier(CLIENTS);

        client = true;

        GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
            @Override public void apply(Integer idx) {
                try {
                    b.await();

                    Ignite node = startGrid(idx + SRVS);

                    if (createCache) {
                        for (int c = 0; c < 5; c++) {
                            for (IgniteCache cache : node.getOrCreateCaches(cacheConfigurations())) {
                                cache.put(c, c);

                                assertEquals(c, cache.get(c));
                            }
                        }
                    }
                }
                catch (Exception e) {
                    throw new IgniteException(e);
                }
            }
        }, CLIENTS, "start-client");

        final int NODES = CLIENTS + SRVS;

        for (int i = 0; i < CLIENTS + 1; i++) {
            Ignite node = ignite(i);

            log.info("Check node: " + node.name());

            assertEquals((Boolean)(i >= SRVS), node.configuration().isClientMode());

            for (int c = 0; c < 5; c++) {
                Collection<ClusterNode> nodes = node.cluster().forCacheNodes("cache-" + c).nodes();

                assertEquals(NODES, nodes.size());

                checkCache(node, "cache-" + c);
            }

            for (int c = 0; c < 5; c++) {
                for (IgniteCache cache : node.getOrCreateCaches(cacheConfigurations())) {
                    cache.put(c, c);

                    assertEquals(c, cache.get(c));
                }
            }
        }
    }

    /**
     * @return Cache configurations.
     */
    private Collection<CacheConfiguration> cacheConfigurations() {
        List<CacheConfiguration> ccfgs = new ArrayList<>();

        for (int i = 0; i < 5; i++)
            ccfgs.add(cacheConfiguration("cache-" + i));

        return ccfgs;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String cacheName) {
        CacheConfiguration ccfg = new CacheConfiguration(cacheName);

        ccfg.setName(cacheName);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 16));

        return ccfg;
    }
    /**
     * @param node Node.
     * @param cacheName Cache name.
     */
    private void checkCache(Ignite node, final String cacheName) {
        assertNotNull(((IgniteKernal)node).context().cache().cache(cacheName));
    }
}