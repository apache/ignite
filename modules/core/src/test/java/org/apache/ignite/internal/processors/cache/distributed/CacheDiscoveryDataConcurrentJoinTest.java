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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.GridAtomicInteger;
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
public class CacheDiscoveryDataConcurrentJoinTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Iteration. */
    private static final int ITERATIONS = 3;

    /** */
    private boolean client;

    /** */
    private ThreadLocal<Integer> staticCaches = new ThreadLocal<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi testSpi = new TcpDiscoverySpi() {
            /** */
            private boolean delay = true;

            @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
                if (getTestIgniteInstanceName(0).equals(ignite.name())) {
                    if (msg instanceof TcpDiscoveryJoinRequestMessage) {
                        TcpDiscoveryJoinRequestMessage msg0 = (TcpDiscoveryJoinRequestMessage)msg;

                        if (delay) {
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

        cfg.setClientMode(client);

        Integer caches = staticCaches.get();

        if (caches != null) {
            cfg.setCacheConfiguration(cacheConfigurations(caches).toArray(new CacheConfiguration[caches]));

            staticCaches.remove();
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60 * 1000L;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentJoin() throws Exception {
        for (int iter = 0; iter < ITERATIONS; iter++) {
            log.info("Iteration: " + iter);

            final int NODES = 6;
            final int MAX_CACHES = 10;

            final GridAtomicInteger caches = new GridAtomicInteger();

            startGrid(0);

            final AtomicInteger idx = new AtomicInteger(1);

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int c = ThreadLocalRandom.current().nextInt(MAX_CACHES) + 1;

                    staticCaches.set(c);

                    startGrid(idx.getAndIncrement());

                    caches.setIfGreater(c);

                    return null;
                }
            }, NODES - 1, "start-node");

            assertTrue(caches.get() > 0);

            for (int i = 0; i < NODES; i++) {
                Ignite node = ignite(i);

                for (int c = 0; c < caches.get(); c++) {
                    Collection<ClusterNode> nodes = node.cluster().forCacheNodes("cache-" + c).nodes();

                    assertEquals(NODES, nodes.size());

                    checkCache(node, "cache-" + c);
                }
            }

            stopAllGrids();
        }
    }

    /**
     * @param caches Number of caches.
     * @return Cache configurations.
     */
    private Collection<CacheConfiguration> cacheConfigurations(int caches) {
        List<CacheConfiguration> ccfgs = new ArrayList<>();

        for (int i = 0; i < caches; i++)
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