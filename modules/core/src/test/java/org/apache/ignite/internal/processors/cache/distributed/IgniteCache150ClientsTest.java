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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ThreadLocalRandom8;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 *
 */
public class IgniteCache150ClientsTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int CACHES = 10;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLocalHost("127.0.0.1");
        cfg.setNetworkTimeout(30_000);
        cfg.setConnectorConfiguration(null);
        cfg.setPeerClassLoadingEnabled(false);
        cfg.setTimeServerPortRange(200);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSocketWriteTimeout(200);
        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setLocalPortRange(200);
        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setJoinTimeout(0);
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setMaxMissedClientHeartbeats(200);

        cfg.setClientMode(!gridName.equals(getTestGridName(0)));

        CacheConfiguration[] ccfgs = new CacheConfiguration[CACHES];

        for (int i = 0 ; i < ccfgs.length; i++) {
            CacheConfiguration ccfg = new CacheConfiguration();

            ccfg.setCacheMode(PARTITIONED);
            ccfg.setAtomicityMode(i % 2 == 0 ? ATOMIC : TRANSACTIONAL);
            ccfg.setWriteSynchronizationMode(PRIMARY_SYNC);
            ccfg.setBackups(1);

            ccfg.setName("cache-" + i);

            ccfgs[i] = ccfg;
        }

        cfg.setCacheConfiguration(ccfgs);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60_000;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void test150Clients() throws Exception {
        Ignite srv = startGrid(0);

        assertFalse(srv.configuration().isClientMode());

        final int CLIENTS = 150;

        final AtomicInteger idx = new AtomicInteger(1);

        final CountDownLatch latch = new CountDownLatch(CLIENTS);

        final List<String> cacheNames = new ArrayList<>();

        for (int i = 0; i < CACHES; i++)
            cacheNames.add("cache-" + i);

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                boolean cnt = false;

                try {
                    Ignite ignite = startGrid(idx.getAndIncrement());

                    assertTrue(ignite.configuration().isClientMode());
                    assertTrue(ignite.cluster().localNode().isClient());

                    latch.countDown();

                    cnt = true;

                    log.info("Started [node=" + ignite.name() + ", left=" + latch.getCount() + ']');

                    ThreadLocalRandom8 rnd = ThreadLocalRandom8.current();

                    while (latch.getCount() > 0) {
                        Thread.sleep(1000);

                        IgniteCache<Object, Object> cache = ignite.cache(cacheNames.get(rnd.nextInt(0, CACHES)));

                        Integer key = rnd.nextInt(0, 100_000);

                        cache.put(key, 0);

                        assertNotNull(cache.get(key));
                    }

                    return null;
                }
                finally {
                    if (!cnt)
                        latch.countDown();
                }
            }
        }, CLIENTS, "start-client");

        fut.get();

        log.info("Started all clients.");

        checkNodes(CLIENTS + 1);
    }

    /**
     * @param expCnt Expected number of nodes.
     */
    private void checkNodes(int expCnt) {
        assertEquals(expCnt, G.allGrids().size());

        long topVer = -1L;

        for (Ignite ignite : G.allGrids()) {
            log.info("Check node: " + ignite.name());

            if (topVer == -1L)
                topVer = ignite.cluster().topologyVersion();
            else
                assertEquals("Unexpected topology version for node: " + ignite.name(),
                    topVer,
                    ignite.cluster().topologyVersion());

            assertEquals("Unexpected number of nodes for node: " + ignite.name(),
                expCnt,
                ignite.cluster().nodes().size());
        }
    }
}