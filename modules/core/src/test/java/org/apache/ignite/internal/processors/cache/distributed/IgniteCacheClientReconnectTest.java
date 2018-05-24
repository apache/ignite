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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
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
    private static final int CACHES = 10;

    /** */
    private static final long TEST_TIME = 60_000;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        if (!client) {
            CacheConfiguration[] ccfgs = new CacheConfiguration[CACHES];

            for (int i = 0; i < CACHES; i++) {
                CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

                ccfg.setCacheMode(PARTITIONED);
                ccfg.setAtomicityMode(TRANSACTIONAL);
                ccfg.setBackups(1);
                ccfg.setName("cache-" + i);
                ccfg.setWriteSynchronizationMode(FULL_SYNC);

                ccfgs[i] = ccfg;
            }

            cfg.setCacheConfiguration(ccfgs);
        }

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(SRV_CNT);
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIME + 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientReconnect() throws Exception {
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