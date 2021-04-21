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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.MvccFeatureChecker.assertMvccWriteConflict;

/**
 *
 */
public class IgniteCacheCreatePutTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setPeerClassLoadingEnabled(false);

        cfg.setMarshaller(new BinaryMarshaller());

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setName("cache*");
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60 * 1000L;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartNodes() throws Exception {
        long stopTime = System.currentTimeMillis() + 2 * 60_000;

        try {
            int iter = 0;

            while (System.currentTimeMillis() < stopTime && iter < 5) {
                log.info("Iteration: " + iter++);

                try {
                    final AtomicInteger idx = new AtomicInteger();

                    GridTestUtils.runMultiThreaded(new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            int node = idx.getAndIncrement();

                            Ignite ignite = startGrid(node);

                            IgniteCache<Object, Object> cache = ignite.getOrCreateCache("cache1");

                            assertNotNull(cache);

                            for (int i = 0; i < 100; i++)
                                cache.put(i, i);

                            return null;
                        }
                    }, GRID_CNT, "start");
                }
                finally {
                    stopAllGrids();
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdatesAndCacheStart() throws Exception {
        final int NODES = 4;

        startGridsMultiThreaded(NODES);

        Ignite ignite0 = ignite(0);

        ignite0.createCache(cacheConfiguration("atomic-cache", ATOMIC));
        ignite0.createCache(cacheConfiguration("tx-cache", TRANSACTIONAL));
        ignite0.createCache(cacheConfiguration("mvcc-tx-cache", TRANSACTIONAL_SNAPSHOT));

        final long stopTime = System.currentTimeMillis() + 60_000;

        final AtomicInteger updateThreadIdx = new AtomicInteger();

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                int nodeIdx = updateThreadIdx.getAndIncrement() % NODES;

                Ignite node = ignite(nodeIdx);

                IgniteCache cache1 = node.cache("atomic-cache");
                IgniteCache cache2 = node.cache("tx-cache");
                IgniteCache cache3 = node.cache("mvcc-tx-cache");

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                int iter = 0;

                while (System.currentTimeMillis() < stopTime) {
                    Integer key = rnd.nextInt(10_000);

                    cache1.put(key, key);

                    cache2.put(key, key);

                    try {
                        cache3.put(key, key);
                    }
                    catch (Exception e) {
                        assertMvccWriteConflict(e); // Do not retry.
                    }

                    if (iter++ % 1000 == 0)
                        log.info("Update iteration: " + iter);
                }

                return null;
            }
        }, NODES * 2, "update-thread");

        final AtomicInteger cacheThreadIdx = new AtomicInteger();

        IgniteInternalFuture<?> cacheFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                int nodeIdx = cacheThreadIdx.getAndIncrement() % NODES;

                Ignite node = ignite(nodeIdx);

                int iter = 0;

                while (System.currentTimeMillis() < stopTime) {
                    String cacheName = "dynamic-cache-" + nodeIdx;

                    CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

                    ccfg.setName(cacheName);

                    node.createCache(ccfg);

                    node.destroyCache(cacheName);

                    U.sleep(500);

                    if (iter++ % 1000 == 0)
                        log.info("Cache create iteration: " + iter);
                }

                return null;
            }
        }, NODES, "cache-thread");

        while (!fut.isDone()) {
            startClientGrid(NODES);

            stopGrid(NODES);

            startGrid(NODES);

            stopGrid(NODES);
        }

        fut.get();
        cacheFut.get();
    }

    /**
     * @param name Cache name.
     * @param atomicityMode Cache atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String name, CacheAtomicityMode atomicityMode) {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setName(name);
        ccfg.setCacheMode(REPLICATED);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        return ccfg;
    }
}
