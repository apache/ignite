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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteNearClientCacheCloseTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNearCacheCloseAtomic1() throws Exception {
        nearCacheClose(1, false, ATOMIC);

        nearCacheClose(1, true, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNearCacheCloseAtomic2() throws Exception {
        nearCacheClose(4, false, ATOMIC);

        nearCacheClose(4, true, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNearCacheCloseTx1() throws Exception {
        nearCacheClose(1, false, TRANSACTIONAL);

        nearCacheClose(1, true, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNearCacheCloseTx2() throws Exception {
        nearCacheClose(4, false, TRANSACTIONAL);

        nearCacheClose(4, true, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNearCacheCloseMvccTx1() throws Exception {
        nearCacheClose(1, false, TRANSACTIONAL_SNAPSHOT);

        if (MvccFeatureChecker.isSupported(MvccFeatureChecker.Feature.NEAR_CACHE))
            nearCacheClose(1, true, TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNearCacheCloseMvccTx2() throws Exception {
        nearCacheClose(4, false, TRANSACTIONAL_SNAPSHOT);

        if (MvccFeatureChecker.isSupported(MvccFeatureChecker.Feature.NEAR_CACHE))
            nearCacheClose(4, true, TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @param srvs Number of server nodes.
     * @param srvNearCache {@code True} to enable near cache on server nodes.
     * @param atomicityMode Cache atomicity mode.
     * @throws Exception If failed.
     */
    private void nearCacheClose(int srvs, boolean srvNearCache, CacheAtomicityMode atomicityMode) throws Exception {
        Ignite srv;

        if (Ignition.allGrids().isEmpty()) {
            srv = startGrids(srvs);

            startClientGrid(srvs);
        }
        else
            srv = grid(0);

        IgniteCache<Object, Object> srvCache = srv.createCache(cacheConfiguration(atomicityMode, srvNearCache));

        List<Integer> keys = new ArrayList<>();

        keys.add(primaryKey(srvCache));

        if (srvs > 1) {
            keys.add(backupKey(srvCache));
            keys.add(nearKey(srvCache));
        }

        awaitPartitionMapExchange();

        for (Integer key : keys) {
            IgniteCache<Object, Object> clientCache =
                ignite(srvs).createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration<>());

            clientCache.put(key, 1);

            clientCache.close();

            srvCache.put(key, 2);

            assertEquals(2, srvCache.get(key));

            srvCache.put(key, 3);

            assertEquals(3, srvCache.get(key));
        }

        srvCache.destroy();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentUpdateAndNearCacheClose() throws Exception {
        final int SRVS = 4;

        startGrids(SRVS);

        startClientGrid(SRVS);
        startClientGrid(SRVS + 1);

        concurrentUpdateAndNearCacheClose(ATOMIC, SRVS + 1);

        concurrentUpdateAndNearCacheClose(TRANSACTIONAL, SRVS + 1);
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param nearClient Index of client node with near cache.
     * @throws Exception If failed.
     */
    private void concurrentUpdateAndNearCacheClose(CacheAtomicityMode atomicityMode,
        final int nearClient)
        throws Exception
    {
        final String cacheName = ignite(0).createCache(cacheConfiguration(atomicityMode, false)).getName();

        for (int iter = 0; iter < 5; iter++) {
            log.info("Iteration: " + iter);

            IgniteCache<Object, Object> nearCache = ignite(nearClient).createNearCache(cacheName,
                new NearCacheConfiguration<>());

            final int KEYS = 1000;

            for (int i = 0; i < KEYS; i++)
                nearCache.put(i, i);

            final AtomicBoolean stop = new AtomicBoolean();

            IgniteInternalFuture<?> updateFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (!stop.get()) {
                        int node = rnd.nextInt(nearClient);

                        IgniteCache<Object, Object> cache = ignite(node).cache(cacheName);

                        if (rnd.nextBoolean()) {
                            Map<Integer, Integer> map = new TreeMap<>();

                            for (int i = 0; i < 10; i++)
                                map.put(rnd.nextInt(KEYS), i);

                            cache.putAll(map);
                        }
                        else
                            cache.put(rnd.nextInt(KEYS), node);
                    }

                    return null;
                }
            }, 10, "update");

            try {
                U.sleep(3000);

                nearCache.close();

                stop.set(true);

                updateFut.get();
            }
            finally {
                stop.set(true);
            }
        }

        ignite(0).destroyCache(cacheName);
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param nearCache {@code True} to enable near cache.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(CacheAtomicityMode atomicityMode, boolean nearCache) {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        if (nearCache)
            ccfg.setNearConfiguration(new NearCacheConfiguration());

        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(1);

        return ccfg;
    }
}
