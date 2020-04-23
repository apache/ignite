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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class NearCacheMultithreadedUpdateTest extends GridCommonAbstractTest {
    /** */
    private static final int SRV_CNT = 3;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(SRV_CNT);

        startClientGrid(SRV_CNT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateMultithreadedTx() throws Exception {
        updateMultithreaded(TRANSACTIONAL, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateMultithreadedTxRestart() throws Exception {
        updateMultithreaded(TRANSACTIONAL, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateMultithreadedAtomic() throws Exception {
        updateMultithreaded(ATOMIC, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateMultithreadedAtomicRestart() throws Exception {
        updateMultithreaded(ATOMIC, true);
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param restart If {@code true} restarts one node.
     * @throws Exception If failed.
     */
    private void updateMultithreaded(CacheAtomicityMode atomicityMode, boolean restart) throws Exception {
        Ignite srv = ignite(0);

        srv.destroyCache(DEFAULT_CACHE_NAME);

        IgniteCache<Integer, Integer> srvCache = srv.createCache(cacheConfiguration(atomicityMode));

        Ignite client = ignite(SRV_CNT);

        assertTrue(client.configuration().isClientMode());

        final IgniteCache<Integer, Integer> clientCache =
            client.createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration<Integer, Integer>());

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> restartFut = null;

        // Primary key for restarted node.
        final Integer key0 = primaryKey(ignite(SRV_CNT - 1).cache(DEFAULT_CACHE_NAME));

        if (restart) {
            restartFut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    while (!stop.get()) {
                        Thread.sleep(300);

                        log.info("Stop node.");

                        stopGrid(SRV_CNT - 1);

                        Thread.sleep(300);

                        log.info("Start node.");

                        startGrid(SRV_CNT - 1);
                    }

                    return null;
                }
            }, "restart-thread");
        }

        try {
            long stopTime = System.currentTimeMillis() + 10_000;

            int iter = 0;

            while (System.currentTimeMillis() < stopTime) {
                if (iter % 100 == 0)
                    log.info("Iteration: " + iter);

                final Integer key = iter++;

                final AtomicInteger val = new AtomicInteger();

                GridTestUtils.runMultiThreaded(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        clientCache.put(key0, val.incrementAndGet());

                        for (int i = 0; i < 10; i++)
                            clientCache.put(key, val.incrementAndGet());

                        return null;
                    }
                }, 20, "update-thread");

                if (restart) {
                    assertEquals(srvCache.get(key), clientCache.get(key));
                    assertEquals(srvCache.get(key0), clientCache.get(key0));
                }
                else {
                    assertEquals(srvCache.get(key), clientCache.localPeek(key));
                    assertEquals(srvCache.get(key0), clientCache.localPeek(key0));
                }
            }

            stop.set(true);

            if (restartFut != null)
                restartFut.get();
        }
        finally {
            stop.set(true);
        }
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(CacheAtomicityMode atomicityMode) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(1);

        return ccfg;
    }
}
