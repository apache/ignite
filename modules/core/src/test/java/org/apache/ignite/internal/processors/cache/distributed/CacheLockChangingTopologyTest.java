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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class CacheLockChangingTopologyTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.ENTRY_LOCK);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockOnChangingTopology_Partitioned() throws Exception {
        lockOnChangingTopology(CacheMode.PARTITIONED, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockOnChangingTopology_PartitionedNearEnabled() throws Exception {
        lockOnChangingTopology(CacheMode.PARTITIONED, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockOnChangingTopology_Replicated() throws Exception {
        lockOnChangingTopology(CacheMode.REPLICATED, false);
    }

    /**
     * @throws Exception If failed.
     */
    private void lockOnChangingTopology(CacheMode cacheMode, boolean nearEnabled) throws Exception {
        Ignite ignite = startGrid(0);

        CacheConfiguration<Long, Long> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setCacheMode(cacheMode);

        if (nearEnabled)
            ccfg.setNearConfiguration(new NearCacheConfiguration<>());

        IgniteCache<Long, Long> cache = ignite.createCache(ccfg);

        IgniteInternalFuture<?> nodeStart = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                for (int i = 0; i < 3; i++) {
                    Thread.sleep(ThreadLocalRandom.current().nextLong(500) + 1000);

                    startGrid(1);

                    awaitPartitionMapExchange();
                }

                return null;
            }
        });

        long stopTime = System.currentTimeMillis() + 60_000;

        long cnt = 0;

        final AtomicReference<Throwable> err = new AtomicReference<>();

        while (!nodeStart.isDone() && System.currentTimeMillis() < stopTime) {
            final long key = cnt++ % 100;

            info("Iteration: " + cnt);

            final Lock lock = cache.lock(key);

            boolean unlocked = false;

            lock.lock();

            try {
                Thread t = new Thread(new Runnable() {
                    @Override public void run() {
                        try {
                            lock.lock();
                            lock.unlock();
                        }
                        catch (Throwable e) {
                            err.set(e);
                        }
                    }
                });

                t.setName("lock-thread");
                t.start();

                Thread.sleep(ThreadLocalRandom.current().nextLong(100) + 50);

                // Check lock was not acquired while it is still locked.
                assertTrue(t.isAlive());

                lock.unlock();

                unlocked = true;

                t.join();
            } finally {
                if (!unlocked)
                    lock.unlock();
            }

            if (err.get() != null)
                fail("Unexpected error: " + err);
        }

        assertTrue("Failed to wait for node start", nodeStart.isDone());
    }
}
