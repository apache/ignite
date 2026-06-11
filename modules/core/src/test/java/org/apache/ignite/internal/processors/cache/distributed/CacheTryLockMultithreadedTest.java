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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
@RunWith(Parameterized.class)
public class CacheTryLockMultithreadedTest extends GridCommonAbstractTest {
    /** */
    private static final int SRVS = 3;

    /** */
    @Parameterized.Parameter
    public CacheMode cacheMode;

    /** */
    @Parameterized.Parameter(1)
    public int backups;

    /** */
    @Parameterized.Parameters(name = "cacheMode={0}, backups={1}")
    public static List<Object[]> parameters() {
        return F.asList(
            new Object[] {REPLICATED, 0},
            new Object[] {PARTITIONED, 0},
            new Object[] {PARTITIONED, 1}
        );
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setCacheMode(cacheMode);
        ccfg.setBackups(backups);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGridsMultiThreaded(SRVS);

        startClientGrid(SRVS);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTryLock() throws Exception {
        Ignite client = grid(SRVS);

        final Integer key = 1;

        final IgniteCache<Integer, Integer> cache = client.cache(DEFAULT_CACHE_NAME);

        final long stopTime = System.currentTimeMillis() + 15_000;

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Lock lock = cache.lock(key);

                while (System.currentTimeMillis() < stopTime) {
                    for (int i = 0; i < 1000; i++) {
                        boolean locked = lock.tryLock(100, MILLISECONDS);

                        if (locked)
                            lock.unlock();
                    }
                }

                return null;
            }
        }, 20, "lock-thread");
    }

    /** */
    @Test
    public void testCancelRequestOnTimeout() throws Exception {
        IgniteEx node = grid(1);

        IgniteCache<Integer, Integer> cache = node.cache(DEFAULT_CACHE_NAME);

        List<Integer> keys = primaryKeys(cache, 100);

        for (Integer key : keys)
            cache.put(key, key);

        long stopTime = U.currentTimeMillis() + 10_000;

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            while (U.currentTimeMillis() < stopTime) {
                Integer key = keys.get(ThreadLocalRandom.current().nextInt(keys.size()));

                Lock lock = cache.lock(key);

                boolean locked = false;

                try {
                    locked = lock.tryLock(10, TimeUnit.MILLISECONDS);

                    if (locked)
                        doSleep(20);
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }
                finally {
                    if (locked)
                        lock.unlock();
                }
            }
        }, 16, "lock-thread");

        fut.get(getTestTimeout());
    }
}
