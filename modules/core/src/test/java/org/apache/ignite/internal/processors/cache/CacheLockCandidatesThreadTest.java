/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;

/**
 * Tests locking of thread of candidates (see GG-17364)
 */
public class CacheLockCandidatesThreadTest extends GridCommonAbstractTest {

    /** */
    private static final String DEFAULT_CACHE_NAME = "default";

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testLockCandidatesThreadForLocalMode() throws Exception {
        lockThreadOfCandidates(CacheMode.LOCAL);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testLockCandidatesThreadForReplicatedMode() throws Exception {
        lockThreadOfCandidates(CacheMode.REPLICATED);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testLockCandidatesThreadForPartitionedMode() throws Exception {
        lockThreadOfCandidates(CacheMode.PARTITIONED);
    }

    /**
     * @param mode Mode.
     */
    private void lockThreadOfCandidates(CacheMode mode) throws Exception {
        startGridsMultiThreaded(1);

        grid(0).createCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(mode));

        try {
            final CountDownLatch unlock = new CountDownLatch(1);
            final CountDownLatch locked = new CountDownLatch(1);

            final IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

            final String triggerKey = "" + ThreadLocalRandom.current().nextInt();

            System.out.println("Trigger: " + triggerKey);

            cache.put(triggerKey, "val");

            IgniteInternalFuture<Object> future = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Lock lock = cache.lock(triggerKey);
                    try {
                        lock.lock();

                        System.out.println("Trigger is locked");

                        locked.countDown();

                        unlock.await();
                    } finally {
                        lock.unlock();

                        System.out.println("Trigger is unlocked");
                    }

                    return null;
                }
            });

            locked.await();

            Map<String, String> map = new TreeMap<>();

            map.put(triggerKey, "trigger-new-val");

            for (int i = 0; i < 4_000; i++)
                map.put("key-" + i, "value");

            IgniteFuture<Void> f = grid(0).cache(DEFAULT_CACHE_NAME).putAllAsync(map);

            Thread.sleep(200);

            unlock.countDown();

            future.get();
            f.get();
        }
        finally {
            stopAllGrids();
        }
    }
}
