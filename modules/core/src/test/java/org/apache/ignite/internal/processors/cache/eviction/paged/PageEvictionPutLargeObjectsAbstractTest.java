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
package org.apache.ignite.internal.processors.cache.eviction.paged;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;

/** */
public abstract class PageEvictionPutLargeObjectsAbstractTest extends GridCommonAbstractTest {
    /** Offheap size for memory policy. */
    private static final int SIZE = 128 * 1024 * 1024;

    /** Offheap size for memory policy. */
    private static final long MAX_SIZE = 256 * 1024 * 1024;

    /** Record size. */
    private static final int RECORD_SIZE = 50 * 4096;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setInitialSize(SIZE)
                    .setMaxSize(MAX_SIZE)
                    .setEmptyPagesPoolSize(1000)
                    .setPersistenceEnabled(false)
                    .setMetricsEnabled(true)
                    .setEvictionThreshold(0.8)
                )
            );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = "IGNITE_DUMP_THREADS_ON_FAILURE", value = "false")
    public void testPutLargeObjects() throws Exception {
        IgniteEx ignite = startGrids(1);

        IgniteCache<Long, TestObject> cache = ignite.createCache(DEFAULT_CACHE_NAME);
        AtomicLong idx = new AtomicLong();
        AtomicBoolean run = new AtomicBoolean(true);

        IgniteInternalFuture<?> getFut = runMultiThreadedAsync(() -> {
            Random rnd = new Random();

            while (run.get()) {
                if (idx.get() > 0)
                    cache.get(rnd.nextLong(idx.get()));
            }
        }, 4, "get");

        TestObject loadObj = new TestObject(RECORD_SIZE);
//        Set<Long> loadedKeys = ConcurrentHashMap.newKeySet();

        IgniteInternalFuture<?> ldrFut = runMultiThreadedAsync(() -> {
            while (run.get()) {
                Long k = idx.incrementAndGet();

                cache.put(k, loadObj);

                //loadedKeys.add(k);
            }

        }, 2, "ldr");

//        IgniteInternalFuture<?> evicFut = runAsync(() -> {
//            CacheEvictionManager evictMgr = grid(0).cachex(DEFAULT_CACHE_NAME).context().evicts();
//
//            while (run.get()) {
//                if (loadedKeys.isEmpty())
//                    continue;
//
//                evictMgr.batchEvict(loadedKeys, null);
//
//                loadedKeys.clear();
//            }
//        });

        runAsync(() -> {
            Thread.sleep(getTestTimeout() / 2);

            run.set(false);
        });

        ldrFut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
        getFut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
//        evicFut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 45L * 1000;
    }
}
