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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for ??
 */
public class CacheBulkUnlockTest extends GridCommonAbstractTest {

    /** */
    private static final String DEFAULT_CACHE_NAME = "default";

    /**
     * @throws Exception if failed.
     */
    public void testBatchUnlockForLocalMode() throws Exception {
        doBulkUnlock(CacheMode.LOCAL);
    }

    /**
     * @throws Exception if failed.
     */
    public void testBatchUnlockForPartitionedMode() throws Exception {
        doBulkUnlock(CacheMode.PARTITIONED);
    }

    private void doBulkUnlock(CacheMode mode) throws Exception {
        startGrid(0);
        grid(0).createCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(mode));

        try {
            final CountDownLatch releaseLatch = new CountDownLatch(1);

            final IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);
            IgniteInternalFuture<Object> future = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Lock lock = cache.lock("trigger");
                    try {
                        lock.lock();
                        releaseLatch.await();
                    } finally {
                        lock.unlock();
                    }

                    return null;
                }
            });

            Map<String, String> putMap = new LinkedHashMap<>();
            putMap.put("trigger", "trigger-new-val");
            for (int i = 0; i < 5_000; i++)
                putMap.put("key-" + i, "value");

            IgniteCache<Object, Object> asyncCache = grid(0).cache(DEFAULT_CACHE_NAME).withAsync();
            asyncCache.putAll(putMap);
            IgniteFuture<Object> resFut = asyncCache.future();

            Thread.sleep(500);
            releaseLatch.countDown();

            future.get();
            resFut.get();
        }
        finally {
            stopAllGrids();
        }
    }
}
