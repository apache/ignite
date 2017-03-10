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

package org.apache.ignite.internal.processors.cache.query;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * The test for the destruction of the cache during the execution of the query
 */
public class IgniteCacheQueryCacheDestroySelfTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    public static final int GRID_CNT = 3;

    /**
     * The main test code.
     */
    public void testQueue() throws Throwable {
        startGridsMultiThreaded(GRID_CNT);

        Ignite ig = ignite(0);

        ig.getOrCreateCache(cacheConfiguration());

        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicReference<Exception> npe = new AtomicReference<>();

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try {
                    while (!stop.get())
                        runQuery();
                }
                catch (Exception e) {
                    NullPointerException npe0 = X.cause(e, NullPointerException.class);

                    if (npe0 != null)
                        npe.compareAndSet(null, npe0);
                    else
                        info("Expected exception: " + e);
                }

                return null;
            }
        }, 6, "query-runner");

        U.sleep(500);

        ig.destroyCache(CACHE_NAME);

        stop.set(true);

        fut.get();

        assertNull(npe.get());
    }

    /**
     * @throws Exception If failed.
     */
    private void runQuery() throws Exception {
        ScanQuery<String, String> scanQuery = new ScanQuery<String, String>()
            .setLocal(true)
            .setFilter(new IgniteBiPredicate<String, String>() {
                @Override public boolean apply(String key, String p) {
                    return key != null && key.isEmpty();
                }
            });

        Ignite ignite = ignite(ThreadLocalRandom.current().nextInt(GRID_CNT));

        IgniteCache<String, String> example = ignite.cache(CACHE_NAME);

        for (int partition : ignite.affinity(CACHE_NAME).primaryPartitions(ignite.cluster().localNode())) {
            scanQuery.setPartition(partition);

            try (QueryCursor cursor = example.query(scanQuery)) {
                for (Object p : cursor) {
                    String value = (String) ((Cache.Entry)p).getValue();

                    assertNotNull(value);
                }
            }
        }
    }

    /**
     * @return Cache configuration for this test.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration cfg = new CacheConfiguration(CACHE_NAME);

        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED)
            .setRebalanceMode(CacheRebalanceMode.SYNC)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setRebalanceThrottle(100)
            .setRebalanceBatchSize(2 * 1024 * 1024)
            .setBackups(1)
            .setEagerTtl(false);

        return cfg;
    }
}
