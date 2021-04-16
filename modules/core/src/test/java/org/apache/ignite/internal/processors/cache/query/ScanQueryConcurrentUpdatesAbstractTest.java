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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * A base for tests that check the behaviour of scan queries run on a data set that is modified concurrently.
 * Actual tests should implement a way of cache creation, modification and destruction.
 */
public abstract class ScanQueryConcurrentUpdatesAbstractTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(4);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Creates a cache with given parameters.
     *
     * @param cacheName Name of the cache.
     * @param cacheMode Cache mode.
     * @param expiration {@link Duration} for {@link javax.cache.expiry.ExpiryPolicy}. If {@code null}, then
     * {@link javax.cache.expiry.ExpiryPolicy} won't be configured.
     *
     * @return Instance of the created cache.
     */
    protected abstract IgniteCache<Integer, Integer> createCache(String cacheName, CacheMode cacheMode,
        Duration expiration);

    /**
     * Performs modification of a provided cache. Records with keys in range {@code 0..(recordsNum - 1)} are updated.
     *
     * @param cache Cache to update.
     * @param recordsNum Number of records to update.
     */
    protected abstract void updateCache(IgniteCache<Integer, Integer> cache, int recordsNum);

    /**
     * Destroys the provided cache.
     *
     * @param cache Cache to destroy.
     */
    protected abstract void destroyCache(IgniteCache<Integer, Integer> cache);

    /**
     * Tests behaviour of scan queries with concurrent modification.
     *
     * @param cache Cache to test.
     * @param recordsNum Number of records to load to the cache.
     */
    private void testStableDataset(IgniteCache<Integer, Integer> cache, int recordsNum) {
        int iterations = 1000;

        AtomicBoolean finished = new AtomicBoolean();

        try {
            updateCache(cache, recordsNum);
            GridTestUtils.runAsync(() -> {
                while (!finished.get())
                    updateCache(cache, recordsNum);
            });

            for (int i = 0; i < iterations; i++) {
                List<Cache.Entry<Integer, Integer>> res = cache.query(new ScanQuery<Integer, Integer>()).getAll();

                assertEquals("Unexpected query result size.", recordsNum, res.size());

                for (Cache.Entry<Integer, Integer> e : res)
                    assertEquals(e.getKey(), e.getValue());
            }
        }
        finally {
            finished.set(true);
            destroyCache(cache);
        }
    }

    /**
     * Tests behaviour of scan queries with entries expired and modified concurrently.
     *
     * @param cache Cache to test.
     */
    private void testExpiringDataset(IgniteCache<Integer, Integer> cache) {
        int iterations = 100;
        int recordsNum = 100;

        try {
            for (int i = 0; i < iterations; i++) {
                updateCache(cache, recordsNum);

                long updateTime = U.currentTimeMillis();

                List<Cache.Entry<Integer, Integer>> res = cache.query(new ScanQuery<Integer, Integer>()).getAll();

                assertTrue("Query result set is too big: " + res.size(), res.size() <= recordsNum);

                for (Cache.Entry<Integer, Integer> e : res)
                    assertEquals(e.getKey(), e.getValue());

                while (U.currentTimeMillis() == updateTime)
                    doSleep(10L);
            }
        }
        finally {
            destroyCache(cache);
        }
    }

    /** */
    @Test
    public void testReplicatedOneRecordLongExpiry() {
        testStableDataset(createCache("replicated_long_expiry",
            CacheMode.REPLICATED, Duration.ONE_HOUR), 1);
    }

    /** */
    @Test
    public void testReplicatedManyRecordsLongExpiry() {
        testStableDataset(createCache("replicated_long_expiry",
            CacheMode.REPLICATED, Duration.ONE_HOUR), 1000);
    }

    /** */
    @Test
    public void testReplicatedOneRecordNoExpiry() {
        testStableDataset(createCache("replicated_no_expiry",
            CacheMode.REPLICATED, null), 1);
    }

    /** */
    @Test
    public void testReplicatedManyRecordsNoExpiry() {
        testStableDataset(createCache("replicated_no_expiry",
            CacheMode.REPLICATED, null), 1000);
    }

    /** */
    @Test
    public void testPartitionedOneRecordLongExpiry() {
        testStableDataset(createCache("partitioned_long_expiry",
            CacheMode.PARTITIONED, Duration.ONE_HOUR), 1);
    }

    /** */
    @Test
    public void testPartitionedManyRecordsLongExpiry() {
        testStableDataset(createCache("partitioned_long_expiry",
            CacheMode.PARTITIONED, Duration.ONE_HOUR), 1000);
    }

    /** */
    @Test
    public void testPartitionedOneRecordNoExpiry() {
        testStableDataset(createCache("partitioned_no_expiry",
            CacheMode.PARTITIONED, null), 1);
    }

    /** */
    @Test
    public void testPartitionedManyRecordsNoExpiry() {
        testStableDataset(createCache("partitioned_no_expiry",
            CacheMode.PARTITIONED, null), 1000);
    }

    /** */
    @Test
    public void testPartitionedShortExpiry() {
        testExpiringDataset(createCache("partitioned_short_expiry",
            CacheMode.PARTITIONED, new Duration(TimeUnit.MILLISECONDS, 1)));
    }

    /** */
    @Test
    public void testReplicatedShortExpiry() {
        testExpiringDataset(createCache("partitioned_short_expiry",
            CacheMode.REPLICATED, new Duration(TimeUnit.MILLISECONDS, 1)));
    }
}
