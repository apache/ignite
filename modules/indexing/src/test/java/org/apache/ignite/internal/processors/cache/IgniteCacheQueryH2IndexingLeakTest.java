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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import javax.cache.Cache;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_INDEXING_CACHE_CLEANUP_PERIOD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_INDEXING_CACHE_THREAD_USAGE_TIMEOUT;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests leaks at the IgniteH2Indexing
 */
@SuppressWarnings("StatementWithEmptyBody")
public class IgniteCacheQueryH2IndexingLeakTest extends GridCommonAbstractTest {
    /** Number of test grids (nodes) */
    private static final int GRID_CNT = 1;

    /** */
    private static final long TEST_TIMEOUT = 2 * 60 * 1000;

    /** Threads to parallel execute queries */
    private static final int THREAD_COUNT = 10;

    /** Timeout */
    private static final long STMT_CACHE_CLEANUP_TIMEOUT = 1000;

    /** Orig cleanup period. */
    private static String origCacheCleanupPeriod;

    /** Orig usage timeout. */
    private static String origCacheThreadUsageTimeout;

    /** */
    private static final int ITERATIONS = 5;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheConfiguration());

        GridQueryProcessor.idxCls = FakeIndexing.class;

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration<?,?> cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);

        cacheCfg.setIndexedTypes(
            Integer.class, Integer.class
        );

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT + 60_000;
    }

    /**
     *
     */
    private static class FakeIndexing extends IgniteH2Indexing {
        /** Instance. */
        private static FakeIndexing instance;

        /**
         * Default constructor.
         */
        public FakeIndexing() {
            instance = this;
        }

        /**
         * Get size of stmtCache
         */
        static int getCachesSize() {
            ConcurrentMap stmtCache = GridTestUtils.getFieldValue(instance, IgniteH2Indexing.class, "stmtCache");
            return stmtCache.size();
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        // Clean up all caches.
        for (int i = 0; i < GRID_CNT; i++) {
            IgniteCache<Object, Object> c = grid(i).cache(null);

            assertEquals(0, c.size());
        }

        final int keyCnt = 10;

        // Put test values into cache.
        final IgniteCache<Integer, Integer> c = grid(0).cache(null);
        for (int i = 0; i < keyCnt; i++)
            c.put(i, i);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        origCacheCleanupPeriod = System.getProperty(IGNITE_H2_INDEXING_CACHE_CLEANUP_PERIOD);
        origCacheThreadUsageTimeout = System.getProperty(IGNITE_H2_INDEXING_CACHE_THREAD_USAGE_TIMEOUT);
        System.setProperty(IGNITE_H2_INDEXING_CACHE_CLEANUP_PERIOD, Long.toString(STMT_CACHE_CLEANUP_TIMEOUT));
        System.setProperty(IGNITE_H2_INDEXING_CACHE_THREAD_USAGE_TIMEOUT, Long.toString(STMT_CACHE_CLEANUP_TIMEOUT));

        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
        System.setProperty(IGNITE_H2_INDEXING_CACHE_CLEANUP_PERIOD,
            origCacheCleanupPeriod !=null ? origCacheCleanupPeriod : "");
        System.setProperty(IGNITE_H2_INDEXING_CACHE_THREAD_USAGE_TIMEOUT,
            origCacheThreadUsageTimeout !=null ? origCacheThreadUsageTimeout : "");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        // Clean up all caches.
        for (int i = 0; i < GRID_CNT; i++) {
            IgniteCache<Object, Object> c = grid(i).cache(null);

            c.removeAll();
        }
    }

    /**
     * @param entries Entries.
     * @param g Grid.
     * @return Affinity nodes.
     */
    private Set<UUID> affinityNodes(Iterable<Cache.Entry<Integer, Integer>> entries, Ignite g) {
        Set<UUID> nodes = new HashSet<>();

        for (Cache.Entry<Integer, Integer> entry : entries)
            nodes.add(g.affinity(null).mapKeyToPrimaryAndBackups(entry.getKey()).iterator().next().id());

        return nodes;
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testLeaksInIgniteH2IndexingOnTerminatedThread() throws Exception {

        final IgniteCache<Integer, Integer> c = grid(0).cache(null);

        for(int i = 0; i < ITERATIONS; ++i) {
            info("Iteration #" + i);

            // Open iterator on the created cursor: add entries to the cache
            IgniteInternalFuture<?> fut = multithreadedAsync(
                new CAX() {
                    @Override public void applyx() throws IgniteCheckedException {
                        c.query(new SqlQuery(Integer.class, "_val >= 0")).iterator();

                        c.query(new SqlQuery(Integer.class, "_val >= 1")).iterator();
                    }
                }, THREAD_COUNT);
            fut.get();

            // Wait for stmt cache entry is created for each thread.
            assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return FakeIndexing.getCachesSize() == THREAD_COUNT;
                }
            }, STMT_CACHE_CLEANUP_TIMEOUT));

            // Wait for stmtCache is cleaned up because all user threads are terminated.
            assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return FakeIndexing.getCachesSize() == 0;
                }
            }, STMT_CACHE_CLEANUP_TIMEOUT * 2));
        }
    }


    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testLeaksInIgniteH2IndexingOnUnusedThread() throws Exception {

        final IgniteCache<Integer, Integer> c = grid(0).cache(null);
        final Object endOfTestMonitor = new Object();

        for(int i = 0; i < ITERATIONS; ++i) {
            info("Iteration #" + i);

            // Open iterator on the created cursor: add entries to the cache
            IgniteInternalFuture<?> fut = multithreadedAsync(
                new CAX() {
                    @Override public void applyx() throws IgniteCheckedException {
                        c.query(new SqlQuery(Integer.class, "_val >= 0")).iterator();

                        while(true) {
                            try {
                                synchronized (endOfTestMonitor) {
                                    endOfTestMonitor.wait();
                                    return;
                                }
                            }
                            catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                    }
                }, THREAD_COUNT);

            // Wait for stmt cache entry is created for each thread.
            assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return FakeIndexing.getCachesSize() == THREAD_COUNT;
                }
            }, STMT_CACHE_CLEANUP_TIMEOUT));

            Thread.sleep(STMT_CACHE_CLEANUP_TIMEOUT);

            // Wait for stmtCache is cleaned up because all user threads don't perform queries a lot of time.
            assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return FakeIndexing.getCachesSize() == 0;
                }
            }, STMT_CACHE_CLEANUP_TIMEOUT * 2));

            synchronized (endOfTestMonitor) {
                endOfTestMonitor.notifyAll();
            }
            fut.get();
        }
    }

    /**
     * Test value.
     */
    private static class TestValue implements Serializable {
        /** Value. */
        @QuerySqlField(index = true)
        private int val;

        /**
         * @param val Value.
         */
        private TestValue(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public int value() {
            return val;
        }
    }
}