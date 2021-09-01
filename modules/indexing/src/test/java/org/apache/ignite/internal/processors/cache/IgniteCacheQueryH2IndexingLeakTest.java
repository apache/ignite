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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.h2.ConcurrentStripedPool;
import org.apache.ignite.internal.processors.query.h2.H2Connection;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_INDEXING_CACHE_CLEANUP_PERIOD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_INDEXING_CACHE_THREAD_USAGE_TIMEOUT;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.IgniteCacheQueryH2IndexingLeakTest.STMT_CACHE_TTL;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assert.assertThat;

/**
 * Tests leaks at the IgniteH2Indexing
 */
@WithSystemProperty(key = IGNITE_H2_INDEXING_CACHE_CLEANUP_PERIOD, value = "200")
@WithSystemProperty(key = IGNITE_H2_INDEXING_CACHE_THREAD_USAGE_TIMEOUT, value = STMT_CACHE_TTL + "")
public class IgniteCacheQueryH2IndexingLeakTest extends GridCommonAbstractTest {
    /** */
    private static final int THREAD_COUNT = 10;

    /** Inactivity time after which statement cache will be cleared. */
    public static final long STMT_CACHE_TTL = 1000;

    /** */
    private static final int ITERATIONS = 5;

    /**
     * We use barrier to get sure that all queries are executed simultaneously.
     * Otherwise arbitrary connection could be reused during iteration,
     * and check will fail.
     */
    static final CyclicBarrier BARRIER = new CyclicBarrier(THREAD_COUNT);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(cacheConfiguration());
    }

    /**
     * @return Cache configuration.
     */
    @SuppressWarnings("unchecked")
    protected CacheConfiguration<?, ?> cacheConfiguration() {
        return defaultCacheConfiguration()
            .setCacheMode(PARTITIONED)
            .setAtomicityMode(TRANSACTIONAL)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setSqlFunctionClasses(IgniteCacheQueryH2IndexingLeakTest.class)
            .setIndexedTypes(
                Integer.class, Integer.class
            );
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 60_000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /**
     * @param qryProcessor Query processor.
     * @return size of statement cache.
     */
    private static int getStatementCacheSize(GridQueryProcessor qryProcessor) {
        IgniteH2Indexing h2Idx = (IgniteH2Indexing)qryProcessor.getIndexing();

        ConcurrentStripedPool<H2Connection> conns = GridTestUtils.getFieldValue(h2Idx.connections(), "connPool");

        return conns.stream().mapToInt(H2Connection::statementCacheSize).sum();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLeaksInIgniteH2IndexingOnTerminatedThread() throws Exception {
        final GridQueryProcessor qryProc = grid(0).context().query();

        final String qry = "select * from \"default\".Integer where _val >= \"default\".barrier()";

        for (int i = 0; i < ITERATIONS; ++i) {
            info("Iteration #" + i);

            final CountDownLatch exitLatch = new CountDownLatch(1);
            final CountDownLatch qryExec = new CountDownLatch(THREAD_COUNT);

            IgniteInternalFuture<?> fut = multithreadedAsync(
                new CAX() {
                    @Override public void applyx() throws IgniteCheckedException {
                        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
                             Statement stmt = conn.createStatement()
                        ) {
                            stmt.executeQuery(qry).next();

                            qryExec.countDown();

                            U.await(exitLatch);
                        }
                        catch (Exception ex) {
                            throw new IgniteCheckedException(ex);
                        }
                    }
                }, THREAD_COUNT);

            try {
                qryExec.await(getTestTimeout(), TimeUnit.MILLISECONDS);

                assertThat(getStatementCacheSize(qryProc), greaterOrEqualTo(THREAD_COUNT));
            }
            finally {
                exitLatch.countDown();
            }

            fut.get();

            assertTrue(
                "Unable to wait while statement cache will be cleared",
                waitForCondition(() -> getStatementCacheSize(qryProc) == 0, STMT_CACHE_TTL * 2)
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLeaksInIgniteH2IndexingOnUnusedThread() throws Exception {
        final GridQueryProcessor qryProc = grid(0).context().query();

        final String qry = "select * from \"default\".Integer where _val >= \"default\".barrier()";

        for (int i = 0; i < ITERATIONS; ++i) {
            info("Iteration #" + i);

            final CountDownLatch exitLatch = new CountDownLatch(1);
            final CountDownLatch qryExec = new CountDownLatch(THREAD_COUNT);

            IgniteInternalFuture<?> fut = multithreadedAsync(
                new CAX() {
                    @Override public void applyx() throws IgniteCheckedException {
                        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");
                             Statement stmt = conn.createStatement()
                        ) {
                            stmt.executeQuery(qry).next();

                            qryExec.countDown();

                            U.await(exitLatch);
                        }
                        catch (SQLException ex) {
                            throw new IgniteCheckedException(ex);
                        }
                    }
                }, THREAD_COUNT);

            try {
                qryExec.await(getTestTimeout(), TimeUnit.MILLISECONDS);

                assertThat(getStatementCacheSize(qryProc), greaterOrEqualTo(THREAD_COUNT));

                assertTrue(
                    "Unable to wait while statement cache will be cleared",
                    waitForCondition(() -> getStatementCacheSize(qryProc) == 0, STMT_CACHE_TTL * 2)
                );
            }
            finally {
                exitLatch.countDown();
            }

            fut.get();
        }
    }

    /** Cyclic arrier to synchronize query executions during iteration. */
    @QuerySqlFunction
    public static int barrier() {
        try {
            BARRIER.await();
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }

        return 42;
    }

    /**
     * @param wanted Wanted.
     */
    private static <T extends Comparable<? super T>> Matcher<T> greaterOrEqualTo(T wanted) {
        return new CustomMatcher<T>("should be greater or equal to " + wanted) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            @Override public boolean matches(Object item) {
                return wanted != null && item instanceof Comparable && ((Comparable)item).compareTo(wanted) >= 0;
            }
        };
    }
}
