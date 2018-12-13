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

package org.apache.ignite.internal.processors.query;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

/**
 * Tests for running queries.
 */
public class RunningQueriesTest extends GridCommonAbstractTest {

    /** Timeout in sec. */
    private static final long TIMEOUT_IN_SEC = 5;

    /** Ignite. */
    private static IgniteEx ignite;

    /** Node count. */
    private static final int NODE_CNT = 2;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = (IgniteEx)startGrids(NODE_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ignite.destroyCache(DEFAULT_CACHE_NAME);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Integer>()
            .setName(DEFAULT_CACHE_NAME)
            .setQueryEntities(Collections.singletonList(new QueryEntity(Integer.class, Integer.class)))
            .setSqlFunctionClasses(TestSQLFunctions.class)
        );

        cache.put(0, 0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        Assert.assertEquals(0, TestSQLFunctions.barrier.getNumberWaiting());
    }

    /**
     *
     */
    public void testQueriesOriginalText() throws Exception {
        TestSQLFunctions.newBarrier(3);

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        IgniteInternalFuture<List<List<?>>> fut1 = GridTestUtils.runAsync(() -> cache.query(new SqlFieldsQuery(
            "SELECT * FROM /* comment */ Integer WHERE awaitBarrier() = 0")).getAll());

        IgniteInternalFuture<List<Cache.Entry<Integer, Integer>>> fut2 = GridTestUtils.runAsync(() -> cache.query(
            new SqlQuery<Integer, Integer>(Integer.class, "FROM /* comment */ Integer WHERE awaitBarrier() = 0"))
            .getAll());

        GridTestUtils.waitForCondition(() -> TestSQLFunctions.barrier.getNumberWaiting() == 1, TIMEOUT_IN_SEC * 1000);

        Collection<GridRunningQueryInfo> runningQueries = ignite.context().query().runningQueries(-1);

        assertEquals(2, runningQueries.size());

        for (GridRunningQueryInfo info : runningQueries)
            assertTrue("Failed to find comment in query: " + info.query(), info.query().contains("/* comment */"));

        assertNoRunningQueries(ignite);

        TestSQLFunctions.barrier.await(TIMEOUT_IN_SEC, TimeUnit.SECONDS);

        fut1.get(TIMEOUT_IN_SEC, TimeUnit.SECONDS);

        fut2.get(TIMEOUT_IN_SEC, TimeUnit.SECONDS);

        assertNoRunningQueries();
    }

    /**
     *
     */
    public void testDML() throws Exception {
        TestSQLFunctions.newBarrier(2);

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery("DELETE FROM /* comment */ Integer WHERE awaitBarrier() = 0 and 1=1");

        IgniteInternalFuture<List<List<?>>> fut = GridTestUtils.runAsync(() -> cache.query(qry).getAll());

        GridTestUtils.waitForCondition(() -> TestSQLFunctions.barrier.getNumberWaiting() == 1, TIMEOUT_IN_SEC * 1000);

        Collection<GridRunningQueryInfo> runningQueries = ignite.context().query().runningQueries(-1);

        assertEquals(1, runningQueries.size());

        assertNoRunningQueries(ignite);

        runningQueries.forEach((info) -> Assert.assertEquals(qry.getSql(), info.query()));

        TestSQLFunctions.barrier.await(TIMEOUT_IN_SEC, TimeUnit.SECONDS);

        fut.get(TIMEOUT_IN_SEC, TimeUnit.SECONDS);

        assertNoRunningQueries();
    }

    /**
     * Check all nodes except passed as parameter on no any running queries.
     *
     * @param excludeNodes Nodes shich will be excluded from check.
     */
    private void assertNoRunningQueries(IgniteEx... excludeNodes) {
        Set<UUID> excludeIds = Stream.of(excludeNodes).map((ignite) -> ignite.localNode().id()).collect(Collectors.toSet());

        for (Ignite g : G.allGrids()) {
            IgniteEx node = (IgniteEx)g;

            if (!excludeIds.contains(node.localNode().id())) {
                Collection<GridRunningQueryInfo> runningQueries = node.context().query().runningQueries(-1);

                Assert.assertEquals(0, runningQueries.size());
            }
        }
    }


    /**
     * Utility class with custom SQL functions.
     */
    public static class TestSQLFunctions {
        /** Barrier. */
        static CyclicBarrier barrier = new CyclicBarrier(2);

        /**
         * Create and set new CyclicBarrier for the function.
         *
         * @param parties the number of threads that must invoke await method before the barrier is tripped
         */
        static void newBarrier(int parties) {
            barrier = new CyclicBarrier(parties);
        }

        /**
         * Await cyclic barrier twice, first time to wait for enter method, second time to wait for collecting running
         * queries.
         */
        @QuerySqlFunction
        public static long awaitBarrier() {
            try {
                barrier.await();
            }
            catch (Exception ignored) {
                // No-op.
            }

            return 0;
        }
    }
}
