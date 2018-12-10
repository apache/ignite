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
import java.util.concurrent.CyclicBarrier;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

/**
 * Tests for running queries.
 */
public class RunningQueriesTest extends GridCommonAbstractTest {

    //ToDo: start grid once for all tests.

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     *
     */
    public void testSqlQueryWithcComment() throws Exception {
        Query qry = new SqlQuery<>(Integer.class, "FROM /* comment */ Integer WHERE awaitBarrier() = 0");

        testQueryWithComment(qry);
    }

    /**
     *
     */
    public void testSqlFieldsQueryWithComment() throws Exception {
        Query qry = new SqlFieldsQuery("SELECT * FROM /* comment */ Integer WHERE awaitBarrier() = 0");

        testQueryWithComment(qry);
    }

    /**
     *
     */
    private void testQueryWithComment(Query qry) throws Exception {
        TestSQLFunctions.newBarrier(2);

        IgniteEx ignite = startGrid(0);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Integer>()
            .setName("cache")
            .setQueryEntities(Collections.singletonList(new QueryEntity(Integer.class, Integer.class)))
            .setSqlFunctionClasses(TestSQLFunctions.class)
        );

        cache.put(0, 0);

        GridTestUtils.runAsync(() -> cache.query(qry).getAll());

        GridTestUtils.waitForCondition(() -> TestSQLFunctions.barrier.getNumberWaiting() == 1, 1000);

        Collection<GridRunningQueryInfo> runningQueries = ignite.context().query().runningQueries(-1);

        TestSQLFunctions.barrier.await();

        assertEquals(2, runningQueries.size());

        for (GridRunningQueryInfo info : runningQueries)
            if (info.userQuery())
                assertTrue("Failed to find comment in user query: " + info.query(), info.query().contains("/* comment */"));
            else
                assertFalse("Comment shouldn't present in subquery: " + info.query(), info.query().contains("/* comment */"));
    }

    /**
     *
     */
    public void testSimpleDML() throws Exception {
        TestSQLFunctions.newBarrier(2);

        IgniteEx ignite = startGrid(0);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Integer>()
            .setName("cache")
            .setQueryEntities(Collections.singletonList(new QueryEntity(Integer.class, Integer.class)))
            .setSqlFunctionClasses(TestSQLFunctions.class)
        );

        cache.put(0, 0);

        SqlFieldsQuery qry = new SqlFieldsQuery("DELETE FROM /* comment */ Integer WHERE awaitBarrier() = 0 and 1=1");

        GridTestUtils.runAsync(() -> cache.query(qry).getAll());

        GridTestUtils.waitForCondition(() -> TestSQLFunctions.barrier.getNumberWaiting() == 1, 1000);

        Collection<GridRunningQueryInfo> runningQueries = ignite.context().query().runningQueries(-1);

        TestSQLFunctions.barrier.await();

        assertEquals(3, runningQueries.size());

        for (GridRunningQueryInfo info : runningQueries) {
            System.out.println(info);
            if (info.userQuery())
                Assert.assertEquals(qry.getSql(), info.query());
            else
                Assert.assertNotEquals(qry.getSql(), info.query());
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
