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
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for running queries.
 */
@RunWith(JUnit4.class)
public class RunningQueriesTest extends GridCommonAbstractTest {
    /**
     *
     */
    @Test
    public void testQueriesOriginalText() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Integer>()
            .setName("cache")
            .setQueryEntities(Collections.singletonList(new QueryEntity(Integer.class, Integer.class)))
            .setSqlFunctionClasses(TestSQLFunctions.class)
        );

        cache.put(0, 0);

        GridTestUtils.runAsync(() -> cache.query(new SqlFieldsQuery(
            "SELECT * FROM /* comment */ Integer WHERE awaitBarrier() = 0")).getAll());

        GridTestUtils.runAsync(() -> cache.query(new SqlQuery<Integer, Integer>(Integer.class,
            "FROM /* comment */ Integer WHERE awaitBarrier() = 0")).getAll());

        TestSQLFunctions.barrier.await();

        Collection<GridRunningQueryInfo> runningQueries = ignite.context().query().runningQueries(-1);

        TestSQLFunctions.barrier.await();

        assertEquals(2, runningQueries.size());

        for (GridRunningQueryInfo info : runningQueries)
            assertTrue("Failed to find comment in query: " + info.query(), info.query().contains("/* comment */"));
    }

    /**
     * Utility class with custom SQL functions.
     */
    public static class TestSQLFunctions {
        /** Barrier. */
        static CyclicBarrier barrier = new CyclicBarrier(3);

        /**
         * Await cyclic barrier twice, first time to wait for enter method, second time to wait for collecting running
         * queries.
         */
        @QuerySqlFunction
        public static long awaitBarrier() {
            try {
                barrier.await();
                barrier.await();
            }
            catch (Exception ignored) {
                // No-op.
            }

            return 0;
        }
    }
}
