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
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.query.running.HeavyQueriesTracker.LONG_QUERY_EXEC_MSG;
import static org.apache.ignite.testframework.GridTestUtils.cartesianProduct;

/** */
@RunWith(Parameterized.class)
public class LongQueryWarningTest extends GridCommonAbstractTest {
    /** */
    private static final long WARN_TIMEOUT = 1000;

    /** */
    private static final long SLEEP = 2 * WARN_TIMEOUT;

    /** */
    private LogListener lsnr;

    /** */
    @Parameterized.Parameter
    public boolean lazy;

    /** */
    @Parameterized.Parameters(name = "lazy={0}")
    public static Collection<Object[]> runConfig() {
        return cartesianProduct(F.asList(false, true));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        lsnr = LogListener.matches(LONG_QUERY_EXEC_MSG).atLeast(1).build();

        ListeningTestLogger log = new ListeningTestLogger(GridAbstractTest.log);

        log.registerListener(lsnr);

        QueryEntity qryEntity = new QueryEntity()
            .setTableName("test")
            .setKeyType(Integer.class.getName())
            .setValueType(Integer.class.getName());

        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<Integer, Integer>("test")
            .setQueryEntities(Collections.singleton(qryEntity))
            .setSqlFunctionClasses(SleepFunction.class);

        return super.getConfiguration(name)
            .setSqlConfiguration(new SqlConfiguration().setLongQueryWarningTimeout(WARN_TIMEOUT))
            .setGridLogger(log)
            .setCacheConfiguration(cacheCfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Checks that warning message is printed when long query is executed in both lazy and non-lazy modes.
     */
    @Test
    public void test() throws Exception {
        IgniteEx ignite = startGrid(getConfiguration("srv"));

        IgniteCache<Integer, Integer> cache = ignite.cache("test");

        for (int i = 0; i < 5; i++)
            cache.put(i, i);

        long startTime = System.currentTimeMillis();

        Iterator<List<?>> iter = cache.query(
            new SqlFieldsQuery("SELECT * FROM test WHERE _key < sleep(?)")
                .setLazy(lazy)
                .setPageSize(1)
                .setArgs(SLEEP)).iterator();

        assertTrue("Iterator with query results must not be empty.", !iter.next().isEmpty());

        long resultSetTime = System.currentTimeMillis() - startTime;

        assertTrue("Expected long query: " + resultSetTime, resultSetTime >= WARN_TIMEOUT);

        assertTrue("Expected long query warning.", lsnr.check());
    }

    /** */
    public static class SleepFunction {
        /** */
        @QuerySqlFunction
        public static long sleep(long x) {
            if (x >= 0)
                try {
                    Thread.sleep(x);
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }

            return x;
        }
    }
}
