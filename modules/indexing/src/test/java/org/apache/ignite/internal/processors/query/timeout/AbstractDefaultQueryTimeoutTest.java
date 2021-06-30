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

package org.apache.ignite.internal.processors.query.timeout;

import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 *
 */
public abstract class AbstractDefaultQueryTimeoutTest extends AbstractIndexingCommonTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    protected boolean updateQuery() {
        return false;
    }

    /**
     * Check the default query timeout.
     * Steps:
     * - start server node;
     * - set default query timeout to 0 ms (timeout is disabled by default);
     * - execute query that takes 1000 ms;
     * - check that query is successful.
     */
    @Test
    public void testNoExplicitTimeout1() throws Exception {
        checkQueryNoExplicitTimeout(1000, 0, false);
    }

    /**
     * Check the default query timeout.
     * Steps:
     * - start server node;
     * - set default query timeout to 1500 ms;
     * - execute query that takes 1000 ms;
     * - check that query is successful.
     */
    @Test
    public void testNoExplicitTimeout2() throws Exception {
        checkQueryNoExplicitTimeout(1000, 1500, false);
    }

    /**
     * Check the default query timeout.
     * Steps:
     * - start server node;
     * - set default query timeout to 500 ms;
     * - execute query that takes 1000 ms;
     * - check that query fails with QueryCancelledException.
     */
    @Test
    public void testNoExplicitTimeout3() throws Exception {
        checkQueryNoExplicitTimeout(1000, 500, true);
    }

    /**
     * Check the explicit query timeout.
     * Steps:
     * - start server node;
     * - set default query timeout to 0 ms (timeout is disabled by default);
     * - execute query that takes 500 ms and explicit query timeout 0 ms;
     * - check that query is successful.
     */
    @Test
    public void testExplicitTimeout1() throws Exception {
        checkQuery(500, 0, 0, false);
    }

    /**
     * Check the explicit query timeout.
     * Steps:
     * - start server node;
     * - set default query timeout to 0 ms (timeout is disabled by default);
     * - execute query that takes 500 ms and explicit query timeout 1000 ms;
     * - check that query is successful.
     */
    @Test
    public void testExplicitTimeout2() throws Exception {
        checkQuery(500, 1000, 0, false);
    }

    /**
     * Check the explicit query timeout.
     * Steps:
     * - start server node;
     * - set default query timeout to 0 ms (timeout is disabled by default);
     * - execute query that takes 2000 ms and explicit query timeout 1000 ms;
     * - check that query fails with QueryCancelledException.
     */
    @Test
    public void testExplicitTimeout3() throws Exception {
        checkQuery(2000, 1000, 0, true);
    }

    /**
     * Check the explicit query timeout.
     * Steps:
     * - start server node;
     * - set default query timeout to 1000 ms;
     * - execute query that takes 1500 ms and explicit query timeout 0 ms (timeout is disabled);
     * - check that query is successful.
     */
    @Test
    public void testExplicitTimeout4() throws Exception {
        checkQuery(1500, 0, 1000, false);
    }

    /**
     * Check the explicit query timeout.
     * Steps:
     * - start server node;
     * - set default query timeout to 1000 ms;
     * - execute query that takes 1500 ms and explicit query timeout 0 ms;
     * - check that query is successful.
     */
    @Test
    public void testExplicitTimeout5() throws Exception {
        checkQuery(1000, 1500, 500, false);
    }

    /**
     * Check the explicit query timeout.
     * Steps:
     * - start server node;
     * - set default query timeout to 500 ms;
     * - execute query that takes 2000 ms and explicit query timeout 1000 ms;
     * - check that query fails with QueryCancelledException.
     */
    @Test
    public void testExplicitTimeout6() throws Exception {
        checkQuery(2000, 1000, 500, true);
    }

    /**
     * Check the explicit query timeout.
     * Steps:
     * - start server node;
     * - set default query timeout to 2000 ms;
     * - execute query that takes 500 ms and explicit query timeout 1000 ms;
     * - check that query is successful.
     */
    @Test
    public void testExplicitTimeout7() throws Exception {
        checkQuery(500, 1000, 2000, false);
    }

    /**
     * Check the explicit query timeout.
     * Steps:
     * - start server node;
     * - set default query timeout to 2000 ms;
     * - execute query that takes 2000 ms and explicit query timeout 1000 ms;
     * - check that query fails with QueryCancelledException.
     */
    @Test
    public void testExplicitTimeout8() throws Exception {
        checkQuery(2000, 1000, 2000, true);
    }

    /**
     * Check the explicit query timeout.
     * Steps:
     * - start server node;
     * - set default query timeout to 2000 ms;
     * - execute two queries that takes 1000 ms simultaneous:
     *      one query with explicit timeout 500 ms and other with explicit timeout 1500 ms;
     * - check that the first query fails with QueryCancelledException and the second successful.
     */
    @Test
    public void testConcurrent() throws Exception {
        IgniteEx ign = startGrid(0);

        setDefaultQueryTimeout(2000);

        prepareQueryExecution();

        TimedQueryHelper helper = new TimedQueryHelper(1000, DEFAULT_CACHE_NAME);

        helper.createCache(ign);

        String qryText = updateQuery() ? helper.buildTimedUpdateQuery() : helper.buildTimedQuery();

        IgniteInternalFuture<?> fut1 = GridTestUtils.runAsync(() -> {
            executeQuery(qryText, 500);

            return null;
        });

        IgniteInternalFuture<?> fut2 = GridTestUtils.runAsync(() -> {
            executeQuery(qryText, 1500);

            return null;
        });

        assertQueryCancelled((Callable<?>)fut1::get);

        fut2.get(); // assert no exception here
    }

    /** */
    private void checkQueryNoExplicitTimeout(long execTime, int defaultTimeout, boolean expectCancelled)
        throws Exception {
        checkQuery0(execTime, null, defaultTimeout, expectCancelled);
    }

    /** */
    protected void checkQuery(long execTime, int explicitTimeout, int defaultTimeout, boolean expectCancelled)
        throws Exception {
        checkQuery0(execTime, explicitTimeout, defaultTimeout, expectCancelled);
    }

    /** */
    private void checkQuery0(long execTime, Integer explicitTimeout, int defaultTimeout, boolean expectCancelled)
        throws Exception {
        startGrid(0);

        setDefaultQueryTimeout(defaultTimeout);

        prepareQueryExecution();

        TimedQueryHelper helper = new TimedQueryHelper(execTime, DEFAULT_CACHE_NAME);

        helper.createCache(grid(0));

        Callable<Void> c = () -> {
            String qryText = updateQuery() ? helper.buildTimedUpdateQuery() : helper.buildTimedQuery();

            if (explicitTimeout != null)
                executeQuery(qryText, explicitTimeout);
            else
                executeQuery(qryText);

            return null;
        };

        if (expectCancelled)
            assertQueryCancelled(c);
        else
            c.call(); // assert no exception here
    }

    /** */
    protected void prepareQueryExecution() throws Exception {
    }

    /** */
    protected abstract void executeQuery(String sql) throws Exception;

    /** */
    protected abstract void executeQuery(String sql, int timeout) throws Exception;

    /** */
    protected abstract void assertQueryCancelled(Callable<?> c);

    /** */
    protected void setDefaultQueryTimeout(final int timeout) throws IgniteCheckedException {
        ((IgniteH2Indexing)grid(0).context().query().getIndexing())
            .distributedConfiguration().defaultQueryTimeout(timeout);

        assertTrue(GridTestUtils.waitForCondition(() -> {
            for (Ignite ign : G.allGrids()) {
                if (((IgniteH2Indexing)((IgniteEx)ign).context().query().getIndexing())
                    .distributedConfiguration().defaultQueryTimeout() != timeout)
                    return false;
            }

            return true;
        }, 2000L));
    }
}
