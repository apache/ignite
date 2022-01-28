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

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 *
 */
public class DefaultQueryTimeoutConfigurationTest extends AbstractIndexingCommonTest {
    /** */
    private long cfgDfltQryTimeout = 0;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(
                new SqlConfiguration()
                    .setDefaultQueryTimeout(getCfgDefaultQueryTimeout())
            );
    }

    /**
     * Check negative value of the default query timeout passed through {@link SqlConfiguration}.
     * Steps:
     * - set default query timeout to negative value;
     * - try to start node;
     * - check appropriate exception is thrown.
     */
    @Test
    public void testNegativeDefaultTimeoutByConfig() throws Exception {
        cfgDfltQryTimeout = -1;

        GridTestUtils.assertThrowsWithCause(() -> {
            startGrid(0);

            return null;
        }, IllegalArgumentException.class);
    }

    /**
     * Check negative value of the default query timeout passed through API of the  {@link DistributedSqlConfiguration}.
     * Steps:
     * - start node;
     * - try to set default query timeout to negative value
     *      via {@link DistributedSqlConfiguration#defaultQueryTimeout(int)};
     * - check appropriate exception is thrown.
     */
    @Test
    public void testNegativeDefaultTimeout() throws Exception {
        startGrid(0);

        GridTestUtils.assertThrowsWithCause(() -> {
            try {
                setDefaultQueryTimeout(-1);
            }
            catch (IgniteCheckedException e) {
                throw new RuntimeException("Unexpected", e);
            }
        }, IllegalArgumentException.class);
    }

    /**
     * Check zero value of the default query timeout passed through API of the  {@link DistributedSqlConfiguration}.
     * Steps:
     * - start node;
     * - try to set default query timeout to zero value
     *      via {@link DistributedSqlConfiguration#defaultQueryTimeout(int)};
     * - no exception must be thrown.
     */
    @Test
    public void testZeroDefaultTimeout() throws Exception {
        startGrid(0);

        setDefaultQueryTimeout(0);
    }

    /**
     * Check positive value of the default query timeout passed through API of the  {@link DistributedSqlConfiguration}.
     * Steps:
     * - start node;
     * - try to set default query timeout to positive value
     *      via {@link DistributedSqlConfiguration#defaultQueryTimeout(int)};
     * - no exception must be thrown.
     */
    @Test
    public void testPositiveDefaultTimeout() throws Exception {
        startGrid(0);

        setDefaultQueryTimeout(1);
    }

    /**
     * Check too big  value of the default query timeout passed through {@link SqlConfiguration}.
     * Steps:
     * - set default query timeout to too big value;
     * - try to start node;
     * - check appropriate exception is thrown.
     */
    @Test
    public void testTooBigDefaultTimeout() throws Exception {
        cfgDfltQryTimeout = Integer.MAX_VALUE + 1L;

        assertTrue(cfgDfltQryTimeout > Integer.MAX_VALUE);

        GridTestUtils.assertThrowsWithCause(() -> {
            startGrid(0);

            return null;
        }, IllegalArgumentException.class);
    }

    /**
     * Check change the default query timeout via API of the {@link DistributedSqlConfiguration} on runtime.
     * Steps:
     * - start two servers an one client node;
     * - set default query timeout to 2000 ms;
     * - execute query that takes 1000 ms;
     * - check that query is successful;
     * - set default query timeout to 500 ms;
     * - execute query that takes 1000 ms;
     * - check that query fails with QueryCancelledException.
     */
    @Test
    public void testChangeDefaultTimeout() throws Exception {
        startGrids(2);
        startClientGrid(2);

        setDefaultQueryTimeout(2000);

        TimedQueryHelper helper = new TimedQueryHelper(1000, DEFAULT_CACHE_NAME);

        helper.createCache(grid(0));

        String sql = helper.buildTimedQuery();

        // assert no exception
        for (Ignite ign : G.allGrids())
            executeQuery(ign, sql);

        setDefaultQueryTimeout(500);

        for (final Ignite ign : G.allGrids())
            GridTestUtils.assertThrowsWithCause(() -> executeQuery(ign, sql), QueryCancelledException.class);
    }

    /** */
    private long getCfgDefaultQueryTimeout() {
        return cfgDfltQryTimeout;
    }

    /** */
    private List<List<?>> executeQuery(Ignite ign, String sql) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        return ((IgniteEx)ign).context().query().querySqlFields(qry, false).getAll();
    }

    /** */
    private void setDefaultQueryTimeout(final int timeout) throws IgniteCheckedException {
        ((IgniteH2Indexing)grid(0).context().query().getIndexing()).distributedConfiguration().defaultQueryTimeout(timeout);

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
