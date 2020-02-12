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

package org.apache.ignite.internal.metric;

import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.query.RunningQueryManager;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for statistics of user initiated queries execution, that require grid restart.
 *
 * @see RunningQueryManager
 */
public class SqlStatisticsUserQueriesLongTest extends UserQueriesTestBase {
    /**
     * Teardown.
     */
    @After
    public void stopAll() {
        stopAllGrids();
    }

    /**
     * Check that after grid starts, counters are 0.
     *
     * @throws Exception on fail.
     */
    @Test
    public void testInitialValuesAreZero() throws Exception {
        startGrids(2);

        createCacheFrom(grid(REDUCER_IDX));

        Assert.assertEquals(0, longMetricValue(REDUCER_IDX, "success"));
        Assert.assertEquals(0, longMetricValue(REDUCER_IDX, "failed"));
        Assert.assertEquals(0, longMetricValue(REDUCER_IDX, "canceled"));

        Assert.assertEquals(0, longMetricValue(MAPPER_IDX, "success"));
        Assert.assertEquals(0, longMetricValue(MAPPER_IDX, "failed"));
        Assert.assertEquals(0, longMetricValue(MAPPER_IDX, "canceled"));
    }

    /**
     * Verify that each fail metric is updated properly if error happened on remote map step.
     *
     * To achive that, we start one server + one client. Queries are started from client, so map step is only on server,
     * and reduce phase is only on client.
     *
     * @throws Exception on fail.
     */
    @Test
    public void testMetricsOnRemoteMapFail() throws Exception {
        startGrid(MAPPER_IDX);
        startClientGrid(REDUCER_IDX);

        IgniteCache cache = createCacheFrom(grid(REDUCER_IDX));

        final String mapFailMsg = "Failed to execute map query on remote node";

        SuspendQuerySqlFunctions.refresh();

        SuspendQuerySqlFunctions.setProcessRowsToSuspend(1);

        assertMetricsIncrementedOnlyOnReducer(() -> GridTestUtils.assertThrows(
            log,
            () -> cache.query(new SqlFieldsQuery("SELECT * FROM TAB WHERE ID < 200 AND failFunction() = 5")).getAll(),
            CacheException.class,
            mapFailMsg), "failed");

        SuspendQuerySqlFunctions.refresh();

        SuspendQuerySqlFunctions.setProcessRowsToSuspend(1);

        assertMetricsIncrementedOnlyOnReducer(() ->
            startAndKillQuery(new SqlFieldsQuery("SELECT * FROM TAB WHERE ID < 200 AND suspendHook(ID) <> 5 ")),
            "success", "failed", "canceled");
    }

    /**
     * Verify that each fail metric is updated properly if error happened on local map step.
     *
     * To check this we start one server node, which is used to perform the query. Secondary clinent node should not
     * participate the query execution.
     *
     * @throws Exception on fail.
     */
    @Test
    public void testMetricsOnLocalMapFail() throws Exception {
        startGrid(REDUCER_IDX);
        startClientGrid(MAPPER_IDX);

        IgniteCache cache = createCacheFrom(grid(REDUCER_IDX));

        final String mapFailMsg = "Failed to execute map query on remote node";

        SuspendQuerySqlFunctions.refresh();

        SuspendQuerySqlFunctions.setProcessRowsToSuspend(1);

        assertMetricsIncrementedOnlyOnReducer(() -> GridTestUtils.assertThrows(
            log,
            () -> cache.query(new SqlFieldsQuery("SELECT * FROM TAB WHERE ID < 200 AND failFunction() = 5")).getAll(),
            CacheException.class,
            mapFailMsg), "failed");

        SuspendQuerySqlFunctions.refresh();

        SuspendQuerySqlFunctions.setProcessRowsToSuspend(1);

        assertMetricsIncrementedOnlyOnReducer(() ->
                startAndKillQuery(new SqlFieldsQuery("SELECT * FROM TAB WHERE ID < 200 AND suspendHook(ID) <> 5 ")),
            "success", "failed", "canceled");
    }

    /**
     * Verify that error metrics are updated if that error happened on reduce step.
     *
     * To achive that, we start one server + one client. Queries are started from client, so map step is only on server,
     * and reduce phase is only on client.
     *
     */
    @Test
    public void testMetricsOnRemoteReduceStepFail() throws Exception {
        startGrid(MAPPER_IDX);

        // Since reduce node is client, it doesn't execute map queries, and reduce part fails.
        startClientGrid(REDUCER_IDX);

        IgniteCache cache = createCacheFrom(grid(REDUCER_IDX));

        final String rdcFailMsg = "Failed to run reduce query locally";

        // general failure
        SuspendQuerySqlFunctions.refresh();

        assertMetricsIncrementedOnlyOnReducer(() -> {
            GridTestUtils.assertThrows(
                log,
                () -> cache.query(new SqlFieldsQuery(
                    "SELECT id, failFunction(count(id)) FROM TAB WHERE ID < 5 GROUP BY NAME HAVING ID < 5")).getAll(),
                CacheException.class,
                rdcFailMsg);
        }, "failed");

        // Cancel is hard to test in reducer phase.
    }
}
