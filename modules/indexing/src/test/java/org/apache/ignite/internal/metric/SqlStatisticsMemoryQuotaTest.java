/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.metric;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link SqlStatisticsHolderMemoryQuotas}. In this test we check that memory metrics reports plausible
 * values. Here we want to verify metrics based on the new framework work well, not {@link H2MemoryTracker}.
 */
public class SqlStatisticsMemoryQuotaTest extends SqlStatisticsAbstractTest {
    /**
     * This callback validates that some memory is reserved.
     */
    private static final MemValidator MEMORY_IS_USED = (freeMem, maxMem) -> {
        if (freeMem == maxMem)
            fail("Expected some memory reserved.");
    };

    /**
     * This callback validates that no "sql" memory is reserved.
     */
    private static final MemValidator MEMORY_IS_FREE = (freeMem, maxMem) -> {
        if (freeMem < maxMem)
            fail(String.format("Expected no memory reserved: [freeMem=%d, maxMem=%d]", freeMem, maxMem));
    };

    /**
     * Clean up.
     */
    @After
    public void cleanUp() {
        stopAllGrids();
    }

    /**
     * Clean up.
     */
    @Before
    public void setup() {
        SqlStatisticsAbstractTest.SuspendQuerySqlFunctions.refresh();
    }


    /**
     * Check values of all sql memory metrics right after grid starts and no queries are executed.
     *
     * @throws Exception on fail.
     */
    @Test
    public void testAllMetricsValuesAfterGridStarts() throws Exception {
        startGrids(2);

        assertEquals(0, longMetricValue(0, "requests"));
        assertEquals(0, longMetricValue(1, "requests"));

        long dfltSqlGlobQuota = (long)(Runtime.getRuntime().maxMemory() * 0.6);

        assertEquals(dfltSqlGlobQuota, longMetricValue(0, "maxMem"));
        assertEquals(dfltSqlGlobQuota, longMetricValue(1, "maxMem"));

        assertEquals(dfltSqlGlobQuota, longMetricValue(0, "freeMem"));
        assertEquals(dfltSqlGlobQuota, longMetricValue(1, "freeMem"));
    }

    /**
     * Check metric for quota requests count is updated: <br/>
     * 1) On distributed query both nodes metrics are updated <br/>
     * 2) On local query, only one node metric is updated <br/>
     *
     * @throws Exception on fail.
     */
    @Test
    public void testRequestsMetricIsUpdatedAfterDistributedAndLocalQueries() throws Exception {
        startGrids(2);

        int connNodeIdx = 1;
        int otherNodeIdx = 0;

        IgniteCache cache = createCacheFrom(grid(connNodeIdx));

        int runCnt = 10;

        final String scanQry = "SELECT * FROM TAB WHERE ID <> 5";

        for (int i = 0; i < runCnt; i++)
            cache.query(new SqlFieldsQuery(scanQry)).getAll();

        long otherCntAfterDistQry = longMetricValue(otherNodeIdx, "requests");
        long connCntAfterDistQry = longMetricValue(connNodeIdx, "requests");

        assertTrue(otherCntAfterDistQry >= runCnt);
        assertTrue(connCntAfterDistQry >= runCnt);

        // And then run local query and check that only connect node metric has changed.

        for (int i = 0; i < runCnt; i++)
            cache.query(new SqlFieldsQuery(scanQry).setLocal(true)).getAll();

        long otherCntAfterLocQry = longMetricValue(otherNodeIdx, "requests");
        long connCntAfterLocQry = longMetricValue(connNodeIdx, "requests");

        assertTrue(otherCntAfterLocQry == otherCntAfterDistQry);
        assertTrue(connCntAfterLocQry >= connCntAfterDistQry + runCnt);
    }

    /**
     * Check that memory metric reports non-zero memory is reserved on both nodes when distributed query is executed and
     * that memory is released when the query finishes.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testMemoryIsTrackedWhenDistributedQueryIsRunningAndReleasedOnFinish() throws Exception {
        startGrids(2);

        int connNodeIdx = 1;
        int otherNodeIdx = 0;

        IgniteCache cache = createCacheFrom(grid(connNodeIdx));

        final String scanQry = "SELECT * FROM TAB WHERE ID <> suspendHook(5)";

        IgniteInternalFuture distQryIsDone =
            runAsyncX(() -> cache.query(new SqlFieldsQuery(scanQry)).getAll());

        SqlStatisticsAbstractTest.SuspendQuerySqlFunctions.awaitQueryStopsInTheMiddle();

        validateMemoryUsageOn(connNodeIdx, MEMORY_IS_USED);
        validateMemoryUsageOn(otherNodeIdx, MEMORY_IS_USED);

        SqlStatisticsAbstractTest.SuspendQuerySqlFunctions.resumeQueryExecution();

        distQryIsDone.get(WAIT_OP_TIMEOUT_SEC, TimeUnit.SECONDS);

        validateMemoryUsageOn(connNodeIdx, MEMORY_IS_FREE);
        validateMemoryUsageOn(otherNodeIdx, MEMORY_IS_FREE);
    }

    /**
     * Check that memory metric reports non-zero memory is reserved only on one node when local query is executed and
     * that memory is released when the query finishes. The other node should not reserve the memory at any moment.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testMemoryIsTrackedWhenLocalQueryIsRunningAndReleasedOnFinish() throws Exception {
        startGrids(2);

        int connNodeIdx = 1;
        int otherNodeIdx = 0;

        IgniteCache cache = createCacheFrom(grid(connNodeIdx));

        final String scanQry = "SELECT * FROM TAB WHERE ID <> suspendHook(5)";

        IgniteInternalFuture locQryIsDone =
            runAsyncX(() -> cache.query(new SqlFieldsQuery(scanQry).setLocal(true)).getAll());

        SqlStatisticsAbstractTest.SuspendQuerySqlFunctions.awaitQueryStopsInTheMiddle();

        validateMemoryUsageOn(connNodeIdx, MEMORY_IS_USED);
        validateMemoryUsageOn(otherNodeIdx, MEMORY_IS_FREE);

        SqlStatisticsAbstractTest.SuspendQuerySqlFunctions.resumeQueryExecution();

        locQryIsDone.get(WAIT_OP_TIMEOUT_SEC, TimeUnit.SECONDS);

        validateMemoryUsageOn(connNodeIdx, MEMORY_IS_FREE);
        validateMemoryUsageOn(otherNodeIdx, MEMORY_IS_FREE);
    }

    /**
     * Check that if we set different sql mem pool sizes for 2 different nodes, appropriate metric values reflect this
     * fact.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testMaxMemMetricShowCustomMaxMemoryValuesForDifferentNodes() throws Exception {
        final int oneMaxMem = 512 * 1024;
        final int otherMaxMem = 1024 * 1024;
        final int unlimMaxMem = -1;

        final int oneNodeIdx = 0;
        final int otherNodeIdx = 1;
        final int unlimNodeIdx = 2;

        startGridWithMaxMem(oneNodeIdx, oneMaxMem);
        startGridWithMaxMem(otherNodeIdx, otherMaxMem);
        startGridWithMaxMem(unlimNodeIdx, unlimMaxMem);

        assertEquals(oneMaxMem, longMetricValue(oneNodeIdx, "maxMem"));
        assertEquals(otherMaxMem, longMetricValue(otherNodeIdx, "maxMem"));
        assertEquals(unlimMaxMem, longMetricValue(unlimNodeIdx, "maxMem"));
    }

    /**
     * Check in complex scenario that metrics are not changed if global (maxMem) quota is unlimited.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testAllMetricsIfMemoryQuotaIsUnlimited() throws Exception {
        final MemValidator quotaUnlim = (free, max) -> {
            assertEquals(-1, max);
            assertEquals(max, free);
        };

        int connNodeIdx = 1;
        int otherNodeIdx = 0;

        startGridWithMaxMem(connNodeIdx, -1);
        startGridWithMaxMem(otherNodeIdx, -1);

        IgniteCache cache = createCacheFrom(grid(connNodeIdx));

        final String scanQry = "SELECT * FROM TAB WHERE ID <> suspendHook(5)";

        IgniteInternalFuture distQryIsDone =
            runAsyncX(() -> cache.query(new SqlFieldsQuery(scanQry)).getAll());

        SqlStatisticsAbstractTest.SuspendQuerySqlFunctions.awaitQueryStopsInTheMiddle();

        validateMemoryUsageOn(connNodeIdx, quotaUnlim);
        validateMemoryUsageOn(otherNodeIdx, quotaUnlim);

        assertEquals(0, longMetricValue(connNodeIdx, "requests"));
        assertEquals(0, longMetricValue(otherNodeIdx, "requests"));

        SqlStatisticsAbstractTest.SuspendQuerySqlFunctions.resumeQueryExecution();

        distQryIsDone.get(WAIT_OP_TIMEOUT_SEC, TimeUnit.SECONDS);

        validateMemoryUsageOn(connNodeIdx, quotaUnlim);
        validateMemoryUsageOn(otherNodeIdx, quotaUnlim);

        assertEquals(0, longMetricValue(connNodeIdx, "requests"));
        assertEquals(0, longMetricValue(otherNodeIdx, "requests"));
    }

    /**
     * Validate memory metrics freeMem and maxMem on the specified node.
     *
     * @param nodeIdx index of the node which metrics to validate.
     * @param validator function(freeMem, maxMem) that validates these values.
     */
    private void validateMemoryUsageOn(int nodeIdx, MemValidator validator) {
        long free = longMetricValue(nodeIdx, "freeMem");
        long maxMem = longMetricValue(nodeIdx, "maxMem");

        if (free > maxMem)
            fail(String.format("Illegal state: there's more free memory (%s) than " +
                "maximum available for sql (%s) on the node %d", free, maxMem, nodeIdx));

        validator.validate(free, maxMem);
    }


    /**
     * Finds LongMetric from sql memory registry by specified metric name and returns it's value.
     *
     * @param gridIdx index of a grid which metric value to find.
     * @param metricName short name of the metric from the "sql memory" metric registry.
     */
    private long longMetricValue(int gridIdx, String metricName) {
        MetricRegistry sqlMemReg = grid(gridIdx).context().metric().registry(SqlStatisticsHolderMemoryQuotas.SQL_QUOTAS_REG_NAME);

        Metric metric = sqlMemReg.findMetric(metricName);

        Assert.assertNotNull("Didn't find metric " + metricName, metric);

        Assert.assertTrue("Expected long metric, but got "+ metric.getClass(),  metric instanceof LongMetric);

        return ((LongMetric)metric).value();
    }



    /**
     * Functional interface to validate memory metrics values.
     */
    private static interface MemValidator {
        /**
         *
         * @param free freeMem metric value.
         * @param max maxMem metric value.
         */
        void validate(long free, long max);
    }
}
