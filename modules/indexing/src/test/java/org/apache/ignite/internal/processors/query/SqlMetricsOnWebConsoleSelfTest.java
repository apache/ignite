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

package org.apache.ignite.internal.processors.query;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.query.VisorQueryDetailMetrics;
import org.apache.ignite.internal.visor.query.VisorQueryDetailMetricsCollectorTaskArg;
import org.apache.ignite.internal.visor.query.VisorQueryHistoryMetricsCollectorTask;
import org.apache.ignite.internal.visor.query.VisorQueryHistoryMetricsResetTask;
import org.junit.Test;

/**
 * Tests for ignite SQL system views.
 */
public class SqlMetricsOnWebConsoleSelfTest extends AbstractIndexingCommonTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @return System schema name.
     */
    protected String systemSchemaName() {
        return "SYS";
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        return super.getConfiguration().setSqlQueryHistorySize(10);
    }

    /**
     * Test Query history system view.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testQueryHistoryMetricsTasks() throws Exception {
        IgniteEx ignite = startGrid(0);

        final String SCHEMA_NAME = "TEST_SCHEMA";

        IgniteCache cache = ignite.createCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setIndexedTypes(Integer.class, String.class)
                .setQueryDetailMetricsSize(10)
                .setSqlSchema(SCHEMA_NAME)
        );

        cache.put(100, "200");

        String sqlHist = "SELECT SCHEMA_NAME, SQL, LOCAL, EXECUTIONS, FAILURES, DURATION_MIN, DURATION_MAX, LAST_START_TIME " +
            "FROM " + systemSchemaName() + ".LOCAL_SQL_QUERY_HISTORY ORDER BY LAST_START_TIME";

        // Execute query without cache context.
        cache.query(new SqlFieldsQuery(sqlHist).setLocal(true)).getAll();

        // Execute query with cache context.
        List<FieldsQueryCursor<List<?>>> cursors = ignite.context().query().querySqlFields(
            ignite.context().cache().cache(DEFAULT_CACHE_NAME).context(),
            new SqlFieldsQuery(sqlHist),
            null,
            true,
            false,
            null
        );

        cursors.forEach(U::closeQuiet);

        // Execute scan query.
        ScanQuery<Object, Object> scanQry = new ScanQuery<>();

        QueryCursor scanCur = cache.query(scanQry);

        scanCur.getAll();
        U.closeQuiet(scanCur);

        // Collect query metrics.
        Collection<VisorQueryDetailMetrics> res = ignite.compute().execute(
            VisorQueryHistoryMetricsCollectorTask.class,
            new VisorTaskArgument<>(
                ignite.localNode().id(),
                new VisorQueryDetailMetricsCollectorTaskArg(-1),
                false
            )
        );

        // First and second queries have the same query string and are grouped in single row.
        assertEquals(2, res.size());

        for (VisorQueryDetailMetrics row: res) {
            if ("SQL_FIELDS".equals(row.getQueryType()))
                assertEquals(2, row.getExecutions());
            else
                assertEquals(1, row.getExecutions());
        }

        // Clear query metrics.
        ignite.compute().execute(
            VisorQueryHistoryMetricsResetTask.class,
            new VisorTaskArgument<>(ignite.localNode().id(), null, false)
        );

        // Collect query metrics after clean.
        res = ignite.compute().execute(
            VisorQueryHistoryMetricsCollectorTask.class,
            new VisorTaskArgument<>(
                ignite.localNode().id(),
                new VisorQueryDetailMetricsCollectorTaskArg(-1),
                false
            )
        );

        assertEquals(0, res.size());
    }
}
