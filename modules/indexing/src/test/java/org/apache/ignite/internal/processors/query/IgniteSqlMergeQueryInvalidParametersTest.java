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

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.log4j.Category;
import org.apache.log4j.Hierarchy;
import org.apache.log4j.Logger;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_MERGE_TABLE_MAX_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE;
import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.DFLT_SQL_MERGE_TABLE_MAX_SIZE;
import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.DFLT_SQL_MERGE_TABLE_PREFETCH_SIZE;

/**
 * Tests behavior with invalid Query parameters.
 */
@SuppressWarnings("unchecked")
public class IgniteSqlMergeQueryInvalidParametersTest extends GridCommonAbstractTest {
    /** Warn logs. */
    private static Set<String> warnLogs = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    /** Logger key. */
    private static Object logKey;

    /** Ignite H2 index. */
    private static IgniteH2Indexing h2Idx;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        System.clearProperty(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE);
        System.clearProperty(IGNITE_SQL_MERGE_TABLE_MAX_SIZE);

        warnLogs.clear();

        h2Idx.start(grid(0).context(), new GridSpinBusyLock());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        getLog4jHashable().put(getLoggerKey(), new TestLoggerDecorator());

        startGrid(0);

        for (String warnLog : warnLogs)
            assertTrue(!warnLog.contains(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE));

        GridQueryProcessor qryProcessor = grid(0).context().query();
        h2Idx = GridTestUtils.getFieldValue(qryProcessor, GridQueryProcessor.class, "idx");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        System.clearProperty(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE);
        System.clearProperty(IGNITE_SQL_MERGE_TABLE_MAX_SIZE);

        getLog4jHashable().remove(getLoggerKey());

        stopAllGrids();
    }

    /**
     *
     */
    public void testParametersValidationDuringMerge() throws Exception {
        SqlFieldsQuery qry = new SqlFieldsQuery("");
        GridCacheTwoStepQuery twoStepQry = new GridCacheTwoStepQuery("", Collections.<QueryTable>emptySet());

        Method mtd = IgniteH2Indexing.class
            .getDeclaredMethod("copyParamsToTwoStepQuery", SqlFieldsQuery.class, GridCacheTwoStepQuery.class);
        mtd.setAccessible(true);

        qry.setSqlMergeTablePrefetchSize(-10)
            .setSqlMergeTableMaxSize(5_000);

        mtd.invoke(h2Idx, qry, twoStepQry);

        assertEquals(1024, twoStepQry.sqlMergeTablePrefetchSize());
        assertEquals(5_000, twoStepQry.sqlMergeTableMaxSize());

        assertTrue(warnLogs.contains("Query parameter sqlMergeTablePrefetchSize (-10) must be positive." +
            " It remains defaulted (1024)"));
        warnLogs.clear();

        qry.setSqlMergeTablePrefetchSize(500)
            .setSqlMergeTableMaxSize(7_000);

        mtd.invoke(h2Idx, qry, twoStepQry);

        assertEquals(256, twoStepQry.sqlMergeTablePrefetchSize());
        assertEquals(7_000, twoStepQry.sqlMergeTableMaxSize());

        assertTrue(warnLogs.contains("Query parameter sqlMergeTablePrefetchSize (500) must be a power of 2." +
            " The value was changed to nearest power of 2 (256)."));
        warnLogs.clear();

        qry.setSqlMergeTablePrefetchSize(-1)
            .setSqlMergeTableMaxSize(200);

        mtd.invoke(h2Idx, qry, twoStepQry);

        assertEquals(1024, twoStepQry.sqlMergeTablePrefetchSize());
        assertEquals(10_000, twoStepQry.sqlMergeTableMaxSize());

        assertTrue(warnLogs.contains("Query parameter sqlMergeTablePrefetchSize (-1) must be positive." +
            " It remains defaulted (1024)"));
        assertTrue(warnLogs.contains("Query parameter sqlMergeTblMaxSize (200) must be greater than " +
            "sqlMergeTablePrefetchSize (1024). It remains defaulted (10000)"));
        warnLogs.clear();

        qry.setSqlMergeTablePrefetchSize(0)
            .setSqlMergeTableMaxSize(0);

        mtd.invoke(h2Idx, qry, twoStepQry);

        assertEquals(1024, twoStepQry.sqlMergeTablePrefetchSize());
        assertEquals(10_000, twoStepQry.sqlMergeTableMaxSize());

        for (String warnLog : warnLogs)
            assertTrue(!warnLog.contains("Query parameter "));
    }

    /**
     *
     */
    public void testParametersValidationDuringStart() throws Exception {
        assertPrefetchSizeEquals(DFLT_SQL_MERGE_TABLE_PREFETCH_SIZE);
        assertMaxSizeEquals(DFLT_SQL_MERGE_TABLE_MAX_SIZE);

        System.setProperty(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE, "500");
        System.setProperty(IGNITE_SQL_MERGE_TABLE_MAX_SIZE, "5000");

        h2Idx.start(grid(0).context(), new GridSpinBusyLock());

        assertPrefetchSizeEquals(256);
        assertMaxSizeEquals(5_000);

        assertTrue(warnLogs.contains(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE + " (500) must be a power of 2. " +
            "The value was changed to nearest power of 2 (256)."));
        warnLogs.clear();

        System.setProperty(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE, "-1");
        System.setProperty(IGNITE_SQL_MERGE_TABLE_MAX_SIZE, "10000");

        h2Idx.start(grid(0).context(), new GridSpinBusyLock());

        assertPrefetchSizeEquals(256);
        assertMaxSizeEquals(10_000);

        assertTrue(warnLogs.contains(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE + " (-1) must be positive and less than " +
            IGNITE_SQL_MERGE_TABLE_MAX_SIZE + " (10000). " + IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE +
            " remains defaulted (256)."));
        warnLogs.clear();

        System.setProperty(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE, "-1");
        System.setProperty(IGNITE_SQL_MERGE_TABLE_MAX_SIZE, "200");

        h2Idx.start(grid(0).context(), new GridSpinBusyLock());

        assertPrefetchSizeEquals(256);
        assertMaxSizeEquals(10_000);

        assertTrue(warnLogs.contains(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE + " (-1) must be positive and less than " +
            IGNITE_SQL_MERGE_TABLE_MAX_SIZE + " (200). The values remains defaulted (256) and (10000)."));
        warnLogs.clear();

        System.setProperty(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE, "10000");
        System.setProperty(IGNITE_SQL_MERGE_TABLE_MAX_SIZE, "5000");

        h2Idx.start(grid(0).context(), new GridSpinBusyLock());

        assertPrefetchSizeEquals(256);
        assertMaxSizeEquals(5000);

        assertTrue(warnLogs.contains(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE + " (10000) must be positive and less than " +
            IGNITE_SQL_MERGE_TABLE_MAX_SIZE + " (5000). " + IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE +
            " remains defaulted (256)."));
        warnLogs.clear();
    }

    /**
     * @param exp Expected.
     */
    private void assertMaxSizeEquals(int exp) {
        assertEquals(exp,
            (int) GridTestUtils.getFieldValue(h2Idx, IgniteH2Indexing.class, "sqlMergeTblMaxSize"));
    }

    /**
     * @param exp Expected.
     */
    private void assertPrefetchSizeEquals(int exp) {
        assertEquals(exp,
            (int) GridTestUtils.getFieldValue(h2Idx, IgniteH2Indexing.class, "sqlMergeTblPrefetchSize"));
    }

    /**
     * @return Log4j {@link Hashtable}.
     */
    private Hashtable<Object, Object> getLog4jHashable() {
        GridTestLog4jLogger log = (GridTestLog4jLogger)log();

        Object impl = GridTestUtils.getFieldValue(log, GridTestLog4jLogger.class, "impl");

        Object repo = GridTestUtils.getFieldValue(impl, Category.class, "repository");

        return GridTestUtils.getFieldValue(repo, Hierarchy.class, "ht");
    }

    /**
     *
     */
    private Object getLoggerKey() throws Exception {
        if (logKey == null) {
            IgniteLogger log = log().getLogger(IgniteH2Indexing.class);

            Object impl = GridTestUtils.getFieldValue(log, GridTestLog4jLogger.class, "impl");

            for (Map.Entry<Object, Object> entry : getLog4jHashable().entrySet()) {
                if (entry.getValue().equals(impl)) {
                    logKey = entry.getKey();

                    break;
                }
            }
        }

        return logKey;
    }

    /**
     * Add warning logs to set.
     */
    private class TestLoggerDecorator extends Logger {
        /**
         * Default constructor.
         */
        TestLoggerDecorator() {
            super(IgniteH2Indexing.class.getName());

            parent = Logger.getRootLogger();
            repository = Logger.getRootLogger().getLoggerRepository();
        }

        /** {@inheritDoc} */
        @Override public void warn(Object msg) {
            super.warn(msg);
            warnLogs.add((String)msg);
        }
    }
}
