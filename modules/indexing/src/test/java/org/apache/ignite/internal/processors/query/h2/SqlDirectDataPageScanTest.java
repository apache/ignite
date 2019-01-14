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
package org.apache.ignite.internal.processors.query.h2;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.lang.Boolean.FALSE;

/**
 */
@RunWith(JUnit4.class)
public class SqlDirectDataPageScanTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDirectScanFlag() throws Exception {
        final String cacheName = "test";

        GridQueryProcessor.idxCls = DirectPageScanIndexing.class;
        IgniteEx server = startGrid(0);
        server.cluster().active(true);

        Ignition.setClientMode(true);
        IgniteEx client = startGrid(1);

        CacheConfiguration<Long,TestData> ccfg = new CacheConfiguration<>(cacheName);
        ccfg.setIndexedTypes(Long.class, TestData.class);
        ccfg.setSqlFunctionClasses(SqlDirectDataPageScanTest.class);

        IgniteCache<Long,TestData> clientCache = client.createCache(ccfg);

        final int keysCnt = 1000;

        for (long i = 0; i < keysCnt; i++)
            clientCache.put(i, new TestData(i));

        IgniteCache<Long,TestData> serverCache = server.cache(cacheName);

        doTestScanQuery(clientCache, keysCnt);
        doTestScanQuery(serverCache, keysCnt);

        doTestSqlQuery(clientCache);
        doTestSqlQuery(serverCache);

        doTestDml(clientCache);
        doTestDml(serverCache);
    }

    private void doTestDml(IgniteCache<Long,TestData> cache) {
        // SQL query (data page scan must be enabled by default).
        DirectPageScanIndexing.callsCnt.set(0);
        int callsCnt = 0;

        checkDml(cache, null);
        assertEquals(++callsCnt, DirectPageScanIndexing.callsCnt.get());

        checkDml(cache, true);
        assertEquals(++callsCnt, DirectPageScanIndexing.callsCnt.get());

        checkDml(cache, false);
        assertEquals(++callsCnt, DirectPageScanIndexing.callsCnt.get());

        checkDml(cache, null);
        assertEquals(++callsCnt, DirectPageScanIndexing.callsCnt.get());
    }

    private void checkDml(IgniteCache<Long,TestData> cache, Boolean dataPageScanEnabled) {
        DirectPageScanIndexing.expectedDataPageScanEnabled = dataPageScanEnabled;

        assertEquals(0L, cache.query(new SqlFieldsQuery(
            "update TestData set z = z + 1 where check_scan_flag(?,false)")
            .setDataPageScanEnabled(DirectPageScanIndexing.expectedDataPageScanEnabled)
            .setArgs(DirectPageScanIndexing.expectedDataPageScanEnabled)
        ).getAll().get(0).get(0));

        checkSqlLastFindDataPageScan(dataPageScanEnabled);
    }

    private void checkSqlLastFindDataPageScan(Boolean dataPageScanEnabled) {
        if (dataPageScanEnabled == FALSE)
            assertNull(CacheDataTree.isLastFindWithDirectDataPageScan()); // HashIdx was not used.
        else
            assertTrue(CacheDataTree.isLastFindWithDirectDataPageScan());
    }

    private void doTestSqlQuery(IgniteCache<Long,TestData> cache) {
        // SQL query (data page scan must be enabled by default).
        DirectPageScanIndexing.callsCnt.set(0);
        int callsCnt = 0;

        checkSqlQuery(cache, null);
        assertEquals(++callsCnt, DirectPageScanIndexing.callsCnt.get());

        checkSqlQuery(cache, true);
        assertEquals(++callsCnt, DirectPageScanIndexing.callsCnt.get());

        checkSqlQuery(cache, false);
        assertEquals(++callsCnt, DirectPageScanIndexing.callsCnt.get());

        checkSqlQuery(cache, null);
        assertEquals(++callsCnt, DirectPageScanIndexing.callsCnt.get());
    }

    private void checkSqlQuery(IgniteCache<Long,TestData> cache, Boolean dataPageScanEnabled) {
        DirectPageScanIndexing.expectedDataPageScanEnabled = dataPageScanEnabled;

        assertTrue(cache.query(new SqlQuery<>(TestData.class,
            "from TestData use index() where check_scan_flag(?,false)") // Force full scan with USE INDEX()
            .setArgs(DirectPageScanIndexing.expectedDataPageScanEnabled)
            .setDataPageScanEnabled(DirectPageScanIndexing.expectedDataPageScanEnabled))
            .getAll().isEmpty());

        checkSqlLastFindDataPageScan(dataPageScanEnabled);
    }

    private void doTestScanQuery(IgniteCache<Long,TestData> cache, int keysCnt) {
        // Scan query (data page scan must be disabled by default).
        TestPredicate.callsCnt.set(0);
        int callsCnt = 0;

        assertTrue(cache.query(new ScanQuery<>(new TestPredicate())).getAll().isEmpty());
        assertFalse(CacheDataTree.isLastFindWithDirectDataPageScan());
        assertEquals(callsCnt += keysCnt, TestPredicate.callsCnt.get());

        checkScanQuery(cache, true, true);
        assertEquals(callsCnt += keysCnt, TestPredicate.callsCnt.get());

        checkScanQuery(cache, false, false);
        assertEquals(callsCnt += keysCnt, TestPredicate.callsCnt.get());

        checkScanQuery(cache, true, true);
        assertEquals(callsCnt += keysCnt, TestPredicate.callsCnt.get());

        checkScanQuery(cache, null, false);
        assertEquals(callsCnt += keysCnt, TestPredicate.callsCnt.get());
    }

    private void checkScanQuery(IgniteCache<Long,TestData> cache, Boolean dataPageScanEnabled, Boolean expLastDataPageScan) {
        assertTrue(cache.query(new ScanQuery<>(new TestPredicate())
            .setDataPageScanEnabled(dataPageScanEnabled)).getAll().isEmpty());
        assertEquals(expLastDataPageScan, CacheDataTree.isLastFindWithDirectDataPageScan());
    }

    /**
     * @param exp Expected flag value.
     * @param res Result to return.
     * @return The given result..
     */
    @QuerySqlFunction(alias = "check_scan_flag")
    public static boolean checkScanFlagFromSql(Boolean exp, boolean res) {
        assertEquals(exp != FALSE, CacheDataTree.isDataPageScanEnabled());

        return res;
    }

    /**
     */
    static class DirectPageScanIndexing extends IgniteH2Indexing {
        /** */
        static volatile Boolean expectedDataPageScanEnabled;

        /** */
        static final AtomicInteger callsCnt = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public ResultSet executeSqlQueryWithTimer(
            PreparedStatement stmt,
            Connection conn,
            String sql,
            @Nullable Collection<Object> params,
            int timeoutMillis,
            @Nullable GridQueryCancel cancel,
            Boolean dataPageScanEnabled
        ) throws IgniteCheckedException {
            callsCnt.incrementAndGet();
            assertEquals(expectedDataPageScanEnabled, dataPageScanEnabled);

            return super.executeSqlQueryWithTimer(stmt, conn, sql, params, timeoutMillis,
                cancel, dataPageScanEnabled);
        }
    }

    /**
     */
    static class TestPredicate implements IgniteBiPredicate<Long,TestData> {
        /** */
        static final AtomicInteger callsCnt = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public boolean apply(Long k, TestData v) {
            callsCnt.incrementAndGet();
            return false;
        }
    }

    /**
     */
    static class TestData implements Serializable {
        /** */
        static final long serialVersionUID = 42L;

        /** */
        @QuerySqlField
        long z;

        /**
         */
        TestData(long z) {
            this.z = z;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestData testData = (TestData)o;

            return z == testData.z;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return (int)(z ^ (z >>> 32));
        }
    }
}
