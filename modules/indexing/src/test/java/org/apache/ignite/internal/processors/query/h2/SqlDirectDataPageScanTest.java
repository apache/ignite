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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlQuery;
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

        CacheConfiguration<Long,Long> ccfg = new CacheConfiguration<>(cacheName);
        ccfg.setIndexedTypes(Long.class, Long.class);
        ccfg.setSqlFunctionClasses(SqlDirectDataPageScanTest.class);

        IgniteCache<Long,Long> clientCache = client.createCache(ccfg);

        final int keysCnt = 1000;

        for (long i = 0; i < keysCnt; i++)
            clientCache.put(i, i);

        IgniteCache<Long,Long> serverCache = server.cache(cacheName);

        doTestScanQuery(clientCache, keysCnt);
        doTestScanQuery(serverCache, keysCnt);

        doTestSqlQuery(clientCache);
        doTestSqlQuery(serverCache);
    }

    private void doTestSqlQuery(IgniteCache<Long,Long> cache) {
        // SQL query (data page scan must be enabled by default).
        DirectPageScanIndexing.callsCnt.set(0);
        int callsCnt = 0;

        DirectPageScanIndexing.expectedDataPageScanEnabled = null;
        assertTrue(cache.query(new SqlQuery<>(Long.class, "check_scan_flag(null)"))
            .getAll().isEmpty());
        assertEquals(++callsCnt, DirectPageScanIndexing.callsCnt.get());

        DirectPageScanIndexing.expectedDataPageScanEnabled = true;
        assertTrue(cache.query(new SqlQuery<>(Long.class, "check_scan_flag(?)")
            .setArgs(DirectPageScanIndexing.expectedDataPageScanEnabled)
            .setDataPageScanEnabled(DirectPageScanIndexing.expectedDataPageScanEnabled))
            .getAll().isEmpty());
        assertEquals(++callsCnt, DirectPageScanIndexing.callsCnt.get());

        DirectPageScanIndexing.expectedDataPageScanEnabled = null;
        assertTrue(cache.query(new SqlQuery<>(Long.class, "check_scan_flag(?)")
            .setArgs(DirectPageScanIndexing.expectedDataPageScanEnabled)
            .setDataPageScanEnabled(DirectPageScanIndexing.expectedDataPageScanEnabled))
            .getAll().isEmpty());
        assertEquals(++callsCnt, DirectPageScanIndexing.callsCnt.get());

        DirectPageScanIndexing.expectedDataPageScanEnabled = false;
        assertTrue(cache.query(new SqlQuery<>(Long.class, "check_scan_flag(?)")
            .setArgs(DirectPageScanIndexing.expectedDataPageScanEnabled)
            .setDataPageScanEnabled(DirectPageScanIndexing.expectedDataPageScanEnabled))
            .getAll().isEmpty());
        assertEquals(++callsCnt, DirectPageScanIndexing.callsCnt.get());
    }

    private void doTestScanQuery(IgniteCache<Long,Long> cache, int keysCnt) {
        // Scan query (data page scan must be disabled by default).
        TestPredicate.callsCnt.set(0);
        int callsCnt = 0;

        TestPredicate p = new TestPredicate();

        assertTrue(cache.query(new ScanQuery<>(p)).getAll().isEmpty());
        assertFalse(CacheDataTree.isLastFindWithDirectDataPageScan());
        assertEquals(callsCnt += keysCnt, TestPredicate.callsCnt.get());

        assertTrue(cache.query(new ScanQuery<>(p)
            .setDataPageScanEnabled(true)).getAll().isEmpty());
        assertTrue(CacheDataTree.isLastFindWithDirectDataPageScan());
        assertEquals(callsCnt += keysCnt, TestPredicate.callsCnt.get());

        assertTrue(cache.query(new ScanQuery<>(p)
            .setDataPageScanEnabled(false)).getAll().isEmpty());
        assertFalse(CacheDataTree.isLastFindWithDirectDataPageScan());
        assertEquals(callsCnt += keysCnt, TestPredicate.callsCnt.get());

        assertTrue(cache.query(new ScanQuery<>(p)
            .setDataPageScanEnabled(true)).getAll().isEmpty());
        assertTrue(CacheDataTree.isLastFindWithDirectDataPageScan());
        assertEquals(callsCnt += keysCnt, TestPredicate.callsCnt.get());

        assertTrue(cache.query(new ScanQuery<>(p)
            .setDataPageScanEnabled(null)).getAll().isEmpty());
        assertFalse(CacheDataTree.isLastFindWithDirectDataPageScan());
        assertEquals(callsCnt += keysCnt, TestPredicate.callsCnt.get());
    }

    /**
     * @param exp Expected flag value.
     * @return Always {@code false}.
     */
    @QuerySqlFunction(alias = "check_scan_flag")
    public static boolean checkScanFlag(Boolean exp) {
        assertEquals(exp != FALSE, CacheDataTree.isDataPageScanEnabled());

        return false;
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
    static class TestPredicate implements IgniteBiPredicate<Long,Long> {
        /** */
        static final AtomicInteger callsCnt = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public boolean apply(Long k, Long v) {
            callsCnt.incrementAndGet();
            return false;
        }
    }
}
