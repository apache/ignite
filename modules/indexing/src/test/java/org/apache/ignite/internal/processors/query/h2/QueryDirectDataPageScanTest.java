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
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
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
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.lang.Boolean.FALSE;

/**
 */
@RunWith(JUnit4.class)
public class QueryDirectDataPageScanTest extends GridCommonAbstractTest {
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
    public void testMultipleIndexedTypes() throws Exception {
        final String cacheName = "test_multi_type";

        IgniteEx server = startGrid(0);
        server.cluster().active(true);

        CacheConfiguration<Object,Object> ccfg = new CacheConfiguration<>(cacheName);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 1));
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setIndexedTypes(
            Integer.class, Integer.class,
            Long.class, String.class,
            Long.class, TestData.class
        );

        IgniteCache<Object,Object> cache = server.createCache(ccfg);

        cache.put(1L, "bla-bla");
        cache.put(2L, new TestData(777L));
        cache.put(3, 3);

        CacheDataTree.isLastFindWithDirectDataPageScan();

        List<List<?>> res = cache.query(new SqlFieldsQuery("select z, _key, _val from TestData use index()")
            .setDataPageScanEnabled(true)).getAll();
        assertEquals(1, res.size());
        assertEquals(777L, res.get(0).get(0));
        assertTrue(CacheDataTree.isLastFindWithDirectDataPageScan());

        res = cache.query(new SqlFieldsQuery("select _val, _key from String use index()")
            .setDataPageScanEnabled(true)).getAll();
        assertEquals(1, res.size());
        assertEquals("bla-bla", res.get(0).get(0));
        assertTrue(CacheDataTree.isLastFindWithDirectDataPageScan());

        res = cache.query(new SqlFieldsQuery("select _key, _val from Integer use index()")
            .setDataPageScanEnabled(true)).getAll();
        assertEquals(1, res.size());
        assertEquals(3, res.get(0).get(0));
        assertTrue(CacheDataTree.isLastFindWithDirectDataPageScan());
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore
    @Test
    public void testConcurrentUpdatesWithMvcc() throws Exception {
        doTestConcurrentUpdates(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentUpdatesNoMvcc() throws Exception {
        try {
            doTestConcurrentUpdates(false);

            throw new IllegalStateException("Expected to detect data inconsistency.");
        } catch (AssertionError e) {
            assertTrue(e.getMessage().startsWith("wrong sum!"));
        }
    }

    private void doTestConcurrentUpdates(boolean enableMvcc) throws Exception {
        final String cacheName = "test_updates";

        IgniteEx server = startGrid(0);
        server.cluster().active(true);

        CacheConfiguration<Long,Long> ccfg = new CacheConfiguration<>(cacheName);
        ccfg.setIndexedTypes(Long.class, Long.class);
        ccfg.setAtomicityMode(enableMvcc ?
            CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT :
            CacheAtomicityMode.TRANSACTIONAL);

        IgniteCache<Long,Long> cache = server.createCache(ccfg);

        long accounts = 100;
        long initialBalance = 100;

        for (long i = 0; i < accounts; i++)
            cache.put(i, initialBalance);

        assertEquals(accounts * initialBalance,((Number)
            cache.query(new SqlFieldsQuery("select sum(_val) from Long use index()")
            .setDataPageScanEnabled(true)).getAll().get(0).get(0)).longValue());
        assertTrue(CacheDataTree.isLastFindWithDirectDataPageScan());

        AtomicBoolean cancel = new AtomicBoolean();

        IgniteInternalFuture<?> updFut = multithreadedAsync(() -> {
            Random rnd = ThreadLocalRandom.current();

            while (!cancel.get() && !Thread.interrupted()) {
                long accountId1 = rnd.nextInt((int)accounts);
                long accountId2 = rnd.nextInt((int)accounts);

                if (accountId1 == accountId2)
                    continue;

                try (
                    Transaction tx = server.transactions().txStart()
                ) {
                    long balance1 = cache.get(accountId1);
                    long balance2 = cache.get(accountId2);

                    if (balance1 == 0) {
                        if (balance2 == 0)
                            continue;

                        long transfer = rnd.nextInt((int)balance2);

                        if (transfer == 0)
                            transfer = balance2;

                        cache.put(accountId1, balance1 + transfer);
                        cache.put(accountId2, balance2 - transfer);
                    }
                    else {
                        long transfer = rnd.nextInt((int)balance1);

                        if (transfer == 0)
                            transfer = balance1;

                        cache.put(accountId1, balance1 - transfer);
                        cache.put(accountId2, balance2 + transfer);
                    }

                    tx.commit();
                }
                catch (CacheException e) {
                    if (!e.getMessage().contains(
                        "Cannot serialize transaction due to write conflict (transaction is marked for rollback)"))
                        throw new IllegalStateException(e);
//                    else
//                        U.warn(log, "Failed to commit TX, will ignore!");
                }
            }
        }, 16, "updater");

        IgniteInternalFuture<?> qryFut = multithreadedAsync(() -> {
            while (!cancel.get() && !Thread.interrupted()) {
                assertEquals("wrong sum!", accounts * initialBalance, ((Number)
                    cache.query(new SqlFieldsQuery("select sum(_val) from Long use index()")
                        .setDataPageScanEnabled(true)).getAll().get(0).get(0)).longValue());
//                info("query ok!");
            }
        }, 2, "query");

        qryFut.listen((f) -> cancel.set(true));
        updFut.listen((f) -> cancel.set(true));

        long start = U.currentTimeMillis();

        while (!cancel.get() && U.currentTimeMillis() - start < 30_000)
            doSleep(100);

        cancel.set(true);

        qryFut.get(3000);
        updFut.get(1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDirectDataPageScan() throws Exception {
        final String cacheName = "test";

        GridQueryProcessor.idxCls = DirectPageScanIndexing.class;
        IgniteEx server = startGrid(0);
        server.cluster().active(true);

        Ignition.setClientMode(true);
        IgniteEx client = startGrid(1);

        CacheConfiguration<Long,TestData> ccfg = new CacheConfiguration<>(cacheName);
        ccfg.setIndexedTypes(Long.class, TestData.class);
        ccfg.setSqlFunctionClasses(QueryDirectDataPageScanTest.class);

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

        doTestLazySql(clientCache, keysCnt);
        doTestLazySql(serverCache, keysCnt);
    }

    private void doTestLazySql(IgniteCache<Long,TestData> cache, int keysCnt) {
        checkLazySql(cache, false, keysCnt);
        checkLazySql(cache, true, keysCnt);
        checkLazySql(cache, null, keysCnt);
    }

    private void checkLazySql(IgniteCache<Long,TestData> cache, Boolean dataPageScanEnabled, int keysCnt) {
        CacheDataTree.isLastFindWithDirectDataPageScan();

        DirectPageScanIndexing.expectedDataPageScanEnabled = dataPageScanEnabled;

        final int expNestedLoops = 5;

        try (
            FieldsQueryCursor<List<?>> cursor = cache.query(
                new SqlFieldsQuery(
                    "select 1 " +
                        "from TestData a use index(), TestData b use index() " +
                        "where a.z between ? and ? " +
                        "and check_scan_flag(?,true)")
                    .setLazy(true)
                    .setDataPageScanEnabled(DirectPageScanIndexing.expectedDataPageScanEnabled)
                    .setArgs(1, expNestedLoops, DirectPageScanIndexing.expectedDataPageScanEnabled)
                    .setPageSize(keysCnt / 2) // Must be less than keysCnt.
            )
        ) {
            int nestedLoops = 0;
            int rowCnt = 0;

            for (List<?> row : cursor) {
                if (dataPageScanEnabled == FALSE)
                    assertNull(CacheDataTree.isLastFindWithDirectDataPageScan()); // HashIndex was never used.
                else {
                    Boolean x = CacheDataTree.isLastFindWithDirectDataPageScan();

                    if (x != null) {
                        assertTrue(x);
                        nestedLoops++;
                    }
                }

                rowCnt++;
            }

            assertEquals(keysCnt * expNestedLoops, rowCnt);
            assertEquals(dataPageScanEnabled == FALSE ? 0 : expNestedLoops, nestedLoops);
        }
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
