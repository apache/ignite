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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
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
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.Ignore;
import org.junit.Test;

import static java.lang.Boolean.FALSE;

/**
 */
public class QueryDataPageScanTest extends GridCommonAbstractTest {
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
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11998")
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
        ccfg.setQueryEntities(
            Arrays.asList(
                new QueryEntity()
                    .setValueType(UUID.class.getName())
                    .setKeyType(Integer.class.getName())
                    .setTableName("Uuids"),
                new QueryEntity()
                    .setValueType(Person.class.getName())
                    .setKeyType(Integer.class.getName())
                    .setTableName("My_Persons")
                    .setFields(Person.getFields())
            )
        );

        IgniteCache<Object,Object> cache = server.createCache(ccfg);

        cache.put(1L, "bla-bla");
        cache.put(2L, new TestData(777L));
        cache.put(3, 3);
        cache.put(7, UUID.randomUUID());
        cache.put(9, new Person("Vasya", 99));

        CacheDataTree.isLastFindWithDataPageScan();

        List<List<?>> res = cache.query(new SqlFieldsQuery("select z, _key, _val from TestData use index()")).getAll();
        assertEquals(1, res.size());
        assertEquals(777L, res.get(0).get(0));
        assertTrue(CacheDataTree.isLastFindWithDataPageScan());

        res = cache.query(new SqlFieldsQuery("select _val, _key from String use index()")).getAll();
        assertEquals(1, res.size());
        assertEquals("bla-bla", res.get(0).get(0));
        assertTrue(CacheDataTree.isLastFindWithDataPageScan());

        res = cache.query(new SqlFieldsQuery("select _key, _val from Integer use index()")).getAll();
        assertEquals(1, res.size());
        assertEquals(3, res.get(0).get(0));
        assertTrue(CacheDataTree.isLastFindWithDataPageScan());

        res = cache.query(new SqlFieldsQuery("select _key, _val from uuids use index()")).getAll();
        assertEquals(1, res.size());
        assertEquals(7, res.get(0).get(0));
        assertTrue(CacheDataTree.isLastFindWithDataPageScan());

        res = cache.query(new SqlFieldsQuery("select age, name from my_persons use index()")).getAll();
        assertEquals(1, res.size());
        assertEquals(99, res.get(0).get(0));
        assertEquals("Vasya", res.get(0).get(1));
        assertTrue(CacheDataTree.isLastFindWithDataPageScan());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11998")
    public void testConcurrentUpdatesWithMvcc() throws Exception {
        doTestConcurrentUpdates(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11998")
    public void testConcurrentUpdatesNoMvcc() throws Exception {
        try {
            doTestConcurrentUpdates(false);

            throw new IllegalStateException("Expected to detect data inconsistency.");
        }
        catch (AssertionError e) {
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
                ).getAll().get(0).get(0)).longValue());
        assertTrue(CacheDataTree.isLastFindWithDataPageScan());

        AtomicBoolean cancel = new AtomicBoolean();

        IgniteInternalFuture<?> updFut = multithreadedAsync(() -> {
            Random rnd = ThreadLocalRandom.current();

            while (!cancel.get() && !Thread.interrupted()) {
                long accountId1 = rnd.nextInt((int)accounts);
                long accountId2 = rnd.nextInt((int)accounts);

                if (accountId1 == accountId2)
                    continue;

                // Sort to avoid MVCC deadlock.
                if (accountId1 > accountId2) {
                    long tmp = accountId1;
                    accountId1 = accountId2;
                    accountId2 = tmp;
                }

                try (
                    Transaction tx = server.transactions().txStart()
                ) {
                    long balance1 = cache.get(accountId1);
                    long balance2 = cache.get(accountId2);

                    if (balance1 <= balance2) {
                        if (balance2 == 0)
                            continue; // balance1 is 0 as well here

                        long transfer = rnd.nextInt((int)balance2);

                        if (transfer == 0)
                            transfer = balance2;

                        balance1 += transfer;
                        balance2 -= transfer;
                    }
                    else {
                        long transfer = rnd.nextInt((int)balance1);

                        if (transfer == 0)
                            transfer = balance1;

                        balance1 -= transfer;
                        balance2 += transfer;
                    }

                    cache.put(accountId1, balance1);
                    cache.put(accountId2, balance2);

                    tx.commit();
                }
                catch (CacheException e) {
                    MvccFeatureChecker.assertMvccWriteConflict(e);

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
                        ).getAll().get(0).get(0)).longValue());
//                info("query ok!");
            }
        }, 2, "query");

        qryFut.listen((f) -> cancel.set(true));
        updFut.listen((f) -> cancel.set(true));

        long start = U.currentTimeMillis();

        while (!cancel.get() && U.currentTimeMillis() - start < 15_000)
            doSleep(100);

        cancel.set(true);

        qryFut.get(3000);
        updFut.get(1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11998")
    public void testDataPageScan() throws Exception {
        final String cacheName = "test";

        GridQueryProcessor.idxCls = DirectPageScanIndexing.class;
        IgniteEx server = startGrid(0);
        server.cluster().active(true);

        IgniteEx client = startClientGrid(1);

        CacheConfiguration<Long,TestData> ccfg = new CacheConfiguration<>(cacheName);
        ccfg.setIndexedTypes(Long.class, TestData.class);
        ccfg.setSqlFunctionClasses(QueryDataPageScanTest.class);

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
        CacheDataTree.isLastFindWithDataPageScan();

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
                    .setArgs(1, expNestedLoops, DirectPageScanIndexing.expectedDataPageScanEnabled)
                    .setPageSize(keysCnt / 10) // Must be less than keysCnt.
            )
        ) {
            int nestedLoops = 0;
            int rowCnt = 0;

            for (List<?> row : cursor) {
                if (dataPageScanEnabled == FALSE)
                    assertNull(CacheDataTree.isLastFindWithDataPageScan()); // HashIndex was never used.
                else {
                    Boolean x = CacheDataTree.isLastFindWithDataPageScan();

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
            .setArgs(DirectPageScanIndexing.expectedDataPageScanEnabled)
        ).getAll().get(0).get(0));

        checkSqlLastFindDataPageScan(dataPageScanEnabled);
    }

    private void checkSqlLastFindDataPageScan(Boolean dataPageScanEnabled) {
        if (dataPageScanEnabled == FALSE)
            assertNull(CacheDataTree.isLastFindWithDataPageScan()); // HashIdx was not used.
        else
            assertTrue(CacheDataTree.isLastFindWithDataPageScan());
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
            )
            .getAll().isEmpty());

        checkSqlLastFindDataPageScan(dataPageScanEnabled);
    }

    private void doTestScanQuery(IgniteCache<Long,TestData> cache, int keysCnt) {
        // Scan query (data page scan must be disabled by default).
        TestPredicate.callsCnt.set(0);
        int callsCnt = 0;

        assertTrue(cache.query(new ScanQuery<>(new TestPredicate())).getAll().isEmpty());
        assertFalse(CacheDataTree.isLastFindWithDataPageScan());
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
        assertTrue(cache.query(new ScanQuery<>(new TestPredicate())).getAll().isEmpty());
        assertEquals(expLastDataPageScan, CacheDataTree.isLastFindWithDataPageScan());
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
            H2PooledConnection conn,
            String sql,
            int timeoutMillis,
            @Nullable GridQueryCancel cancel,
            Boolean dataPageScanEnabled,
            final H2QueryInfo qryInfo
        ) throws IgniteCheckedException {
            callsCnt.incrementAndGet();
            assertEquals(expectedDataPageScanEnabled, dataPageScanEnabled);

            return super.executeSqlQueryWithTimer(stmt, conn, sql, timeoutMillis,
                cancel, dataPageScanEnabled, qryInfo);
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

    /**
     * Externalizable class to make it non-binary.
     */
    static class Person implements Externalizable {
        String name;

        int age;

        public Person() {
            // No-op
        }

        Person(String name, int age) {
            this.name = Objects.requireNonNull(name);
            this.age = age;
        }

        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeUTF(name);
            out.writeInt(age);
        }

        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            name = in.readUTF();
            age = in.readInt();
        }

        static LinkedHashMap<String,String> getFields() {
            LinkedHashMap<String,String> m = new LinkedHashMap<>();

            m.put("age", "INT");
            m.put("name", "VARCHAR");

            return m;
        }
    }
}
