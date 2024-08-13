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

package org.apache.ignite.internal.processors.tx;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.thin.TcpIgniteClient;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.lang.String.format;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.junit.Assume.assumeFalse;

/** */
@RunWith(Parameterized.class)
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_ALLOW_TX_AWARE_QUERIES, value = "true")
public class TransactionIsolationTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE = "cache";

    /** */
    public static final String SQL = "sql";

    /** */
    public static final User JOHN = new User(1, "John Connor");

    /** */
    public static final User SARAH = new User(2, "Sarah Connor");

    /** */
    public static final User KYLE = new User(3, "Kyle Reese");

    /** */
    public static final int TX_TIMEOUT = 60_000;

    /** */
    public static final int TX_SIZE = 10;

    /** */
    @Parameterized.Parameter()
    public String modify;

    /** */
    @Parameterized.Parameter(1)
    public boolean thin;

    /** */
    @Parameterized.Parameter(2)
    public boolean partitionAwareness;

    /** */
    @Parameterized.Parameter(3)
    public CacheMode mode;

    /** */
    @Parameterized.Parameter(4)
    public int gridCnt;

    /** */
    @Parameterized.Parameter(5)
    public int backups;

    /** */
    private static IgniteEx srv;

    /** */
    private static IgniteEx cli;

    /** */
    private static IgniteClient thinCli;

    /** */
    private TransactionConcurrency txConcurrency = TransactionConcurrency.OPTIMISTIC;

    /** */
    private TransactionIsolation txIsolation = READ_COMMITTED;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "modify={0},thin={1},partitionAwareness={2},mode={3},gridCnt={4},backups={5}")
    public static Collection<?> parameters() {
        List<Object[]> params = new ArrayList<>();

        String[] apis = new String[] {CACHE, SQL};

        for (String modify : apis) {
            for (CacheMode mode : CacheMode.values()) {
                for (int gridCnt : new int[]{1, 3, 5}) {
                    int[] backups = gridCnt > 1
                        ? new int[]{1, gridCnt - 1}
                        : new int[]{0};

                    for (int backup: backups) {
                        params.add(new Object[]{modify, false, false, mode, gridCnt, backup});

                        for (boolean partitionAwareness : new boolean[]{false, true}) {
                            params.add(new Object[]{modify, true, partitionAwareness, mode, gridCnt, backup});
                        }
                    }
                }
            }
        }

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setSqlConfiguration(
            new SqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration()));
    }

    /** */
    private void init() throws Exception {
        srv = startGrids(gridCnt);
        cli = startClientGrid("client");
        thinCli = TcpIgniteClient.start(new ClientConfiguration()
            .setAddresses(Config.SERVER)
            .setPartitionAwarenessEnabled(partitionAwareness));

        for (CacheMode mode : CacheMode.values()) {
            String users = tableName("USERS", mode);
            String tbl = tableName("TBL", mode);

            LinkedHashMap<String, String> flds = new LinkedHashMap<>();

            flds.put("id", Integer.class.getName());
            flds.put("userId", Integer.class.getName());
            flds.put("fio", String.class.getName());

            cli.createCache(new CacheConfiguration<>()
                .setName(users)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(mode)
                .setBackups(backups)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setSqlSchema(QueryUtils.DFLT_SCHEMA)
                .setQueryEntities(Collections.singleton(new QueryEntity()
                    .setTableName(users)
                    .setKeyType(Integer.class.getName())
                    .setValueType(User.class.getName())
                    .setKeyFieldName("id")
                    .setFields(flds)
                    .setIndexes(Arrays.asList(
                        new QueryIndex()
                            .setName("IDX_FIO_" + users)
                            .setFieldNames(Collections.singleton("fio"), true).setInlineSize(Character.BYTES * 20),
                        new QueryIndex()
                            .setName("IDX_USERID_" + users)
                            .setFieldNames(Collections.singleton("userId"), true)
                    )))));

            cli.createCache(new CacheConfiguration<Integer, Integer>()
                .setName(tbl)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(backups)
                .setCacheMode(this.mode)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setSqlSchema(QueryUtils.DFLT_SCHEMA)
                .setQueryEntities(Collections.singleton(new QueryEntity()
                    .setTableName(tbl)
                    .setKeyType(Long.class.getName())
                    .setValueType(Long.class.getName()))));
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (F.isEmpty(Ignition.allGrids())) {
            init();
        }

        insert(F.t(1, JOHN));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cli.cache(users()).removeAll();
    }

    /** */
    @Test
    public void testIndexScan() {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-22874", thin);

        delete(1);

        assertEquals("Table must be empty", 0L, executeSql(format("SELECT COUNT(*) FROM %s", users())).get(0).get(0));

        for (int i = 0; i < 5; i++) {
            int start = i * 10;

            for (int j = 0; j < 5; j++) {
                int id = start + j + 1;

                insert(F.t(id, new User(id, "User" + j))); // Intentionally repeat FIO to make same indexed keys.
            }
        }

        assertEquals(25L, executeSql(format("SELECT COUNT(*) FROM %s", users())).get(0).get(0));

        insideTx(() -> {
            for (int i = 0; i < 5; i++) {
                int start = i * 10 + 5;

                assertEquals(i * 10 + 1, executeSql(format("SELECT MIN(userid) FROM %s WHERE userid > ?", users()), i * 10).get(0).get(0));

                for (int j = 0; j < 5; j++) {
                    int id = start + j + 1;

                    insert(F.t(id, new User(id, "User" + j))); // Intentionally repeat FIO to make same indexed keys.

                    long expTblSz = 25L + i * 5 + j + 1;

                    assertEquals(expTblSz, executeSql(format("SELECT COUNT(*) FROM %s", users())).get(0).get(0));

                    List<List<?>> rows = executeSql(format("SELECT fio FROM %s ORDER BY fio", users()));

                    assertEquals(expTblSz, rows.size());

                    ensureSorted(rows, true);

                    assertEquals(
                        id,
                        executeSql(format("SELECT MIN(userid) FROM %s WHERE userid BETWEEN ? AND ?", users()), id, 500).get(0).get(0)
                    );
                }
            }

            for (int i = 0; i < 5; i++) {
                int start = i * 10;

                for (int j = 0; j < 5; j++) {
                    int id = start + j + 1;

                    delete(id);

                    long expTblSz = 50L - (i * 5 + j + 1);

                    assertEquals(expTblSz, executeSql(format("SELECT COUNT(*) FROM %s", users())).get(0).get(0));

                    List<List<?>> rows = executeSql(format("SELECT fio FROM %s ORDER BY fio DESC", users()));

                    assertEquals(expTblSz, rows.size());

                    ensureSorted(rows, false);
                }
            }
        }, true);

        assertEquals(25L, executeSql(format("SELECT COUNT(*) FROM %s", users())).get(0).get(0));
    }

    /** */
    private static void ensureSorted(List<List<?>> rows, boolean asc) {
        for (int k = 1; k < rows.size(); k++) {
            String fio0 = (String)rows.get(k - 1).get(0);
            String fio1 = (String)rows.get(k).get(0);

            assertTrue(asc ? (fio0.compareTo(fio1) <= 0) : (fio0.compareTo(fio1) >= 0));
        }
    }

    /** */
    @Test
    public void testInsert() {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-22874", thin);

        Runnable checkDflts = () -> {
            assertNull(CACHE, select(4, CACHE));
            assertNull(SQL, select(4, SQL));
        };

        checkDflts.run();

        insideTx(() -> {
            checkDflts.run();

            insert(F.t(4, JOHN));

            assertEquals(CACHE, JOHN, select(4, CACHE));
            assertEquals(SQL, JOHN, select(4, SQL));
        }, false);

        checkDflts.run();
    }

    /** */
    @Test
    public void testUpdate() {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-22874", thin);

        Runnable checkDflts = () -> {
            assertEquals(JOHN, select(1, CACHE));
            assertEquals(JOHN, select(1, SQL));
        };

        checkDflts.run();

        insideTx(() -> {
            checkDflts.run();

            update(F.t(1, SARAH));

            assertEquals(SARAH, select(1, CACHE));
            assertEquals(SARAH, select(1, SQL));

            update(F.t(1, KYLE));

            assertEquals(KYLE, select(1, CACHE));
            assertEquals(KYLE, select(1, SQL));
        }, false);

        checkDflts.run();
    }

    /** */
    @Test
    public void testDelete() {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-22874", thin);

        Runnable checkDflts = () -> {
            assertEquals(JOHN, select(1, CACHE));
            assertEquals(JOHN, select(1, SQL));
        };

        checkDflts.run();

        insideTx(() -> {
            checkDflts.run();

            delete(1);

            assertNull(select(1, CACHE));
            assertNull(select(1, SQL));
        }, false);

        checkDflts.run();
    }

    /** */
    @Test
    public void testVisibility() {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-22874", thin);

        executeSql(format("DELETE FROM %s", tbl()));

        IgniteCache<Long, Long> cache = cli.cache(tbl());
        ClientCache<Long, Long> thinCache = thinCli.cache(tbl());

        assertEquals("Table must be empty", 0L, executeSql(format("SELECT COUNT(*) FROM %s", tbl())).get(0).get(0));

        long cnt = 100;

        LongStream.range(1, 1 + cnt).forEach(i -> insideTx(() -> {
            if (thin) {
                thinCache.put(i, i + 1);

                assertEquals("Must see transaction related data", (Long)(i + 1), thinCache.get(i));
            }
            else {
                cache.put(i, i + 1);

                assertEquals("Must see transaction related data", (Long)(i + 1), cache.get(i));
            }

            List<List<?>> sqlData = executeSql(format("SELECT COUNT(*) FROM %s", tbl()));

            assertEquals("Must count properly", i, sqlData.get(0).get(0));
        }, true));

        List<List<?>> sqlData = executeSql(format("SELECT COUNT(*) FROM %s", tbl()));

        assertEquals("Must see committed data", cnt, sqlData.get(0).get(0));
    }

    /** */
    private void insideTx(RunnableX test, boolean commit) {
        if (thin) {
            try (ClientTransaction tx = thinCli.transactions().txStart(txConcurrency, txIsolation, TX_TIMEOUT)) {
                test.run();

                if (commit)
                    tx.commit();
                else
                    tx.rollback();
            }

        }
        else {
            try (Transaction tx = cli.transactions().txStart(txConcurrency, txIsolation, TX_TIMEOUT, TX_SIZE)) {
                test.run();

                if (commit)
                    tx.commit();
                else
                    tx.rollback();
            }
        }
    }

    /** */
    private User select(Integer id, String api) {
        if (api.equals(CACHE))
            return thin
                ? (User)thinCli.cache(users()).get(id)
                : (User)cli.cache(users()).get(id);
        else if (api.equals(SQL)) {
            List<List<?>> res = executeSql(format("SELECT _VAL FROM %s WHERE _KEY = ?", users()), id);

            assertNotNull(res);

            return res.isEmpty() ? null : ((User)res.get(0).get(0));
        }

        fail("Unknown select: " + api);

        return null;
    }

    /** */
    private void insert(IgniteBiTuple<Integer, User> data) {
        if (modify.equals(CACHE)) {
            if (thin)
                thinCli.cache(users()).put(data.get1(), data.get2());
            else
                cli.cache(users()).put(data.get1(), data.get2());
        }
        else if (modify.equals(SQL)) {
            executeSql(format("INSERT INTO %s(id, userid, fio) VALUES(?, ?, ?)", users()),
                data.get1(),
                data.get2().userId,
                data.get2().fio);
        }
        else
            fail("Unknown insert: " + modify);
    }

    /** */
    private void update(IgniteBiTuple<Integer, User> data) {
        if (modify.equals(CACHE)) {
            if (thin)
                thinCli.cache(users()).put(data.get1(), data.get2());
            else
                cli.cache(users()).put(data.get1(), data.get2());
        }
        else if (modify.equals(SQL)) {
            executeSql(format("UPDATE %s SET userid = ?, fio = ? WHERE id = ?", users()),
                data.get2().userId,
                data.get2().fio,
                data.get1());
        }
        else
            fail("Unknown update: " + modify);
    }

    /** */
    private void delete(int id) {
        if (modify.equals(CACHE))
            cli.cache(users()).remove(id);
        else if (modify.equals(SQL))
            executeSql(format("DELETE FROM %s WHERE id = ?", users()), id);
        else
            fail("Unknown delete: " + modify);
    }

    /** */
    public static class User {
        /** */
        private final int userId;

        /** */
        private final String fio;

        /** */
        public User(int id, String fio) {
            this.userId = id;
            this.fio = fio;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            User user = (User)o;
            return userId == user.userId && Objects.equals(fio, user.fio);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(userId, fio);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(User.class, this);
        }
    }

    /** */
    public List<List<?>> executeSql(String sqlText, Object... args) {
        String explain = "EXPLAIN PLAN FOR ";

        if (!sqlText.startsWith(explain)) {
            List<List<?>> res = executeSql(explain + sqlText);
            for (List<?> r : res)
                r.forEach(System.out::println);
        }

        SqlFieldsQuery qry = new SqlFieldsQuery(sqlText)
            .setArgs(args)
            .setTimeout(5, TimeUnit.SECONDS);

        return thin
            ? thinCli.query(qry).getAll()
            : cli.cache(F.first(cli.cacheNames())).query(qry).getAll();
    }

    /** */
    private String users() {
        return tableName("USERS", mode);
    }

    /** */
    private String tbl() {
        return tableName("TBL", mode);
    }

    /** */
    private static String tableName(String tbl, CacheMode mode) {
        return tbl + "_" + mode;
    }
}
