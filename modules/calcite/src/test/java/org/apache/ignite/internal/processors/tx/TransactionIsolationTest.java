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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/** */
@RunWith(Parameterized.class)
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_ALLOW_TX_AWARE_QUERIES, value = "true")
public class TransactionIsolationTest extends GridCommonAbstractTest {
    /** */
    public static final String USERS = "USERS";

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
    public String insert;

    /** */
    @Parameterized.Parameter(1)
    public String update;

    /** */
    @Parameterized.Parameter(2)
    public String delete;

    /** */
    private static IgniteEx srv;

    /** */
    private static IgniteEx cli;

    /** */
    private TransactionConcurrency txConcurrency = TransactionConcurrency.OPTIMISTIC;

    /** */
    private TransactionIsolation txIsolation = READ_COMMITTED;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "insert={0},update={1},delete={2}")
    public static Collection<?> parameters() {
        List<Object[]> params = new ArrayList<>();

        String[] apis = new String[] {CACHE, SQL};

        for (String insert : apis) {
            for (String update : apis) {
                for (String delete : apis) {
                    params.add(new Object[]{insert, update, delete});
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

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        srv = startGrids(3);
        cli = startClientGrid("client");

        LinkedHashMap<String, String> flds = new LinkedHashMap<>();

        flds.put("id", Integer.class.getName());
        flds.put("userId", Integer.class.getName());
        flds.put("fio", String.class.getName());

        cli.createCache(new CacheConfiguration<>()
            .setName(USERS)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setQueryEntities(Collections.singleton(new QueryEntity()
                .setTableName(USERS)
                .setKeyType(Integer.class.getName())
                .setValueType(User.class.getName())
                .setKeyFieldName("id")
                .setFields(flds)
                .setIndexes(Collections.singleton(new QueryIndex()
                    .setName("IDX_FIO_USERS")
                    .setFieldNames(Collections.singleton("fio"), true).setInlineSize(Character.BYTES * 20))))));

        cli.createCache(new CacheConfiguration<Integer, Integer>()
            .setName("TBL")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setQueryEntities(Collections.singleton(new QueryEntity()
                .setTableName("TBL")
                .setKeyType(Long.class.getName())
                .setValueType(Long.class.getName()))));

    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        insert(F.t(1, JOHN));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cli.cache(USERS).removeAll();
    }

    /** */
    @Test
    public void testIndexScan() {
        delete(1);

        assertEquals("Table must be empty", 0L, executeSql(cli, "SELECT COUNT(*) FROM USERS.USERS").get(0).get(0));

        for (int i = 0; i < 5; i++) {
            int start = i * 10;

            for (int j = 0; j < 5; j++) {
                int id = start + j + 1;

                log.info("id = " + id);

                insert(F.t(id, new User(id, "User" + j))); // Intentionally repeat FIO to make same indexed keys.
            }
        }

        assertEquals(25L, executeSql(cli, "SELECT COUNT(*) FROM USERS.USERS").get(0).get(0));

        try (Transaction tx = cli.transactions().txStart(txConcurrency, txIsolation, TX_TIMEOUT, TX_SIZE)) {
            for (int i = 0; i < 5; i++) {
                int start = i * 10 + 5;

                for (int j = 0; j < 5; j++) {
                    int id = start + j + 1;

                    log.info("id = " + id);

                    insert(F.t(id, new User(id, "User" + j))); // Intentionally repeat FIO to make same indexed keys.

                    long expTblSz = 25L + i * 5 + j + 1;

                    assertEquals(expTblSz, executeSql(cli, "SELECT COUNT(*) FROM USERS.USERS").get(0).get(0));

                    List<List<?>> rows = executeSql(cli, "SELECT fio FROM USERS.USERS ORDER BY fio");

                    assertEquals(expTblSz, rows.size());

                    ensureSorted(rows, true);
                }
            }

            for (int i = 0; i < 5; i++) {
                int start = i * 10;

                for (int j = 0; j < 5; j++) {
                    int id = start + j + 1;

                    log.info("id = " + id);

                    delete(id);

                    long expTblSz = 50L - (i * 5 + j + 1);

                    assertEquals(expTblSz, executeSql(cli, "SELECT COUNT(*) FROM USERS.USERS").get(0).get(0));

                    List<List<?>> rows = executeSql(cli, "SELECT fio FROM USERS.USERS ORDER BY fio DESC");

                    assertEquals(expTblSz, rows.size());

                    ensureSorted(rows, false);
                }
            }

            tx.commit();
        }

        assertEquals(25L, executeSql(srv, "SELECT COUNT(*) FROM USERS.USERS").get(0).get(0));
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
        insideRollbackedTx(() -> {
            assertNull(CACHE, select(4, CACHE));
            assertNull(SQL, select(4, SQL));

            insert(F.t(4, JOHN));

            assertEquals(CACHE, JOHN, select(4, CACHE));
            assertEquals(SQL, JOHN, select(4, SQL));
        });

        assertNull(CACHE, select(4, CACHE));
        assertNull(SQL, select(4, SQL));
    }

    /** */
    @Test
    public void testUpdate() {
        insideRollbackedTx(() -> {
            assertEquals(JOHN, select(1, CACHE));
            assertEquals(JOHN, select(1, SQL));

            update(F.t(1, SARAH));

            assertEquals(SARAH, select(1, CACHE));
            assertEquals(SARAH, select(1, SQL));

            update(F.t(1, KYLE));

            assertEquals(KYLE, select(1, CACHE));
            assertEquals(KYLE, select(1, SQL));
        });

        assertEquals(JOHN, select(1, CACHE));
        assertEquals(JOHN, select(1, SQL));
    }

    /** */
    @Test
    public void testDelete() {
        insideRollbackedTx(() -> {
            assertEquals(JOHN, select(1, CACHE));
            assertEquals(JOHN, select(1, SQL));

            delete(1);

            assertNull(select(1, CACHE));
            assertNull(select(1, SQL));
        });

        assertEquals(JOHN, select(1, CACHE));
        assertEquals(JOHN, select(1, SQL));
    }

    /** */
    @Test
    public void testVisibility() {
        executeSql(srv, "DELETE FROM TBL.TBL");

        IgniteCache<Long, Long> cache = cli.cache("TBL");

        assertEquals("Table must be empty", 0L, executeSql(srv, "SELECT COUNT(*) FROM TBL.TBL").get(0).get(0));

        long cnt = 100;

        LongStream.range(1, 1 + cnt).forEach(i -> {
            try (Transaction tx = srv.transactions().txStart(txConcurrency, txIsolation, TX_TIMEOUT, TX_SIZE)) {
                cache.put(i, i + 1);

                assertEquals("Must see transaction related data", (Long)(i + 1), cache.get(i));

                List<List<?>> sqlData = executeSql(srv, "SELECT COUNT(*) FROM TBL.TBL");

                assertEquals("Must count properly", i, sqlData.get(0).get(0));

                tx.commit();
            }
        });

        List<List<?>> sqlData = executeSql(srv, "SELECT COUNT(*) FROM TBL.TBL");

        assertEquals("Must see committed data", cnt, sqlData.get(0).get(0));
    }

    /** */
    private void insideRollbackedTx(RunnableX test) {
        try (Transaction tx = cli.transactions().txStart(txConcurrency, txIsolation, TX_TIMEOUT, TX_SIZE)) {
            test.run();

            tx.rollback();
        }
    }

    /** */
    private User select(Integer id, String api) {
        if (api.equals(CACHE))
            return (User)cli.cache(USERS).get(id);
        else if (api.equals(SQL)) {
            List<List<?>> res = executeSql(cli, "SELECT _VAL FROM USERS.USERS WHERE _KEY = ?", id);

            assertNotNull(res);

            return res.isEmpty() ? null : ((User)res.get(0).get(0));
        }

        fail("Unknown select: " + api);

        return null;
    }

    /** */
    private void insert(IgniteBiTuple<Integer, User> data) {
        if (insert.equals(CACHE))
            cli.cache(USERS).put(data.get1(), data.get2());
        else if (insert.equals(SQL)) {
            executeSql(cli, "INSERT INTO USERS.USERS(id, userid, fio) VALUES(?, ?, ?)",
                data.get1(),
                data.get2().userId,
                data.get2().fio);
        }
        else
            fail("Unknown insert: " + insert);
    }

    /** */
    private void update(IgniteBiTuple<Integer, User> data) {
        if (update.equals(CACHE))
            cli.cache(USERS).put(data.get1(), data.get2());
        else if (update.equals(SQL)) {
            executeSql(cli, "UPDATE USERS.USERS SET userid = ?, fio = ? WHERE id = ?",
                data.get2().userId,
                data.get2().fio,
                data.get1());
        }
        else
            fail("Unknown update: " + update);
    }

    /** */
    private void delete(int id) {
        if (delete.equals(CACHE))
            cli.cache(USERS).remove(id);
        else if (delete.equals(SQL))
            executeSql(cli, "DELETE FROM USERS.USERS WHERE id = ?", id);
        else
            fail("Unknown delete: " + delete);
    }

    /** */
    public static class User {
        /** */
        final int userId;

        /** */
        final String fio;

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
    }

    /** */
    public static List<List<?>> executeSql(IgniteEx node, String sqlText, Object... args) {
        String explain = "EXPLAIN PLAN FOR ";
        if (!sqlText.startsWith(explain)) {
            List<List<?>> res = executeSql(node, explain + sqlText);
            for (List<?> r : res) {
                r.forEach(System.out::println);
            }
        }

        return node.cache(F.first(node.cacheNames())).query(new SqlFieldsQuery(sqlText)
            .setArgs(args)
            .setTimeout(5, TimeUnit.SECONDS)
        ).getAll();
    }
}
