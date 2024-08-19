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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.ignite.Ignite;
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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.junit.Assume.assumeFalse;

/** */
@RunWith(Parameterized.class)
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_ALLOW_TX_AWARE_QUERIES, value = "true")
public class SqlTransactionsIsolationTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE = "cache";

    /** */
    public static final String SQL = "sql";

    /** */
    public static final String USERS = "USERS";

    /** */
    public static final String DEPARTMENTS = "DEPARTMENTS";

    /** */
    public static final String TBL = "TBL";

    /** */
    public enum ExecutorType {
        /** */
        SERVER,

        /** */
        CLIENT,

        /** */
        THIN
    }

    /** */
    public static final User JOHN = new User(1, 0, "John Connor");

    /** */
    public static final User SARAH = new User(2, 0, "Sarah Connor");

    /** */
    public static final User KYLE = new User(3, 0, "Kyle Reese");

    /** */
    public static final int TX_TIMEOUT = 60_000;

    /** */
    public static final int TX_SIZE = 10;

    /** */
    @Parameterized.Parameter()
    public String modify;

    /** */
    @Parameterized.Parameter(1)
    public ExecutorType type;

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
    @Parameterized.Parameter(6)
    public boolean commit;

    /** */
    @Parameterized.Parameter(7)
    public boolean multi;

    /** */
    private static IgniteEx srv;

    /** */
    private static IgniteEx cli;

    /** */
    private static IgniteClient thinCli;

    /** */
    private final TransactionConcurrency txConcurrency = TransactionConcurrency.OPTIMISTIC;

    /** */
    private final TransactionIsolation txIsolation = READ_COMMITTED;

    /** */
    private final Map<Integer, Set<Integer>> partsToKeys = new HashMap<>();

    /** @return Test parameters. */
    @Parameterized.Parameters(
        name = "modify={0},qryExecutor={1},partitionAwareness={2},mode={3},gridCnt={4},backups={5},commit={6},multi={7}")
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
                        for (boolean commit : new boolean[]{false, true}) {
                            for (boolean mutli : new boolean[] {false, true}) {
                                params.add(new Object[]{modify, ExecutorType.SERVER, false, mode, gridCnt, backup, commit, mutli});
                                params.add(new Object[]{modify, ExecutorType.CLIENT, false, mode, gridCnt, backup, commit, mutli});

/*
                                for (boolean partitionAwareness : new boolean[]{false, true}) {
                                    params.add(new Object[]{
                                        modify,
                                        ExecutorType.THIN,
                                        partitionAwareness,
                                        mode,
                                        gridCnt,
                                        backup,
                                        commit,
                                        mutli
                                    });
                                }
*/
                            }
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
            String users = tableName(USERS, mode);
            String deps = tableName(DEPARTMENTS, mode);
            String tbl = tableName(TBL, mode);

            LinkedHashMap<String, String> userFlds = new LinkedHashMap<>();

            userFlds.put("id", Integer.class.getName());
            userFlds.put("userId", Integer.class.getName());
            userFlds.put("departmentId", Integer.class.getName());
            userFlds.put("fio", String.class.getName());

            LinkedHashMap<String, String> depFlds = new LinkedHashMap<>();

            depFlds.put("id", Integer.class.getName());
            depFlds.put("name", String.class.getName());

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
                    .setFields(userFlds)
                    .setIndexes(Arrays.asList(
                        new QueryIndex()
                            .setName("IDX_FIO_" + users)
                            .setFieldNames(Collections.singleton("fio"), true).setInlineSize(Character.BYTES * 20),
                        new QueryIndex()
                            .setName("IDX_USERID_" + users)
                            .setFieldNames(Collections.singleton("userId"), true)
                    )))));

            cli.createCache(new CacheConfiguration<>()
                .setName(deps)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(mode)
                .setBackups(backups)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setSqlSchema(QueryUtils.DFLT_SCHEMA)
                .setQueryEntities(Collections.singleton(new QueryEntity()
                    .setTableName(deps)
                    .setKeyType(Integer.class.getName())
                    .setValueType(String.class.getName())
                    .setKeyFieldName("id")
                    .setFields(depFlds))));

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

        node().cache(users()).removeAll();
        node().cache(tbl()).removeAll();
        node().cache(departments()).removeAll();

        insert(F.t(1, JOHN));
    }

    /** */
    @Test
    public void testIndexScan() {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-22874", type == ExecutorType.THIN);

        delete(1);

        assertEquals("Table must be empty", 0L, sql(format("SELECT COUNT(*) FROM %s", users())).get(0).get(0));

        int stepCnt = 7;
        int stepSz = 12;
        int outOfTxSz = 3;
        int inTxSz = 9;

        assert outOfTxSz + inTxSz == stepSz;

        for (int i = 0; i < stepCnt; i++) {
            int outOfTxStart = i * stepSz;

            for (int j = 0; j < outOfTxSz; j++) {
                int id = outOfTxStart + j;

                insert(F.t(id, new User(id, 0, "User" + j))); // Intentionally repeat FIO to make same indexed keys.
            }
        }

        assertEquals((long)(stepCnt * outOfTxSz), sql(format("SELECT COUNT(*) FROM %s", users())).get(0).get(0));

        checkQueryWithPartitionFilter();

        insideTx(() -> {
            for (int i = 0; i < stepCnt; i++) {
                int outOfTxStart = i * stepSz;
                int inTxStart = i * stepSz + outOfTxSz;

                assertEquals(
                    outOfTxStart,
                    sql(format("SELECT MIN(userid) FROM %s WHERE userid >= ?", users()), outOfTxStart).get(0).get(0)
                );
                assertEquals(
                    outOfTxStart + 1,
                    sql(format("SELECT MIN(userid) FROM %s WHERE userid > ?", users()), outOfTxStart).get(0).get(0)
                );

                for (int j = 0; j < inTxSz; j++) {
                    final int id = inTxStart + j;

                    insert(F.t(id, new User(id, 0, "User" + j))); // Intentionally repeat FIO to make same indexed keys.

                    // Concurrent query must not see any transaction data.
                    runAsync(() -> {
                        RunnableX check = () -> {
                            assertEquals((long)(stepCnt * outOfTxSz), sql(format("SELECT COUNT(*) FROM %s", users())).get(0).get(0));

                            assertNull(select(id, CACHE));
                            assertNull(select(id, SQL));
                        };

                        insideTx(check, false);
                        check.run();
                    }).get(TX_TIMEOUT);

                    long expTblSz = (long)(stepCnt * outOfTxSz) + i * inTxSz + j + 1;

                    assertEquals(expTblSz, sql(format("SELECT COUNT(*) FROM %s", users())).get(0).get(0));

                    List<List<?>> rows = sql(format("SELECT fio FROM %s ORDER BY fio", users()));

                    assertEquals(expTblSz, rows.size());

                    ensureSorted(rows, true);

                    assertEquals(
                        id,
                        sql(format("SELECT MIN(userid) FROM %s WHERE userid BETWEEN ? AND ?", users()), id, 500).get(0).get(0)
                    );
                }

                checkQueryWithPartitionFilter();

                List<List<?>> res = sql(
                    format("SELECT id FROM %s WHERE userId >= ? AND userId <= ? ORDER BY id LIMIT ?", users()),
                    i * stepSz,
                    (i + 1) * stepSz,
                    stepSz
                );

                assertEquals(stepSz, res.size());

                for (int j = 0; j < stepSz; j++)
                    assertEquals(outOfTxStart + j, res.get(j).get(0));

                int offset = 3;

                res = sql(
                    format("SELECT id FROM %s WHERE userId >= ? AND userId <= ? ORDER BY id LIMIT ? OFFSET ?", users()),
                    i * stepSz,
                    (i + 1) * stepSz,
                    stepSz - offset,
                    offset
                );

                assertEquals(stepSz - offset, res.size());

                for (int j = 0; j < stepSz - offset; j++)
                    assertEquals(outOfTxStart + j + offset, res.get(j).get(0));
            }

            for (int i = 0; i < stepCnt; i++) {
                int start = i * stepSz;

                for (int j = 0; j < outOfTxSz; j++) {
                    int id = start + j;

                    delete(id);

                    long expTblSz = (stepCnt * stepSz) - (i * outOfTxSz + j + 1);

                    assertEquals(expTblSz, sql(format("SELECT COUNT(*) FROM %s", users())).get(0).get(0));

                    List<List<?>> rows = sql(format("SELECT fio FROM %s ORDER BY fio DESC", users()));

                    assertEquals(expTblSz, rows.size());

                    ensureSorted(rows, false);
                }
            }

            checkQueryWithPartitionFilter();
        }, true);

        assertEquals((long)inTxSz * stepCnt, sql(format("SELECT COUNT(*) FROM %s", users())).get(0).get(0));
    }

    /** */
    @Test
    public void testJoin() {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-22874", type == ExecutorType.THIN);

        partsToKeys.clear();

        sql(format("DELETE FROM %s", users()));
        sql(format("DELETE FROM %s", departments()));

        assertEquals("Table must be empty", 0L, sql(format("SELECT COUNT(*) FROM %s", users())).get(0).get(0));
        assertEquals("Table must be empty", 0L, sql(format("SELECT COUNT(*) FROM %s", departments())).get(0).get(0));

        int depCnt = 5;
        int userCnt = 5;

        Map<Integer, List<User>> data = new HashMap<>();

        IntFunction<String> uname = i -> "User " + i + 1;

        IntConsumer insertDepartment = i -> {
            data.put(i + 1, new ArrayList<>());

            sql(format("INSERT INTO %s(ID, NAME) VALUES(?, ?)", departments()), i + 1, (i + 1) + " department");
        };

        BiConsumer<Integer, Integer> insertUser = (id, depId) -> {
            User user = new User(id, depId, uname.apply(depId));

            insert(F.t(id, user));

            data.get(depId).add(user);
        };

        for (int i = 0; i < depCnt; i++)
            insertDepartment.accept(i);

        for (int i = 0; i < depCnt; i++) {
            for (int j = 0; j < userCnt; j++)
                insertUser.accept(i * depCnt + j, j + 1);
        }

        insertDepartment.accept(depCnt);

        checkJoin(data, uname);

        insideTx(() -> {
            for (int i = depCnt + 1; i < depCnt * 2; i++)
                insertDepartment.accept(i);

            for (int i = 0; i < depCnt * 2; i++) {
                for (int j = 0; j < userCnt; j++) {
                    if (i >= depCnt)
                        insertUser.accept(i * depCnt + j, i + 1);

                    checkJoin(data, uname);
                }
            }

        }, false);
    }

    /** */
    private void checkJoin(Map<Integer, List<User>> data, IntFunction<String> uname) {
        List<List<?>> res = sql(
            format("SELECT d.id, u.id, u.fio FROM %s d JOIN %s u ON d.id = u.departmentId", departments(), users())
        );

        Map<Integer, List<User>> data0 = new HashMap<>();

        data.forEach((key, value) -> data0.put(key, new ArrayList<>(value)));

        for (List<?> row : res) {
            List<User> users = data0.get(row.get(0));

            assertNotNull(users);

            assertTrue(users.removeIf(u -> {
                if (u.userId != (Integer)row.get(1))
                    return false;

                assertEquals(u.fio, row.get(2));

                return true;
            }));
        }

        data0.values().forEach(l -> assertTrue(l.isEmpty()));

        res = sql(
            format("SELECT d.id, COUNT(*) FROM %s d JOIN %s u ON d.id = u.departmentId GROUP BY d.id", departments(), users())
        );

        assertEquals((int)data.values().stream().filter(((Predicate<List<?>>)List::isEmpty).negate()).count(), res.size());

        for (List<?> row : res)
            assertEquals((long)data.get(row.get(0)).size(), row.get(1));

        res = sql(
            format("SELECT d.id, COUNT(*) FROM %s d LEFT JOIN %s u ON d.id = u.departmentId GROUP BY d.id", departments(), users())
        );

        assertEquals(data.size(), res.size());

        for (List<?> row : res) {
            long size = data.get(row.get(0)).size();

            assertEquals(size == 0 ? 1 : size, row.get(1));
        }

        res = sql(
            format("SELECT d.id, u.fio FROM %s d JOIN %s u ON d.id = u.departmentId", departments(), users())
        );

        assertEquals(data.values().stream().mapToInt(List::size).sum(), res.size());

        for (List<?> row : res)
            assertEquals(uname.apply((Integer)row.get(0)), row.get(1));
    }

    /** */
    @Test
    public void testInsert() {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-22874", type == ExecutorType.THIN);

        Runnable checkBefore = () -> {
            for (int i = 4; i <= (multi ? 6 : 4); i++) {
                assertNull(CACHE, select(i, CACHE));
                assertNull(SQL, select(i, SQL));
            }
        };

        Runnable checkAfter = () -> {
            for (int i = 4; i <= (multi ? 6 : 4); i++) {
                assertEquals(CACHE, JOHN, select(i, CACHE));
                assertEquals(SQL, JOHN, select(i, SQL));
            }
        };

        checkBefore.run();

        insideTx(() -> {
            checkBefore.run();

            if (multi)
                insert(F.t(4, JOHN), F.t(5, JOHN), F.t(6, JOHN));
            else
                insert(F.t(4, JOHN));

            checkAfter.run();
        }, commit);

        if (commit)
            checkAfter.run();
        else
            checkBefore.run();
    }

    /** */
    @Test
    public void testUpdate() {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-22874", type == ExecutorType.THIN);

        if (multi)
            insert(F.t(2, JOHN), F.t(3, JOHN));

        Runnable checkBefore = () -> {
            for (int i = 1; i <= (multi ? 3 : 1); i++) {
                assertEquals(JOHN, select(i, CACHE));
                assertEquals(JOHN, select(i, SQL));
            }
        };

        Runnable checkAfter = () -> {
            for (int i = 1; i <= (multi ? 3 : 1); i++) {
                assertEquals(KYLE, select(i, CACHE));
                assertEquals(KYLE, select(i, SQL));
            }
        };

        checkBefore.run();

        insideTx(() -> {
            checkBefore.run();

            if (multi)
                update(F.t(1, SARAH), F.t(2, SARAH), F.t(3, SARAH));
            else
                update(F.t(1, SARAH));

            for (int i = 1; i <= (multi ? 3 : 1); i++) {
                assertEquals(SARAH, select(i, CACHE));
                assertEquals(SARAH, select(i, SQL));
            }

            if (multi)
                update(F.t(1, KYLE), F.t(2, KYLE), F.t(3, KYLE));
            else
                update(F.t(1, KYLE));

            checkAfter.run();
        }, commit);

        if (commit)
            checkAfter.run();
        else
            checkBefore.run();
    }

    /** */
    @Test
    public void testDelete() {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-22874", type == ExecutorType.THIN);

        if (multi)
            insert(F.t(2, JOHN), F.t(3, JOHN));

        Runnable checkBefore = () -> {
            for (int i = 1; i <= (multi ? 3 : 1); i++) {
                assertEquals(JOHN, select(i, CACHE));
                assertEquals(JOHN, select(i, SQL));
            }
        };

        Runnable checkAfter = () -> {
            for (int i = 1; i <= (multi ? 3 : 1); i++) {
                assertNull(select(i, CACHE));
                assertNull(select(i, SQL));
            }
        };

        checkBefore.run();

        insideTx(() -> {
            checkBefore.run();

            if (multi)
                delete(1, 2, 3);
            else
                delete(1);

            checkAfter.run();
        }, commit);

        if (commit)
            checkAfter.run();
        else
            checkBefore.run();
    }

    /** */
    @Test
    public void testVisibility() {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-22874", type == ExecutorType.THIN);

        sql(format("DELETE FROM %s", tbl()));

        assertEquals("Table must be empty", 0L, sql(format("SELECT COUNT(*) FROM %s", tbl())).get(0).get(0));

        long cnt = 100;

        LongStream.range(1, 1 + cnt).forEach(i -> insideTx(() -> {
            if (type == ExecutorType.THIN) {
                ClientCache<Long, Long> thinCache = thinCli.cache(tbl());

                thinCache.put(i, i + 1);

                assertEquals("Must see transaction related data", (Long)(i + 1), thinCache.get(i));
            }
            else {
                IgniteCache<Long, Long> cache = node().cache(tbl());

                cache.put(i, i + 1);

                assertEquals("Must see transaction related data", (Long)(i + 1), cache.get(i));
            }

            List<List<?>> sqlData = sql(format("SELECT COUNT(*) FROM %s", tbl()));

            assertEquals("Must count properly", i, sqlData.get(0).get(0));
        }, true));

        List<List<?>> sqlData = sql(format("SELECT COUNT(*) FROM %s", tbl()));

        assertEquals("Must see committed data", cnt, sqlData.get(0).get(0));
    }

    /** */
    private void insideTx(RunnableX test, boolean commit) {
        if (type == ExecutorType.THIN) {
            try (ClientTransaction tx = thinCli.transactions().txStart(txConcurrency, txIsolation, TX_TIMEOUT)) {
                test.run();

                if (commit)
                    tx.commit();
                else
                    tx.rollback();
            }
        }
        else {
            Ignite initiator = node();

            assertNotNull(initiator);

            try (Transaction tx = initiator.transactions().txStart(txConcurrency, txIsolation, TX_TIMEOUT, TX_SIZE)) {
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
            return type == ExecutorType.THIN
                ? (User)thinCli.cache(users()).get(id)
                : (User)node().cache(users()).get(id);
        else if (api.equals(SQL)) {
            List<List<?>> res = sql(format("SELECT _VAL FROM %s WHERE _KEY = ?", users()), id);

            assertNotNull(res);

            return res.isEmpty() ? null : ((User)res.get(0).get(0));
        }

        fail("Unknown select: " + api);

        return null;
    }

    /** */
    private void insert(IgniteBiTuple<Integer, User>... entries) {
        for (IgniteBiTuple<Integer, User> data : entries) {
            assertTrue(partsToKeys
                .computeIfAbsent(userPartition(data.get1()), k -> new HashSet<>())
                .add(data.get1()));
        }

        doInsert(entries);
    }

    /** */
    private void doInsert(IgniteBiTuple<Integer, User>[] entries) {
        if (modify.equals(CACHE)) {
            if (multi) {
                Map<Integer, User> data = Arrays.stream(entries).collect(Collectors.toMap(IgniteBiTuple::get1, IgniteBiTuple::get2));

                if (type == ExecutorType.THIN)
                    thinCli.cache(users()).putAll(data);
                else
                    node().cache(users()).putAll(data);
            }
            else {
                for (IgniteBiTuple<Integer, User> data : entries) {
                    if (type == ExecutorType.THIN)
                        thinCli.cache(users()).put(data.get1(), data.get2());
                    else
                        node().cache(users()).put(data.get1(), data.get2());
                }
            }
        }
        else if (modify.equals(SQL)) {
            String insert = format("INSERT INTO %s(id, userid, departmentId, fio) VALUES(?, ?, ?, ?)", users());

            int colCnt = 4;

            if (multi) {
                StringBuilder sql = new StringBuilder();
                Object[] params = new Object[entries.length * colCnt];

                for (int i = 0; i < entries.length; i++) {
                    IgniteBiTuple<Integer, User> data = entries[i];

                    if (i != 0)
                        sql.append(";");

                    sql.append(insert);

                    params[i * colCnt] = data.get1();
                    params[i * colCnt + 1] = data.get2().userId;
                    params[i * colCnt + 2] = data.get2().departmentId;
                    params[i * colCnt + 3] = data.get2().fio;
                }

                sql(sql.toString(), params);
            }
            else {
                for (IgniteBiTuple<Integer, User> data : entries)
                    sql(insert, data.get1(), data.get2().userId, data.get2().departmentId, data.get2().fio);
            }
        }
        else
            fail("Unknown insert: " + modify);
    }

    /** */
    private void update(IgniteBiTuple<Integer, User>...entries) {
        for (IgniteBiTuple<Integer, User> data : entries) {
            int part = userPartition(data.get1());

            assertTrue(partsToKeys.containsKey(part));
            assertTrue(partsToKeys.get(part).contains(data.get1()));
        }

        if (modify.equals(CACHE))
            doInsert(entries);
        else if (modify.equals(SQL)) {
            String update = format("UPDATE %s SET userid = ?, departmentId = ?, fio = ? WHERE id = ?", users());

            int colCnt = 4;

            if (multi) {
                StringBuilder sql = new StringBuilder();
                Object[] params = new Object[entries.length * colCnt];

                for (int i = 0; i < entries.length; i++) {
                    IgniteBiTuple<Integer, User> data = entries[i];

                    if (i != 0)
                        sql.append(";");

                    sql.append(update);

                    params[i * colCnt] = data.get2().userId;
                    params[i * colCnt + 1] = data.get2().fio;
                    params[i * colCnt + 2] = data.get2().fio;
                    params[i * colCnt + 3] = data.get1();
                }

                sql(sql.toString(), params);
            }
            else {
                for (IgniteBiTuple<Integer, User> data : entries)
                    sql(update, data.get2().userId, data.get2().fio, data.get1());
            }
        }
        else
            fail("Unknown update: " + modify);
    }

    /** */
    private void delete(int... keys) {
        for (int key : keys) {
            int part = userPartition(key);

            assertTrue(partsToKeys.containsKey(part));
            assertTrue(partsToKeys.get(part).remove(key));
        }

        if (modify.equals(CACHE)) {
            if (multi) {
                Set<Integer> toRemove = Arrays.stream(keys).boxed().collect(Collectors.toSet());

                if (type == ExecutorType.THIN)
                    thinCli.cache(users()).removeAll(toRemove);
                else
                    node().cache(users()).removeAll(toRemove);
            }
            else {
                for (int id : keys) {
                    if (type == ExecutorType.THIN)
                        thinCli.cache(users()).remove(id);
                    else
                        node().cache(users()).remove(id);
                }
            }
        }
        else if (modify.equals(SQL)) {
            String delete = format("DELETE FROM %s WHERE id = ?", users());

            if (multi) {
                StringBuilder sql = new StringBuilder();

                for (int i = 0; i < keys.length; i++) {
                    if (i != 0)
                        sql.append(";");

                    sql.append(delete);
                }

                sql(sql.toString(), Arrays.stream(keys).boxed().toArray(Object[]::new));
            }
            else {
                for (int id : keys)
                    sql(delete, id);
            }
        }
        else
            fail("Unknown delete: " + modify);
    }

    /** */
    private void checkQueryWithPartitionFilter() {
        // https://issues.apache.org/jira/browse/IGNITE-22993
        if (mode != CacheMode.PARTITIONED)
            return;

        for (Map.Entry<Integer, Set<Integer>> partToKeys : partsToKeys.entrySet()) {
            assertEquals(
                (long)partToKeys.getValue().size(),
                sql(format("SELECT COUNT(*) FROM %s", users()), new int[]{partToKeys.getKey()}).get(0).get(0)
            );

            assertEquals(
                partToKeys.getValue().size(),
                sql(format("SELECT * FROM %s", users()), new int[]{partToKeys.getKey()}).size()
            );
        }
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
    public static class User {
        /** */
        private final int userId;

        /** */
        private final int departmentId;

        /** */
        private final String fio;

        /** */
        public User(int userId, int departmentId, String fio) {
            this.userId = userId;
            this.departmentId = departmentId;
            this.fio = fio;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            User user = (User)o;
            return userId == user.userId && departmentId == user.departmentId && Objects.equals(fio, user.fio);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(userId, departmentId, fio);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(User.class, this);
        }
    }

    /** */
    public List<List<?>> sql(String sqlText, Object... args) {
        return sql(sqlText, null, args);
    }

    /** */
    public List<List<?>> sql(String sqlText, int[] parts, Object... args) {
        if (!multi) {
            String explain = "EXPLAIN PLAN FOR ";

            if (!sqlText.startsWith(explain)) {
                List<List<?>> res = sql(explain + sqlText);
                for (List<?> r : res)
                    r.forEach(System.out::println);
            }
        }

        SqlFieldsQuery qry = new SqlFieldsQuery(sqlText)
            .setArgs(args)
            .setTimeout(5, SECONDS);

        if (!F.isEmpty(parts))
            qry.setPartitions(parts);

        if (type == ExecutorType.THIN)
            return thinCli.query(qry).getAll();

        if (multi) {
            return node().context().query().querySqlFields(qry, false, false).get(0).getAll();
        }
        else
            return node().cache(F.first(cli.cacheNames())).query(qry).getAll();
    }

    /** */
    private IgniteEx node() {
        assertTrue(type == ExecutorType.CLIENT || type == ExecutorType.SERVER);

        return type == ExecutorType.CLIENT ? cli : srv;
    }

    /** */
    private int userPartition(int id) {
        return affinity(cli.cache(users())).partition(id);
    }

    /** */
    private String users() {
        return tableName(USERS, mode);
    }

    /** */
    private String departments() {
        return tableName(DEPARTMENTS, mode);
    }

    /** */
    private String tbl() {
        return tableName(TBL, mode);
    }

    /** */
    private static String tableName(String tbl, CacheMode mode) {
        return tbl + "_" + mode;
    }
}
