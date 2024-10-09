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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.jdbc.thin.ConnectionPropertiesImpl.PROP_PREFIX;
import static org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest.ModifyApi.CACHE;
import static org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest.ModifyApi.QUERY;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.junit.Assume.assumeFalse;

/** */
public class SqlTransactionsIsolationTest extends AbstractQueryTransactionIsolationTest {
    /** */
    public static final String DEPARTMENTS = "DEPARTMENTS";

    /** */
    public static final String TBL = "TBL";

    /** */
    private ThreadLocal<Connection> jdbcThinConn = ThreadLocal.withInitial(() -> {
        try {
            String addrs = partitionAwareness
                ? Ignition.allGrids().stream()
                    .filter(n -> !n.configuration().isClientMode())
                    .map(n -> "127.0.0.1:" + ((IgniteEx)n).context().clientListener().port())
                    .collect(Collectors.joining(","))
                : "127.0.0.1:10800";

            return DriverManager.getConnection("jdbc:ignite:thin://" + addrs + "?"
                + PROP_PREFIX + "partitionAwareness=" + partitionAwareness + "&"
                + PROP_PREFIX + "transactionConcurrency=" + txConcurrency);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    });

    /** */
    private final Map<Integer, Set<Integer>> partsToKeys = new HashMap<>();

    /** @return Test parameters. */
    @Parameterized.Parameters(
        name = "gridCnt={0},backups={1},partitionAwareness={2},mode={3},execType={4},modify={5},commit={6},multi={7},txConcurrency={8}")
    public static Collection<?> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (int gridCnt : new int[]{1, 3}) {
            int[] backups = gridCnt > 1
                ? new int[]{1, gridCnt - 1}
                : new int[]{0};
            for (int backup : backups) {
                for (CacheMode mode : CacheMode.values()) {
                    for (ModifyApi modify : ModifyApi.values()) {
                        for (boolean commit : new boolean[]{false, true}) {
                            for (boolean mutli : new boolean[]{false, true}) {
                                for (TransactionConcurrency txConcurrency : TransactionConcurrency.values()) {
                                    ExecutorType[] nodeExecTypes = {ExecutorType.SERVER, ExecutorType.CLIENT};
                                    ExecutorType[] thinExecTypes;

                                    if (modify == QUERY) {
                                        thinExecTypes = new ExecutorType[]{
                                            ExecutorType.THIN_VIA_CACHE_API,
                                            ExecutorType.THIN_VIA_QUERY,
                                            ExecutorType.THIN_JDBC
                                        };
                                    }
                                    else {
                                        thinExecTypes = new ExecutorType[]{
                                            ExecutorType.THIN_VIA_CACHE_API,
                                            ExecutorType.THIN_VIA_QUERY
                                        };
                                    }

                                    for (ExecutorType execType : nodeExecTypes) {
                                        params.add(new Object[]{
                                            gridCnt,
                                            backup,
                                            false, //partitionAwareness
                                            mode,
                                            execType,
                                            modify,
                                            commit,
                                            mutli,
                                            txConcurrency
                                        });
                                    }

                                    for (ExecutorType execType : thinExecTypes) {
                                        for (boolean partitionAwareness : new boolean[]{false, true}) {
                                            params.add(new Object[]{
                                                gridCnt,
                                                backup,
                                                partitionAwareness,
                                                mode,
                                                execType,
                                                modify,
                                                commit,
                                                mutli,
                                                txConcurrency
                                            });
                                        }
                                    }
                                }
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
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getSqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void createCaches() {
        super.createCaches();

        for (CacheMode mode : CacheMode.values()) {
            String deps = tableName(DEPARTMENTS, mode);

            LinkedHashMap<String, String> depFlds = new LinkedHashMap<>();

            depFlds.put("id", Integer.class.getName());
            depFlds.put("name", String.class.getName());

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
                .setName(tableName(TBL, mode))
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(backups)
                .setCacheMode(mode)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setSqlSchema(QueryUtils.DFLT_SCHEMA)
                .setQueryEntities(Collections.singleton(new QueryEntity()
                    .setTableName(tableName(TBL, mode))
                    .setKeyType(Long.class.getName())
                    .setValueType(Long.class.getName()))));
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        assertEquals(mode, cli.cache(tbl()).getConfiguration(CacheConfiguration.class).getCacheMode());
    }

    /** */
    @Test
    public void testIndexScan() {
        delete(1);

        assertUsersSize(0);

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

        assertUsersSize(stepCnt * outOfTxSz);

        checkQueryWithPartitionFilter();

        insideTx(() -> {
            int nullFio = 0;

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

                    String fio = ThreadLocalRandom.current().nextBoolean()
                        ? ("User" + j) // Intentionally repeat FIO to make same indexed keys.
                        : null;

                    if (fio == null)
                        nullFio++;

                    insert(F.t(id, new User(id, 0, fio)));

                    if (txConcurrency == TransactionConcurrency.OPTIMISTIC) {
                        // Concurrent query must not see any transaction data.
                        runAsync(() -> {
                            RunnableX check = () -> {
                                assertUsersSize(stepCnt * outOfTxSz);

                                if (type != ExecutorType.THIN_JDBC)
                                    assertNull(select(id, CACHE));
                                assertNull(select(id, QUERY));
                            };

                            insideTx(check, false);
                            check.run();
                        }).get(TX_TIMEOUT);
                    }

                    long expTblSz = (long)(stepCnt * outOfTxSz) + i * inTxSz + j + 1;

                    assertUsersSize(expTblSz);

                    List<List<?>> rows = sql(format("SELECT fio FROM %s ORDER BY fio", users()));

                    assertEquals(expTblSz, rows.size());

                    ensureSorted(rows, true);

                    rows = sql(format("SELECT COUNT(fio) FROM %s", users()));

                    assertEquals(expTblSz - nullFio, rows.get(0).get(0));

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

                    assertUsersSize(expTblSz);

                    List<List<?>> rows = sql(format("SELECT fio FROM %s ORDER BY fio DESC", users()));

                    assertEquals(expTblSz, rows.size());

                    ensureSorted(rows, false);
                }
            }

            checkQueryWithPartitionFilter();
        }, true);

        assertUsersSize(inTxSz * stepCnt);
    }

    /** */
    @Test
    public void testJoin() {
        partsToKeys.clear();

        sql(format("DELETE FROM %s", users()));
        sql(format("DELETE FROM %s", departments()));

        assertUsersSize(0);
        assertTableSize(0, departments());

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
    @Test
    public void testVisibility() {
        assumeFalse(type == ExecutorType.THIN_JDBC);

        sql(format("DELETE FROM %s", tbl()));

        assertTableSize(0, tbl());

        long cnt = 100;

        LongStream.range(1, 1 + cnt).forEach(i -> insideTx(() -> {
            if (type == ExecutorType.THIN_VIA_CACHE_API || type == ExecutorType.THIN_VIA_QUERY) {
                ClientCache<Long, Long> thinCache = thinCli.cache(tbl());

                thinCache.put(i, i + 1);

                assertEquals("Must see transaction related data", (Long)(i + 1), thinCache.get(i));
            }
            else {
                IgniteCache<Long, Long> cache = node().cache(tbl());

                cache.put(i, i + 1);

                assertEquals("Must see transaction related data", (Long)(i + 1), cache.get(i));
            }

            assertTableSize(i, tbl());
        }, true));

        assertTableSize(cnt, tbl());
    }

    /** */
    private void checkJoin(Map<Integer, List<User>> data, IntFunction<String> uname) {
        List<List<?>> res = sql(
            format("SELECT d.id, u.id, u.fio FROM %s d JOIN %s u ON d.id = u.departmentId", departments(), users())
        );

        Map<Integer, List<User>> data0 = new HashMap<>();

        data.forEach((key, value) -> data0.put(key, new ArrayList<>(value)));

        assertEquals(data0.values().stream().mapToInt(List::size).sum(), res.size());

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

    /** {@inheritDoc} */
    @Override protected void insideTx(RunnableX test, boolean commit) {
        if (type == ExecutorType.THIN_JDBC) {
            try {
                jdbcThinConn.get().setAutoCommit(false);
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }

            try {
                test.run();
            }
            finally {
                try {
                    if (commit)
                        jdbcThinConn.get().commit();
                    else
                        jdbcThinConn.get().rollback();

                    jdbcThinConn.get().setAutoCommit(true);
                }
                catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        else
            super.insideTx(test, commit);
    }

    /** {@inheritDoc} */
    @Override protected User select(Integer id, ModifyApi api) {
        if (api == QUERY) {
            List<List<?>> res = sql(format("SELECT _VAL FROM %s WHERE _KEY = ?", users()), id);

            assertNotNull(res);

            return res.isEmpty() ? null : ((User)res.get(0).get(0));
        }

        return super.select(id, api);
    }

    /** {@inheritDoc} */
    @Override protected void insert(IgniteBiTuple<Integer, User>... entries) {
        for (IgniteBiTuple<Integer, User> data : entries) {
            assertTrue(partsToKeys
                .computeIfAbsent(userPartition(data.get1()), k -> new HashSet<>())
                .add(data.get1()));
        }

        super.insert(entries);
    }

    /** {@inheritDoc} */
    @Override protected void doInsert(boolean update, IgniteBiTuple<Integer, User>... entries) {
        if (modify == QUERY) {
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
            super.doInsert(update, entries);
    }

    /** {@inheritDoc} */
    @Override protected void update(IgniteBiTuple<Integer, User>...entries) {
        for (IgniteBiTuple<Integer, User> data : entries) {
            int part = userPartition(data.get1());

            assertTrue(partsToKeys.containsKey(part));
            assertTrue(partsToKeys.get(part).contains(data.get1()));
        }

        if (modify == QUERY) {
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
                    params[i * colCnt + 1] = data.get2().departmentId;
                    params[i * colCnt + 2] = data.get2().fio;
                    params[i * colCnt + 3] = data.get1();
                }

                sql(sql.toString(), params);
            }
            else {
                for (IgniteBiTuple<Integer, User> data : entries)
                    sql(update, data.get2().userId, data.get2().departmentId, data.get2().fio, data.get1());
            }
        }
        else
            super.update(entries);
    }

    /** {@inheritDoc} */
    @Override protected void delete(int... keys) {
        for (int key : keys) {
            int part = userPartition(key);

            assertTrue(partsToKeys.containsKey(part));
            assertTrue(partsToKeys.get(part).remove(key));
        }

        if (modify == QUERY) {
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
            super.delete(keys);
    }

    /** */
    private void checkQueryWithPartitionFilter() {
        // https://issues.apache.org/jira/browse/IGNITE-22993
        if (mode != CacheMode.PARTITIONED)
            return;

        // Partitions filter not supported by JDBC.
        if (type == ExecutorType.THIN_JDBC)
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
    private void assertUsersSize(long sz) {
        assertTableSize(sz, users());
    }

    /** */
    private void assertTableSize(long sz, String tbl) {
        assertEquals(sz, sql(format("SELECT COUNT(*) FROM %s", tbl)).get(0).get(0));
    }

    /** */
    private static void ensureSorted(List<List<?>> rows, boolean asc) {
        for (int k = 1; k < rows.size(); k++) {
            String fio0 = (String)rows.get(k - 1).get(0);
            String fio1 = (String)rows.get(k).get(0);

            int cmp = fio0 == null
                ? (fio1 == null ? 0 : -1)
                : (fio1 == null ? 1 : fio0.compareTo(fio1));

            assertTrue(asc ? (cmp <= 0) : (cmp >= 0));
        }
    }

    /** */
    public List<List<?>> sql(String sqlText, Object... args) {
        return sql(sqlText, null, args);
    }

    /** */
    public List<List<?>> sql(String sqlText, int[] parts, Object... args) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sqlText)
            .setArgs(args)
            .setTimeout(5, SECONDS);

        if (!F.isEmpty(parts))
            qry.setPartitions(parts);

        if (type == ExecutorType.THIN_VIA_QUERY)
            return unwrapBinary(thinCli.query(qry).getAll());
        else if (type == ExecutorType.THIN_VIA_CACHE_API)
            return unwrapBinary(thinCli.cache(F.first(thinCli.cacheNames())).query(qry).getAll());
        else if (type == ExecutorType.THIN_JDBC) {
            assertTrue("Partition filter not supported", F.isEmpty(parts));

            try {
                PreparedStatement stmt = jdbcThinConn.get().prepareStatement(sqlText);

                if (!F.isEmpty(args)) {
                    for (int i = 0; i < args.length; i++)
                        stmt.setObject(i + 1, args[i]);
                }

                List<List<?>> res = new ArrayList<>();

                if (sqlText.startsWith("SELECT")) {
                    try (ResultSet rset = stmt.executeQuery()) {
                        int colCnt = rset.getMetaData().getColumnCount();

                        while (rset.next()) {
                            List<Object> row = new ArrayList<>();

                            res.add(row);

                            for (int i = 0; i < colCnt; i++)
                                row.add(rset.getObject(i + 1));
                        }

                        return res;
                    }
                }
                else
                    res.add(List.of(stmt.executeUpdate()));

                return res;
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        if (multi)
            return node().context().query().querySqlFields(qry, false, false).get(0).getAll();
        else
            return node().cache(F.first(cli.cacheNames())).query(qry).getAll();
    }

    /** */
    private int userPartition(int id) {
        return affinity(cli.cache(users())).partition(id);
    }

    /** */
    private String departments() {
        return tableName(DEPARTMENTS, mode);
    }

    /** */
    private String tbl() {
        return tableName(TBL, mode);
    }

    /** {@inheritDoc} */
    @Override protected void ensureModeSupported() {
        assumeFalse(
            "Thin client doesn't support multiple statements for SQL",
            multi && modify == QUERY && (type == ExecutorType.THIN_VIA_CACHE_API || type == ExecutorType.THIN_VIA_QUERY)
        );
    }

    /** */
    private List<List<?>> unwrapBinary(List<List<?>> all) {
        return all.stream()
            .map(row -> row.stream()
                .map(col -> col instanceof BinaryObject ? ((BinaryObject)col).deserialize() : col)
                .collect(Collectors.toList()))
            .collect(Collectors.toList());
    }
}
