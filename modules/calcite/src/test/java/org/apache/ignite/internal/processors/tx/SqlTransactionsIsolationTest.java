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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.odbc.ClientMessage;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.jdbc.thin.ConnectionPropertiesImpl.PROP_PREFIX;
import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.CONN_CTX_META_KEY;
import static org.apache.ignite.internal.processors.tx.SqlTransactionsIsolationTest.ModifyApi.CACHE;
import static org.apache.ignite.internal.processors.tx.SqlTransactionsIsolationTest.ModifyApi.ENTRY_PROCESSOR;
import static org.apache.ignite.internal.processors.tx.SqlTransactionsIsolationTest.ModifyApi.SQL;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.junit.Assume.assumeFalse;

/** */
@RunWith(Parameterized.class)
public class SqlTransactionsIsolationTest extends GridCommonAbstractTest {
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
        THIN_VIA_CACHE_API,

        /** */
        THIN_VIA_QUERY,

        /** */
        THIN_JDBC
    }

    /** */
    public enum ModifyApi {
        /** */
        CACHE,

        /** */
        ENTRY_PROCESSOR,

        /** */
        SQL
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
    public ModifyApi modify;

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
    @Parameterized.Parameter(8)
    public TransactionConcurrency txConcurrency;

    /** */
    private static IgniteEx srv;

    /** */
    private static IgniteEx cli;

    /** */
    private static IgniteClient thinCli;

    /** */
    private static ClientConfiguration thinCliCfg;

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
    private final TransactionIsolation txIsolation = READ_COMMITTED;

    /** */
    private final Map<Integer, Set<Integer>> partsToKeys = new HashMap<>();

    /** @return Test parameters. */
    @Parameterized.Parameters(
        name = "modify={0},qryExecutor={1},partitionAwareness={2},mode={3},gridCnt={4},backups={5},commit={6},multi={7},txConcurrency={8}")
    public static Collection<?> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (CacheMode cacheMode : CacheMode.values()) {
            for (int gridCnt : new int[]{1, 3}) {
                int[] backups = gridCnt > 1
                    ? new int[]{1, gridCnt - 1}
                    : new int[]{0};
                for (int backup: backups) {
                    for (ModifyApi modify : ModifyApi.values()) {
                        for (boolean commit : new boolean[]{false, true}) {
                            for (boolean mutli : new boolean[] {false, true}) {
                                for (TransactionConcurrency txConcurrency : TransactionConcurrency.values()) {
                                    ExecutorType[] nodeExecTypes = {ExecutorType.SERVER, ExecutorType.CLIENT};
                                    ExecutorType[] thinExecTypes;

                                    if (modify == SQL) {
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
                                            modify,
                                            execType,
                                            false, //partitionAwareness
                                            cacheMode,
                                            gridCnt,
                                            backup,
                                            commit,
                                            mutli,
                                            txConcurrency
                                        });
                                    }

                                    for (ExecutorType execType : thinExecTypes) {
                                        for (boolean partitionAwareness : new boolean[]{false, true}) {
                                            params.add(new Object[]{modify,
                                                execType,
                                                partitionAwareness,
                                                cacheMode,
                                                gridCnt,
                                                backup,
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
        cfg.getTransactionConfiguration().setTxAwareQueriesEnabled(true);

        return cfg;
    }

    /** */
    private void startAllGrids() throws Exception {
        srv = startGrids(gridCnt);
        cli = startClientGrid("client");
    }

    /** */
    private void startThinClient() {
        thinCliCfg = new ClientConfiguration()
            .setAddresses(Config.SERVER)
            .setPartitionAwarenessEnabled(partitionAwareness);
        thinCli = Ignition.startClient(thinCliCfg);
    }

    /** */
    private void createCaches() {
        cli.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(mode)
            .setBackups(backups));

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

        for (CacheMode mode : CacheMode.values()) {
            String users = tableName(USERS, mode);
            String deps = tableName(DEPARTMENTS, mode);

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

        if (gridCnt != Ignition.allGrids().size() - 1) {
            stopAllGrids();

            startAllGrids();

            startThinClient();

            createCaches();
        }

        if (partitionAwareness != thinCliCfg.isPartitionAwarenessEnabled())
            startThinClient();

        boolean recreate = cli.cacheNames().stream()
            .map(name -> cli.cache(name).getConfiguration(CacheConfiguration.class))
            .filter(ccfg -> ccfg.getCacheMode() == CacheMode.PARTITIONED)
            .anyMatch(ccfg -> backups != ccfg.getBackups());

        if (recreate) {
            cli.cacheNames().forEach(cli::destroyCache);

            createCaches();
        }

        cli.cache(users()).removeAll();
        cli.cache(tbl()).removeAll();
        cli.cache(departments()).removeAll();

        insert(F.t(1, JOHN));

        assertEquals(gridCnt + 1, Ignition.allGrids().size());
        assertEquals(partitionAwareness, thinCliCfg.isPartitionAwarenessEnabled());
        assertEquals(mode, cli.cache(tbl()).getConfiguration(CacheConfiguration.class).getCacheMode());
        cli.cacheNames().stream()
            .map(name -> cli.cache(name).getConfiguration(CacheConfiguration.class))
            .filter(ccfg -> ccfg.getCacheMode() == CacheMode.PARTITIONED)
            .forEach(ccfg -> assertEquals(ccfg.getName(), backups, ccfg.getBackups()));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (int i = 0; i < gridCnt + 1; i++) {
            IgniteEx srv = i == gridCnt ? cli : grid(i);

            assertTrue(srv.context().cache().context().tm().activeTransactions().isEmpty());

            GridNioServer<ClientMessage> nioSrv = GridTestUtils.getFieldValue(srv.context().clientListener(), "srv");

            for (GridNioSession ses : nioSrv.sessions()) {
                Object meta = ses.meta(CONN_CTX_META_KEY);

                if (meta instanceof ClientConnectionContext)
                    assertTrue(GridTestUtils.<Map<?, ?>>getFieldValue(meta, "txs").isEmpty());
                else if (meta instanceof JdbcConnectionContext)
                    assertNull(GridTestUtils.getFieldValue(meta, "txCtx"));
                else
                    throw new IllegalStateException("Unknown context");
            }
        }
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
                                assertNull(select(id, SQL));
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

    /** */
    @Test
    public void testInsert() {
        ensureModeSupported();

        Runnable checkBefore = () -> {
            for (int i = 4; i <= (multi ? 6 : 4); i++) {
                if (type != ExecutorType.THIN_JDBC)
                    assertNull(CACHE.name(), select(i, CACHE));
                assertNull(SQL.name(), select(i, SQL));
            }
        };

        Runnable checkAfter = () -> {
            for (int i = 4; i <= (multi ? 6 : 4); i++) {
                if (type != ExecutorType.THIN_JDBC)
                    assertEquals(CACHE.name(), JOHN, select(i, CACHE));
                assertEquals(SQL.name(), JOHN, select(i, SQL));
            }
        };

        checkBefore.run();

        insideTx(() -> {
            checkBefore.run();

            if (multi)
                insert(F.t(4, JOHN), F.t(5, JOHN), F.t(6, JOHN));
            else
                insert(F.t(4, JOHN));

            if (modify == SQL) {
                assertThrows(
                    log,
                    () -> doInsert(false, F.t(4, JOHN)),
                    IgniteException.class,
                    "Failed to INSERT some keys because they are already in cache"
                );
            }

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
        ensureModeSupported();

        if (multi)
            insert(F.t(2, JOHN), F.t(3, JOHN));

        Runnable checkBefore = () -> {
            for (int i = 1; i <= (multi ? 3 : 1); i++) {
                if (type != ExecutorType.THIN_JDBC)
                    assertEquals(JOHN, select(i, CACHE));
                assertEquals(JOHN, select(i, SQL));
            }
        };

        Runnable checkAfter = () -> {
            for (int i = 1; i <= (multi ? 3 : 1); i++) {
                if (type != ExecutorType.THIN_JDBC)
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
                if (type != ExecutorType.THIN_JDBC)
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
        ensureModeSupported();

        if (multi)
            insert(F.t(2, JOHN), F.t(3, JOHN));

        Runnable checkBefore = () -> {
            for (int i = 1; i <= (multi ? 3 : 1); i++) {
                if (type != ExecutorType.THIN_JDBC)
                    assertEquals(JOHN, select(i, CACHE));
                assertEquals(JOHN, select(i, SQL));
            }
        };

        Runnable checkAfter = () -> {
            for (int i = 1; i <= (multi ? 3 : 1); i++) {
                if (type != ExecutorType.THIN_JDBC)
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
    private void insideTx(RunnableX test, boolean commit) {
        if (type == ExecutorType.THIN_VIA_CACHE_API || type == ExecutorType.THIN_VIA_QUERY) {
            try (ClientTransaction tx = thinCli.transactions().txStart(txConcurrency, txIsolation, TX_TIMEOUT)) {
                for (int i = 0; i < 3; i++)
                    thinCli.cache(DEFAULT_CACHE_NAME).put(i, i);

                test.run();

                if (commit) {
                    for (int i = 0; i < 3; i++)
                        thinCli.cache(DEFAULT_CACHE_NAME).remove(i, i);

                    tx.commit();
                }
                else
                    tx.rollback();
            }
        }
        else if (type == ExecutorType.THIN_JDBC) {
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
        else {
            Ignite initiator = node();

            assertNotNull(initiator);

            try (Transaction tx = initiator.transactions().txStart(txConcurrency, txIsolation, TX_TIMEOUT, TX_SIZE)) {
                for (int i = 0; i < 3; i++)
                    initiator.cache(DEFAULT_CACHE_NAME).put(i, i);

                test.run();

                if (commit) {
                    for (int i = 0; i < 3; i++)
                        initiator.cache(DEFAULT_CACHE_NAME).remove(i, i);

                    tx.commit();
                }
                else
                    tx.rollback();
            }
        }
    }

    /** */
    private User select(Integer id, ModifyApi api) {
        if (api == CACHE)
            return (type == ExecutorType.THIN_VIA_CACHE_API || type == ExecutorType.THIN_VIA_QUERY)
                ? (User)thinCli.cache(users()).get(id)
                : (User)node().cache(users()).get(id);
        else if (api == SQL) {
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

        doInsert(false, entries);
    }

    /** */
    private void doInsert(boolean update, IgniteBiTuple<Integer, User>... entries) {
        if (modify == CACHE) {
            if (multi) {
                Map<Integer, User> data = Arrays.stream(entries).collect(Collectors.toMap(IgniteBiTuple::get1, IgniteBiTuple::get2));

                if (type == ExecutorType.THIN_VIA_CACHE_API || type == ExecutorType.THIN_VIA_QUERY)
                    thinCli.cache(users()).putAll(data);
                else
                    node().cache(users()).putAll(data);
            }
            else {
                for (IgniteBiTuple<Integer, User> data : entries) {
                    if (type == ExecutorType.THIN_VIA_CACHE_API || type == ExecutorType.THIN_VIA_QUERY)
                        thinCli.cache(users()).put(data.get1(), data.get2());
                    else
                        node().cache(users()).put(data.get1(), data.get2());
                }
            }
        }
        else if (modify == ENTRY_PROCESSOR) {
            if (multi) {
                Set<Integer> keys = Arrays.stream(entries).map(IgniteBiTuple::get1).collect(Collectors.toSet());
                Map<Integer, User> data = Arrays.stream(entries).collect(Collectors.toMap(IgniteBiTuple::get1, IgniteBiTuple::get2));

                if (type == ExecutorType.THIN_VIA_QUERY || type == ExecutorType.THIN_VIA_CACHE_API)
                    thinCli.cache(users()).invokeAll(keys, new UpdateEntryProcessor<>(update), data);
                else
                    node().cache(users()).invokeAll(keys, new UpdateEntryProcessor<>(update), data);
            }
            else {
                for (IgniteBiTuple<Integer, User> data : entries) {
                    if (type == ExecutorType.THIN_VIA_QUERY || type == ExecutorType.THIN_VIA_CACHE_API)
                        thinCli.cache(users()).invoke(data.get1(), new UpdateEntryProcessor<>(update), data.get2());
                    else
                        node().cache(users()).invoke(data.get1(), new UpdateEntryProcessor<>(update), data.get2());
                }
            }
        }
        else if (modify == SQL) {
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

        if (modify == CACHE || modify == ENTRY_PROCESSOR)
            doInsert(true, entries);
        else if (modify == SQL) {
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
            fail("Unknown update: " + modify);
    }

    /** */
    private void delete(int... keys) {
        for (int key : keys) {
            int part = userPartition(key);

            assertTrue(partsToKeys.containsKey(part));
            assertTrue(partsToKeys.get(part).remove(key));
        }

        if (modify == CACHE) {
            if (multi) {
                Set<Integer> toRemove = Arrays.stream(keys).boxed().collect(Collectors.toSet());

                if (type == ExecutorType.THIN_VIA_CACHE_API || type == ExecutorType.THIN_VIA_QUERY)
                    thinCli.cache(users()).removeAll(toRemove);
                else
                    node().cache(users()).removeAll(toRemove);
            }
            else {
                for (int id : keys) {
                    if (type == ExecutorType.THIN_VIA_CACHE_API || type == ExecutorType.THIN_VIA_QUERY)
                        thinCli.cache(users()).remove(id);
                    else
                        node().cache(users()).remove(id);
                }
            }
        }
        else if (modify == ENTRY_PROCESSOR) {
            if (multi) {
                Set<Integer> toRemove = Arrays.stream(keys).boxed().collect(Collectors.toSet());

                if (type == ExecutorType.THIN_VIA_QUERY || type == ExecutorType.THIN_VIA_CACHE_API)
                    thinCli.cache(users()).invokeAll(toRemove, new RemoveEntryProcessor<>());
                else
                    node().cache(users()).invokeAll(toRemove, new RemoveEntryProcessor<>());
            }
            else {
                for (int id : keys) {
                    if (type == ExecutorType.THIN_VIA_QUERY || type == ExecutorType.THIN_VIA_CACHE_API)
                        thinCli.cache(users()).invoke(id, new RemoveEntryProcessor<>());
                    else
                        node().cache(users()).invoke(id, new RemoveEntryProcessor<>());
                }
            }
        }
        else if (modify == SQL) {
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

    /** */
    private void ensureModeSupported() {
        assumeFalse(
            "Thin client doesn't support multiple statements for SQL",
            multi && modify == SQL && (type == ExecutorType.THIN_VIA_CACHE_API || type == ExecutorType.THIN_VIA_QUERY)
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

    /** */
    public static class RemoveEntryProcessor<K, V, T> implements EntryProcessor<K, V, T> {
        /** {@inheritDoc} */
        @Override public T process(MutableEntry<K, V> entry, Object... arguments) throws EntryProcessorException {
            entry.remove();

            return null;
        }
    }

    /** */
    public static class UpdateEntryProcessor<K, V, T> implements EntryProcessor<K, V, T> {
        /** */
        private final boolean update;

        /** */
        public UpdateEntryProcessor() {
            this(true);
        }

        /** */
        public UpdateEntryProcessor(boolean update) {
            this.update = update;
        }

        /** {@inheritDoc} */
        @Override public T process(MutableEntry<K, V> entry, Object... arguments) throws EntryProcessorException {
            assertEquals("Expect entry " + (update ? "" : "not") + " exists", update, entry.exists());

            if (arguments[0] instanceof User)
                entry.setValue((V)arguments[0]);
            else
                entry.setValue((V)((Map<Integer, User>)arguments[0]).get((Integer)entry.getKey()));

            return null;
        }
    }
}
