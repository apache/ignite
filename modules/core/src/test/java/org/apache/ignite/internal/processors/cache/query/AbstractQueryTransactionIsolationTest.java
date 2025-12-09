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

package org.apache.ignite.internal.processors.cache.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
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
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest.ExecutorType.SERVER;
import static org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest.ExecutorType.THIN_VIA_QUERY;
import static org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest.ModifyApi.CACHE;
import static org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest.ModifyApi.ENTRY_PROCESSOR;
import static org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest.ModifyApi.QUERY;
import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.CONN_CTX_META_KEY;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/** */
public abstract class AbstractQueryTransactionIsolationTest extends GridCommonAbstractTest {
    /** */
    public static final String USERS = "USERS";

    /** */
    public static final int TX_TIMEOUT = 60_000;

    /** */
    public static final int TX_SIZE = 10;

    /** */
    public static final User JOHN = new User(1, 0, "John Connor");

    /** */
    public static final User SARAH = new User(2, 0, "Sarah Connor");

    /** */
    public static final User KYLE = new User(3, 0, "Kyle Reese");

    /** */
    protected static IgniteEx srv;

    /** */
    protected static IgniteEx cli;

    /** */
    protected static IgniteClient thinCli;

    /** */
    protected static ClientConfiguration thinCliCfg;

    /** */
    private final TransactionIsolation txIsolation = READ_COMMITTED;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getTransactionConfiguration().setTxAwareQueriesEnabled(true);

        return cfg;
    }

    /** */
    private void startAllGrids(int gridCnt) throws Exception {
        srv = startGrids(gridCnt);
        cli = startClientGrid("client");
    }

    /** */
    private void startThinClient(boolean partitionAwareness) {
        thinCliCfg = new ClientConfiguration()
            .setAddresses(Config.SERVER)
            .setPartitionAwarenessEnabled(partitionAwareness);
        thinCli = Ignition.startClient(thinCliCfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    void beforeEach(int gridCnt, boolean partitionAwareness, int backups, ExecutorType type, ModifyApi modify, boolean multi, CacheMode mode) {
        boolean recreate = cli.cacheNames().stream()
            .map(name -> cli.cache(name).getConfiguration(CacheConfiguration.class))
            .filter(ccfg -> ccfg.getCacheMode() == CacheMode.PARTITIONED)
            .anyMatch(ccfg -> backups != ccfg.getBackups());

        if (recreate) {
            cli.cacheNames().forEach(cli::destroyCache);

            createCaches(mode, backups);
        }

        cli.cacheNames().forEach(name -> cli.cache(name).removeAll());

        insert(type, modify, multi, mode, F.t(1, JOHN));

        assertEquals(gridCnt + 1, Ignition.allGrids().size());
        assertEquals(partitionAwareness, thinCliCfg.isPartitionAwarenessEnabled());
        cli.cacheNames().stream()
            .map(name -> cli.cache(name).getConfiguration(CacheConfiguration.class))
            .filter(ccfg -> ccfg.getCacheMode() == CacheMode.PARTITIONED)
            .forEach(ccfg -> assertEquals(ccfg.getName(), backups, ccfg.getBackups()));
    }

    void tearDown(int gridCnt) {
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
                else if (meta != null)
                    throw new IllegalStateException("Unknown context");
            }
        }
    }

    private static List<Arguments> insertInvariants() {
        List<Arguments> args = new ArrayList<>();

        for (int gridCnt : new int[]{1, 3}) {
            int[] backups = gridCnt > 1
                    ? new int[]{1, gridCnt - 1}
                    : new int[]{0};

            for (int backup : backups) {
                for (CacheMode mode : CacheMode.values()) {
                    for (ModifyApi modify : new ModifyApi[]{ModifyApi.CACHE, ModifyApi.ENTRY_PROCESSOR}) {
                        for (boolean commit : new boolean[]{false, true}) {
                            for (boolean mutli : new boolean[]{false, true}) {
                                for (TransactionConcurrency txConcurrency : TransactionConcurrency.values()) {
                                    for (ExecutorType execType : new ExecutorType[]{SERVER, ExecutorType.CLIENT}) {
                                        args.add(Arguments.arguments(
                                                gridCnt,
                                                backup,
                                                false, //partition awareness
                                                mode,
                                                execType,
                                                modify,
                                                commit,
                                                mutli,
                                                txConcurrency
                                        ));
                                    }

                                    for (boolean partitionAwareness : new boolean[]{false, true}) {
                                        args.add(Arguments.arguments(
                                                gridCnt,
                                                backup,
                                                partitionAwareness,
                                                mode,
                                                THIN_VIA_QUERY, // executor type
                                                modify,
                                                commit,
                                                mutli,
                                                txConcurrency
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        return args;
    }

    /** */
    @ParameterizedTest
    @MethodSource("insertInvariants")
    public void testInsert(
            int gridCnt,
            int backups,
            boolean partitionAwareness,
            CacheMode mode,
            ExecutorType type,
            ModifyApi modify,
            boolean commit,
            boolean multi,
            TransactionConcurrency txConcurrency
    ) throws Exception {
        ensureModeSupported();

        if (gridCnt != Ignition.allGrids().size() - 1) {
            stopAllGrids();

            startAllGrids(gridCnt);

            startThinClient(partitionAwareness);

            createCaches(mode, backups);
        }

        if (partitionAwareness != thinCliCfg.isPartitionAwarenessEnabled())
            startThinClient(partitionAwareness);

        beforeEach(gridCnt, partitionAwareness, backups, type, modify, multi, mode);

        Runnable checkBefore = () -> {
            for (int i = 4; i <= (multi ? 6 : 4); i++) {
                if (type != ExecutorType.THIN_JDBC)
                    assertNull(CACHE.name(), select(i, CACHE, type, mode));
                assertNull(QUERY.name(), select(i, QUERY, type, mode));
            }
        };

        Runnable checkAfter = () -> {
            for (int i = 4; i <= (multi ? 6 : 4); i++) {
                if (type != ExecutorType.THIN_JDBC)
                    assertEquals(CACHE.name(), JOHN, select(i, CACHE, type, mode));
                assertEquals(QUERY.name(), JOHN, select(i, QUERY, type, mode));
            }
        };

        checkBefore.run();

        insideTx(() -> {
            checkBefore.run();

            if (multi)
                insert(type, modify, multi, mode, F.t(4, JOHN), F.t(5, JOHN), F.t(6, JOHN));
            else
                insert(type, modify, multi, mode, F.t(4, JOHN));

            if (modify == QUERY) {
                assertThrows(
                    log,
                    () -> doInsert(false, type, modify, multi, mode, F.t(4, JOHN)),
                    IgniteException.class,
                    "Failed to INSERT some keys because they are already in cache"
                );
            }

            checkAfter.run();
        }, commit, type, txConcurrency);

        if (commit)
            checkAfter.run();
        else
            checkBefore.run();

        tearDown(gridCnt);
    }

    /** */
    @ParameterizedTest
    @MethodSource("insertInvariants")
    public void testUpdate(
        int gridCnt,
        int backups,
        boolean partitionAwareness,
        CacheMode mode,
        ExecutorType type,
        ModifyApi modify,
        boolean commit,
        boolean multi,
        TransactionConcurrency txConcurrency
    ) throws Exception {
        ensureModeSupported();

        if (multi)
            insert(type, modify, multi, mode, F.t(2, JOHN), F.t(3, JOHN));

        Runnable checkBefore = () -> {
            for (int i = 1; i <= (multi ? 3 : 1); i++) {
                if (type != ExecutorType.THIN_JDBC)
                    assertEquals(JOHN, select(i, CACHE, type, mode));
                assertEquals(JOHN, select(i, QUERY, type, mode));
            }
        };

        Runnable checkAfter = () -> {
            for (int i = 1; i <= (multi ? 3 : 1); i++) {
                if (type != ExecutorType.THIN_JDBC)
                    assertEquals(KYLE, select(i, CACHE, type, mode));
                assertEquals(KYLE, select(i, QUERY, type, mode));
            }
        };

        checkBefore.run();

        insideTx(() -> {
            checkBefore.run();

            if (multi)
                update(type, modify, multi, mode, F.t(1, SARAH), F.t(2, SARAH), F.t(3, SARAH));
            else
                update(type, modify, multi, mode, F.t(1, SARAH));

            for (int i = 1; i <= (multi ? 3 : 1); i++) {
                if (type != ExecutorType.THIN_JDBC)
                    assertEquals(SARAH, select(i, CACHE, type, mode));
                assertEquals(SARAH, select(i, QUERY, type, mode));
            }

            if (multi)
                update(type, modify, multi, mode, F.t(1, KYLE), F.t(2, KYLE), F.t(3, KYLE));
            else
                update(type, modify, multi, mode, F.t(1, KYLE));

            checkAfter.run();
        }, commit, type, txConcurrency);

        if (commit)
            checkAfter.run();
        else
            checkBefore.run();
    }

    /** */
    @ParameterizedTest
    @MethodSource("insertInvariants")
    public void testDelete(
            int gridCnt,
            int backups,
            boolean partitionAwareness,
            CacheMode mode,
            ExecutorType type,
            ModifyApi modify,
            boolean commit,
            boolean multi,
            TransactionConcurrency txConcurrency
    ) throws Exception {
        ensureModeSupported();

        if (multi)
            insert(type, modify, multi, mode, F.t(2, JOHN), F.t(3, JOHN));

        Runnable checkBefore = () -> {
            for (int i = 1; i <= (multi ? 3 : 1); i++) {
                if (type != ExecutorType.THIN_JDBC)
                    assertEquals(JOHN, select(i, CACHE, type, mode));
                assertEquals(JOHN, select(i, QUERY, type, mode));
            }
        };

        Runnable checkAfter = () -> {
            for (int i = 1; i <= (multi ? 3 : 1); i++) {
                if (type != ExecutorType.THIN_JDBC)
                    assertNull(select(i, CACHE, type, mode));
                assertNull(select(i, QUERY, type, mode));
            }
        };

        checkBefore.run();

        insideTx(() -> {
            checkBefore.run();

            if (multi)
                delete(type, modify, multi, mode, 1, 2, 3);
            else
                delete(type, modify, multi, mode, 1);

            checkAfter.run();
        }, commit, type, txConcurrency);

        if (commit)
            checkAfter.run();
        else
            checkBefore.run();
    }

    /** */
    protected void createCaches(CacheMode mode, int backups) {
        cli.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(mode)
            .setBackups(backups));

        for (CacheMode cacheMode : CacheMode.values()) {
            String users = tableName(USERS, cacheMode);

            LinkedHashMap<String, String> userFlds = new LinkedHashMap<>();

            userFlds.put("id", Integer.class.getName());
            userFlds.put("userId", Integer.class.getName());
            userFlds.put("departmentId", Integer.class.getName());
            userFlds.put("fio", String.class.getName());

            cli.createCache(new CacheConfiguration<>()
                .setName(users)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(cacheMode)
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
        }
    }

    /** */
    protected void insideTx(RunnableX test, boolean commit, ExecutorType type, TransactionConcurrency txConcurrency) {
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
        else if (type == ExecutorType.SERVER || type == ExecutorType.CLIENT) {
            Ignite initiator = node(type);

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
        else
            fail("Unknown executor type: " + type);
    }

    /** */
    protected User select(Integer id, ModifyApi api, ExecutorType type, CacheMode mode) {
        if (api == CACHE)
            return (type == ExecutorType.THIN_VIA_CACHE_API || type == ExecutorType.THIN_VIA_QUERY)
                ? (User)thinCli.cache(users(mode)).get(id)
                : (User)node(type).cache(users(mode)).get(id);

        fail("Unknown select: " + api);

        return null;
    }

    /** */
    protected void insert(ExecutorType type, ModifyApi modify, boolean multi, CacheMode mode, IgniteBiTuple<Integer, User>... entries) {
        doInsert(false, type, modify, multi, mode, entries);
    }

    /** */
    protected void doInsert(boolean update, ExecutorType type, ModifyApi modify, boolean multi, CacheMode mode, IgniteBiTuple<Integer, User>... entries) {
        if (modify == CACHE) {
            if (multi) {
                Map<Integer, User> data = Arrays.stream(entries).collect(Collectors.toMap(IgniteBiTuple::get1, IgniteBiTuple::get2));

                if (type == ExecutorType.THIN_VIA_CACHE_API || type == ExecutorType.THIN_VIA_QUERY)
                    thinCli.cache(users(mode)).putAll(data);
                else
                    node(type).cache(users(mode)).putAll(data);
            }
            else {
                for (IgniteBiTuple<Integer, User> data : entries) {
                    if (type == ExecutorType.THIN_VIA_CACHE_API || type == ExecutorType.THIN_VIA_QUERY)
                        thinCli.cache(users(mode)).put(data.get1(), data.get2());
                    else
                        node(type).cache(users(mode)).put(data.get1(), data.get2());
                }
            }
        }
        else if (modify == ENTRY_PROCESSOR) {
            if (multi) {
                Set<Integer> keys = Arrays.stream(entries).map(IgniteBiTuple::get1).collect(Collectors.toSet());
                Map<Integer, User> data = Arrays.stream(entries).collect(Collectors.toMap(IgniteBiTuple::get1, IgniteBiTuple::get2));

                if (type == ExecutorType.THIN_VIA_QUERY || type == ExecutorType.THIN_VIA_CACHE_API)
                    thinCli.cache(users(mode)).invokeAll(keys, new UpdateEntryProcessor<>(update), data);
                else
                    node(type).cache(users(mode)).invokeAll(keys, new UpdateEntryProcessor<>(update), data);
            }
            else {
                for (IgniteBiTuple<Integer, User> data : entries) {
                    if (type == ExecutorType.THIN_VIA_QUERY || type == ExecutorType.THIN_VIA_CACHE_API)
                        thinCli.cache(users(mode)).invoke(data.get1(), new UpdateEntryProcessor<>(update), data.get2());
                    else
                        node(type).cache(users(mode)).invoke(data.get1(), new UpdateEntryProcessor<>(update), data.get2());
                }
            }
        }
        else
            fail("Unknown insert: " + modify);
    }

    /** */
    protected void update(ExecutorType type, ModifyApi modify, boolean multi, CacheMode mode, IgniteBiTuple<Integer, User>...entries) {
        if (modify == CACHE || modify == ENTRY_PROCESSOR)
            doInsert(true, type, modify, multi, mode, entries);
        else
            fail("Unknown update: " + modify);
    }

    /** */
    protected void delete(ExecutorType type, ModifyApi modify, boolean multi, CacheMode mode, int... keys) {
        if (modify == CACHE) {
            if (multi) {
                Set<Integer> toRemove = Arrays.stream(keys).boxed().collect(Collectors.toSet());

                if (type == ExecutorType.THIN_VIA_CACHE_API || type == ExecutorType.THIN_VIA_QUERY)
                    thinCli.cache(users(mode)).removeAll(toRemove);
                else
                    node(type).cache(users(mode)).removeAll(toRemove);
            }
            else {
                for (int id : keys) {
                    if (type == ExecutorType.THIN_VIA_CACHE_API || type == ExecutorType.THIN_VIA_QUERY)
                        thinCli.cache(users(mode)).remove(id);
                    else
                        node(type).cache(users(mode)).remove(id);
                }
            }
        }
        else if (modify == ENTRY_PROCESSOR) {
            if (multi) {
                Set<Integer> toRemove = Arrays.stream(keys).boxed().collect(Collectors.toSet());

                if (type == ExecutorType.THIN_VIA_QUERY || type == ExecutorType.THIN_VIA_CACHE_API)
                    thinCli.cache(users(mode)).invokeAll(toRemove, new RemoveEntryProcessor<>());
                else
                    node(type).cache(users(mode)).invokeAll(toRemove, new RemoveEntryProcessor<>());
            }
            else {
                for (int id : keys) {
                    if (type == ExecutorType.THIN_VIA_QUERY || type == ExecutorType.THIN_VIA_CACHE_API)
                        thinCli.cache(users(mode)).invoke(id, new RemoveEntryProcessor<>());
                    else
                        node(type).cache(users(mode)).invoke(id, new RemoveEntryProcessor<>());
                }
            }
        }
        else
            fail("Unknown delete: " + modify);
    }

    /** */
    protected static IgniteEx node(ExecutorType type) {
        assertTrue(type == ExecutorType.CLIENT || type == ExecutorType.SERVER);

        return type == ExecutorType.CLIENT ? cli : srv;
    }

    /** */
    protected static String users(CacheMode mode) {
        return tableName(USERS, mode);
    }

    /** */
    protected static String tableName(String tbl, CacheMode mode) {
        return tbl + "_" + mode;
    }

    /** */
    protected void ensureModeSupported() {
        // No-op.
    }

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
        QUERY
    }

    /** */
    public static class User {
        /** */
        @GridToStringInclude
        public final int userId;

        /** */
        @GridToStringInclude
        public final int departmentId;

        /** */
        @GridToStringInclude
        public final String fio;

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
