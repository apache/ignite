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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
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

import static org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest.ModifyApi.CACHE;
import static org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest.ModifyApi.ENTRY_PROCESSOR;
import static org.apache.ignite.internal.processors.cache.query.AbstractQueryTransactionIsolationTest.ModifyApi.QUERY;
import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.CONN_CTX_META_KEY;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/** */
@RunWith(Parameterized.class)
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
    @Parameterized.Parameter()
    public int gridCnt;

    /** */
    @Parameterized.Parameter(1)
    public int backups;

    /** */
    @Parameterized.Parameter(2)
    public boolean partitionAwareness;

    /** */
    @Parameterized.Parameter(3)
    public CacheMode mode;

    /** */
    @Parameterized.Parameter(4)
    public ExecutorType type;

    /** */
    @Parameterized.Parameter(5)
    public ModifyApi modify;

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

        cli.cacheNames().forEach(name -> cli.cache(name).removeAll());

        insert(F.t(1, JOHN));

        assertEquals(gridCnt + 1, Ignition.allGrids().size());
        assertEquals(partitionAwareness, thinCliCfg.isPartitionAwarenessEnabled());
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
                else if (meta != null)
                    throw new IllegalStateException("Unknown context");
            }
        }
    }

    /** */
    @Test
    public void testInsert() {
        ensureModeSupported();

        Runnable checkBefore = () -> {
            for (int i = 4; i <= (multi ? 6 : 4); i++) {
                if (type != ExecutorType.THIN_JDBC)
                    assertNull(CACHE.name(), select(i, CACHE));
                assertNull(QUERY.name(), select(i, QUERY));
            }
        };

        Runnable checkAfter = () -> {
            for (int i = 4; i <= (multi ? 6 : 4); i++) {
                if (type != ExecutorType.THIN_JDBC)
                    assertEquals(CACHE.name(), JOHN, select(i, CACHE));
                assertEquals(QUERY.name(), JOHN, select(i, QUERY));
            }
        };

        checkBefore.run();

        insideTx(() -> {
            checkBefore.run();

            if (multi)
                insert(F.t(4, JOHN), F.t(5, JOHN), F.t(6, JOHN));
            else
                insert(F.t(4, JOHN));

            if (modify == QUERY) {
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
                assertEquals(JOHN, select(i, QUERY));
            }
        };

        Runnable checkAfter = () -> {
            for (int i = 1; i <= (multi ? 3 : 1); i++) {
                if (type != ExecutorType.THIN_JDBC)
                    assertEquals(KYLE, select(i, CACHE));
                assertEquals(KYLE, select(i, QUERY));
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
                assertEquals(SARAH, select(i, QUERY));
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
                assertEquals(JOHN, select(i, QUERY));
            }
        };

        Runnable checkAfter = () -> {
            for (int i = 1; i <= (multi ? 3 : 1); i++) {
                if (type != ExecutorType.THIN_JDBC)
                    assertNull(select(i, CACHE));
                assertNull(select(i, QUERY));
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
    protected void createCaches() {
        cli.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(mode)
            .setBackups(backups));

        for (CacheMode mode : CacheMode.values()) {
            String users = tableName(USERS, mode);

            LinkedHashMap<String, String> userFlds = new LinkedHashMap<>();

            userFlds.put("id", Integer.class.getName());
            userFlds.put("userId", Integer.class.getName());
            userFlds.put("departmentId", Integer.class.getName());
            userFlds.put("fio", String.class.getName());

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
        }
    }

    /** */
    protected void insideTx(RunnableX test, boolean commit) {
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
        else
            fail("Unknown executor type: " + type);
    }

    /** */
    protected User select(Integer id, ModifyApi api) {
        if (api == CACHE)
            return (type == ExecutorType.THIN_VIA_CACHE_API || type == ExecutorType.THIN_VIA_QUERY)
                ? (User)thinCli.cache(users()).get(id)
                : (User)node().cache(users()).get(id);

        fail("Unknown select: " + api);

        return null;
    }

    /** */
    protected void insert(IgniteBiTuple<Integer, User>... entries) {
        doInsert(false, entries);
    }

    /** */
    protected void doInsert(boolean update, IgniteBiTuple<Integer, User>... entries) {
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
        else
            fail("Unknown insert: " + modify);
    }

    /** */
    protected void update(IgniteBiTuple<Integer, User>...entries) {
        if (modify == CACHE || modify == ENTRY_PROCESSOR)
            doInsert(true, entries);
        else
            fail("Unknown update: " + modify);
    }

    /** */
    protected void delete(int... keys) {
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
        else
            fail("Unknown delete: " + modify);
    }

    /** */
    protected IgniteEx node() {
        assertTrue(type == ExecutorType.CLIENT || type == ExecutorType.SERVER);

        return type == ExecutorType.CLIENT ? cli : srv;
    }

    /** */
    protected String users() {
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
        public final int userId;

        /** */
        public final int departmentId;

        /** */
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
