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
package org.apache.ignite.internal.processors.cache.mvcc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.connect;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
@RunWith(Parameterized.class)
public class CacheMvccSelectForUpdateQueryBasicTest extends CacheMvccAbstractTest {
    /** */
    private static final int CACHE_SIZE = 100;

    /** */
    private static Random RAND = new Random();

    /** */
    @Parameterized.Parameter(0)
    public CacheMode cacheMode;

    /** */
    @Parameterized.Parameter(1)
    public int backups;

    /** */
    @Parameterized.Parameter(2)
    public boolean fromClient;

    /** */
    @Parameterized.Parameter(3)
    public boolean segmented;

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "cacheMode={0}, backups={1}, fromClient={2}, segmented={3}")
    public static Collection parameters() {
        return Arrays.asList(new Object[][] {
            // cacheMode, backups, from client, segmented
            {REPLICATED, 0, true, false},
            {REPLICATED, 0, false, false},
            {PARTITIONED, 0, true, false},
            {PARTITIONED, 0, false, true},
            {PARTITIONED, 1, true, true},
            {PARTITIONED, 1, false, false},
            {PARTITIONED, 2, true, false},
        });
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        startGridsMultiThreaded(3);

        client = true;

        startGrid(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        super.afterTest(); // Check mvcc state and stop nodes
    }

    /** {@inheritDoc} */
    @Override public void afterTest() throws Exception {
        verifyOldVersionsCleaned();

        verifyCoordinatorInternalState();

        grid(3).destroyCache("SQL_PUBLIC_PERSON");
    }

    /** {@inheritDoc} */
    @Override public void beforeTest() throws Exception {
        Ignite client = grid(3);

        CacheConfiguration<Object, Object> dummyCfg = new CacheConfiguration<>("dummy")
            .setSqlSchema("PUBLIC").setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        // Create dummy cache as entry point.
        client.getOrCreateCache(dummyCfg);

        String templateName = String.valueOf(cacheMode) + backups + fromClient + segmented;

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(templateName);

        ccfg.setCacheMode(cacheMode);
        ccfg.setBackups(backups);

        if (segmented)
            ccfg.setQueryParallelism(4);

        client.addCacheConfiguration(ccfg);

        // Create MVCC table and cache.
        runSql(client, "CREATE TABLE person (id INT PRIMARY KEY, name VARCHAR, salary INT) " +
            "WITH \"ATOMICITY=TRANSACTIONAL_SNAPSHOT, TEMPLATE=" + templateName + "\"", false);

        runSql(client, "CREATE INDEX salaryIdx ON person(salary)", false);

        // Populate MVCC cache. Salaries 0, 10, 20, 30,..., 990.
        for (int i = 0; i < CACHE_SIZE; i++)
            runSql(client, "INSERT INTO person (id, name, salary) VALUES ("
                + i + ", 'name" + i + "', " + i * 10 + ")", false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSingleLock() throws Exception {
        Ignite node = getNode();

        String sql = "SELECT id, name FROM person WHERE salary = 100";
        String sqlForUpdate = sql + " FOR UPDATE";

        // Check SELECT FOR UPDATE.
        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            List<List<?>> res = runSql(node, sqlForUpdate, false).getAll();

            assertEquals(1, res.size());

            List<Integer> keys = new ArrayList<>();

            for (List<?> r : res) {
                assertEquals(2, r.size());

                int key = (Integer)r.get(0);

                assertEquals(10, key);

                keys.add(key);
            }

            checkLocks(keys);

            tx.commit();
        }

        checkLocks(Collections.emptyList());

        // Check cached statement works in the same way.
        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            List<List<?>> res = runSql(node, sqlForUpdate, false).getAll();

            assertEquals(1, res.size());

            List<Integer> keys = new ArrayList<>();

            for (List<?> r : res) {
                assertEquals(2, r.size());

                int key = (Integer)r.get(0);

                assertEquals(10, key);

                keys.add(key);
            }

            checkLocks(keys);

            tx.rollback();

            checkLocks(Collections.emptyList());
        }

        // Check cached statement doesn't lock keys without tx.
        {
            List<List<?>> res = runSql(node, sqlForUpdate, false).getAll();

            assertEquals(1, res.size());

            List<Integer> keys = new ArrayList<>();

            for (List<?> r : res) {
                assertEquals(2, r.size());

                int key = (Integer)r.get(0);

                assertEquals(10, key);

                keys.add(key);
            }

            checkLocks(Collections.emptyList());
        }

        // Check cached statement locks keys in tx.
        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            List<List<?>> res = runSql(node, sqlForUpdate, false).getAll();

            assertEquals(1, res.size());

            List<Integer> keys = new ArrayList<>();

            for (List<?> r : res) {
                assertEquals(2, r.size());

                int key = (Integer)r.get(0);

                assertEquals(10, key);

                keys.add(key);
            }

            // Run dummy DML.
            runSql(node, "UPDATE Person SET name='test' WHERE id=" + keys.get(0), false).getAll();

            checkLocks(keys);

            tx.commit();
        }

        checkLocks(Collections.emptyList());

        // Check SFU and non-SFU selects are cached separately.
        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            List<List<?>> res = runSql(node, sql, false).getAll();

            assertEquals(1, res.size());

            for (List<?> r : res)
                assertEquals(2, r.size());

            checkLocks(Collections.emptyList());

            tx.commit();
        }

        // Check SFU and non-SFU selects are cached separately.
        {
            List<List<?>> res = runSql(node, sql, false).getAll();

            assertEquals(1, res.size());

            for (List<?> r : res)
                assertEquals(2, r.size());

            checkLocks(Collections.emptyList());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSelectForUpdateLocal() throws Exception {
        checkSelectForUpdate(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSelectForUpdateDistributed() throws Exception {
        checkSelectForUpdate(false);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkSelectForUpdate(boolean loc) throws Exception {
        Ignite node = loc ? grid(0) : getNode();

        String sql = "SELECT name, id  FROM person WHERE MOD(salary, 3) = 0 ORDER BY id";
        String sqlForUpdate = sql + " FOR UPDATE";

        // Check statement doesn't lock keys without tx.
        {
            List<List<?>> res = runSql(node, sqlForUpdate, loc).getAll();

            // Every third row should be fetched in distributed case.
            assertTrue(loc ? !res.isEmpty() : res.size() >= CACHE_SIZE / 4 && res.size() <= CACHE_SIZE / 2);

            List<Integer> keys = new ArrayList<>();

            for (List<?> r : res) {
                assertEquals(2, r.size());

                keys.add((Integer)r.get(1));
            }

            checkLocks(Collections.emptyList());
        }

        // Check SELECT FOR UPDATE.
        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            List<List<?>> res = runSql(node, sqlForUpdate, loc).getAll();

            // Every third row should be fetched in distributed case.
            assertTrue(loc ? !res.isEmpty() : res.size() >= CACHE_SIZE / 4 && res.size() <= CACHE_SIZE / 2);

            List<Integer> keys = new ArrayList<>();

            for (List<?> r : res) {
                assertEquals(2, r.size());

                keys.add((Integer)r.get(1));
            }

            // Run dummy DML.
            runSql(node, "UPDATE Person SET name='test' WHERE id=" + keys.get(0), loc).getAll();

            checkLocks(keys);

            tx.rollback();

            checkLocks(Collections.emptyList());
        }

        // Check cached statement doesn't lock keys without tx.
        {
            List<List<?>> res = runSql(node, sqlForUpdate, loc).getAll();

            // Every third row should be fetched in distributed case.
            assertTrue(loc ? !res.isEmpty() : res.size() >= CACHE_SIZE / 4 && res.size() <= CACHE_SIZE / 2);

            List<Integer> keys = new ArrayList<>();

            for (List<?> r : res) {
                assertEquals(2, r.size());

                keys.add((Integer)r.get(1));
            }

            checkLocks(Collections.emptyList());
        }

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            List<List<?>> res = runSql(node, sqlForUpdate, loc).getAll();

            // Every third row should be fetched in distributed case.
            assertTrue(loc ? !res.isEmpty() : res.size() >= CACHE_SIZE / 4 && res.size() <= CACHE_SIZE / 2);

            List<Integer> keys = new ArrayList<>();

            for (List<?> r : res) {
                assertEquals(2, r.size());

                keys.add((Integer)r.get(1));
            }

            // Run dummy DML.
            runSql(node, "UPDATE Person SET name='test' WHERE id=" + keys.get(0), loc).getAll();

            checkLocks(keys);

            tx.rollback();

            checkLocks(Collections.emptyList());
        }

        // Check SFU and non-SFU selects are cached separately.
        {
            List<List<?>> res = runSql(node, sql, loc).getAll();

            // Every third row should be fetched in distributed case.
            assertTrue(loc ? !res.isEmpty() : res.size() >= CACHE_SIZE / 4 && res.size() <= CACHE_SIZE / 2);

            for (List<?> r : res)
                assertEquals(2, r.size());

            checkLocks(Collections.emptyList());
        }

        // Check SFU and non-SFU selects are cached separately.
        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            List<List<?>> res = runSql(node, sql, loc).getAll();

            // Every third row should be fetched in distributed case.
            assertTrue(loc ? !res.isEmpty() : res.size() >= CACHE_SIZE / 4 && res.size() <= CACHE_SIZE / 2);

            List<Integer> keys = new ArrayList<>();

            for (List<?> r : res) {
                assertEquals(2, r.size());

                keys.add((Integer)r.get(1));
            }

            checkLocks(Collections.emptyList());

            tx.rollback();
        }

        checkLocks(Collections.emptyList());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJdbcAutoCommitFalseStartsTx() throws Exception {
        Ignite node = getNode();

        try (Connection c = connect((IgniteEx)node)) {
            c.setAutoCommit(false);

            String sql = "SELECT name, id  FROM person WHERE MOD(salary, 3) = 0 ORDER BY id";
            String sqlForUpdate = sql + " FOR UPDATE";

            List<Integer> keys = runJdbcSql(c, sqlForUpdate, 2, 2);

            assertTrue(keys.size() >= CACHE_SIZE / 4 && keys.size() <= CACHE_SIZE / 2);

            checkLocks(keys);

            runJdbcSql(c, sql, 2, 2);

            checkLocks(keys);

            runJdbcSql(c, "COMMIT", -1, -1);

            checkLocks(Collections.emptyList());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJdbcAutoCommitTrueKeysLocked() throws Exception {
        Ignite node = getNode();

        try (Connection c = connect((IgniteEx)node)) {
            c.setAutoCommit(true);

            runJdbcSql(c, "BEGIN", -1, -1);

            String sql = "SELECT name, id  FROM person WHERE MOD(salary, 3) = 0 ORDER BY id";
            String sqlForUpdate = sql + " FOR UPDATE";

            List<Integer> keys = runJdbcSql(c, sqlForUpdate, 2, 2);

            assertTrue(keys.size() >= CACHE_SIZE / 4 && keys.size() <= CACHE_SIZE / 2);

            checkLocks(keys);

            runJdbcSql(c, sql, 2, 2);

            checkLocks(keys);

            runJdbcSql(c, "COMMIT", -1, -1);

            checkLocks(Collections.emptyList());

            // Should not be locked because we are out of tx.
            runJdbcSql(c, sqlForUpdate, 2, 2);

            checkLocks(Collections.emptyList());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSelectForUpdateLocalWithArgs() throws Exception {
        checkSelectForUpdateWithArgs(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSelectForUpdateDistributedWithArgs() throws Exception {
        checkSelectForUpdateWithArgs(false);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkSelectForUpdateWithArgs(boolean loc) throws Exception {
        Ignite node = loc ? grid(0) : getNode();

        String sql = "SELECT name, id  FROM person WHERE salary >= ? AND salary < ? ORDER BY id";
        String sqlForUpdate = sql + " FOR UPDATE";

        // Check cached statement doesn't lock keys without tx.
        {
            List<List<?>> res = runSql(node, sqlForUpdate, loc, 0, 200).getAll();

            // Every third row should be fetched in distributed case.
            assertTrue(loc ? !res.isEmpty() : res.size() == 20);

            List<Integer> keys = new ArrayList<>();

            for (List<?> r : res) {
                assertEquals(2, r.size());

                keys.add((Integer)r.get(1));
            }

            checkLocks(Collections.emptyList());
        }

        // Check SELECT FOR UPDATE.
        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            List<List<?>> res = runSql(node, sqlForUpdate, loc, 0, 200).getAll();

            // Every third row should be fetched in distributed case.
            assertTrue(loc ? !res.isEmpty() : res.size() == 20);

            List<Integer> keys = new ArrayList<>();

            for (List<?> r : res) {
                assertEquals(2, r.size());

                keys.add((Integer)r.get(1));
            }

            // Run dummy DML.
            runSql(node, "UPDATE Person SET name='test' WHERE id=" + keys.get(0), loc).getAll();

            checkLocks(keys);

            tx.rollback();

            checkLocks(Collections.emptyList());
        }

        // Check cached statement doesn't lock keys without tx.
        {
            List<List<?>> res = runSql(node, sqlForUpdate, loc, 0, 200).getAll();

            // Every third row should be fetched in distributed case.
            assertTrue(loc ? !res.isEmpty() : res.size() == 20);

            List<Integer> keys = new ArrayList<>();

            for (List<?> r : res) {
                assertEquals(2, r.size());

                keys.add((Integer)r.get(1));
            }

            checkLocks(Collections.emptyList());
        }

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            List<List<?>> res = runSql(node, sqlForUpdate, loc, 0, 200).getAll();

            // Every third row should be fetched in distributed case.
            assertTrue(loc ? !res.isEmpty() : res.size() == 20);

            List<Integer> keys = new ArrayList<>();

            for (List<?> r : res) {
                assertEquals(2, r.size());

                keys.add((Integer)r.get(1));
            }

            // Run dummy DML.
            runSql(node, "UPDATE Person SET name='test' WHERE id=" + keys.get(0), loc).getAll();

            checkLocks(keys);

            tx.rollback();

            checkLocks(Collections.emptyList());
        }

        // Check SFU and non-SFU selects are cached separately.
        {
            List<List<?>> res = runSql(node, sql, loc, 0, 200).getAll();

            // Every third row should be fetched in distributed case.
            assertTrue(loc ? !res.isEmpty() : res.size() == 20);

            for (List<?> r : res)
                assertEquals(2, r.size());

            checkLocks(Collections.emptyList());
        }

        // Check SFU and non-SFU selects are cached separately.
        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            List<List<?>> res = runSql(node, sql, loc, 0, 200).getAll();

            // Every third row should be fetched in distributed case.
            assertTrue(loc ? !res.isEmpty() : res.size() == 20);

            List<Integer> keys = new ArrayList<>();

            for (List<?> r : res) {
                assertEquals(2, r.size());

                keys.add((Integer)r.get(1));
            }

            checkLocks(Collections.emptyList());

            tx.rollback();
        }

        checkLocks(Collections.emptyList());
    }

    /**
     * Check that an attempt to get a lock on any key from given list fails by timeout.
     *
     * @param lockedKeys Keys to check.
     * @throws Exception if failed.
     */
    @SuppressWarnings({"unchecked"})
    private void checkLocks(List<Integer> lockedKeys) throws Exception {
        List<Integer> allKeys = IntStream.range(0, CACHE_SIZE).boxed().collect(Collectors.toList());
        List<Integer> nonLockedKeys = new ArrayList<>(allKeys);

        nonLockedKeys.removeAll(lockedKeys);

        List<Ignite> nodes = Ignition.allGrids();

        Ignite node = nodes.get(RAND.nextInt(nodes.size()));

        List<T2<Integer, IgniteInternalFuture>> calls = new ArrayList<>();

        for (int key : allKeys) {
            calls.add(new T2<>(key, GridTestUtils.runAsync(new Callable<Void>() {
                /** {@inheritDoc} */
                @Override public Void call() {
                    try (Transaction tx = node.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
                        TransactionIsolation.REPEATABLE_READ)) {

                        // TODO uncomment to reproduce "https://issues.apache.org/jira/browse/IGNITE-11349"
//                        if (RAND.nextBoolean()) {
//                            node.cache("dummy").query(new SqlFieldsQuery("update person" +
//                                " set name='check' where id = " + key)
//                                .setTimeout(1, TimeUnit.SECONDS))
//                                .getAll();
//                        }
//                        else {
                            node.cache("dummy").query(new SqlFieldsQuery("SELECT * FROM person WHERE id=" + key +
                                " FOR UPDATE")
                                .setTimeout(1, TimeUnit.SECONDS))
                                .getAll();
//                        }

                        tx.rollback();

                        return null;
                    }
                }
            })));
        }

        for (T2<Integer, IgniteInternalFuture> pair : calls) {
            if (nonLockedKeys.contains(pair.getKey())) {
                try {
                    pair.getValue().get(TX_TIMEOUT);
                }
                catch (Exception e) {
                    if (e.getMessage() != null && e.getMessage().contains("Failed to acquire lock within provided timeout"))
                        throw new Exception("Key is locked, though it shouldn't be. Key: " + pair.getKey(), e);

                    throw e;
                }
            }
            else {
                try {
                    pair.getValue().get();

                    fail("Key is not locked: " + pair.getKey());
                }
                catch (Exception e) {
                    CacheException e0 = X.cause(e, CacheException.class);

                    assert e0 != null;

                    assert e0.getMessage() != null &&
                        e0.getMessage().contains("Failed to acquire lock within provided timeout") :
                        X.getFullStackTrace(e);
                }
            }
        }
    }

    /**
     * @return Node to run query from.
     */
    private Ignite getNode() {
        Ignite node = fromClient ? grid(3) : grid(RAND.nextInt(2));

        assert fromClient == node.cluster().localNode().isClient();

        return node;
    }

    /**
     * @param node Node.
     * @param sql Sql string.
     * @return Result.
     */
    private FieldsQueryCursor<List<?>> runSql(Ignite node, String sql, boolean local, Object... args) {
        return node.cache("dummy").query(new SqlFieldsQuery(sql).setLocal(local).setArgs(args));
    }

    /**
     * @param c Connection.
     * @param sql Sql string.
     * @param keyIdx Index of key field in result set.
     * @param colCnt Expected columns count in result set.
     */
    private List<Integer> runJdbcSql(Connection c, String sql, int keyIdx, int colCnt) throws SQLException {
        try (Statement stmt = c.createStatement()) {
            stmt.execute(sql);

            ResultSet rs = stmt.getResultSet();

            if (rs == null) {
                assertEquals(keyIdx, -1);

                return null;
            }

            List<Integer> res = new ArrayList<>();

            while (rs.next()) {
                assertEquals(colCnt, rs.getMetaData().getColumnCount());

                res.add(rs.getInt(keyIdx));
            }

            return res;
        }
    }
}
