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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;

/**
 * Integration tests for {@code SELECT ... FOR UPDATE} syntax.
 */
public class SelectForUpdateIntegrationTest extends GridCommonAbstractTest {
    /** */
    private static Ignite ignite0;
    private static Ignite ignite1;
    private static Ignite client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setTransactionConfiguration(new TransactionConfiguration()
                .setTxAwareQueriesEnabled(true))
            .setSqlConfiguration(new SqlConfiguration()
                .setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration()
                    .setDefault(true)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite0 = startGridsMultiThreaded(3);
        ignite1 = grid(1);
        client = startClientGrid();

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        ignite0 = null;
        ignite1 = null;
        client = null;

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        sql("CREATE TABLE Person (id INT PRIMARY KEY, name VARCHAR, age INT, deptId INT, managerId INT) " +
            "WITH atomicity=TRANSACTIONAL");
        sql("INSERT INTO Person (id, name, age) VALUES " +
            "(1, 'Alice', 20), (2, 'Bob', 21), (3, 'Ann', 22), (4, 'Bill', 23)" +
            ", (5, 'Alex', 24), (6, 'Ben', 25), (7, 'Cathy', 26), (8, 'Carl', 27), (9, 'Diana', 28)" +
            ", (10, 'David', 29), (11, 'Eva', 30), (12, 'Evan', 31), (13, 'Fiona', 32), (14, 'Frank', 33)" +
            ", (15, 'Grace', 34), (16, 'George', 35), (17, 'Hannah', 36), (18, 'Harry', 37), (19, 'Ivy', 38)" +
            ", (20, 'Ian', 39), (21, 'Jack', 40), (22, 'Jill', 41), (23, 'Karen', 42), (24, 'Kyle', 43)" +
            ", (25, 'Laura', 44), (26, 'Leo', 45), (27, 'Mia', 46), (28, 'Mike', 47), (29, 'Nina', 48)" +
            ", (30, 'Nick', 49)");
        sql("UPDATE Person SET deptId = 1, managerId = 2 WHERE id = 1");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        sql("DROP TABLE IF EXISTS Dept");
        sql("DROP TABLE IF EXISTS Person");

        super.afterTest();
    }

    /** SELECT FOR UPDATE without OF locks rows of every table participating in a JOIN. */
    @Test
    public void testSelectForUpdateJoinLocksAllTables() throws Exception {
        createDeptTable();

        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        IgniteInternalFuture<?> lockFut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                assertRows(
                    sql("SELECT p.id FROM Person p JOIN Dept d ON p.deptId = d.id WHERE p.id = 1 FOR UPDATE"),
                    Arrays.asList(1)
                );

                locked.countDown();

                assertTrue("Timed out waiting to release JOIN locks", release.await(30, TimeUnit.SECONDS));

                tx.commit();
            }
        });

        try {
            assertTrue("JOIN transaction did not acquire locks in time", locked.await(10, TimeUnit.SECONDS));

            assertTableRowLocked(ignite1, "Person", 1);
            assertTableRowLocked(ignite1, "Dept", 1);
        }
        finally {
            release.countDown();
        }

        lockFut.get(10_000);
    }

    /** FOR UPDATE OF locks only the table owning the specified JOIN column. */
    @Test
    public void testSelectForUpdateJoinOfLocksSelectedTable() throws Exception {
        createDeptTable();

        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        IgniteInternalFuture<?> lockFut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                assertRows(
                    sql("SELECT p.id FROM Person p JOIN Dept d ON p.deptId = d.id WHERE p.id = 1 FOR UPDATE OF p.id"),
                    Arrays.asList(1)
                );

                locked.countDown();

                assertTrue("Timed out waiting to release JOIN locks", release.await(30, TimeUnit.SECONDS));

                tx.commit();
            }
        });

        try {
            assertTrue("JOIN transaction did not acquire locks in time", locked.await(10, TimeUnit.SECONDS));

            assertTableRowLocked(ignite1, "Person", 1);

            try (Transaction tx = ignite1.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                assertRows(sql(ignite1, "SELECT * FROM Dept WHERE id = 1 FOR UPDATE NOWAIT"),
                    Arrays.asList(1, "Engineering"));

                tx.commit();
            }
        }
        finally {
            release.countDown();
        }

        lockFut.get(10_000);
    }

    /** A qualified OF column selects a particular table occurrence in a self-join. */
    @Test
    public void testSelectForUpdateSelfJoinOfUsesAlias() throws Exception {
        assertSelfJoinOfLocks("employee", 1, 2);
        assertSelfJoinOfLocks("manager", 2, 1);
    }

    /** Creates a second transactional table used by JOIN tests. */
    private void createDeptTable() {
        sql("CREATE TABLE Dept (id INT PRIMARY KEY, name VARCHAR) WITH atomicity=TRANSACTIONAL");
        sql("INSERT INTO Dept VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'HR')");
    }

    /** Verifies that OF resolves an alias to the correct side of a self-join. */
    private void assertSelfJoinOfLocks(String alias, int lockedId, int unlockedId) throws Exception {
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        IgniteInternalFuture<?> lockFut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                assertRows(sql("SELECT employee.id, manager.id FROM Person employee " +
                    "JOIN Person manager ON employee.managerId = manager.id " +
                    "WHERE employee.id = 1 FOR UPDATE OF " + alias + ".id"), Arrays.asList(1, 2));

                locked.countDown();

                assertTrue("Timed out waiting to release self-join lock", release.await(30, TimeUnit.SECONDS));

                tx.commit();
            }
        });

        try {
            assertTrue("Self-join transaction did not acquire lock in time", locked.await(10, TimeUnit.SECONDS));

            assertTableRowLocked(ignite1, "Person", lockedId);
            assertTableRowUnlocked(ignite1, "Person", unlockedId);
        }
        finally {
            release.countDown();
        }

        lockFut.get(10_000);
    }

    /** FOR UPDATE without an active transaction produces "requires an active PESSIMISTIC transaction". */
    @Test
    public void testSelectForUpdateOutsideTransaction() {
        GridTestUtils.assertThrowsAnyCause(log, () -> sql("SELECT id FROM Person FOR UPDATE"),
            IgniteSQLException.class, "SELECT FOR UPDATE requires an active PESSIMISTIC transaction");
    }

    /** SELECT FOR UPDATE inside an OPTIMISTIC transaction throws an appropriate error. */
    @Test
    public void testSelectForUpdateInOptimisticTransaction() {
        try (Transaction tx = ignite0.transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
            GridTestUtils.assertThrowsAnyCause(log, () -> sql("SELECT * FROM Person WHERE id = 1 FOR UPDATE"),
                IgniteSQLException.class, "PESSIMISTIC");
        }
    }

    /** FOR UPDATE OF columns is supported. */
    @Test
    public void forUpdateOfColumn() {
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            List<List<?>> rows = sql("SELECT id FROM Person FOR UPDATE OF id");

            assertEquals(30, rows.size());
        }
    }

    /** FOR UPDATE WAIT n seconds is supported. */
    @Test
    public void forUpdateWait() {
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            List<List<?>> rows = sql("SELECT id FROM Person FOR UPDATE WAIT 5");

            assertEquals(30, rows.size());
        }
    }

    /** FOR UPDATE NOWAIT is supported. */
    @Test
    public void forUpdateNowait() {
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            List<List<?>> rows = sql("SELECT id FROM Person FOR UPDATE NOWAIT");

            assertEquals(30, rows.size());
        }
    }

    /** FOR UPDATE with WHERE is supported. */
    @Test
    public void forUpdateWithWhere() {
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            List<List<?>> rows = sql("SELECT id FROM Person WHERE age >= 30 FOR UPDATE");

            assertEquals(20, rows.size());
        }
    }

    /** Basic SELECT FOR UPDATE returns the correct rows and commits successfully. */
    @Test
    public void testSelectForUpdateByPrimaryKey() {
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertRows(sql("SELECT * FROM Person WHERE id = 2 FOR UPDATE"),
                Arrays.asList(2, "Bob", 21, null, null));

            tx.commit();
        }
    }

    /** SELECT FOR UPDATE with no matching rows returns an empty result set. */
    @Test
    public void testSelectForUpdateNoRows() {
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            List<List<?>> rows = sql("SELECT * FROM Person WHERE id = 9999 FOR UPDATE");

            assertEquals(0, rows.size());

            tx.commit();
        }
    }

    /** SELECT FOR UPDATE within the same transaction allows repeated locks on the same key. */
    @Test
    public void testSelectForUpdateRepeatedInSameTx() {
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertRows(sql("SELECT * FROM Person WHERE id = 2 FOR UPDATE"),
                Arrays.asList(2, "Bob", 21, null, null));

            // Second FOR UPDATE on the same key in the same transaction should succeed.
            assertRows(sql("SELECT * FROM Person WHERE id = 2 FOR UPDATE"),
                Arrays.asList(2, "Bob", 21, null, null));

            tx.commit();
        }
    }

    /**
     * SELECT FOR UPDATE acquires a pessimistic lock: a second transaction trying to update the
     * same row with NOWAIT immediately fails.
     */
    @Test
    public void testSelectForUpdateBlocksOtherTransaction() throws Exception {
        CountDownLatch tx1Locked = new CountDownLatch(1);
        CountDownLatch tx1Done = new CountDownLatch(1);

        // Transaction 1: acquire lock on row id=5 and hold it.
        IgniteInternalFuture<?> tx1 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                sql("SELECT * FROM Person WHERE id = 5 FOR UPDATE");

                tx1Locked.countDown();    // Signal that lock is held.

                tx1Done.await(10, TimeUnit.SECONDS);  // Wait for tx2 to finish.

                tx.commit();
            }
        });

        // Wait until tx1 holds the lock.
        assertTrue("tx1 did not acquire lock in time", tx1Locked.await(10, TimeUnit.SECONDS));

        // Transaction 2: try to lock the same row with NOWAIT – must fail.
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            GridTestUtils.assertThrowsAnyCause(log, () -> sql("SELECT * FROM Person WHERE id = 5 FOR UPDATE NOWAIT"),
                IgniteSQLException.class, "could not acquire lock");

            tx.rollback();
        }
        finally {
            tx1Done.countDown();
        }

        tx1.get(10_000);
    }

    /** SELECT FOR UPDATE executed on a client node blocks lock attempts from all server nodes. */
    @Test
    public void testSelectForUpdateFromClientBlocksServerNodes() throws Exception {
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        IgniteInternalFuture<?> lockFut = lockRow(client, 7, locked, release);

        try {
            assertTrue("Client transaction did not acquire lock in time", locked.await(10, TimeUnit.SECONDS));

            assertRowLocked(ignite0, 7);
            assertRowLocked(ignite1, 7);
        }
        finally {
            release.countDown();
        }

        lockFut.get(10_000);
    }

    /** SELECT FOR UPDATE executed on a server node blocks lock attempts from client and server nodes. */
    @Test
    public void testSelectForUpdateFromServerBlocksClientAndServerNodes() throws Exception {
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        IgniteInternalFuture<?> lockFut = lockRow(ignite1, 8, locked, release);

        try {
            assertTrue("Server transaction did not acquire lock in time", locked.await(10, TimeUnit.SECONDS));

            assertRowLocked(client, 8);
            assertRowLocked(ignite0, 8);
        }
        finally {
            release.countDown();
        }

        lockFut.get(10_000);
    }

    /** A failed NOWAIT lock attempt does not invalidate the transaction. */
    @Test
    public void testTransactionRemainsActiveAfterLockFailure() throws Exception {
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        IgniteInternalFuture<?> lockFut = lockRow(ignite0, 9, locked, release);

        try {
            assertTrue("Transaction did not acquire lock in time", locked.await(10, TimeUnit.SECONDS));

            try (Transaction tx = ignite1.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                GridTestUtils.assertThrowsAnyCause(log,
                    () -> sql(ignite1, "SELECT * FROM Person WHERE id = 9 FOR UPDATE NOWAIT"),
                    IgniteSQLException.class,
                    "could not acquire lock");

                assertEquals(ACTIVE, tx.state());

                release.countDown();

                sql(ignite1, "SELECT * FROM Person WHERE id = 9 FOR UPDATE WAIT 5");

                assertEquals(1L, sql(ignite1, "UPDATE Person SET age = 100 WHERE id = 9").get(0).get(0));

                assertEquals(ACTIVE, tx.state());

                tx.commit();
            }
        }
        finally {
            release.countDown();
        }

        lockFut.get(10_000);

        assertRows(sql("SELECT id, age FROM Person WHERE id = 9"), Arrays.asList(9, 100));
    }

    /** Acquires a row lock on the specified node and holds it until the release latch is opened. */
    private IgniteInternalFuture<?> lockRow(
        Ignite ignite,
        int id,
        CountDownLatch locked,
        CountDownLatch release
    ) {
        return GridTestUtils.runAsync(() -> {
            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                sql(ignite, "SELECT * FROM Person WHERE id = ? FOR UPDATE", id);

                locked.countDown();

                assertTrue("Timed out waiting to release row lock", release.await(30, TimeUnit.SECONDS));

                tx.commit();
            }
        });
    }

    /** Verifies that a transaction on the specified node cannot acquire the row lock. */
    private void assertRowLocked(Ignite ignite, int id) {
        assertTableRowLocked(ignite, "Person", id);
    }

    /** Verifies that a transaction cannot acquire a row lock in the specified table. */
    private void assertTableRowLocked(Ignite ignite, String tableName, int id) {
        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            GridTestUtils.assertThrowsAnyCause(log,
                () -> sql(ignite, "SELECT * FROM " + tableName + " WHERE id = ? FOR UPDATE NOWAIT", id),
                IgniteSQLException.class,
                "could not acquire lock");

            tx.rollback();
        }
    }

    /** Verifies that a transaction can acquire the row lock in the specified table. */
    private void assertTableRowUnlocked(Ignite ignite, String tableName, int id) {
        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertRows(sql(ignite, "SELECT id FROM " + tableName + " WHERE id = ? FOR UPDATE NOWAIT", id),
                Arrays.asList(id));

            tx.commit();
        }
    }

    /** */
    private List<List<?>> sql(String sql, Object... args) {
        return sql(ignite0, sql, args);
    }

    /** */
    private List<List<?>> sql(Ignite ignite,  String sql, Object... args) {
        return ((IgniteEx)ignite).context().query().querySqlFields(
            new SqlFieldsQuery(sql).setSchema("PUBLIC").setArgs(args), true).getAll();
    }

    /** */
    private void assertRows(List<List<?>> rows, List<?>... expRows) {
        assertEquals(expRows.length, rows.size());
        assertEquals(new HashSet<>(Arrays.asList(expRows)), new HashSet<>(rows));
    }
}
