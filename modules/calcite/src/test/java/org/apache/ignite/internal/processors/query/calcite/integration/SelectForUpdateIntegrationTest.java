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

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.message.QueryBatchMessage;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.integration.AbstractBasicIntegrationTransactionalTest.SqlTransactionMode.ALL;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;

/**
 * Integration tests for {@code SELECT ... FOR UPDATE} syntax.
 */
public class SelectForUpdateIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    private static IgniteEx ignite0;

    /** */
    private static IgniteEx ignite1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setTransactionConfiguration(new TransactionConfiguration()
                .setTxAwareQueriesEnabled(true))
            .setCommunicationSpi(new TestRecordingCommunicationSpi());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite0 = grid(0);
        ignite1 = grid(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        ignite0 = null;
        ignite1 = null;

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected QueryChecker assertQuery(Ignite ignite, String qry) {
        Transaction tx = ignite.transactions().tx();
        QueryChecker checker;

        if (tx == null)
            checker = super.assertQuery(ignite, qry);
        else {
            checker = new QueryChecker(qry, tx, ALL) {
                @Override public void check() {
                    tx.suspend();

                    try {
                        super.check();
                    }
                    finally {
                        tx.resume();
                    }
                }

                @Override protected QueryEngine getEngine() {
                    return Commons.lookupComponent(((IgniteEx)ignite).context(), QueryEngine.class);
                }
            };
        }

        return checker;
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
                assertQuery(ignite0,
                    "SELECT p.id FROM Person p JOIN Dept d ON p.deptId = d.id WHERE p.id = 1 FOR UPDATE")
                    .returns(1)
                    .check();

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
                assertQuery(ignite0,
                    "SELECT p.id FROM Person p JOIN Dept d ON p.deptId = d.id WHERE p.id = 1 FOR UPDATE OF p.id")
                    .returns(1)
                    .check();

                locked.countDown();

                assertTrue("Timed out waiting to release JOIN locks", release.await(30, TimeUnit.SECONDS));

                tx.commit();
            }
        });

        try {
            assertTrue("JOIN transaction did not acquire locks in time", locked.await(10, TimeUnit.SECONDS));

            assertTableRowLocked(ignite1, "Person", 1);

            try (Transaction tx = ignite1.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                assertQuery(ignite1, "SELECT * FROM Dept WHERE id = 1 FOR UPDATE NOWAIT")
                    .returns(1, "Engineering")
                    .check();

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
                assertQuery(ignite0, "SELECT employee.id, manager.id FROM Person employee " +
                    "JOIN Person manager ON employee.managerId = manager.id " +
                    "WHERE employee.id = 1 FOR UPDATE OF " + alias + ".id")
                    .returns(1, 2)
                    .check();

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
        assertThrows("SELECT id FROM Person FOR UPDATE", IgniteSQLException.class,
            IgniteResource.INSTANCE.selectForUpdateRequiresPessimisticTx().str());
    }

    /** SELECT FOR UPDATE inside an OPTIMISTIC transaction throws an appropriate error. */
    @Test
    public void testSelectForUpdateInOptimisticTransaction() {
        try (Transaction tx = ignite0.transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
            GridTestUtils.assertThrowsAnyCause(log,
                () -> sql(ignite0, "SELECT * FROM Person WHERE id = 1 FOR UPDATE"),
                IgniteSQLException.class, "PESSIMISTIC");
        }
    }

    /** SELECT FOR UPDATE does not support DISTINCT because a result row may represent several cache entries. */
    @Test
    public void testSelectForUpdateRejectsDistinct() {
        assertSelectForUpdateUnsupported(
            "SELECT DISTINCT deptId FROM Person FOR UPDATE",
            "DISTINCT");
    }

    /** SELECT FOR UPDATE does not support GROUP BY because grouped rows do not identify individual cache entries. */
    @Test
    public void testSelectForUpdateRejectsGroupBy() {
        assertSelectForUpdateUnsupported(
            "SELECT deptId, COUNT(*) FROM Person GROUP BY deptId FOR UPDATE",
            "GROUP BY");
    }

    /** SELECT FOR UPDATE does not support HAVING because it operates on grouped result rows. */
    @Test
    public void testSelectForUpdateRejectsHaving() {
        assertSelectForUpdateUnsupported(
            "SELECT COUNT(*) FROM Person HAVING COUNT(*) > 1 FOR UPDATE",
            "HAVING");
    }

    /** Aggregate queries without GROUP BY also collapse cache entries into a derived result row. */
    @Test
    public void testSelectForUpdateRejectsAggregateWithoutGroupBy() {
        assertSelectForUpdateUnsupported(
            "SELECT COUNT(*) FROM Person FOR UPDATE",
            "aggregate functions");
    }

    /** An aggregate used only by ORDER BY still collapses all source rows into one result row. */
    @Test
    public void testSelectForUpdateRejectsAggregateInOrderBy() {
        assertSelectForUpdateUnsupported(
            "SELECT 1 FROM Person ORDER BY COUNT(*) FOR UPDATE",
            "aggregate functions");
    }

    /** SELECT FOR UPDATE cannot lock rows exposed by a system view. */
    @Test
    public void testSelectForUpdateRejectsSystemView() {
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertThrows(ignite0, "SELECT node_id FROM SYS.NODES FOR UPDATE", IgniteSQLException.class,
                "Column '_KEY' not found in table 'NODES'");
        }
    }

    /** SELECT FOR UPDATE cannot lock rows produced by a table function. */
    @Test
    public void testSelectForUpdateRejectsTableFunction() {
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertThrows(ignite0, "SELECT x FROM TABLE(SYSTEM_RANGE(1, 2)) FOR UPDATE", IgniteSQLException.class,
                "SELECT FOR UPDATE is only supported for tables and JOINs of tables");
        }
    }

    /** A windowed aggregate in ORDER BY preserves one result row per source row. */
    @Test
    public void testSelectForUpdateSupportsWindowedAggregateInOrderBy() {
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertQuery(ignite0, "SELECT id FROM Person ORDER BY COUNT(*) OVER (ORDER BY id DESC) LIMIT 2 FOR UPDATE")
                .ordered()
                .returns(30)
                .returns(29)
                .check();
        }
    }

    /** ORDER BY, LIMIT and OFFSET are preserved while preparing SELECT FOR UPDATE. */
    @Test
    public void testSelectForUpdateSupportsOrderByLimitAndOffset() {
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertQuery(ignite0, "SELECT id FROM Person ORDER BY id DESC LIMIT 2 OFFSET 1 FOR UPDATE")
                .ordered()
                .returns(29)
                .returns(28)
                .check();
        }
    }

    /** Windowed aggregates preserve the identity of every source row and remain supported. */
    @Test
    public void testSelectForUpdateSupportsWindowedAggregates() {
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertQuery(ignite0, "SELECT id, COUNT(*) OVER () FROM Person WHERE id <= 2 FOR UPDATE")
                .returns(1, 2L)
                .returns(2, 2L)
                .check();
        }
    }

    /** FOR UPDATE OF columns is supported. */
    @Test
    public void forUpdateOfColumn() {
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertQuery(ignite0, "SELECT id FROM Person FOR UPDATE OF id")
                .resultSize(30)
                .check();
        }
    }

    /** FOR UPDATE WAIT n seconds is supported. */
    @Test
    public void forUpdateWait() {
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertQuery(ignite0, "SELECT id FROM Person FOR UPDATE WAIT 5")
                .resultSize(30)
                .check();
        }
    }

    /** FOR UPDATE NOWAIT is supported. */
    @Test
    public void forUpdateNowait() {
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertQuery(ignite0, "SELECT id FROM Person FOR UPDATE NOWAIT")
                .resultSize(30)
                .check();
        }
    }

    /** SELECT is executed again when the selected row version changes before locking. */
    @Test
    public void testSelectForUpdateRetriesQueryAfterVersionChange() throws Exception {
        UUID clientNodeId = client.cluster().localNode().id();
        CountDownLatch batchBlocked = new CountDownLatch(1);

        for (int i = 0; i < 3; i++) {
            TestRecordingCommunicationSpi.spi(grid(i)).blockMessages((node, msg) -> {
                boolean block = node.id().equals(clientNodeId) && msg instanceof QueryBatchMessage;

                if (block)
                    batchBlocked.countDown();

                return block;
            });
        }

        IgniteInternalFuture<?> selectFut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                assertQuery("SELECT id, age FROM Person WHERE id = 10 FOR UPDATE WAIT 5")
                    .returns(10, 100)
                    .check();

                tx.commit();
            }
        });

        try {
            assertTrue("The first SELECT result was not intercepted",
                batchBlocked.await(10, TimeUnit.SECONDS));

            assertQuery(ignite0, "UPDATE Person SET age = 100 WHERE id = 10")
                .returns(1L)
                .check();
        }
        finally {
            for (int i = 0; i < 3; i++)
                TestRecordingCommunicationSpi.spi(grid(i)).stopBlock();
        }

        selectFut.get(10_000);
    }

    /** FOR UPDATE with WHERE is supported. */
    @Test
    public void forUpdateWithWhere() {
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertQuery(ignite0, "SELECT id FROM Person WHERE age >= 30 FOR UPDATE")
                .resultSize(20)
                .check();
        }
    }

    /** Basic SELECT FOR UPDATE returns the correct rows and commits successfully. */
    @Test
    public void testSelectForUpdateByPrimaryKey() {
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertQuery(ignite0, "SELECT * FROM Person WHERE id = 2 FOR UPDATE")
                .returns(2, "Bob", 21, null, null)
                .check();

            tx.commit();
        }
    }

    /** SELECT FOR UPDATE with no matching rows returns an empty result set. */
    @Test
    public void testSelectForUpdateNoRows() {
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertQuery(ignite0, "SELECT * FROM Person WHERE id = 9999 FOR UPDATE")
                .resultSize(0)
                .check();

            tx.commit();
        }
    }

    /** SELECT FOR UPDATE within the same transaction allows repeated locks on the same key. */
    @Test
    public void testSelectForUpdateRepeatedInSameTx() {
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertQuery(ignite0, "SELECT * FROM Person WHERE id = 2 FOR UPDATE")
                .returns(2, "Bob", 21, null, null)
                .check();

            // Second FOR UPDATE on the same key in the same transaction should succeed.
            assertQuery(ignite0, "SELECT * FROM Person WHERE id = 2 FOR UPDATE")
                .returns(2, "Bob", 21, null, null)
                .check();

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
                assertQuery(ignite0, "SELECT * FROM Person WHERE id = 5 FOR UPDATE")
                    .returns(5, "Alex", 24, null, null)
                    .check();

                tx1Locked.countDown();    // Signal that lock is held.

                tx1Done.await(10, TimeUnit.SECONDS);  // Wait for tx2 to finish.

                tx.commit();
            }
        });

        // Wait until tx1 holds the lock.
        assertTrue("tx1 did not acquire lock in time", tx1Locked.await(10, TimeUnit.SECONDS));

        // Transaction 2: try to lock the same row with NOWAIT – must fail.
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            GridTestUtils.assertThrowsAnyCause(log,
                () -> sql(ignite0, "SELECT * FROM Person WHERE id = 5 FOR UPDATE NOWAIT"),
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

                assertQuery(ignite1, "SELECT * FROM Person WHERE id = 9 FOR UPDATE WAIT 5")
                    .returns(9, "Diana", 28, null, null)
                    .check();

                assertQuery(ignite1, "UPDATE Person SET age = 100 WHERE id = 9")
                    .returns(1L)
                    .check();

                assertEquals(ACTIVE, tx.state());

                tx.commit();
            }
        }
        finally {
            release.countDown();
        }

        lockFut.get(10_000);

        assertQuery("SELECT id, age FROM Person WHERE id = 9")
            .returns(9, 100)
            .check();
    }

    /** Acquires a row lock on the specified node and holds it until the release latch is opened. */
    private IgniteInternalFuture<?> lockRow(
        IgniteEx ignite,
        int id,
        CountDownLatch locked,
        CountDownLatch release
    ) {
        return GridTestUtils.runAsync(() -> {
            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                assertQuery(ignite, "SELECT * FROM Person WHERE id = ? FOR UPDATE")
                    .withParams(id)
                    .resultSize(1)
                    .check();

                locked.countDown();

                assertTrue("Timed out waiting to release row lock", release.await(30, TimeUnit.SECONDS));

                tx.commit();
            }
        });
    }

    /** Verifies that a transaction on the specified node cannot acquire the row lock. */
    private void assertRowLocked(IgniteEx ignite, int id) {
        assertTableRowLocked(ignite, "Person", id);
    }

    /** Verifies that a transaction cannot acquire a row lock in the specified table. */
    private void assertTableRowLocked(IgniteEx ignite, String tableName, int id) {
        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            GridTestUtils.assertThrowsAnyCause(log,
                () -> sql(ignite, "SELECT * FROM " + tableName + " WHERE id = ? FOR UPDATE NOWAIT", id),
                IgniteSQLException.class,
                "could not acquire lock");

            tx.rollback();
        }
    }

    /** Verifies that a transaction can acquire the row lock in the specified table. */
    private void assertTableRowUnlocked(IgniteEx ignite, String tableName, int id) {
        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertQuery(ignite, "SELECT id FROM " + tableName + " WHERE id = ? FOR UPDATE NOWAIT")
                .withParams(id)
                .returns(id)
                .check();

            tx.commit();
        }
    }

    /** Checks that a row-collapsing SELECT form is rejected before lock execution. */
    private void assertSelectForUpdateUnsupported(String qry, String clause) {
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            GridTestUtils.assertThrowsAnyCause(log, () -> sql(ignite0, qry), IgniteSQLException.class,
                "SELECT FOR UPDATE does not support " + clause);
        }
    }

}
