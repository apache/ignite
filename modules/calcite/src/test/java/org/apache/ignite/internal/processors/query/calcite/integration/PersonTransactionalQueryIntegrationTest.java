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

/** Tests queries over transactional Person table. */
public class PersonTransactionalQueryIntegrationTest extends GridCommonAbstractTest {
    /** */
    private IgniteEx node;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(new SqlConfiguration().setQueryEnginesConfiguration(
                new CalciteQueryEngineConfiguration().setDefault(true)))
            .setTransactionConfiguration(new TransactionConfiguration().setTxAwareQueriesEnabled(true));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        node = startGrids(3);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Test
    public void testSelectByPrimaryKey() {
        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            createAndPopulatePersonTable();

            assertRows("SELECT * FROM Person WHERE id = 2",
                row(2, "Bob", 22));

            tx.commit();
        }
    }

    /** */
    @Test
    public void testSelectBySecondaryIndexRange() {
        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            createAndPopulatePersonTable();

            assertRows("SELECT * FROM Person WHERE age < 30",
                row(1, "Alice", 21),
                row(2, "Bob", 22),
                row(3, "Ann", 23),
                row(4, "Carl", 24),
                row(5, "Alex", 25),
                row(6, "Diana", 26),
                row(7, "Person7", 27),
                row(8, "Person8", 28),
                row(9, "Person9", 29));

            tx.commit();
        }
    }

    /** */
    @Test
    public void testSelectByPrimaryKeyRange() {
        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            createAndPopulatePersonTable();

            assertRows("SELECT * FROM Person WHERE id < 10",
                row(1, "Alice", 21),
                row(2, "Bob", 22),
                row(3, "Ann", 23),
                row(4, "Carl", 24),
                row(5, "Alex", 25),
                row(6, "Diana", 26),
                row(7, "Person7", 27),
                row(8, "Person8", 28),
                row(9, "Person9", 29));

            tx.commit();
        }
    }

    /** */
    @Test
    public void testSelectByNameLike() {
        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            createAndPopulatePersonTable();

            assertRows("SELECT * FROM Person WHERE name LIKE 'A%' FOR UPDATE",
                row(1, "Alice", 21),
                row(3, "Ann", 23),
                row(5, "Alex", 25));

            tx.commit();
        }
    }

    // ==================== SELECT FOR UPDATE tests ====================

    /** Basic SELECT FOR UPDATE returns the correct rows and commits successfully. */
    @Test
    public void testSelectForUpdateByPrimaryKey() {
        createAndPopulatePersonTable();

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertRows("SELECT * FROM Person WHERE id = 2 FOR UPDATE",
                row(2, "Bob", 22));

            tx.commit();
        }
    }

    /** SELECT FOR UPDATE with no matching rows returns an empty result set. */
    @Test
    public void testSelectForUpdateNoRows() {
        createAndPopulatePersonTable();

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            List<List<?>> rows = sql("SELECT * FROM Person WHERE id = 9999 FOR UPDATE");

            assertEquals(0, rows.size());

            tx.commit();
        }
    }

    /** SELECT FOR UPDATE NOWAIT succeeds when there is no contention. */
    @Test
    public void testSelectForUpdateNowaitNoContention() {
        createAndPopulatePersonTable();

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertRows("SELECT * FROM Person WHERE id = 1 FOR UPDATE NOWAIT",
                row(1, "Alice", 21));

            tx.commit();
        }
    }

    /** SELECT FOR UPDATE WAIT n succeeds when there is no contention. */
    @Test
    public void testSelectForUpdateWaitNoContention() {
        createAndPopulatePersonTable();

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertRows("SELECT * FROM Person WHERE id = 3 FOR UPDATE WAIT 5",
                row(3, "Ann", 23));

            tx.commit();
        }
    }

    /** SELECT FOR UPDATE within the same transaction allows repeated locks on the same key. */
    @Test
    public void testSelectForUpdateRepeatedInSameTx() {
        createAndPopulatePersonTable();

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertRows("SELECT * FROM Person WHERE id = 2 FOR UPDATE",
                row(2, "Bob", 22));

            // Second FOR UPDATE on the same key in the same transaction should succeed.
            assertRows("SELECT * FROM Person WHERE id = 2 FOR UPDATE",
                row(2, "Bob", 22));

            tx.commit();
        }
    }

    /** SELECT FOR UPDATE outside a transaction throws an appropriate error. */
    @Test
    public void testSelectForUpdateOutsideTransaction() {
        createAndPopulatePersonTable();

        GridTestUtils.assertThrowsAnyCause(log, () -> {
            sql("SELECT * FROM Person WHERE id = 1 FOR UPDATE");
            return null;
        }, IgniteSQLException.class, "PESSIMISTIC");
    }

    /** SELECT FOR UPDATE inside an OPTIMISTIC transaction throws an appropriate error. */
    @Test
    public void testSelectForUpdateInOptimisticTransaction() {
        createAndPopulatePersonTable();

        try (Transaction tx = node.transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
            GridTestUtils.assertThrowsAnyCause(log, () -> {
                sql("SELECT * FROM Person WHERE id = 1 FOR UPDATE");
                return null;
            }, IgniteSQLException.class, "PESSIMISTIC");

            tx.rollback();
        }
    }

    /** SELECT FOR UPDATE with a JOIN throws an appropriate error. */
    @Test
    public void testSelectForUpdateJoinRejected() {
        createAndPopulatePersonTable();
        sql("CREATE TABLE Dept (id INT PRIMARY KEY, name VARCHAR) WITH atomicity=TRANSACTIONAL");
        sql("INSERT INTO Dept(id, name) VALUES (1, 'Engineering')");

        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            GridTestUtils.assertThrowsAnyCause(log, () -> {
                sql("SELECT p.id FROM Person p JOIN Dept d ON p.id = d.id FOR UPDATE");
                return null;
            }, IgniteSQLException.class, "JOIN");

            tx.rollback();
        }
    }

    /**
     * SELECT FOR UPDATE acquires a pessimistic lock: a second transaction trying to update the
     * same row with NOWAIT immediately fails.
     */
    @Test
    public void testSelectForUpdateBlocksOtherTransaction() throws Exception {
        createAndPopulatePersonTable();

        CountDownLatch tx1Locked = new CountDownLatch(1);
        CountDownLatch tx1Done = new CountDownLatch(1);

        // Transaction 1: acquire lock on row id=5 and hold it.
        IgniteInternalFuture<?> tx1 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = node.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                sql("SELECT * FROM Person WHERE id = 5 FOR UPDATE");

                tx1Locked.countDown();    // Signal that lock is held.

                tx1Done.await(10, TimeUnit.SECONDS);  // Wait for tx2 to finish.

                tx.commit();
            }
        });

        // Wait until tx1 holds the lock.
        assertTrue("tx1 did not acquire lock in time", tx1Locked.await(10, TimeUnit.SECONDS));

        // Transaction 2: try to lock the same row with NOWAIT – must fail.
        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            GridTestUtils.assertThrowsAnyCause(log, () -> {
                sql("SELECT * FROM Person WHERE id = 5 FOR UPDATE NOWAIT");
                return null;
            }, IgniteSQLException.class, "could not acquire lock");

            tx.rollback();
        }
        finally {
            tx1Done.countDown();
        }

        tx1.get(10_000);
    }

    // ==================== Helpers ====================

    /** */
    private void createAndPopulatePersonTable() {
        sql("CREATE TABLE IF NOT EXISTS Person (id INT PRIMARY KEY, name VARCHAR, age INT) WITH atomicity=TRANSACTIONAL");
        sql("CREATE INDEX IF NOT EXISTS age_idx ON Person(age)");

        for (int i = 1; i <= 30; i++)
            sql("INSERT INTO Person(id, name, age) VALUES (?, ?, ?)", i, personName(i), 20 + i);
    }

    /** */
    private void assertRows(String sql, List<?>... expRows) {
        List<List<?>> rows = sql(sql);

        assertEquals(expRows.length, rows.size());
        assertEquals(new HashSet<>(Arrays.asList(expRows)), new HashSet<>(rows));
    }

    /** */
    private List<?> row(Object... vals) {
        return Arrays.asList(vals);
    }

    /** */
    private List<List<?>> sql(String sql, Object... args) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(sql).setSchema("PUBLIC").setArgs(args), true).getAll();
    }

    /** */
    private String personName(int id) {
        switch (id) {
            case 1:
                return "Alice";

            case 2:
                return "Bob";

            case 3:
                return "Ann";

            case 4:
                return "Carl";

            case 5:
                return "Alex";

            case 6:
                return "Diana";

            default:
                return "Person" + id;
        }
    }
}
