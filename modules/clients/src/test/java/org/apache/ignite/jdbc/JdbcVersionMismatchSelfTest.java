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

package org.apache.ignite.jdbc;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import javax.cache.CacheException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * JDBC version mismatch test.
 */
public class JdbcVersionMismatchSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid();

        try (Connection conn = connect()) {
            executeUpdate(conn,
                "CREATE TABLE test (a INT PRIMARY KEY, b INT, c VARCHAR) WITH \"atomicity=TRANSACTIONAL_SNAPSHOT, cache_name=TEST\"");

            executeUpdate(conn, "INSERT INTO test VALUES (1, 1, 'test_1')");
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testVersionMismatchJdbc() throws Exception {
        try (Connection conn1 = connect(); Connection conn2 = connect()) {
            conn1.setAutoCommit(false);
            conn2.setAutoCommit(false);

            // Start first transaction and observe some values.
            assertEquals(1, executeQuery(conn1, "SELECT * FROM test").size());

            // Change values while first transaction is still in progress.
            executeUpdate(conn2, "INSERT INTO test VALUES (2, 2, 'test_2')");
            executeUpdate(conn2, "COMMIT");
            assertEquals(2, executeQuery(conn2, "SELECT * FROM test").size());

            // Force version mismatch.
            try {
                executeUpdate(conn1, "INSERT INTO test VALUES (2, 2, 'test_2')");

                fail();
            }
            catch (SQLException e) {
                assertNotNull(e.getMessage());
                assertTrue(e.getMessage().contains("Mvcc version mismatch"));
            }

            // Try executing any other statement from the same transaction.
            // TODO
            assertEquals(1, executeQuery(conn1, "SELECT * FROM test").size());

            // TODO: Native API.

            //executeUpdate(conn1, "INSERT INTO test VALUES (3, 3, 'test_3')");
        }

        // TODO: Ensure that thread state is cleared.
    }

    /**
     * @throws Exception If failed.
     */
    public void testVersionMismatchNative() throws Exception {
        Ignite ignite = grid();

        IgniteCache cache = ignite.cache("TEST");

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            // Start first transaction and observe some values.
            assertEquals(1, cache.query(new SqlFieldsQuery("SELECT * FROM test")).getAll().size());

            // Change values while first transaction is still in progress.
            Thread thread = new Thread(new Runnable() {
                @Override public void run() {
                    try (Transaction txOther = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        assertEquals(1, cache.query(new SqlFieldsQuery("SELECT * FROM test")).getAll().size());

                        cache.query(new SqlFieldsQuery("INSERT INTO test VALUES (2, 2, 'test_2')")).getAll();

                        assertEquals(2, cache.query(new SqlFieldsQuery("SELECT * FROM test")).getAll().size());

                        txOther.commit();
                    }
                }
            });

            thread.start();
            thread.join();

            // Force version mismatch.
            try {
                cache.query(new SqlFieldsQuery("INSERT INTO test VALUES (2, 2, 'test_2')")).getAll();

                fail();
            }
            catch (CacheException e) {
                assertNotNull(e.getMessage());
                assertTrue(e.getMessage().contains("Mvcc version mismatch"));
            }

            // Try executing any other statement from the same transaction.
            assertEquals(1, cache.query(new SqlFieldsQuery("SELECT * FROM test")).getAll().size());

            tx.rollback();
        }

        // TODO: Ensure that thread state is cleared.
    }

    /**
     * Establish JDBC connection.
     *
     * @return Connection.
     * @throws Exception If failed.
     */
    private Connection connect() throws Exception {
        return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800");
    }

    /**
     * Execute update statement.
     *
     * @param conn Connection.
     * @param sql SQL.
     * @throws Exception If failed.
     */
    private static void executeUpdate(Connection conn, String sql) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }

    /**
     * Execute query.
     *
     * @param conn Connection.
     * @param sql SQL.
     * @return Result.
     * @throws Exception If failed.
     */
    private static List<List<Object>> executeQuery(Connection conn, String sql) throws Exception {
        List<List<Object>> rows = new ArrayList<>();

        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(sql)) {
                int colCnt = rs.getMetaData().getColumnCount();

                while (rs.next()) {
                    List<Object> row = new ArrayList<>(colCnt);

                    for (int i = 0; i < colCnt; i++)
                        row.add(rs.getObject(i + 1));

                    rows.add(row);
                }
            }
        }

        return rows;
    }
}
