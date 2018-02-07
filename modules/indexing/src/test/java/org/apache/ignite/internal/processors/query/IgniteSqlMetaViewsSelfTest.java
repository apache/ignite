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

package org.apache.ignite.internal.processors.query;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.views.IgniteSqlMetaViewProcessor;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.h2.api.ErrorCode;

/**
 * Tests for ignite SQL meta views.
 */
public class IgniteSqlMetaViewsSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Execute sql statement by JDBC connection.
     *
     * @param sql Sql.
     */
    private void execSqlJdbc(String sql) throws SQLException {
        IgniteH2Indexing idx = (IgniteH2Indexing)grid().context().query().getIndexing();

        try (Connection c = idx.connectionForSchema(IgniteSqlMetaViewProcessor.SCHEMA_NAME)) {
            try (Statement s = c.createStatement()) {
                s.executeUpdate(sql);
            }
        }
    }

    /**
     * Execute sql statement by ignite cache query processor.
     *
     * @param sql Sql.
     */
    private void execSqlIgnite(String sql) throws SQLException {
        IgniteCache cache = grid().getOrCreateCache("cache");

        cache.query(new SqlFieldsQuery(sql)).getAll();
    }

    /**
     * @param sql Sql.
     * @param expJdbcErrorCode Expected jdbc error code.
     * @param expIgniteErrorCode Expected ignite error code.
     */
    private void assertSqlError(final String sql, int expJdbcErrorCode, int expIgniteErrorCode) {
        SQLException jdbcE = (SQLException)GridTestUtils.assertThrowsWithCause(new Callable<Void>() {
            @Override public Void call() throws Exception {
                execSqlJdbc(sql);

                return null;
            }
        }, SQLException.class);


        Throwable igniteT = GridTestUtils.assertThrowsWithCause(new Callable<Void>() {
            @Override public Void call() throws Exception {
                execSqlIgnite(sql);

                return null;
            }
        }, IgniteSQLException.class);

        IgniteSQLException igniteE = X.cause(igniteT, IgniteSQLException.class);

        if (expJdbcErrorCode != 0)
            assertEquals(expJdbcErrorCode, jdbcE.getErrorCode());

        if (expIgniteErrorCode != 0)
            assertEquals(expIgniteErrorCode, igniteE.statusCode());
    }
    
    /**
     * Test meta views modifications.
     */
    public void testModifications() throws Exception {
        startGrid();

        assertSqlError("DROP TABLE IGNITE.LOCAL_TRANSACTIONS", ErrorCode.CANNOT_DROP_TABLE_1, 0);

        assertSqlError("TRUNCATE TABLE IGNITE.LOCAL_TRANSACTIONS", ErrorCode.CANNOT_TRUNCATE_1, 0);

        assertSqlError("ALTER TABLE IGNITE.LOCAL_TRANSACTIONS RENAME TO IGNITE.TX",
            ErrorCode.FEATURE_NOT_SUPPORTED_1, 0);

        assertSqlError("ALTER TABLE IGNITE.LOCAL_TRANSACTIONS ADD COLUMN C VARCHAR",
            ErrorCode.FEATURE_NOT_SUPPORTED_1, 0);

        assertSqlError("ALTER TABLE IGNITE.LOCAL_TRANSACTIONS DROP COLUMN XID", ErrorCode.FEATURE_NOT_SUPPORTED_1, 0);

        assertSqlError("ALTER TABLE IGNITE.LOCAL_TRANSACTIONS RENAME COLUMN XID TO C",
            ErrorCode.FEATURE_NOT_SUPPORTED_1, 0);

        assertSqlError("CREATE INDEX IDX ON IGNITE.LOCAL_TRANSACTIONS(XID)", ErrorCode.FEATURE_NOT_SUPPORTED_1, 0);

        assertSqlError("INSERT INTO IGNITE.LOCAL_TRANSACTIONS (XID) VALUES ('-')", ErrorCode.FEATURE_NOT_SUPPORTED_1,
            IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        try (Transaction tx = grid().transactions().txStart()) {
            assertSqlError("UPDATE IGNITE.LOCAL_TRANSACTIONS SET XID = '-'", 0,
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

            assertSqlError("DELETE IGNITE.LOCAL_TRANSACTIONS", 0, IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

            tx.commit();
        }
    }

    /**
     * @param rowData Row data.
     * @param colTypes Column types.
     */
    private void assertColumnTypes(List<?> rowData, Class<?> ... colTypes) {
        for (int i = 0; i < colTypes.length; i++) {
            if (rowData.get(i) != null)
                assertEquals("Column " + i + " type", rowData.get(i).getClass(), colTypes[i]);
        }
    }

    /**
     * Test local transactions meta view.
     *
     * @throws Exception If failed.
     */
    public void testLocalTransactionsSystemView() throws Exception {
        int txCnt = 3;

        final Ignite ignite = startGrid();

        IgniteCache cache = ignite.getOrCreateCache("cache");

        final CountDownLatch latchTxStart = new CountDownLatch(txCnt);

        final CountDownLatch latchTxBody = new CountDownLatch(1);

        final CountDownLatch latchTxEnd = new CountDownLatch(txCnt);

        multithreadedAsync(new Runnable() {
            @Override public void run() {
                try(Transaction tx = ignite.transactions().txStart()) {
                    tx.timeout(1_000_000L);

                    latchTxStart.countDown();

                    latchTxBody.await();

                    tx.commit();
                }
                catch (InterruptedException e) {
                    fail("Thread interrupted");
                }

                latchTxEnd.countDown();
            }
        }, txCnt);

        latchTxStart.await();

        List<List<?>> res = cache.query(
            new SqlFieldsQuery("SELECT XID, START_NODE_ID, START_TIME, TIMEOUT, TIMEOUT_MILLIS, IS_TIMED_OUT, " +
                "START_THREAD_ID, ISOLATION, CONCURRENCY, IMPLICIT, IS_INVALIDATE, STATE, SIZE, " +
                "STORE_ENABLED, STORE_WRITE_THROUGH, IO_POLICY, IMPLICIT_SINGLE, IS_EMPTY, OTHER_NODE_ID, " +
                "EVENT_NODE_ID, ORIGINATING_NODE_ID, IS_NEAR, IS_DHT, IS_COLOCATED, IS_LOCAL, IS_SYSTEM, " +
                "IS_USER, SUBJECT_ID, IS_DONE, IS_INTERNAL, IS_ONE_PHASE_COMMIT " +
                "FROM IGNITE.LOCAL_TRANSACTIONS"
            )
        ).getAll();

        assertColumnTypes(res.get(0), String.class, UUID.class, Timestamp.class, Time.class, Long.class, Boolean.class,
            Long.class, String.class, String.class, Boolean.class, Boolean.class, String.class, Integer.class,
            Boolean.class, Boolean.class, Byte.class, Boolean.class, Boolean.class, UUID.class,
            UUID.class, UUID.class, Boolean.class, Boolean.class, Boolean.class, Boolean.class, Boolean.class,
            Boolean.class, UUID.class, Boolean.class, Boolean.class, Boolean.class
        );

        // Assert values.
        assertEquals(ignite.cluster().localNode().id(), res.get(0).get(1));

        assertTrue(U.currentTimeMillis() > ((Timestamp)res.get(0).get(2)).getTime());

        assertEquals("00:16:40" /* 1_000_000 ms */, res.get(0).get(3).toString());

        assertEquals(1_000_000L, res.get(0).get(4));

        assertEquals(false, res.get(0).get(5));

        // Assert row count.
        assertEquals(txCnt, res.size());

        latchTxBody.countDown();

        latchTxEnd.await();

        assertEquals(0, cache.query(
            new SqlFieldsQuery("SELECT XID FROM LOCAL_TRANSACTIONS").setSchema("IGNITE")
        ).getAll().size());
    }
}
