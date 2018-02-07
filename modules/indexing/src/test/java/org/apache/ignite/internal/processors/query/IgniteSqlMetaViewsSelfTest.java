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
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.views.IgniteSqlMetaViewProcessor;
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
     * Execute sql statement.
     *
     * @param sql Sql.
     */
    private void execSql(String sql) throws SQLException {
        IgniteH2Indexing idx = (IgniteH2Indexing)grid().context().query().getIndexing();

        try (Connection c = idx.connectionForSchema(IgniteSqlMetaViewProcessor.SCHEMA_NAME)) {
            try (Statement s = c.createStatement()) {
                s.executeUpdate(sql);
            }
        }
    }

    /**
     * @param sql Sql.
     * @param errorCode Error code.
     */
    private void assertSqlError(final String sql, int errorCode) {
        SQLException sqlE = (SQLException)GridTestUtils.assertThrowsWithCause(new Callable<Void>() {
            @Override public Void call() throws Exception {
                execSql(sql);

                return null;
            }
        }, SQLException.class);

        if (errorCode != 0)
            assertEquals(errorCode, sqlE.getErrorCode());
    }
    
    /**
     * Test meta views modifications.
     */
    public void testModifications() throws Exception {
        startGrid();

        assertSqlError("DROP TABLE IGNITE.LOCAL_TRANSACTIONS", ErrorCode.CANNOT_DROP_TABLE_1);

        assertSqlError("TRUNCATE TABLE IGNITE.LOCAL_TRANSACTIONS", ErrorCode.CANNOT_TRUNCATE_1);

        assertSqlError("ALTER TABLE IGNITE.LOCAL_TRANSACTIONS RENAME TO IGNITE.TX", ErrorCode.FEATURE_NOT_SUPPORTED_1);

        assertSqlError("ALTER TABLE IGNITE.LOCAL_TRANSACTIONS ADD COLUMN C VARCHAR", ErrorCode.FEATURE_NOT_SUPPORTED_1);

        assertSqlError("ALTER TABLE IGNITE.LOCAL_TRANSACTIONS DROP COLUMN XID", ErrorCode.FEATURE_NOT_SUPPORTED_1);

        assertSqlError("ALTER TABLE IGNITE.LOCAL_TRANSACTIONS RENAME COLUMN XID TO C", ErrorCode.FEATURE_NOT_SUPPORTED_1);

        assertSqlError("CREATE INDEX IDX ON IGNITE.LOCAL_TRANSACTIONS(XID)", ErrorCode.FEATURE_NOT_SUPPORTED_1);

        assertSqlError("INSERT INTO IGNITE.LOCAL_TRANSACTIONS (XID) VALUES ('-')", ErrorCode.FEATURE_NOT_SUPPORTED_1);

        try (Transaction tx = grid().transactions().txStart()) {
            assertSqlError("UPDATE IGNITE.LOCAL_TRANSACTIONS SET XID = '-'", 0);

            assertSqlError("DELETE IGNITE.LOCAL_TRANSACTIONS", 0);

            tx.commit();
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
            new SqlFieldsQuery("SELECT XID, START_NODE_ID, START_TIME, TIMEOUT, TIMEOUT_MILLIS, IS_TIMED_OUT " +
                "FROM IGNITE.LOCAL_TRANSACTIONS")
        ).getAll();

        assertEquals(ignite.cluster().localNode().id(), res.get(0).get(1));

        assertTrue(U.currentTimeMillis() > ((Timestamp)res.get(0).get(2)).getTime());

        assertEquals("00:16:40" /* 1_000_000 ms */, res.get(0).get(3).toString());

        assertEquals(1_000_000L, res.get(0).get(4));

        assertEquals(false, res.get(0).get(5));

        assertEquals(txCnt, res.size());

        latchTxBody.countDown();

        latchTxEnd.await();

        assertEquals(0, cache.query(
            new SqlFieldsQuery("SELECT XID FROM LOCAL_TRANSACTIONS").setSchema("IGNITE")
        ).getAll().size());
    }
}
