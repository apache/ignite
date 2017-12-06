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

package org.apache.ignite.internal.processors.cache.index;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GatewayProtectedCacheProxy;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionState;

/**
 * Tests to check behavior regarding transactions started via SQL.
 */
public class SqlTransactionsSelfTest extends AbstractSchemaSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(commonConfiguration(0));

        execute(node(), "CREATE TABLE INTS(k int primary key, v int) WITH \"wrap_value=false,cache_name=ints," +
            "atomicity=transactional\"");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * Test that BEGIN opens a transaction.
     */
    public void testBegin() {
        execute(node(), "BEGIN");

        assertSqlTxPresent();

        assertTxState(tx().proxy(), TransactionState.ACTIVE);
    }

    /**
     * Test that COMMIT commits a transaction.
     */
    public void testCommit() {
        execute(node(), "BEGIN WORK");

        assertSqlTxPresent();

        Transaction tx = tx().proxy();

        assertTxState(tx, TransactionState.ACTIVE);

        execute(node(), "COMMIT TRANSACTION");

        assertTxState(tx, TransactionState.COMMITTED);

        assertSqlTxNotPresent();
    }

    /**
     * Test that COMMIT without a transaction yields an exception.
     */
    public void testCommitNoTransaction() {
        assertSqlException(new RunnableX() {
            @Override public void run() throws Exception {
                execute(node(), "COMMIT");
            }
        }, IgniteQueryErrorCode.NO_TRANSACTION);
    }

    /**
     * Test that ROLLBACK rolls back a transaction.
     */
    public void testRollback() {
        execute(node(), "BEGIN TRANSACTION");

        assertSqlTxPresent();

        Transaction tx = tx().proxy();

        assertTxState(tx, TransactionState.ACTIVE);

        execute(node(), "ROLLBACK TRANSACTION");

        assertTxState(tx, TransactionState.ROLLED_BACK);

        assertSqlTxNotPresent();
    }

    /**
     * Test that attempting to open a nested transaction yields an exception.
     */
    public void testNestedTransaction() {
        execute(node(), "BEGIN");

        assertSqlException(new RunnableX() {
            @Override public void run() throws Exception {
                execute(node(), "BEGIN");
            }
        }, IgniteQueryErrorCode.TRANSACTION_EXISTS);
    }

    /**
     * Test that attempting to perform various SQL operations within non SQL transaction yields an exception.
     */
    public void testSqlOperationsWithinNonSqlTransaction() {
        assertSqlOperationWithinNonSqlTransactionThrows("COMMIT");

        assertSqlOperationWithinNonSqlTransactionThrows("ROLLBACK");

        assertSqlOperationWithinNonSqlTransactionThrows("SELECT * from ints");

        assertSqlOperationWithinNonSqlTransactionThrows("create index idx on ints(v)");

        assertSqlOperationWithinNonSqlTransactionThrows("CREATE TABLE T(k int primary key, v int)");
    }

    /**
     * Check that trying to run given SQL statement both locally and in distributed mode yields an exception.
     * @param sql SQL statement.
     */
    private void assertSqlOperationWithinNonSqlTransactionThrows(final String sql) {
        try (Transaction ignored = node().transactions().txStart()) {
            assertSqlException(new RunnableX() {
                @Override public void run() throws Exception {
                    execute(node(), sql);
                }
            }, IgniteQueryErrorCode.TRANSACTION_TYPE_MISMATCH);
        }

        try (Transaction ignored = node().transactions().txStart()) {
            assertSqlException(new RunnableX() {
                @Override public void run() throws Exception {
                    node().cache("ints").query(new SqlFieldsQuery(sql).setLocal(true)).getAll();
                }
            }, IgniteQueryErrorCode.TRANSACTION_TYPE_MISMATCH);
        }
    }

    /**
     * Test that attempting to perform a cache GET operation from within an SQL transaction fails.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private void checkCacheOperationThrows(final String opName, final Object... args) {
        execute(node(), "BEGIN");

        try {
            GridTestUtils.assertThrows(null, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try {
                        Object res = U.invoke(GatewayProtectedCacheProxy.class, node().cache("ints"), opName, args);

                        if (opName.endsWith("Async"))
                            ((IgniteFuture)res).get();
                    }
                    catch (IgniteCheckedException e) {
                        if (e.getCause() != null) {
                            if (e.getCause().getCause() != null)
                                throw (Exception)e.getCause().getCause();
                            else
                                throw (Exception) e.getCause();
                        }
                        else
                            throw e;
                    }

                    return null;
                }
            }, IgniteCheckedException.class, "Cache operations are forbidden inside of an SQL transaction");
        }
        finally {
            try {
                execute(node(), "COMMIT");
            }
            catch (Throwable e) {
                // No-op.
            }
        }
    }

    /**
     * Test that attempting to perform a cache PUT operation from within an SQL transaction fails.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testCacheOperationsFromSqlTransaction() {
        checkCacheOperationThrows("get", 1);

        checkCacheOperationThrows("put", 1, 1);

        checkCacheOperationThrows("getAll", new HashSet<>(Arrays.asList(1, 2)));

        checkCacheOperationThrows("putAll", Collections.singletonMap(1, 1));

        checkCacheOperationThrows("getAllAsync", new HashSet<>(Arrays.asList(1, 2)));

        checkCacheOperationThrows("putAllAsync", Collections.singletonMap(1, 1));
    }

    /**
     * @return Node.
     */
    private IgniteEx node() {
        return grid(0);
    }

    /**
     * @return Currently open transaction.
     */
    private GridNearTxLocal tx() {
        return node().context().cache().context().tm().userTx();
    }

    /**
     * Check that there's an open transaction with SQL flag.
     */
    private void assertSqlTxPresent() {
        assertNotNull(tx());

        assertTrue(tx().sql());
    }

    /**
     * Check that there's no open transaction.
     */
    private void assertSqlTxNotPresent() {
        assertNull(tx());
    }

    /**
     * Check transaction state.
     */
    private static void assertTxState(Transaction tx, TransactionState state) {
        assertEquals(state, tx.state());
    }
}
