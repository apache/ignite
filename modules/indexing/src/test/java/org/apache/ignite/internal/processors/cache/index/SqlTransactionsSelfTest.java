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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GatewayProtectedCacheProxy;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionState;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests to check behavior regarding transactions started via SQL.
 */
@Ignore("https://issues.apache.org/jira/browse/IGNITE-13723")
public class SqlTransactionsSelfTest extends AbstractSchemaSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(commonConfiguration(0));

        super.execute(node(), "CREATE TABLE INTS(k int primary key, v int) WITH \"wrap_value=false,cache_name=ints," +
            "atomicity=transactional_snapshot\"");
    }

    /**
     * Test that BEGIN opens a transaction.
     */
    @Test
    public void testBegin() {
        execute(node(), "BEGIN");

        assertTxPresent();

        assertTxState(tx(), TransactionState.ACTIVE);
    }

    /**
     * Test that COMMIT commits a transaction.
     */
    @Test
    public void testCommit() {
        execute(node(), "BEGIN WORK");

        assertTxPresent();

        Transaction tx = tx();

        assertTxState(tx, TransactionState.ACTIVE);

        execute(node(), "COMMIT TRANSACTION");

        assertTxState(tx, TransactionState.COMMITTED);

        assertSqlTxNotPresent();
    }

    /**
     * Test that COMMIT without a transaction yields nothing.
     */
    @Test
    public void testCommitNoTransaction() {
        execute(node(), "COMMIT");
    }

    /**
     * Test that ROLLBACK without a transaction yields nothing.
     */
    @Test
    public void testRollbackNoTransaction() {
        execute(node(), "ROLLBACK");
    }

    /**
     * Test that ROLLBACK rolls back a transaction.
     */
    @Test
    public void testRollback() {
        execute(node(), "BEGIN TRANSACTION");

        assertTxPresent();

        Transaction tx = tx();

        assertTxState(tx, TransactionState.ACTIVE);

        execute(node(), "ROLLBACK TRANSACTION");

        assertTxState(tx, TransactionState.ROLLED_BACK);

        assertSqlTxNotPresent();
    }

    /**
     * Test that attempting to perform various SQL operations within non SQL transaction yields an exception.
     */
    @Test
    public void testSqlOperationsWithinNonSqlTransaction() {
        assertSqlOperationWithinNonSqlTransactionThrows("COMMIT");

        assertSqlOperationWithinNonSqlTransactionThrows("ROLLBACK");

        assertSqlOperationWithinNonSqlTransactionThrows("SELECT * from ints");

        assertSqlOperationWithinNonSqlTransactionThrows("DELETE from ints");

        assertSqlOperationWithinNonSqlTransactionThrows("INSERT INTO ints(k, v) values(10, 15)");

        assertSqlOperationWithinNonSqlTransactionThrows("MERGE INTO ints(k, v) values(10, 15)");

        assertSqlOperationWithinNonSqlTransactionThrows("UPDATE ints SET v = 100 WHERE k = 5");

        assertSqlOperationWithinNonSqlTransactionThrows("create index idx on ints(v)");

        assertSqlOperationWithinNonSqlTransactionThrows("CREATE TABLE T(k int primary key, v int)");
    }

    /**
     * Check that trying to run given SQL statement both locally and in distributed mode yields an exception
     * if transaction already has been marked as being of SQL type.
     * @param sql SQL statement.
     */
    private void assertSqlOperationWithinNonSqlTransactionThrows(final String sql) {
        try (Transaction ignored = node().transactions().txStart()) {
            node().cache("ints").put(1, 1);

            assertSqlException(new Runnable() {
                @Override public void run() {
                    execute(node(), sql);
                }
            }, IgniteQueryErrorCode.TRANSACTION_TYPE_MISMATCH);
        }

        try (Transaction ignored = node().transactions().txStart()) {
            node().cache("ints").put(1, 1);

            assertSqlException(new Runnable() {
                @Override public void run() {
                    node().cache("ints").query(new SqlFieldsQuery(sql).setLocal(true)).getAll();
                }
            }, IgniteQueryErrorCode.TRANSACTION_TYPE_MISMATCH);
        }
    }

    /**
     * Test that attempting to perform a cache API operation from within an SQL transaction fails.
     */
    private void checkCacheOperationThrows(final String opName, final Object... args) {
        execute(node(), "BEGIN");

        try {
            GridTestUtils.assertThrows(null, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try {
                        // We need to detect types based on arguments due to multiple overloads.
                        Class[] types;

                        if (F.isEmpty(args))
                            types = (Class[])X.EMPTY_OBJECT_ARRAY;
                        else {
                            types = new Class[args.length];

                            for (int i = 0; i < args.length; i++)
                                types[i] = argTypeForObject(args[i]);
                        }

                        Object res = U.invoke(GatewayProtectedCacheProxy.class, node().cache("ints"),
                            opName, types, args);

                        if (opName.endsWith("Async"))
                            ((IgniteFuture)res).get();
                    }
                    catch (IgniteCheckedException e) {
                        if (e.getCause() != null) {
                            try {
                                if (e.getCause().getCause() != null)
                                    throw (Exception)e.getCause().getCause();
                                else
                                    fail();
                            }
                            catch (IgniteException e1) {
                                // Some public API methods don't have IgniteCheckedException on their signature
                                // and thus may wrap it into an IgniteException.
                                if (e1.getCause() != null)
                                    throw (Exception)e1.getCause();
                                else
                                    fail();
                            }
                        }
                        else
                            fail();
                    }

                    return null;
                }
            }, IgniteCheckedException.class,
                "SQL queries and cache operations may not be used in the same transaction.");
        }
        finally {
            try {
                execute(node(), "ROLLBACK");
            }
            catch (Throwable e) {
                // No-op.
            }
        }
    }

    /**
     *
     */
    private static Class<?> argTypeForObject(Object arg) {
        if (arg instanceof Set)
            return Set.class;
        else if (arg instanceof Map)
            return Map.class;
        else if (arg.getClass().getName().startsWith("java.lang."))
            return Object.class;
        else if (arg instanceof CacheEntryProcessor)
            return CacheEntryProcessor.class;
        else if (arg instanceof EntryProcessor)
            return EntryProcessor.class;
        else
            return arg.getClass();
    }

    /**
     * Test that attempting to perform a cache PUT operation from within an SQL transaction fails.
     */
    @Test
    public void testCacheOperationsFromSqlTransaction() {
        checkCacheOperationThrows("get", 1);

        checkCacheOperationThrows("getAsync", 1);

        checkCacheOperationThrows("getEntry", 1);

        checkCacheOperationThrows("getEntryAsync", 1);

        checkCacheOperationThrows("getAndPut", 1, 1);

        checkCacheOperationThrows("getAndPutAsync", 1, 1);

        checkCacheOperationThrows("getAndPutIfAbsent", 1, 1);

        checkCacheOperationThrows("getAndPutIfAbsentAsync", 1, 1);

        checkCacheOperationThrows("getAndReplace", 1, 1);

        checkCacheOperationThrows("getAndReplaceAsync", 1, 1);

        checkCacheOperationThrows("getAndRemove", 1);

        checkCacheOperationThrows("getAndRemoveAsync", 1);

        checkCacheOperationThrows("containsKey", 1);

        checkCacheOperationThrows("containsKeyAsync", 1);

        checkCacheOperationThrows("put", 1, 1);

        checkCacheOperationThrows("putAsync", 1, 1);

        checkCacheOperationThrows("putIfAbsent", 1, 1);

        checkCacheOperationThrows("putIfAbsentAsync", 1, 1);

        checkCacheOperationThrows("remove", 1);

        checkCacheOperationThrows("removeAsync", 1);

        checkCacheOperationThrows("remove", 1, 1);

        checkCacheOperationThrows("removeAsync", 1, 1);

        checkCacheOperationThrows("replace", 1, 1);

        checkCacheOperationThrows("replaceAsync", 1, 1);

        checkCacheOperationThrows("replace", 1, 1, 1);

        checkCacheOperationThrows("replaceAsync", 1, 1, 1);

        checkCacheOperationThrows("getAll", new HashSet<>(Arrays.asList(1, 2)));

        checkCacheOperationThrows("containsKeys", new HashSet<>(Arrays.asList(1, 2)));

        checkCacheOperationThrows("getEntries", new HashSet<>(Arrays.asList(1, 2)));

        checkCacheOperationThrows("putAll", Collections.singletonMap(1, 1));

        checkCacheOperationThrows("removeAll", new HashSet<>(Arrays.asList(1, 2)));

        checkCacheOperationThrows("getAllAsync", new HashSet<>(Arrays.asList(1, 2)));

        checkCacheOperationThrows("containsKeysAsync", new HashSet<>(Arrays.asList(1, 2)));

        checkCacheOperationThrows("getEntriesAsync", new HashSet<>(Arrays.asList(1, 2)));

        checkCacheOperationThrows("putAllAsync", Collections.singletonMap(1, 1));

        checkCacheOperationThrows("removeAllAsync", new HashSet<>(Arrays.asList(1, 2)));

        checkCacheOperationThrows("invoke", 1, ENTRY_PROC, X.EMPTY_OBJECT_ARRAY);

        checkCacheOperationThrows("invoke", 1, CACHE_ENTRY_PROC, X.EMPTY_OBJECT_ARRAY);

        checkCacheOperationThrows("invokeAsync", 1, ENTRY_PROC, X.EMPTY_OBJECT_ARRAY);

        checkCacheOperationThrows("invokeAsync", 1, CACHE_ENTRY_PROC, X.EMPTY_OBJECT_ARRAY);

        checkCacheOperationThrows("invokeAll", Collections.singletonMap(1, CACHE_ENTRY_PROC), X.EMPTY_OBJECT_ARRAY);

        checkCacheOperationThrows("invokeAll", Collections.singleton(1), CACHE_ENTRY_PROC, X.EMPTY_OBJECT_ARRAY);

        checkCacheOperationThrows("invokeAll", Collections.singleton(1), ENTRY_PROC, X.EMPTY_OBJECT_ARRAY);

        checkCacheOperationThrows("invokeAllAsync", Collections.singletonMap(1, CACHE_ENTRY_PROC),
            X.EMPTY_OBJECT_ARRAY);

        checkCacheOperationThrows("invokeAllAsync", Collections.singleton(1), CACHE_ENTRY_PROC, X.EMPTY_OBJECT_ARRAY);

        checkCacheOperationThrows("invokeAllAsync", Collections.singleton(1), ENTRY_PROC, X.EMPTY_OBJECT_ARRAY);
    }

    /** */
    private static final EntryProcessor<Integer, Integer, Object> ENTRY_PROC =
        new EntryProcessor<Integer, Integer, Object>() {
            @Override public Object process(MutableEntry<Integer, Integer> entry, Object... arguments)
                throws EntryProcessorException {
                return null;
            }
        };

    /** */
    private static final CacheEntryProcessor<Integer, Integer, Object> CACHE_ENTRY_PROC =
        new CacheEntryProcessor<Integer, Integer, Object>() {
            @Override public Object process(MutableEntry<Integer, Integer> entry, Object... arguments)
                throws EntryProcessorException {
                return null;
            }
        };

    /**
     * @return Node.
     */
    private IgniteEx node() {
        return grid(0);
    }

    /**
     * @return Currently open transaction.
     */
    private Transaction tx() {
        return node().transactions().tx();
    }

    /**
     * Check that there's an open transaction with SQL flag.
     */
    private void assertTxPresent() {
        assertNotNull(tx());
    }

    /** {@inheritDoc} */
    @Override protected List<List<?>> execute(Ignite node, String sql) {
        return node.cache("ints").query(new SqlFieldsQuery(sql).setSchema(QueryUtils.DFLT_SCHEMA)).getAll();
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
