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

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.query.QueryUtils;
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

        super.execute(node(), "CREATE TABLE INTS(k int primary key, v int) WITH \"wrap_value=false,cache_name=ints," +
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

        assertTxPresent();

        assertTxState(tx().proxy(), TransactionState.ACTIVE);
    }

    /**
     * Test that COMMIT commits a transaction.
     */
    public void testCommit() {
        execute(node(), "BEGIN WORK");

        assertTxPresent();

        Transaction tx = tx().proxy();

        assertTxState(tx, TransactionState.ACTIVE);

        execute(node(), "COMMIT TRANSACTION");

        assertTxState(tx, TransactionState.COMMITTED);

        assertSqlTxNotPresent();
    }

    /**
     * Test that COMMIT without a transaction yields nothing.
     */
    public void testCommitNoTransaction() {
        execute(node(), "COMMIT");
    }

    /**
     * Test that ROLLBACK without a transaction yields nothing.
     */
    public void testRollbackNoTransaction() {
        execute(node(), "ROLLBACK");
    }

    /**
     * Test that ROLLBACK rolls back a transaction.
     */
    public void testRollback() {
        execute(node(), "BEGIN TRANSACTION");

        assertTxPresent();

        Transaction tx = tx().proxy();

        assertTxState(tx, TransactionState.ACTIVE);

        execute(node(), "ROLLBACK TRANSACTION");

        assertTxState(tx, TransactionState.ROLLED_BACK);

        assertSqlTxNotPresent();
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
