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

package org.apache.ignite.internal.processors.tx;

import java.util.List;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.integration.AbstractBasicIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal.SAVEPOINTS_EXPLICIT_TX_ONLY;
import static org.apache.ignite.internal.processors.query.calcite.integration.AbstractBasicIntegrationTransactionalTest.SqlTransactionMode.ALL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/** Tests SQL savepoint commands executed by Calcite. */
public class SqlTransactionsSavepointTest extends AbstractBasicIntegrationTest {
    /** */
    private static final String TBL = "SAVEPOINT_TEST_TABLE";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setTransactionConfiguration(new TransactionConfiguration()
                .setTxAwareQueriesEnabled(true))
            .setSqlConfiguration(new SqlConfiguration()
                .setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration()));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        sql("CREATE TABLE " + TBL + "(ID INT PRIMARY KEY, VAL VARCHAR) WITH atomicity=transactional");
    }

    /** */
    @Test
    public void testSavepointCommandsInSqlScript() {
        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            sqlScript(
                "INSERT INTO " + TBL + " VALUES (1, 'before_sp1');" +
                "SAVEPOINT sp1;" +
                "INSERT INTO " + TBL + " VALUES (2, 'after_sp1');" +
                "SAVEPOINT sp2;" +
                "UPDATE " + TBL + " SET VAL = 'after_sp2' WHERE ID = 1;" +
                "DELETE FROM " + TBL + " WHERE ID = 2;" +
                "ROLLBACK TO SAVEPOINT sp2"
            );

            assertQuery(tx, "SELECT ID, VAL FROM " + TBL + " ORDER BY ID")
                .returns(1, "before_sp1")
                .returns(2, "after_sp1")
                .ordered()
                .check();

            sqlScript(
                "ROLLBACK TO SAVEPOINT sp1;" +
                "INSERT INTO " + TBL + " VALUES (3, 'after_sp1_again')"
            );

            assertQuery(tx, "SELECT ID, VAL FROM " + TBL + " ORDER BY ID")
                .returns(1, "before_sp1")
                .returns(3, "after_sp1_again")
                .ordered()
                .check();

            tx.commit();
        }

        assertQuery("SELECT ID, VAL FROM " + TBL + " ORDER BY ID")
            .returns(1, "before_sp1")
            .returns(3, "after_sp1_again")
            .ordered()
            .check();
    }

    /** */
    @Test
    public void testSqlDmlChangesCanBeRolledBackToSavepointUsingSqlCommands() {
        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            sql("INSERT INTO " + TBL + " VALUES (1, 'before_sp1')");

            sql("SAVEPOINT sp1");

            sql("UPDATE " + TBL + " SET VAL = 'after_sp1' WHERE ID = 1");
            sql("INSERT INTO " + TBL + " VALUES (2, 'after_sp1')");

            sql("SAVEPOINT sp2");

            sql("DELETE FROM " + TBL + " WHERE ID = 1");
            sql("INSERT INTO " + TBL + " VALUES (3, 'after_sp2')");

            assertQuery(tx, "SELECT ID, VAL FROM " + TBL + " ORDER BY ID")
                .returns(2, "after_sp1")
                .returns(3, "after_sp2")
                .ordered()
                .check();

            sql("ROLLBACK TO SAVEPOINT sp2");

            assertQuery(tx, "SELECT ID, VAL FROM " + TBL + " ORDER BY ID")
                .returns(1, "after_sp1")
                .returns(2, "after_sp1")
                .ordered()
                .check();

            sql("ROLLBACK TO SAVEPOINT sp1");

            assertQuery(tx, "SELECT ID, VAL FROM " + TBL + " ORDER BY ID")
                .returns(1, "before_sp1")
                .check();

            tx.commit();
        }

        assertQuery("SELECT ID, VAL FROM " + TBL + " ORDER BY ID")
            .returns(1, "before_sp1")
            .check();
    }

    /** */
    @Test
    public void testSqlDmlChangesCanBeRolledBackToSavepoint() {
        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            sql("INSERT INTO " + TBL + " VALUES (1, 'before_sp1')");

            tx.savepoint("sp1");

            sql("UPDATE " + TBL + " SET VAL = 'after_sp1' WHERE ID = 1");
            sql("INSERT INTO " + TBL + " VALUES (2, 'after_sp1')");

            tx.savepoint("sp2");

            sql("DELETE FROM " + TBL + " WHERE ID = 1");
            sql("INSERT INTO " + TBL + " VALUES (3, 'after_sp2')");

            assertQuery(tx, "SELECT ID, VAL FROM " + TBL + " ORDER BY ID")
                .returns(2, "after_sp1")
                .returns(3, "after_sp2")
                .ordered()
                .check();

            tx.rollbackToSavepoint("sp2");

            assertQuery(tx, "SELECT ID, VAL FROM " + TBL + " ORDER BY ID")
                .returns(1, "after_sp1")
                .returns(2, "after_sp1")
                .ordered()
                .check();

            tx.rollbackToSavepoint("sp1");

            assertQuery(tx, "SELECT ID, VAL FROM " + TBL + " ORDER BY ID")
                .returns(1, "before_sp1")
                .check();

            tx.commit();
        }

        assertQuery("SELECT ID, VAL FROM " + TBL + " ORDER BY ID")
            .returns(1, "before_sp1")
            .check();
    }

    /** */
    @Test
    public void testSavepointCommandsRequireTransaction() {
        assertThrows(
            "SAVEPOINT sp1",
            IgniteSQLException.class,
            SAVEPOINTS_EXPLICIT_TX_ONLY
        );

        assertThrows(
            "ROLLBACK TO SAVEPOINT sp1",
            IgniteSQLException.class,
            SAVEPOINTS_EXPLICIT_TX_ONLY
        );
    }

    /** */
    @Override protected List<List<?>> sql(String sqlText, Object... args) {
        return client.context().query().querySqlFields(query(sqlText, args), false, false).get(0).getAll();
    }

    /** */
    private QueryChecker assertQuery(Transaction tx, String qry) {
        return new QueryChecker(qry, tx, ALL) {
            /** {@inheritDoc} */
            @Override protected QueryEngine getEngine() {
                return Commons.lookupComponent(client.context(), QueryEngine.class);
            }
        };
    }

    /** */
    private void sqlScript(String sqlText) {
        for (FieldsQueryCursor<List<?>> cur : client.context().query().querySqlFields(query(sqlText), false, false))
            cur.getAll();
    }

    /** */
    private static SqlFieldsQuery query(String sqlText, Object... args) {
        return new SqlFieldsQuery(sqlText)
            .setArgs(args)
            .setTimeout(5, SECONDS);
    }
}
