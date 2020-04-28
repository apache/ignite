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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.connect;
import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.execute;

/**
 * Test for {@code SELECT FOR UPDATE} queries.
 */
public class CacheMvccSelectForUpdateQueryTest extends GridCommonAbstractTest {
    /** */
    private static final int CACHE_SIZE = 10;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite grid = startGridsMultiThreaded(2);

        try (Connection c = connect((IgniteEx)grid)) {
            c.setAutoCommit(false);

            execute(c, "create table person (id int primary key, firstName varchar, lastName varchar) " +
                "with \"atomicity=transactional_snapshot,cache_name=Person\"");

            execute(c, "create table person_nonMvcc (id int primary key, firstName varchar, lastName varchar) " +
                "with \"atomicity=transactional,cache_name=Person_nonMvcc\"");

            try (Transaction tx = grid(0).transactions().txStart(TransactionConcurrency.PESSIMISTIC,
                TransactionIsolation.REPEATABLE_READ)) {

                for (int i = 1; i <= CACHE_SIZE; i++) {
                    execute(c, "insert into person(id, firstName, lastName) " +
                        "values(" + i + ",'firstName" + i + "','lastName" + i + "')");
                }

                tx.commit();
            }
        }
    }

    /**
     *
     */
    @Test
    public void testSelectForUpdateWithUnion() {
        assertQueryThrows("select id from person union select 1 for update",
            "SELECT UNION FOR UPDATE is not supported.");
    }

    /**
     *
     */
    @Test
    public void testSelectForUpdateWithMultipleStatements() {
        assertQueryThrows("select id from person for update; select firstName from person for update",
            "Multiple statements queries are not supported");
    }

    /**
     *
     */
    @Test
    public void testSelectForUpdateWithJoin() {
        assertQueryThrows("select p1.id from person p1 join person p2 on p1.id = p2.id for update",
            "SELECT FOR UPDATE with joins is not supported.");
    }

    /**
     *
     */
    @Test
    public void testSelectForUpdateWithLimit() {
        assertQueryThrows("select id from person limit 0,5 for update",
            "LIMIT/OFFSET clauses are not supported for SELECT FOR UPDATE.");
    }

    /**
     *
     */
    @Test
    public void testSelectForUpdateWithOffset() {
        assertQueryThrows("select id from person offset 10 for update",
            "LIMIT/OFFSET clauses are not supported for SELECT FOR UPDATE.");
    }

    /**
     *
     */
    @Test
    public void testSelectForUpdateWithDistinct() {
        assertQueryThrows("select distinct firstName from PERSON for update",
            "DISTINCT clause is not supported for SELECT FOR UPDATE.");
    }

    /**
     *
     */
    @Test
    public void testSelectForUpdateWithSubQuery() {
        assertQueryThrows("select id, firstName from PERSON where id = (SELECT COUNT(*) FROM person) for update",
            "Sub queries are not supported for SELECT FOR UPDATE.");
    }

    /**
     *
     */
    @Test
    public void testSelectForUpdateNonMvccCache() {
        assertQueryThrows("select id, firstName from person_nonMvcc for update",
            "SELECT FOR UPDATE query requires transactional cache with MVCC enabled.");
    }

    /**
     *
     */
    @Test
    public void testSelectForUpdateWithGroupings() {
        assertQueryThrows("select count(*) from person for update",
            "SELECT FOR UPDATE with aggregates and/or GROUP BY is not supported.");

        assertQueryThrows("select lastName, count(*) from person group by lastName for update",
            "SELECT FOR UPDATE with aggregates and/or GROUP BY is not supported.");
    }

    /**
     * Test that query throws exception with expected message.
     * @param qry SQL.
     * @param exMsg Expected message.
     */
    private void assertQueryThrows(String qry, String exMsg) {
        assertQueryThrows(qry, exMsg, false);

        assertQueryThrows(qry, exMsg, true);
    }

    /**
     * Test that query throws exception with expected message.
     * @param qry SQL.
     * @param exMsg Expected message.
     * @param loc Local query flag.
     */
    @SuppressWarnings("ThrowableNotThrown")
    private void assertQueryThrows(String qry, String exMsg, boolean loc) {
        Ignite node = grid(0);

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() {
                List r = node.cache("Person").query(new SqlFieldsQuery(qry).setLocal(loc)).getAll();

                return r;
            }
        }, IgniteSQLException.class, exMsg);
    }
}
