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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test SQLSTATE codes propagation with (any) Ignite JDBC driver.
 */
public abstract class JdbcErrorsAbstractSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(getConfiguration(getTestIgniteInstanceName(0))
            .setCacheConfiguration(new CacheConfiguration("test")
                .setQueryEntities(Collections.singletonList(new QueryEntity(Integer.class, Integer.class)))));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * Test that H2 specific error codes get propagated to Ignite SQL exceptions.
     * @throws SQLException if failed.
     */
    public void testParsingErrors() throws SQLException {
        checkErrorState("gibberish", "42000");
    }

    /**
     * Test that error codes from tables related DDL operations get propagated to Ignite SQL exceptions.
     * @throws SQLException if failed.
     */
    public void testTableErrors() throws SQLException {
        checkErrorState("DROP TABLE \"PUBLIC\".missing", "42000");
    }

    /**
     * Test that error codes from indexes related DDL operations get propagated to Ignite SQL exceptions.
     * @throws SQLException if failed.
     */
    public void testIndexErrors() throws SQLException {
        checkErrorState("DROP INDEX \"PUBLIC\".missing", "42000");
    }

    /**
     * Test that error codes from DML operations get propagated to Ignite SQL exceptions.
     * @throws SQLException if failed.
     */
    public void testDmlErrors() throws SQLException {
        checkErrorState("INSERT INTO \"test\".INTEGER(_key, _val) values(1, null)", "22004");

        checkErrorState("INSERT INTO \"test\".INTEGER(_key, _val) values(1, 'zzz')", "22004");
    }

    /**
     * Test error code for the case when user attempts to refer a future currently unsupported.
     * @throws SQLException if failed.
     */
    public void testUnsupportedSql() throws SQLException {
        checkErrorState("ALTER TABLE \"test\".Integer DROP COLUMN _key", "0A000");
    }

    /**
     * @return Connection to execute statements on.
     * @throws SQLException if failed.
     */
    protected abstract Connection getConnection() throws SQLException;

    /**
     * Test that running given SQL statement yields expected SQLSTATE code.
     * @param sql statement.
     * @param expState expected SQLSTATE code.
     * @throws SQLException if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private void checkErrorState(final String sql, String expState) throws SQLException {
        checkErrorState(new ConnClosure() {
            @Override public void run(Connection conn) throws Exception {
                try (final PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.execute();
                }
            }
        }, expState);
    }

    /**
     * Test that running given closure yields expected SQLSTATE code.
     * @param clo closure.
     * @param expState expected SQLSTATE code.
     * @throws SQLException if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    protected void checkErrorState(final ConnClosure clo, String expState) throws SQLException {
        checkErrorState(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                try (final Connection conn = getConnection()) {
                    clo.run(conn);

                    fail();

                    return null;
                }
            }
        }, expState);
    }

    /**
     * Test that running given closure yields expected SQLSTATE code.
     * @param clo closure.
     * @param expState expected SQLSTATE code.
     * @throws SQLException if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    protected void checkErrorState(final IgniteCallable<Void> clo, String expState) throws SQLException {
        SQLException ex = (SQLException)GridTestUtils.assertThrows(null, clo, SQLException.class, null);

        assertEquals(expState, ex.getSQLState());
    }

    /**
     * Runnable that accepts a {@link Connection} and can throw an exception.
     */
    protected interface ConnClosure {
        /**
         * @throws Exception On error.
         */
        void run(Connection conn) throws Exception;
    }
}
