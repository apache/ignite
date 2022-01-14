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

package org.apache.ignite.internal.runner.app.jdbc;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Statement cancel test.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-16205")
public class ItJdbcStatementCancelSelfTest extends ItJdbcAbstractStatementSelfTest {
    /**
     * Trying to cancel stament without query. In given case cancel is noop, so no exception expected.
     */
    @Test
    public void testCancelingStmtWithoutQuery() {
        try {
            stmt.cancel();
        } catch (Exception e) {
            log.error("Unexpected exception.", e);

            fail("Unexpected exception");
        }
    }

    /**
     * Trying to retrieve result set of a canceled query.
     * SQLException with message "The query was cancelled while executing." expected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testResultSetRetrievalInCanceledStatement() throws Exception {
        stmt.execute("SELECT 1; SELECT 2; SELECT 3;");

        assertNotNull(stmt.getResultSet());

        stmt.cancel();

        assertThrows(SQLException.class, () -> stmt.getResultSet(), "The query was cancelled while executing.");
    }

    /**
     * Trying to cancel already cancelled query.
     * No exceptions exceped.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCancelCanceledQuery() throws Exception {
        stmt.execute("SELECT 1;");

        assertNotNull(stmt.getResultSet());

        stmt.cancel();

        stmt.cancel();

        assertThrows(SQLException.class, () -> stmt.getResultSet(), "The query was cancelled while executing.");
    }

    /**
     * Trying to cancel closed query.
     * SQLException with message "Statement is closed." expected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCancelClosedStmt() throws Exception {
        stmt.close();

        assertThrows(SQLException.class, () -> stmt.cancel(), "Statement is closed.");
    }

    /**
     * Trying to call <code>resultSet.next()</code> on a canceled query.
     * SQLException with message "The query was cancelled while executing." expected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testResultSetNextAfterCanceling() throws Exception {
        stmt.setFetchSize(10);

        ResultSet rs = stmt.executeQuery("select * from PUBLIC.PERSON");

        assertTrue(rs.next());

        stmt.cancel();

        assertThrows(SQLException.class, rs::next, "The query was cancelled while executing.");
    }

    /**
     * Ensure that it's possible to execute new query on cancelled statement.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCancelAnotherStmt() throws Exception {
        stmt.setFetchSize(10);

        ResultSet rs = stmt.executeQuery("select * from PUBLIC.PERSON");

        assertTrue(rs.next());

        stmt.cancel();

        ResultSet rs2 = stmt.executeQuery("select * from PUBLIC.PERSON order by ID asc");

        assertTrue(rs2.next(), "The other cursor mustn't be closed");
    }

    /**
     * Ensure that stament cancel doesn't affect another statement workflow, created by the same connection.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCancelAnotherStmtResultSet() throws Exception {
        try (Statement anotherStmt = conn.createStatement()) {
            ResultSet rs1 = stmt.executeQuery("select * from PUBLIC.PERSON WHERE ID % 2 = 0");

            ResultSet rs2 = anotherStmt.executeQuery("select * from PUBLIC.PERSON  WHERE ID % 2 <> 0");

            stmt.cancel();

            assertThrows(SQLException.class, rs1::next, "The query was cancelled while executing.");

            assertTrue(rs2.next(), "The other cursor mustn't be closed");
        }
    }
}
