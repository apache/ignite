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

package org.apache.ignite.internal.sql;

import org.apache.ignite.internal.sql.command.SqlBeginTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.sql.command.SqlCommitTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlRollbackTransactionCommand;
import org.junit.Test;

/**
 * Tests for processing of keywords BEGIN, COMMIT, ROLLBACK, START.
 */
public class SqlParserTransactionalKeywordsSelfTest extends SqlParserAbstractSelfTest {
    /**
     * Test parsing of different forms of BEGIN/START.
     */
    @Test
    public void testBegin() {
        assertBegin("begin");
        assertBegin("BEGIN");
        assertBegin("BEGIN work");
        assertBegin("begin Transaction");
        assertBegin("StarT TransactioN");

        assertParseError(null, "begin index", "Unexpected token: \"INDEX\"");
        assertParseError(null, "start work", "Unexpected token: \"WORK\" (expected: \"TRANSACTION\")");
        assertParseError(null, "start", "Unexpected end of command (expected: \"TRANSACTION\")");
    }

    /**
     * Test parsing of different forms of COMMIT.
     */
    @Test
    public void testCommit() {
        assertCommit("commit");
        assertCommit("COMMIT transaction");

        assertParseError(null, "commit index", "Unexpected token: \"INDEX\"");
    }

    /**
     * Test parsing of different forms of ROLLBACK.
     */
    @Test
    public void testRollback() {
        assertRollback("rollback");
        assertRollback("ROLLBACK transaction");

        assertParseError(null, "rollback index", "Unexpected token: \"INDEX\"");
    }

    /**
     * Test that given SQL is parsed as a BEGIN command.
     * @param sql command.
     */
    private static void assertBegin(String sql) {
        assertTrue(parse(sql) instanceof SqlBeginTransactionCommand);
    }

    /**
     * Test that given SQL is parsed as a BEGIN command.
     * @param sql command.
     */
    private static void assertCommit(String sql) {
        assertTrue(parse(sql) instanceof SqlCommitTransactionCommand);
    }

    /**
     * Test that given SQL is parsed as a BEGIN command.
     * @param sql command.
     */
    private static void assertRollback(String sql) {
        assertTrue(parse(sql) instanceof SqlRollbackTransactionCommand);
    }

    /**
     * Parse single SQL command.
     * @param sql command.
     * @return parsed command.
     */
    private static SqlCommand parse(String sql) {
        return new SqlParser(null, sql).nextCommand();
    }
}
