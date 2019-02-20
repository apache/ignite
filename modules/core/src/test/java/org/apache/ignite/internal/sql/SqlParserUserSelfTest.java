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

import org.apache.ignite.internal.sql.command.SqlAlterUserCommand;
import org.apache.ignite.internal.sql.command.SqlCreateUserCommand;
import org.apache.ignite.internal.sql.command.SqlDropUserCommand;
import org.junit.Test;

/**
 * Tests for SQL parser: CREATE INDEX.
 */
@SuppressWarnings({"UnusedReturnValue"})
public class SqlParserUserSelfTest extends SqlParserAbstractSelfTest {
    /**
     * Tests for CREATE USER command.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateUser() throws Exception {
        // Base.
        parseValidateCreate("CREATE USER test WITH PASSWORD 'test'", "TEST", "test");
        parseValidateCreate("CREATE USER \"test\" WITH PASSWORD 'test'", "test", "test");
        parseValidateCreate("CREATE USER \"Test Name\" WITH PASSWORD 'PaSSword'",
            "Test Name", "PaSSword");
        parseValidateCreate("CREATE USER test WITH PASSWORD '~!''@#$%^&*()_+=-`:\"|?.,/'",
            "TEST", "~!'@#$%^&*()_+=-`:\"|?.,/");

        assertParseError(null, "CREATE USER 'test' WITH PASSWORD 'test'",
            "Unexpected token: \"test\" (expected: \"[username identifier]\")");
        assertParseError(null, "CREATE USER \"PUBLIC\".\"test\" WITH PASSWORD 'test'",
            "Unexpected token: \".\" (expected: \"WITH\")");
    }

    /**
     * Tests for ALTER USER command.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAlterUser() throws Exception {
        // Base.
        parseValidateAlter("ALTER USER test WITH PASSWORD 'test'", "TEST", "test");
        parseValidateAlter("ALTER USER \"test\" WITH PASSWORD 'test'", "test", "test");
        parseValidateAlter("ALTER USER \"Test Name\" WITH PASSWORD 'PaSSword'",
            "Test Name", "PaSSword");
        parseValidateAlter("ALTER USER test WITH PASSWORD '~!''@#$%^&*()_+=-`:\"|?.,/'",
            "TEST", "~!'@#$%^&*()_+=-`:\"|?.,/");

        assertParseError(null, "ALTER USER 'test' WITH PASSWORD 'test'",
            "Unexpected token: \"test\" (expected: \"[username identifier]\")");
        assertParseError(null, "ALTER USER \"PUBLIC\".\"test\" WITH PASSWORD 'test'",
            "Unexpected token: \".\" (expected: \"WITH\")");
    }

    /**
     * Tests for ALTER USER command.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDropUser() throws Exception {
        // Base.
        parseValidateDrop("DROP USER test", "TEST");
        parseValidateDrop("DROP USER \"test\"", "test");
        parseValidateDrop("DROP USER \"Test Name\"", "Test Name");

        assertParseError(null, "DROP USER 'test'",
            "Unexpected token: \"test\" (expected: \"[username identifier]\")");
        assertParseError(null, "DROP USER \"PUBLIC\".\"test\"",
            "Unexpected token: \".\"");
    }

    /**
     * Parse and validate SQL script.
     *
     * @param sql SQL.
     * @param expUserName Expected user name.
     * @param expPasswd Expected user password.
     * @return Command.
     */
    private static SqlCreateUserCommand parseValidateCreate(String sql, String expUserName, String expPasswd) {
        SqlCreateUserCommand cmd = (SqlCreateUserCommand)new SqlParser(null, sql).nextCommand();

        assertEquals(expUserName, cmd.userName());
        assertEquals(expPasswd, cmd.password());

        return cmd;
    }

    /**
     * Parse and validate SQL script.
     *
     * @param sql SQL.
     * @param expUserName Expected user name.
     * @param expPasswd Expected user password.
     * @return Command.
     */
    private static SqlAlterUserCommand parseValidateAlter(String sql, String expUserName, String expPasswd) {
        SqlAlterUserCommand cmd = (SqlAlterUserCommand)new SqlParser(null, sql).nextCommand();

        assertEquals(expUserName, cmd.userName());
        assertEquals(expPasswd, cmd.password());

        return cmd;
    }

    /**
     * Parse and validate SQL script.
     *
     * @param sql SQL.
     * @param expUserName Expected user name.
     * @return Command.
     */
    private static SqlDropUserCommand parseValidateDrop(String sql, String expUserName) {
        SqlDropUserCommand cmd = (SqlDropUserCommand)new SqlParser(null, sql).nextCommand();

        assertEquals(expUserName, cmd.userName());

        return cmd;
    }
}
