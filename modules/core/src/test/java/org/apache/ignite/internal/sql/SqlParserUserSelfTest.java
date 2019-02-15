/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
