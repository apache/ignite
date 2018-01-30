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

import org.apache.ignite.internal.sql.command.SqlCreateUserCommand;

/**
 * Tests for SQL parser: CREATE INDEX.
 */
@SuppressWarnings({"UnusedReturnValue", "ThrowableNotThrown"})
public class SqlParserUserSelfTest extends SqlParserAbstractSelfTest {
    /**
     * Tests for CREATE USER command.
     *
     * @throws Exception If failed.
     */
    public void testCreateUser() throws Exception {
        // Base.
        parseValidateCreate("CREATE USER test WITH PASSWORD 'test'", "TEST", "test");
        parseValidateCreate("CREATE USER \"test\" WITH PASSWORD 'test'", "test", "test");
        parseValidateCreate("CREATE USER \"Test Name\" WITH PASSWORD 'PaSSword'",
            "Test Name", "PaSSword");
        parseValidateCreate("CREATE USER test WITH PASSWORD '~!''@#$%^&*()_+=-`:\"|?.,/'",
            "TEST", "~!'@#$%^&*()_+=-`:\"|?.,/");

        assertParseError(null, "CREATE USER 'test' WITH PASSWORD 'test'",
            "Unexpected token: \"test\" (expected: \"[identifier]\")]");
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
}
