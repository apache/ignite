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

import org.apache.ignite.internal.sql.command.SqlCreateViewCommand;
import org.apache.ignite.internal.sql.command.SqlDropViewCommand;
import org.junit.Test;

/**
 * Tests for SQL parser: CREATE VIEW.
 */
public class SqlParserViewSelfTest extends SqlParserAbstractSelfTest {
    /**
     * Test for CREATE VIEW command.
     */
    @Test
    public void testCreateView() {
        parseValidateCreate("CREATE VIEW test AS SELECT * FROM test2",
            null, "TEST", "SELECT * FROM test2", false);

        parseValidateCreate("CREATE OR REPLACE VIEW test AS SELECT * FROM test2",
            null, "TEST", "SELECT * FROM test2", true);

        parseValidateCreate("CREATE VIEW test.test AS SELECT * FROM test2",
            "TEST", "TEST", "SELECT * FROM test2", false);

        parseValidateCreate("CREATE VIEW \"test\" AS SELECT * FROM test2",
            null, "test", "SELECT * FROM test2", false);

        parseValidateCreate("CREATE VIEW \"test\".\"test\" AS SELECT * FROM test2",
            "test", "test", "SELECT * FROM test2", false);

        parseValidateCreate("CREATE VIEW \"te.st\".\"te.st\" AS SELECT * FROM test2",
            "te.st", "te.st", "SELECT * FROM test2", false);

        parseValidateCreate("CREATE VIEW test.\"test\" AS SELECT * FROM test2",
            "TEST", "test", "SELECT * FROM test2", false);

        parseValidateCreate("CREATE VIEW test AS SELECT * FROM test2;",
            null, "TEST", "SELECT * FROM test2", false);

        parseValidateCreate("CREATE VIEW test AS SELECT * FROM test2 ;",
            null, "TEST", "SELECT * FROM test2", false);

        parseValidateCreate("CREATE VIEW test AS SELECT * FROM test2; SELECT * FROM test",
            null, "TEST", "SELECT * FROM test2", false);

        assertParseError(null, "CREATE VIEW test.test.test AS SELECT * FROM test2",
            "Unexpected token: \".\" (expected: \"AS\")");

        assertParseError(null, "CREATE VIEW test AS UNEXPECTED",
            "Unexpected token: \"UNEXPECTED\" (expected: \"SELECT\")");

        assertParseError(null, "CREATE VIEW test AS \"SELECT * FROM test2\"",
            "Unexpected token: \"SELECT * FROM test2\" (expected: \"SELECT\")");

        assertParseError(null, "CREATE OR REPLACE INDEX test",
            "Unexpected token: \"INDEX\" (expected: \"VIEW\")");

        assertParseError(null, "CREATE OR DROP VIEW test AS SELECT * FROM test2",
            "Unexpected token: \"DROP\" (expected: \"REPLACE\")");
    }

    /**
     * Test for DROP VIEW command.
     */
    @Test
    public void testDropView() {
        parseValidateDrop("DROP VIEW test", null, "TEST", false);
        parseValidateDrop("DROP VIEW test.test", "TEST", "TEST", false);
        parseValidateDrop("DROP VIEW test.\"test\"", "TEST", "test", false);
        parseValidateDrop("DROP VIEW \"test\".test", "test", "TEST", false);
        parseValidateDrop("DROP VIEW \"te.st\".\"te.st\"", "te.st", "te.st", false);
        parseValidateDrop("DROP VIEW IF EXISTS test", null, "TEST", true);

        assertParseError(null, "DROP VIEW test.test.test",
            "Unexpected token: \".\"");

        assertParseError(null, "DROP VIEW 'test'",
            "Unexpected token: \"test\" (expected: \"[qualified identifier]\", \"IF\")");

        assertParseError(null, "DROP VIEW IF EXISTS 'test'",
            "Unexpected token: \"test\" (expected: \"[qualified identifier]\")");
    }

    /**
     * Parse and validate CREATE VIEW command.
     */
    private static void parseValidateCreate(
        String sql,
        String schemaName,
        String viewName,
        String viewSql,
        boolean replace
    ) {
        SqlCreateViewCommand cmd = (SqlCreateViewCommand)new SqlParser(null, sql).nextCommand();

        assertEquals(schemaName, cmd.schemaName());
        assertEquals(viewName, cmd.viewName());
        assertEquals(viewSql, cmd.viewSql());
        assertEquals(replace, cmd.replace());
    }

    /**
     * Parse and validate DROP VIEW command.
     */
    private static void parseValidateDrop(String sql, String schemaName, String viewName, boolean ifExists) {
        SqlDropViewCommand cmd = (SqlDropViewCommand)new SqlParser(null, sql).nextCommand();

        assertEquals(schemaName, cmd.schemaName());
        assertEquals(viewName, cmd.viewName());
        assertEquals(ifExists, cmd.ifExists());
    }
}
