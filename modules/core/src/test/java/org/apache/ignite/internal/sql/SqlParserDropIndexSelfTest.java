/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql;

import org.apache.ignite.internal.sql.command.SqlDropIndexCommand;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for SQL parser: CREATE INDEX.
 */
@RunWith(JUnit4.class)
public class SqlParserDropIndexSelfTest extends SqlParserAbstractSelfTest {
    /**
     * Tests for DROP INDEX command.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDropIndex() throws Exception {
        // Base.
        parseValidate(null, "DROP INDEX idx", null, "IDX");
        parseValidate(null, "DROP INDEX IDX", null, "IDX");
        parseValidate(null, "DROP INDEX iDx", null, "IDX");

        parseValidate(null, "DROP INDEX \"idx\"", null, "idx");
        parseValidate(null, "DROP INDEX \"IDX\"", null, "IDX");
        parseValidate(null, "DROP INDEX \"iDx\"", null, "iDx");

        assertParseError(null, "DROP INDEX", "Unexpected");

        // Schema.
        parseValidate("SCHEMA", "DROP INDEX idx", "SCHEMA", "IDX");
        parseValidate("schema", "DROP INDEX idx", "schema", "IDX");
        parseValidate("sChema", "DROP INDEX idx", "sChema", "IDX");

        parseValidate(null, "DROP INDEX \"SCHEMA\".idx", "SCHEMA", "IDX");
        parseValidate(null, "DROP INDEX \"schema\".idx", "schema", "IDX");
        parseValidate(null, "DROP INDEX \"sChema\".idx", "sChema", "IDX");

        parseValidate(null, "DROP INDEX \"schema\".\"idx\"", "schema", "idx");

        assertParseError(null, "DROP INDEX .idx", "Unexpected");

        // IF EXISTS
        SqlDropIndexCommand cmd;

        cmd = parseValidate(null, "DROP INDEX schema.idx", "SCHEMA", "IDX");
        assertFalse(cmd.ifExists());

        cmd = parseValidate(null, "DROP INDEX IF EXISTS schema.idx", "SCHEMA", "IDX");
        assertTrue(cmd.ifExists());

        assertParseError(null, "DROP INDEX IF idx", "Unexpected token: \"IDX\"");

        assertParseError(null, "DROP INDEX EXISTS idx", "Unexpected token: \"EXISTS\"");
    }

    /**
     * Parse and validate SQL script.
     *
     * @param schema Schema.
     * @param sql SQL.
     * @param expSchemaName Expected schema name.
     * @param expIdxName Expected index name.
     * @return Command.
     */
    private static SqlDropIndexCommand parseValidate(String schema, String sql, String expSchemaName,
        String expIdxName) {
        SqlDropIndexCommand cmd = (SqlDropIndexCommand)new SqlParser(schema, sql).nextCommand();

        validate(cmd, expSchemaName, expIdxName);

        return cmd;
    }

    /**
     * Validate command.
     *
     * @param cmd Command.
     * @param expSchemaName Expected schema name.
     * @param expIdxName Expected index name.
     */
    private static void validate(SqlDropIndexCommand cmd, String expSchemaName, String expIdxName) {
        assertEquals(expSchemaName, cmd.schemaName());
        assertEquals(expIdxName, cmd.indexName());
    }
}
