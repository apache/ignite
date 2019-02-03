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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.sql.command.SqlCreateIndexCommand;
import org.apache.ignite.internal.sql.command.SqlIndexColumn;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static org.apache.ignite.internal.sql.SqlKeyword.INLINE_SIZE;
import static org.apache.ignite.internal.sql.SqlKeyword.PARALLEL;

/**
 * Tests for SQL parser: CREATE INDEX.
 */
@SuppressWarnings({"UnusedReturnValue"})
public class SqlParserCreateIndexSelfTest extends SqlParserAbstractSelfTest {
    /** Default properties */
    private static final Map<String, Object> DEFAULT_PROPS = getProps(null, null);

    /**
     * Tests for CREATE INDEX command.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateIndex() throws Exception {
        // Base.
        parseValidate(null, "CREATE INDEX idx ON tbl(a)", null, "TBL", "IDX", DEFAULT_PROPS, "A", false);
        parseValidate(null, "CREATE INDEX idx ON tbl(a);", null, "TBL", "IDX", DEFAULT_PROPS, "A", false);
        parseValidate(null, "CREATE INDEX idx ON tbl(a ASC)", null, "TBL", "IDX", DEFAULT_PROPS, "A", false);
        parseValidate(null, "CREATE INDEX idx ON tbl(a DESC)", null, "TBL", "IDX", DEFAULT_PROPS, "A", true);
        assertParseError(null, "CREATE INDEX idx ON tbl(a) ,", "Unexpected token: \",\"");

        // Case (in)sensitivity.
        parseValidate(null, "CREATE INDEX IDX ON TBL(COL)", null, "TBL", "IDX", DEFAULT_PROPS, "COL", false);
        parseValidate(null, "CREATE INDEX iDx ON tBl(cOl)", null, "TBL", "IDX", DEFAULT_PROPS, "COL", false);

        parseValidate(null, "CREATE INDEX \"idx\" ON tbl(col)", null, "TBL", "idx", DEFAULT_PROPS, "COL", false);
        parseValidate(null, "CREATE INDEX \"iDx\" ON tbl(col)", null, "TBL", "iDx", DEFAULT_PROPS, "COL", false);

        parseValidate(null, "CREATE INDEX idx ON \"tbl\"(col)", null, "tbl", "IDX", DEFAULT_PROPS, "COL", false);
        parseValidate(null, "CREATE INDEX idx ON \"tBl\"(col)", null, "tBl", "IDX", DEFAULT_PROPS, "COL", false);

        parseValidate(null, "CREATE INDEX idx ON tbl(\"col\")", null, "TBL", "IDX", DEFAULT_PROPS, "col", false);
        parseValidate(null, "CREATE INDEX idx ON tbl(\"cOl\")", null, "TBL", "IDX", DEFAULT_PROPS, "cOl", false);

        parseValidate(null, "CREATE INDEX idx ON tbl(\"cOl\" ASC)", null, "TBL", "IDX", DEFAULT_PROPS, "cOl", false);
        parseValidate(null, "CREATE INDEX idx ON tbl(\"cOl\" DESC)", null, "TBL", "IDX", DEFAULT_PROPS, "cOl", true);

        // Columns.
        parseValidate(null, "CREATE INDEX idx ON tbl(a, b)", null, "TBL", "IDX", DEFAULT_PROPS, "A", false, "B", false);

        parseValidate(null, "CREATE INDEX idx ON tbl(a ASC, b)", null, "TBL", "IDX", DEFAULT_PROPS, "A", false, "B", false);
        parseValidate(null, "CREATE INDEX idx ON tbl(a, b ASC)", null, "TBL", "IDX", DEFAULT_PROPS, "A", false, "B", false);
        parseValidate(null, "CREATE INDEX idx ON tbl(a ASC, b ASC)", null, "TBL", "IDX", DEFAULT_PROPS, "A", false, "B", false);

        parseValidate(null, "CREATE INDEX idx ON tbl(a DESC, b)", null, "TBL", "IDX", DEFAULT_PROPS, "A", true, "B", false);
        parseValidate(null, "CREATE INDEX idx ON tbl(a, b DESC)", null, "TBL", "IDX", DEFAULT_PROPS, "A", false, "B", true);
        parseValidate(null, "CREATE INDEX idx ON tbl(a DESC, b DESC)", null, "TBL", "IDX", DEFAULT_PROPS, "A", true, "B", true);

        parseValidate(null, "CREATE INDEX idx ON tbl(a ASC, b DESC)", null, "TBL", "IDX", DEFAULT_PROPS, "A", false, "B", true);
        parseValidate(null, "CREATE INDEX idx ON tbl(a DESC, b ASC)", null, "TBL", "IDX", DEFAULT_PROPS, "A", true, "B", false);

        parseValidate(null, "CREATE INDEX idx ON tbl(a, b, c)", null, "TBL", "IDX", DEFAULT_PROPS, "A", false, "B", false, "C", false);
        parseValidate(null, "CREATE INDEX idx ON tbl(a DESC, b, c)", null, "TBL", "IDX", DEFAULT_PROPS, "A", true, "B", false, "C", false);
        parseValidate(null, "CREATE INDEX idx ON tbl(a, b DESC, c)", null, "TBL", "IDX", DEFAULT_PROPS, "A", false, "B", true, "C", false);
        parseValidate(null, "CREATE INDEX idx ON tbl(a, b, c DESC)", null, "TBL", "IDX", DEFAULT_PROPS, "A", false, "B", false, "C", true);

        // Negative cases.
        assertParseError(null, "CREATE INDEX idx ON tbl()", "Unexpected token");
        assertParseError(null, "CREATE INDEX idx ON tbl(a, a)", "Column already defined: A");
        assertParseError(null, "CREATE INDEX idx ON tbl(a, b, a)", "Column already defined: A");
        assertParseError(null, "CREATE INDEX idx ON tbl(b, a, a)", "Column already defined: A");

        // Tests with schema.
        parseValidate(null, "CREATE INDEX idx ON schema.tbl(a)", "SCHEMA", "TBL", "IDX", DEFAULT_PROPS, "A", false);
        parseValidate(null, "CREATE INDEX idx ON \"schema\".tbl(a)", "schema", "TBL", "IDX", DEFAULT_PROPS, "A", false);
        parseValidate(null, "CREATE INDEX idx ON \"sChema\".tbl(a)", "sChema", "TBL", "IDX", DEFAULT_PROPS, "A", false);

        parseValidate("SCHEMA", "CREATE INDEX idx ON tbl(a)", "SCHEMA", "TBL", "IDX", DEFAULT_PROPS, "A", false);
        parseValidate("schema", "CREATE INDEX idx ON tbl(a)", "schema", "TBL", "IDX", DEFAULT_PROPS, "A", false);
        parseValidate("sChema", "CREATE INDEX idx ON tbl(a)", "sChema", "TBL", "IDX", DEFAULT_PROPS, "A", false);

        // No index name.
        parseValidate(null, "CREATE INDEX ON tbl(a)", null, "TBL", null, DEFAULT_PROPS, "A", false);
        parseValidate(null, "CREATE INDEX ON schema.tbl(a)", "SCHEMA", "TBL", null, DEFAULT_PROPS, "A", false);

        // NOT EXISTS
        SqlCreateIndexCommand cmd;

        cmd = parseValidate(null, "CREATE INDEX idx ON schema.tbl(a)", "SCHEMA", "TBL", "IDX", DEFAULT_PROPS, "A", false);
        assertFalse(cmd.ifNotExists());

        cmd = parseValidate(null, "CREATE INDEX IF NOT EXISTS idx ON schema.tbl(a)", "SCHEMA", "TBL", "IDX", DEFAULT_PROPS, "A", false);
        assertTrue(cmd.ifNotExists());

        assertParseError(null, "CREATE INDEX IF idx ON tbl(a)", "Unexpected token: \"IDX\"");
        assertParseError(null, "CREATE INDEX IF NOT idx ON tbl(a)", "Unexpected token: \"IDX\"");
        assertParseError(null, "CREATE INDEX IF EXISTS idx ON tbl(a)", "Unexpected token: \"EXISTS\"");
        assertParseError(null, "CREATE INDEX NOT EXISTS idx ON tbl(a)", "Unexpected token: \"NOT\"");

        // SPATIAL
        cmd = parseValidate(null, "CREATE INDEX idx ON schema.tbl(a)", "SCHEMA", "TBL", "IDX", DEFAULT_PROPS, "A", false);
        assertFalse(cmd.spatial());

        cmd = parseValidate(null, "CREATE SPATIAL INDEX idx ON schema.tbl(a)", "SCHEMA", "TBL", "IDX", DEFAULT_PROPS, "A", false);
        assertTrue(cmd.spatial());

        // UNIQUE
        assertParseError(null, "CREATE UNIQUE INDEX idx ON tbl(a)", "Unsupported keyword: \"UNIQUE\"");

        // HASH
        assertParseError(null, "CREATE HASH INDEX idx ON tbl(a)", "Unsupported keyword: \"HASH\"");

        // PRIMARY KEY
        assertParseError(null, "CREATE PRIMARY KEY INDEX idx ON tbl(a)", "Unsupported keyword: \"PRIMARY\"");

        // PARALLEL
        parseValidate(null, "CREATE INDEX idx ON tbl(a DESC) PARALLEL 1", null, "TBL", "IDX", getProps(1, null), "A", true);
        parseValidate(null, "CREATE INDEX idx ON tbl(a DESC) PARALLEL  3", null, "TBL", "IDX", getProps(3, null), "A", true);
        parseValidate(null, "CREATE INDEX idx ON tbl(a DESC)   PARALLEL  7", null, "TBL", "IDX", getProps(7, null), "A", true);
        parseValidate(null, "CREATE INDEX idx ON tbl(a DESC)   PARALLEL  0", null, "TBL", "IDX", getProps(0, null), "A", true);
        assertParseError(null, "CREATE INDEX idx ON tbl(a DESC) PARALLEL ", "Failed to parse SQL statement \"CREATE INDEX idx ON tbl(a DESC) PARALLEL [*]\"");
        assertParseError(null, "CREATE INDEX idx ON tbl(a DESC) PARALLEL abc", "Unexpected token: \"ABC\"");
        assertParseError(null, "CREATE INDEX idx ON tbl(a DESC) PARALLEL -2", "Failed to parse SQL statement \"CREATE INDEX idx ON tbl(a DESC) PARALLEL -[*]2\": Illegal PARALLEL value. Should be positive: -2");

        // INLINE_SIZE option
        assertParseError(null, "CREATE INDEX ON tbl(a) INLINE_SIZE",
            "Unexpected end of command (expected: \"[integer]\")");

        assertParseError(null, "CREATE INDEX ON tbl(a) INLINE_SIZE HASH",
            "Unexpected token: \"HASH\" (expected: \"[integer]\")");

        assertParseError(null, "CREATE INDEX ON tbl(a) INLINE_SIZE elegua",
            "Unexpected token: \"ELEGUA\" (expected: \"[integer]\")");

        assertParseError(null, "CREATE INDEX ON tbl(a) INLINE_SIZE -9223372036854775808",
            "Unexpected token: \"9223372036854775808\" (expected: \"[integer]\")");

        assertParseError(null, "CREATE INDEX ON tbl(a) INLINE_SIZE " + Integer.MIN_VALUE,
            "Illegal INLINE_SIZE value. Should be positive: " + Integer.MIN_VALUE);

        assertParseError(null, "CREATE INDEX ON tbl(a) INLINE_SIZE -1", "Failed to parse SQL statement \"CREATE INDEX ON tbl(a) INLINE_SIZE -[*]1\": Illegal INLINE_SIZE value. Should be positive: -1");

        parseValidate(null, "CREATE INDEX idx ON schema.tbl(a) INLINE_SIZE 0", "SCHEMA", "TBL", "IDX", getProps(null, 0), "A", false);
        parseValidate(null, "CREATE INDEX idx ON schema.tbl(a) INLINE_SIZE 1", "SCHEMA", "TBL", "IDX", getProps(null, 1), "A", false);
        parseValidate(null, "CREATE INDEX idx ON schema.tbl(a) INLINE_SIZE " + Integer.MAX_VALUE,
            "SCHEMA", "TBL", "IDX", getProps(null, Integer.MAX_VALUE), "A", false);

        // Both parallel and inline size
        parseValidate(null, "CREATE INDEX idx ON schema.tbl(a) INLINE_SIZE 5 PARALLEL 7", "SCHEMA", "TBL", "IDX", getProps(7, 5), "A", false);
        parseValidate(null, "CREATE INDEX idx ON schema.tbl(a) PARALLEL 3 INLINE_SIZE 9 ", "SCHEMA", "TBL", "IDX", getProps(3, 9), "A", false);

        assertParseError(null, "CREATE INDEX idx ON schema.tbl(a) PARALLEL 3 INLINE_SIZE 9 PARALLEL 2", "Failed to parse SQL statement \"CREATE INDEX idx ON schema.tbl(a) PARALLEL 3 INLINE_SIZE [*]9 PARALLEL 2\": Only one PARALLEL clause may be specified.");
        assertParseError(null, "CREATE INDEX idx ON schema.tbl(a) PARALLEL 3 INLINE_SIZE 9 abc ", "Failed to parse SQL statement \"CREATE INDEX idx ON schema.tbl(a) PARALLEL 3 INLINE_SIZE 9 [*]abc \": Unexpected token: \"ABC\"");
        assertParseError(null, "CREATE INDEX idx ON schema.tbl(a) PARALLEL  INLINE_SIZE 9 abc ", "Failed to parse SQL statement \"CREATE INDEX idx ON schema.tbl(a) PARALLEL  [*]INLINE_SIZE 9 abc \": Unexpected token: \"INLINE_SIZE\" (expected: \"[integer]\")");
        assertParseError(null, "CREATE INDEX idx ON schema.tbl(a) PARALLEL 3 INLINE_SIZE abc ", "Failed to parse SQL statement \"CREATE INDEX idx ON schema.tbl(a) PARALLEL 3 INLINE_SIZE [*]abc \": Unexpected token: \"ABC\" (expected: \"[integer]\")");

    }

    /**
     * Parse and validate SQL script.
     *
     * @param schema Schema.
     * @param sql SQL.
     * @param expSchemaName Expected schema name.
     * @param expTblName Expected table name.
     * @param expIdxName Expected index name.
     * @param props Expected properties.
     * @param expColDefs Expected column definitions.
     * @return Command.
     */
    private static SqlCreateIndexCommand parseValidate(String schema, String sql, String expSchemaName,
        String expTblName, String expIdxName, Map<String, Object> props, Object... expColDefs) {
        SqlCreateIndexCommand cmd = (SqlCreateIndexCommand)new SqlParser(schema, sql).nextCommand();

        validate(cmd, expSchemaName, expTblName, expIdxName, props, expColDefs);

        return cmd;
    }

    /**
     * Validate create index command.
     *
     * @param cmd Command.
     * @param expSchemaName Expected schema name.
     * @param expTblName Expected table name.
     * @param expIdxName Expected index name.
     * @param props Expected properties.
     * @param expColDefs Expected column definitions.
     */
    private static void validate(SqlCreateIndexCommand cmd, String expSchemaName, String expTblName, String expIdxName,
        Map<String, Object> props, Object... expColDefs) {
        assertEquals(expSchemaName, cmd.schemaName());
        assertEquals(expTblName, cmd.tableName());
        assertEquals(expIdxName, cmd.indexName());

        Map<String, Object> cmpProps = getProps(cmd.parallel(), cmd.inlineSize());

        assertEquals(cmpProps, props);

        if (F.isEmpty(expColDefs) || expColDefs.length % 2 == 1)
            throw new IllegalArgumentException("Column definitions must be even.");

        Collection<SqlIndexColumn> cols = cmd.columns();

        assertEquals(expColDefs.length / 2, cols.size());

        Iterator<SqlIndexColumn> colIter = cols.iterator();

        for (int i = 0; i < expColDefs.length;) {
            SqlIndexColumn col = colIter.next();

            String expColName = (String)expColDefs[i++];
            Boolean expDesc = (Boolean) expColDefs[i++];

            assertEquals(expColName, col.name());
            assertEquals(expDesc, (Boolean)col.descending());
        }
    }

    /**
     * Returns map with command properties.
     *
     * @param parallel Parallel property value. <code>Null</code> for a default value.
     * @param inlineSize Inline size property value. <code>Null</code> for a default value.
     * @return Command properties.
     */
    private static Map<String, Object> getProps(Integer parallel, Integer inlineSize) {
        if (parallel == null)
            parallel = 0;

        if (inlineSize == null)
            inlineSize = QueryIndex.DFLT_INLINE_SIZE;

        Map<String, Object> props = new HashMap<>();

        props.put(PARALLEL, parallel);
        props.put(INLINE_SIZE, inlineSize);

        return Collections.unmodifiableMap(props);
    }
}
