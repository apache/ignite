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

import com.google.common.base.Optional;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.sql.command.SqlCreateTableCommand;
import org.apache.ignite.internal.sql.param.ParamTestUtils;
import org.apache.ignite.internal.sql.param.TestParamDef;
import org.apache.ignite.testframework.StrOrRegex;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.ignite.internal.sql.SqlKeyword.AFFINITY_KEY;
import static org.apache.ignite.internal.sql.SqlKeyword.ATOMICITY;
import static org.apache.ignite.internal.sql.SqlKeyword.BACKUPS;
import static org.apache.ignite.internal.sql.SqlKeyword.CACHE_GROUP;
import static org.apache.ignite.internal.sql.SqlKeyword.CACHE_NAME;
import static org.apache.ignite.internal.sql.SqlKeyword.DATA_REGION;
import static org.apache.ignite.internal.sql.SqlKeyword.DEFAULT;
import static org.apache.ignite.internal.sql.SqlKeyword.KEY_TYPE;
import static org.apache.ignite.internal.sql.SqlKeyword.NO_WRAP_KEY;
import static org.apache.ignite.internal.sql.SqlKeyword.NO_WRAP_VALUE;
import static org.apache.ignite.internal.sql.SqlKeyword.TEMPLATE;
import static org.apache.ignite.internal.sql.SqlKeyword.VAL_TYPE;
import static org.apache.ignite.internal.sql.SqlKeyword.WRAP_KEY;
import static org.apache.ignite.internal.sql.SqlKeyword.WRAP_VALUE;
import static org.apache.ignite.internal.sql.SqlKeyword.WRITE_SYNCHRONIZATION_MODE;

/**
 * Tests for SQL parser: CREATE TABLE.
 */
@SuppressWarnings({"UnusedReturnValue", "ThrowableNotThrown"})
public class SqlParserCreateTableSelfTest extends SqlParserAbstractSelfTest {

    /** FIXME */
    private static final String PARTITIONED_TMPL_NAME = CacheMode.PARTITIONED.name();
    /** FIXME */
    private static final String REPLICATED_TMPL_NAME = CacheMode.REPLICATED.name();

    /** FIXME */
    private static final String PK_NAME = "pk";
    /** FIXME */
    private static final String WRONG_PK_NAME = "wrong-pk";

    /** FIXME */
    private static final String[] GOOD_IDS = new String[] { "test" };
    /** FIXME */
    private static final String[] BAD_IDS = new String[] { "1badId", "`badId" };

    /** FIXME */
    private static final String GOOD_STR = "test";

    /** FIXME */
    private static final List<TestParamDef<?>> PARAM_TESTS = new LinkedList<>();
    /** FIXME */
    private static final List<TestParamDef.DefValPair<?>> DEFAULT_PARAM_VALS;

    static {
        PARAM_TESTS.add(new TestParamDef<>(TEMPLATE, "templateName", String.class,
            Arrays.asList(
                new TestParamDef.MissingValue<String>(null),
                new TestParamDef.ValidValue<String>(DEFAULT, null),
                new TestParamDef.ValidIdentityValue<>(PARTITIONED_TMPL_NAME),
                new TestParamDef.ValidIdentityValue<>(REPLICATED_TMPL_NAME)
        )));

        PARAM_TESTS.add(new TestParamDef<>(BACKUPS, "backups", Integer.class,
            Arrays.asList(
                new TestParamDef.MissingValue<Integer>(null),
                new TestParamDef.ValidValue<Integer>(DEFAULT, null),
                new TestParamDef.ValidIdentityValue<>(0),
                new TestParamDef.ValidIdentityValue<>(1),
                new TestParamDef.ValidIdentityValue<>(Integer.MAX_VALUE),
                new TestParamDef.InvalidValue<Integer>("-1",
                    StrOrRegex.of("Number of backups should be positive")),
                new TestParamDef.InvalidValue<Integer>(Integer.toString(Integer.MIN_VALUE),
                    StrOrRegex.of("Number of backups should be positive"))
        )));

        PARAM_TESTS.add(ParamTestUtils.makeBasicEnumDef(ATOMICITY, "atomicityMode", CacheAtomicityMode.class,
            Optional.<CacheAtomicityMode>fromNullable(null),
            Optional.<CacheAtomicityMode>fromNullable(null)));

        PARAM_TESTS.add(ParamTestUtils.makeBasicEnumDef(WRITE_SYNCHRONIZATION_MODE, "writeSynchronizationMode",
            CacheWriteSynchronizationMode.class,
            Optional.<CacheWriteSynchronizationMode>fromNullable(null),
            Optional.<CacheWriteSynchronizationMode>fromNullable(null)));

        PARAM_TESTS.add(new TestParamDef<>(CACHE_GROUP, "cacheGroup", String.class,
            ParamTestUtils.makeBasicStrTestValues(
                Optional.<String>fromNullable(null), Optional.<String>fromNullable(null),
                GOOD_STR)));

        PARAM_TESTS.add(new TestParamDef<>(AFFINITY_KEY, "affinityKey", String.class,
            Arrays.asList(
                new TestParamDef.MissingValue<String>(null),
                new TestParamDef.ValidValue<>('"' + PK_NAME + '"', PK_NAME.toUpperCase()),
                new TestParamDef.ValidIdentityValue<>(PK_NAME.toUpperCase()),
                new TestParamDef.InvalidValue<String>(WRONG_PK_NAME,
                    StrOrRegex.of("Affinity key column with given name not found"))
            )));

        PARAM_TESTS.add(new TestParamDef<>(CACHE_NAME, "cacheName", String.class,
            ParamTestUtils.makeBasicStrTestValues(
                Optional.<String>fromNullable(null), Optional.<String>fromNullable(null),
                GOOD_STR)));

        PARAM_TESTS.add(new TestParamDef<>(DATA_REGION, "dataRegionName", String.class,
            ParamTestUtils.makeBasicStrTestValues(
                Optional.<String>fromNullable(null), Optional.<String>fromNullable(null),
                GOOD_STR)));

        PARAM_TESTS.add(new TestParamDef<>(KEY_TYPE, "keyTypeName", String.class,
            ParamTestUtils.makeBasicIdTestValues(GOOD_IDS, BAD_IDS,
                Optional.<String>fromNullable(null), Optional.<String>fromNullable(null),
                StrOrRegex.of("optionally quoted identifier key type"))));

        PARAM_TESTS.add(new TestParamDef<>(VAL_TYPE, "valueTypeName", String.class,
            ParamTestUtils.makeBasicIdTestValues(GOOD_IDS, BAD_IDS,
                Optional.<String>fromNullable(null), Optional.<String>fromNullable(null),
                StrOrRegex.of("optionally quoted identifier value type"))));

        PARAM_TESTS.add(ParamTestUtils.makeBasicBoolDef(WRAP_KEY, NO_WRAP_KEY, "wrapKey",
            Optional.<Boolean>fromNullable(null), Optional.<Boolean>fromNullable(null)));

        PARAM_TESTS.add(ParamTestUtils.makeBasicBoolDef(WRAP_VALUE, NO_WRAP_VALUE, "wrapValue",
            Optional.<Boolean>fromNullable(null), Optional.<Boolean>fromNullable(null)));

        DEFAULT_PARAM_VALS = createDefaultParamVals(PARAM_TESTS);
    }

    /** FIXME */
    @SuppressWarnings("unchecked")
    private static List<TestParamDef.DefValPair<?>> createDefaultParamVals(List<TestParamDef<?>> paramTests) {
        List<TestParamDef.DefValPair<?>> defParamVals = new LinkedList<>();

        for (TestParamDef<?> def : paramTests) {

            TestParamDef.Value<?> missingVal = null;

            for (TestParamDef.Value<?> val : def.testValues()) {

                if (val instanceof TestParamDef.MissingValue) {
                    if (missingVal != null)
                        assertEquals("Two or more different missing values", missingVal.fieldValue(), val.fieldValue());
                    else
                        missingVal = val;
                }
            }

            if (missingVal != null)
                defParamVals.add(new TestParamDef.DefValPair(def, missingVal, TestParamDef.Syntax.KEY_EQ_VAL));
        }

        return defParamVals;
    }

    /**
     * Tests for CREATE TABLE command.
     *
     * @throws Exception If failed.
     */
    public void testBasicSyntax() throws Exception {

        assertParseError(null,
            "CREATE TABLE",
            "Unexpected end of command (expected: \"[qualified identifier]\", \"IF\")");

        assertParseError(null,
            "CREATE TABLE tbl",
            "Unexpected end of command (expected: \"(\")");

        assertParseError(null,
            "CREATE TABLE (a INT PRIMARY KEY), b VARCHAR)",
            "Unexpected token: \"(\" (expected: \"[qualified identifier]\", \"IF\")");

        assertParseError(null,
            "CREATE TABLE (a int, b varchar)",
            "Unexpected token: \"(\" (expected: \"[qualified identifier]\", \"IF\")");

        assertParseError(null,
            "CREATE TABLE tbl (a int, a varchar)",
            "Column already defined: A");

        assertParseError(null,
            "CREATE TABLE tbl (a INT, b VARCHAR, a INT PRIMARY KEY)",
            "Column already defined: A");

        assertParseError(null,
            "CREATE TABLE tbl (a INT, b VARCHAR, a date PRIMARY KEY)",
            "Column already defined: A");
    }

    /** FIXME */
    public void testParams() throws Exception {

        String baseCmd = "CREATE TABLE tbl (" + PK_NAME + " INT PRIMARY KEY, b VARCHAR)";

        for (TestParamDef testParamDef : PARAM_TESTS)
            testParameter(null, baseCmd, testParamDef, DEFAULT_PARAM_VALS);


//        // Base.
//        parseValidate(null, "CREATE INDEX idx ON tbl(a)", null, "TBL", "IDX", DEFAULT_PROPS, "A", false);
//        parseValidate(null, "CREATE INDEX idx ON tbl(a ASC)", null, "TBL", "IDX", DEFAULT_PROPS, "A", false);
//        parseValidate(null, "CREATE INDEX idx ON tbl(a DESC)", null, "TBL", "IDX", DEFAULT_PROPS, "A", true);
//
//        // Case (in)sensitivity.
//        parseValidate(null, "CREATE INDEX IDX ON TBL(COL)", null, "TBL", "IDX", DEFAULT_PROPS, "COL", false);
//        parseValidate(null, "CREATE INDEX iDx ON tBl(cOl)", null, "TBL", "IDX", DEFAULT_PROPS, "COL", false);
//
//        parseValidate(null, "CREATE INDEX \"idx\" ON tbl(col)", null, "TBL", "idx", DEFAULT_PROPS, "COL", false);
//        parseValidate(null, "CREATE INDEX \"iDx\" ON tbl(col)", null, "TBL", "iDx", DEFAULT_PROPS, "COL", false);
//
//        parseValidate(null, "CREATE INDEX idx ON \"tbl\"(col)", null, "tbl", "IDX", DEFAULT_PROPS, "COL", false);
//        parseValidate(null, "CREATE INDEX idx ON \"tBl\"(col)", null, "tBl", "IDX", DEFAULT_PROPS, "COL", false);
//
//        parseValidate(null, "CREATE INDEX idx ON tbl(\"col\")", null, "TBL", "IDX", DEFAULT_PROPS, "col", false);
//        parseValidate(null, "CREATE INDEX idx ON tbl(\"cOl\")", null, "TBL", "IDX", DEFAULT_PROPS, "cOl", false);
//
//        parseValidate(null, "CREATE INDEX idx ON tbl(\"cOl\" ASC)", null, "TBL", "IDX", DEFAULT_PROPS, "cOl", false);
//        parseValidate(null, "CREATE INDEX idx ON tbl(\"cOl\" DESC)", null, "TBL", "IDX", DEFAULT_PROPS, "cOl", true);
//
//        // Columns.
//        parseValidate(null, "CREATE INDEX idx ON tbl(a, b)", null, "TBL", "IDX", DEFAULT_PROPS, "A", false, "B", false);
//
//        parseValidate(null, "CREATE INDEX idx ON tbl(a ASC, b)", null, "TBL", "IDX", DEFAULT_PROPS, "A", false, "B", false);
//        parseValidate(null, "CREATE INDEX idx ON tbl(a, b ASC)", null, "TBL", "IDX", DEFAULT_PROPS, "A", false, "B", false);
//        parseValidate(null, "CREATE INDEX idx ON tbl(a ASC, b ASC)", null, "TBL", "IDX", DEFAULT_PROPS, "A", false, "B", false);
//
//        parseValidate(null, "CREATE INDEX idx ON tbl(a DESC, b)", null, "TBL", "IDX", DEFAULT_PROPS, "A", true, "B", false);
//        parseValidate(null, "CREATE INDEX idx ON tbl(a, b DESC)", null, "TBL", "IDX", DEFAULT_PROPS, "A", false, "B", true);
//        parseValidate(null, "CREATE INDEX idx ON tbl(a DESC, b DESC)", null, "TBL", "IDX", DEFAULT_PROPS, "A", true, "B", true);
//
//        parseValidate(null, "CREATE INDEX idx ON tbl(a ASC, b DESC)", null, "TBL", "IDX", DEFAULT_PROPS, "A", false, "B", true);
//        parseValidate(null, "CREATE INDEX idx ON tbl(a DESC, b ASC)", null, "TBL", "IDX", DEFAULT_PROPS, "A", true, "B", false);
//
//        parseValidate(null, "CREATE INDEX idx ON tbl(a, b, c)", null, "TBL", "IDX", DEFAULT_PROPS, "A", false, "B", false, "C", false);
//        parseValidate(null, "CREATE INDEX idx ON tbl(a DESC, b, c)", null, "TBL", "IDX", DEFAULT_PROPS, "A", true, "B", false, "C", false);
//        parseValidate(null, "CREATE INDEX idx ON tbl(a, b DESC, c)", null, "TBL", "IDX", DEFAULT_PROPS, "A", false, "B", true, "C", false);
//        parseValidate(null, "CREATE INDEX idx ON tbl(a, b, c DESC)", null, "TBL", "IDX", DEFAULT_PROPS, "A", false, "B", false, "C", true);
//
//        // Negative cases.
//        assertParseError(null, "CREATE INDEX idx ON tbl()", "Unexpected token");
//        assertParseError(null, "CREATE INDEX idx ON tbl(a, a)", "Column already defined: A");
//        assertParseError(null, "CREATE INDEX idx ON tbl(a, b, a)", "Column already defined: A");
//        assertParseError(null, "CREATE INDEX idx ON tbl(b, a, a)", "Column already defined: A");
//
//        // Tests with schema.
//        parseValidate(null, "CREATE INDEX idx ON schema.tbl(a)", "SCHEMA", "TBL", "IDX", DEFAULT_PROPS, "A", false);
//        parseValidate(null, "CREATE INDEX idx ON \"schema\".tbl(a)", "schema", "TBL", "IDX", DEFAULT_PROPS, "A", false);
//        parseValidate(null, "CREATE INDEX idx ON \"sChema\".tbl(a)", "sChema", "TBL", "IDX", DEFAULT_PROPS, "A", false);
//
//        parseValidate("SCHEMA", "CREATE INDEX idx ON tbl(a)", "SCHEMA", "TBL", "IDX", DEFAULT_PROPS, "A", false);
//        parseValidate("schema", "CREATE INDEX idx ON tbl(a)", "schema", "TBL", "IDX", DEFAULT_PROPS, "A", false);
//        parseValidate("sChema", "CREATE INDEX idx ON tbl(a)", "sChema", "TBL", "IDX", DEFAULT_PROPS, "A", false);
//
//        // No index name.
//        parseValidate(null, "CREATE INDEX ON tbl(a)", null, "TBL", null, DEFAULT_PROPS, "A", false);
//        parseValidate(null, "CREATE INDEX ON schema.tbl(a)", "SCHEMA", "TBL", null, DEFAULT_PROPS, "A", false);
//
//        // NOT EXISTS
//        SqlCreateIndexCommand cmd;
//
//        cmd = parseValidate(null, "CREATE INDEX idx ON schema.tbl(a)", "SCHEMA", "TBL", "IDX", DEFAULT_PROPS, "A", false);
//        assertFalse(cmd.ifNotExists());
//
//        cmd = parseValidate(null, "CREATE INDEX IF NOT EXISTS idx ON schema.tbl(a)", "SCHEMA", "TBL", "IDX", DEFAULT_PROPS, "A", false);
//        assertTrue(cmd.ifNotExists());
//
//        assertParseError(null, "CREATE INDEX IF idx ON tbl(a)", "Unexpected token: \"IDX\"");
//        assertParseError(null, "CREATE INDEX IF NOT idx ON tbl(a)", "Unexpected token: \"IDX\"");
//        assertParseError(null, "CREATE INDEX IF EXISTS idx ON tbl(a)", "Unexpected token: \"EXISTS\"");
//        assertParseError(null, "CREATE INDEX NOT EXISTS idx ON tbl(a)", "Unexpected token: \"NOT\"");
//
//        // SPATIAL
//        cmd = parseValidate(null, "CREATE INDEX idx ON schema.tbl(a)", "SCHEMA", "TBL", "IDX", DEFAULT_PROPS, "A", false);
//        assertFalse(cmd.spatial());
//
//        cmd = parseValidate(null, "CREATE SPATIAL INDEX idx ON schema.tbl(a)", "SCHEMA", "TBL", "IDX", DEFAULT_PROPS, "A", false);
//        assertTrue(cmd.spatial());
//
//        // UNIQUE
//        assertParseError(null, "CREATE UNIQUE INDEX idx ON tbl(a)", "Unsupported keyword: \"UNIQUE\"");
//
//        // HASH
//        assertParseError(null, "CREATE HASH INDEX idx ON tbl(a)", "Unsupported keyword: \"HASH\"");
//
//        // PRIMARY KEY
//        assertParseError(null, "CREATE PRIMARY KEY INDEX idx ON tbl(a)", "Unsupported keyword: \"PRIMARY\"");
//
//        // PARALLEL
//        parseValidate(null, "CREATE INDEX idx ON tbl(a DESC) PARALLEL 1", null, "TBL", "IDX", getProps(1, null), "A", true);
//        parseValidate(null, "CREATE INDEX idx ON tbl(a DESC) PARALLEL  3", null, "TBL", "IDX", getProps(3, null), "A", true);
//        parseValidate(null, "CREATE INDEX idx ON tbl(a DESC)   PARALLEL  7", null, "TBL", "IDX", getProps(7, null), "A", true);
//        parseValidate(null, "CREATE INDEX idx ON tbl(a DESC)   PARALLEL  0", null, "TBL", "IDX", getProps(0, null), "A", true);
//        assertParseError(null, "CREATE INDEX idx ON tbl(a DESC) PARALLEL ", "Failed to parse SQL statement \"CREATE INDEX idx ON tbl(a DESC) PARALLEL [*]\"");
//        assertParseError(null, "CREATE INDEX idx ON tbl(a DESC) PARALLEL abc", "Unexpected token: \"ABC\"");
//        assertParseError(null, "CREATE INDEX idx ON tbl(a DESC) PARALLEL -2", "Failed to parse SQL statement \"CREATE INDEX idx ON tbl(a DESC) PARALLEL -[*]2\": Illegal PARALLEL value. Should be positive: -2");
//
//        // INLINE_SIZE option
//        assertParseError(null, "CREATE INDEX ON tbl(a) INLINE_SIZE",
//            "Unexpected end of command (expected: \"[integer]\")");
//
//        assertParseError(null, "CREATE INDEX ON tbl(a) INLINE_SIZE HASH",
//            "Unexpected token: \"HASH\" (expected: \"[integer]\")");
//
//        assertParseError(null, "CREATE INDEX ON tbl(a) INLINE_SIZE elegua",
//            "Unexpected token: \"ELEGUA\" (expected: \"[integer]\")");
//
//        assertParseError(null, "CREATE INDEX ON tbl(a) INLINE_SIZE -9223372036854775808",
//            "Unexpected token: \"9223372036854775808\" (expected: \"[integer]\")");
//
//        assertParseError(null, "CREATE INDEX ON tbl(a) INLINE_SIZE " + Integer.MIN_VALUE,
//            "Illegal INLINE_SIZE value. Should be positive: " + Integer.MIN_VALUE);
//
//        assertParseError(null, "CREATE INDEX ON tbl(a) INLINE_SIZE -1", "Failed to parse SQL statement \"CREATE INDEX ON tbl(a) INLINE_SIZE -[*]1\": Illegal INLINE_SIZE value. Should be positive: -1");
//
//        parseValidate(null, "CREATE INDEX idx ON schema.tbl(a) INLINE_SIZE 0", "SCHEMA", "TBL", "IDX", getProps(null, 0), "A", false);
//        parseValidate(null, "CREATE INDEX idx ON schema.tbl(a) INLINE_SIZE 1", "SCHEMA", "TBL", "IDX", getProps(null, 1), "A", false);
//        parseValidate(null, "CREATE INDEX idx ON schema.tbl(a) INLINE_SIZE " + Integer.MAX_VALUE,
//            "SCHEMA", "TBL", "IDX", getProps(null, Integer.MAX_VALUE), "A", false);
//
//        // Both parallel and inline size
//        parseValidate(null, "CREATE INDEX idx ON schema.tbl(a) INLINE_SIZE 5 PARALLEL 7", "SCHEMA", "TBL", "IDX", getProps(7, 5), "A", false);
//        parseValidate(null, "CREATE INDEX idx ON schema.tbl(a) PARALLEL 3 INLINE_SIZE 9 ", "SCHEMA", "TBL", "IDX", getProps(3, 9), "A", false);
//
//        assertParseError(null, "CREATE INDEX idx ON schema.tbl(a) PARALLEL 3 INLINE_SIZE 9 PARALLEL 2", "Failed to parse SQL statement \"CREATE INDEX idx ON schema.tbl(a) PARALLEL 3 INLINE_SIZE [*]9 PARALLEL 2\": Only one PARALLEL clause may be specified.");
//        assertParseError(null, "CREATE INDEX idx ON schema.tbl(a) PARALLEL 3 INLINE_SIZE 9 abc ", "Failed to parse SQL statement \"CREATE INDEX idx ON schema.tbl(a) PARALLEL 3 INLINE_SIZE 9 [*]abc \": Unexpected token: \"ABC\"");
//        assertParseError(null, "CREATE INDEX idx ON schema.tbl(a) PARALLEL  INLINE_SIZE 9 abc ", "Failed to parse SQL statement \"CREATE INDEX idx ON schema.tbl(a) PARALLEL  [*]INLINE_SIZE 9 abc \": Unexpected token: \"INLINE_SIZE\" (expected: \"[integer]\")");
//        assertParseError(null, "CREATE INDEX idx ON schema.tbl(a) PARALLEL 3 INLINE_SIZE abc ", "Failed to parse SQL statement \"CREATE INDEX idx ON schema.tbl(a) PARALLEL 3 INLINE_SIZE [*]abc \": Unexpected token: \"ABC\" (expected: \"[integer]\")");
    }



    /**
     * Parse and validate SQL script.
     *
     * @param schema Schema.
     * @param sql SQL.
     * @param expSchemaName Expected schema name.
     * @param expTblName Expected table name.
     * @param props Expected properties.
     * @param expColDefs Expected column definitions.
     * @return Command.
     */
    private static SqlCreateTableCommand parseValidate(String schema, String sql, String expSchemaName,
        String expTblName, Map<String, Object> props, Object... expColDefs) {

        SqlCreateTableCommand cmd = (SqlCreateTableCommand)new SqlParser(schema, sql).nextCommand();

        validate(cmd, expSchemaName, expTblName, props, expColDefs);

        return cmd;
    }

    /**
     * Validate create index command.
     *
     * @param cmd Command.
     * @param expSchemaName Expected schema name.
     * @param expTblName Expected table name.
     * @param props Expected properties.
     * @param expColDefs Expected column definitions.
     */
    private static void validate(SqlCreateTableCommand cmd, String expSchemaName, String expTblName,
        Map<String, Object> props, Object... expColDefs) {

//        assertEquals(expSchemaName, cmd.schemaName());
//        assertEquals(expTblName, cmd.tableName());
//
//        Map<String, Object> cmpProps = getProps(cmd.parallel(), cmd.inlineSize());
//
//        assertEquals(cmpProps, props);
//
//        if (F.isEmpty(expColDefs) || expColDefs.length % 2 == 1)
//            throw new IllegalArgumentException("Column definitions must be even.");
//
//        Map<String, SqlColumn> cols = cmd.columns();
//
//        assertEquals(expColDefs.length / 2, cols.size());
//
//        Iterator<SqlColumn> colIter = cols.values().iterator();
//
//        for (int i = 0; i < expColDefs.length;) {
//            SqlIndexColumn col = colIter.next();
//
//            String expColName = (String)expColDefs[i++];
//            Boolean expDesc = (Boolean) expColDefs[i++];
//
//            assertEquals(expColName, col.name());
//            assertEquals(expDesc, (Boolean)col.descending());
//        }
    }

    /**
     * Returns map with command properties.
     *
     * @param parallel Parallel property value. <code>Null</code> for a default value.
     * @param inlineSize Inline size property value. <code>Null</code> for a default value.
     * @return Command properties.
     */
    private static Map<String, Object> getProps(Integer parallel, Integer inlineSize) {
//        if (parallel == null)
//            parallel = 0;
//
//        if (inlineSize == null)
//            inlineSize = QueryIndex.DFLT_INLINE_SIZE;
//
        Map<String, Object> props = new HashMap<>();
//
//        props.put(PARALLEL, parallel);
//        props.put(INLINE_SIZE, inlineSize);
//
        return Collections.unmodifiableMap(props);
    }


}
