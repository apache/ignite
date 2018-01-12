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
import org.apache.ignite.internal.sql.command.SqlColumn;
import org.apache.ignite.internal.sql.command.SqlColumnType;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.sql.command.SqlCreateTableCommand;
import org.apache.ignite.internal.sql.param.ParamTestUtils;
import org.apache.ignite.internal.sql.param.TestParamDef;
import org.apache.ignite.testframework.GridTestUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.apache.ignite.internal.sql.SqlKeyword.AFFINITY_KEY;
import static org.apache.ignite.internal.sql.SqlKeyword.ATOMICITY;
import static org.apache.ignite.internal.sql.SqlKeyword.BACKUPS;
import static org.apache.ignite.internal.sql.SqlKeyword.BIGINT;
import static org.apache.ignite.internal.sql.SqlKeyword.BIT;
import static org.apache.ignite.internal.sql.SqlKeyword.BOOL;
import static org.apache.ignite.internal.sql.SqlKeyword.BOOLEAN;
import static org.apache.ignite.internal.sql.SqlKeyword.CACHE_GROUP;
import static org.apache.ignite.internal.sql.SqlKeyword.CACHE_NAME;
import static org.apache.ignite.internal.sql.SqlKeyword.CHAR;
import static org.apache.ignite.internal.sql.SqlKeyword.CHARACTER;
import static org.apache.ignite.internal.sql.SqlKeyword.DATA_REGION;
import static org.apache.ignite.internal.sql.SqlKeyword.DATE;
import static org.apache.ignite.internal.sql.SqlKeyword.DATETIME;
import static org.apache.ignite.internal.sql.SqlKeyword.DEC;
import static org.apache.ignite.internal.sql.SqlKeyword.DECIMAL;
import static org.apache.ignite.internal.sql.SqlKeyword.DOUBLE;
import static org.apache.ignite.internal.sql.SqlKeyword.FLOAT;
import static org.apache.ignite.internal.sql.SqlKeyword.FLOAT4;
import static org.apache.ignite.internal.sql.SqlKeyword.FLOAT8;
import static org.apache.ignite.internal.sql.SqlKeyword.INT;
import static org.apache.ignite.internal.sql.SqlKeyword.INT2;
import static org.apache.ignite.internal.sql.SqlKeyword.INT4;
import static org.apache.ignite.internal.sql.SqlKeyword.INT8;
import static org.apache.ignite.internal.sql.SqlKeyword.INTEGER;
import static org.apache.ignite.internal.sql.SqlKeyword.KEY_TYPE;
import static org.apache.ignite.internal.sql.SqlKeyword.LONG;
import static org.apache.ignite.internal.sql.SqlKeyword.LONGVARCHAR;
import static org.apache.ignite.internal.sql.SqlKeyword.MEDIUMINT;
import static org.apache.ignite.internal.sql.SqlKeyword.NCHAR;
import static org.apache.ignite.internal.sql.SqlKeyword.NUMBER;
import static org.apache.ignite.internal.sql.SqlKeyword.NUMERIC;
import static org.apache.ignite.internal.sql.SqlKeyword.NVARCHAR;
import static org.apache.ignite.internal.sql.SqlKeyword.NVARCHAR2;
import static org.apache.ignite.internal.sql.SqlKeyword.REAL;
import static org.apache.ignite.internal.sql.SqlKeyword.SIGNED;
import static org.apache.ignite.internal.sql.SqlKeyword.SMALLDATETIME;
import static org.apache.ignite.internal.sql.SqlKeyword.SMALLINT;
import static org.apache.ignite.internal.sql.SqlKeyword.TEMPLATE;
import static org.apache.ignite.internal.sql.SqlKeyword.TIME;
import static org.apache.ignite.internal.sql.SqlKeyword.TIMESTAMP;
import static org.apache.ignite.internal.sql.SqlKeyword.TINYINT;
import static org.apache.ignite.internal.sql.SqlKeyword.UUID;
import static org.apache.ignite.internal.sql.SqlKeyword.VAL_TYPE;
import static org.apache.ignite.internal.sql.SqlKeyword.VARCHAR;
import static org.apache.ignite.internal.sql.SqlKeyword.VARCHAR2;
import static org.apache.ignite.internal.sql.SqlKeyword.VARCHAR_CASESENSITIVE;
import static org.apache.ignite.internal.sql.SqlKeyword.WRAP_KEY;
import static org.apache.ignite.internal.sql.SqlKeyword.WRAP_VAL;
import static org.apache.ignite.internal.sql.SqlKeyword.WRITE_SYNCHRONIZATION_MODE;
import static org.apache.ignite.internal.sql.SqlKeyword.YEAR;

/**
 * Tests for SQL parser: CREATE TABLE.
 */
@SuppressWarnings({"UnusedReturnValue", "ThrowableNotThrown"})
public class SqlParserCreateTableSelfTest extends SqlParserAbstractSelfTest {

    /** Correct primary key name. */
    private static final String PK_NAME = "pk";

    /** Incorrect primary key name (incorrect identifier). */
    private static final String WRONG_PK_NAME = "wrong-pk";

    /** List of good identifiers. */
    private static final String[] GOOD_IDS = new String[] { "test" };
    /** List of incorrect identifiers. */
    private static final String[] BAD_IDS = new String[] { "1badId", "`badId" };

    /** List of good string values. */
    private static final String GOOD_STR = "test";

    /** Parameter tests. */
    private static final List<TestParamDef<?>> PARAM_TESTS = new LinkedList<>();
    /** Default parameter values (when not specified in SQL command). */
    private static final List<TestParamDef.DefValPair<?>> DEFAULT_PARAM_VALS;

    static {
        PARAM_TESTS.add(new TestParamDef<>(TEMPLATE, "templateName", String.class,
            Arrays.asList(
                new TestParamDef.MissingValue<String>(null),
                new TestParamDef.ValidIdentityValue<>(CacheMode.PARTITIONED.name()),
                new TestParamDef.ValidIdentityValue<>(CacheMode.REPLICATED.name())
        )));

        PARAM_TESTS.add(new TestParamDef<>(BACKUPS, "backups", Integer.class,
            Arrays.asList(
                new TestParamDef.MissingValue<Integer>(null),
                new TestParamDef.ValidIdentityValue<>(0),
                new TestParamDef.ValidIdentityValue<>(1),
                new TestParamDef.ValidIdentityValue<>(Integer.MAX_VALUE)
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
                new TestParamDef.ValidValue<>('"' + PK_NAME + '"', PK_NAME),
                new TestParamDef.InvalidValue<String>(WRONG_PK_NAME,
                    "Unexpected token: \"-\"")
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
                "re:Unexpected token: \".*\" \\(expected: \"\\[identifier\\]\"\\)")));

        PARAM_TESTS.add(new TestParamDef<>(VAL_TYPE, "valueTypeName", String.class,
            ParamTestUtils.makeBasicIdTestValues(GOOD_IDS, BAD_IDS,
                Optional.<String>fromNullable(null), Optional.<String>fromNullable(null),
                "re:Unexpected token: \".*\" \\(expected: \"\\[identifier\\]\"\\)")));

        PARAM_TESTS.add(ParamTestUtils.makeBasicBoolDef(WRAP_KEY, "wrapKey",
            Optional.<Boolean>fromNullable(null)));

        PARAM_TESTS.add(ParamTestUtils.makeBasicBoolDef(WRAP_VAL, "wrapValue",
            Optional.<Boolean>fromNullable(null)));

        DEFAULT_PARAM_VALS = ParamTestUtils.createDefaultParamVals(PARAM_TESTS);
    }

    /** Mapping: supported SQL type -> internal type for types without precision */
    private static final Map<String, SqlColumnType> SIMPLE_COL_TYPES = new HashMap<>();

    static {
        Map<String, SqlColumnType> m = SIMPLE_COL_TYPES;

        m.put(BIT, SqlColumnType.BOOLEAN);
        m.put(BOOL, SqlColumnType.BOOLEAN);
        m.put(BOOLEAN, SqlColumnType.BOOLEAN);

        m.put(TINYINT, SqlColumnType.BYTE);

        m.put(INT2, SqlColumnType.SHORT);
        m.put(SMALLINT, SqlColumnType.SHORT);
        m.put(YEAR, SqlColumnType.SHORT);
        
        m.put(INT, SqlColumnType.INT);
        m.put(INT4, SqlColumnType.INT);
        m.put(INTEGER, SqlColumnType.INT);
        m.put(MEDIUMINT, SqlColumnType.INT);
        m.put(SIGNED, SqlColumnType.INT);

        m.put(BIGINT, SqlColumnType.LONG);
        m.put(INT8, SqlColumnType.LONG);
        m.put(LONG, SqlColumnType.LONG);

        m.put(FLOAT4, SqlColumnType.FLOAT);
        m.put(REAL, SqlColumnType.FLOAT);

        m.put(DOUBLE, SqlColumnType.DOUBLE);

        m.put(FLOAT, SqlColumnType.DOUBLE);
        m.put(FLOAT8, SqlColumnType.DOUBLE);

        m.put(DATE, SqlColumnType.DATE);
        m.put(TIME, SqlColumnType.TIME);

        m.put(DATETIME, SqlColumnType.TIMESTAMP);
        m.put(SMALLDATETIME, SqlColumnType.TIMESTAMP);
        m.put(TIMESTAMP, SqlColumnType.TIMESTAMP);

        m.put(UUID, SqlColumnType.UUID);
    }

    /** Mapping: supported SQL type -> internal type for types with mandatory precision */
    private static final Map<String, SqlColumnType> LENGTH_COL_TYPES = new HashMap<>();

    static {
        Map<String, SqlColumnType> m = LENGTH_COL_TYPES;

        // Currently we don't have such types
    }

    /** Mapping: supported SQL type -> internal type for types with optional precision */
    private static final Map<String, SqlColumnType> OPT_LENGTH_COL_TYPES = new HashMap<>();

    static {
        Map<String, SqlColumnType> m = OPT_LENGTH_COL_TYPES;

        m.put(CHAR, SqlColumnType.CHAR);
        m.put(CHARACTER, SqlColumnType.CHAR);
        m.put(NCHAR, SqlColumnType.CHAR);

        m.put(LONGVARCHAR, SqlColumnType.VARCHAR);
        m.put(NVARCHAR, SqlColumnType.VARCHAR);
        m.put(NVARCHAR2, SqlColumnType.VARCHAR);
        m.put(VARCHAR, SqlColumnType.VARCHAR);
        m.put(VARCHAR2, SqlColumnType.VARCHAR);
        m.put(VARCHAR_CASESENSITIVE, SqlColumnType.VARCHAR);
    }

    /** Mapping: supported SQL type -> internal type for types with precision */
    private static final Map<String, SqlColumnType> OPT_PRECISION_SCALE_COL_TYPES = new HashMap<>();

    static {
        Map<String, SqlColumnType> m = OPT_PRECISION_SCALE_COL_TYPES;

        m.put(DEC, SqlColumnType.DECIMAL);
        m.put(DECIMAL, SqlColumnType.DECIMAL);
        m.put(NUMBER, SqlColumnType.DECIMAL);
        m.put(NUMERIC, SqlColumnType.DECIMAL);
    }

    /**
     * Tests for base CREATE TABLE command syntax.
     */
    public void testBasicSyntax() throws Exception {

        GridTestUtils.runWithH2FallbackDisabled(true, new Callable<Void>() {
            @Override public Void call() {

                assertParseError(null,
                    "CREATE TABLE",
                    "Unexpected end of command (expected: \"[qualified identifier]\", \"IF\")");

                assertParseError(null,
                    "CREATE TABLE tbl",
                    "Unexpected end of command (expected: \"(\")");

                assertParseError(null,
                    "CREATE TABLE (a INT PRIMARY KEY), b CHAR)",
                    "Unexpected token: \"(\" (expected: \"[qualified identifier]\", \"IF\")");

                assertParseError(null,
                    "CREATE TABLE (a int, b varchar)",
                    "Unexpected token: \"(\" (expected: \"[qualified identifier]\", \"IF\")");

                assertParseError(null,
                    "CREATE TABLE tbl (a int, a char)",
                    "Column already defined: A");

                assertParseError(null,
                    "CREATE TABLE tbl (a INT, b CHAR, a INT PRIMARY KEY)",
                    "Column already defined: A");

                assertParseError(null,
                    "CREATE TABLE tbl (a INT, b CHAR, a date PRIMARY KEY)",
                    "Column already defined: A");

                return null;
            }
        });
    }

    /**
     * Verifies that simple SQL types are handled correctly.
     */
    public void testSimpleColTypes() {
        String prefix = "CREATE TABLE tbl (";
        String suffix = ")";

        for (Map.Entry<String, SqlColumnType> typMapping : SIMPLE_COL_TYPES.entrySet()) {
            String sql;

            sql = prefix + "\"" + PK_NAME + "\" " + typMapping.getKey() + "(1,2) PRIMARY KEY" + suffix;

            assertParseError(null, sql,
                "Unexpected token: \"(\"");

            sql = prefix + ("\"" + PK_NAME + "\" " + typMapping.getKey() + " PRIMARY KEY") + suffix;

            SqlCommand cmd = parse(null, sql);

            assertTrue(cmd instanceof SqlCreateTableCommand);

            SqlCreateTableCommand ctCmd = (SqlCreateTableCommand) cmd;

            try {
                assertEquals(1, ctCmd.primaryKeyColumnNames().size());
                assertTrue(ctCmd.primaryKeyColumnNames().contains(PK_NAME));

                assertEquals(1, ctCmd.columns().size());

                SqlColumn col = ctCmd.columns().get(PK_NAME);

                assertEquals(col.name(), PK_NAME);
                assertEquals(col.type(), typMapping.getValue());
                assertNull("precision is not null", col.precision());
                assertNull("scale is not null", col.scale());
                assertNull("isNullable is not null", col.isNullable());
            }
            catch (AssertionError e) {
                throw new AssertionError("Error parsing command '" + sql + "'", e);
            }
        }
    }

    /**
     * Verifies that mandatory {@code (length)} definitions in corresponding SQL types are handled correctly.
     */
    public void testLengthColTypes() {
        String prefix = "CREATE TABLE tbl (";
        String suffix = ")";

        for (Map.Entry<String, SqlColumnType> typMapping : LENGTH_COL_TYPES.entrySet()) {
            String sql = prefix + "\"" + PK_NAME + "\" " + typMapping.getKey() + " PRIMARY KEY" + suffix;

            assertParseError(null, sql,
                "field length is mandatory for this type");

            sql = prefix + "\"" + PK_NAME + "\" " + typMapping.getKey() + "() PRIMARY KEY" + suffix;

            assertParseError(null, sql,
                "Unexpected token: \")\" (expected: \"[integer]\")");

            sql = prefix + "\"" + PK_NAME + "\" " + typMapping.getKey() + "(NULL) PRIMARY KEY" + suffix;

            assertParseError(null, sql,
                "Unexpected token: \"NULL\" (expected: \"[integer]\")");

            for (Integer precision : new Integer[] {Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE}) {
                sql = prefix + "\"" + PK_NAME + "\" " + typMapping.getKey() + "(" + precision + ") PRIMARY KEY" + suffix;

                SqlCommand cmd = parse(null, sql);

                assertTrue(cmd instanceof SqlCreateTableCommand);

                SqlCreateTableCommand ctCmd = (SqlCreateTableCommand)cmd;

                try {
                    assertEquals(1, ctCmd.primaryKeyColumnNames().size());
                    assertTrue(ctCmd.primaryKeyColumnNames().contains(PK_NAME));

                    assertEquals(1, ctCmd.columns().size());

                    SqlColumn col = ctCmd.columns().get(PK_NAME);

                    assertEquals(col.name(), PK_NAME);
                    assertEquals(col.type(), typMapping.getValue());
                    assertEquals(precision, col.precision());
                    assertNull("scale is not null", col.scale());
                    assertNull("isNullable is not null", col.isNullable());
                }
                catch (AssertionError e) {
                    throw new AssertionError("Error parsing command '" + sql + "'", e);
                }
            }
        }
    }

    /**
     * Verifies that optional {@code (length)} definitions in corresponding SQL types are handled correctly.
     */
    public void testOptionalLengthColTypes() {
        String prefix = "CREATE TABLE tbl (";
        String suffix = ")";

        for (Map.Entry<String, SqlColumnType> typMapping : OPT_LENGTH_COL_TYPES.entrySet()) {
            String sql;

            sql = prefix + "\"" + PK_NAME + "\" " + typMapping.getKey() + "() PRIMARY KEY" + suffix;

            assertParseError(null, sql,
                "Unexpected token: \")\" (expected: \"[integer]\")");

            sql = prefix + "\"" + PK_NAME + "\" " + typMapping.getKey() + "(NULL) PRIMARY KEY" + suffix;

            assertParseError(null, sql,
                "Unexpected token: \"NULL\" (expected: \"[integer]\")");

            for (Integer precision : new Integer[] {null, Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE}) {

                sql = prefix + "\"" + PK_NAME + "\" " + typMapping.getKey() +
                    (precision != null ? ("(" + precision + ")") : "") +
                    " PRIMARY KEY" + suffix;

                SqlCommand cmd = parse(null, sql);

                assertTrue(cmd instanceof SqlCreateTableCommand);

                SqlCreateTableCommand ctCmd = (SqlCreateTableCommand)cmd;

                try {
                    assertEquals(1, ctCmd.primaryKeyColumnNames().size());
                    assertTrue(ctCmd.primaryKeyColumnNames().contains(PK_NAME));

                    assertEquals(1, ctCmd.columns().size());

                    SqlColumn col = ctCmd.columns().get(PK_NAME);

                    assertEquals(col.name(), PK_NAME);
                    assertEquals(col.type(), typMapping.getValue());
                    assertEquals(precision, col.precision());
                    assertNull("scale is not null", col.scale());
                    assertNull("isNullable is not null", col.isNullable());
                }
                catch (AssertionError e) {
                    throw new AssertionError("Error parsing command '" + sql + "'", e);
                }
            }
        }
    }

    /**
     * Verifies that optional {@code (scale,precision)} definitions in corresponding SQL types are handled correctly.
     */
    public void testOptionalPrecisionScaleColTypes() {
        String prefix = "CREATE TABLE tbl (";
        String suffix = ")";

        for (Map.Entry<String, SqlColumnType> typMapping : OPT_PRECISION_SCALE_COL_TYPES.entrySet()) {
            String sql;

            sql = prefix + "\"" + PK_NAME + "\" " + typMapping.getKey() + "() PRIMARY KEY" + suffix;

            assertParseError(null, sql,
                "Unexpected token: \")\" (expected: \"[integer]\")");

            sql = prefix + "\"" + PK_NAME + "\" " + typMapping.getKey() + "(NULL) PRIMARY KEY" + suffix;

            assertParseError(null, sql,
                "Unexpected token: \"NULL\" (expected: \"[integer]\")");

            sql = prefix + "\"" + PK_NAME + "\" " + typMapping.getKey() + "(,) PRIMARY KEY" + suffix;

            assertParseError(null, sql,
                "Unexpected token: \",\" (expected: \"[integer]\")");

            sql = prefix + "\"" + PK_NAME + "\" " + typMapping.getKey() + "(,1) PRIMARY KEY" + suffix;

            assertParseError(null, sql,
                "Unexpected token: \",\" (expected: \"[integer]\")");

            for (Integer scale: new Integer[] {Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE}) {

                for (Integer precision: new Integer[] {null, Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE}) {

                    sql = prefix + "\"" + PK_NAME + "\" " + typMapping.getKey() + "(" + scale +
                        (precision != null ? (", " + precision) : "") +
                        ") PRIMARY KEY" + suffix;

                    SqlCommand cmd = parse(null, sql);

                    assertTrue(cmd instanceof SqlCreateTableCommand);

                    SqlCreateTableCommand ctCmd = (SqlCreateTableCommand)cmd;

                    try {
                        assertEquals(1, ctCmd.primaryKeyColumnNames().size());
                        assertTrue(ctCmd.primaryKeyColumnNames().contains(PK_NAME));

                        assertEquals(1, ctCmd.columns().size());

                        SqlColumn col = ctCmd.columns().get(PK_NAME);

                        assertEquals(col.name(), PK_NAME);
                        assertEquals(col.type(), typMapping.getValue());
                        assertEquals(precision, col.precision());
                        assertEquals(scale, col.scale());
                        assertNull("isNullable is not null", col.isNullable());
                    }
                    catch (AssertionError e) {
                        throw new AssertionError("Error parsing command '" + sql + "'", e);
                    }
                }
            }
        }
    }

    /**
     * Tests handling of primary keys in SQL CREATE TABLE command
     */
    public void testPrimaryKeys() {

        assertParseError(null,
            "CREATE TABLE tbl (a INT PRIMARY)",
            "Unexpected token: \")\" (expected: \"KEY\")");

        assertParseError(null,
            "CREATE TABLE tbl (a INT KEY)",
            "Unexpected token: \"KEY\"");

        assertParseError(null,
            "CREATE TABLE tbl (a INT PRIMARY KEY, b CHAR PRIMARY KEY)",
            "PRIMARY KEY is already defined.");

        assertParseError(null,
            "CREATE TABLE tbl (a INT, b CHAR, PRIMARY)",
            "Unexpected token: \")\" (expected: \"KEY\")");

        assertParseError(null,
            "CREATE TABLE tbl (a INT, b CHAR, KEY)",
            "Unexpected token: \"KEY\" (expected: \"[identifier]\")");

        assertParseError(null,
            "CREATE TABLE tbl (a INT, b CHAR, PRIMARY KEY)",
            "Unexpected token: \")\" (expected: \"(\")");

        assertParseError(null,
            "CREATE TABLE tbl (a INT, b CHAR, PRIMARY KEY NULL)",
            "Unexpected token: \"NULL\" (expected: \"(\")");

        assertParseError(null,
            "CREATE TABLE tbl (a INT, b CHAR, PRIMARY KEY ())",
            "Unexpected token: \")\" (expected: \"[identifier]\")");

        assertParseError(null,
            "CREATE TABLE tbl (a INT, b CHAR, PRIMARY KEY (NULL))",
            "Unexpected token: \"NULL\" (expected: \"[identifier]\")");

        assertParseError(null,
            "CREATE TABLE tbl (a INT, b CHAR, PRIMARY KEY (,))",
            "Unexpected token: \",\" (expected: \"[identifier]\")");

        assertParseError(null,
            "CREATE TABLE tbl (a INT, b CHAR PRIMARY KEY (a, b))",
            "Unexpected token: \"(\" (expected: \",\", \")\")");
    }


    /**
     * Tests for parameters of CREATE TABLE command. Each parameter is tested with a list of options.
     *
     * @throws AssertionError If failed.
     */
    @SuppressWarnings("unchecked")
    public void testParams() throws Exception {

        GridTestUtils.runWithH2FallbackDisabled(true, new Callable<Void>() {
            @Override public Void call() {

                String baseCmd = "CREATE TABLE tbl (\"" + PK_NAME + "\" INT PRIMARY KEY, b CHAR)";

                for (TestParamDef testParamDef : PARAM_TESTS)
                    testParameter(null, baseCmd, testParamDef, DEFAULT_PARAM_VALS);

                return null;
            }
        });
    }
}
