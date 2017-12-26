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
import org.apache.ignite.internal.sql.param.ParamTestUtils;
import org.apache.ignite.internal.sql.param.TestParamDef;
import org.apache.ignite.internal.util.H2FallbackTempDisabler;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

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
                new TestParamDef.ValidValue<String>(DEFAULT, null),
                new TestParamDef.ValidIdentityValue<>(CacheMode.PARTITIONED.name()),
                new TestParamDef.ValidIdentityValue<>(CacheMode.REPLICATED.name())
        )));

        PARAM_TESTS.add(new TestParamDef<>(BACKUPS, "backups", Integer.class,
            Arrays.asList(
                new TestParamDef.MissingValue<Integer>(null),
                new TestParamDef.ValidValue<Integer>(DEFAULT, null),
                new TestParamDef.ValidIdentityValue<>(0),
                new TestParamDef.ValidIdentityValue<>(1),
                new TestParamDef.ValidIdentityValue<>(Integer.MAX_VALUE),
                new TestParamDef.InvalidValue<Integer>("-1",
                    "Number of backups should be positive"),
                new TestParamDef.InvalidValue<Integer>(Integer.toString(Integer.MIN_VALUE),
                    "Number of backups should be positive")
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
                    "Affinity key column with given name not found")
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
                "optionally quoted identifier key type")));

        PARAM_TESTS.add(new TestParamDef<>(VAL_TYPE, "valueTypeName", String.class,
            ParamTestUtils.makeBasicIdTestValues(GOOD_IDS, BAD_IDS,
                Optional.<String>fromNullable(null), Optional.<String>fromNullable(null),
                "optionally quoted identifier value type")));

        PARAM_TESTS.add(ParamTestUtils.makeBasicBoolDef(WRAP_KEY, NO_WRAP_KEY, "wrapKey",
            Optional.<Boolean>fromNullable(null), Optional.<Boolean>fromNullable(null)));

        PARAM_TESTS.add(ParamTestUtils.makeBasicBoolDef(WRAP_VALUE, NO_WRAP_VALUE, "wrapValue",
            Optional.<Boolean>fromNullable(null), Optional.<Boolean>fromNullable(null)));

        DEFAULT_PARAM_VALS = createDefaultParamVals(PARAM_TESTS);
    }

    /**
     * Creates {@link SqlParserCreateTableSelfTest#DEFAULT_PARAM_VALS} list by taking defaults from
     * {@link SqlParserCreateTableSelfTest#PARAM_TESTS}.
     *
     * @param paramTests Tests for parameters.
     * @return List with default parameter values.
     */
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
     * Tests for base CREATE TABLE command.
     */
    public void testBasicSyntax() {

        try (H2FallbackTempDisabler disabler = new H2FallbackTempDisabler(true)) {

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
    }

    /**
     * Tests for parameters of CREATE TABLE command. Each parameter is tested with a list of options.
     *
     * @throws AssertionError If failed.
     */
    public void testParams() {

        try (H2FallbackTempDisabler disabler = new H2FallbackTempDisabler(true)) {

            String baseCmd = "CREATE TABLE tbl (\"" + PK_NAME + "\" INT PRIMARY KEY, b VARCHAR)";

            for (TestParamDef testParamDef : PARAM_TESTS)
                testParameter(null, baseCmd, testParamDef, DEFAULT_PARAM_VALS);
        }
    }
}
