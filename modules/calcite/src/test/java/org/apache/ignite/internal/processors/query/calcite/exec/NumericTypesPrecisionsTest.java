/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.Arrays;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Test for numeric types precisions. */
public class NumericTypesPrecisionsTest {
    /** */
    private static final IgniteTypeFactory TYPE_FACTORY = Commons.typeFactory();

    /** */
    private static final int STRING_PRECISION = 65536;

    /** */
    private static final int DECIMAL_PRECISION = 32767;

    /** */
    private static final int DECIMAL_SCALE = 32767;

    /** */
    private final RelDataTypeSystem typeSys = IgniteTypeSystem.INSTANCE;

    /** */
    private static final RelDataType TINYINT = TYPE_FACTORY.createSqlType(SqlTypeName.TINYINT);

    /** */
    private static final RelDataType SMALLINT = TYPE_FACTORY.createSqlType(SqlTypeName.SMALLINT);

    /** */
    private static final RelDataType INTEGER = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    /** */
    private static final RelDataType REAL = TYPE_FACTORY.createSqlType(SqlTypeName.REAL);

    /** */
    private static final RelDataType FLOAT = TYPE_FACTORY.createSqlType(SqlTypeName.FLOAT);

    /** */
    private static final RelDataType DOUBLE = TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE);

    /** */
    private static final RelDataType DECIMAL = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 1000, 10);

    /** */
    private static final RelDataType BIGINT = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);

    /** */
    private static final RelDataType[] TEST_SUITE = new RelDataType[] {TINYINT, SMALLINT, INTEGER, REAL, FLOAT, DOUBLE, DECIMAL, BIGINT};

    /** */
    @Test
    public void testLeastRestrictiveTinyInt() {
        RelDataType[] expected = new RelDataType[] {TINYINT, SMALLINT, INTEGER, REAL, FLOAT, DOUBLE, DECIMAL, BIGINT};

        doTestExpectedLeastRestrictive(TINYINT, expected);
    }

    /** */
    @Test
    public void testLeastRestrictiveSmallInt() {
        RelDataType[] expected = new RelDataType[] {SMALLINT, SMALLINT, INTEGER, REAL, FLOAT, DOUBLE, DECIMAL, BIGINT};

        doTestExpectedLeastRestrictive(SMALLINT, expected);
    }

    /** */
    @Test
    public void testLeastRestrictiveInteger() {
        RelDataType[] expected = new RelDataType[] {INTEGER, INTEGER, INTEGER, REAL, FLOAT, DOUBLE, DECIMAL, BIGINT};

        doTestExpectedLeastRestrictive(INTEGER, expected);
    }

    /** */
    @Test
    public void testLeastRestrictiveFloat() {
        RelDataType[] expected = new RelDataType[] {FLOAT, FLOAT, FLOAT, FLOAT, FLOAT, DOUBLE, DOUBLE, FLOAT};

        doTestExpectedLeastRestrictive(FLOAT, expected);
    }

    /** */
    @Test
    public void testLeastRestrictiveDouble() {
        RelDataType[] expected = new RelDataType[] {DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE};

        doTestExpectedLeastRestrictive(DOUBLE, expected);
    }

    /** */
    @Test
    public void testLeastRestrictiveDecimal() {
        RelDataType[] expected = new RelDataType[] {DECIMAL, DECIMAL, DECIMAL, DOUBLE, DOUBLE, DOUBLE, DECIMAL, DECIMAL};

        doTestExpectedLeastRestrictive(DECIMAL, expected);
    }

    /** */
    @Test
    public void testLeastRestrictiveBigInt() {
        RelDataType[] expected = new RelDataType[] {BIGINT, BIGINT, BIGINT, REAL, FLOAT, DOUBLE, DECIMAL, BIGINT};

        doTestExpectedLeastRestrictive(BIGINT, expected);
    }

    /** This test is mostly for tracking possible chages with Calcite's version updates. */
    @Test
    public void testMaxPrecision() {
        assertEquals(STRING_PRECISION, typeSys.getMaxPrecision(SqlTypeName.CHAR));
        assertEquals(STRING_PRECISION, typeSys.getMaxPrecision(SqlTypeName.VARCHAR));
        assertEquals(STRING_PRECISION, typeSys.getMaxPrecision(SqlTypeName.BINARY));
        assertEquals(STRING_PRECISION, typeSys.getMaxPrecision(SqlTypeName.VARBINARY));

        assertEquals(3, typeSys.getMaxPrecision(SqlTypeName.TINYINT));
        assertEquals(5, typeSys.getMaxPrecision(SqlTypeName.SMALLINT));
        assertEquals(10, typeSys.getMaxPrecision(SqlTypeName.INTEGER));
        assertEquals(19, typeSys.getMaxPrecision(SqlTypeName.BIGINT));
        assertEquals(7, typeSys.getMaxPrecision(SqlTypeName.REAL));
        assertEquals(15, typeSys.getMaxPrecision(SqlTypeName.FLOAT));
        assertEquals(15, typeSys.getMaxPrecision(SqlTypeName.DOUBLE));
        assertEquals(DECIMAL_PRECISION, typeSys.getMaxPrecision(SqlTypeName.DECIMAL));

        assertEquals(0, typeSys.getMaxPrecision(SqlTypeName.DATE));
        assertEquals(3, typeSys.getMaxPrecision(SqlTypeName.TIME));
        assertEquals(3, typeSys.getMaxPrecision(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE));
        assertEquals(3, typeSys.getMaxPrecision(SqlTypeName.TIMESTAMP));
        assertEquals(3, typeSys.getMaxPrecision(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE));
    }

    /** This test is mostly for tracking possible chages with Calcite's version updates. */
    @Test
    public void testDefaultPrecision() {
        assertEquals(1, typeSys.getDefaultPrecision(SqlTypeName.CHAR));
        assertEquals(RelDataType.PRECISION_NOT_SPECIFIED, typeSys.getDefaultPrecision(SqlTypeName.VARCHAR));
        assertEquals(1, typeSys.getDefaultPrecision(SqlTypeName.BINARY));
        assertEquals(RelDataType.PRECISION_NOT_SPECIFIED, typeSys.getDefaultPrecision(SqlTypeName.VARBINARY));

        assertEquals(3, typeSys.getDefaultPrecision(SqlTypeName.TINYINT));
        assertEquals(5, typeSys.getDefaultPrecision(SqlTypeName.SMALLINT));
        assertEquals(10, typeSys.getDefaultPrecision(SqlTypeName.INTEGER));
        assertEquals(19, typeSys.getDefaultPrecision(SqlTypeName.BIGINT));
        assertEquals(7, typeSys.getDefaultPrecision(SqlTypeName.REAL));
        assertEquals(15, typeSys.getDefaultPrecision(SqlTypeName.FLOAT));
        assertEquals(15, typeSys.getDefaultPrecision(SqlTypeName.DOUBLE));
        assertEquals(DECIMAL_PRECISION, typeSys.getDefaultPrecision(SqlTypeName.DECIMAL));

        assertEquals(0, typeSys.getDefaultPrecision(SqlTypeName.DATE));
        assertEquals(0, typeSys.getDefaultPrecision(SqlTypeName.TIME));
        assertEquals(3, typeSys.getDefaultPrecision(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE));
        assertEquals(3, typeSys.getDefaultPrecision(SqlTypeName.TIMESTAMP));
        assertEquals(0, typeSys.getDefaultPrecision(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE));
    }


    /** This test is mostly for tracking possible chages with Calcite's version updates. */
    @Test
    public void testMaxNumericPrecision() {
        assertEquals(DECIMAL_PRECISION, typeSys.getMaxNumericPrecision());
    }

    /** This test is mostly for tracking possible chages with Calcite's version updates. */
    @Test
    public void testMaxNumericScale() {
        assertEquals(DECIMAL_SCALE, typeSys.getMaxNumericScale());
    }

    /** */
    private static void doTestExpectedLeastRestrictive(RelDataType testType, RelDataType[] expectedLeast) {
        assert expectedLeast.length == TEST_SUITE.length;

        for (int i = 0; i < TEST_SUITE.length; ++i) {
            RelDataType actualType = TYPE_FACTORY.leastRestrictive(Arrays.asList(testType, TEST_SUITE[i]));

            assertEquals("leastRestrictive(" + testType + ", " + TEST_SUITE[i] + ")", expectedLeast[i], actualType);
        }
    }
}
