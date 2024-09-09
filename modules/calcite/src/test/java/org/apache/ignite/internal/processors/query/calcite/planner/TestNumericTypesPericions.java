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

package org.apache.ignite.internal.processors.query.calcite.planner;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** */
public class TestNumericTypesPericions {
    /** */
    private static final IgniteTypeFactory TYPE_FACTORY = new IgniteTypeFactory();

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
    @Test
    public void testLeastRestrictiveTinyInt() {
        List<TypesHolder> types = Stream.of(
            new TypesHolder(TINYINT, TINYINT, TINYINT),
            new TypesHolder(TINYINT, SMALLINT, SMALLINT),
            new TypesHolder(TINYINT, INTEGER, INTEGER),
            new TypesHolder(TINYINT, FLOAT, FLOAT),
            new TypesHolder(TINYINT, REAL, REAL),
            new TypesHolder(TINYINT, DOUBLE, DOUBLE),
            new TypesHolder(TINYINT, DECIMAL, DECIMAL),
            new TypesHolder(TINYINT, BIGINT, BIGINT)
        ).collect(Collectors.toList());

        doTestExpectedLeastRestrictive(types);
    }

    /** */
    @Test
    public void testLeastRestrictiveSmallInt() {
        List<TypesHolder> types = Stream.of(
            new TypesHolder(SMALLINT, TINYINT, SMALLINT),
            new TypesHolder(SMALLINT, SMALLINT, SMALLINT),
            new TypesHolder(SMALLINT, INTEGER, INTEGER),
            new TypesHolder(SMALLINT, FLOAT, FLOAT),
            new TypesHolder(SMALLINT, REAL, REAL),
            new TypesHolder(SMALLINT, DOUBLE, DOUBLE),
            new TypesHolder(SMALLINT, DECIMAL, DECIMAL),
            new TypesHolder(SMALLINT, BIGINT, BIGINT)
        ).collect(Collectors.toList());

        doTestExpectedLeastRestrictive(types);
    }

    /** */
    @Test
    public void testLeastRestrictiveInteger() {
        List<TypesHolder> types = Stream.of(
            new TypesHolder(INTEGER, TINYINT, INTEGER),
            new TypesHolder(INTEGER, SMALLINT, INTEGER),
            new TypesHolder(INTEGER, INTEGER, INTEGER),
            new TypesHolder(INTEGER, FLOAT, FLOAT),
            new TypesHolder(INTEGER, REAL, REAL),
            new TypesHolder(INTEGER, DOUBLE, DOUBLE),
            new TypesHolder(INTEGER, DECIMAL, DECIMAL),
            new TypesHolder(INTEGER, BIGINT, BIGINT)
        ).collect(Collectors.toList());

        doTestExpectedLeastRestrictive(types);
    }

    /** */
    @Test
    public void testLeastRestrictiveFloat() {
        List<TypesHolder> types = Stream.of(
            new TypesHolder(FLOAT, TINYINT, FLOAT),
            new TypesHolder(FLOAT, SMALLINT, FLOAT),
            new TypesHolder(FLOAT, INTEGER, FLOAT),
            new TypesHolder(FLOAT, FLOAT, FLOAT),
            new TypesHolder(FLOAT, REAL, FLOAT),
            new TypesHolder(FLOAT, DOUBLE, DOUBLE),
            new TypesHolder(FLOAT, DECIMAL, DOUBLE),
            new TypesHolder(FLOAT, BIGINT, FLOAT)
        ).collect(Collectors.toList());

        doTestExpectedLeastRestrictive(types);
    }

    /** */
    @Test
    public void testLeastRestrictiveDouble() {
        List<TypesHolder> types = Stream.of(
            new TypesHolder(DOUBLE, TINYINT, DOUBLE),
            new TypesHolder(DOUBLE, SMALLINT, DOUBLE),
            new TypesHolder(DOUBLE, INTEGER, DOUBLE),
            new TypesHolder(DOUBLE, FLOAT, DOUBLE),
            new TypesHolder(DOUBLE, REAL, DOUBLE),
            new TypesHolder(DOUBLE, DOUBLE, DOUBLE),
            new TypesHolder(DOUBLE, DECIMAL, DOUBLE),
            new TypesHolder(DOUBLE, BIGINT, DOUBLE)
        ).collect(Collectors.toList());

        doTestExpectedLeastRestrictive(types);
    }

    /** */
    @Test
    public void testLeastRestrictiveDecimal() {
        List<TypesHolder> types = Stream.of(
            new TypesHolder(DECIMAL, TINYINT, DECIMAL),
            new TypesHolder(DECIMAL, SMALLINT, DECIMAL),
            new TypesHolder(DECIMAL, INTEGER, DECIMAL),
            new TypesHolder(DECIMAL, FLOAT, DOUBLE),
            new TypesHolder(DECIMAL, REAL, DOUBLE),
            new TypesHolder(DECIMAL, DOUBLE, DOUBLE),
            new TypesHolder(DECIMAL, DECIMAL, DECIMAL),
            new TypesHolder(DECIMAL, BIGINT, DECIMAL)
        ).collect(Collectors.toList());

        doTestExpectedLeastRestrictive(types);
    }

    /** */
    @Test
    public void testLeastRestrictiveBigInt() {
        List<TypesHolder> types = Stream.of(
            new TypesHolder(BIGINT, TINYINT, BIGINT),
            new TypesHolder(BIGINT, SMALLINT, BIGINT),
            new TypesHolder(BIGINT, INTEGER, BIGINT),
            new TypesHolder(BIGINT, FLOAT, FLOAT),
            new TypesHolder(BIGINT, REAL, REAL),
            new TypesHolder(BIGINT, DOUBLE, DOUBLE),
            new TypesHolder(BIGINT, DECIMAL, DECIMAL),
            new TypesHolder(BIGINT, BIGINT, BIGINT)
        ).collect(Collectors.toList());

        doTestExpectedLeastRestrictive(types);
    }

    /** */
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

    /** */
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


    /** */
    @Test
    public void testMaxNumericPrecision() {
        assertEquals(DECIMAL_PRECISION, typeSys.getMaxNumericPrecision());
    }

    /** */
    @Test
    public void testMaxNumericScale() {
        assertEquals(DECIMAL_SCALE, typeSys.getMaxNumericScale());
    }

    /** */
    private static final class TypesHolder {
        /** */
        private final RelDataType type1;

        /** */
        private final RelDataType type2;

        /** */
        private final RelDataType expectedLeast;

        /** */
        private TypesHolder(RelDataType type1, RelDataType type2, @Nullable RelDataType least) {
            this.type1 = type1;
            this.type2 = type2;
            this.expectedLeast = least;
        }
    }

    /** */
    private static void doTestExpectedLeastRestrictive(List<TypesHolder> types) {
        for (TypesHolder holder : types) {
            RelDataType actualType = TYPE_FACTORY.leastRestrictive(Arrays.asList(holder.type1, holder.type2));

            assertEquals("leastRestrictive(" + holder.type1 + ", " + holder.type2 + ")", holder.expectedLeast, actualType);

            assertEquals("leastRestrictive(" + holder.type2 + ", " + holder.type1 + ")", holder.expectedLeast, actualType);
        }
    }
}
