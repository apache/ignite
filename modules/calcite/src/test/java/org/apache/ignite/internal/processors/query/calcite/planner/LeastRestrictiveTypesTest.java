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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests the behaviour of {@link IgniteTypeFactory#leastRestrictive(List)}.
 */
public class LeastRestrictiveTypesTest {

    private static final IgniteTypeFactory TYPE_FACTORY = new IgniteTypeFactory();

    private static final RelDataType TINYINT = TYPE_FACTORY.createSqlType(SqlTypeName.TINYINT);

    private static final RelDataType SMALLINT = TYPE_FACTORY.createSqlType(SqlTypeName.SMALLINT);

    private static final RelDataType INTEGER = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    private static final RelDataType FLOAT = TYPE_FACTORY.createSqlType(SqlTypeName.FLOAT);

    private static final RelDataType DOUBLE = TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE);

    private static final RelDataType REAL = TYPE_FACTORY.createSqlType(SqlTypeName.REAL);

    private static final RelDataType BIGINT = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);

    private static final RelDataType DECIMAL = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 1000, 10);

    @Test
    public void testTinyInt() {
        for(List<Object> paramSet: tinyIntTests()){
            RelDataType t1 = (RelDataType)paramSet.get(0);
            RelDataType t2 = (RelDataType)paramSet.get(1);
            LeastRestrictiveType leastRestrictiveType = (LeastRestrictiveType)paramSet.get(2);

            expectLeastRestrictiveType(t1, t2, leastRestrictiveType);
            expectLeastRestrictiveType(t2, t1, leastRestrictiveType);
        }
    }

    private static List<List<Object>> tinyIntTests() {
        List<List<Object>> tests = new ArrayList<>();

        tests.add(Stream.of(TINYINT, TINYINT, new LeastRestrictiveType(TINYINT)).collect(Collectors.toList()));
        tests.add(Stream.of(TINYINT, SMALLINT, new LeastRestrictiveType(SMALLINT)).collect(Collectors.toList()));
        tests.add(Stream.of(TINYINT, INTEGER, new LeastRestrictiveType(INTEGER)).collect(Collectors.toList()));
        tests.add(Stream.of(TINYINT, FLOAT, new LeastRestrictiveType(FLOAT)).collect(Collectors.toList()));
        tests.add(Stream.of(TINYINT, REAL, new LeastRestrictiveType(REAL)).collect(Collectors.toList()));
        tests.add(Stream.of(TINYINT, DOUBLE, new LeastRestrictiveType(DOUBLE)).collect(Collectors.toList()));
        tests.add(Stream.of(TINYINT, DECIMAL, new LeastRestrictiveType(DECIMAL)).collect(Collectors.toList()));
        tests.add(Stream.of(TINYINT, BIGINT, new LeastRestrictiveType(BIGINT)).collect(Collectors.toList()));

        return tests;
    }

    @Test
    public void testSmallInt() {
        for(List<Object> paramSet: smallIntTests()){
            RelDataType t1 = (RelDataType)paramSet.get(0);
            RelDataType t2 = (RelDataType)paramSet.get(1);
            LeastRestrictiveType leastRestrictiveType = (LeastRestrictiveType)paramSet.get(2);

            expectLeastRestrictiveType(t1, t2, leastRestrictiveType);
            expectLeastRestrictiveType(t2, t1, leastRestrictiveType);
        }
    }

    private static List<List<Object>> smallIntTests() {
        List<List<Object>> tests = new ArrayList<>();

        tests.add(Stream.of(SMALLINT, TINYINT, new LeastRestrictiveType(SMALLINT)).collect(Collectors.toList()));
        tests.add(Stream.of(SMALLINT, SMALLINT, new LeastRestrictiveType(SMALLINT)).collect(Collectors.toList()));
        tests.add(Stream.of(SMALLINT, INTEGER, new LeastRestrictiveType(INTEGER)).collect(Collectors.toList()));
        tests.add(Stream.of(SMALLINT, FLOAT, new LeastRestrictiveType(FLOAT)).collect(Collectors.toList()));
        tests.add(Stream.of(SMALLINT, REAL, new LeastRestrictiveType(REAL)).collect(Collectors.toList()));
        tests.add(Stream.of(SMALLINT, DOUBLE, new LeastRestrictiveType(DOUBLE)).collect(Collectors.toList()));
        tests.add(Stream.of(SMALLINT, DECIMAL, new LeastRestrictiveType(DECIMAL)).collect(Collectors.toList()));
        tests.add(Stream.of(SMALLINT, BIGINT, new LeastRestrictiveType(BIGINT)).collect(Collectors.toList()));

        return tests;
    }

    @Test
    public void testInteger() {
        for(List<Object> paramSet:intTests()){
            RelDataType t1 = (RelDataType)paramSet.get(0);
            RelDataType t2 = (RelDataType)paramSet.get(1);
            LeastRestrictiveType leastRestrictiveType = (LeastRestrictiveType)paramSet.get(2);

            expectLeastRestrictiveType(t1, t2, leastRestrictiveType);
            expectLeastRestrictiveType(t2, t1, leastRestrictiveType);
        }
    }

    private static List<List<Object>> intTests() {
        List<List<Object>> tests = new ArrayList<>();

        tests.add(Stream.of(INTEGER, TINYINT, new LeastRestrictiveType(INTEGER)).collect(Collectors.toList()));
        tests.add(Stream.of(INTEGER, SMALLINT, new LeastRestrictiveType(INTEGER)).collect(Collectors.toList()));
        tests.add(Stream.of(INTEGER, INTEGER, new LeastRestrictiveType(INTEGER)).collect(Collectors.toList()));
        tests.add(Stream.of(INTEGER, FLOAT, new LeastRestrictiveType(FLOAT)).collect(Collectors.toList()));
        tests.add(Stream.of(INTEGER, REAL, new LeastRestrictiveType(REAL)).collect(Collectors.toList()));
        tests.add(Stream.of(INTEGER, DOUBLE, new LeastRestrictiveType(DOUBLE)).collect(Collectors.toList()));
        tests.add(Stream.of(INTEGER, DECIMAL, new LeastRestrictiveType(DECIMAL)).collect(Collectors.toList()));
        tests.add(Stream.of(INTEGER, BIGINT, new LeastRestrictiveType(BIGINT)).collect(Collectors.toList()));

        return tests;
    }

    @Test
    public void testFloat() {
        for(List<Object> paramSet: floatTests()){
            RelDataType t1 = (RelDataType)paramSet.get(0);
            RelDataType t2 = (RelDataType)paramSet.get(1);
            LeastRestrictiveType leastRestrictiveType = (LeastRestrictiveType)paramSet.get(2);
            
            expectLeastRestrictiveType(t1, t2, leastRestrictiveType);
            expectLeastRestrictiveType(t2, t1, leastRestrictiveType);  
        }
    }

    private static List<List<Object>> floatTests() {
        List<List<Object>> tests = new ArrayList<>();

//        tests.add(Stream.of(FLOAT, TINYINT, new LeastRestrictiveType(FLOAT)).collect(Collectors.toList()));
//        tests.add(Stream.of(FLOAT, SMALLINT, new LeastRestrictiveType(FLOAT)).collect(Collectors.toList()));
//        tests.add(Stream.of(FLOAT, INTEGER, new LeastRestrictiveType(FLOAT)).collect(Collectors.toList()));
//        tests.add(Stream.of(FLOAT, FLOAT, new LeastRestrictiveType(FLOAT)).collect(Collectors.toList()));
//        tests.add(Stream.of(FLOAT, REAL, new LeastRestrictiveType(FLOAT)).collect(Collectors.toList()));
        tests.add(Stream.of(FLOAT, DOUBLE, new LeastRestrictiveType(DOUBLE)).collect(Collectors.toList()));
//        tests.add(Stream.of(FLOAT, DECIMAL, new LeastRestrictiveType(DOUBLE)).collect(Collectors.toList()));
//        tests.add(Stream.of(FLOAT, BIGINT, new LeastRestrictiveType(FLOAT)).collect(Collectors.toList()));

        return tests;
    }

    @Test
    public void testDouble() {
        for(List<Object> paramSet: doubleTests()){
            RelDataType t1 = (RelDataType)paramSet.get(0);
            RelDataType t2 = (RelDataType)paramSet.get(1);
            LeastRestrictiveType leastRestrictiveType = (LeastRestrictiveType)paramSet.get(2);

            expectLeastRestrictiveType(t1, t2, leastRestrictiveType);
            expectLeastRestrictiveType(t2, t1, leastRestrictiveType);
        }
    }

    private static List<List<Object>> doubleTests() {
        List<List<Object>> tests = new ArrayList<>();

//        tests.add(Stream.of(DOUBLE, TINYINT, new LeastRestrictiveType(DOUBLE)).collect(Collectors.toList()));
//        tests.add(Stream.of(DOUBLE, SMALLINT, new LeastRestrictiveType(DOUBLE)).collect(Collectors.toList()));
//        tests.add(Stream.of(DOUBLE, INTEGER, new LeastRestrictiveType(DOUBLE)).collect(Collectors.toList()));
        tests.add(Stream.of(DOUBLE, FLOAT, new LeastRestrictiveType(DOUBLE)).collect(Collectors.toList()));
//        tests.add(Stream.of(DOUBLE, REAL, new LeastRestrictiveType(DOUBLE)).collect(Collectors.toList()));
//        tests.add(Stream.of(DOUBLE, DOUBLE, new LeastRestrictiveType(DOUBLE)).collect(Collectors.toList()));
//        tests.add(Stream.of(DOUBLE, DECIMAL, new LeastRestrictiveType(DOUBLE)).collect(Collectors.toList()));
//        tests.add(Stream.of(DOUBLE, BIGINT, new LeastRestrictiveType(DOUBLE)).collect(Collectors.toList()));

        return tests;
    }

    @Test
    public void testDecimal() {
        for(List<Object> paramSet: decimalTests()){
            RelDataType t1 = (RelDataType)paramSet.get(0);
            RelDataType t2 = (RelDataType)paramSet.get(1);
            LeastRestrictiveType leastRestrictiveType = (LeastRestrictiveType)paramSet.get(2);

            expectLeastRestrictiveType(t1, t2, leastRestrictiveType);
            expectLeastRestrictiveType(t2, t1, leastRestrictiveType);
        }
    }

    private static List<List<Object>> decimalTests() {
        List<List<Object>> tests = new ArrayList<>();

        tests.add(Stream.of(DECIMAL, TINYINT, new LeastRestrictiveType(DECIMAL)).collect(Collectors.toList()));
        tests.add(Stream.of(DECIMAL, SMALLINT, new LeastRestrictiveType(DECIMAL)).collect(Collectors.toList()));
        tests.add(Stream.of(DECIMAL, INTEGER, new LeastRestrictiveType(DECIMAL)).collect(Collectors.toList()));
        tests.add(Stream.of(DECIMAL, FLOAT, new LeastRestrictiveType(DOUBLE)).collect(Collectors.toList()));
        tests.add(Stream.of(DECIMAL, REAL, new LeastRestrictiveType(DOUBLE)).collect(Collectors.toList()));
        tests.add(Stream.of(DECIMAL, DOUBLE, new LeastRestrictiveType(DOUBLE)).collect(Collectors.toList()));
        tests.add(Stream.of(DECIMAL, DECIMAL, new LeastRestrictiveType(DECIMAL)).collect(Collectors.toList()));
        tests.add(Stream.of(DECIMAL, BIGINT, new LeastRestrictiveType(DECIMAL)).collect(Collectors.toList()));

        return tests;
    }

    @Test
    public void testBigInt() {
        for(List<Object> paramSet: bigIntTests()){
            RelDataType t1 = (RelDataType)paramSet.get(0);
            RelDataType t2 = (RelDataType)paramSet.get(1);
            LeastRestrictiveType leastRestrictiveType = (LeastRestrictiveType)paramSet.get(2);

            expectLeastRestrictiveType(t1, t2, leastRestrictiveType);
            expectLeastRestrictiveType(t2, t1, leastRestrictiveType);
        }
    }

    private static List<List<Object>> bigIntTests() {
        List<List<Object>> tests = new ArrayList<>();

        tests.add(Stream.of(BIGINT, TINYINT, new LeastRestrictiveType(BIGINT)).collect(Collectors.toList()));
        tests.add(Stream.of(BIGINT, SMALLINT, new LeastRestrictiveType(BIGINT)).collect(Collectors.toList()));
        tests.add(Stream.of(BIGINT, INTEGER, new LeastRestrictiveType(BIGINT)).collect(Collectors.toList()));
        tests.add(Stream.of(BIGINT, FLOAT, new LeastRestrictiveType(FLOAT)).collect(Collectors.toList()));
        tests.add(Stream.of(BIGINT, REAL, new LeastRestrictiveType(REAL)).collect(Collectors.toList()));
        tests.add(Stream.of(BIGINT, DOUBLE, new LeastRestrictiveType(DOUBLE)).collect(Collectors.toList()));
        tests.add(Stream.of(BIGINT, DECIMAL, new LeastRestrictiveType(DECIMAL)).collect(Collectors.toList()));
        tests.add(Stream.of(BIGINT, BIGINT, new LeastRestrictiveType(BIGINT)).collect(Collectors.toList()));

        return tests;
    }

    private static final class LeastRestrictiveType {
        final RelDataType relDataType;

        private LeastRestrictiveType(SqlTypeName sqlTypeName) {
            this.relDataType = TYPE_FACTORY.createSqlType(sqlTypeName);
        }

        private LeastRestrictiveType(@Nullable RelDataType relDataType) {
            this.relDataType = relDataType;
        }

        @Override
        public String toString() {
            return relDataType != null ? relDataType.toString() : "<none>";
        }
    }

    private static RelDataType newType(SqlTypeName typeName) {
        return TYPE_FACTORY.createSqlType(typeName);
    }

    private static void expectLeastRestrictiveType(SqlTypeName type1, SqlTypeName type2, SqlTypeName expectedType) {
        RelDataType type1RelDataType = newType(type1);
        RelDataType type2RelDataType = newType(type2);

        LeastRestrictiveType expectedLeastRestrictive = new LeastRestrictiveType(newType(expectedType));

        expectLeastRestrictiveType(type1RelDataType, type2RelDataType, expectedLeastRestrictive);
    }

    private static void expectLeastRestrictiveType(RelDataType type1, RelDataType type2, LeastRestrictiveType expectedType) {
        RelDataType actualType = TYPE_FACTORY.leastRestrictive(Arrays.asList(type1, type2));

        assertEquals("leastRestrictive(" + type1 + "," + type2 + ")", expectedType.relDataType, actualType);
    }
}
