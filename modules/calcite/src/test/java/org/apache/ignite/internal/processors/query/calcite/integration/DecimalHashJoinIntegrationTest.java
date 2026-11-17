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
package org.apache.ignite.internal.processors.query.calcite.integration;

import java.math.BigDecimal;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsSubPlan;

/** */
public class DecimalHashJoinIntegrationTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /** */
    @Test
    public void testEqualDecimalKeysWithDifferentScales() {
        sql("CREATE TABLE LEFT_ROWS (id INT PRIMARY KEY, k DECIMAL)");
        sql("CREATE TABLE RIGHT_ROWS (id INT PRIMARY KEY, k DECIMAL)");

        sql("INSERT INTO LEFT_ROWS (id, k) VALUES (1, ?)", new BigDecimal("11845600"));
        sql("INSERT INTO RIGHT_ROWS (id, k) VALUES (1, ?)", new BigDecimal("1.18456E+7"));

        BigDecimal leftRowsVal = (BigDecimal)sql("SELECT k FROM LEFT_ROWS WHERE id = 1").get(0).get(0);
        BigDecimal rightRowsVal = (BigDecimal)sql("SELECT k FROM RIGHT_ROWS WHERE id = 1").get(0).get(0);

        assertEquals(0, leftRowsVal.compareTo(rightRowsVal));
        assertEquals(0, leftRowsVal.scale());
        assertEquals(-2, rightRowsVal.scale());

        assertQuery("SELECT /*+ HASH_JOIN */ l.k, r.k FROM LEFT_ROWS l JOIN RIGHT_ROWS r ON l.k = r.k "
            + "WHERE l.k = 11845600")
            .matches(containsSubPlan("IgniteHashJoin"))
            .resultSize(1)
            .check();
    }

    /** */
    @ParameterizedTest(name = "left={0}, right={1}, expRows={2}")
    @CsvSource({
        "11845600, 11845600, 1",
        "11845600, 11845601, 0",
        "11845600, 1.18457E+7, 0",
        "11845600.00, 11845600.00, 1",
        "11845600.00, 11845601.00, 0",
        "11845600, 11845600.00, 1",
        "0, 0.00, 1",
        "-11845600, -1.18456E+7, 1"
    })
    public void testDecimalKeys(String left, String right, int expRows) {
        createTables(new BigDecimal(left), new BigDecimal(right));

        assertQuery("SELECT /*+ HASH_JOIN */ l.id, r.id FROM LEFT_ROWS l JOIN RIGHT_ROWS r ON l.k = r.k WHERE l.k = ?")
            .withParams(new BigDecimal(left))
            .matches(containsSubPlan("IgniteHashJoin"))
            .resultSize(expRows)
            .check();
    }

    /** */
    @ParameterizedTest(name = "{0}: left={2}, right={3}, expRows={4}")
    @MethodSource("hintCases")
    public void testJoinHints(String hint, String plan, String left, String right, int expRows) {
        createTables(new BigDecimal(left), new BigDecimal(right));

        assertQuery("SELECT /*+ " + hint + " */ l.id, r.id FROM LEFT_ROWS l JOIN RIGHT_ROWS r ON l.k = r.k WHERE l.k = ?")
            .withParams(new BigDecimal(left))
            .matches(containsSubPlan(plan))
            .resultSize(expRows)
            .check();
    }

    /** */
    @ParameterizedTest(name = "left={0}, right={1}, expRows={2}")
    @CsvSource({
        "11845600, 11845600, 1",
        "11845600, 11845601, 0",
        "11845600, 1.18456E+7, 1",
        "11845600, 1.18457E+7, 0"
    })
    public void testJoinWithoutHints(String left, String right, int expRows) {
        createTables(new BigDecimal(left), new BigDecimal(right));

        assertQuery("SELECT l.id, r.id FROM LEFT_ROWS l JOIN RIGHT_ROWS r ON l.k = r.k WHERE l.k = ?")
            .withParams(new BigDecimal(left))
            .resultSize(expRows)
            .check();
    }

    /** */
    @ParameterizedTest(name = "right precision={0}, scale={1}, expRows={2}")
    @CsvSource({
        "12, 0, 11845600, 1",
        "20, 0, 11845600, 1",
        "12, 2, 11845600, 1",
        "20, 2, 11845600, 1",
        "20, 0, 11845601, 0",
        "20, 2, 11845601, 0"
    })
    public void testDifferentDeclaredPrecision(int precision, int scale, String right, int expRows) {
        sql("CREATE TABLE LEFT_ROWS (id INT PRIMARY KEY, k DECIMAL(10, 0))");
        sql("CREATE TABLE RIGHT_ROWS (id INT PRIMARY KEY, k DECIMAL(" + precision + ", " + scale + "))");

        sql("INSERT INTO LEFT_ROWS VALUES (1, ?)", new BigDecimal("11845600"));
        sql("INSERT INTO RIGHT_ROWS VALUES (1, ?)", new BigDecimal(right));

        assertQuery("SELECT /*+ HASH_JOIN */ l.id, r.id FROM LEFT_ROWS l JOIN RIGHT_ROWS r ON l.k = r.k")
            .matches(containsSubPlan("IgniteHashJoin"))
            .resultSize(expRows)
            .check();
    }

    /** Both equal and unequal keys are tested with the same scale and different scales. */
    private static Stream<Arguments> hintCases() {
        return Stream.of(
            new String[] {"HASH_JOIN", "IgniteHashJoin"},
            new String[] {"NL_JOIN", "IgniteNestedLoopJoin"},
            new String[] {"MERGE_JOIN", "IgniteMergeJoin"},
            new String[] {"CNL_JOIN", "IgniteCorrelatedNestedLoopJoin"}
        ).flatMap(hint -> Stream.of(
            Arguments.of(hint[0], hint[1], "11845600", "11845600", 1),
            Arguments.of(hint[0], hint[1], "11845600", "11845601", 0),
            Arguments.of(hint[0], hint[1], "11845600", "1.18456E+7", 1),
            Arguments.of(hint[0], hint[1], "11845600", "1.18457E+7", 0)
        ));
    }

    /** */
    private void createTables(BigDecimal left, BigDecimal right) {
        sql("CREATE TABLE LEFT_ROWS (id INT PRIMARY KEY, k DECIMAL)");
        sql("CREATE TABLE RIGHT_ROWS (id INT PRIMARY KEY, k DECIMAL)");

        sql("INSERT INTO LEFT_ROWS VALUES (1, ?)", left);
        sql("INSERT INTO RIGHT_ROWS VALUES (1, ?)", right);
    }
}
