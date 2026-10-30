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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Period;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.apache.ignite.internal.processors.query.calcite.exec.rel.AbstractNode.IN_BUFFER_SIZE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/** Checks external Java values in nested ROW, ARRAY and MAP results. */
public class NestedResultTypesIntegrationTest extends AbstractBasicIntegrationTest {
    /** SQL literals and their expected external values, independent of the execution representation. */
    private static Stream<Arguments> values() {
        return Stream.of(
            Arguments.of("DATE '2026-01-01'", Date.valueOf("2026-01-01")),
            Arguments.of("TIME '12:34:56'", Time.valueOf("12:34:56")),
            Arguments.of("TIMESTAMP '2026-01-01 12:34:56'", Timestamp.valueOf("2026-01-01 12:34:56")),
            Arguments.of("INTERVAL '1' DAY", Duration.ofDays(1)),
            Arguments.of("INTERVAL '1-2' YEAR TO MONTH", Period.of(1, 2, 0)),
            Arguments.of("X'0102'", new byte[] {1, 2})
        );
    }

    /** Wrapping the same value in ARRAY must not expose its internal representation. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("values")
    public void testArrayElement(String expr, Object expected) {
        String qry = "SELECT ARRAY[" + expr + ']';

        assertQuery(qry)
            .withResultChecker(rows -> {
                List<?> arr = assertInstanceOf(List.class, singleValue(rows), qry);

                assertEquals(1, arr.size());
                assertExternalValue(expected, arr.get(0), qry);
            })
            .check();
    }

    /** Conversion must descend through multiple levels, as required by ARRAY-of-ROW paths. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("values")
    public void testArrayOfRows(String expr, Object expected) {
        String qry = "SELECT ARRAY[ROW(" + expr + ", 42)]";

        assertQuery(qry)
            .withResultChecker(rows -> {
                List<?> arr = assertInstanceOf(List.class, singleValue(rows), qry);

                assertEquals(1, arr.size());

                Object[] row = assertInstanceOf(Object[].class, arr.get(0), qry);

                assertEquals(2, row.length);
                assertEquals(42, row[1]);
                assertExternalValue(expected, row[0], qry);
            })
            .check();
    }

    /** NULL fields in an ARRAY-of-ROW result must remain NULL after conversion. */
    @Test
    public void testArrayOfRowsWithNullField() {
        String qry = "SELECT ARRAY[ROW(CAST(NULL AS DATE), 42)]";

        assertQuery(qry)
            .withResultChecker(rows -> {
                List<?> arr = assertInstanceOf(List.class, singleValue(rows), qry);

                assertEquals(1, arr.size());

                Object[] row = assertInstanceOf(Object[].class, arr.get(0), qry);

                assertEquals(2, row.length);
                assertEquals(42, row[1]);
                assertNull(row[0]);
            })
            .check();
    }

    /** The constructor's ROW value is a single result column containing two fields. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("values")
    public void testRowField(String expr, Object expected) {
        String qry = "SELECT ROW(" + expr + ", 42)";

        assertQuery(qry)
            .withResultChecker(rows -> {
                Object[] row = assertInstanceOf(Object[].class, singleValue(rows), qry);

                assertEquals(2, row.length);
                assertEquals(42, row[1]);
                assertExternalValue(expected, row[0], qry);
            })
            .check();
    }

    /** Each field is converted according to its own type, preserving order and NULL values. */
    @Test
    public void testRowWithDifferentFieldTypes() {
        String qry = "SELECT ROW(DATE '2026-01-01', 42, TIME '12:34:56', 'text', CAST(NULL AS DATE), "
            + "TIMESTAMP '2026-01-01 12:34:56', INTERVAL '1' DAY, INTERVAL '1-2' YEAR TO MONTH, X'0102')";

        assertQuery(qry)
            .withResultChecker(rows -> {
                Object[] row = assertInstanceOf(Object[].class, singleValue(rows), qry);

                assertEquals(9, row.length);
                assertExternalValue(Date.valueOf("2026-01-01"), row[0], qry);
                assertExternalValue(42, row[1], qry);
                assertExternalValue(Time.valueOf("12:34:56"), row[2], qry);
                assertExternalValue("text", row[3], qry);
                assertNull(row[4]);
                assertExternalValue(Timestamp.valueOf("2026-01-01 12:34:56"), row[5], qry);
                assertExternalValue(Duration.ofDays(1), row[6], qry);
                assertExternalValue(Period.of(1, 2, 0), row[7], qry);
                assertExternalValue(new byte[] {1, 2}, row[8], qry);
            })
            .check();
    }

    /** Fields of different types are also converted when the ROW is inside an ARRAY. */
    @Test
    public void testArrayOfRowsWithDifferentFieldTypes() {
        String qry = "SELECT ARRAY[ROW(DATE '2026-01-01', 42, TIME '12:34:56', 'text', CAST(NULL AS DATE), "
            + "TIMESTAMP '2026-01-01 12:34:56', INTERVAL '1' DAY, INTERVAL '1-2' YEAR TO MONTH, X'0102')]";

        assertQuery(qry)
            .withResultChecker(rows -> {
                List<?> arr = assertInstanceOf(List.class, singleValue(rows), qry);

                assertEquals(1, arr.size());

                Object[] row = assertInstanceOf(Object[].class, arr.get(0), qry);

                assertEquals(9, row.length);
                assertExternalValue(Date.valueOf("2026-01-01"), row[0], qry);
                assertExternalValue(42, row[1], qry);
                assertExternalValue(Time.valueOf("12:34:56"), row[2], qry);
                assertExternalValue("text", row[3], qry);
                assertNull(row[4]);
                assertExternalValue(Timestamp.valueOf("2026-01-01 12:34:56"), row[5], qry);
                assertExternalValue(Duration.ofDays(1), row[6], qry);
                assertExternalValue(Period.of(1, 2, 0), row[7], qry);
                assertExternalValue(new byte[] {1, 2}, row[8], qry);
            })
            .check();
    }

    /** MAP values must use the same external types as other result values. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("values")
    public void testMapValue(String expr, Object expected) {
        String qry = "SELECT MAP['value', " + expr + ']';

        assertQuery(qry)
            .withResultChecker(rows -> {
                Map<?, ?> map = assertInstanceOf(Map.class, singleValue(rows), qry);

                assertEquals(1, map.size());
                assertExternalValue(expected, map.get("value"), qry);
            })
            .check();
    }

    /** MAP keys must also be converted, including binary keys compared by content. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("values")
    public void testMapKey(String expr, Object expected) {
        String qry = "SELECT MAP[" + expr + ", 42]";

        assertQuery(qry)
            .withResultChecker(rows -> {
                Map<?, ?> map = assertInstanceOf(Map.class, singleValue(rows), qry);

                assertEquals(1, map.size());

                Map.Entry<?, ?> entry = map.entrySet().iterator().next();

                assertExternalValue(expected, entry.getKey(), qry);
                assertEquals(42, entry.getValue());
            })
            .check();
    }

    /** Converts keys and values together and preserves entries with NULL values. */
    @Test
    public void testMapWithDifferentKeyAndValueTypes() {
        String qry = "SELECT MAP[DATE '2026-01-01', TIMESTAMP '2026-01-01 12:34:56', "
            + "DATE '2026-01-02', CAST(NULL AS TIMESTAMP)]";

        assertQuery(qry)
            .withResultChecker(rows -> {
                Map<?, ?> map = assertInstanceOf(Map.class, singleValue(rows), qry);

                assertEquals(2, map.size());
                assertExternalValue(Timestamp.valueOf("2026-01-01 12:34:56"), map.get(Date.valueOf("2026-01-01")), qry);
                assertTrue(map.containsKey(Date.valueOf("2026-01-02")));
                assertNull(map.get(Date.valueOf("2026-01-02")));
            })
            .check();
    }

    /** Conversion descends from a MAP value into a ROW and its ARRAY field. */
    @Test
    public void testMapWithRowValue() {
        String qry = "SELECT MAP[DATE '2026-01-01', ROW(TIMESTAMP '2026-01-01 12:34:56', "
            + "ARRAY[INTERVAL '1' DAY], CAST(NULL AS TIME))]";

        assertQuery(qry)
            .withResultChecker(rows -> {
                Map<?, ?> map = assertInstanceOf(Map.class, singleValue(rows), qry);

                assertEquals(1, map.size());

                Object[] row = assertInstanceOf(Object[].class, map.get(Date.valueOf("2026-01-01")), qry);

                assertEquals(3, row.length);
                assertExternalValue(Timestamp.valueOf("2026-01-01 12:34:56"), row[0], qry);

                List<?> arr = assertInstanceOf(List.class, row[1], qry);

                assertEquals(1, arr.size());
                assertExternalValue(Duration.ofDays(1), arr.get(0), qry);
                assertNull(row[2]);
            })
            .check();
    }

    /** A MAP nested in a CYCLE-like ARRAY-of-ROW result also requires conversion. */
    @Test
    public void testArrayOfRowsWithMapField() {
        String qry = "SELECT ARRAY[ROW(MAP['value', DATE '2026-01-01'], 42)]";

        assertQuery(qry)
            .withResultChecker(rows -> {
                List<?> arr = assertInstanceOf(List.class, singleValue(rows), qry);

                assertEquals(1, arr.size());

                Object[] row = assertInstanceOf(Object[].class, arr.get(0), qry);

                assertEquals(2, row.length);
                assertEquals(42, row[1]);

                Map<?, ?> map = assertInstanceOf(Map.class, row[0], qry);

                assertEquals(1, map.size());
                assertExternalValue(Date.valueOf("2026-01-01"), map.get("value"), qry);
            })
            .check();
    }

    /** Returning an ARRAY of ROWs must not change values still used by subsequent UNNEST iterations. */
    @Test
    public void testArrayOfRowsReusedByJoin() {
        // Exceed the execution buffers so UNNEST continues after the first results have been converted.
        int rowsCnt = 8 * IN_BUFFER_SIZE;
        String qry = "SELECT a.arr, OCTET_LENGTH(u.bin) FROM TABLE(SYSTEM_RANGE(1, " + rowsCnt + ")) s "
            + "CROSS JOIN (SELECT ARRAY_AGG(ROW(X'0102', x)) arr FROM TABLE(SYSTEM_RANGE(1, 1))) a "
            + "CROSS JOIN UNNEST(a.arr) u(bin, id)";

        assertQuery(grid(0), qry)
            .resultSize(rowsCnt)
            .withResultChecker(rows -> {
                for (List<?> row : rows) {
                    assertEquals(2, row.size());
                    assertEquals(2, row.get(1));

                    List<?> arr = assertInstanceOf(List.class, row.get(0), qry);

                    assertEquals(1, arr.size());

                    Object[] nestedRow = assertInstanceOf(Object[].class, arr.get(0), qry);

                    assertEquals(2, nestedRow.length);
                    assertExternalValue(new byte[] {1, 2}, nestedRow[0], qry);
                    assertEquals(1L, nestedRow[1]);
                }
            })
            .check();
    }

    /** Extracts the only result value without normalizing it. */
    private static Object singleValue(List<List<?>> rows) {
        assertEquals(1, rows.size());
        assertEquals(1, rows.get(0).size());

        return rows.get(0).get(0);
    }

    /** Checks both Java type and value, including binary content. */
    private static void assertExternalValue(Object expected, Object actual, String qry) {
        assertInstanceOf(expected.getClass(), actual, qry + "; actual value: " + actual);

        if (expected instanceof byte[])
            assertArrayEquals((byte[])expected, (byte[])actual, qry);
        else
            assertEquals(qry, expected, actual);
    }
}
