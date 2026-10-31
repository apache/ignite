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
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/** SQL-standard CYCLE semantics from CALCITE-7814, exercised by Ignite's distributed engine. */
public class CycleIntegrationTest extends AbstractBasicIntegrationTest {
    /** Partitioned graph edges are read from all servers; each branch keeps its own path. */
    @ParameterizedTest
    @ValueSource(strings = {"UNION ALL", "UNION"})
    public void testDistributedGraph(String union) throws Exception {
        sql("CREATE TABLE edges(id INT PRIMARY KEY, src VARCHAR, dst VARCHAR) WITH \"backups=0\"");

        String cacheName = QueryUtils.createTableCacheName("PUBLIC", "EDGES");
        int first = primaryKey(grid(0).cache(cacheName));
        int second = primaryKey(grid(1).cache(cacheName));
        int third = primaryKey(grid(2).cache(cacheName));

        sql("INSERT INTO edges VALUES (?, 'A', 'B'), (?, 'B', 'A'), (?, 'A', 'C')", first, second, third);

        assertQuery("WITH RECURSIVE walk(node) AS (VALUES (CAST('A' AS VARCHAR)) " + union +
            " SELECT e.dst FROM walk w JOIN edges e ON e.src = w.node) " +
            "CYCLE node SET c USING p SELECT node, c, p FROM walk ORDER BY CARDINALITY(p), node")
            .withResultChecker(rows -> {
                assertEquals(4, rows.size());
                assertEquals(Arrays.asList("A", false), rows.get(0).subList(0, 2));
                assertEquals(Arrays.asList("B", false), rows.get(1).subList(0, 2));
                assertEquals(Arrays.asList("C", false), rows.get(2).subList(0, 2));
                assertEquals(Arrays.asList("A", true), rows.get(3).subList(0, 2));
                assertPath(rows.get(0).get(2), new Object[] {"A"});
                assertPath(rows.get(1).get(2), new Object[] {"A"}, new Object[] {"B"});
                assertPath(rows.get(2).get(2), new Object[] {"A"}, new Object[] {"C"});
                assertPath(rows.get(3).get(2), new Object[] {"A"}, new Object[] {"B"}, new Object[] {"A"});
            })
            .check();
    }

    /** The same vertex reached through separate branches must not be treated as globally visited. */
    @Test
    public void testIndependentPaths() {
        assertQuery("WITH RECURSIVE edges(src, dst) AS (VALUES ('A', 'B'), ('A', 'C'), ('B', 'D'), " +
            "('C', 'D'), ('D', 'A')), walk(node) AS (VALUES ('A') UNION ALL " +
            "SELECT e.dst FROM walk w JOIN edges e ON e.src = w.node) " +
            "CYCLE node SET c USING p SELECT node, c, CARDINALITY(p) FROM walk")
            .returns("A", false, 1).returns("B", false, 2).returns("C", false, 2)
            .returns("D", false, 3).returns("D", false, 3).returns("A", true, 4).returns("A", true, 4).check();
    }

    /** Result conversion preserves SQL types inside the generated ARRAY[ROW]. */
    @Test
    public void testDatePath() {
        Date date = Date.valueOf("2026-10-27");

        assertQuery("WITH RECURSIVE t(d, n) AS (VALUES (DATE '2026-10-27', 1) UNION ALL SELECT * FROM t) " +
            "CYCLE d, n SET c USING p SELECT c, p FROM t ORDER BY CARDINALITY(p)")
            .withResultChecker(rows -> {
                assertEquals(2, rows.size());
                assertEquals(false, rows.get(0).get(0));
                assertEquals(true, rows.get(1).get(0));
                assertPath(rows.get(0).get(1), new Object[] {date, 1});
                assertPath(rows.get(1).get(1), new Object[] {date, 1}, new Object[] {date, 1});
            }).check();
    }

    /** Interval key casts retain their qualifiers and precision. */
    @ParameterizedTest
    @ValueSource(strings = {"INTERVAL '1' DAY", "INTERVAL '123-4' YEAR(3) TO MONTH",
        "INTERVAL '123 04:05:06.789' DAY(3) TO SECOND(3)"})
    public void testIntervalKey(String key) {
        assertQuery("WITH RECURSIVE t(k, d) AS (VALUES (" + key + ", 0) " +
            "UNION ALL SELECT k, d + 1 FROM t WHERE d < 2) CYCLE k SET c USING p " +
            "SELECT d, c, k = " + key + ", CARDINALITY(p) FROM t")
            .returns(0, false, true, 1).returns(1, true, true, 2).check();
    }

    /** A WITH local to the recursive query retains its scope. */
    @Test
    public void testLocalWith() {
        assertQuery("WITH RECURSIVE t(n) AS (WITH seed(n) AS (VALUES (1)) " +
            "SELECT n FROM seed UNION ALL SELECT n FROM t) CYCLE n SET c USING p SELECT n, c FROM t")
            .returns(1, false).returns(1, true).check();
    }

    /** Quoted identifiers retain their casing. */
    @Test
    public void testQuotedNames() {
        assertQuery("WITH RECURSIVE \"Walk\"(\"Node\") AS (VALUES (1) " +
            "UNION ALL SELECT w.\"Node\" FROM \"Walk\" w) " +
            "CYCLE \"Node\" SET \"Cycle\" USING \"Path\" SELECT \"Node\", \"Cycle\" FROM \"Walk\"")
            .returns(1, false).returns(1, true).check();
    }

    /** Explicit mark values include reversed Boolean polarity and single-character strings. */
    @ParameterizedTest
    @ValueSource(strings = {"TRUE", "FALSE", "'Y'", "' '", "'\u00e9'"})
    public void testExplicitMarks(String mark) {
        boolean bool = !mark.startsWith("'");
        String dflt = bool ? Boolean.toString(!Boolean.parseBoolean(mark)) : "'N'";

        assertQuery("WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n FROM t) " +
            "CYCLE n SET c TO " + mark + " DEFAULT " + dflt + " USING p SELECT n, c FROM t")
            .returns(1, bool ? Boolean.valueOf(dflt) : "N")
            .returns(1, bool ? Boolean.valueOf(mark) : mark.substring(1, mark.length() - 1)).check();
    }

    /** The row closing a cycle is returned, but never expanded. */
    @Test
    public void testCycleExecution() {
        assertQuery("WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT MOD(n + 1, 3) FROM t) " +
            "CYCLE n SET c TO 'Y' DEFAULT 'N' USING p SELECT n, c, CARDINALITY(p) FROM t")
            .returns(1, "N", 1).returns(2, "N", 2).returns(0, "N", 3).returns(1, "Y", 4).check();
    }

    /** Both UNION modes compare the complete key, and the path participates in duplicate elimination. */
    @ParameterizedTest
    @ValueSource(strings = {"UNION ALL", "UNION DISTINCT"})
    public void testCompositeKey(String union) {
        assertQuery("WITH RECURSIVE t(n, k, depth) AS (VALUES (0, 0, 0) " + union +
            " SELECT MOD(n + 1, 2), MOD(k + 1, 3), depth + 1 FROM t WHERE depth < 10) " +
            "CYCLE n, k SET c USING p SELECT n, k, depth, c, CARDINALITY(p) FROM t")
            .returns(0, 0, 0, false, 1).returns(1, 1, 1, false, 2).returns(0, 2, 2, false, 3)
            .returns(1, 0, 3, false, 4).returns(0, 1, 4, false, 5).returns(1, 2, 5, false, 6)
            .returns(0, 0, 6, true, 7).check();
    }

    /** The original star projection excludes generated columns; paths are independent immutable values. */
    @ParameterizedTest
    @ValueSource(strings = {"*", "t.*", "n"})
    public void testPath(String projection) {
        assertQuery("WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT " + projection + " FROM t) " +
            "CYCLE n SET c USING p SELECT * FROM t ORDER BY CARDINALITY(p)")
            .withResultChecker(rows -> {
                assertEquals(2, rows.size());
                assertEquals(Arrays.asList(1, false), rows.get(0).subList(0, 2));
                assertEquals(Arrays.asList(1, true), rows.get(1).subList(0, 2));
                assertPath(rows.get(0).get(2), new Object[] {1});
                assertPath(rows.get(1).get(2), new Object[] {1}, new Object[] {1});
            }).check();
    }

    /** Deep equality must eliminate duplicate seeds including their generated ARRAY[ROW] paths. */
    @Test
    public void testDistinctSeed() {
        assertQuery("WITH RECURSIVE t(n) AS (VALUES (1), (1) UNION SELECT n FROM t) " +
            "CYCLE n SET c USING p SELECT n, c, CARDINALITY(p) FROM t")
            .returns(1, false, 1).returns(1, true, 2).check();
    }

    /** SQL UNKNOWN for nullable keys is not interpreted as a cycle. */
    @Test
    public void testNullKey() {
        assertQuery("WITH RECURSIVE t(n, d) AS (VALUES (CAST(NULL AS INTEGER), 0) " +
            "UNION ALL SELECT n, d + 1 FROM t WHERE d < 3) CYCLE n SET c USING p SELECT n, d, c FROM t")
            .returns(null, 0, false).returns(null, 1, false).returns(null, 2, false).returns(null, 3, false).check();
    }

    /** Generated columns are visible to subsequent WITH items. */
    @Test
    public void testFollowingWithItem() {
        assertQuery("WITH RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM t WHERE n < 3) " +
            "CYCLE n SET c USING p, u AS (SELECT n, c, CARDINALITY(p) AS d FROM t) SELECT * FROM u")
            .returns(1, false, 1).returns(2, false, 2).returns(3, false, 3).check();
    }

    /** Alias columns must not shadow the generated mark or path. */
    @Test
    public void testColumnAliasCollisions() {
        assertQuery("WITH RECURSIVE t(n, k, m) AS (VALUES (1, 2, 3) " +
            "UNION ALL SELECT w.c, w.c0, w.p FROM t AS w(c, c0, p)) " +
            "CYCLE n SET c USING p SELECT n, k, m, c, CARDINALITY(p) FROM t")
            .returns(1, 2, 3, false, 1).returns(1, 2, 3, true, 2).check();
    }

    /** Generated columns must not introduce new natural join conditions. */
    @Test
    public void testNaturalJoin() {
        assertQuery("WITH RECURSIVE t(n) AS (VALUES (1) " +
            "UNION ALL SELECT n FROM t NATURAL JOIN (VALUES (1, TRUE)) AS v(n, c)) " +
            "CYCLE n SET c TO 'Y' DEFAULT 'N' USING p SELECT n, c FROM t")
            .returns(1, "N").returns(1, "Y").check();
    }

    /** USING expands stars without exposing cache system columns or hiding the recursive reference. */
    @ParameterizedTest
    @ValueSource(strings = {"", " ORDER BY n"})
    public void testUsingJoinWithCache(String orderBy) {
        sql("CREATE TABLE cycle_nodes(n INT PRIMARY KEY)");
        sql("INSERT INTO cycle_nodes(n) VALUES (1)");

        assertQuery("WITH RECURSIVE t(n) AS (VALUES (1) " +
            "UNION ALL SELECT * FROM t JOIN cycle_nodes USING (n)" + orderBy + ") " +
            "CYCLE n SET c USING p SELECT n, c, CARDINALITY(p) FROM t")
            .returns(1, false, 1).returns(1, true, 2).check();
    }

    /** Names in the path ROW are available through UNNEST, in CYCLE key order. */
    @Test
    public void testUnnestPath() {
        assertQuery("WITH RECURSIVE t(\"nodeId\", k) AS (VALUES (1, 2) UNION ALL SELECT * FROM t) " +
            "CYCLE k, \"nodeId\" SET c USING p SELECT u.k, u.\"nodeId\" FROM t CROSS JOIN UNNEST(p) u WHERE c")
            .returns(2, 1).returns(2, 1).check();
    }

    /** Several CYCLE items can use the preceding item's generated columns in their seed. */
    @Test
    public void testMultipleCycleItems() {
        assertQuery("WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n FROM t) CYCLE n SET c USING p, " +
            "u(n) AS (SELECT n FROM t WHERE c UNION ALL SELECT n FROM u) CYCLE n SET c USING p " +
            "SELECT n, c, CARDINALITY(p) FROM u")
            .returns(1, false, 1).returns(1, true, 2).check();
    }

    /** ARRAY fields compare as whole values, including NULL elements. */
    @Test
    public void testArrayKey() {
        assertQuery("WITH RECURSIVE t(k) AS (VALUES (ARRAY[1, CAST(NULL AS INTEGER)]) UNION ALL SELECT k FROM t) " +
            "CYCLE k SET c USING p SELECT c, CARDINALITY(p) FROM t")
            .returns(false, 1).returns(true, 2).check();
    }

    /** A nested ROW with a NULL field yields UNKNOWN rather than closing a cycle. */
    @Test
    public void testNullableRowKey() {
        assertQuery("WITH RECURSIVE t(k, d) AS (SELECT ROW(1, CAST(NULL AS INTEGER)), 0 " +
            "UNION ALL SELECT k, d + 1 FROM t WHERE d < 2) CYCLE k SET c USING p SELECT d, c FROM t")
            .returns(0, false).returns(1, false).returns(2, false).check();
    }

    /** CYCLE does not change an empty seed or an existing recursive predicate. */
    @Test
    public void testPredicates() {
        assertQuery("WITH RECURSIVE t(n) AS (SELECT n FROM (VALUES (1)) v(n) WHERE FALSE UNION ALL SELECT n FROM t) " +
            "CYCLE n SET c USING p SELECT * FROM t").resultSize(0).check();

        assertQuery("WITH RECURSIVE t(n) AS (VALUES (1), (2), (3) " +
            "UNION ALL SELECT n FROM t WHERE n > 1 AND n < 3) CYCLE n SET c USING p SELECT n, c FROM t")
            .returns(1, false).returns(2, false).returns(3, false).returns(2, true).check();
    }

    /** Invalid mark values are rejected before lowering. */
    @ParameterizedTest
    @ValueSource(strings = {"TO NULL DEFAULT 'N'", "TO 'Y' DEFAULT NULL", "TO 'Y' DEFAULT 'Y'",
        "TO '' DEFAULT 'N'", "TO 'YY' DEFAULT 'N'", "TO TRUE DEFAULT TRUE", "TO TRUE DEFAULT UNKNOWN",
        "TO TRUE DEFAULT 'N'", "TO 1 DEFAULT 0", "TO -1 DEFAULT 0", "TO n DEFAULT 'N'",
        "TO 'Y' DEFAULT ''", "TO 'Y' DEFAULT 'NN'", "TO FALSE DEFAULT FALSE", "TO 'Y' DEFAULT FALSE",
        "TO 1.0 DEFAULT 0.0", "TO 1e0 DEFAULT 0e0", "TO 'Y' DEFAULT 0", "TO ' ' DEFAULT ' '",
        "TO X'01' DEFAULT X'0100'", "TO INTERVAL '1' DAY DEFAULT INTERVAL '24' HOUR"})
    public void testInvalidMark(String values) {
        assertThrows("WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n FROM t) CYCLE n SET c " +
            values + " USING p SELECT * FROM t", IgniteSQLException.class,
            "CYCLE mark and default must be distinct, non-null Boolean or compatible single-character string literals");
    }

    /** Checks every key tuple rather than relying on Java List equality for nested Object[] values. */
    private static void assertPath(Object value, Object[]... expected) {
        List<?> path = assertInstanceOf(List.class, value);

        assertEquals(expected.length, path.size());

        for (int i = 0; i < expected.length; i++)
            assertArrayEquals(expected[i], assertInstanceOf(Object[].class, path.get(i)));
    }
}
