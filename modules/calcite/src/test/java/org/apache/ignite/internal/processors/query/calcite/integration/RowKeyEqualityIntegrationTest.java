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

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsSubPlan;

/** Checks that independently constructed ROW keys are compared and hashed by their contents. */
public class RowKeyEqualityIntegrationTest extends AbstractBasicIntegrationTest {
    /** Physical aggregation plans; each corresponds to one converter rule. */
    private static final String[] AGGREGATE_VARIANTS = {
        "ColocatedHash", "MapReduceHash", "ColocatedSort", "MapReduceSort"
    };

    /** Force hash aggregation so that ordering cannot hide an equality or hashing defect. */
    private static final String HASH_AGG_HINT = "/*+ DISABLE_RULE('ColocatedSortAggregateConverterRule', "
        + "'MapReduceSortAggregateConverterRule') */ ";

    /** Primary key of the first record with val = 1. */
    private int firstId;

    /** Primary key of the second record with val = 1, owned by another node. */
    private int secondId;

    /** Each record has a different primary node; the first two have equal values. */
    @BeforeEach
    public void createTable() {
        sql("CREATE TABLE row_keys (id INT PRIMARY KEY, val INT)");

        String cacheName = QueryUtils.createTableCacheName("PUBLIC", "ROW_KEYS");

        firstId = primaryKey(grid(0).cache(cacheName));
        secondId = primaryKey(grid(1).cache(cacheName));
        int thirdId = primaryKey(grid(2).cache(cacheName));

        sql("INSERT INTO row_keys VALUES (?, 1), (?, 1), (?, 2)", firstId, secondId, thirdId);
    }

    /** DISTINCT must retain two keys, regardless of whether the value is wrapped in ROW or ARRAY. */
    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("distinctCases")
    public void testDistinct(String aggregate, String key) {
        String hint = Arrays.stream(AGGREGATE_VARIANTS)
            .filter(variant -> !variant.equals(aggregate))
            .map(variant -> variant + "AggregateConverterRule")
            .collect(Collectors.joining("', '", "/*+ DISABLE_RULE('", "') */ "));

        QueryChecker checker = assertQuery("SELECT " + hint + "DISTINCT " + key + " FROM row_keys");

        if (aggregate.startsWith("MapReduce")) {
            checker.matches(containsSubPlan("Ignite" + aggregate.replace("MapReduce", "Map") + "Aggregate"))
                .matches(containsSubPlan("Ignite" + aggregate.replace("MapReduce", "Reduce") + "Aggregate"));
        }
        else
            checker.matches(containsSubPlan("Ignite" + aggregate + "Aggregate"));

        checker.resultSize(2).check();
    }

    /** Every key representation is checked with all four aggregation plans. */
    private static Stream<Arguments> distinctCases() {
        return Arrays.stream(AGGREGATE_VARIANTS).flatMap(aggregate ->
            Stream.of("val", "ARRAY[val]", "ROW(val, 'x')", "ARRAY[ROW(val, 'x')]")
                .map(key -> Arguments.of(aggregate, key)));
    }

    /** Ordering complex values must respect direction and explicit placement of null keys. */
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"ARRAY[val]", "ROW('x', val)", "ARRAY[ROW('x', val)]"})
    public void testOrderBy(String key) {
        String qry = "SELECT id FROM (SELECT id, CASE WHEN val IS NULL THEN NULL ELSE " + key + " END AS k "
            + "FROM (VALUES (1, 2), (2, 1), (3, 3), (4, CAST(NULL AS INTEGER))) t(id, val)) ORDER BY k ";

        assertQuery(qry + "ASC NULLS FIRST").ordered().returns(4).returns(2).returns(1).returns(3).check();
        assertQuery(qry + "ASC NULLS LAST").ordered().returns(2).returns(1).returns(3).returns(4).check();
        assertQuery(qry + "DESC NULLS FIRST").ordered().returns(4).returns(3).returns(1).returns(2).check();
        assertQuery(qry + "DESC NULLS LAST").ordered().returns(3).returns(1).returns(2).returns(4).check();
    }

    /** Calcite orders null elements after non-null elements within an ascending ROW or ARRAY. */
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"ARRAY[val]", "ROW('x', val)", "ARRAY[ROW('x', val)]"})
    public void testOrderByWithNullElements(String key) {
        String qry = "SELECT id FROM (SELECT id, " + key + " AS k "
            + "FROM (VALUES (1, 2), (2, 1), (3, 3), (4, CAST(NULL AS INTEGER))) t(id, val)) ORDER BY k ";

        assertQuery(qry + "ASC").ordered().returns(2).returns(1).returns(3).returns(4).check();
        assertQuery(qry + "DESC").ordered().returns(4).returns(3).returns(1).returns(2).check();
    }

    /** Equal keys must share a group of size two; the different key forms a group of size one. */
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"val", "ARRAY[val]", "ROW(val, 'x')", "ARRAY[ROW(val, 'x')]"})
    public void testGroupBy(String key) {
        assertQuery("SELECT " + HASH_AGG_HINT + "COUNT(*) FROM (SELECT " + key + " AS k FROM row_keys) GROUP BY k")
            .matches(containsSubPlan("HashAggregate"))
            .returns(2L)
            .returns(1L)
            .check();
    }

    /** UNION must remove equal keys constructed in separate input branches. */
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"val", "ARRAY[val]", "ROW(val, 'x')", "ARRAY[ROW(val, 'x')]"})
    public void testUnion(String key) {
        assertQuery("SELECT " + HASH_AGG_HINT + "COUNT(*) FROM ("
            + "SELECT " + key + " FROM row_keys WHERE id = ? UNION "
            + "SELECT " + key + " FROM row_keys WHERE id <> ?)")
            .withParams(firstId, firstId)
            .returns(2L)
            .check();
    }

    /** INTERSECT must find the equal key in both inputs and exclude the different key. */
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"val", "ARRAY[val]", "ROW(val, 'x')", "ARRAY[ROW(val, 'x')]"})
    public void testIntersect(String key) {
        assertQuery("SELECT COUNT(*) FROM ("
            + "SELECT " + key + " FROM row_keys WHERE id = ? INTERSECT "
            + "SELECT " + key + " FROM row_keys WHERE id <> ?)")
            .withParams(firstId, firstId)
            .returns(1L)
            .check();
    }

    /** Hash joins must find the same ARRAY-of-ROW key constructed on either side. */
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"val", "ARRAY[val]", "ROW(val, 'x')", "ARRAY[ROW(val, 'x')]"})
    public void testHashJoin(String key) {
        assertQuery("SELECT /*+ DISABLE_RULE('MergeJoinConverter', 'NestedLoopJoinConverter', "
            + "'CorrelatedNestedLoopJoin') */ l.id, r.id FROM "
            + "(SELECT id, " + key + " AS k FROM row_keys WHERE id = ?) l JOIN "
            + "(SELECT id, " + key + " AS k FROM row_keys WHERE id <> ?) r ON l.k = r.k")
            .withParams(firstId, firstId)
            .matches(containsSubPlan("HashJoin"))
            .returns(firstId, secondId)
            .check();
    }

    /** Null fields and additional nesting must not prevent duplicate elimination. */
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {
        "ARRAY[ROW(val, CAST(NULL AS INTEGER))]",
        "ARRAY[ROW(ROW(val, 'x'), ARRAY[val])]",
        "MAP['key', ROW(val, 'x')]",
        "MAP[ROW(val, 'x'), 'value']"
    })
    public void testNestedKeys(String key) {
        assertQuery("SELECT COUNT(*) FROM (SELECT " + HASH_AGG_HINT + "DISTINCT " + key + " FROM row_keys)")
            .returns(2L)
            .check();
    }

    /** Recursive UNION must eliminate independently constructed duplicates in the seed and subsequent iterations. */
    @Test
    public void testRecursiveUnion() {
        assertQuery("WITH RECURSIVE t(n, k) AS ("
            + "SELECT val, ARRAY[ROW(val, 'x')] FROM row_keys WHERE val = 1 UNION "
            + "SELECT n + 1, ARRAY[ROW(n + 1, 'x')] FROM t WHERE n < 2) SELECT COUNT(*) FROM t")
            .returns(2L)
            .check();
    }
}
