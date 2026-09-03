/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.ignite.internal.processors.query.calcite.planner;

import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.IgniteScalarFunction;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRecursiveTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRepeatUnion;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteUnionAll;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteValues;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.junit.Test;

/** Planner tests for recursive common table expressions. */
public class RecursiveCtePlannerTest extends AbstractPlannerTest {
    /** Employee hierarchy query used for distribution and index planning checks. */
    private static final String EMPLOYEE_HIERARCHY_QUERY =
        "WITH RECURSIVE employee_hierarchy (id, manager_id, depth) AS (" +
            "SELECT id, manager_id, 0 FROM employee WHERE manager_id IS NULL " +
            "UNION ALL " +
            "SELECT e.id, e.manager_id, h.depth + 1 " +
            "FROM employee e " +
            "JOIN employee_hierarchy h ON e.manager_id = h.id" +
        ") " +
        "SELECT id, manager_id, depth FROM employee_hierarchy";

    /** Employee hierarchy query that requests indexed correlated lookups in the recursive term. */
    private static final String INDEXED_EMPLOYEE_HIERARCHY_QUERY =
        EMPLOYEE_HIERARCHY_QUERY
            .replace("SELECT e.id", "SELECT /*+ CNL_JOIN */ e.id")
            .replace("FROM employee e", "FROM employee /*+ FORCE_INDEX(EMPLOYEE_MANAGER_IDX) */ e");

    /** Checks the physical operators used to maintain the recursive delta. */
    @Test
    public void testRecursiveDeltaPlan() throws Exception {
        IgniteSchema schema = new IgniteSchema(DEFAULT_SCHEMA);

        String sql =
            "WITH RECURSIVE numbers(n) AS (" +
                "SELECT 1 " +
                "UNION ALL " +
                "SELECT n + 1 FROM numbers WHERE n < 3" +
            ") " +
            "SELECT n FROM numbers";

        assertPlan(sql, schema, isInstanceOf(IgniteRepeatUnion.class)
            .and(hasDistribution(IgniteDistributions.single()))
            .and(input(0, isInstanceOf(IgniteValues.class)))
            .and(input(1, hasChildThat(isInstanceOf(IgniteRecursiveTableScan.class))))
        );
    }

    /** A replicated source can be read on the coordinator without an exchange. */
    @Test
    public void testRecursiveCteWithReplicatedTable() throws Exception {
        IgniteSchema schema = hierarchySchema(IgniteDistributions.broadcast(), false);

        assertPlan(EMPLOYEE_HIERARCHY_QUERY, schema, isInstanceOf(IgniteRepeatUnion.class)
            .and(hasDistribution(IgniteDistributions.single()))
            .and(hasChildThat(isInstanceOf(Exchange.class)).negate())
        );
    }

    /** A partitioned source has to be transferred to the coordinator-side recursive plan. */
    @Test
    public void testRecursiveCteWithPartitionedTable() throws Exception {
        IgniteDistribution distribution = IgniteDistributions.affinity(0, "EMPLOYEE", "hash");
        IgniteSchema schema = hierarchySchema(distribution, false);

        assertPlan(EMPLOYEE_HIERARCHY_QUERY, schema, isInstanceOf(IgniteRepeatUnion.class)
            .and(hasDistribution(IgniteDistributions.single()))
            .and(input(1, hasChildThat(isInstanceOf(Spool.class)
                .and(hasChildThat(isInstanceOf(Exchange.class))))))
        );
    }

    /** A replicated indexed input can be rewound without materialization. */
    @Test
    public void testRecursiveCteWithReplicatedIndexedTable() throws Exception {
        IgniteSchema schema = hierarchySchema(IgniteDistributions.broadcast(), true);

        assertPlan(INDEXED_EMPLOYEE_HIERARCHY_QUERY, schema, isInstanceOf(IgniteRepeatUnion.class)
            .and(hasDistribution(IgniteDistributions.single()))
            .and(input(1, hasChildThat(isInstanceOf(IgniteIndexScan.class))))
            .and(input(1, hasChildThat(isInstanceOf(Spool.class)).negate()))
        );
    }

    /** A non-deterministic projection in a table scan must be evaluated on every iteration. */
    @Test
    public void testNonDeterministicTableScanIsNotMaterialized() throws Exception {
        IgniteSchema schema = recursiveMarkersSchema(false);

        String sql = "WITH RECURSIVE numbers(n, marker) AS (" +
            "SELECT 1, 0 " +
            "UNION ALL " +
            "SELECT n + 1, v.marker " +
            "FROM numbers " +
            "CROSS JOIN (" +
                "SELECT nextRecursiveValue() AS marker FROM recursive_markers" +
            ") v " +
            "WHERE n < 4" +
            ") " +
            "SELECT n, marker FROM numbers";

        assertPlan(sql, schema, isInstanceOf(IgniteRepeatUnion.class)
            .and(input(1, hasChildThat(isInstanceOf(IgniteTableScan.class)
                .and(scan -> scan.projects() != null)
                .and(scan -> scan.projects().toString().contains("NEXTRECURSIVEVALUE")))))
            .and(input(1, hasChildThat(isInstanceOf(Spool.class)).negate()))
        );
    }

    /** A non-deterministic condition in an index scan must be evaluated on every iteration. */
    @Test
    public void testNonDeterministicIndexScanIsNotMaterialized() throws Exception {
        IgniteSchema schema = recursiveMarkersSchema(true);

        String sql = "WITH RECURSIVE numbers(n, marker) AS (" +
            "SELECT 1, 0 " +
            "UNION ALL " +
            "SELECT n + 1, " +
                "(SELECT marker FROM recursive_markers /*+ FORCE_INDEX */ " +
                    "WHERE id = numbers.n AND nextRecursiveValue() > 0) " +
            "FROM numbers " +
            "WHERE n < 4" +
            ") " +
            "SELECT n, marker FROM numbers";

        assertPlan(sql, schema, isInstanceOf(IgniteRepeatUnion.class)
            .and(input(1, hasChildThat(isInstanceOf(IgniteIndexScan.class)
                .and(scan -> scan.condition() != null)
                .and(scan -> scan.condition().toString().contains("NEXTRECURSIVEVALUE")))))
            .and(input(1, hasChildThat(isInstanceOf(Spool.class)).negate()))
        );
    }

    /** Calcite places multiple non-recursive branches into the seed input of RepeatUnion. */
    @Test
    public void testRecursiveCteWithMultipleSeedBranches() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("T", IgniteDistributions.single(), "ID", SqlTypeName.INTEGER)
        );

        String sql =
            "WITH RECURSIVE numbers(n) AS (" +
                "SELECT ID FROM T WHERE ID = 1 " +
                "UNION ALL " +
                "SELECT ID FROM T WHERE ID = 2 " +
                "UNION ALL " +
                "SELECT n + 1 FROM numbers WHERE n < 3" +
            ") " +
            "SELECT n FROM numbers";

        assertPlan(sql, schema, isInstanceOf(IgniteRepeatUnion.class)
            .and(hasDistribution(IgniteDistributions.single()))
            .and(input(0, isInstanceOf(IgniteUnionAll.class)
                .and(input(0, isTableScan("T")))
                .and(input(1, isTableScan("T")))))
        );
    }

    /** Creates an employee table with the requested distribution and optional manager index. */
    private static IgniteSchema hierarchySchema(IgniteDistribution distribution, boolean withManagerIdx) {
        TestTable table = createTable(
            "EMPLOYEE",
            distribution,
            "ID", Integer.class,
            "MANAGER_ID", Integer.class
        );

        if (withManagerIdx)
            table.addIndex("EMPLOYEE_MANAGER_IDX", 1);

        return createSchema(table);
    }

    /** Creates a replicated table and registers a non-deterministic function used by scan tests. */
    private static IgniteSchema recursiveMarkersSchema(boolean withIndex) throws NoSuchMethodException {
        TestTable table = createTable(
            "RECURSIVE_MARKERS",
            IgniteDistributions.broadcast(),
            "ID", SqlTypeName.INTEGER,
            "MARKER", SqlTypeName.INTEGER
        );

        if (withIndex)
            table.addIndex("RECURSIVE_MARKERS_ID_IDX", 0);

        IgniteSchema schema = createSchema(table);

        schema.addFunction(
            "NEXTRECURSIVEVALUE",
            IgniteScalarFunction.create(
                RecursiveCtePlannerTest.class.getMethod("nextRecursiveValue"),
                false
            )
        );

        return schema;
    }

    /** Function used only to build a non-deterministic expression in planner tests. */
    public static int nextRecursiveValue() {
        return 1;
    }

}
