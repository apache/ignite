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

import java.util.List;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Exchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRecursiveTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRecursiveTableSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRepeatUnion;
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

    /** Checks the physical operators used to maintain the recursive delta. */
    @Test
    public void testRecursiveDeltaPlan() throws Exception {
        IgniteSchema schema = new IgniteSchema(DEFAULT_SCHEMA);

        IgniteRel plan = physicalPlan(
            "WITH RECURSIVE numbers(n) AS (" +
                "SELECT 1 " +
                "UNION ALL " +
                "SELECT n + 1 FROM numbers WHERE n < 3" +
            ") " +
            "SELECT n FROM numbers",
            schema
        );

        assertRecursivePlan(plan);
        assertEquals(2, findNodes(plan, byClass(IgniteRecursiveTableSpool.class)).size());
        assertEquals(1, findNodes(plan, byClass(IgniteRecursiveTableScan.class)).size());

        checkSplitAndSerialization(plan, schema);
    }

    /** A replicated source can be read on the coordinator without an exchange. */
    @Test
    public void testRecursiveCteWithReplicatedTable() throws Exception {
        IgniteSchema schema = hierarchySchema(IgniteDistributions.broadcast(), false);

        IgniteRel plan = physicalPlan(EMPLOYEE_HIERARCHY_QUERY, schema);

        assertRecursivePlan(plan);
        assertTrue(planDescription(plan), findNodes(plan, byClass(Exchange.class)).isEmpty());

        checkSplitAndSerialization(plan, schema);
    }

    /** A partitioned source has to be transferred to the coordinator-side recursive plan. */
    @Test
    public void testRecursiveCteWithPartitionedTable() throws Exception {
        IgniteDistribution distribution = IgniteDistributions.affinity(0, "EMPLOYEE", "hash");
        IgniteSchema schema = hierarchySchema(distribution, false);

        IgniteRel plan = physicalPlan(EMPLOYEE_HIERARCHY_QUERY, schema);

        assertRecursivePlan(plan);
        assertFalse(planDescription(plan), findNodes(plan, byClass(Exchange.class)).isEmpty());

        checkSplitAndSerialization(plan, schema);
    }

    /** Checks that a declared index remains available when planning the seed input. */
    @Test
    public void testRecursiveCteWithReplicatedIndexedTable() throws Exception {
        IgniteSchema schema = hierarchySchema(IgniteDistributions.broadcast(), true);

        IgniteRel plan = physicalPlan(EMPLOYEE_HIERARCHY_QUERY, schema);

        assertRecursivePlan(plan);

        IgniteRepeatUnion repeatUnion = findFirstNode(plan, byClass(IgniteRepeatUnion.class));

        assertFalse(planDescription(plan), findNodes(repeatUnion.getLeft(), byClass(IgniteIndexScan.class)).isEmpty());

        checkSplitAndSerialization(plan, schema);
    }

    /** Calcite places multiple non-recursive branches into the seed input of RepeatUnion. */
    @Test
    public void testRecursiveCteWithMultipleSeedBranches() throws Exception {
        IgniteSchema schema = new IgniteSchema(DEFAULT_SCHEMA);

        IgniteRel plan = physicalPlan(
            "WITH RECURSIVE numbers(n) AS (" +
                "SELECT 1 " +
                "UNION ALL " +
                "SELECT 10 " +
                "UNION ALL " +
                "SELECT n + 1 FROM numbers WHERE n < 3" +
            ") " +
            "SELECT n FROM numbers",
            schema
        );

        assertRecursivePlan(plan);

        IgniteRepeatUnion repeatUnion = findFirstNode(plan, byClass(IgniteRepeatUnion.class));
        IgniteValues seedValues = findFirstNode(repeatUnion.getLeft(), byClass(IgniteValues.class));

        assertNotNull(planDescription(plan), seedValues);
        assertEquals(planDescription(plan), 2, seedValues.getTuples().size());

        checkSplitAndSerialization(plan, schema);
    }

    /** Creates an employee table with the requested distribution and optional manager index. */
    private static IgniteSchema hierarchySchema(IgniteDistribution distribution, boolean withManagerIndex) {
        TestTable table = createTable(
            "EMPLOYEE",
            distribution,
            "ID", Integer.class,
            "MANAGER_ID", Integer.class
        );

        if (withManagerIndex)
            table.addIndex("EMPLOYEE_MANAGER_IDX", 1);

        return createSchema(table);
    }

    /** Checks common properties of every recursive plan. */
    private void assertRecursivePlan(IgniteRel plan) {
        List<IgniteRepeatUnion> repeatUnions = findNodes(plan, byClass(IgniteRepeatUnion.class));

        assertEquals(planDescription(plan), 1, repeatUnions.size());
        assertEquals(IgniteDistributions.single(), repeatUnions.get(0).distribution());
    }

    /** Returns a physical plan suitable for an assertion message. */
    private static String planDescription(IgniteRel plan) {
        return "Invalid plan:\n" + RelOptUtil.toString(plan);
    }
}
