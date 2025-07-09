/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.planner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerPhase;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.rel.AbstractIgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteColocatedAggregateBase;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.junit.Test;

/** Tests to verify correlated subquery planning. */
public class CorrelatedSubqueryPlannerTest extends AbstractPlannerTest {
    /**
     * Test verifies the row type is consistent for correlation variable and
     * node that actually puts this variable into a context.
     *
     * In this particular test the row type of the left input of CNLJ node should
     * match the row type correlated variable in the filter was created with.
     *
     * @throws Exception In case of any unexpected error.
     */
    @Test
    public void test() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("T1", IgniteDistributions.single(), "A", Integer.class,
                "B", Integer.class, "C", Integer.class, "D", Integer.class, "E", Integer.class)
        );

        String sql = "" +
            "SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b)\n" +
            "  FROM t1\n" +
            " WHERE (a>e-2 AND a<e+2)\n" +
            "    OR c>d\n" +
            " ORDER BY 1;";

        IgniteRel rel = physicalPlan(sql, schema, "FilterTableScanMergeRule");

        IgniteFilter filter = findFirstNode(rel, byClass(IgniteFilter.class)
            .and(f -> RexUtils.hasCorrelation(((Filter)f).getCondition())));

        RexFieldAccess fieldAccess = findFirst(filter.getCondition(), RexFieldAccess.class);

        assertNotNull(fieldAccess);
        assertEquals(
            RexCorrelVariable.class,
            fieldAccess.getReferenceExpr().getClass()
        );

        IgniteCorrelatedNestedLoopJoin join = findFirstNode(rel, byClass(IgniteCorrelatedNestedLoopJoin.class));

        assertEquals(
            fieldAccess.getReferenceExpr().getType(),
            join.getLeft().getRowType()
        );
    }

    /** TODO */
    @Test
    public void testCorrelateInSecondFilterSubquery() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("T1", IgniteDistributions.single(), "ID1", Integer.class, "NAME1", String.class, "REF1", Integer.class),
            createTable("T2", IgniteDistributions.single(), "ID2", Integer.class, "REF2", Integer.class),
            createTable("T3", IgniteDistributions.single(), "ID3", Integer.class, "REF3", Integer.class)
        );

        // IN
        assertPlan("SELECT ID1 FROM T1 WHERE EXISTS (" +
                "SELECT REF2 FROM T2 WHERE ID2 IN (SELECT REF3 FROM T3 WHERE ID3 = T1.REF1))", schema,
            nodeOrAnyChild(isTableScan("T1")));

        // EXISTS
        assertPlan("SELECT ID1 FROM T1 WHERE ID1 IN (" +
                "SELECT REF2 FROM T2 WHERE EXISTS (SELECT REF3 FROM T3 WHERE ID3 = T1.REF1))", schema,
            nodeOrAnyChild(isTableScan("T1")));

        // SOME
        assertPlan("SELECT ID1 FROM T1 WHERE ID1 IN (" +
                "SELECT REF2 FROM T2 WHERE ID2 < ANY (SELECT REF3 FROM T3 WHERE T3.ID3 = T1.REF1))", schema,
            nodeOrAnyChild(isTableScan("T1")));
    }

    /** */
    @Test
    public void testCorrelatesInJoin() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("T0", IgniteDistributions.single(), "ID", Integer.class, "PADDING_COL1", Integer.class,
                "PADDING_COL2", Integer.class, "VAL", Integer.class),
            createTable("T1", IgniteDistributions.single(), "ID", Integer.class, "VAL", Integer.class)
        );

        String sql = "SELECT T1.ID FROM T0 JOIN T1 ON T1.ID = (SELECT inner_t1.ID FROM T1 AS inner_t1 WHERE inner_t1.VAL = t0.VAL)";

        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
            .and(hasChildThat(isTableScan("T0"))).and(hasChildThat(isTableScan("T1")))));
    }

    /**
     * Test verifies resolving of collisions in the left hand of correlates.
     */
    @Test
    public void testCorrelatesCollisionsLeftHand() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("T1", IgniteDistributions.single(), "A", Integer.class,
                "B", Integer.class, "C", Integer.class, "D", Integer.class)
        );

        String sql = "SELECT * FROM t1 as cor WHERE " +
            "EXISTS (SELECT 1 FROM t1 WHERE t1.b = cor.a) AND " +
            "EXISTS (SELECT 1 FROM t1 WHERE t1.c = cor.a) AND " +
            "EXISTS (SELECT 1 FROM t1 WHERE t1.d = cor.a)";

        PlanningContext ctx = plannerCtx(sql, schema);

        try (IgnitePlanner planner = ctx.planner()) {
            RelNode rel = convertSubQueries(planner, ctx);

            List<LogicalCorrelate> correlates = findNodes(rel, byClass(LogicalCorrelate.class));

            assertEquals(3, correlates.size());

            // There are collisions by correlation id.
            assertEquals(correlates.get(0).getCorrelationId(), correlates.get(1).getCorrelationId());
            assertEquals(correlates.get(0).getCorrelationId(), correlates.get(2).getCorrelationId());

            rel = planner.replaceCorrelatesCollisions(rel);

            correlates = findNodes(rel, byClass(LogicalCorrelate.class));

            assertEquals(3, correlates.size());

            // There are no collisions by correlation id.
            assertFalse(correlates.get(0).getCorrelationId().equals(correlates.get(1).getCorrelationId()));
            assertFalse(correlates.get(0).getCorrelationId().equals(correlates.get(2).getCorrelationId()));
            assertFalse(correlates.get(1).getCorrelationId().equals(correlates.get(2).getCorrelationId()));

            List<LogicalFilter> filters = findNodes(rel, byClass(LogicalFilter.class)
                .and(f -> RexUtils.hasCorrelation(((Filter)f).getCondition())));

            assertEquals(3, filters.size());

            // Filters match correlates in reverse order (we find outer correlate first, but inner filter first).
            assertEquals(Collections.singleton(correlates.get(0).getCorrelationId()),
                RexUtils.extractCorrelationIds(filters.get(2).getCondition()));

            assertEquals(Collections.singleton(correlates.get(1).getCorrelationId()),
                RexUtils.extractCorrelationIds(filters.get(1).getCondition()));

            assertEquals(Collections.singleton(correlates.get(2).getCorrelationId()),
                RexUtils.extractCorrelationIds(filters.get(0).getCondition()));
        }
    }

    /**
     * Test verifies resolving of collisions in the right hand of correlates.
     */
    @Test
    public void testCorrelatesCollisionsRightHand() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("T1", IgniteDistributions.single(), "A", Integer.class)
        );

        String sql = "SELECT (SELECT (SELECT (SELECT cor.a))) FROM t1 as cor";

        PlanningContext ctx = plannerCtx(sql, schema);

        try (IgnitePlanner planner = ctx.planner()) {
            RelNode rel = convertSubQueries(planner, ctx);

            List<LogicalCorrelate> correlates = findNodes(rel, byClass(LogicalCorrelate.class));

            assertEquals(3, correlates.size());

            // There are collisions by correlation id.
            assertEquals(correlates.get(0).getCorrelationId(), correlates.get(1).getCorrelationId());
            assertEquals(correlates.get(0).getCorrelationId(), correlates.get(2).getCorrelationId());

            rel = planner.replaceCorrelatesCollisions(rel);

            correlates = findNodes(rel, byClass(LogicalCorrelate.class));

            assertEquals(1, correlates.size());
        }
    }

    /**
     * Test verifies resolving of collisions in right and left hands of correlates.
     */
    @Test
    public void testCorrelatesCollisionsMixed() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("T1", IgniteDistributions.single(), "A", Integer.class,
                "B", Integer.class, "C", Integer.class)
        );

        String sql = "SELECT * FROM t1 as cor WHERE " +
            "EXISTS (SELECT 1 FROM t1 WHERE t1.b = (SELECT cor.a)) AND " +
            "EXISTS (SELECT 1 FROM t1 WHERE t1.c = (SELECT cor.a))";

        PlanningContext ctx = plannerCtx(sql, schema);

        try (IgnitePlanner planner = ctx.planner()) {
            RelNode rel = convertSubQueries(planner, ctx);

            List<LogicalCorrelate> correlates = findNodes(rel, byClass(LogicalCorrelate.class));

            assertEquals(4, correlates.size());

            // There are collisions by correlation id.
            assertEquals(correlates.get(0).getCorrelationId(), correlates.get(1).getCorrelationId());
            assertEquals(correlates.get(0).getCorrelationId(), correlates.get(2).getCorrelationId());
            assertEquals(correlates.get(0).getCorrelationId(), correlates.get(3).getCorrelationId());

            rel = planner.replaceCorrelatesCollisions(rel);

            correlates = findNodes(rel, byClass(LogicalCorrelate.class));

            assertEquals(2, correlates.size());

            // There are no collisions by correlation id.
            assertFalse(correlates.get(0).getCorrelationId().equals(correlates.get(1).getCorrelationId()));

            List<LogicalProject> projects = findNodes(rel, byClass(LogicalProject.class)
                .and(f -> RexUtils.hasCorrelation(((Project)f).getProjects())));

            assertEquals(2, projects.size());

            assertEquals(Collections.singleton(correlates.get(0).getCorrelationId()),
                RexUtils.extractCorrelationIds(projects.get(1).getProjects()));

            assertEquals(Collections.singleton(correlates.get(1).getCorrelationId()),
                RexUtils.extractCorrelationIds(projects.get(0).getProjects()));
        }
    }

    /**
     * Test correlated distribution bypass set of nodes.
     */
    @Test
    public void testCorrelatedDistribution() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("TA1", IgniteDistributions.affinity(0, "cache1", "hash"),
                "A", Integer.class, "B", Integer.class, "C", Integer.class),
            createTable("TA2", IgniteDistributions.affinity(1, "cache2", "hash"),
                "A", Integer.class, "B", Integer.class, "C", Integer.class),
            createTable("TH1", IgniteDistributions.hash(Arrays.asList(0, 1)),
                "A", Integer.class, "B", Integer.class, "C", Integer.class),
            createTable("TH2", IgniteDistributions.hash(Arrays.asList(1, 2)),
                "A", Integer.class, "B", Integer.class, "C", Integer.class),
            createTable("TH3", IgniteDistributions.hash(Arrays.asList(0, 2)),
                "A", Integer.class, "B", Integer.class, "C", Integer.class)
        );

        Predicate<RelNode> colocatedPredicate = hasChildThat(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
            .and(input(0, isInstanceOf(IgniteTableScan.class)))
            .and(input(1, isInstanceOf(IgniteColocatedAggregateBase.class)
                .and(hasChildThat(isInstanceOf(IgniteExchange.class)).negate())
            )));

        // Affinity distribution.
        String sql = "SELECT a FROM ta1 WHERE EXISTS (SELECT a FROM ta2 WHERE ta2.b = ta1.a)";
        assertPlan(sql, schema, colocatedPredicate);

        // Hash distribution on two columns.
        sql = "SELECT a FROM th1 WHERE EXISTS (SELECT a FROM th2 WHERE th2.b = th1.a AND th2.c = th1.b)";
        assertPlan(sql, schema, colocatedPredicate);

        sql = "SELECT a FROM th1 WHERE EXISTS (SELECT a FROM th3 WHERE th3.a = th1.a AND th3.c = th1.b)";
        assertPlan(sql, schema, colocatedPredicate);

        // Additional AND condition in filter.
        sql = "SELECT a FROM th1 WHERE EXISTS (SELECT a FROM th2 WHERE th2.b = th1.a AND th2.c = th1.b AND th2.a = 1)";
        assertPlan(sql, schema, colocatedPredicate);

        // Aggregate with affinity distribution.
        sql = "SELECT (SELECT sum(a) FROM ta2 WHERE ta2.b = ta1.a) FROM ta1";
        assertPlan(sql, schema, colocatedPredicate);

        // Aggregate with set op with hash distribution on two columns.
        sql = "SELECT (SELECT sum(a) FROM (" +
            "   SELECT a FROM th2 WHERE th2.b = th1.a AND th2.c = th1.b " +
            "   INTERSECT " +
            "   SELECT a FROM th3 WHERE th3.a = th1.a AND th3.c = th1.b" +
            ")) FROM th1";
        assertPlan(sql, schema, colocatedPredicate);

        // Correlate on top of another join.
        sql = "SELECT a FROM ta1 WHERE ta1.a IN (SELECT a FROM th1) AND EXISTS (SELECT 1 FROM ta2 WHERE ta2.b = ta1.a)";
        assertPlan(sql, schema, hasChildThat(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
            .and(input(0, isInstanceOf(AbstractIgniteJoin.class)))
            .and(input(1, isInstanceOf(IgniteColocatedAggregateBase.class)
                .and(hasChildThat(isInstanceOf(IgniteExchange.class)).negate())
        ))));

        // Condition on not colocated column.
        sql = "SELECT a FROM ta1 WHERE EXISTS (SELECT a FROM ta2 WHERE ta2.a = ta1.a)";
        assertPlan(sql, schema, colocatedPredicate.negate());

        // Additional OR condition in filter.
        sql = "SELECT a FROM th1 WHERE EXISTS (SELECT a FROM th2 WHERE th2.b = th1.a AND th2.c = th1.b OR th2.a = 1)";
        assertPlan(sql, schema, colocatedPredicate.negate());

        // Not full set of hash distribution keys.
        sql = "SELECT a FROM th1 WHERE EXISTS (SELECT a FROM th2 WHERE th2.a = th1.a AND th2.c = th1.b)";
        assertPlan(sql, schema, colocatedPredicate.negate());

        sql = "SELECT a FROM th1 WHERE EXISTS (SELECT a FROM th2 WHERE th2.b = 1 AND th2.c = th1.b)";
        assertPlan(sql, schema, colocatedPredicate.negate());

        sql = "SELECT a FROM th1 WHERE EXISTS (SELECT a FROM th3 WHERE th3.a = th1.a AND th3.c = th1.c)";
        assertPlan(sql, schema, colocatedPredicate.negate());

        // Wrong order of hash distribution keys.
        sql = "SELECT a FROM th1 WHERE EXISTS (SELECT a FROM th2 WHERE th2.c = th1.a AND th2.b = th1.b)";
        assertPlan(sql, schema, colocatedPredicate.negate());

        // One input of set-op has not full set of hash distribution keys.
        sql = "SELECT (SELECT sum(a) FROM (" +
            "   SELECT a FROM th2 WHERE th2.a = th1.a AND th2.c = th1.b " +
            "   INTERSECT " +
            "   SELECT a FROM th3 WHERE th3.a = th1.a AND th3.c = th1.b" +
            ")) FROM th1";
        assertPlan(sql, schema, colocatedPredicate.negate());
    }

    /** */
    @Test
    public void testFunctionsRewriteWithCorrelatedSubquery() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("T1", IgniteDistributions.single(), "ID", Integer.class),
            createTable("T2", IgniteDistributions.single(), "ID", Integer.class)
        );

        // Also check result type inference.
        Predicate<RelDataType> rowTypeCheckerChar = t -> t.getFieldCount() == 1
            && t.getFieldList().get(0).getType().getSqlTypeName() == SqlTypeName.CHAR;

        Predicate<RelDataType> rowTypeCheckerNotNull = t -> rowTypeCheckerChar.test(t)
            && !t.getFieldList().get(0).getType().isNullable();

        Predicate<RelDataType> rowTypeCheckerNullable = t -> rowTypeCheckerChar.test(t)
            && t.getFieldList().get(0).getType().isNullable();

        assertPlan("SELECT COALESCE((SELECT MAX('0') FROM T1 WHERE T1.ID = T2.ID), '1') FROM T2", schema,
            n -> rowTypeCheckerNotNull.test(n.getRowType()));

        assertPlan("SELECT NVL((SELECT MAX('0') FROM T1 WHERE T1.ID = T2.ID), '1') FROM T2", schema,
            n -> rowTypeCheckerNotNull.test(n.getRowType()));

        assertPlan("SELECT NULLIF((SELECT MAX('0') FROM T1 WHERE T1.ID = T2.ID), '1') FROM T2", schema,
            n -> rowTypeCheckerNullable.test(n.getRowType()));
    }

    /** */
    private RelNode convertSubQueries(IgnitePlanner planner, PlanningContext ctx) throws Exception {
        // Parse and validate.
        SqlNode sqlNode = planner.validate(planner.parse(ctx.query()));

        // Create original logical plan.
        RelNode rel = planner.rel(sqlNode).rel;

        // Convert sub-queries to correlates.
        return planner.transform(PlannerPhase.HEP_DECORRELATE, rel.getTraitSet(), rel);
    }

    /**
     * Returns the first found node of given class from expression tree.
     *
     * @param expr Expression to search in.
     * @param clz Target class of the node we are looking for.
     * @param <T> Type of the node we are looking for.
     * @return the first matching node in terms of DFS or null if there
     *  is no such node.
     */
    private static <T> T findFirst(RexNode expr, Class<T> clz) {
        if (clz.isAssignableFrom(expr.getClass()))
            return clz.cast(expr);

        if (!(expr instanceof RexCall))
            return null;

        RexCall call = (RexCall)expr;

        for (RexNode op : call.getOperands()) {
            T res = findFirst(op, clz);

            if (res != null)
                return res;
        }

        return null;
    }
}
