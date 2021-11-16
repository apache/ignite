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

package org.apache.ignite.internal.processors.query.calcite.planner;

import static org.apache.calcite.tools.Frameworks.createRootSchema;
import static org.apache.calcite.tools.Frameworks.newConfigBuilder;
import static org.apache.ignite.internal.processors.query.calcite.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.MultiStepPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.MultiStepQueryPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerPhase;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryTemplate;
import org.apache.ignite.internal.processors.query.calcite.prepare.Splitter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * PlannerTest.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class PlannerTest extends AbstractPlannerTest {
    private static List<String> NODES;

    /**
     * Init.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @BeforeAll
    public static void init() {
        NODES = new ArrayList<>(4);

        for (int i = 0; i < 4; i++) {
            NODES.add(UUID.randomUUID().toString());
        }
    }

    @Test
    public void testSplitterColocatedPartitionedPartitioned() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("NAME", f.createJavaType(String.class))
                        .add("PROJECTID", f.createJavaType(Integer.class))
                        .build()) {
            @Override
            public IgniteIndex getIndex(String idxName) {
                return new IgniteIndex(null, null, null);
            }

            @Override
            public ColocationGroup colocationGroup(PlanningContext ctx) {
                return ColocationGroup.forAssignments(Arrays.asList(
                        select(NODES, 0, 1),
                        select(NODES, 1, 2),
                        select(NODES, 2, 0),
                        select(NODES, 0, 1),
                        select(NODES, 1, 2)
                ));
            }

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.affinity(0, "Developer", "hash");
            }
        };

        TestTable project = new TestTable(
                new RelDataTypeFactory.Builder(f)
                    .add("ID", f.createJavaType(Integer.class))
                    .add("NAME", f.createJavaType(String.class))
                    .add("VER", f.createJavaType(Integer.class))
                    .build()) {
            @Override public ColocationGroup colocationGroup(PlanningContext ctx) {
                return ColocationGroup.forAssignments(Arrays.asList(
                    select(NODES, 0, 1),
                    select(NODES, 1, 2),
                    select(NODES, 2, 0),
                    select(NODES, 0, 1),
                    select(NODES, 1, 2)));
            }

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.affinity(0, "Project", "hash");
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("DEVELOPER", developer);
        publicSchema.addTable("PROJECT", project);

        SchemaPlus schema = createRootSchema(false)
                .add("PUBLIC", publicSchema);

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 "
                + "FROM PUBLIC.Developer d JOIN ("
                + "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp"
                + ") p "
                + "ON d.id = p.id0";

        PlanningContext ctx = PlanningContext.builder()
                .localNodeId(first(NODES))
                .originatingNodeId(first(NODES))
                .parentContext(Contexts.empty())
                .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                        .defaultSchema(schema)
                        .build())
                .query(sql)
                .parameters(2)
                .build();

        assertNotNull(ctx);

        IgniteRel phys = physicalPlan(sql, ctx);

        assertNotNull(phys);

        MultiStepPlan plan = new MultiStepQueryPlan(new QueryTemplate(this::intermediateMapping,
                new Splitter().go(phys)), null);

        assertNotNull(plan);

        plan.init(ctx);

        assertNotNull(plan);

        assertEquals(2, plan.fragments().size());
    }

    @Test
    public void testSplitterColocatedReplicatedReplicated() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("NAME", f.createJavaType(String.class))
                        .add("PROJECTID", f.createJavaType(Integer.class))
                        .build()) {
            @Override
            public ColocationGroup colocationGroup(PlanningContext ctx) {
                return ColocationGroup.forNodes(select(NODES, 0, 1, 2, 3));
            }

            @Override
            public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        TestTable project = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("NAME", f.createJavaType(String.class))
                        .add("VER", f.createJavaType(Integer.class))
                        .build()) {
            @Override
            public ColocationGroup colocationGroup(PlanningContext ctx) {
                return ColocationGroup.forNodes(select(NODES, 0, 1, 2, 3));
            }

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("DEVELOPER", developer);
        publicSchema.addTable("PROJECT", project);

        SchemaPlus schema = createRootSchema(false)
                .add("PUBLIC", publicSchema);

        String sql = "SELECT d.id, (d.id + 1) as id2, d.name, d.projectId, p.id0, p.ver0 "
                + "FROM PUBLIC.Developer d JOIN ("
                + "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp"
                + ") p "
                + "ON d.id = p.id0 "
                + "WHERE (d.projectId + 1) > ?";

        PlanningContext ctx = PlanningContext.builder()
                .localNodeId(first(NODES))
                .originatingNodeId(first(NODES))
                .parentContext(Contexts.empty())
                .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                        .defaultSchema(schema)
                        .build())
                .query(sql)
                .parameters(2)
                .build();

        IgniteRel phys = physicalPlan(sql, ctx);

        assertNotNull(phys);

        MultiStepPlan plan = new MultiStepQueryPlan(new QueryTemplate(this::intermediateMapping,
                new Splitter().go(phys)), null);

        assertNotNull(plan);

        plan.init(ctx);

        assertNotNull(plan);

        assertEquals(1, plan.fragments().size());
    }

    @Test
    public void testSplitterPartiallyColocatedReplicatedAndPartitioned() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("NAME", f.createJavaType(String.class))
                        .add("PROJECTID", f.createJavaType(Integer.class))
                        .build()) {
            @Override
            public ColocationGroup colocationGroup(PlanningContext ctx) {
                return ColocationGroup.forNodes(select(NODES, 0));
            }

            @Override
            public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        TestTable project = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("NAME", f.createJavaType(String.class))
                        .add("VER", f.createJavaType(Integer.class))
                        .build()) {
            @Override
            public ColocationGroup colocationGroup(PlanningContext ctx) {
                return ColocationGroup.forAssignments(Arrays.asList(
                        select(NODES, 1, 2),
                        select(NODES, 2, 3),
                        select(NODES, 3, 0),
                        select(NODES, 0, 1)
                ));
            }

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.affinity(0, "Project", "hash");
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("DEVELOPER", developer);
        publicSchema.addTable("PROJECT", project);

        SchemaPlus schema = createRootSchema(false)
                .add("PUBLIC", publicSchema);

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 "
                + "FROM PUBLIC.Developer d JOIN ("
                + "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp"
                + ") p "
                + "ON d.id = p.id0 "
                + "WHERE (d.projectId + 1) > ?";

        PlanningContext ctx = PlanningContext.builder()
                .localNodeId(first(NODES))
                .originatingNodeId(first(NODES))
                .parentContext(Contexts.empty())
                .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                        .defaultSchema(schema)
                        .build())
                .query(sql)
                .parameters(2)
                .build();

        IgniteRel phys = physicalPlan(sql, ctx);

        assertNotNull(phys);

        MultiStepPlan plan = new MultiStepQueryPlan(new QueryTemplate(this::intermediateMapping,
                new Splitter().go(phys)), null);

        assertNotNull(plan);

        plan.init(ctx);

        assertEquals(3, plan.fragments().size());
    }

    @Test
    public void testSplitterPartiallyColocated1() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("NAME", f.createJavaType(String.class))
                        .add("PROJECTID", f.createJavaType(Integer.class))
                        .build()) {
            @Override
            public ColocationGroup colocationGroup(PlanningContext ctx) {
                return ColocationGroup.forNodes(select(NODES, 1, 2, 3));
            }

            @Override
            public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        TestTable project = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("NAME", f.createJavaType(String.class))
                        .add("VER", f.createJavaType(Integer.class))
                        .build()) {
            @Override
            public ColocationGroup colocationGroup(PlanningContext ctx) {
                return ColocationGroup.forAssignments(Arrays.asList(
                        select(NODES, 0),
                        select(NODES, 1),
                        select(NODES, 2)
                ));
            }

            @Override
            public IgniteDistribution distribution() {
                return IgniteDistributions.affinity(0, "Project", "hash");
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("DEVELOPER", developer);
        publicSchema.addTable("PROJECT", project);

        SchemaPlus schema = createRootSchema(false)
                .add("PUBLIC", publicSchema);

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 "
                + "FROM PUBLIC.Developer d JOIN ("
                + "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp"
                + ") p "
                + "ON d.projectId = p.id0 "
                + "WHERE (d.projectId + 1) > ?";

        PlanningContext ctx = PlanningContext.builder()
                .localNodeId(first(NODES))
                .originatingNodeId(first(NODES))
                .parentContext(Contexts.empty())
                .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                        .defaultSchema(schema)
                        .build())
                .query(sql)
                .parameters(2)
                .build();

        IgniteRel phys = physicalPlan(sql, ctx);

        assertNotNull(phys);

        MultiStepPlan plan = new MultiStepQueryPlan(new QueryTemplate(this::intermediateMapping,
                new Splitter().go(phys)), null);

        assertNotNull(plan);

        plan.init(ctx);

        assertNotNull(plan);

        assertEquals(3, plan.fragments().size());
    }

    @Test
    public void testSplitterPartiallyColocated2() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("NAME", f.createJavaType(String.class))
                        .add("PROJECTID", f.createJavaType(Integer.class))
                        .build()) {
            @Override
            public ColocationGroup colocationGroup(PlanningContext ctx) {
                return ColocationGroup.forNodes(select(NODES, 0));
            }

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        TestTable project = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("NAME", f.createJavaType(String.class))
                        .add("VER", f.createJavaType(Integer.class))
                        .build()) {
            @Override
            public ColocationGroup colocationGroup(PlanningContext ctx) {
                return ColocationGroup.forAssignments(Arrays.asList(
                        select(NODES, 1),
                        select(NODES, 2),
                        select(NODES, 3)
                ));
            }

            @Override
            public IgniteDistribution distribution() {
                return IgniteDistributions.affinity(0, "Project", "hash");
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("DEVELOPER", developer);
        publicSchema.addTable("PROJECT", project);

        SchemaPlus schema = createRootSchema(false)
                  .add("PUBLIC", publicSchema);

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 "
                + "FROM PUBLIC.Developer d JOIN ("
                + "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp"
                + ") p "
                + "ON d.projectId = p.id0 "
                + "WHERE (d.projectId + 1) > ?";

        PlanningContext ctx = PlanningContext.builder()
                .localNodeId(first(NODES))
                .originatingNodeId(first(NODES))
                .parentContext(Contexts.empty())
                .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                        .defaultSchema(schema)
                        .build())
                .query(sql)
                .parameters(2)
                .build();

        IgniteRel phys = physicalPlan(sql, ctx);

        assertNotNull(phys);

        MultiStepPlan plan = new MultiStepQueryPlan(new QueryTemplate(this::intermediateMapping,
                new Splitter().go(phys)), null);

        assertNotNull(plan);

        plan.init(ctx);

        assertEquals(3, plan.fragments().size());
    }

    @Test
    public void testSplitterNonColocated() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("NAME", f.createJavaType(String.class))
                        .add("PROJECTID", f.createJavaType(Integer.class))
                        .build()) {
            @Override
            public ColocationGroup colocationGroup(PlanningContext ctx) {
                return ColocationGroup.forNodes(select(NODES, 2));
            }

            @Override
            public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        TestTable project = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("NAME", f.createJavaType(String.class))
                        .add("VER", f.createJavaType(Integer.class))
                        .build()) {
            @Override
            public ColocationGroup colocationGroup(PlanningContext ctx) {
                return ColocationGroup.forNodes(select(NODES, 0, 1));
            }

            @Override
            public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("DEVELOPER", developer);
        publicSchema.addTable("PROJECT", project);

        SchemaPlus schema = createRootSchema(false)
                .add("PUBLIC", publicSchema);

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 "
                + "FROM PUBLIC.Developer d JOIN ("
                + "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp"
                + ") p "
                + "ON d.projectId = p.ver0 "
                + "WHERE (d.projectId + 1) > ?";

        PlanningContext ctx = PlanningContext.builder()
                .localNodeId(first(NODES))
                .originatingNodeId(first(NODES))
                .parentContext(Contexts.empty())
                .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                        .defaultSchema(schema)
                        .build())
                .query(sql)
                .parameters(2)
                .build();

        IgniteRel phys = physicalPlan(sql, ctx);

        assertNotNull(phys);

        MultiStepPlan plan = new MultiStepQueryPlan(new QueryTemplate(this::intermediateMapping,
                new Splitter().go(phys)), null);

        assertNotNull(plan);

        plan.init(ctx);

        assertNotNull(plan);

        assertEquals(2, plan.fragments().size());
    }

    @Test
    public void testMergeFilters() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable testTbl = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("VAL", f.createJavaType(String.class))
                        .build()) {
            @Override
            public IgniteDistribution distribution() {
                return IgniteDistributions.single();
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("TEST", testTbl);

        SchemaPlus schema = createRootSchema(false)
                .add("PUBLIC", publicSchema);

        String sql = "SELECT val from (\n"
                + "   SELECT * \n"
                + "   FROM TEST \n"
                + "   WHERE VAL = 10) \n"
                + "WHERE VAL = 10";

        PlanningContext ctx = PlanningContext.builder()
                .localNodeId(first(NODES))
                .originatingNodeId(first(NODES))
                .parentContext(Contexts.empty())
                .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                        .defaultSchema(schema)
                        .build())
                .query(sql)
                .parameters(2)
                .build();

        RelNode phys = physicalPlan(sql, publicSchema);

        assertNotNull(phys);

        AtomicInteger filterCnt = new AtomicInteger();

        // Counts filters af the plan.
        phys.childrenAccept(
            new RelVisitor() {
                @Override public void visit(RelNode node, int ordinal, RelNode parent) {
                    if (node instanceof IgniteFilter) {
                        filterCnt.incrementAndGet();
                    }

                    super.visit(node, ordinal, parent);
                }
            }
        );

        // Checks that two filter merged into one filter.
        // Expected plan:
        // IgniteProject(VAL=[$1])
        //  IgniteProject(ID=[$0], VAL=[$1])
        //    IgniteFilter(condition=[=(CAST($1):INTEGER, 10)])
        //      IgniteTableScan(table=[[PUBLIC, TEST]])
        assertEquals(0, filterCnt.get());
    }

    @Test
    public void testJoinPushExpressionRule() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable emp = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("NAME", f.createJavaType(String.class))
                        .add("DEPTNO", f.createJavaType(Integer.class))
                        .build()) {

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        TestTable dept = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("DEPTNO", f.createJavaType(Integer.class))
                        .add("NAME", f.createJavaType(String.class))
                        .build()) {

            @Override
            public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("EMP", emp);
        publicSchema.addTable("DEPT", dept);

        SchemaPlus schema = createRootSchema(false)
                .add("PUBLIC", publicSchema);

        String sql = "select d.deptno, e.deptno "
                + "from dept d, emp e "
                + "where d.deptno + e.deptno = 2";

        PlanningContext ctx = PlanningContext.builder()
                .localNodeId(first(NODES))
                .originatingNodeId(first(NODES))
                .parentContext(Contexts.empty())
                .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                        .defaultSchema(schema)
                        .costFactory(new IgniteCostFactory(1, 100, 1, 1))
                        .build())
                .query(sql)
                .build();

        RelRoot relRoot;

        try (IgnitePlanner planner = ctx.planner()) {
            assertNotNull(planner);

            String qry = ctx.query();

            assertNotNull(qry);

            // Parse
            SqlNode sqlNode = planner.parse(qry);

            // Validate
            sqlNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            relRoot = planner.rel(sqlNode);

            RelNode rel = relRoot.rel;

            assertNotNull(rel);
            assertEquals("LogicalProject(DEPTNO=[$0], DEPTNO0=[$4])\n"
                            + "  LogicalFilter(condition=[=(CAST(+($0, $4)):INTEGER, 2)])\n"
                            + "    LogicalJoin(condition=[true], joinType=[inner])\n"
                            + "      IgniteLogicalTableScan(table=[[PUBLIC, DEPT]])\n"
                            + "      IgniteLogicalTableScan(table=[[PUBLIC, EMP]])\n",
                    RelOptUtil.toString(rel));

            // Transformation chain
            RelTraitSet desired = rel.getCluster().traitSet()
                    .replace(IgniteConvention.INSTANCE)
                    .replace(IgniteDistributions.single())
                    .replace(CorrelationTrait.UNCORRELATED)
                    .simplify();

            IgniteRel phys = planner.transform(PlannerPhase.OPTIMIZATION, desired, rel);

            assertNotNull(phys);
            assertEquals(
                    "IgniteProject(DEPTNO=[$3], DEPTNO0=[$2])\n"
                            + "  IgniteCorrelatedNestedLoopJoin(condition=[=(CAST(+($3, $2)):INTEGER, 2)], "
                            + "joinType=[inner], correlationVariables=[[$cor2]])\n"
                            + "    IgniteTableScan(table=[[PUBLIC, EMP]])\n"
                            + "    IgniteTableScan(table=[[PUBLIC, DEPT]], filters=[=(CAST(+($t0, $cor2.DEPTNO)):INTEGER, 2)])\n",
                    RelOptUtil.toString(phys),
                    "Invalid plan:\n" + RelOptUtil.toString(phys)
            );

            checkSplitAndSerialization(phys, publicSchema);
        }
    }

    @Test
    public void testMergeJoinIsNotAppliedForNonEquiJoin() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable emp = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("ID", f.createJavaType(Integer.class))
                        .add("NAME", f.createJavaType(String.class))
                        .add("DEPTNO", f.createJavaType(Integer.class))
                        .build(), 1000) {

            @Override
            public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        emp.addIndex(new IgniteIndex(RelCollations.of(ImmutableIntList.of(1, 2)), "emp_idx", emp));

        TestTable dept = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("DEPTNO", f.createJavaType(Integer.class))
                        .add("NAME", f.createJavaType(String.class))
                        .build(), 100) {

            @Override
            public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        dept.addIndex(new IgniteIndex(RelCollations.of(ImmutableIntList.of(1, 0)), "dep_idx", dept));

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("EMP", emp);
        publicSchema.addTable("DEPT", dept);

        String sql = "select d.deptno, d.name, e.id, e.name from dept d join emp e "
                + "on d.deptno = e.deptno and e.name >= d.name order by e.name, d.deptno";

        RelNode phys = physicalPlan(sql, publicSchema, "CorrelatedNestedLoopJoin");

        assertNotNull(phys);
        assertEquals("IgniteSort(sort0=[$3], sort1=[$0], dir0=[ASC-nulls-first], dir1=[ASC-nulls-first])\n"
                        + "  IgniteProject(DEPTNO=[$3], NAME=[$4], ID=[$0], NAME0=[$1])\n"
                        + "    IgniteNestedLoopJoin(condition=[AND(=($3, $2), >=($1, $4))], joinType=[inner])\n"
                        + "      IgniteTableScan(table=[[PUBLIC, EMP]])\n"
                        + "      IgniteTableScan(table=[[PUBLIC, DEPT]])\n",
                RelOptUtil.toString(phys));
    }

    @Test
    public void testNotStandardFunctions() throws Exception {
        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        publicSchema.addTable(
                "TEST",
                new TestTable(
                        new RelDataTypeFactory.Builder(f)
                                .add("ID", f.createJavaType(Integer.class))
                                .add("VAL", f.createJavaType(String.class))
                                .build()) {

                    @Override
                    public IgniteDistribution distribution() {
                        return IgniteDistributions.affinity(0, "TEST", "hash");
                    }
                }
        );

        String[] queries = {
                "select REVERSE(val) from TEST", // MYSQL
                "select TO_DATE(val, 'yyyymmdd') from TEST" // ORACLE
        };

        for (String sql : queries) {
            IgniteRel phys = physicalPlan(
                    sql,
                    publicSchema
            );

            checkSplitAndSerialization(phys, publicSchema);
        }
    }

    /**
     * IntermediateMapping.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    private List<String> intermediateMapping(long topVer, boolean single,
            @Nullable Predicate<ClusterNode> filter) {
        return single ? select(NODES, 0) : select(NODES, 0, 1, 2, 3);
    }
}
