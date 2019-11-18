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

package org.apache.ignite.internal.processors.query.calcite;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.calcite.metadata.DistributionRegistry;
import org.apache.ignite.internal.processors.query.calcite.metadata.LocationRegistry;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.Query;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rule.PlannerPhase;
import org.apache.ignite.internal.processors.query.calcite.rule.PlannerType;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.schema.RowType;
import org.apache.ignite.internal.processors.query.calcite.serialize.Expression;
import org.apache.ignite.internal.processors.query.calcite.serialize.RexToExpTranslator;
import org.apache.ignite.internal.processors.query.calcite.splitter.QueryPlan;
import org.apache.ignite.internal.processors.query.calcite.splitter.Splitter;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
//@WithSystemProperty(key = "calcite.debug", value = "true")
public class CalciteQueryProcessorTest extends GridCommonAbstractTest {

    private static CalciteQueryProcessor proc;
    private static SchemaPlus schema;

    private static TestRegistry registry;
    private static List<ClusterNode> nodes;

    @BeforeClass
    public static void setupClass() {
        proc = new CalciteQueryProcessor();

        proc.setLogger(log);
        proc.start(new GridTestKernalContext(log));

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable(new IgniteTable("Developer", "Developer",
            RowType.builder()
                .keyField("id", Integer.class, true)
                .field("name", String.class)
                .field("projectId", Integer.class)
                .field("cityId", Integer.class)
                .build()));

        publicSchema.addTable(new IgniteTable("Project", "Project",
            RowType.builder()
                .keyField("id", Integer.class, true)
                .field("name", String.class)
                .field("ver", Integer.class)
                .build()));



        publicSchema.addTable(new IgniteTable("Country", "Country",
            RowType.builder()
                .keyField("id", Integer.class, true)
                .field("name", String.class)
                .field("countryCode", Integer.class)
                .build()));

        publicSchema.addTable(new IgniteTable("City", "City",
            RowType.builder()
                .keyField("id", Integer.class, true)
                .field("name", String.class)
                .field("countryId", Integer.class)
                .build()));

        schema = Frameworks
            .createRootSchema(false)
            .add("PUBLIC", publicSchema);

        nodes = new ArrayList<>(4);

        for (int i = 0; i < 4; i++) {
            nodes.add(new GridTestNode(UUID.randomUUID()));
        }

        registry = new TestRegistry();
    }

    @Test
    public void testLogicalPlan() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.id0 + 1" +
            "WHERE (d.projectId + 1) > ?";

        Context ctx = proc.context(Contexts.of(schema, registry, AffinityTopologyVersion.NONE), sql, new Object[]{2});

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.unwrap(Query.class);

            assertNotNull(query);

            // Parse
            SqlNode sqlNode = planner.parse(query.sql());

            // Validate
            sqlNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            relRoot = planner.rel(sqlNode);
        }

        assertNotNull(relRoot.rel);
    }

    @Test
    public void testLogicalPlanDefaultSchema() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM Project pp" +
            ") p " +
            "ON d.projectId = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        Context ctx = proc.context(Contexts.of(schema, registry, AffinityTopologyVersion.NONE), sql, new Object[]{2});

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.unwrap(Query.class);

            assertNotNull(query);

            // Parse
            SqlNode sqlNode = planner.parse(query.sql());

            // Validate
            sqlNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            relRoot = planner.rel(sqlNode);
        }

        assertNotNull(relRoot.rel);


    }

    @Test
    public void testCorrelatedQuery() throws Exception {
        String sql = "SELECT d.id, (SELECT p.name FROM Project p WHERE p.id = d.id) name, d.projectId " +
            "FROM Developer d";

        Context ctx = proc.context(Contexts.of(schema, registry, AffinityTopologyVersion.NONE), sql, new Object[]{2});

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.unwrap(Query.class);

            assertNotNull(query);

            // Parse
            SqlNode sqlNode = planner.parse(query.sql());

            // Validate
            sqlNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            relRoot = planner.rel(sqlNode);
        }

        assertNotNull(relRoot.rel);
    }

    @Test
    public void testHepPlaner() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        Context ctx = proc.context(Contexts.of(schema, registry, AffinityTopologyVersion.NONE), sql, new Object[]{2});

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.unwrap(Query.class);

            assertNotNull(query);

            // Parse
            SqlNode sqlNode = planner.parse(query.sql());

            // Validate
            sqlNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            relRoot = planner.rel(sqlNode);

            RelNode rel = relRoot.rel;

            // Transformation chain
            rel = planner.transform(PlannerType.HEP, PlannerPhase.SUBQUERY_REWRITE, rel, rel.getTraitSet());

            relRoot = relRoot.withRel(rel).withKind(sqlNode.getKind());
        }

        assertNotNull(relRoot.rel);
    }

    @Test
    public void testVolcanoPlanerDistributed() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        Context ctx = proc.context(Contexts.of(schema, registry, AffinityTopologyVersion.NONE), sql, new Object[]{2});

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.unwrap(Query.class);

            assertNotNull(query);

            // Parse
            SqlNode sqlNode = planner.parse(query.sql());

            // Validate
            sqlNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            relRoot = planner.rel(sqlNode);

            RelNode rel = relRoot.rel;

            // Transformation chain
            RelTraitSet desired = rel.getCluster().traitSet()
                .replace(IgniteRel.IGNITE_CONVENTION)
                .replace(IgniteDistributions.single())
                .simplify();

            rel = planner.transform(PlannerType.VOLCANO, PlannerPhase.LOGICAL, rel, desired);

            relRoot = relRoot.withRel(rel).withKind(sqlNode.getKind());
        }

        assertNotNull(relRoot.rel);

        RexToExpTranslator translator = new RexToExpTranslator();

        Project proj = (Project) relRoot.rel.getInput(0);

        List<Expression> expressions = translator.visitList(proj.getProjects());

        assertNotNull(expressions);
    }

    @Test
    public void testSplitterCollocatedPartitionedPartitioned() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.id = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        Context ctx = proc.context(Contexts.of(schema, registry, AffinityTopologyVersion.NONE), sql, new Object[]{2});

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.unwrap(Query.class);

            assertNotNull(planner);

            // Parse
            SqlNode sqlNode = planner.parse(query.sql());

            // Validate
            sqlNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            relRoot = planner.rel(sqlNode);

            RelNode rel = relRoot.rel;

            // Transformation chain
            rel = planner.transform(PlannerType.HEP, PlannerPhase.SUBQUERY_REWRITE, rel, rel.getTraitSet());

            RelTraitSet desired = rel.getCluster().traitSet()
                .replace(IgniteRel.IGNITE_CONVENTION)
                .replace(IgniteDistributions.single())
                .simplify();

            rel = planner.transform(PlannerType.VOLCANO, PlannerPhase.LOGICAL, rel, desired);

            relRoot = relRoot.withRel(rel).withKind(sqlNode.getKind());
        }

        assertNotNull(relRoot);

        QueryPlan plan = new Splitter().go((IgniteRel) relRoot.rel);

        assertNotNull(plan);

        plan.init(ctx);

        assertNotNull(plan);

        assertTrue(plan.fragments().size() == 2);
    }

    @Test
    public void testSplitterCollocatedReplicatedReplicated() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.id = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        TestRegistry registry = new TestRegistry(){
            @Override public DistributionTrait distribution(int cacheId, RowType rowType) {
                return IgniteDistributions.broadcast();
            }

            @Override public NodesMapping distributed(int cacheId, AffinityTopologyVersion topVer) {
                if (cacheId == CU.cacheId("Developer"))
                    return new NodesMapping(select(nodes, 0,1,2), null, NodesMapping.HAS_REPLICATED_CACHES);
                if (cacheId == CU.cacheId("Project"))
                    return new NodesMapping(select(nodes, 0,1,2), null, NodesMapping.HAS_REPLICATED_CACHES);

                throw new AssertionError("Unexpected cache id:" + cacheId);
            }
        };


        Context ctx = proc.context(Contexts.of(schema, registry, AffinityTopologyVersion.NONE), sql, new Object[]{2});

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.unwrap(Query.class);

            assertNotNull(planner);

            // Parse
            SqlNode sqlNode = planner.parse(query.sql());

            // Validate
            sqlNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            relRoot = planner.rel(sqlNode);

            RelNode rel = relRoot.rel;

            // Transformation chain
            rel = planner.transform(PlannerType.HEP, PlannerPhase.SUBQUERY_REWRITE, rel, rel.getTraitSet());

            RelTraitSet desired = rel.getCluster().traitSet()
                .replace(IgniteRel.IGNITE_CONVENTION)
                .replace(IgniteDistributions.single())
                .simplify();

            rel = planner.transform(PlannerType.VOLCANO, PlannerPhase.LOGICAL, rel, desired);

            relRoot = relRoot.withRel(rel).withKind(sqlNode.getKind());
        }

        assertNotNull(relRoot);

        QueryPlan plan = new Splitter().go((IgniteRel) relRoot.rel);

        assertNotNull(plan);

        plan.init(ctx);

        assertNotNull(plan);

        assertTrue(plan.fragments().size() == 2);
    }

    @Test
    public void testSplitterCollocatedReplicatedAndPartitioned() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.id = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        TestRegistry registry = new TestRegistry(){
            @Override public DistributionTrait distribution(int cacheId, RowType rowType) {
                if (cacheId == CU.cacheId("Project"))
                    return IgniteDistributions.broadcast();

                return IgniteDistributions.hash(rowType.distributionKeys(), IgniteDistributions.hashFunction());
            }

            @Override public NodesMapping distributed(int cacheId, AffinityTopologyVersion topVer) {
                if (cacheId == CU.cacheId("Developer"))
                    return new NodesMapping(null, Arrays.asList(
                        select(nodes, 0,1),
                        select(nodes, 1,2),
                        select(nodes, 2,0),
                        select(nodes, 0,1),
                        select(nodes, 1,2)
                    ), NodesMapping.HAS_PARTITIONED_CACHES);
                if (cacheId == CU.cacheId("Project"))
                    return new NodesMapping(select(nodes, 0,1), null, (byte)(NodesMapping.HAS_REPLICATED_CACHES | NodesMapping.PARTIALLY_REPLICATED));

                throw new AssertionError("Unexpected cache id:" + cacheId);
            }
        };

        Context ctx = proc.context(Contexts.of(schema, registry, AffinityTopologyVersion.NONE), sql, new Object[]{2});

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.unwrap(Query.class);

            assertNotNull(planner);

            // Parse
            SqlNode sqlNode = planner.parse(query.sql());

            // Validate
            sqlNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            relRoot = planner.rel(sqlNode);

            RelNode rel = relRoot.rel;

            // Transformation chain
            rel = planner.transform(PlannerType.HEP, PlannerPhase.SUBQUERY_REWRITE, rel, rel.getTraitSet());

            RelTraitSet desired = rel.getCluster().traitSet()
                .replace(IgniteRel.IGNITE_CONVENTION)
                .replace(IgniteDistributions.single())
                .simplify();

            rel = planner.transform(PlannerType.VOLCANO, PlannerPhase.LOGICAL, rel, desired);

            relRoot = relRoot.withRel(rel).withKind(sqlNode.getKind());
        }

        assertNotNull(relRoot);

        QueryPlan plan = new Splitter().go((IgniteRel) relRoot.rel);

        assertNotNull(plan);

        plan.init(ctx);

        assertNotNull(plan);

        assertTrue(plan.fragments().size() == 2);
    }

    @Test
    public void testSplitterPartiallyCollocated() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        TestRegistry registry = new TestRegistry(){
            @Override public DistributionTrait distribution(int cacheId, RowType rowType) {
                if (cacheId == CU.cacheId("Project"))
                    return IgniteDistributions.broadcast();

                return IgniteDistributions.hash(rowType.distributionKeys(), IgniteDistributions.hashFunction());
            }

            @Override public NodesMapping distributed(int cacheId, AffinityTopologyVersion topVer) {
                if (cacheId == CU.cacheId("Developer"))
                    return new NodesMapping(null, Arrays.asList(
                        select(nodes, 1),
                        select(nodes, 2),
                        select(nodes, 2),
                        select(nodes, 0),
                        select(nodes, 1)
                    ), NodesMapping.HAS_PARTITIONED_CACHES);
                if (cacheId == CU.cacheId("Project"))
                    return new NodesMapping(select(nodes, 0,1), null, (byte)(NodesMapping.HAS_REPLICATED_CACHES | NodesMapping.PARTIALLY_REPLICATED));

                throw new AssertionError("Unexpected cache id:" + cacheId);
            }
        };

        Context ctx = proc.context(Contexts.of(schema, registry, AffinityTopologyVersion.NONE), sql, new Object[]{2});

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.unwrap(Query.class);

            assertNotNull(planner);

            // Parse
            SqlNode sqlNode = planner.parse(query.sql());

            // Validate
            sqlNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            relRoot = planner.rel(sqlNode);

            RelNode rel = relRoot.rel;

            // Transformation chain
            rel = planner.transform(PlannerType.HEP, PlannerPhase.SUBQUERY_REWRITE, rel, rel.getTraitSet());

            RelTraitSet desired = rel.getCluster().traitSet()
                .replace(IgniteRel.IGNITE_CONVENTION)
                .replace(IgniteDistributions.single())
                .simplify();

            rel = planner.transform(PlannerType.VOLCANO, PlannerPhase.LOGICAL, rel, desired);

            relRoot = relRoot.withRel(rel).withKind(sqlNode.getKind());
        }

        assertNotNull(relRoot);

        QueryPlan plan = new Splitter().go((IgniteRel) relRoot.rel);

        assertNotNull(plan);

        plan.init(ctx);

        assertNotNull(plan);

        assertTrue(plan.fragments().size() == 3);
    }

    @Test
    public void testSplitterNonCollocated() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.ver0 " +
            "WHERE (d.projectId + 1) > ?";

        TestRegistry registry = new TestRegistry(){
            @Override public DistributionTrait distribution(int cacheId, RowType rowType) {
                return IgniteDistributions.broadcast();
            }

            @Override public NodesMapping distributed(int cacheId, AffinityTopologyVersion topVer) {
                if (cacheId == CU.cacheId("Developer"))
                    return new NodesMapping(select(nodes, 2), null, (byte)(NodesMapping.HAS_REPLICATED_CACHES | NodesMapping.PARTIALLY_REPLICATED));

                else if (cacheId == CU.cacheId("Project"))
                    return new NodesMapping(select(nodes, 0,1), null, (byte)(NodesMapping.HAS_REPLICATED_CACHES | NodesMapping.PARTIALLY_REPLICATED));

                throw new AssertionError("Unexpected cache id:" + cacheId);
            }
        };

        Context ctx = proc.context(Contexts.of(schema, registry, AffinityTopologyVersion.NONE), sql, new Object[]{2});

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.unwrap(Query.class);

            assertNotNull(planner);

            // Parse
            SqlNode sqlNode = planner.parse(query.sql());

            // Validate
            sqlNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            relRoot = planner.rel(sqlNode);

            RelNode rel = relRoot.rel;

            // Transformation chain
            rel = planner.transform(PlannerType.HEP, PlannerPhase.SUBQUERY_REWRITE, rel, rel.getTraitSet());

            RelTraitSet desired = rel.getCluster().traitSet()
                .replace(IgniteRel.IGNITE_CONVENTION)
                .replace(IgniteDistributions.single())
                .simplify();

            rel = planner.transform(PlannerType.VOLCANO, PlannerPhase.LOGICAL, rel, desired);

            relRoot = relRoot.withRel(rel).withKind(sqlNode.getKind());
        }

        assertNotNull(relRoot);

        QueryPlan plan = new Splitter().go((IgniteRel) relRoot.rel);

        assertNotNull(plan);

        plan.init(ctx);

        assertNotNull(plan);

        assertTrue(plan.fragments().size() == 3);
    }

    @Test
    public void testSplitterPartiallyReplicated1() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.id = p.id0 " +
            "WHERE (d.projectId + 1) > ?";


        TestRegistry registry = new TestRegistry(){
            @Override public DistributionTrait distribution(int cacheId, RowType rowType) {
                if (cacheId == CU.cacheId("Project"))
                    return IgniteDistributions.broadcast();

                return IgniteDistributions.hash(rowType.distributionKeys(), IgniteDistributions.hashFunction());
            }

            @Override public NodesMapping distributed(int cacheId, AffinityTopologyVersion topVer) {
                if (cacheId == CU.cacheId("Developer"))
                    return new NodesMapping(null, Arrays.asList(
                        select(nodes, 0,1),
                        select(nodes, 1,2),
                        select(nodes, 2,0),
                        select(nodes, 0,1),
                        select(nodes, 1,2)
                    ), NodesMapping.HAS_PARTITIONED_CACHES);
                if (cacheId == CU.cacheId("Project"))
                    return new NodesMapping(select(nodes, 0,1), null, (byte)(NodesMapping.HAS_REPLICATED_CACHES | NodesMapping.PARTIALLY_REPLICATED));

                throw new AssertionError("Unexpected cache id:" + cacheId);
            }
        };

        Context ctx = proc.context(Contexts.of(schema, registry, AffinityTopologyVersion.NONE), sql, new Object[]{2});

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.unwrap(Query.class);

            assertNotNull(planner);

            // Parse
            SqlNode sqlNode = planner.parse(query.sql());

            // Validate
            sqlNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            relRoot = planner.rel(sqlNode);

            RelNode rel = relRoot.rel;

            // Transformation chain
            rel = planner.transform(PlannerType.HEP, PlannerPhase.SUBQUERY_REWRITE, rel, rel.getTraitSet());

            RelTraitSet desired = rel.getCluster().traitSet()
                .replace(IgniteRel.IGNITE_CONVENTION)
                .replace(IgniteDistributions.single())
                .simplify();

            rel = planner.transform(PlannerType.VOLCANO, PlannerPhase.LOGICAL, rel, desired);

            relRoot = relRoot.withRel(rel).withKind(sqlNode.getKind());
        }

        assertNotNull(relRoot);

        QueryPlan plan = new Splitter().go((IgniteRel) relRoot.rel);

        assertNotNull(plan);

        plan.init(ctx);

        assertNotNull(plan);

        assertTrue(plan.fragments().size() == 2);
    }

    @Test
    public void testSplitterPartiallyReplicated2() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.id = p.id0 " +
            "WHERE (d.projectId + 1) > ?";


        TestRegistry registry = new TestRegistry(){
            @Override public DistributionTrait distribution(int cacheId, RowType rowType) {
                if (cacheId == CU.cacheId("Project"))
                    return IgniteDistributions.broadcast();

                return IgniteDistributions.hash(rowType.distributionKeys(), IgniteDistributions.hashFunction());
            }

            @Override public NodesMapping distributed(int cacheId, AffinityTopologyVersion topVer) {
                if (cacheId == CU.cacheId("Developer"))
                    return new NodesMapping(null, Arrays.asList(
                        select(nodes, 0,1),
                        select(nodes, 2),
                        select(nodes, 2,0),
                        select(nodes, 0,1),
                        select(nodes, 1,2)
                    ), NodesMapping.HAS_PARTITIONED_CACHES);
                if (cacheId == CU.cacheId("Project"))
                    return new NodesMapping(select(nodes, 0,1), null, (byte)(NodesMapping.HAS_REPLICATED_CACHES | NodesMapping.PARTIALLY_REPLICATED));

                throw new AssertionError("Unexpected cache id:" + cacheId);
            }
        };
        Context ctx = proc.context(Contexts.of(schema, registry, AffinityTopologyVersion.NONE), sql, new Object[]{2});

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.unwrap(Query.class);

            assertNotNull(planner);

            // Parse
            SqlNode sqlNode = planner.parse(query.sql());

            // Validate
            sqlNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            relRoot = planner.rel(sqlNode);

            RelNode rel = relRoot.rel;

            // Transformation chain
            rel = planner.transform(PlannerType.HEP, PlannerPhase.SUBQUERY_REWRITE, rel, rel.getTraitSet());

            RelTraitSet desired = rel.getCluster().traitSet()
                .replace(IgniteRel.IGNITE_CONVENTION)
                .replace(IgniteDistributions.single())
                .simplify();

            rel = planner.transform(PlannerType.VOLCANO, PlannerPhase.LOGICAL, rel, desired);

            relRoot = relRoot.withRel(rel).withKind(sqlNode.getKind());
        }

        assertNotNull(relRoot);

        QueryPlan plan = new Splitter().go((IgniteRel) relRoot.rel);

        assertNotNull(plan);

        plan.init(ctx);

        assertNotNull(plan);

        assertTrue(plan.fragments().size() == 3);
    }

    private static <T> List<T> select(List<T> src, int... idxs) {
        ArrayList<T> res = new ArrayList<>(idxs.length);

        for (int idx : idxs) {
            res.add(src.get(idx));
        }

        return res;
    }

    private static class TestRegistry implements LocationRegistry, DistributionRegistry {
        @Override public NodesMapping random(AffinityTopologyVersion topVer) {
            return new NodesMapping(select(nodes, 0,1,2,3), null, (byte) 0);
        }

        @Override public NodesMapping local() {
            return new NodesMapping(select(nodes, 0), null, (byte) 0);
        }

        @Override public DistributionTrait distribution(int cacheId, RowType rowType) {
            return IgniteDistributions.hash(rowType.distributionKeys(), IgniteDistributions.hashFunction());
        }

        @Override public NodesMapping distributed(int cacheId, AffinityTopologyVersion topVer) {
            if (cacheId == CU.cacheId("Developer"))
                return new NodesMapping(null, Arrays.asList(
                    select(nodes, 0,1),
                    select(nodes, 1,2),
                    select(nodes, 2,0),
                    select(nodes, 0,1),
                    select(nodes, 1,2)
                ), NodesMapping.HAS_PARTITIONED_CACHES);
            if (cacheId == CU.cacheId("Project"))
                return new NodesMapping(null, Arrays.asList(
                    select(nodes, 0,1),
                    select(nodes, 1,2),
                    select(nodes, 2,0),
                    select(nodes, 0,1),
                    select(nodes, 1,2)
                ), NodesMapping.HAS_PARTITIONED_CACHES);

            throw new AssertionError("Unexpected cache id:" + cacheId);
        }
    }
}