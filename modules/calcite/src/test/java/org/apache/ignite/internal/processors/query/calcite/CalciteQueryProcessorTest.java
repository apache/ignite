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
import java.util.Map;
import java.util.UUID;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
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
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.calcite.exec.ConsumerNode;
import org.apache.ignite.internal.processors.query.calcite.exec.Implementor;
import org.apache.ignite.internal.processors.query.calcite.exec.Node;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.prepare.ContextValue;
import org.apache.ignite.internal.processors.query.calcite.prepare.DataContextImpl;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerPhase;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerType;
import org.apache.ignite.internal.processors.query.calcite.prepare.Query;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.serialize.expression.Expression;
import org.apache.ignite.internal.processors.query.calcite.serialize.expression.RexToExpTranslator;
import org.apache.ignite.internal.processors.query.calcite.serialize.relation.RelGraph;
import org.apache.ignite.internal.processors.query.calcite.serialize.relation.RelToGraphConverter;
import org.apache.ignite.internal.processors.query.calcite.splitter.QueryPlan;
import org.apache.ignite.internal.processors.query.calcite.splitter.Splitter;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.RowType;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.util.Commons.igniteRel;

/**
 *
 */
//@WithSystemProperty(key = "calcite.debug", value = "true")
public class CalciteQueryProcessorTest extends GridCommonAbstractTest {

    private GridTestKernalContext kernalContext;
    private CalciteQueryProcessor proc;
    private SchemaPlus schema;
    private List<UUID> nodes;

    private TestIgniteTable city;
    private TestIgniteTable country;
    private TestIgniteTable project;
    private TestIgniteTable developer;

    @Before
    public void setup() {
        kernalContext = new GridTestKernalContext(log);
        proc = new CalciteQueryProcessor();
        proc.setLogger(log);
        proc.start(kernalContext);

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        developer = new TestIgniteTable("Developer", "Developer",
            RowType.builder()
                .keyField("id", Integer.class, true)
                .field("name", String.class)
                .field("projectId", Integer.class)
                .field("cityId", Integer.class)
                .build(), Arrays.asList(
            new Object[]{0, null, 0, "Igor", 0, 1},
            new Object[]{1, null, 1, "Roman", 0, 0}
        ));

        project = new TestIgniteTable("Project", "Project",
            RowType.builder()
                .keyField("id", Integer.class, true)
                .field("name", String.class)
                .field("ver", Integer.class)
                .build(), Arrays.asList(
            new Object[]{0, null, 0, "Calcite", 1},
            new Object[]{1, null, 1, "Ignite", 1}
        ));

        country = new TestIgniteTable("Country", "Country",
            RowType.builder()
                .keyField("id", Integer.class, true)
                .field("name", String.class)
                .field("countryCode", Integer.class)
                .build(), Arrays.<Object[]>asList(
            new Object[]{0, null, 0, "Russia", 7}
        ));

        city = new TestIgniteTable("City", "City",
            RowType.builder()
                .keyField("id", Integer.class, true)
                .field("name", String.class)
                .field("countryId", Integer.class)
                .build(), Arrays.asList(
            new Object[]{0, null, 0, "Moscow", 0},
            new Object[]{1, null, 1, "Saint Petersburg", 0}
        ));

        publicSchema.addTable(developer);
        publicSchema.addTable(project);
        publicSchema.addTable(country);
        publicSchema.addTable(city);

        schema = Frameworks
            .createRootSchema(false)
            .add("PUBLIC", publicSchema);

        nodes = new ArrayList<>(4);

        for (int i = 0; i < 4; i++) {
            nodes.add(UUID.randomUUID());
        }
    }

    @Test
    public void testLogicalPlan() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.id0 + 1" +
            "WHERE (d.projectId + 1) > ?";

        PlannerContext ctx = proc.context(Contexts.empty(), sql, new Object[]{2}, this::context);

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.query();

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

        PlannerContext ctx = proc.context(Contexts.empty(), sql, new Object[]{2}, this::context);

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.query();

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

        PlannerContext ctx = proc.context(Contexts.empty(), sql, new Object[]{2}, this::context);

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.query();

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

        PlannerContext ctx = proc.context(Contexts.empty(), sql, new Object[]{2}, this::context);

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.query();

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

        PlannerContext ctx = proc.context(Contexts.empty(), sql, new Object[]{2}, this::context);

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.query();

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
                .replace(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single())
                .simplify();

            rel = planner.transform(PlannerType.VOLCANO, PlannerPhase.OPTIMIZATION, rel, desired);

            relRoot = relRoot.withRel(rel).withKind(sqlNode.getKind());
        }

        assertNotNull(relRoot.rel);

        RexToExpTranslator translator = new RexToExpTranslator();

        Project proj = (Project) relRoot.rel.getInput(0);

        List<Expression> expressions = translator.translate(proj.getProjects());

        assertNotNull(expressions);
    }

    @Test
    public void testPlanSerializationDeserialization() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.id = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        PlannerContext ctx = proc.context(Contexts.empty(), sql, new Object[]{2}, this::context);

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        byte[] convertedBytes;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.query();

            assertNotNull(planner);

            // Parse
            SqlNode sqlNode = planner.parse(query.sql());

            // Validate
            sqlNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            RelNode rel = planner.convert(sqlNode);

            // Transformation chain
            rel = planner.transform(PlannerType.HEP, PlannerPhase.SUBQUERY_REWRITE, rel, rel.getTraitSet());

            RelTraitSet desired = rel.getCluster().traitSet()
                .replace(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single())
                .simplify();

            rel = planner.transform(PlannerType.VOLCANO, PlannerPhase.OPTIMIZATION, rel, desired);

            assertNotNull(rel);

            QueryPlan plan = planner.plan(rel);

            assertNotNull(plan);

            assertTrue(plan.fragments().size() == 2);

            plan.init(ctx);

            RelGraph graph = new RelToGraphConverter().go(igniteRel(plan.fragments().get(1).root()));

            convertedBytes = new JdkMarshaller().marshal(graph);

            assertNotNull(convertedBytes);
        }

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)) {
            assertNotNull(planner);

            RelGraph graph = new JdkMarshaller().unmarshal(convertedBytes, getClass().getClassLoader());

            assertNotNull(graph);

            RelNode rel = planner.convert(graph);

            assertNotNull(rel);
        }
    }

    @Test
    public void testSplitterCollocatedPartitionedPartitioned() throws Exception {
        Object key = new Object();

        developer.identityKey(key);
        project.identityKey(key);

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.id = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        PlannerContext ctx = proc.context(Contexts.empty(), sql, new Object[]{2}, this::context);

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.query();

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
                .replace(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single())
                .simplify();

            rel = planner.transform(PlannerType.VOLCANO, PlannerPhase.OPTIMIZATION, rel, desired);

            relRoot = relRoot.withRel(rel).withKind(sqlNode.getKind());
        }

        assertNotNull(relRoot);

        QueryPlan plan = new Splitter().go(igniteRel(relRoot.rel));

        assertNotNull(plan);

        plan.init(ctx);

        assertNotNull(plan);

        assertTrue(plan.fragments().size() == 2);
    }

    @Test
    public void testPhysicalPlan() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.name0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.name as name0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        PlannerContext ctx = proc.context(Contexts.empty(), sql, new Object[]{-10}, this::context);

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.query();

            assertNotNull(planner);

            // Parse
            SqlNode sqlNode = planner.parse(query.sql());

            // Validate
            sqlNode = planner.validate(sqlNode);

            // Convert to Relational operators graph
            RelRoot relRoot = planner.rel(sqlNode);

            RelNode rel = relRoot.rel;

            // Transformation chain
            rel = planner.transform(PlannerType.HEP, PlannerPhase.SUBQUERY_REWRITE, rel, rel.getTraitSet());

            RelTraitSet desired = rel.getCluster().traitSetOf(IgniteConvention.INSTANCE);

            RelNode phys = planner.transform(PlannerType.VOLCANO, PlannerPhase.OPTIMIZATION, rel, desired);

            assertNotNull(phys);

            Map<String, Object> params = ctx.query().params(F.asMap(ContextValue.QUERY_ID.valueName(), new GridCacheVersion()));

            Implementor implementor = new Implementor(new DataContextImpl(params, ctx));

            Node<Object[]> exec = implementor.go(igniteRel(phys));

            assertNotNull(exec);

            assertTrue(exec instanceof ConsumerNode);

            ConsumerNode consumer = (ConsumerNode) exec;

            assertTrue(consumer.hasNext());

            ArrayList<Object[]> res = new ArrayList<>();

            while (consumer.hasNext())
                res.add(consumer.next());

            assertFalse(res.isEmpty());

            Assert.assertArrayEquals(new Object[]{0, "Igor", 0, "Calcite", 1}, res.get(0));
            Assert.assertArrayEquals(new Object[]{1, "Roman", 0, "Calcite", 1}, res.get(1));
        }
    }

    @Test
    public void testSplitterCollocatedReplicatedReplicated() throws Exception {
        String sql = "SELECT d.id, (d.id + 1) as id2, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.id = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        MappingService ms = new MappingService() {
            @Override public NodesMapping random(AffinityTopologyVersion topVer) {
                return new NodesMapping(select(nodes, 0,1,2,3), null, (byte) 0);
            }

            @Override public NodesMapping local() {
                return new NodesMapping(select(nodes, 0), null, (byte) 0);
            }

            @Override public NodesMapping distributed(int cacheId, AffinityTopologyVersion topVer) {
                if (cacheId == CU.cacheId("Developer"))
                    return new NodesMapping(select(nodes, 0,1,2), null, NodesMapping.HAS_REPLICATED_CACHES);
                if (cacheId == CU.cacheId("Project"))
                    return new NodesMapping(select(nodes, 0,1,2), null, NodesMapping.HAS_REPLICATED_CACHES);

                throw new AssertionError("Unexpected cache id:" + cacheId);
            }
        };

        PlannerContext ctx = proc.context(Contexts.empty(), sql, new Object[]{2}, (c, q) -> context(c, q, ms));
        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.query();

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
                .replace(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single())
                .simplify();

            rel = planner.transform(PlannerType.VOLCANO, PlannerPhase.OPTIMIZATION, rel, desired);

            relRoot = relRoot.withRel(rel).withKind(sqlNode.getKind());
        }

        assertNotNull(relRoot);

        QueryPlan plan = new Splitter().go(igniteRel(relRoot.rel));

        assertNotNull(plan);

        plan.init(ctx);

        assertNotNull(plan);

        assertTrue(plan.fragments().size() == 2);
    }

    @Test
    public void testSplitterCollocatedReplicatedAndPartitioned() throws Exception {
        developer.identityKey(new Object());

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.id = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        MappingService ms = new MappingService() {
            @Override public NodesMapping random(AffinityTopologyVersion topVer) {
                return new NodesMapping(select(nodes, 0,1,2,3), null, (byte) 0);
            }

            @Override public NodesMapping local() {
                return new NodesMapping(select(nodes, 0), null, (byte) 0);
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

        PlannerContext ctx = proc.context(Contexts.empty(), sql, new Object[]{2}, (c, q) -> context(c, q, ms));

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.query();

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
                .replace(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single())
                .simplify();

            rel = planner.transform(PlannerType.VOLCANO, PlannerPhase.OPTIMIZATION, rel, desired);

            relRoot = relRoot.withRel(rel).withKind(sqlNode.getKind());
        }

        assertNotNull(relRoot);

        QueryPlan plan = new Splitter().go(igniteRel(relRoot.rel));

        assertNotNull(plan);

        plan.init(ctx);

        assertNotNull(plan);

        assertTrue(plan.fragments().size() == 2);
    }

    @Test
    public void testSplitterPartiallyCollocated() throws Exception {
        developer.identityKey(new Object());

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        MappingService ms = new MappingService() {
            @Override public NodesMapping random(AffinityTopologyVersion topVer) {
                return new NodesMapping(select(nodes, 0,1,2,3), null, (byte) 0);
            }

            @Override public NodesMapping local() {
                return new NodesMapping(select(nodes, 0), null, (byte) 0);
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

        PlannerContext ctx = proc.context(Contexts.empty(), sql, new Object[]{2}, (c, q) -> context(c, q, ms));

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.query();

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
                .replace(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single())
                .simplify();

            rel = planner.transform(PlannerType.VOLCANO, PlannerPhase.OPTIMIZATION, rel, desired);

            relRoot = relRoot.withRel(rel).withKind(sqlNode.getKind());
        }

        assertNotNull(relRoot);

        QueryPlan plan = new Splitter().go(igniteRel(relRoot.rel));

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

        MappingService ms = new MappingService() {
            @Override public NodesMapping random(AffinityTopologyVersion topVer) {
                return new NodesMapping(select(nodes, 0,1,2,3), null, (byte) 0);
            }

            @Override public NodesMapping local() {
                return new NodesMapping(select(nodes, 0), null, (byte) 0);
            }

            @Override public NodesMapping distributed(int cacheId, AffinityTopologyVersion topVer) {
                if (cacheId == CU.cacheId("Developer"))
                    return new NodesMapping(select(nodes, 2), null, (byte)(NodesMapping.HAS_REPLICATED_CACHES | NodesMapping.PARTIALLY_REPLICATED));

                else if (cacheId == CU.cacheId("Project"))
                    return new NodesMapping(select(nodes, 0,1), null, (byte)(NodesMapping.HAS_REPLICATED_CACHES | NodesMapping.PARTIALLY_REPLICATED));

                throw new AssertionError("Unexpected cache id:" + cacheId);
            }
        };

        PlannerContext ctx = proc.context(Contexts.empty(), sql, new Object[]{2}, (c, q) -> context(c, q, ms));

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.query();

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
                .replace(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single())
                .simplify();

            rel = planner.transform(PlannerType.VOLCANO, PlannerPhase.OPTIMIZATION, rel, desired);

            relRoot = relRoot.withRel(rel).withKind(sqlNode.getKind());
        }

        assertNotNull(relRoot);

        QueryPlan plan = new Splitter().go(igniteRel(relRoot.rel));

        assertNotNull(plan);

        plan.init(ctx);

        assertNotNull(plan);

        assertTrue(plan.fragments().size() == 3);
    }

    @Test
    public void testSplitterPartiallyReplicated1() throws Exception {
        developer.identityKey(new Object());

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.id = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        MappingService ms = new MappingService() {
            @Override public NodesMapping random(AffinityTopologyVersion topVer) {
                return new NodesMapping(select(nodes, 0,1,2,3), null, (byte) 0);
            }

            @Override public NodesMapping local() {
                return new NodesMapping(select(nodes, 0), null, (byte) 0);
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

        PlannerContext ctx = proc.context(Contexts.empty(), sql, new Object[]{2}, (c, q) -> context(c, q, ms));

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.query();

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
                .replace(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single())
                .simplify();

            rel = planner.transform(PlannerType.VOLCANO, PlannerPhase.OPTIMIZATION, rel, desired);

            relRoot = relRoot.withRel(rel).withKind(sqlNode.getKind());
        }

        assertNotNull(relRoot);

        QueryPlan plan = new Splitter().go(igniteRel(relRoot.rel));

        assertNotNull(plan);

        plan.init(ctx);

        assertNotNull(plan);

        assertTrue(plan.fragments().size() == 2);
    }

    @Test
    public void testSplitterPartiallyReplicated2() throws Exception {
        developer.identityKey(new Object());

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.id = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        MappingService ms = new MappingService() {
            @Override public NodesMapping random(AffinityTopologyVersion topVer) {
                return new NodesMapping(select(nodes, 0,1,2,3), null, (byte) 0);
            }

            @Override public NodesMapping local() {
                return new NodesMapping(select(nodes, 0), null, (byte) 0);
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

        PlannerContext ctx = proc.context(Contexts.empty(), sql, new Object[]{2}, (c, q) -> context(c, q, ms));

        assertNotNull(ctx);

        RelTraitDef[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        RelRoot relRoot;

        try (IgnitePlanner planner = proc.planner(traitDefs, ctx)){
            assertNotNull(planner);

            Query query = ctx.query();

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
                .replace(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single())
                .simplify();

            rel = planner.transform(PlannerType.VOLCANO, PlannerPhase.OPTIMIZATION, rel, desired);

            relRoot = relRoot.withRel(rel).withKind(sqlNode.getKind());
        }

        assertNotNull(relRoot);

        QueryPlan plan = new Splitter().go(igniteRel(relRoot.rel));

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

    private PlannerContext context(Context c, Query q) {
        MappingService ms = new MappingService() {
            @Override public NodesMapping random(AffinityTopologyVersion topVer) {
                return new NodesMapping(select(nodes, 0,1,2,3), null, (byte) 0);
            }

            @Override public NodesMapping local() {
                return new NodesMapping(select(nodes, 0), null, (byte) 0);
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
        };

        return context(c, q, ms);
    }

    private PlannerContext context(Context parent, Query query, MappingService ms) {
        return PlannerContext.builder()
            .parentContext(parent)
            .logger(log)
            .kernalContext(kernalContext)
            .queryProcessor(proc)
            .query(query)
            .schema(schema)
            .topologyVersion(AffinityTopologyVersion.NONE)
            .mappingService(ms)
            .build();
    }

    public static class TestIgniteTable extends IgniteTable {
        private final List<Object[]> data;
        private Object identityKey;
        public TestIgniteTable(String tableName, String cacheName, RowType rowType, List<Object[]> data) {
            super(tableName, cacheName, rowType, null);
            this.data = data;
        }

        public void identityKey(Object identityKey) {
            this.identityKey = identityKey;
        }

        @Override public Object identityKey() {
            return identityKey;
        }

        @Override public Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(data);
        }
    }
}