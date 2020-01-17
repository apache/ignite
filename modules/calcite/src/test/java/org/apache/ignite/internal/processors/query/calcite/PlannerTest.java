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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.calcite.exec.BypassExchangeService;
import org.apache.ignite.internal.processors.query.calcite.exec.ConsumerNode;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.Implementor;
import org.apache.ignite.internal.processors.query.calcite.exec.Node;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteCalciteContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerPhase;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerType;
import org.apache.ignite.internal.processors.query.calcite.prepare.Query;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.serialize.relation.RelGraph;
import org.apache.ignite.internal.processors.query.calcite.serialize.relation.RelToGraphConverter;
import org.apache.ignite.internal.processors.query.calcite.splitter.Fragment;
import org.apache.ignite.internal.processors.query.calcite.splitter.QueryPlan;
import org.apache.ignite.internal.processors.query.calcite.splitter.Splitter;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.TestTableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.processors.query.calcite.util.Commons.igniteRel;

/**
 *
 */
//@WithSystemProperty(key = "calcite.debug", value = "true")
@SuppressWarnings({"TooBroadScope", "FieldCanBeLocal", "ArraysAsListWithZeroOrOneArgument"})
public class PlannerTest extends GridCommonAbstractTest {
    /** */
    private GridKernalContext kernal;

    /** */
    private SchemaPlus schema;

    /** */
    private List<UUID> nodes;

    /** */
    private TestIgniteTable city;

    /** */
    private TestIgniteTable country;

    /** */
    private TestIgniteTable project;

    /** */
    private TestIgniteTable developer;

    /** */
    private Map<UUID, ExecutorService> executors;

    /** */
    private volatile Throwable lastException;

    /** */
    @Before
    public void setup() throws IgniteCheckedException {
        kernal = newContext();

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        developer = new TestIgniteTable(
            ImmutableList.of("PUBLIC", "Developer"),
            new TestTableDescriptor()
                .cacheName("Developer")
                .identityKey("hash")
                .field("id", Integer.class, true)
                .field("name", String.class)
                .field("projectId", Integer.class)
                .field("cityId", Integer.class),
            Arrays.asList(
                new Object[]{0, "Igor", 0, 1},
                new Object[]{1, "Roman", 0, 0}
        ));

        project = new TestIgniteTable(
            ImmutableList.of("PUBLIC", "Project"),
            new TestTableDescriptor()
                .cacheName("Project")
                .identityKey("hash")
                .field("id", Integer.class, true)
                .field("name", String.class)
                .field("ver", Integer.class),
            Arrays.asList(
                new Object[]{0, "Calcite", 1},
                new Object[]{1, "Ignite", 1}
        ));

        country = new TestIgniteTable(
            ImmutableList.of("PUBLIC", "Country"),
            new TestTableDescriptor()
                .cacheName("Country")
                .field("id", Integer.class, true)
                .field("name", String.class)
                .field("countryCode", Integer.class),
            Arrays.<Object[]>asList(
                new Object[]{0, "Russia", 7}
        ));

        city = new TestIgniteTable(
            ImmutableList.of("PUBLIC", "City"),
            new TestTableDescriptor()
                .cacheName("City")
                .field("id", Integer.class, true)
                .field("name", String.class)
                .field("countryId", Integer.class),
            Arrays.asList(
                new Object[]{0, "Moscow", 0},
                new Object[]{1, "Saint Petersburg", 0}
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

        executors = new ConcurrentHashMap<>();
    }

    @After
    public void tearDown() throws Throwable {
        for (ExecutorService executor : executors.values())
            U.shutdownNow(getClass(), executor, log());

        if (lastException != null)
            throw lastException;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLogicalPlan() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.id0 + 1" +
            "WHERE (d.projectId + 1) > ?";

        RelTraitDef<?>[] traitDefs = {
            ConventionTraitDef.INSTANCE
        };

        IgniteCalciteContext ctx = context(sql, new Object[]{2}, traitDefs);

        assertNotNull(ctx);

        RelRoot relRoot;

        try (IgnitePlanner planner = ctx.planner()){
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

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLogicalPlanDefaultSchema() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM Project pp" +
            ") p " +
            "ON d.projectId = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        RelTraitDef<?>[] traitDefs = {
            ConventionTraitDef.INSTANCE
        };

        IgniteCalciteContext ctx = context(sql, new Object[]{2}, traitDefs);

        assertNotNull(ctx);

        RelRoot relRoot;

        try (IgnitePlanner planner = ctx.planner()){
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

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCorrelatedQuery() throws Exception {
        String sql = "SELECT d.id, (SELECT p.name FROM Project p WHERE p.id = d.id) name, d.projectId " +
            "FROM Developer d";

        RelTraitDef<?>[] traitDefs = {
            ConventionTraitDef.INSTANCE
        };

        IgniteCalciteContext ctx = context(sql, new Object[]{2}, traitDefs);

        assertNotNull(ctx);

        RelRoot relRoot;

        try (IgnitePlanner planner = ctx.planner()){
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

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testHepPlaner() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        RelTraitDef<?>[] traitDefs = {
            ConventionTraitDef.INSTANCE
        };

        IgniteCalciteContext ctx = context(sql, new Object[]{2}, traitDefs);

        assertNotNull(ctx);

        RelRoot relRoot;

        try (IgnitePlanner planner = ctx.planner()){
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

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVolcanoPlanerDistributed() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        RelTraitDef<?>[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        IgniteCalciteContext ctx = context(sql, new Object[]{2}, traitDefs);

        assertNotNull(ctx);

        RelRoot relRoot;

        try (IgnitePlanner planner = ctx.planner()){
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
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPlanSerializationDeserialization() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.id = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        RelTraitDef<?>[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        IgniteCalciteContext ctx = context(sql, new Object[]{2}, traitDefs);

        assertNotNull(ctx);

        byte[] convertedBytes;

        try (IgnitePlanner planner = ctx.planner()){
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

            QueryPlan plan = new Splitter().go(igniteRel(rel));

            assertNotNull(plan);

            assertTrue(plan.fragments().size() == 2);

            plan.init(ctx);

            RelGraph graph = new RelToGraphConverter().go(igniteRel(plan.fragments().get(1).root()));

            convertedBytes = new JdkMarshaller().marshal(graph);

            assertNotNull(convertedBytes);
        }

        try (IgnitePlanner planner = ctx.planner()) {
            assertNotNull(planner);

            RelGraph graph = new JdkMarshaller().unmarshal(convertedBytes, getClass().getClassLoader());

            assertNotNull(graph);

            RelNode rel = planner.convert(graph);

            assertNotNull(rel);
        }
    }

    /**
     * @throws Exception If failed.
     */
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

        RelTraitDef<?>[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        IgniteCalciteContext ctx = context(sql, new Object[]{2}, traitDefs);

        assertNotNull(ctx);

        RelRoot relRoot;

        try (IgnitePlanner planner = ctx.planner()){
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

        assertEquals(2, plan.fragments().size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPhysicalPlan() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.name0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.name as name0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        RelTraitDef<?>[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        MappingService ms = new MappingService() {
            @Override public NodesMapping intermediateMapping(AffinityTopologyVersion topVer, int desiredCnt, Predicate<ClusterNode> nodeFilter) {
                return new NodesMapping(select(nodes, 0), null, NodesMapping.DEDUPLICATED);
            }

            @Override public NodesMapping cacheMapping(int cacheId, AffinityTopologyVersion topVer) {
                if (cacheId == CU.cacheId("Developer"))
                    return new NodesMapping(select(nodes, 0), null, NodesMapping.HAS_REPLICATED_CACHES);
                if (cacheId == CU.cacheId("Project"))
                    return new NodesMapping(select(nodes, 0), null, NodesMapping.HAS_REPLICATED_CACHES);

                throw new AssertionError("Unexpected cache id:" + cacheId);
            }
        };

        developer.identityKey(null);
        project.identityKey(null);

        IgniteCalciteContext ctx = context(sql, new Object[]{-10}, traitDefs, ms);

        assertNotNull(ctx);

        try (IgnitePlanner planner = ctx.planner()){
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

            RelTraitSet desired = rel.getCluster()
                .traitSetOf(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single());

            RelNode phys = planner.transform(PlannerType.VOLCANO, PlannerPhase.OPTIMIZATION, rel, desired);

            assertNotNull(phys);

            QueryPlan plan = new Splitter().go(igniteRel(phys));

            assertNotNull(plan);

            plan.init(ctx);

            List<Fragment> fragments = plan.fragments();

            ConsumerNode consumer = null;

            UUID queryId = UUID.randomUUID();

            for (Fragment fragment : fragments) {
                ExecutionContext ectx = new ExecutionContext(
                    ctx,
                    queryId,
                    fragment.fragmentId(),
                    null,
                    Commons.parametersMap(ctx.query().parameters()));
                Implementor implementor = new Implementor(ectx);
                Node<Object[]> exec = implementor.go(igniteRel(fragment.root()));

                if (!fragment.local())
                    exec.context().execute(exec::request);
                else
                    consumer = new ConsumerNode(ectx, exec);
            }

            assertNotNull(consumer);
            assertTrue(consumer.hasNext());

            ArrayList<Object[]> res = new ArrayList<>();

            while (consumer.hasNext())
                res.add(consumer.next());

            assertFalse(res.isEmpty());

            Assert.assertArrayEquals(new Object[]{0, "Igor", 0, "Calcite", 1}, res.get(0));
            Assert.assertArrayEquals(new Object[]{1, "Roman", 0, "Calcite", 1}, res.get(1));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSplitterCollocatedReplicatedReplicated() throws Exception {
        String sql = "SELECT d.id, (d.id + 1) as id2, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.id = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        MappingService ms = new MappingService() {
            @Override public NodesMapping intermediateMapping(AffinityTopologyVersion topVer, int desiredCnt, Predicate<ClusterNode> nodeFilter) {
                return new NodesMapping(select(nodes, 0,1,2), null, NodesMapping.DEDUPLICATED);
            }

            @Override public NodesMapping cacheMapping(int cacheId, AffinityTopologyVersion topVer) {
                if (cacheId == CU.cacheId("Developer"))
                    return new NodesMapping(select(nodes, 0,1,2), null, NodesMapping.HAS_REPLICATED_CACHES);
                if (cacheId == CU.cacheId("Project"))
                    return new NodesMapping(select(nodes, 0,1,2), null, NodesMapping.HAS_REPLICATED_CACHES);

                throw new AssertionError("Unexpected cache id:" + cacheId);
            }
        };

        developer.identityKey(null);
        project.identityKey(null);

        RelTraitDef<?>[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        IgniteCalciteContext ctx = context(sql, new Object[]{2}, traitDefs, ms);

        assertNotNull(ctx);

        RelRoot relRoot;

        try (IgnitePlanner planner = ctx.planner()){
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

        assertEquals(1, plan.fragments().size());
    }

    /**
     * @throws Exception If failed.
     */
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
            @Override public NodesMapping intermediateMapping(AffinityTopologyVersion topVer, int desiredCnt, Predicate<ClusterNode> nodeFilter) {
                return new NodesMapping(select(nodes, 0,1,2,3), null, NodesMapping.DEDUPLICATED);
            }

            @Override public NodesMapping cacheMapping(int cacheId, AffinityTopologyVersion topVer) {
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

        project.identityKey(null);

        RelTraitDef<?>[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        IgniteCalciteContext ctx = context(sql, new Object[]{2}, traitDefs, ms);

        assertNotNull(ctx);

        RelRoot relRoot;

        try (IgnitePlanner planner = ctx.planner()){
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

        assertEquals(2, plan.fragments().size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Ignore() // TODO here we have less effective plan than possible. Let's think on how improve it.
    public void testSplitterPartiallyCollocated() throws Exception {
        developer.identityKey(new Object());

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        MappingService ms = new MappingService() {
            @Override public NodesMapping intermediateMapping(AffinityTopologyVersion topVer, int desiredCnt, Predicate<ClusterNode> nodeFilter) {
                return new NodesMapping(select(nodes, 0,1,2,3), null, NodesMapping.DEDUPLICATED);
            }

            @Override public NodesMapping cacheMapping(int cacheId, AffinityTopologyVersion topVer) {
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

        project.identityKey(null);

        RelTraitDef<?>[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        IgniteCalciteContext ctx = context(sql, new Object[]{2}, traitDefs, ms);

        assertNotNull(ctx);

        RelRoot relRoot;

        try (IgnitePlanner planner = ctx.planner()){
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

        assertEquals(2, plan.fragments().size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Ignore() // TODO here we have less effective plan than possible. Let's think on how improve it.
    public void testSplitterNonCollocated() throws Exception {
        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.ver0 " +
            "WHERE (d.projectId + 1) > ?";

        MappingService ms = new MappingService() {
            @Override public NodesMapping intermediateMapping(AffinityTopologyVersion topVer, int desiredCnt, Predicate<ClusterNode> nodeFilter) {
                return new NodesMapping(select(nodes, 0,1,2,3), null, NodesMapping.DEDUPLICATED);
            }

            @Override public NodesMapping cacheMapping(int cacheId, AffinityTopologyVersion topVer) {
                if (cacheId == CU.cacheId("Developer"))
                    return new NodesMapping(select(nodes, 2), null, (byte)(NodesMapping.HAS_REPLICATED_CACHES | NodesMapping.PARTIALLY_REPLICATED));

                else if (cacheId == CU.cacheId("Project"))
                    return new NodesMapping(select(nodes, 0,1), null, (byte)(NodesMapping.HAS_REPLICATED_CACHES | NodesMapping.PARTIALLY_REPLICATED));

                throw new AssertionError("Unexpected cache id:" + cacheId);
            }
        };

        developer.identityKey(null);
        project.identityKey(null);

        RelTraitDef<?>[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        IgniteCalciteContext ctx = IgniteCalciteContext.builder(context(sql, new Object[]{2}, traitDefs, ms))
            .localNodeId(nodes.get(3))
            .build();

        assertNotNull(ctx);

        RelRoot relRoot;

        try (IgnitePlanner planner = ctx.planner()){
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

        assertEquals(2, plan.fragments().size());
    }

    /**
     * @throws Exception If failed.
     */
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
            @Override public NodesMapping intermediateMapping(AffinityTopologyVersion topVer, int desiredCnt, Predicate<ClusterNode> nodeFilter) {
                return new NodesMapping(select(nodes, 0,1,2,3), null, NodesMapping.DEDUPLICATED);
            }

            @Override public NodesMapping cacheMapping(int cacheId, AffinityTopologyVersion topVer) {
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

        project.identityKey(null);

        RelTraitDef<?>[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        IgniteCalciteContext ctx = context(sql, new Object[]{2}, traitDefs, ms);

        assertNotNull(ctx);

        RelRoot relRoot;

        try (IgnitePlanner planner = ctx.planner()){
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

        assertEquals(2, plan.fragments().size());
    }

    /**
     * @throws Exception If failed.
     */
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
            @Override public NodesMapping intermediateMapping(AffinityTopologyVersion topVer, int desiredCnt, Predicate<ClusterNode> nodeFilter) {
                return new NodesMapping(select(nodes, 0,1,2,3), null, NodesMapping.DEDUPLICATED);
            }

            @Override public NodesMapping cacheMapping(int cacheId, AffinityTopologyVersion topVer) {
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

        project.identityKey(null);

        RelTraitDef<?>[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        IgniteCalciteContext ctx = context(sql, new Object[]{2}, traitDefs, ms);

        assertNotNull(ctx);

        RelRoot relRoot;

        try (IgnitePlanner planner = ctx.planner()){
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

        assertEquals(3, plan.fragments().size());
    }

    /** */
    private static <T> List<T> select(List<T> src, int... idxs) {
        ArrayList<T> res = new ArrayList<>(idxs.length);

        for (int idx : idxs) {
            res.add(src.get(idx));
        }

        return res;
    }

    /** */
    private IgniteCalciteContext context(String query, Object[] params, RelTraitDef<?>[] traitDefs) {
        MappingService ms = new MappingService() {
            @Override public NodesMapping intermediateMapping(AffinityTopologyVersion topVer, int desiredCnt, Predicate<ClusterNode> nodeFilter) {
                return new NodesMapping(select(nodes, 0,1,2,3), null, NodesMapping.DEDUPLICATED);
            }

            @Override public NodesMapping cacheMapping(int cacheId, AffinityTopologyVersion topVer) {
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

        return context(query, params, traitDefs, ms);
    }

    /** */
    private IgniteCalciteContext context(String query, Object[] params, RelTraitDef<?>[] traitDefs, MappingService ms) {
        ExecutorService exec = executors.computeIfAbsent(nodes.get(0), id -> Executors.newSingleThreadExecutor());

        return IgniteCalciteContext.builder()
            .localNodeId(nodes.get(0))
            .parentContext(FRAMEWORK_CONFIG.getContext())
            .kernalContext(kernal)
            .frameworkConfig(Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema)
                .traitDefs(traitDefs)
                .build())
            .logger(log)
            .exchangeService(new BypassExchangeService(executors, log()))
            .query(new Query(query, params))
            .topologyVersion(AffinityTopologyVersion.NONE)
            .mappingService(ms)
            .taskExecutor((qid, fid, t) -> CompletableFuture.runAsync(t, exec).exceptionally(this::handle))
            .build();
    }

    /** */
    private Void handle(Throwable ex) {
        log().error(ex.getMessage(), ex);

        lastException = ex;

        return null;
    }

    /** */
    private static class TestIgniteTable extends IgniteTable {
        /** */
        private final TestTableDescriptor desc;

        /** */
        private final List<Object[]> data;

        /** */
        private TestIgniteTable(List<String> fullName, TestTableDescriptor desc, List<Object[]> data) {
            super(fullName, desc);
            this.desc = desc;
            this.data = data;
        }

        /** */
        private void identityKey(Object identityKey) {
            desc.identityKey(identityKey);
        }

        /** {@inheritDoc} */
        @Override public Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(data);
        }
    }
}
