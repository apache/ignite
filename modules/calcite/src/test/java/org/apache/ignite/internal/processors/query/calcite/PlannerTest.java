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
import java.util.function.Predicate;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.calcite.exec.ConsumerNode;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.Implementor;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.Node;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.processors.query.calcite.message.TestIoManager;
import org.apache.ignite.internal.processors.query.calcite.message.TestMessageService;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteCalciteContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerPhase;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerType;
import org.apache.ignite.internal.processors.query.calcite.prepare.Query;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.DistributedTable;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.schema.SortedTable;
import org.apache.ignite.internal.processors.query.calcite.serialize.relation.RelGraph;
import org.apache.ignite.internal.processors.query.calcite.serialize.relation.RelToGraphConverter;
import org.apache.ignite.internal.processors.query.calcite.splitter.Fragment;
import org.apache.ignite.internal.processors.query.calcite.splitter.QueryPlan;
import org.apache.ignite.internal.processors.query.calcite.splitter.Splitter;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.calcite.tools.Frameworks.createRootSchema;
import static org.apache.calcite.tools.Frameworks.newConfigBuilder;
import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping.DEDUPLICATED;
import static org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping.HAS_REPLICATED_CACHES;
import static org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping.PARTIALLY_REPLICATED;
import static org.apache.ignite.internal.processors.query.calcite.util.Commons.igniteRel;

/**
 *
 */
//@WithSystemProperty(key = "calcite.debug", value = "true")
@SuppressWarnings({"TooBroadScope", "FieldCanBeLocal"})
public class PlannerTest extends GridCommonAbstractTest {
    /** */
    private List<UUID> nodes;

    private List<QueryTaskExecutorImpl> executors;

    private volatile Throwable lastException;

    /** */
    @Before
    public void setup() throws IgniteCheckedException {
        nodes = new ArrayList<>(4);

        for (int i = 0; i < 4; i++)
            nodes.add(UUID.randomUUID());
    }

    @After
    public void tearDown() throws Throwable {
        if (!F.isEmpty(executors))
            executors.forEach(QueryTaskExecutorImpl::onStop);

        if (lastException != null)
            throw lastException;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLogicalPlan() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("PROJECTID", f.createJavaType(Integer.class))
                .build()) {
        };

        TestTable project = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("VER", f.createJavaType(Integer.class))
                .build()) {
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("DEVELOPER", developer);
        publicSchema.addTable("PROJECT", project);

        SchemaPlus schema = createRootSchema(false)
            .add("PUBLIC", publicSchema);

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.id0 + 1" +
            "WHERE (d.projectId + 1) > ?";

        RelTraitDef<?>[] traitDefs = {
            ConventionTraitDef.INSTANCE
        };

        IgniteCalciteContext ctx = IgniteCalciteContext.builder()
            .localNodeId(F.first(nodes))
            .originatingNodeId(F.first(nodes))
            .parentContext(Contexts.empty())
            .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema)
                .traitDefs(traitDefs)
                .build())
            .logger(log)
            .query(new Query(sql, new Object[]{2}))
            .topologyVersion(AffinityTopologyVersion.NONE)
            .build();

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
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("PROJECTID", f.createJavaType(Integer.class))
                .build()) {
        };

        TestTable project = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("VER", f.createJavaType(Integer.class))
                .build()) {
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("DEVELOPER", developer);
        publicSchema.addTable("PROJECT", project);

        SchemaPlus schema = createRootSchema(false)
            .add("PUBLIC", publicSchema);

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM Project pp" +
            ") p " +
            "ON d.projectId = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        RelTraitDef<?>[] traitDefs = {
            ConventionTraitDef.INSTANCE
        };

        IgniteCalciteContext ctx = IgniteCalciteContext.builder()
            .localNodeId(F.first(nodes))
            .originatingNodeId(F.first(nodes))
            .parentContext(Contexts.empty())
            .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema)
                .traitDefs(traitDefs)
                .build())
            .logger(log)
            .query(new Query(sql, new Object[]{2}))
            .topologyVersion(AffinityTopologyVersion.NONE)
            .build();

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
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("PROJECTID", f.createJavaType(Integer.class))
                .build()) {
        };

        TestTable project = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("VER", f.createJavaType(Integer.class))
                .build()) {
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("DEVELOPER", developer);
        publicSchema.addTable("PROJECT", project);

        SchemaPlus schema = createRootSchema(false)
            .add("PUBLIC", publicSchema);

        String sql = "SELECT d.id, (SELECT p.name FROM Project p WHERE p.id = d.id) name, d.projectId " +
            "FROM Developer d";

        RelTraitDef<?>[] traitDefs = {
            ConventionTraitDef.INSTANCE
        };

        IgniteCalciteContext ctx = IgniteCalciteContext.builder()
            .localNodeId(F.first(nodes))
            .originatingNodeId(F.first(nodes))
            .parentContext(Contexts.empty())
            .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema)
                .traitDefs(traitDefs)
                .build())
            .logger(log)
            .query(new Query(sql, new Object[]{2}))
            .topologyVersion(AffinityTopologyVersion.NONE)
            .build();

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
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("PROJECTID", f.createJavaType(Integer.class))
                .build()) {
            @Override public Enumerable<Object[]> scan(DataContext root) {
                return Linq4j.asEnumerable(Arrays.asList(
                    new Object[]{0, "Igor", 0},
                    new Object[]{1, "Roman", 0}
                ));
            }

            @Override public NodesMapping mapping(IgniteCalciteContext ctx) {
                return new NodesMapping(null, Arrays.asList(
                    select(nodes, 0, 1),
                    select(nodes, 1, 2),
                    select(nodes, 2, 0),
                    select(nodes, 0, 1),
                    select(nodes, 1, 2)
                ), NodesMapping.HAS_PARTITIONED_CACHES);
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
            @Override public Enumerable<Object[]> scan(DataContext root) {
                return Linq4j.asEnumerable(Arrays.asList(
                    new Object[]{0, "Calcite", 1},
                    new Object[]{1, "Ignite", 1}
                ));
            }

            @Override public NodesMapping mapping(IgniteCalciteContext ctx) {
                return new NodesMapping(null, Arrays.asList(
                    select(nodes, 0, 1),
                    select(nodes, 1, 2),
                    select(nodes, 2, 0),
                    select(nodes, 0, 1),
                    select(nodes, 1, 2)
                ), NodesMapping.HAS_PARTITIONED_CACHES);
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

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        RelTraitDef<?>[] traitDefs = {
            ConventionTraitDef.INSTANCE
        };

        IgniteCalciteContext ctx = IgniteCalciteContext.builder()
            .localNodeId(F.first(nodes))
            .originatingNodeId(F.first(nodes))
            .parentContext(Contexts.empty())
            .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema)
                .traitDefs(traitDefs)
                .build())
            .logger(log)
            .query(new Query(sql, new Object[]{2}))
            .topologyVersion(AffinityTopologyVersion.NONE)
            .build();

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
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("PROJECTID", f.createJavaType(Integer.class))
                .build()) {
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
            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.affinity(0, "Project", "hash");
            }
        };

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("DEVELOPER", developer);
        publicSchema.addTable("PROJECT", project);

        SchemaPlus schema = createRootSchema(false)
            .add("PUBLIC", publicSchema);

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

        IgniteCalciteContext ctx = IgniteCalciteContext.builder()
            .localNodeId(F.first(nodes))
            .originatingNodeId(F.first(nodes))
            .parentContext(Contexts.empty())
            .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema)
                .traitDefs(traitDefs)
                .build())
            .logger(log)
            .query(new Query(sql, new Object[]{2}))
            .topologyVersion(AffinityTopologyVersion.NONE)
            .build();

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
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("PROJECTID", f.createJavaType(Integer.class))
                .build()) {
            @Override public NodesMapping mapping(IgniteCalciteContext ctx) {
                return new NodesMapping(null, Arrays.asList(
                    select(nodes, 0, 1),
                    select(nodes, 1, 2),
                    select(nodes, 2, 0),
                    select(nodes, 0, 1),
                    select(nodes, 1, 2)
                ), NodesMapping.HAS_PARTITIONED_CACHES);
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
            @Override public NodesMapping mapping(IgniteCalciteContext ctx) {
                return new NodesMapping(null, Arrays.asList(
                    select(nodes, 0, 1),
                    select(nodes, 1, 2),
                    select(nodes, 2, 0),
                    select(nodes, 0, 1),
                    select(nodes, 1, 2)
                ), NodesMapping.HAS_PARTITIONED_CACHES);
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

        IgniteCalciteContext ctx = IgniteCalciteContext.builder()
            .localNodeId(F.first(nodes))
            .originatingNodeId(F.first(nodes))
            .parentContext(Contexts.empty())
            .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema)
                .traitDefs(traitDefs)
                .build())
            .logger(log)
            .query(new Query(sql, new Object[]{2}))
            .topologyVersion(AffinityTopologyVersion.NONE)
            .build();

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

            plan.init(this::intermediateMapping, ctx);

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
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("PROJECTID", f.createJavaType(Integer.class))
                .build()) {
            @Override public NodesMapping mapping(IgniteCalciteContext ctx) {
                return new NodesMapping(null, Arrays.asList(
                    select(nodes, 0, 1),
                    select(nodes, 1, 2),
                    select(nodes, 2, 0),
                    select(nodes, 0, 1),
                    select(nodes, 1, 2)
                ), NodesMapping.HAS_PARTITIONED_CACHES);
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
            @Override public NodesMapping mapping(IgniteCalciteContext ctx) {
                return new NodesMapping(null, Arrays.asList(
                    select(nodes, 0, 1),
                    select(nodes, 1, 2),
                    select(nodes, 2, 0),
                    select(nodes, 0, 1),
                    select(nodes, 1, 2)
                ), NodesMapping.HAS_PARTITIONED_CACHES);
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

        IgniteCalciteContext ctx = IgniteCalciteContext.builder()
            .localNodeId(F.first(nodes))
            .originatingNodeId(F.first(nodes))
            .parentContext(Contexts.empty())
            .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema)
                .traitDefs(traitDefs)
                .build())
            .logger(log)
            .query(new Query(sql, new Object[]{2}))
            .topologyVersion(AffinityTopologyVersion.NONE)
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

        plan.init(this::intermediateMapping, ctx);

        assertNotNull(plan);

        assertEquals(2, plan.fragments().size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPhysicalPlan() throws Exception {
        executors = new ArrayList<>();

        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("PROJECTID", f.createJavaType(Integer.class))
                .build()) {
            @Override public Enumerable<Object[]> scan(DataContext root) {
                return Linq4j.asEnumerable(Arrays.asList(
                    new Object[]{0, "Igor", 0},
                    new Object[]{1, "Roman", 0}
                ));
            }

            @Override public NodesMapping mapping(IgniteCalciteContext ctx) {
                return new NodesMapping(select(nodes, 1), null, (byte) (HAS_REPLICATED_CACHES | PARTIALLY_REPLICATED));
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
            @Override public Enumerable<Object[]> scan(DataContext root) {
                return Linq4j.asEnumerable(Arrays.asList(
                    new Object[]{0, "Calcite", 1},
                    new Object[]{1, "Ignite", 1}
                ));
            }

            @Override public NodesMapping mapping(IgniteCalciteContext ctx) {
                return new NodesMapping(select(nodes, 1), null, (byte) (HAS_REPLICATED_CACHES | PARTIALLY_REPLICATED));
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

        IgniteCalciteContext ctx = IgniteCalciteContext.builder()
            .localNodeId(F.first(nodes))
            .originatingNodeId(F.first(nodes))
            .parentContext(Contexts.empty())
            .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema)
                .traitDefs(traitDefs)
                .build())
            .logger(log)
            .query(new Query(sql, new Object[]{-10}))
            .topologyVersion(AffinityTopologyVersion.NONE)
            .build();

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

            plan.init(this::intermediateMapping, ctx);

            List<Fragment> fragments = plan.fragments();
            assertEquals(2, fragments.size());

            UUID queryId = UUID.randomUUID();

            TestIoManager mgr = new TestIoManager();
            GridTestKernalContext kernal;
            QueryTaskExecutorImpl taskExecutor;
            TestMessageService messageService;
            MailboxRegistryImpl mailboxRegistry;
            ExchangeServiceImpl exchangeService;
            ExecutionContext ectx;
            Node<Object[]> exec;

            //// Local part

            Fragment fragment = fragments.get(0);
            assert fragment.local();

            kernal = newContext();

            taskExecutor = new QueryTaskExecutorImpl(kernal);
            taskExecutor.onStart(kernal);
            executors.add(taskExecutor);

            messageService = new TestMessageService(nodes.get(0), kernal);
            messageService.taskExecutor(taskExecutor);
            mgr.register(messageService);

            mailboxRegistry = new MailboxRegistryImpl(kernal);

            exchangeService = new ExchangeServiceImpl(kernal);
            exchangeService.taskExecutor(taskExecutor);
            exchangeService.messageService(messageService);
            exchangeService.mailboxRegistry(mailboxRegistry);
            exchangeService.registerListeners();

            ectx = new ExecutionContext(taskExecutor, ctx, queryId, fragment.fragmentId(), null, Commons.parametersMap(query.parameters()));

            exec = new Implementor(c1 -> r1 -> 0, mailboxRegistry, exchangeService,
                new TestFailureProcessor(kernal), ectx, log()).go(igniteRel(fragment.root()));

            ConsumerNode consumer = new ConsumerNode(ectx, exec);

            //// Remote part

            fragment = fragments.get(1);

            assert !fragment.local();

            kernal = newContext();

            taskExecutor = new QueryTaskExecutorImpl(kernal);
            taskExecutor.onStart(kernal);
            executors.add(taskExecutor);

            messageService = new TestMessageService(nodes.get(1), kernal);
            messageService.taskExecutor(taskExecutor);
            mgr.register(messageService);

            mailboxRegistry = new MailboxRegistryImpl(kernal);

            exchangeService = new ExchangeServiceImpl(kernal);
            exchangeService.taskExecutor(taskExecutor);
            exchangeService.messageService(messageService);
            exchangeService.mailboxRegistry(mailboxRegistry);
            exchangeService.registerListeners();

            ectx = new ExecutionContext(
                taskExecutor,
                IgniteCalciteContext.builder()
                    .localNodeId(nodes.get(1))
                    .originatingNodeId(nodes.get(0))
                    .parentContext(Contexts.empty())
                    .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                        .defaultSchema(schema)
                        .traitDefs(traitDefs)
                        .build())
                    .logger(log)
                    .build(),
                queryId,
                fragment.fragmentId(),
                null,
                Commons.parametersMap(query.parameters()));

            exec = new Implementor(c -> r -> 0, mailboxRegistry, exchangeService,
                new TestFailureProcessor(kernal), ectx, log()).go(igniteRel(fragment.root()));

            //// Start execution

            exec.context().execute(exec::request);

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
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("PROJECTID", f.createJavaType(Integer.class))
                .build()) {
            @Override public NodesMapping mapping(IgniteCalciteContext ctx) {
                return new NodesMapping(select(nodes, 0,1,2,3), null, HAS_REPLICATED_CACHES);
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
            @Override public NodesMapping mapping(IgniteCalciteContext ctx) {
                return new NodesMapping(select(nodes, 0,1,2,3), null, HAS_REPLICATED_CACHES);
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

        String sql = "SELECT d.id, (d.id + 1) as id2, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.id = p.id0 " +
            "WHERE (d.projectId + 1) > ?";

        RelTraitDef<?>[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        IgniteCalciteContext ctx = IgniteCalciteContext.builder()
            .localNodeId(F.first(nodes))
            .originatingNodeId(F.first(nodes))
            .parentContext(Contexts.empty())
            .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema)
                .traitDefs(traitDefs)
                .build())
            .logger(log)
            .query(new Query(sql, new Object[]{2}))
            .topologyVersion(AffinityTopologyVersion.NONE)
            .build();

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

        plan.init((t,d,n) -> null, ctx);

        assertNotNull(plan);

        assertEquals(1, plan.fragments().size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSplitterCollocatedReplicatedAndPartitioned() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("PROJECTID", f.createJavaType(Integer.class))
                .build()) {
            @Override public NodesMapping mapping(IgniteCalciteContext ctx) {
                return new NodesMapping(select(nodes, 0), null, (byte) (HAS_REPLICATED_CACHES | PARTIALLY_REPLICATED));
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
            @Override public NodesMapping mapping(IgniteCalciteContext ctx) {
                return new NodesMapping(null, Arrays.asList(
                    select(nodes, 1,2),
                    select(nodes, 2,3),
                    select(nodes, 3,0),
                    select(nodes, 0,1)
                ), NodesMapping.HAS_PARTITIONED_CACHES);
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

        IgniteCalciteContext ctx = IgniteCalciteContext.builder()
            .localNodeId(F.first(nodes))
            .originatingNodeId(F.first(nodes))
            .parentContext(Contexts.empty())
            .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema)
                .traitDefs(traitDefs)
                .build())
            .logger(log)
            .query(new Query(sql, new Object[]{2}))
            .topologyVersion(AffinityTopologyVersion.NONE)
            .build();

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

        plan.init(this::intermediateMapping, ctx);

        assertNotNull(plan);

        assertEquals(2, plan.fragments().size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSplitterPartiallyCollocated1() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("PROJECTID", f.createJavaType(Integer.class))
                .build()) {
            @Override public NodesMapping mapping(IgniteCalciteContext ctx) {
                return new NodesMapping(select(nodes, 1,2,3), null, (byte) (HAS_REPLICATED_CACHES | PARTIALLY_REPLICATED));
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
            @Override public NodesMapping mapping(IgniteCalciteContext ctx) {
                return new NodesMapping(null, Arrays.asList(
                    select(nodes, 0),
                    select(nodes, 1),
                    select(nodes, 2)
                ), NodesMapping.HAS_PARTITIONED_CACHES);
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

        IgniteCalciteContext ctx = IgniteCalciteContext.builder()
            .localNodeId(F.first(nodes))
            .originatingNodeId(F.first(nodes))
            .parentContext(Contexts.empty())
            .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema)
                .traitDefs(traitDefs)
                .build())
            .logger(log)
            .query(new Query(sql, new Object[]{2}))
            .topologyVersion(AffinityTopologyVersion.NONE)
            .build();

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

        plan.init(this::intermediateMapping, ctx);

        assertNotNull(plan);

        assertEquals(3, plan.fragments().size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSplitterPartiallyCollocated2() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("PROJECTID", f.createJavaType(Integer.class))
                .build()) {
            @Override public NodesMapping mapping(IgniteCalciteContext ctx) {
                return new NodesMapping(select(nodes, 0), null, (byte) (HAS_REPLICATED_CACHES | PARTIALLY_REPLICATED));
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
            @Override public NodesMapping mapping(IgniteCalciteContext ctx) {
                return new NodesMapping(null, Arrays.asList(
                    select(nodes, 1),
                    select(nodes, 2),
                    select(nodes, 3)
                ), NodesMapping.HAS_PARTITIONED_CACHES);
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

        IgniteCalciteContext ctx = IgniteCalciteContext.builder()
            .localNodeId(F.first(nodes))
            .originatingNodeId(F.first(nodes))
            .parentContext(Contexts.empty())
            .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema)
                .traitDefs(traitDefs)
                .build())
            .logger(log)
            .query(new Query(sql, new Object[]{2}))
            .topologyVersion(AffinityTopologyVersion.NONE)
            .build();

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

        plan.init(this::intermediateMapping, ctx);

        assertNotNull(plan);

        assertEquals(2, plan.fragments().size());
    }


    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSplitterNonCollocated() throws Exception {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable developer = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("PROJECTID", f.createJavaType(Integer.class))
                .build()) {
            @Override public NodesMapping mapping(IgniteCalciteContext ctx) {
                return new NodesMapping(select(nodes, 2), null, (byte) (HAS_REPLICATED_CACHES | PARTIALLY_REPLICATED));
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
            @Override public NodesMapping mapping(IgniteCalciteContext ctx) {
                return new NodesMapping(select(nodes, 0,1), null, (byte) (HAS_REPLICATED_CACHES | PARTIALLY_REPLICATED));
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

        String sql = "SELECT d.id, d.name, d.projectId, p.id0, p.ver0 " +
            "FROM PUBLIC.Developer d JOIN (" +
            "SELECT pp.id as id0, pp.ver as ver0 FROM PUBLIC.Project pp" +
            ") p " +
            "ON d.projectId = p.ver0 " +
            "WHERE (d.projectId + 1) > ?";

        RelTraitDef<?>[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE
        };

        IgniteCalciteContext ctx = IgniteCalciteContext.builder()
            .localNodeId(F.first(nodes))
            .originatingNodeId(F.first(nodes))
            .parentContext(Contexts.empty())
            .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema)
                .traitDefs(traitDefs)
                .build())
            .logger(log)
            .query(new Query(sql, new Object[]{2}))
            .topologyVersion(AffinityTopologyVersion.NONE)
            .build();

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

        plan.init(this::intermediateMapping, ctx);

        assertNotNull(plan);

        assertEquals(2, plan.fragments().size());
    }

    /** */
    private NodesMapping intermediateMapping(@NotNull AffinityTopologyVersion topVer, int desiredCount, @Nullable Predicate<ClusterNode> filter) {
        List<UUID> nodes = desiredCount == 1 ? select(this.nodes, 0) : select(this.nodes, 0, 1, 2, 3);
        return new NodesMapping(nodes, null, DEDUPLICATED);
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
    private abstract static class TestTable extends AbstractTable implements TranslatableTable, ScannableTable, DistributedTable, SortedTable {
        /** */
        private final RelProtoDataType protoType;

        /** */
        private TestTable(RelDataType type) {
            protoType = RelDataTypeImpl.proto(type);
        }

        /** {@inheritDoc} */
        @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
            RelOptCluster cluster = context.getCluster();
            RelTraitSet traitSet = cluster.traitSetOf(IgniteConvention.INSTANCE)
                .replaceIfs(RelCollationTraitDef.INSTANCE, this::collations)
                .replaceIf(DistributionTraitDef.INSTANCE, this::distribution);

            return new IgniteTableScan(cluster, traitSet, relOptTable);
        }

        /** {@inheritDoc} */
        @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return protoType.apply(typeFactory);
        }

        /** {@inheritDoc} */
        @Override public Enumerable<Object[]> scan(DataContext root) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public NodesMapping mapping(IgniteCalciteContext ctx) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public IgniteDistribution distribution() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public List<RelCollation> collations() {
            throw new AssertionError();
        }
    }

    /** */
    private class TestFailureProcessor extends FailureProcessor {
        /** */
        private TestFailureProcessor(GridTestKernalContext kernal) {
            super(kernal);
        }

        /** {@inheritDoc} */
        @Override public boolean process(FailureContext failureCtx) {
            Throwable ex = failureContext().error();
            log().error(ex.getMessage(), ex);

            lastException = ex;

            return true;
        }
    }
}
