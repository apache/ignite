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

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.calcite.GridCommonCalciteAbstractTest;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.processors.query.calcite.externalize.RelJsonReader;
import org.apache.ignite.internal.processors.query.calcite.message.CalciteMessage;
import org.apache.ignite.internal.processors.query.calcite.message.MessageServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.message.TestIoManager;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.Cloner;
import org.apache.ignite.internal.processors.query.calcite.prepare.Fragment;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerPhase;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.Splitter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTraitDef;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;

import static org.apache.calcite.tools.Frameworks.createRootSchema;
import static org.apache.calcite.tools.Frameworks.newConfigBuilder;
import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.processors.query.calcite.externalize.RelJsonWriter.toJson;

/**
 *
 */
//@WithSystemProperty(key = "calcite.debug", value = "true")
@SuppressWarnings({"TooBroadScope", "FieldCanBeLocal", "TypeMayBeWeakened", "unchecked"})
public abstract class AbstractPlannerTest extends GridCommonCalciteAbstractTest {
    /** */
    protected List<UUID> nodes;

    /** */
    protected List<QueryTaskExecutorImpl> executors;

    /** */
    protected volatile Throwable lastE;

    /** */
    @Before
    public void setup() {
        nodes = new ArrayList<>(4);

        for (int i = 0; i < 4; i++)
            nodes.add(UUID.randomUUID());
    }

    /** */
    @After
    public void tearDown() throws Throwable {
        if (!F.isEmpty(executors))
            executors.forEach(QueryTaskExecutorImpl::tearDown);

        if (lastE != null)
            throw lastE;
    }

    /** */
    interface TestVisitor {
        public void visit(RelNode node, int ordinal, RelNode parent);
    }

    /** */
    public static class TestRelVisitor extends RelVisitor {
        /** */
        final TestVisitor v;

        /** */
        TestRelVisitor(TestVisitor v) {
            this.v = v;
        }

        /** {@inheritDoc} */
        @Override public void visit(RelNode node, int ordinal, RelNode parent) {
            v.visit(node, ordinal, parent);

            super.visit(node, ordinal, parent);
        }
    }

    /** */
    protected static void relTreeVisit(RelNode n, TestVisitor v) {
        v.visit(n, -1, null);

        n.childrenAccept(new TestRelVisitor(v));
    }

    /** */
    public static <T extends RelNode> T findFirstNode(RelNode plan, Predicate<RelNode> pred) {
        return F.first(findNodes(plan, pred));
    }

    /** */
    public static <T extends RelNode> List<T> findNodes(RelNode plan, Predicate<RelNode> pred) {
        List<T> ret = new ArrayList<>();

        if (pred.test(plan))
            ret.add((T)plan);

        plan.childrenAccept(
            new RelVisitor() {
                @Override public void visit(RelNode node, int ordinal, RelNode parent) {
                    if (pred.test(node))
                        ret.add((T)node);

                    super.visit(node, ordinal, parent);
                }
            }
        );

        return ret;
    }

    /** */
    public static <T extends RelNode> Predicate<RelNode> byClass(Class<T> cls) {
        return node -> cls.isInstance(node);
    }

    /** */
    public static <T extends RelNode> Predicate<RelNode> byClass(Class<T> cls, Predicate<RelNode> pred) {
        return node -> cls.isInstance(node) && pred.test(node);
    }

    /** */
    protected IgniteRel physicalPlan(String sql, IgniteSchema publicSchema, String... disabledRules) throws Exception {
        SchemaPlus schema = createRootSchema(false)
            .add("PUBLIC", publicSchema);

        RelTraitDef<?>[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE,
            RelCollationTraitDef.INSTANCE,
            RewindabilityTraitDef.INSTANCE,
            CorrelationTraitDef.INSTANCE
        };

        PlanningContext ctx = PlanningContext.builder()
            .localNodeId(F.first(nodes))
            .originatingNodeId(F.first(nodes))
            .parentContext(Contexts.empty())
            .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema)
                .traitDefs(traitDefs)
                .build())
            .logger(log)
            .query(sql)
            .topologyVersion(AffinityTopologyVersion.NONE)
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

            // Transformation chain
            RelTraitSet desired = rel.getTraitSet()
                .replace(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single())
                .replace(CorrelationTrait.UNCORRELATED)
                .replace(RewindabilityTrait.ONE_WAY)
                .simplify();

            planner.setDisabledRules(ImmutableSet.copyOf(disabledRules));

            try {
                IgniteRel res = planner.transform(PlannerPhase.OPTIMIZATION, desired, rel);

//                System.out.println(planner.dump());

                return res;
            }
            catch (Throwable ex) {
                System.err.println(planner.dump());

                throw ex;
            }
        }
    }

    /** */
    protected RelNode originalLogicalTree(String sql, IgniteSchema publicSchema, String... disabledRules) throws Exception {
        SchemaPlus schema = createRootSchema(false)
            .add("PUBLIC", publicSchema);

        RelTraitDef<?>[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE,
            RelCollationTraitDef.INSTANCE,
            RewindabilityTraitDef.INSTANCE,
            CorrelationTraitDef.INSTANCE
        };

        PlanningContext ctx = PlanningContext.builder()
            .localNodeId(F.first(nodes))
            .originatingNodeId(F.first(nodes))
            .parentContext(Contexts.empty())
            .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema)
                .traitDefs(traitDefs)
                .build())
            .logger(log)
            .query(sql)
            .topologyVersion(AffinityTopologyVersion.NONE)
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

            return rel;
        }
    }

    /** */
    protected void checkSplitAndSerialization(IgniteRel rel, IgniteSchema publicSchema) {
        assertNotNull(rel);

        rel = Cloner.clone(rel);

        SchemaPlus schema = createRootSchema(false)
            .add("PUBLIC", publicSchema);

        List<Fragment> fragments = new Splitter().go(rel);
        List<String> serialized = new ArrayList<>(fragments.size());

        for (Fragment fragment : fragments)
            serialized.add(toJson(fragment.root()));

        assertNotNull(serialized);

        RelTraitDef<?>[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE,
            RelCollationTraitDef.INSTANCE,
            RewindabilityTraitDef.INSTANCE,
            CorrelationTraitDef.INSTANCE
        };

        PlanningContext ctx = PlanningContext.builder()
            .localNodeId(F.first(nodes))
            .originatingNodeId(F.first(nodes))
            .parentContext(Contexts.empty())
            .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema)
                .traitDefs(traitDefs)
                .build())
            .logger(log)
            .topologyVersion(AffinityTopologyVersion.NONE)
            .build();

        List<RelNode> deserializedNodes = new ArrayList<>();

        try (IgnitePlanner ignored = ctx.planner()) {
            for (String s : serialized) {
                RelJsonReader reader = new RelJsonReader(ctx.cluster(), ctx.catalogReader());
                deserializedNodes.add(reader.read(s));
            }
        }

        List<RelNode> expectedRels = fragments.stream()
            .map(f -> f.root())
            .collect(Collectors.toList());

        assertEquals("Invalid deserialization fragments count", expectedRels.size(), deserializedNodes.size());

        for (int i = 0; i < expectedRels.size(); ++i) {
            RelNode expected = expectedRels.get(i);
            RelNode deserialized = deserializedNodes.get(i);

            clearTraits(expected);
            clearTraits(deserialized);

            if (!expected.deepEquals(deserialized))
            assertTrue(
                "Invalid serialization / deserialization.\n" +
                    "Expected:\n" + RelOptUtil.toString(expected) +
                    "Deserialized:\n" + RelOptUtil.toString(deserialized),
                expected.deepEquals(deserialized)
            );
        }
    }

    /** */
    protected void clearTraits(RelNode rel) {
        GridTestUtils.setFieldValue(rel, AbstractRelNode.class, "traitSet", RelTraitSet.createEmpty());
        rel.getInputs().forEach(this::clearTraits);
    }

    /** */
    protected List<UUID> intermediateMapping(@NotNull AffinityTopologyVersion topVer, boolean single, @Nullable Predicate<ClusterNode> filter) {
        return single ? select(nodes, 0) : select(nodes, 0, 1, 2, 3);
    }

    /** */
    public static <T> List<T> select(List<T> src, int... idxs) {
        ArrayList<T> res = new ArrayList<>(idxs.length);

        for (int idx : idxs)
            res.add(src.get(idx));

        return res;
    }

    /** */
    protected <Row> Row row(ExecutionContext<Row> ctx, ImmutableBitSet requiredColumns, Object... fields) {
        Type[] types = new Type[fields.length];
        for (int i = 0; i < fields.length; i++)
            types[i] = fields[i] == null ? Object.class : fields[i].getClass();

        if (requiredColumns == null) {
            for (int i = 0; i < fields.length; i++)
                types[i] = fields[i] == null ? Object.class : fields[i].getClass();
        }
        else {
            for (int i = 0, j = requiredColumns.nextSetBit(0); j != -1; j = requiredColumns.nextSetBit(j + 1), i++)
                types[i] = fields[i] == null ? Object.class : fields[i].getClass();
        }

        return ctx.rowHandler().factory(types).create(fields);
    }

    /** */
    abstract static class TestTable implements IgniteTable {
        /** */
        private final String name;

        /** */
        private final RelProtoDataType protoType;

        /** */
        private final Map<String, IgniteIndex> indexes = new HashMap<>();

        /** */
        private final RewindabilityTrait rewindable;

        /** */
        private final double rowCnt;

        /** */
        TestTable(RelDataType type) {
            this(type, RewindabilityTrait.REWINDABLE);
        }

        /** */
        TestTable(RelDataType type, RewindabilityTrait rewindable) {
            this(type, rewindable, 100.0);
        }

        /** */
        TestTable(RelDataType type, RewindabilityTrait rewindable, double rowCnt) {
            this(UUID.randomUUID().toString(), type, rewindable, rowCnt);
        }

        /** */
        TestTable(String name, RelDataType type, RewindabilityTrait rewindable, double rowCnt) {
            protoType = RelDataTypeImpl.proto(type);
            this.rewindable = rewindable;
            this.rowCnt = rowCnt;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public IgniteLogicalTableScan toRel(RelOptCluster cluster, RelOptTable relOptTbl) {
            RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE)
                .replaceIf(RewindabilityTraitDef.INSTANCE, () -> rewindable)
                .replaceIf(DistributionTraitDef.INSTANCE, this::distribution);

            return IgniteLogicalTableScan.create(cluster, traitSet, relOptTbl, null, null, null);
        }

        /** {@inheritDoc} */
        @Override public IgniteLogicalIndexScan toRel(RelOptCluster cluster, RelOptTable relOptTbl, String idxName) {
            RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE)
                .replaceIf(DistributionTraitDef.INSTANCE, this::distribution)
                .replaceIf(RewindabilityTraitDef.INSTANCE, () -> rewindable)
                .replaceIf(RelCollationTraitDef.INSTANCE, getIndex(idxName)::collation);

            return IgniteLogicalIndexScan.create(cluster, traitSet, relOptTbl, idxName, null, null, null);
        }

        /** {@inheritDoc} */
        @Override public RelDataType getRowType(RelDataTypeFactory typeFactory, ImmutableBitSet bitSet) {
            RelDataType rowType = protoType.apply(typeFactory);

            if (bitSet != null) {
                RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(typeFactory);
                for (int i = bitSet.nextSetBit(0); i != -1; i = bitSet.nextSetBit(i + 1))
                    b.add(rowType.getFieldList().get(i));
                rowType = b.build();
            }

            return rowType;
        }

        /** {@inheritDoc} */
        @Override public Statistic getStatistic() {
            return new Statistic() {
                /** {@inheritDoc */
                @Override public Double getRowCount() {
                    return rowCnt;
                }

                /** {@inheritDoc */
                @Override public boolean isKey(ImmutableBitSet cols) {
                    return false;
                }

                /** {@inheritDoc */
                @Override public List<ImmutableBitSet> getKeys() {
                    throw new AssertionError();
                }

                /** {@inheritDoc */
                @Override public List<RelReferentialConstraint> getReferentialConstraints() {
                    throw new AssertionError();
                }

                /** {@inheritDoc */
                @Override public List<RelCollation> getCollations() {
                    return Collections.emptyList();
                }

                /** {@inheritDoc */
                @Override public RelDistribution getDistribution() {
                    throw new AssertionError();
                }
            };
        }

        /** {@inheritDoc} */
        @Override public <Row> Iterable<Row> scan(
            ExecutionContext<Row> execCtx,
            ColocationGroup group, Predicate<Row> filter,
            Function<Row, Row> transformer,
            ImmutableBitSet bitSet
        ) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public Schema.TableType getJdbcTableType() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public boolean isRolledUp(String col) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean rolledUpColumnValidInsideAgg(
            String column,
            SqlCall call,
            SqlNode parent,
            CalciteConnectionConfig config
        ) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public ColocationGroup colocationGroup(PlanningContext ctx) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public IgniteDistribution distribution() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public TableDescriptor descriptor() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public Map<String, IgniteIndex> indexes() {
            return Collections.unmodifiableMap(indexes);
        }

        /** {@inheritDoc} */
        @Override public void addIndex(IgniteIndex idxTbl) {
            indexes.put(idxTbl.name(), idxTbl);
        }

        /** */
        public TestTable addIndex(RelCollation collation, String name) {
            indexes.put(name, new IgniteIndex(collation, name, null, this));

            return this;
        }

        /** {@inheritDoc} */
        @Override public IgniteIndex getIndex(String idxName) {
            return indexes.get(idxName);
        }

        /** {@inheritDoc} */
        @Override public void removeIndex(String idxName) {
            throw new AssertionError();
        }

        /** */
        public String name() {
            return name;
        }
    }

    /** */
    static class TestMessageServiceImpl extends MessageServiceImpl {
        /** */
        private final TestIoManager mgr;

        /** */
        TestMessageServiceImpl(GridTestKernalContext kernal, TestIoManager mgr) {
            super(kernal);
            this.mgr = mgr;
        }

        /** {@inheritDoc} */
        @Override public void send(UUID nodeId, CalciteMessage msg) {
            mgr.send(localNodeId(), nodeId, msg);
        }

        /** {@inheritDoc} */
        @Override public boolean alive(UUID nodeId) {
            return true;
        }

        /** {@inheritDoc} */
        @Override protected void prepareMarshal(Message msg) {
            // No-op;
        }

        /** {@inheritDoc} */
        @Override protected void prepareUnmarshal(Message msg) {
            // No-op;
        }
    }

    /** */
    class TestFailureProcessor extends FailureProcessor {
        /** */
        TestFailureProcessor(GridTestKernalContext kernal) {
            super(kernal);
        }

        /** {@inheritDoc} */
        @Override public boolean process(FailureContext failureCtx) {
            Throwable ex = failureContext().error();
            log().error(ex.getMessage(), ex);

            lastE = ex;

            return true;
        }
    }
}
