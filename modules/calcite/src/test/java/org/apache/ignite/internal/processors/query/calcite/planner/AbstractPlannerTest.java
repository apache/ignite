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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
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
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.externalize.RelJsonReader;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.Cloner;
import org.apache.ignite.internal.processors.query.calcite.prepare.Fragment;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerHelper;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.Splitter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.ColumnDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTraitDef;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.ArrayUtils;
import org.jetbrains.annotations.Nullable;

import static org.apache.calcite.tools.Frameworks.createRootSchema;
import static org.apache.calcite.tools.Frameworks.newConfigBuilder;
import static org.apache.ignite.internal.processors.query.calcite.externalize.RelJsonWriter.toJson;
import static org.apache.ignite.internal.processors.query.calcite.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** */
public abstract class AbstractPlannerTest extends IgniteAbstractTest {
    /** */
    protected static final IgniteTypeFactory TYPE_FACTORY = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

    /** */
    protected static final int DEFAULT_TBL_SIZE = 500_000;

    /** Last error message. */
    private String lastErrorMsg;

    /** */
    interface TestVisitor {
        /** */
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
        return first(findNodes(plan, pred));
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
        return cls::isInstance;
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
            .parentContext(Contexts.empty())
            .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema)
                .traitDefs(traitDefs)
                .build())
            .query(sql)
            .build();

        try (IgnitePlanner planner = ctx.planner()) {
            assertNotNull(planner);

            planner.setDisabledRules(ImmutableSet.copyOf(disabledRules));

            String qry = ctx.query();

            assertNotNull(qry);

            // Parse
            SqlNode sqlNode = planner.parse(qry);

            // Validate
            sqlNode = planner.validate(sqlNode);

            try {
                IgniteRel rel = PlannerHelper.optimize(sqlNode, planner);

                checkSplitAndSerialization(rel, publicSchema);

//                System.out.println(RelOptUtil.toString(rel));

                return rel;
            }
            catch (Throwable ex) {
                System.err.println(planner.dump());

                throw ex;
            }
        }
    }

    /** */
    public static <T> List<T> select(List<T> src, int... idxs) {
        ArrayList<T> res = new ArrayList<>(idxs.length);

        for (int idx : idxs)
            res.add(src.get(idx));

        return res;
    }

    /** */
    protected static void createTable(IgniteSchema schema, String name, RelDataType type, IgniteDistribution distr) {
        TestTable table = new TestTable(type) {
            @Override public IgniteDistribution distribution() {
                return distr;
            }

            @Override public String name() {
                return name;
            }
        };

        schema.addTable(name, table);
    }

    /** */
    protected <T extends RelNode> void assertPlan(String sql, IgniteSchema schema, Predicate<T> predicate,
        String... disabledRules) throws Exception {
        IgniteRel plan = physicalPlan(sql, schema, disabledRules);

        if (!predicate.test((T)plan)) {
            String invalidPlanMsg = "Invalid plan (" + lastErrorMsg + "):\n" +
                RelOptUtil.toString(plan, SqlExplainLevel.ALL_ATTRIBUTES);

            fail(invalidPlanMsg);
        }
    }

    /**
     * Predicate builder for "Instance of class" condition.
     */
    protected <T extends RelNode> Predicate<T> isInstanceOf(Class<T> cls) {
        return node -> {
            if (cls.isInstance(node))
                return true;

            lastErrorMsg = "Unexpected node class [node=" + node + ", cls=" + cls.getSimpleName() + ']';

            return false;
        };
    }

    /**
     * Predicate builder for "Table scan with given name" condition.
     */
    protected <T extends RelNode> Predicate<IgniteTableScan> isTableScan(String tableName) {
        return isInstanceOf(IgniteTableScan.class).and(
            n -> {
                String scanTableName = n.getTable().unwrap(TestTable.class).name();

                if (tableName.equalsIgnoreCase(scanTableName))
                    return true;

                lastErrorMsg = "Unexpected table name [exp=" + tableName + ", act=" + scanTableName + ']';

                return false;
            });
    }

    /**
     * Predicate builder for "Any child satisfy predicate" condition.
     */
    protected <T extends RelNode> Predicate<RelNode> hasChildThat(Predicate<T> predicate) {
        return new Predicate<RelNode>() {
            public boolean checkRecursively(RelNode node) {
                if (predicate.test((T)node))
                    return true;

                for (RelNode input : node.getInputs()) {
                    if (checkRecursively(input))
                        return true;
                }

                return false;
            }

            @Override public boolean test(RelNode node) {
                for (RelNode input : node.getInputs()) {
                    if (checkRecursively(input))
                        return true;
                }

                lastErrorMsg = "Not found child for defined condition [node=" + node + ']';

                return false;
            }
        };
    }

    /**
     * Predicate builder for "Current node or any child satisfy predicate" condition.
     */
    protected Predicate<RelNode> nodeOrAnyChild(Predicate<? extends RelNode> predicate) {
        return (Predicate<RelNode>)predicate.or(hasChildThat(predicate));
    }

    /**
     * Predicate builder for "Input with given index satisfy predicate" condition.
     */
    protected <T extends RelNode> Predicate<RelNode> input(int idx, Predicate<T> predicate) {
        return node -> {
            int size = nullOrEmpty(node.getInputs()) ? 0 : node.getInputs().size();
            if (size <= idx) {
                lastErrorMsg = "No input for node [idx=" + idx + ", node=" + node + ']';

                return false;
            }

            return predicate.test((T)node.getInput(idx));
        };
    }

    /**
     * Creates test table with given params.
     *
     * @param name Name of the table.
     * @param distr Distribution of the table.
     * @param fields List of the required fields. Every odd item should be a string
     *               representing a column name, every even item should be a class representing column's type.
     *               E.g. {@code createTable("MY_TABLE", distribution, "ID", Integer.class, "VAL", String.class)}.
     * @return Instance of the {@link TestTable}.
     */
    protected static TestTable createTable(String name, IgniteDistribution distr, Object... fields) {
        return createTable(name, DEFAULT_TBL_SIZE, distr, fields);
    }

    /**
     * Creates test table with given params.
     *
     * @param name Name of the table.
     * @param size Required size of the table.
     * @param distr Distribution of the table.
     * @param fields List of the required fields. Every odd item should be a string
     *               representing a column name, every even item should be a class representing column's type.
     *               E.g. {@code createTable("MY_TABLE", 500, distribution, "ID", Integer.class, "VAL", String.class)}.
     * @return Instance of the {@link TestTable}.
     */
    protected static TestTable createTable(String name, int size, IgniteDistribution distr, Object... fields) {
        if (ArrayUtils.nullOrEmpty(fields) || fields.length % 2 != 0)
            throw new IllegalArgumentException("'fields' should be non-null array with even number of elements");

        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(TYPE_FACTORY);

        for (int i = 0; i < fields.length; i += 2)
            b.add((String)fields[i], TYPE_FACTORY.createJavaType((Class<?>)fields[i + 1]));

        return new TestTable(name, b.build(), RewindabilityTrait.REWINDABLE, size) {
            @Override public IgniteDistribution distribution() {
                return distr;
            }
        };
    }

    /**
     * Creates public schema from provided tables.
     *
     * @param tbls Tables to create schema for.
     * @return Public schema.
     */
    protected static IgniteSchema createSchema(TestTable... tbls) {
        IgniteSchema schema = new IgniteSchema("PUBLIC");

        for (TestTable tbl : tbls)
            schema.addTable(tbl.name(), tbl);

        return schema;
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

        List<String> nodes = new ArrayList<>(4);

        for (int i = 0; i < 4; i++)
            nodes.add(UUID.randomUUID().toString());

        PlanningContext ctx = PlanningContext.builder()
            .localNodeId(first(nodes))
            .originatingNodeId(first(nodes))
            .parentContext(Contexts.empty())
            .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(schema)
                .traitDefs(traitDefs)
                .build())
            .build();

        List<RelNode> deserializedNodes = new ArrayList<>();

        try (IgnitePlanner ignored = ctx.planner()) {
            for (String s : serialized) {
                RelJsonReader reader = new RelJsonReader(ctx.cluster(), ctx.catalogReader());
                deserializedNodes.add(reader.read(s));
            }
        }

        List<RelNode> expectedRels = fragments.stream()
            .map(Fragment::root)
            .collect(Collectors.toList());

        assertEquals(expectedRels.size(), deserializedNodes.size(), "Invalid deserialization fragments count");

        for (int i = 0; i < expectedRels.size(); ++i) {
            RelNode expected = expectedRels.get(i);
            RelNode deserialized = deserializedNodes.get(i);

            clearTraits(expected);
            clearTraits(deserialized);

            if (!expected.deepEquals(deserialized))
                assertTrue(
                    expected.deepEquals(deserialized),
                    "Invalid serialization / deserialization.\n" +
                        "Expected:\n" + RelOptUtil.toString(expected) +
                        "Deserialized:\n" + RelOptUtil.toString(deserialized)
                );
        }
    }

    /** */
    protected void clearTraits(RelNode rel) {
        IgniteTestUtils.setFieldValue(rel, AbstractRelNode.class, "traitSet", RelTraitSet.createEmpty());
        rel.getInputs().forEach(this::clearTraits);
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
        private final TableDescriptor desc;

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

            desc = new TestTableDescriptor(this::distribution, type);
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
        @Override public <Row> Iterable<Row> scan(
                ExecutionContext<Row> execCtx,
                ColocationGroup group,
                Predicate<Row> filter,
                Function<Row, Row> rowTransformer,
                @Nullable ImmutableBitSet usedColumns
        ) {
            throw new AssertionError();
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
            return desc;
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
            indexes.put(name, new IgniteIndex(collation, name, this));

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
    static class TestTableDescriptor implements TableDescriptor {
        /** */
        private final Supplier<IgniteDistribution> distributionSupp;

        /** */
        private final RelDataType rowType;

        /** */
        TestTableDescriptor(Supplier<IgniteDistribution> distribution, RelDataType rowType) {
            this.distributionSupp = distribution;
            this.rowType = rowType;
        }

        /** {@inheritDoc} */
        @Override public IgniteDistribution distribution() {
            return distributionSupp.get();
        }

        /** {@inheritDoc} */
        @Override public RelDataType rowType(IgniteTypeFactory factory, ImmutableBitSet usedColumns) {
            return rowType;
        }

        /** {@inheritDoc} */
        @Override public boolean isUpdateAllowed(RelOptTable tbl, int colIdx) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public ColumnDescriptor columnDescriptor(String fieldName) {
            RelDataTypeField field = rowType.getField(fieldName, false, false);
            return new TestColumnDescriptor(field.getIndex(), fieldName);
        }

        /** {@inheritDoc} */
        @Override public boolean isGeneratedAlways(RelOptTable table, int iColumn) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public ColocationGroup colocationGroup(PlanningContext ctx) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public ColumnStrategy generationStrategy(RelOptTable table, int iColumn) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public RexNode newColumnDefaultValue(RelOptTable table, int iColumn, InitializerContext context) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public BiFunction<InitializerContext, RelNode, RelNode> postExpressionConversionHook() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public RexNode newAttributeInitializer(RelDataType type, SqlFunction constructor, int iAttribute,
            List<RexNode> constructorArgs, InitializerContext context) {
            throw new AssertionError();
        }
    }

    /** */
    static class TestColumnDescriptor implements ColumnDescriptor {
        /** */
        private final int idx;

        /** */
        private final String name;

        /** */
        TestColumnDescriptor(int idx, String name) {
            this.idx = idx;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public boolean key() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean hasDefaultValue() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public int fieldIndex() {
            return idx;
        }

        /** {@inheritDoc} */
        @Override public RelDataType logicalType(IgniteTypeFactory f) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public Class<?> storageType() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public Object defaultValue() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public void set(Object dst, Object val) {
            throw new AssertionError();
        }
    }
}
