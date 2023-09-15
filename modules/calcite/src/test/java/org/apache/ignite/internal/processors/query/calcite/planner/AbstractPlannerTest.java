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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.externalize.RelJsonReader;
import org.apache.ignite.internal.processors.query.calcite.message.CalciteMessage;
import org.apache.ignite.internal.processors.query.calcite.message.MessageServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.message.TestIoManager;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseQueryContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.Cloner;
import org.apache.ignite.internal.processors.query.calcite.prepare.Fragment;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.MappingQueryContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerHelper;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.Splitter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.ColumnDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.schema.ModifyTuple;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
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
@SuppressWarnings({"TooBroadScope", "FieldCanBeLocal", "TypeMayBeWeakened"})
public abstract class AbstractPlannerTest extends GridCommonAbstractTest {
    /** */
    protected static final IgniteTypeFactory TYPE_FACTORY = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

    /** */
    protected static final int DEFAULT_TBL_SIZE = 500_000;

    /** */
    protected static final String DEFAULT_SCHEMA = "PUBLIC";

    /** */
    protected List<UUID> nodes;

    /** */
    protected List<QueryTaskExecutorImpl> executors;

    /** */
    protected volatile Throwable lastE;

    /** Last error message. */
    protected String lastErrorMsg;

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
        return cls::isInstance;
    }

    /** */
    public static <T extends RelNode> Predicate<RelNode> byClass(Class<T> cls, Predicate<RelNode> pred) {
        return node -> cls.isInstance(node) && pred.test(node);
    }

    /** */
    protected PlanningContext plannerCtx(String sql, IgniteSchema publicSchema, String... disabledRules) {
        return plannerCtx(sql, Collections.singleton(publicSchema), disabledRules);
    }

    /** */
    protected PlanningContext plannerCtx(String sql, Collection<IgniteSchema> schemas, String... disabledRules) {
        PlanningContext ctx = PlanningContext.builder()
            .parentContext(baseQueryContext(schemas))
            .query(sql)
            .build();

        IgnitePlanner planner = ctx.planner();

        assertNotNull(planner);

        planner.setDisabledRules(ImmutableSet.copyOf(disabledRules));

        return ctx;
    }

    /** */
    protected IgniteRel physicalPlan(String sql, IgniteSchema publicSchema, String... disabledRules) throws Exception {
        return physicalPlan(plannerCtx(sql, publicSchema, disabledRules));
    }

    /** */
    protected IgniteRel physicalPlan(String sql, Collection<IgniteSchema> schemas, String... disabledRules) throws Exception {
        return physicalPlan(plannerCtx(sql, schemas, disabledRules));
    }

    /** */
    protected IgniteRel physicalPlan(PlanningContext ctx) throws Exception {
        try (IgnitePlanner planner = ctx.planner()) {
            assertNotNull(planner);
            assertNotNull(ctx.query());

            return physicalPlan(planner, ctx.query());
        }
    }

    /** */
    protected IgniteRel physicalPlan(IgnitePlanner planner, String qry) throws Exception {
        // Parse
        SqlNode sqlNode = planner.parse(qry);

        // Validate
        sqlNode = planner.validate(sqlNode);

        try {
            return PlannerHelper.optimize(sqlNode, planner, log);
        }
        catch (Throwable ex) {
            System.err.println(planner.dump());

            throw ex;
        }
    }

    /** */
    protected RelNode originalLogicalTree(String sql, IgniteSchema publicSchema, String... disabledRules) throws Exception {
        PlanningContext ctx = plannerCtx(sql, publicSchema);

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
        checkSplitAndSerialization(rel, Collections.singleton(publicSchema));
    }

    /** */
    protected void checkSplitAndSerialization(IgniteRel rel, Collection<IgniteSchema> schemas) {
        assertNotNull(rel);

        rel = Cloner.clone(rel);

        List<Fragment> fragments = new Splitter().go(rel);
        List<String> serialized = new ArrayList<>(fragments.size());

        for (Fragment fragment : fragments)
            serialized.add(toJson(fragment.root()));

        assertNotNull(serialized);

        BaseQueryContext ctx = baseQueryContext(schemas);

        List<RelNode> deserializedNodes = new ArrayList<>();

        for (String s : serialized) {
            RelJsonReader reader = new RelJsonReader(ctx);

            deserializedNodes.add(reader.read(s));
        }

        List<RelNode> expectedRels = fragments.stream()
            .map(Fragment::root)
            .collect(Collectors.toList());

        assertEquals("Invalid deserialization fragments count", expectedRels.size(), deserializedNodes.size());

        for (int i = 0; i < expectedRels.size(); ++i) {
            RelNode expected = expectedRels.get(i);
            RelNode deserialized = deserializedNodes.get(i);

            clearTraits(expected);
            clearTraits(deserialized);

            if (!expected.deepEquals(deserialized)) {
                // RelOptUtil.toString requires metadata, that can be obtained only from the real cluster.
                deserialized = Cloner.clone((IgniteRel)deserialized, expected.getCluster());

                assertTrue(
                    "Invalid serialization / deserialization.\n" +
                        "Expected:\n" + RelOptUtil.toString(expected) +
                        "Deserialized:\n" + RelOptUtil.toString(deserialized),
                    expected.deepEquals(deserialized)
                );
            }
        }
    }

    /** */
    protected void clearTraits(RelNode rel) {
        GridTestUtils.setFieldValue(rel, AbstractRelNode.class, "traitSet", RelTraitSet.createEmpty());
        rel.getInputs().forEach(this::clearTraits);
    }

    /** */
    protected List<UUID> intermediateMapping(@NotNull AffinityTopologyVersion topVer, boolean single,
        @Nullable Predicate<ClusterNode> filter) {
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
    protected static TestTable createTable(IgniteSchema schema, String name, RelDataType type, IgniteDistribution distr,
        List<List<UUID>> assignment) {
        TestTable table = new TestTable(type) {
            @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
                if (F.isEmpty(assignment))
                    return super.colocationGroup(ctx);
                else
                    return ColocationGroup.forAssignments(assignment);
            }

            @Override public IgniteDistribution distribution() {
                return distr;
            }

            @Override public String name() {
                return name;
            }
        };

        schema.addTable(name, table);

        return table;
    }

    /** */
    protected <T extends RelNode> void assertPlan(
        String sql,
        IgniteSchema schema,
        Predicate<T> predicate,
        String... disabledRules
    ) throws Exception {
        assertPlan(sql, Collections.singleton(schema), predicate, disabledRules);
    }

    /** */
    protected <T extends RelNode> void assertPlan(
        String sql,
        Collection<IgniteSchema> schemas,
        Predicate<T> predicate,
        String... disabledRules
    ) throws Exception {
        IgniteRel plan = physicalPlan(sql, schemas, disabledRules);

        checkSplitAndSerialization(plan, schemas);

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
                String scanTableName = Util.last(n.getTable().getQualifiedName());

                if (tableName.equalsIgnoreCase(scanTableName))
                    return true;

                lastErrorMsg = "Unexpected table name [exp=" + tableName + ", act=" + scanTableName + ']';

                return false;
            });
    }

    /**
     * Predicate builder for "Index scan with given name" condition.
     */
    protected <T extends RelNode> Predicate<IgniteIndexScan> isIndexScan(String tableName, String idxName) {
        return isInstanceOf(IgniteIndexScan.class).and(
            n -> {
                String scanTableName = Util.last(n.getTable().getQualifiedName());

                if (!tableName.equalsIgnoreCase(scanTableName)) {
                    lastErrorMsg = "Unexpected table name [exp=" + tableName + ", act=" + scanTableName + ']';

                    return false;
                }

                if (!idxName.equals(n.indexName())) {
                    lastErrorMsg = "Unexpected index name [exp=" + idxName + ", act=" + n.indexName() + ']';

                    return false;
                }

                return true;
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
     * Predicate builder for "Operator has distribution" condition.
     */
    protected <T extends IgniteRel> Predicate<IgniteRel> hasDistribution(IgniteDistribution distribution) {
        return node -> {
            if (distribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED &&
                node.distribution().getType() == RelDistribution.Type.HASH_DISTRIBUTED)
                return distribution.satisfies(node.distribution());
            else
                return distribution.equals(node.distribution());
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
            if (F.size(node.getInputs()) <= idx) {
                lastErrorMsg = "No input for node [idx=" + idx + ", node=" + node + ']';

                return false;
            }

            return predicate.test((T)node.getInput(idx));
        };
    }

    /**
     * Predicate builder for "First input satisfies predicate" condition.
     */
    protected <T extends RelNode> Predicate<RelNode> input(Predicate<T> predicate) {
        return input(0, predicate);
    }

    /**
     * Predicate builder for "Operator has column names" condition.
     */
    protected <T extends RelNode> Predicate<T> hasColumns(String... cols) {
        return node -> {
            RelDataType rowType = node.getRowType();

            String err = "Unexpected columns [expected=" + Arrays.toString(cols) + ", actual=" + rowType.getFieldNames() + ']';

            if (rowType.getFieldCount() != cols.length) {
                lastErrorMsg = err;

                return false;
            }

            for (int i = 0; i < cols.length; i++) {
                if (!cols[i].equals(rowType.getFieldNames().get(i))) {
                    lastErrorMsg = err;

                    return false;
                }
            }

            return true;
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
        if (F.isEmpty(fields) || fields.length % 2 != 0)
            throw new IllegalArgumentException("'fields' should be non-null array with even number of elements");

        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(TYPE_FACTORY);

        for (int i = 0; i < fields.length; i += 2)
            b.add((String)fields[i], TYPE_FACTORY.createJavaType((Class<?>)fields[i + 1]));

        return new TestTable(name, b.build(), size) {
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
    protected BaseQueryContext baseQueryContext(Collection<IgniteSchema> schemas) {
        SchemaPlus rootSchema = createRootSchema(false);
        SchemaPlus dfltSchema = null;

        for (IgniteSchema igniteSchema : schemas) {
            SchemaPlus schema = rootSchema.add(igniteSchema.getName(), igniteSchema);

            if (dfltSchema == null || DEFAULT_SCHEMA.equals(schema.getName()))
                dfltSchema = schema;
        }

        return BaseQueryContext.builder()
            .frameworkConfig(
                newConfigBuilder(FRAMEWORK_CONFIG)
                    .defaultSchema(dfltSchema)
                    .build()
            )
            .logger(log)
            .build();
    }

    /** */
    static class TestTableDescriptor implements CacheTableDescriptor {
        /** */
        private final Supplier<IgniteDistribution> distributionSupp;

        /** */
        private final RelDataType rowType;

        /** */
        public TestTableDescriptor(Supplier<IgniteDistribution> distribution, RelDataType rowType) {
            this.distributionSupp = distribution;
            this.rowType = rowType;
        }

        /** {@inheritDoc} */
        @Override public GridCacheContextInfo cacheInfo() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridCacheContext cacheContext() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public IgniteDistribution distribution() {
            return distributionSupp.get();
        }

        /** {@inheritDoc} */
        @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
            throw new AssertionError();
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
        @Override public boolean match(CacheDataRow row) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public <Row> Row toRow(ExecutionContext<Row> ectx, CacheDataRow row, RowHandler.RowFactory<Row> factory,
            @Nullable ImmutableBitSet requiredColumns) throws IgniteCheckedException {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public <Row> ModifyTuple toTuple(ExecutionContext<Row> ectx, Row row, TableModify.Operation op,
            @Nullable Object arg) throws IgniteCheckedException {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public ColumnDescriptor columnDescriptor(String fieldName) {
            RelDataTypeField field = rowType.getField(fieldName, false, false);
            return new TestColumnDescriptor(field.getIndex(), fieldName);
        }

        /** {@inheritDoc} */
        @Override public Collection<ColumnDescriptor> columnDescriptors() {
            return Commons.transform(rowType.getFieldList(), f -> new TestColumnDescriptor(f.getIndex(), f.getName()));
        }

        /** {@inheritDoc} */
        @Override public GridQueryTypeDescriptor typeDescription() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public boolean isGeneratedAlways(RelOptTable table, int iColumn) {
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
        public TestColumnDescriptor(int idx, String name) {
            this.idx = idx;
            this.name = name;
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
