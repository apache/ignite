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

package org.apache.ignite.internal.processors.query.calcite.rules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerPhase;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;

import static org.apache.calcite.tools.Frameworks.createRootSchema;
import static org.apache.calcite.tools.Frameworks.newConfigBuilder;
import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.FRAMEWORK_CONFIG;

/**
 * Test OR -> UnionAll rewrite rule.
 *
 * Example:
 * SELECT * FROM products
 * WHERE category = 'Photo' OR subcategory ='Camera Media';
 *
 * A query above will be rewritten to next (or equivalient similar query)
 *
 * SELECT * FROM products
 *      WHERE category = 'Photo'
 * UNION ALL
 * SELECT * FROM products
 *      WHERE category != 'Photo' AND subcategory ='Camera Media';
 */
public class OrToUnionRuleTest extends GridCommonAbstractTest {
    /** Node list. */
    private List<UUID> nodes;

    /** Planning context. */
    private PlanningContext ctx;

    /** Setup. */
    @Before
    public void setup() {
        nodes = new ArrayList<>(4);

        for (int i = 0; i < 1; i++)
            nodes.add(UUID.randomUUID());

        createPlanningContext();
    }

    /**
     * Creates schema.
     */
    private SchemaPlus createSchema() {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable products = new TestTable(
            new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("CATEGORY", f.createJavaType(String.class))
                .add("CAT_ID", f.createJavaType(Integer.class))
                .add("SUBCATEGORY", f.createJavaType(String.class))
                .add("SUBCAT_ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .build()) {

            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        products.addIndex(new IgniteIndex(RelCollations.of(1), "IDX_CATEGORY", null, null));
        products.addIndex(new IgniteIndex(RelCollations.of(2), "IDX_CAT_ID", null, null));
        products.addIndex(new IgniteIndex(RelCollations.of(3), "IDX_SUBCATEGORY", null, null));
        products.addIndex(new IgniteIndex(RelCollations.of(4), "IDX_SUBCAT_ID", null, null));

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("PRODUCTS", products);

        return createRootSchema(false)
            .add("PUBLIC", publicSchema);
    }

    /**
     * Check 'OR -> UNION' rule is applied for equality conditions on indexed columns.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEqualityOrToUnionAllRewrite() throws Exception {
        String qry = "SELECT ID " +
            "FROM products " +
            "WHERE category = 'Photo' " +
            "OR subcategory ='Camera Media'";

        final PlanMatcher checker = new PlanMatcher(
            CoreMatchers.allOf(
                PlanMatcher.containsUnionAll(),
                PlanMatcher.containsScan("PUBLIC", "PRODUCTS", "IDX_CATEGORY"),
                PlanMatcher.containsScan("PUBLIC", "PRODUCTS", "IDX_SUBCATEGORY")
            )
        );

        checkPlan(qry, checker);
    }

    /**
     * Check 'OR -> UNION' rule is applied for equality conditions on indexed columns.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNonDistinctOrToUnionAllRewrite() throws Exception {
        String qry = "SELECT ID " +
            "FROM products " +
            "WHERE subcategory = 'Camera Lens' " +
            "OR subcategory ='Camera Media'";

        final PlanMatcher checker = new PlanMatcher(
            CoreMatchers.allOf(
                PlanMatcher.containsUnionAll(),
                PlanMatcher.containsScan("PUBLIC", "PRODUCTS", "IDX_SUBCATEGORY"),
                PlanMatcher.containsScan("PUBLIC", "PRODUCTS", "IDX_SUBCATEGORY")
            )
        );

        checkPlan(qry, checker);
    }

    /**
     * Check 'OR -> UNION' rule is applied for mixed conditions on indexed columns.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMixedOrToUnionAllRewrite() throws Exception {
        String qry = "SELECT ID " +
            "FROM products " +
            "WHERE category = 'Photo' " +
            "OR (subcat_id > 10 AND subcat_id < 15)";

        final PlanMatcher checker = new PlanMatcher(
            CoreMatchers.allOf(
                PlanMatcher.containsUnionAll(),
                PlanMatcher.containsScan("PUBLIC", "PRODUCTS", "IDX_CATEGORY"),
                PlanMatcher.containsScan("PUBLIC", "PRODUCTS", "IDX_SUBCAT_ID")
            )
        );

        checkPlan(qry, checker);
    }

    /**
     * Check 'OR -> UNION' rule is not applied for range conditions on indexed columns.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRangeOrToUnionAllRewrite() throws Exception {
        String qry = "SELECT ID " +
            "FROM products " +
            "WHERE cat_id > 1 " +
            "OR subcategory < 10";

        final PlanMatcher checker = new PlanMatcher(
            CoreMatchers.allOf(
                CoreMatchers.not(PlanMatcher.containsUnionAll()),
                PlanMatcher.containsScan("PUBLIC", "PRODUCTS", "PK")
            )
        );

        checkPlan(qry, checker);
    }

    /**
     * Check 'OR -> UNION' rule is not applied if (at least) one of column is not indexed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNonIndexedOrToUnionAllRewrite() throws Exception {
        String qry = "SELECT ID " +
            "FROM products " +
            "WHERE name = 'Tesla Model S' " +
            "OR category = 'Photo'";

        final PlanMatcher checker = new PlanMatcher(
            CoreMatchers.allOf(
                CoreMatchers.not(PlanMatcher.containsUnionAll()),
                PlanMatcher.containsScan("PUBLIC", "PRODUCTS", "PK")
            )
        );

        checkPlan(qry, checker);
    }

    /**
     * Check query plan.
     *
     * @param qry     Query.
     * @param checker Plan checker.
     * @throws Exception if failed.
     */
    private void checkPlan(String qry, PlanMatcher checker) throws Exception {
        try (IgnitePlanner planner = ctx.planner()) {
            assertNotNull(planner);
            assertNotNull(qry);

            // Parse
            SqlNode sqlNode = planner.parse(qry);
            // Validate
            sqlNode = planner.validate(sqlNode);
            // Convert to Relational operators graph
            RelRoot relRoot = planner.rel(sqlNode);

            RelNode rel = relRoot.rel;

            assertNotNull(rel);
            log.info("Logical plan: " + RelOptUtil.toString(rel));

            // Transformation chain
            RelTraitSet desired = rel.getCluster().traitSet()
                .replace(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single())
                .simplify();

            RelNode phys = planner.transform(PlannerPhase.OPTIMIZATION, desired, rel);

            final String actualPlan = RelOptUtil.toString(phys);
            log.info("Execution plan: " + actualPlan);

            checker.check(actualPlan);
        }
    }

    /**
     * Initialize planning context.
     */
    private void createPlanningContext() {
        SchemaPlus schema = createSchema();

        RelTraitDef<?>[] traitDefs = {
            DistributionTraitDef.INSTANCE,
            ConventionTraitDef.INSTANCE,
            RelCollationTraitDef.INSTANCE

        };

        ctx = PlanningContext.builder()
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
    }

    /**
     *
     */
    private abstract static class TestTable implements IgniteTable {
        /** */
        private final RelProtoDataType protoType;

        /** Table indices. */
        private final Map<String, IgniteIndex> indexes = new LinkedHashMap<>();

        /** */
        private TestTable(RelDataType type) {
            protoType = RelDataTypeImpl.proto(type);

            addIndex(new IgniteIndex(RelCollations.of(0), "PK", null, this));
        }

        /** {@inheritDoc} */
        @Override public RelNode toRel(RelOptTable.ToRelContext ctx, RelOptTable relOptTbl) {
            RelOptCluster cluster = ctx.getCluster();

            RelTraitSet traitSet = cluster.traitSetOf(IgniteConvention.INSTANCE)
                .replaceIf(RelCollationTraitDef.INSTANCE, () -> getIndex("PK").collation())
                .replaceIf(DistributionTraitDef.INSTANCE, this::distribution);

            return new IgniteTableScan(cluster, traitSet, relOptTbl, "PK", null);
        }

        /** {@inheritDoc} */
        @Override public IgniteTableScan toRel(RelOptCluster cluster, RelOptTable relOptTbl, String idxName) {
            if (getIndex(idxName) == null)
                return null;

            RelTraitSet traitSet = cluster.traitSetOf(IgniteConvention.INSTANCE)
                .replaceIf(RelCollationTraitDef.INSTANCE, () -> getIndex(idxName).collation())
                .replaceIf(DistributionTraitDef.INSTANCE, this::distribution);

            return new IgniteTableScan(cluster, traitSet, relOptTbl, idxName, null);
        }

        /** {@inheritDoc} */
        @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return protoType.apply(typeFactory);
        }

        /** {@inheritDoc} */
        @Override public Statistic getStatistic() {
            return new Statistic() {
                /** {@inheritDoc */
                @Override public Double getRowCount() {
                    return 100.0;
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
        @Override public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
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
        @Override public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent,
            CalciteConnectionConfig config) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public NodesMapping mapping(PlanningContext ctx) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public IgniteDistribution distribution() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public List<RelCollation> collations() {
            return indexes.values().stream().map(IgniteIndex::collation).collect(Collectors.toList());
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

        /** {@inheritDoc} */
        @Override public IgniteIndex getIndex(String idxName) {
            return indexes.get(idxName);
        }

        /** {@inheritDoc} */
        @Override public void removeIndex(String idxName) {
            throw new AssertionError();
        }
    }
}
