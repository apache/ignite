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

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.plan.volcano.VolcanoTimeoutException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseQueryContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheIndexImpl;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.testframework.GridTestUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;

import static org.apache.calcite.tools.Frameworks.createRootSchema;
import static org.apache.calcite.tools.Frameworks.newConfigBuilder;
import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.FRAMEWORK_CONFIG;

/**
 * Test planner timeout.
 */
public class PlannerTimeoutTest extends AbstractPlannerTest {
    /** */
    private static final long PLANNER_TIMEOUT = 1_000;

    /** */
    private static final IgniteTypeFactory typeFactory = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

    /** */
    @Test
    public void testLongPlanningTimeout() throws Exception {
        SchemaPlus schema = createSchema("PUBLIC",
            Pair.of("T1", createTestTable(Pair.of("A", Integer.class), Pair.of("B", Integer.class))),
            Pair.of("T2", createTestTable(Pair.of("A", Integer.class), Pair.of("C", Integer.class)))
        );

        String sql = "SELECT * FROM T1 JOIN T2 ON T1.A = T2.A";

        PlanningContext ctx = PlanningContext.builder()
            .parentContext(BaseQueryContext.builder()
                .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                    .defaultSchema(schema)
                    .costFactory(new IgniteCostFactory(1, 100, 1, 1))
                    .build())
                .logger(log)
                .build()
            )
            .plannerTimeout(PLANNER_TIMEOUT)
            .query(sql)
            .build();

        AtomicReference<IgniteRel> plan = new AtomicReference<>();
        AtomicReference<RelOptPlanner.CannotPlanException> plannerError = new AtomicReference<>();

        GridTestUtils.assertTimeout(3 * PLANNER_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
            try (IgnitePlanner planner = ctx.planner()) {
                plan.set(physicalPlan(planner, ctx.query()));

                VolcanoPlanner volcanoPlanner = GridTestUtils.getFieldValue(planner, "planner");

                assertNotNull(volcanoPlanner);

                GridTestUtils.assertThrowsWithCause(volcanoPlanner::checkCancel, VolcanoTimeoutException.class);
            }
            catch (RelOptPlanner.CannotPlanException e) {
                plannerError.set(e);
            }
            catch (Exception e) {
                throw new RuntimeException("Planning failed", e);
            }
        });

        assertTrue(plan.get() != null || plannerError.get() != null);

        if (plan.get() != null) {
            new RelVisitor() {
                @Override public void visit(
                    RelNode node,
                    int ordinal,
                    @Nullable RelNode parent) {
                    assertNotNull(node.getTraitSet().getTrait(IgniteConvention.INSTANCE.getTraitDef()));
                    super.visit(node, ordinal, parent);
                }
            }.go(plan.get());
        }
    }

    /** */
    private static SchemaPlus createSchema(String name, Pair<String, TestTable>... tables) {
        IgniteSchema schema = new IgniteSchema(name);

        for (Pair<String, TestTable> tbl: tables)
            schema.addTable(tbl.left, tbl.right);

        return createRootSchema(false).add(name, schema);
    }

    /** */
    private static TestTable createTestTable(Pair<String, Class<?>>... cols) {
        RelDataTypeFactory.Builder relDataTypeFactoryBuilder = new RelDataTypeFactory.Builder(typeFactory);

        for (Pair<String, Class<?>> col: cols) {
            relDataTypeFactoryBuilder.add(col.left, typeFactory.createType(col.right));
        }

        TestTable table = new TestTable(relDataTypeFactoryBuilder.build()) {
            @Override public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
            }
        };

        RelCollation pkColl = TraitUtils.createCollation(Collections.singletonList(0));
        table.addIndex(new CacheIndexImpl(pkColl, "pk", null, table) {
            @Override public RelCollation collation() {
                Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);

                return super.collation();
            }
        });

        return table;
    }
}
