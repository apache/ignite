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
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.plan.volcano.VolcanoTimeoutException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheIndexImpl;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Test planner timeout.
 */
public class PlannerTimeoutTest extends AbstractPlannerTest {
    /** */
    private static final long PLANNER_TIMEOUT = 1_000;

    /** */
    @Test
    public void testLongPlanningTimeout() throws Exception {
        IgniteSchema schema = createSchema(
            createTestTable("T1", "A", Integer.class, "B", Integer.class),
            createTestTable("T2", "A", Integer.class, "B", Integer.class)
        );

        String sql = "SELECT * FROM T1 JOIN T2 ON T1.A = T2.A";

        PlanningContext ctx = PlanningContext.builder()
            .parentContext(baseQueryContext(Collections.singletonList(schema)))
            .plannerTimeout(PLANNER_TIMEOUT)
            .query(sql)
            .build();

        AtomicReference<IgniteRel> plan = new AtomicReference<>();
        AtomicReference<RelOptPlanner.CannotPlanException> plannerError = new AtomicReference<>();

        GridTestUtils.assertTimeout(3 * PLANNER_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
            try (IgnitePlanner planner = ctx.planner()) {
                plan.set(physicalPlan(planner, ctx.query()));

                VolcanoPlanner volcanoPlanner = (VolcanoPlanner)ctx.cluster().getPlanner();

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
                    RelNode parent
                ) {
                    assertNotNull(node.getTraitSet().getTrait(IgniteConvention.INSTANCE.getTraitDef()));
                    super.visit(node, ordinal, parent);
                }
            }.go(plan.get());
        }
    }

    /** */
    private static TestTable createTestTable(String name, Object... cols) {
        TestTable table = createTable(name, IgniteDistributions.broadcast(), cols);

        RelCollation pkColl = TraitUtils.createCollation(Collections.singletonList(0));
        table.addIndex(new CacheIndexImpl(pkColl, "pk", null, table) {
            @Override public RelCollation collation() {
                doSleep(300);

                return super.collation();
            }
        });

        return table;
    }
}
