package org.apache.ignite.internal.processors.query.calcite.planner;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.plan.volcano.VolcanoTimeoutException;
import org.apache.calcite.rel.RelCollation;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheIndexImpl;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** */
public class PlannerDumpTest extends AbstractPlannerTest {
    /** */
    private static final int PLANNER_TIMEOUT = 1_000;

    /** */
    @Test
    public void testPlanningErrorDumpIsTruncated() throws Exception {
        IgniteSchema schema = createSchema(createLargeTestTable("BIG_TABLE", 50));

        StringBuilder sql = new StringBuilder("SELECT * FROM BIG_TABLE t1\n");

        for (int i = 2; i <= 10; i++)
            sql.append("JOIN BIG_TABLE t").append(i).append(" ON t1.col0 = t").append(i).append(".col0\n");

        PlanningContext ctx = PlanningContext.builder()
                .parentContext(baseQueryContext(Collections.singletonList(schema)))
                .plannerTimeout(1_000)
                .query(sql.toString())
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

        assertNull(plan.get());

        assertNotNull(plannerError.get());
    }

    /** */
    private static TestTable createLargeTestTable(String name, int colCnt) {
        Object[] cols = new Object[2 * colCnt];

        for (int i = 0; i < colCnt; i++) {
            cols[2 * i] = "COL" + i;
            cols[2 * i + 1] = Integer.class;
        }

        TestTable table = createTable(name, IgniteDistributions.single(), cols);

        RelCollation pkColl = TraitUtils.createCollation(Collections.singletonList(0));
        table.addIndex(new CacheIndexImpl(pkColl, "pk", null, table) {
            @Override public RelCollation collation() {
                doSleep(PLANNER_TIMEOUT);

                return super.collation();
            }
        });

        return table;
    }
}
