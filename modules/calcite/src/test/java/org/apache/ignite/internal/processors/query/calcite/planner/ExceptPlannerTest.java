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

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteMapMinus;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteReduceMinus;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteSingleMinus;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Before;
import org.junit.Test;

/**
 * Test to verify EXCEPT operator.
 */
public class ExceptPlannerTest extends AbstractPlannerTest {
    /** Public schema. */
    private IgniteSchema publicSchema;

    /** Last error. */
    private String lastError;

    /** {@inheritDoc} */
    @Before
    @Override public void setup() {
        super.setup();

        publicSchema = new IgniteSchema("PUBLIC");

        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        RelDataType type = new RelDataTypeFactory.Builder(f)
            .add("ID", f.createJavaType(Integer.class))
            .add("NAME", f.createJavaType(String.class))
            .add("SALARY", f.createJavaType(Double.class))
            .build();

        createTable("RANDOM_TBL1", type, IgniteDistributions.random(), null);
        createTable("RANDOM_TBL2", type, IgniteDistributions.random(), null);
        createTable("BROADCAST_TBL1", type, IgniteDistributions.broadcast(), null);
        createTable("BROADCAST_TBL2", type, IgniteDistributions.broadcast(), null);
        createTable("SINGLE_TBL1", type, IgniteDistributions.single(), null);
        createTable("SINGLE_TBL2", type, IgniteDistributions.single(), null);

        List<List<UUID>> assignment = Arrays.asList(
            select(nodes, 0, 1),
            select(nodes, 1, 2),
            select(nodes, 2, 0),
            select(nodes, 0, 1),
            select(nodes, 1, 2)
        );

        createTable("AFFINITY_TBL1", type, IgniteDistributions.affinity(0, "Test1", "hash"),
            assignment);

        createTable("AFFINITY_TBL2", type, IgniteDistributions.affinity(0, "Test2", "hash"),
            assignment);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptRandom() throws Exception {
        String sql = "" +
            "SELECT * FROM random_tbl1 " +
            "EXCEPT " +
            "SELECT * FROM random_tbl2 ";

        assertPlan(sql, isInstanceOf(IgniteReduceMinus.class).and(n -> !n.all)
            .and(hasChildThat(isInstanceOf(IgniteMapMinus.class)
                .and(input(0, isTableScan("random_tbl1")))
                .and(input(1, isTableScan("random_tbl2")))
            ))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptAllRandom() throws Exception {
        String sql = "" +
            "SELECT * FROM random_tbl1 " +
            "EXCEPT ALL " +
            "SELECT * FROM random_tbl2 ";

        assertPlan(sql, isInstanceOf(IgniteReduceMinus.class).and(n -> n.all)
            .and(hasChildThat(isInstanceOf(IgniteMapMinus.class)
                .and(input(0, isTableScan("random_tbl1")))
                .and(input(1, isTableScan("random_tbl2")))
            ))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptBroadcast() throws Exception {
        String sql = "" +
            "SELECT * FROM broadcast_tbl1 " +
            "EXCEPT " +
            "SELECT * FROM broadcast_tbl2 ";

        assertPlan(sql, isInstanceOf(IgniteSingleMinus.class)
            .and(input(0, isTableScan("broadcast_tbl1")))
            .and(input(1, isTableScan("broadcast_tbl2")))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptSingle() throws Exception {
        String sql = "" +
            "SELECT * FROM single_tbl1 " +
            "EXCEPT " +
            "SELECT * FROM single_tbl2 ";

        assertPlan(sql, isInstanceOf(IgniteSingleMinus.class)
            .and(input(0, isTableScan("single_tbl1")))
            .and(input(1, isTableScan("single_tbl2"))));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptSingleAndRandom() throws Exception {
        String sql = "" +
            "SELECT * FROM single_tbl1 " +
            "EXCEPT " +
            "SELECT * FROM random_tbl1 ";

        assertPlan(sql, isInstanceOf(IgniteSingleMinus.class)
            .and(input(0, isTableScan("single_tbl1")))
            .and(input(1, hasChildThat(isTableScan("random_tbl1")))));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptSingleAndAffinity() throws Exception {
        String sql = "" +
            "SELECT * FROM single_tbl1 " +
            "EXCEPT " +
            "SELECT * FROM affinity_tbl1 ";

        assertPlan(sql, isInstanceOf(IgniteSingleMinus.class)
            .and(input(0, isTableScan("single_tbl1")))
            .and(input(1, hasChildThat(isTableScan("affinity_tbl1")))));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptSingleAndBroadcast() throws Exception {
        String sql = "" +
            "SELECT * FROM single_tbl1 " +
            "EXCEPT " +
            "SELECT * FROM broadcast_tbl1 ";

        assertPlan(sql, isInstanceOf(IgniteSingleMinus.class)
            .and(input(0, isTableScan("single_tbl1")))
            .and(input(1, isTableScan("broadcast_tbl1")))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptAffinity() throws Exception {
        String sql = "" +
            "SELECT * FROM affinity_tbl1 " +
            "EXCEPT " +
            "SELECT * FROM affinity_tbl2 ";

        assertPlan(sql, isInstanceOf(IgniteReduceMinus.class)
            .and(hasChildThat(isInstanceOf(IgniteMapMinus.class)
                .and(input(0, isTableScan("affinity_tbl1")))
                .and(input(1, isTableScan("affinity_tbl2")))
            ))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptBroadcastAndRandom() throws Exception {
        String sql = "" +
            "SELECT * FROM random_tbl1 " +
            "EXCEPT " +
            "SELECT * FROM broadcast_tbl1 ";

        assertPlan(sql, isInstanceOf(IgniteSingleMinus.class)
            .and(input(0, hasChildThat(isTableScan("random_tbl1"))))
            .and(input(1, isTableScan("broadcast_tbl1")))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptRandomNested() throws Exception {
        String sql = "" +
            "SELECT * FROM random_tbl2 EXCEPT (" +
            "   SELECT * FROM random_tbl1 " +
            "   EXCEPT " +
            "   SELECT * FROM random_tbl2" +
            ")";

        assertPlan(sql, isInstanceOf(IgniteSingleMinus.class)
            .and(input(0, hasChildThat(isTableScan("random_tbl2"))))
            .and(input(1, isInstanceOf(IgniteReduceMinus.class)
                .and(hasChildThat(isInstanceOf(IgniteMapMinus.class)
                    .and(input(0, isTableScan("random_tbl1")))
                    .and(input(1, isTableScan("random_tbl2")))
                ))
            ))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptBroadcastAndRandomNested() throws Exception {
        String sql = "" +
            "SELECT * FROM broadcast_tbl1 EXCEPT (" +
            "   SELECT * FROM random_tbl1 " +
            "   EXCEPT " +
            "   SELECT * FROM random_tbl2" +
            ")";

        assertPlan(sql, isInstanceOf(IgniteSingleMinus.class)
            .and(input(0, isTableScan("broadcast_tbl1")))
            .and(input(1, isInstanceOf(IgniteReduceMinus.class)
                .and(hasChildThat(isInstanceOf(IgniteMapMinus.class)
                    .and(input(0, isTableScan("random_tbl1")))
                    .and(input(1, isTableScan("random_tbl2")))
                ))
            ))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptMerge() throws Exception {
        String sql = "" +
            "SELECT * FROM random_tbl1 " +
            "EXCEPT " +
            "SELECT * FROM random_tbl2 " +
            "EXCEPT " +
            "SELECT * FROM affinity_tbl1 " +
            "EXCEPT " +
            "SELECT * FROM affinity_tbl2 ";

        assertPlan(sql, isInstanceOf(IgniteReduceMinus.class)
            .and(hasChildThat(isInstanceOf(IgniteMapMinus.class)
                .and(input(0, isTableScan("random_tbl1")))
                .and(input(1, isTableScan("random_tbl2")))
                .and(input(2, isTableScan("affinity_tbl1")))
                .and(input(3, isTableScan("affinity_tbl2")))
            ))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptAllMerge() throws Exception {
        String sql = "" +
            "SELECT * FROM random_tbl1 " +
            "EXCEPT ALL " +
            "SELECT * FROM random_tbl2 " +
            "EXCEPT ALL " +
            "SELECT * FROM affinity_tbl1 " +
            "EXCEPT ALL " +
            "SELECT * FROM affinity_tbl2 ";

        assertPlan(sql, isInstanceOf(IgniteReduceMinus.class).and(n -> n.all)
            .and(hasChildThat(isInstanceOf(IgniteMapMinus.class)
                .and(input(0, isTableScan("random_tbl1")))
                .and(input(1, isTableScan("random_tbl2")))
                .and(input(2, isTableScan("affinity_tbl1")))
                .and(input(3, isTableScan("affinity_tbl2")))
            ))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptAllWithExceptMerge() throws Exception {
        String sql = "" +
            "SELECT * FROM random_tbl1 " +
            "EXCEPT ALL " +
            "SELECT * FROM random_tbl2 " +
            "EXCEPT " +
            "SELECT * FROM affinity_tbl1 ";

        assertPlan(sql, isInstanceOf(IgniteReduceMinus.class).and(n -> !n.all)
            .and(hasChildThat(isInstanceOf(IgniteMapMinus.class)
                .and(input(0, isTableScan("random_tbl1")))
                .and(input(1, isTableScan("random_tbl2")))
                .and(input(2, isTableScan("affinity_tbl1")))
            ))
        );
    }

    /** */
    private <T extends RelNode> void assertPlan(String sql, Predicate<T> predicate) throws Exception {
        IgniteRel plan = physicalPlan(sql, publicSchema);

        if (!predicate.test((T)plan)) {
            String invalidPlanMsg = "Invalid plan (" + lastError + "):\n" +
                RelOptUtil.toString(plan, SqlExplainLevel.ALL_ATTRIBUTES);

            fail(invalidPlanMsg);
        }
    }

    /** */
    private <T extends RelNode> Predicate<T> isInstanceOf(Class<T> cls) {
        return node -> {
            if (cls.isInstance(node))
                return true;

            lastError = "Unexpected node class [node=" + node + ", cls=" + cls.getSimpleName() + ']';

            return false;
        };
    }

    /** */
    private <T extends RelNode> Predicate<IgniteTableScan> isTableScan(String tableName) {
        return isInstanceOf(IgniteTableScan.class).and(
            n -> {
                String scanTableName = n.getTable().unwrap(TestTable.class).name();

                if (tableName.equalsIgnoreCase(scanTableName))
                    return true;

                lastError = "Unexpected table name [exp=" + tableName + ", act=" + scanTableName + ']';

                return false;
            });
    }

    /** */
    private <T extends RelNode> Predicate<RelNode> hasChildThat(Predicate<T> predicate) {
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

                lastError = "Not found child for defined condition [node=" + node + ']';

                return false;
            }
        };
    }

    /** */
    private <T extends RelNode> Predicate<RelNode> input(Predicate<T> predicate) {
        return node -> {
            if (F.isEmpty(node.getInputs())) {
                lastError = "No inputs for node [node=" + node + ']';

                return false;
            }

            return predicate.test((T)node.getInput(0));
        };
    }

    /** */
    private <T extends RelNode> Predicate<RelNode> input(int idx, Predicate<T> predicate) {
        return node -> {
            if (F.size(node.getInputs()) <= idx) {
                lastError = "No input for node [idx=" + idx + ", node=" + node + ']';

                return false;
            }

            return predicate.test((T)node.getInput(idx));
        };
    }

    /** */
    private void createTable(String name, RelDataType type, IgniteDistribution distr, List<List<UUID>> assignment) {
        TestTable table = new TestTable(type) {
            @Override public ColocationGroup colocationGroup(PlanningContext ctx) {
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

        publicSchema.addTable(name, table);
    }
}
