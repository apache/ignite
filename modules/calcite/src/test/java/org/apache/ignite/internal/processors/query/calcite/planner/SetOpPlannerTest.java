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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTrimExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteColocatedIntersect;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteColocatedMinus;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteColocatedSetOp;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteMapIntersect;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteMapMinus;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteMapSetOp;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteReduceIntersect;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteReduceMinus;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteReduceSetOp;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteSetOp;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test to verify set op (EXCEPT, INTERSECT).
 */
@RunWith(Parameterized.class)
public class SetOpPlannerTest extends AbstractPlannerTest {
    /** Algorithm. */
    @Parameterized.Parameter
    public SetOp setOp;

    /** */
    @Parameterized.Parameters(name = "SetOp = {0}")
    public static List<Object[]> parameters() {
        return Stream.of(SetOp.values()).map(a -> new Object[]{a}).collect(Collectors.toList());
    }

    /** Public schema. */
    private IgniteSchema publicSchema;

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

        createTable(publicSchema, "RANDOM_TBL1", type, IgniteDistributions.random(), null);
        createTable(publicSchema, "RANDOM_TBL2", type, IgniteDistributions.random(), null);
        createTable(publicSchema, "BROADCAST_TBL1", type, IgniteDistributions.broadcast(), null);
        createTable(publicSchema, "BROADCAST_TBL2", type, IgniteDistributions.broadcast(), null);
        createTable(publicSchema, "SINGLE_TBL1", type, IgniteDistributions.single(), null);
        createTable(publicSchema, "SINGLE_TBL2", type, IgniteDistributions.single(), null);

        List<List<UUID>> assignment = Arrays.asList(
            select(nodes, 0, 1),
            select(nodes, 1, 2),
            select(nodes, 2, 0),
            select(nodes, 0, 1),
            select(nodes, 1, 2)
        );

        createTable(publicSchema, "AFFINITY_TBL1", type,
            IgniteDistributions.affinity(0, "Test1", "hash"), assignment);

        createTable(publicSchema, "AFFINITY_TBL2", type,
            IgniteDistributions.affinity(0, "Test2", "hash"), assignment);

        createTable(publicSchema, "AFFINITY_TBL3", type,
            IgniteDistributions.affinity(1, "Test3", "hash"), assignment);

        createTable(publicSchema, "AFFINITY_TBL4", type,
            IgniteDistributions.affinity(0, "Test4", "hash2"), assignment);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetOpRandom() throws Exception {
        String sql =
            "SELECT * FROM random_tbl1 " +
            setOp() +
            "SELECT * FROM random_tbl2 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.reduce).and(n -> !n.all())
            .and(hasChildThat(isInstanceOf(setOp.map)
                .and(input(0, isTableScan("random_tbl1")))
                .and(input(1, isTableScan("random_tbl2")))
            ))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetOpAllRandom() throws Exception {
        String sql =
            "SELECT * FROM random_tbl1 " +
            setOpAll() +
            "SELECT * FROM random_tbl2 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.reduce).and(IgniteSetOp::all)
            .and(hasChildThat(isInstanceOf(setOp.map)
                .and(input(0, isTableScan("random_tbl1")))
                .and(input(1, isTableScan("random_tbl2")))
            ))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetOpBroadcast() throws Exception {
        String sql =
            "SELECT * FROM broadcast_tbl1 " +
            setOp() +
            "SELECT * FROM broadcast_tbl2 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.colocated)
            .and(input(0, isTableScan("broadcast_tbl1")))
            .and(input(1, isTableScan("broadcast_tbl2")))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetOpSingle() throws Exception {
        String sql =
            "SELECT * FROM single_tbl1 " +
            setOp() +
            "SELECT * FROM single_tbl2 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.colocated)
            .and(input(0, isTableScan("single_tbl1")))
            .and(input(1, isTableScan("single_tbl2"))));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetOpSingleAndRandom() throws Exception {
        String sql =
            "SELECT * FROM single_tbl1 " +
            setOp() +
            "SELECT * FROM random_tbl1 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.colocated)
            .and(hasDistribution(IgniteDistributions.single()))
            .and(input(0, isTableScan("single_tbl1")))
            .and(input(1, hasChildThat(isTableScan("random_tbl1")))));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetOpSingleAndAffinity() throws Exception {
        String sql =
            "SELECT * FROM single_tbl1 " +
            setOp() +
            "SELECT * FROM affinity_tbl1 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.colocated)
            .and(hasDistribution(IgniteDistributions.single()))
            .and(input(0, isTableScan("single_tbl1")))
            .and(input(1, hasChildThat(isTableScan("affinity_tbl1")))));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetOpSingleAndBroadcast() throws Exception {
        String sql =
            "SELECT * FROM single_tbl1 " +
            setOp() +
            "SELECT * FROM broadcast_tbl1 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.colocated)
            .and(input(0, isTableScan("single_tbl1")))
            .and(input(1, isTableScan("broadcast_tbl1")))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetOpAffinity() throws Exception {
        String sql =
            "SELECT * FROM affinity_tbl1 " +
            setOp() +
            "SELECT * FROM affinity_tbl2 ";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteExchange.class)
            .and(input(isInstanceOf(setOp.colocated)
                .and(hasDistribution(IgniteDistributions.affinity(0, null, "hash")))
                .and(input(0, isTableScan("affinity_tbl1")))
                .and(input(1, isTableScan("affinity_tbl2")))
            ))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetOpAffinityAndBroadcast() throws Exception {
        String sql =
            "SELECT * FROM affinity_tbl1 " +
                setOp() +
                "SELECT * FROM broadcast_tbl1 ";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteExchange.class)
            .and(input(isInstanceOf(setOp.colocated)
                .and(hasDistribution(IgniteDistributions.affinity(0, null, "hash")))
                .and(input(0, isTableScan("affinity_tbl1")))
                .and(input(1, isInstanceOf(IgniteTrimExchange.class)
                    .and(input(isTableScan("broadcast_tbl1")))
                ))
            ))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetOpNonColocatedAffinity() throws Exception {
        String sql =
            "SELECT * FROM affinity_tbl1 " +
                setOp() +
                "SELECT * FROM affinity_tbl3 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.reduce)
            .and(hasChildThat(isInstanceOf(setOp.map)
                .and(input(0, isTableScan("affinity_tbl1")))
                .and(input(1, isTableScan("affinity_tbl3")))
            ))
        );

        sql =
            "SELECT * FROM affinity_tbl1 " +
                setOp() +
                "SELECT * FROM affinity_tbl4 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.reduce)
            .and(hasChildThat(isInstanceOf(setOp.map)
                .and(input(0, isTableScan("affinity_tbl1")))
                .and(input(1, isTableScan("affinity_tbl4")))
            ))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetOpAffinityNested() throws Exception {
        String sql =
            "SELECT * FROM affinity_tbl2 " + setOp() + "(" +
                "   SELECT * FROM affinity_tbl1 " +
                setOp() +
                "   SELECT * FROM affinity_tbl2" +
                ")";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteExchange.class)
                .and(input(isInstanceOf(setOp.colocated)
                    .and(hasChildThat(isTableScan("affinity_tbl2")))
                    .and(hasChildThat(isInstanceOf(setOp.colocated)
                        .and(input(0, isTableScan("affinity_tbl1")))
                        .and(input(1, isTableScan("affinity_tbl2")))
                    ))
                )),
            "MinusMergeRule", "IntersectMergeRule"
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetOpBroadcastAndRandom() throws Exception {
        String sql =
            "SELECT * FROM random_tbl1 " +
            setOp() +
            "SELECT * FROM broadcast_tbl1 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.colocated)
            .and(input(0, hasChildThat(isTableScan("random_tbl1"))))
            .and(input(1, isTableScan("broadcast_tbl1")))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetOpRandomNested() throws Exception {
        String sql =
            "SELECT * FROM random_tbl2 " + setOp() + "(" +
            "   SELECT * FROM random_tbl1 " +
            setOp() +
            "   SELECT * FROM random_tbl2" +
            ")";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.colocated)
            .and(input(0, hasChildThat(isTableScan("random_tbl2"))))
            .and(input(1, isInstanceOf(setOp.reduce)
                .and(hasChildThat(isInstanceOf(setOp.map)
                    .and(input(0, isTableScan("random_tbl1")))
                    .and(input(1, isTableScan("random_tbl2")))
                ))
            )),
            "MinusMergeRule", "IntersectMergeRule", "IntersectReorderRule"
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetOpBroadcastAndRandomNested() throws Exception {
        String sql =
            "SELECT * FROM broadcast_tbl1 " + setOp() + "(" +
            "   SELECT * FROM random_tbl1 " +
            setOp() +
            "   SELECT * FROM random_tbl2" +
            ")";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.colocated)
            .and(hasChildThat(isTableScan("broadcast_tbl1")))
            .and(hasChildThat(isInstanceOf(setOp.reduce)
                .and(hasChildThat(isInstanceOf(setOp.map)
                    .and(input(0, isTableScan("random_tbl1")))
                    .and(input(1, isTableScan("random_tbl2")))
                ))
            )),
            "MinusMergeRule", "IntersectMergeRule"
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetOpMerge() throws Exception {
        String sql =
            "SELECT * FROM random_tbl1 " +
            setOp() +
            "SELECT * FROM random_tbl2 " +
            setOp() +
            "SELECT * FROM affinity_tbl1 " +
            setOp() +
            "SELECT * FROM affinity_tbl2 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.reduce)
            .and(hasChildThat(isInstanceOf(setOp.map)
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
    public void testSetOpAllMerge() throws Exception {
        String sql =
            "SELECT * FROM random_tbl1 " +
            setOpAll() +
            "SELECT * FROM random_tbl2 " +
            setOpAll() +
            "SELECT * FROM affinity_tbl1 " +
            setOpAll() +
            "SELECT * FROM affinity_tbl2 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.reduce).and(IgniteSetOp::all)
            .and(hasChildThat(isInstanceOf(setOp.map)
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
    public void testSetOpAllWithExceptMerge() throws Exception {
        String sql =
            "SELECT * FROM random_tbl1 " +
            setOpAll() +
            "SELECT * FROM random_tbl2 " +
            setOp() +
            "SELECT * FROM affinity_tbl1 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.reduce).and(n -> !n.all())
            .and(hasChildThat(isInstanceOf(setOp.map)
                .and(input(0, isTableScan("random_tbl1")))
                .and(input(1, isTableScan("random_tbl2")))
                .and(input(2, isTableScan("affinity_tbl1")))
            ))
        );
    }

    /** */
    private String setOp() {
        return setOp.name() + ' ';
    }

    /** */
    private String setOpAll() {
        return setOp.name() + " ALL ";
    }

    /** */
    enum SetOp {
        /** */
        EXCEPT(
            IgniteColocatedMinus.class,
            IgniteMapMinus.class,
            IgniteReduceMinus.class
        ),

        /** */
        INTERSECT(
            IgniteColocatedIntersect.class,
            IgniteMapIntersect.class,
            IgniteReduceIntersect.class
        );

        /** */
        public final Class<? extends IgniteColocatedSetOp> colocated;

        /** */
        public final Class<? extends IgniteMapSetOp> map;

        /** */
        public final Class<? extends IgniteReduceSetOp> reduce;

        /** */
        SetOp(
            Class<? extends IgniteColocatedSetOp> colocated,
            Class<? extends IgniteMapSetOp> map,
            Class<? extends IgniteReduceSetOp> reduce) {
            this.colocated = colocated;
            this.map = map;
            this.reduce = reduce;
        }
    }
}
