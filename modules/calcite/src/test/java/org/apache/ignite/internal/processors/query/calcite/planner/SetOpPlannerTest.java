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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteMapIntersect;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteMapMinus;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteMapSetOp;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteReduceIntersect;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteReduceMinus;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteReduceSetOp;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteSetOp;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteSingleIntersect;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteSingleMinus;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteSingleSetOp;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Test to verify set op (EXCEPT, INTERSECT).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SetOpPlannerTest extends AbstractPlannerTest {
    /** Public schema. */
    private IgniteSchema publicSchema;

    /**
     *
     */
    @BeforeAll
    public void setup() {
        publicSchema = new IgniteSchema("PUBLIC");

        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        RelDataType type = new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("SALARY", f.createJavaType(Double.class))
                .build();

        createTable(publicSchema, "RANDOM_TBL1", type, IgniteDistributions.random());
        createTable(publicSchema, "RANDOM_TBL2", type, IgniteDistributions.random());
        createTable(publicSchema, "BROADCAST_TBL1", type, IgniteDistributions.broadcast());
        createTable(publicSchema, "BROADCAST_TBL2", type, IgniteDistributions.broadcast());
        createTable(publicSchema, "SINGLE_TBL1", type, IgniteDistributions.single());
        createTable(publicSchema, "SINGLE_TBL2", type, IgniteDistributions.single());

        createTable(publicSchema, "AFFINITY_TBL1", type,
                IgniteDistributions.affinity(0, "Test1", "hash"));

        createTable(publicSchema, "AFFINITY_TBL2", type,
                IgniteDistributions.affinity(0, "Test2", "hash"));
    }

    /**
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpRandom(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM random_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM random_tbl2 ";

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
    @ParameterizedTest
    @EnumSource
    public void testSetOpAllRandom(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM random_tbl1 "
                + setOpAll(setOp)
                + "SELECT * FROM random_tbl2 ";

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
    @ParameterizedTest
    @EnumSource
    public void testSetOpBroadcast(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM broadcast_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM broadcast_tbl2 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.single)
                .and(input(0, isTableScan("broadcast_tbl1")))
                .and(input(1, isTableScan("broadcast_tbl2")))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpSingle(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM single_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM single_tbl2 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.single)
                .and(input(0, isTableScan("single_tbl1")))
                .and(input(1, isTableScan("single_tbl2"))));
    }

    /**
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpSingleAndRandom(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM single_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM random_tbl1 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.single)
                .and(input(0, isTableScan("single_tbl1")))
                .and(input(1, hasChildThat(isTableScan("random_tbl1")))));
    }

    /**
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpSingleAndAffinity(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM single_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM affinity_tbl1 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.single)
                .and(input(0, isTableScan("single_tbl1")))
                .and(input(1, hasChildThat(isTableScan("affinity_tbl1")))));
    }

    /**
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpSingleAndBroadcast(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM single_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM broadcast_tbl1 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.single)
                .and(input(0, isTableScan("single_tbl1")))
                .and(input(1, isTableScan("broadcast_tbl1")))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpAffinity(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM affinity_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM affinity_tbl2 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.reduce)
                .and(hasChildThat(isInstanceOf(setOp.map)
                        .and(input(0, isTableScan("affinity_tbl1")))
                        .and(input(1, isTableScan("affinity_tbl2")))
                ))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpBroadcastAndRandom(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM random_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM broadcast_tbl1 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.single)
                .and(input(0, hasChildThat(isTableScan("random_tbl1"))))
                .and(input(1, isTableScan("broadcast_tbl1")))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpRandomNested(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM random_tbl2 "
                + setOp(setOp) + "("
                + "   SELECT * FROM random_tbl1 "
                + setOp(setOp)
                + "   SELECT * FROM random_tbl2"
                + ")";

        if (setOp == SetOp.EXCEPT) {
            assertPlan(sql, publicSchema, isInstanceOf(setOp.single)
                    .and(input(0, hasChildThat(isTableScan("random_tbl2"))))
                    .and(input(1, isInstanceOf(setOp.reduce)
                            .and(hasChildThat(isInstanceOf(setOp.map)
                                    .and(input(0, isTableScan("random_tbl1")))
                                    .and(input(1, isTableScan("random_tbl2")))
                            ))
                    ))
            );
        } else {
            // INTERSECT operator is commutative and can be merged.
            assertPlan(sql, publicSchema, isInstanceOf(setOp.reduce)
                    .and(hasChildThat(isInstanceOf(setOp.map)
                            .and(input(0, isTableScan("random_tbl2")))
                            .and(input(1, isTableScan("random_tbl1")))
                            .and(input(2, isTableScan("random_tbl2")))
                    ))
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpBroadcastAndRandomNested(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM broadcast_tbl1 "
                + setOp(setOp)
                + "("
                + "   SELECT * FROM random_tbl1 "
                + setOp(setOp)
                + "   SELECT * FROM random_tbl2"
                + ")";

        if (setOp == SetOp.EXCEPT) {
            assertPlan(sql, publicSchema, isInstanceOf(setOp.single)
                    .and(input(0, isTableScan("broadcast_tbl1")))
                    .and(input(1, isInstanceOf(setOp.reduce)
                            .and(hasChildThat(isInstanceOf(setOp.map)
                                    .and(input(0, isTableScan("random_tbl1")))
                                    .and(input(1, isTableScan("random_tbl2")))
                            ))
                    ))
            );
        } else {
            // INTERSECT operator is commutative and can be merged.
            assertPlan(sql, publicSchema, isInstanceOf(setOp.reduce)
                    .and(hasChildThat(isInstanceOf(setOp.map)
                            .and(input(0, nodeOrAnyChild(isTableScan("broadcast_tbl1"))))
                            .and(input(1, isTableScan("random_tbl1")))
                            .and(input(2, isTableScan("random_tbl2")))
                    ))
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpMerge(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM random_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM random_tbl2 "
                + setOp(setOp)
                + "SELECT * FROM affinity_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM affinity_tbl2 ";

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
    @ParameterizedTest
    @EnumSource
    public void testSetOpAllMerge(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM random_tbl1 "
                + setOpAll(setOp)
                + "SELECT * FROM random_tbl2 "
                + setOpAll(setOp)
                + "SELECT * FROM affinity_tbl1 "
                + setOpAll(setOp)
                + "SELECT * FROM affinity_tbl2 ";

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
    @ParameterizedTest
    @EnumSource
    public void testSetOpAllWithExceptMerge(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM random_tbl1 "
                + setOpAll(setOp)
                + "SELECT * FROM random_tbl2 "
                + setOp(setOp)
                + "SELECT * FROM affinity_tbl1 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.reduce).and(n -> !n.all())
                .and(hasChildThat(isInstanceOf(setOp.map)
                        .and(input(0, isTableScan("random_tbl1")))
                        .and(input(1, isTableScan("random_tbl2")))
                        .and(input(2, isTableScan("affinity_tbl1")))
                ))
        );
    }

    /**
     *
     */
    private String setOp(SetOp setOp) {
        return setOp.name() + ' ';
    }

    /**
     *
     */
    private String setOpAll(SetOp setOp) {
        return setOp.name() + " ALL ";
    }

    /**
     *
     */
    enum SetOp {
        /**
         *
         */
        EXCEPT(
                IgniteSingleMinus.class,
                IgniteMapMinus.class,
                IgniteReduceMinus.class
        ),

        /**
         *
         */
        INTERSECT(
                IgniteSingleIntersect.class,
                IgniteMapIntersect.class,
                IgniteReduceIntersect.class
        );

        /**
         *
         */
        public final Class<? extends IgniteSingleSetOp> single;

        /**
         *
         */
        public final Class<? extends IgniteMapSetOp> map;

        /**
         *
         */
        public final Class<? extends IgniteReduceSetOp> reduce;

        /**
         *
         */
        SetOp(
                Class<? extends IgniteSingleSetOp> single,
                Class<? extends IgniteMapSetOp> map,
                Class<? extends IgniteReduceSetOp> reduce) {
            this.single = single;
            this.map = map;
            this.reduce = reduce;
        }
    }
}
