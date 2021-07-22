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
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteMapMinus;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteReduceMinus;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteSingleMinus;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Test to verify EXCEPT operator.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ExceptPlannerTest extends AbstractPlannerTest {
    /** Public schema. */
    private IgniteSchema publicSchema;

    /** */
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
    @Test
    public void testExceptRandom() throws Exception {
        String sql = "" +
            "SELECT * FROM random_tbl1 " +
            "EXCEPT " +
            "SELECT * FROM random_tbl2 ";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteReduceMinus.class).and(n -> !n.all)
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

        assertPlan(sql, publicSchema, isInstanceOf(IgniteReduceMinus.class).and(n -> n.all)
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

        assertPlan(sql, publicSchema, isInstanceOf(IgniteSingleMinus.class)
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

        assertPlan(sql, publicSchema, isInstanceOf(IgniteSingleMinus.class)
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

        assertPlan(sql, publicSchema, isInstanceOf(IgniteSingleMinus.class)
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

        assertPlan(sql, publicSchema, isInstanceOf(IgniteSingleMinus.class)
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

        assertPlan(sql, publicSchema, isInstanceOf(IgniteSingleMinus.class)
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

        assertPlan(sql, publicSchema, isInstanceOf(IgniteReduceMinus.class)
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

        assertPlan(sql, publicSchema, isInstanceOf(IgniteSingleMinus.class)
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

        assertPlan(sql, publicSchema, isInstanceOf(IgniteSingleMinus.class)
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

        assertPlan(sql, publicSchema, isInstanceOf(IgniteSingleMinus.class)
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

        assertPlan(sql, publicSchema, isInstanceOf(IgniteReduceMinus.class)
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

        assertPlan(sql, publicSchema, isInstanceOf(IgniteReduceMinus.class).and(n -> n.all)
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

        assertPlan(sql, publicSchema, isInstanceOf(IgniteReduceMinus.class).and(n -> !n.all)
            .and(hasChildThat(isInstanceOf(IgniteMapMinus.class)
                .and(input(0, isTableScan("random_tbl1")))
                .and(input(1, isTableScan("random_tbl2")))
                .and(input(2, isTableScan("affinity_tbl1")))
            ))
        );
    }
}
