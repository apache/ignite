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

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTrimExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteUncollect;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.broadcast;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.single;

/**
 * Test uncollect operator.
 */
public class UncollectPlannerTest extends AbstractPlannerTest {
    /** Public schema. */
    private IgniteSchema publicSchema;

    /** {@inheritDoc} */
    @Before
    @Override public void setup() {
        super.setup();

        publicSchema = createSchema(
            createTable("HASH_TBL", IgniteDistributions.hash(ImmutableIntList.of(0)),
                "ID", SqlTypeName.INTEGER, "NAME", SqlTypeName.VARCHAR),
            createTable("RANDOM_TBL", IgniteDistributions.random(),
                "ID", SqlTypeName.INTEGER, "NAME", SqlTypeName.VARCHAR),
            createTable("BROADCAST_TBL", broadcast(),
                "ID", SqlTypeName.INTEGER, "NAME", SqlTypeName.VARCHAR)
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimpleUncollect() throws Exception {
        String sql = "SELECT * FROM UNNEST(ARRAY[1, 2, 3])";

        assertPlan(sql, publicSchema, isInstanceOf(Uncollect.class));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBroadcastTableAndUnnestJoin() throws Exception {
        String sql = "SELECT * FROM broadcast_tbl t JOIN UNNEST(ARRAY[1, 2, 3]) AS r(x) ON (t.id = r.x)";

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteExchange.class)).negate()
            .and(nodeOrAnyChild(isInstanceOf(Join.class)
                .and(nodeOrAnyChild(isTableScan("broadcast_tbl")))
                .and(nodeOrAnyChild(isInstanceOf(IgniteUncollect.class)
                    .and(nodeOrAnyChild(isInstanceOf(IgniteUncollect.class)
                        .and(distributionSatisfies(single()))
                    ))
                ))
            ))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRandomTableAndUnnestJoin() throws Exception {
        String sql = "SELECT * FROM random_tbl t JOIN UNNEST(ARRAY[1, 2, 3]) AS r(x) ON (t.id = r.x)";

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(Join.class)
            .and(nodeOrAnyChild(isInstanceOf(IgniteExchange.class)
                .and(nodeOrAnyChild(isTableScan("random_tbl")))))
            .and(nodeOrAnyChild(isInstanceOf(IgniteExchange.class).negate())
                .and(nodeOrAnyChild(isInstanceOf(IgniteUncollect.class)
                    .and(distributionSatisfies(single()))
                ))
            )
        ));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testHashTableAndUnnestJoin() throws Exception {
        String sql = "SELECT * FROM hash_tbl t JOIN UNNEST(ARRAY[1, 2, 3]) AS r(x) ON (t.id = r.x)";

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(Join.class)
            .and(nodeOrAnyChild(isInstanceOf(IgniteExchange.class).negate())
                .and(nodeOrAnyChild(isTableScan("hash_tbl"))))
            .and(nodeOrAnyChild(isInstanceOf(IgniteTrimExchange.class))
                .and(nodeOrAnyChild(isInstanceOf(IgniteUncollect.class)
                    .and(hasDistribution(broadcast()))
                ))
            )
        ));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCorrelatedUnnestJoin() throws Exception {
        String sql = "SELECT t.id, (SELECT * FROM UNNEST(ARRAY[t.id])) FROM random_tbl t";

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
            .and(input(0, nodeOrAnyChild(isTableScan("random_tbl"))))
            .and(input(1, nodeOrAnyChild(isInstanceOf(IgniteExchange.class).negate())
                .and(nodeOrAnyChild(isInstanceOf(IgniteUncollect.class)
                    .and(nodeOrAnyChild(isInstanceOf(IgniteUncollect.class)
                        .and(distributionSatisfies(single()))
                    ))
                ))
            ))
        ));
    }
}
