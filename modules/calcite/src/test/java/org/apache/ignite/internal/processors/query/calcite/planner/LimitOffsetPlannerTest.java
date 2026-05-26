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
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteLimit;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteUnionAll;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.util.RexUtils.doubleFromRex;

/**
 * Planner test for LIMIT and OFFSET.
 */
public class LimitOffsetPlannerTest extends AbstractPlannerTest {
    /** Row count in table. */
    private static final double ROW_CNT = 100d;

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLimit() throws Exception {
        IgniteSchema publicSchema = createSchemaWithTable(IgniteDistributions.broadcast());

        String sql = "SELECT * FROM TEST OFFSET 10 ROWS FETCH FIRST 10 ROWS ONLY";

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteLimit.class))
            .and(hasChildThat(isInstanceOf(IgniteSort.class)).negate()));

        sql = "SELECT * FROM TEST ORDER BY ID OFFSET 10 ROWS FETCH FIRST 10 ROWS ONLY";

        assertPlan(sql, publicSchema, nodeOrAnyChild(isInstanceOf(IgniteLimit.class)
            .and(hasChildThat(isInstanceOf(IgniteSort.class)))));
    }

    /** */
    @Test
    public void testNestedLimitOffsetWithUnion() throws Exception {
        IgniteSchema publicSchema = createSchemaWithTable(IgniteDistributions.random());

        assertPlan("(SELECT ID FROM TEST WHERE ID = 2) UNION ALL " +
                "SELECT ID FROM (SELECT ID from (SELECT ID FROM TEST OFFSET 20) ORDER BY ID OFFSET 10)",
            publicSchema,
            nodeOrAnyChild(isInstanceOf(IgniteLimit.class).and(l -> doubleFromRex(l.offset(), -1) == 10d))
        );
    }

    /** */
    @Test
    public void testEstimateRows() throws Exception {
        IgniteSchema publicSchema = createSchemaWithTable(IgniteDistributions.random());

        assertPlan("SELECT * FROM TEST ORDER BY ID LIMIT 10", publicSchema,
            nodeOrAnyChild(isInstanceOf(IgniteLimit.class)
                .and(l -> l.getCluster().getMetadataQuery().getRowCount(l) == 10d)));

        assertPlan("SELECT * FROM TEST ORDER BY ID LIMIT " + ROW_CNT + " OFFSET 15", publicSchema,
            nodeOrAnyChild(isInstanceOf(IgniteLimit.class)
                .and(l -> l.getCluster().getMetadataQuery().getRowCount(l) == ROW_CNT - 15d)));

        assertPlan("SELECT * FROM TEST ORDER BY ID OFFSET 60", publicSchema,
            nodeOrAnyChild(isInstanceOf(IgniteLimit.class)
                .and(l -> l.getCluster().getMetadataQuery().getRowCount(l) == ROW_CNT - 60d)));

        assertPlan("SELECT * FROM TEST ORDER BY ID LIMIT 1 OFFSET " + ROW_CNT * 2, publicSchema,
            nodeOrAnyChild(isInstanceOf(IgniteLimit.class)
                // Estimated row count returned by IgniteLimit node is 0, but after validation it becomes 1
                // if it less than 1.
                .and(l -> l.getCluster().getMetadataQuery().getRowCount(l) == 1)));
    }

    /** */
    @Test
    public void testOrderOfRels() throws Exception {
        IgniteSchema publicSchema = createSchemaWithTable(IgniteDistributions.random());

        // Simple case, Limit can't be pushed down under Exchange or Sort. Sort before Exchange is more preferable.
        assertPlan("SELECT * FROM TEST ORDER BY ID LIMIT 5 OFFSET 10", publicSchema,
            isInstanceOf(IgniteLimit.class)
                .and(input(isInstanceOf(IgniteExchange.class)
                    .and(input(isInstanceOf(IgniteSort.class)
                        .and(s -> doubleFromRex(s.fetch, -1) == 5.0)
                        .and(s -> doubleFromRex(s.offset, -1) == 10.0))))));

        // Same simple case but witout offset.
        assertPlan("SELECT * FROM TEST ORDER BY ID LIMIT 5", publicSchema,
            isInstanceOf(IgniteLimit.class)
                .and(input(isInstanceOf(IgniteExchange.class)
                    .and(input(isInstanceOf(IgniteSort.class)
                        .and(s -> doubleFromRex(s.fetch, -1) == 5.0)
                        .and(s -> s.offset == null))))));

        // No special limited sort required if LIMIT is not set.
        assertPlan("SELECT * FROM TEST ORDER BY ID OFFSET 10", publicSchema,
            isInstanceOf(IgniteLimit.class)
                .and(input(isInstanceOf(IgniteExchange.class)
                    .and(input(isInstanceOf(IgniteSort.class)
                        .and(s -> s.fetch == null)
                        .and(s -> s.offset == null))))));

        // Simple case without ordering.
        assertPlan("SELECT * FROM TEST OFFSET 10 ROWS FETCH FIRST 5 ROWS ONLY", publicSchema,
            isInstanceOf(IgniteLimit.class)
                .and(s -> doubleFromRex(s.fetch(), -1) == 5)
                .and(s -> doubleFromRex(s.offset(), -1) == 10)
                .and(input(isInstanceOf(IgniteExchange.class)))
                    .and(hasChildThat(isInstanceOf(IgniteSort.class)).negate()));

        // Check that Sort node is not eliminated by aggregation and Exchange node is not eliminated by distribution
        // required by parent nodes.
        assertPlan("SELECT * FROM TEST UNION (SELECT * FROM TEST ORDER BY ID LIMIT 10)", publicSchema,
            nodeOrAnyChild(isInstanceOf(IgniteUnionAll.class)
                .and(hasChildThat(isInstanceOf(IgniteLimit.class)
                    .and(input(isInstanceOf(IgniteExchange.class)
                        .and(input(isInstanceOf(IgniteSort.class)
                            .and(s -> doubleFromRex(s.fetch, -1) == 10.0)))))))));

        // Check that internal Sort node is not eliminated by external Sort node with different collation.
        assertPlan("SELECT * FROM (SELECT * FROM TEST ORDER BY ID LIMIT 10) ORDER BY VAL", publicSchema,
            nodeOrAnyChild(isInstanceOf(IgniteSort.class)
                .and(hasChildThat(isInstanceOf(IgniteLimit.class)
                    .and(input(isInstanceOf(IgniteExchange.class)
                        .and(input(isInstanceOf(IgniteSort.class)
                            .and(s -> doubleFromRex(s.fetch, -1) == 10.0)))))))));

        // Check that extended collation is passed through the Limit node if it satisfies the Limit collation.
        assertPlan("SELECT * FROM (SELECT * FROM TEST ORDER BY ID LIMIT 10) ORDER BY ID, VAL", publicSchema,
            isInstanceOf(IgniteLimit.class)
                .and(input(isInstanceOf(IgniteExchange.class)
                    .and(input(isInstanceOf(IgniteSort.class)
                        .and(input(isInstanceOf(IgniteTableScan.class)))
                        .and(s -> s.collation().getKeys().equals(ImmutableIntList.of(0, 1))))))));

        // Check that external Sort node is not required if external collation is subset of internal collation.
        assertPlan("SELECT * FROM (SELECT * FROM TEST ORDER BY ID, VAL LIMIT 10) ORDER BY ID", publicSchema,
            isInstanceOf(IgniteLimit.class)
                .and(input(isInstanceOf(IgniteExchange.class)
                    .and(input(isInstanceOf(IgniteSort.class)
                        .and(s -> doubleFromRex(s.fetch, -1) == 10.0))))));

        // Check double limit when external collation is a subset of internal collation.
        assertPlan("SELECT * FROM (SELECT * FROM TEST ORDER BY ID, VAL LIMIT 10) ORDER BY ID LIMIT 5 OFFSET 3",
            publicSchema,
            isInstanceOf(IgniteLimit.class)
                .and(input(isInstanceOf(IgniteLimit.class)
                    .and(input(isInstanceOf(IgniteExchange.class)
                        .and(input(isInstanceOf(IgniteSort.class)
                            .and(s -> doubleFromRex(s.fetch, -1) == 10.0)
                            .and(s -> s.offset == null))))))));

        // Check limit/exchange/sort rel order in subquery.
        assertPlan("SELECT NULLIF((SELECT id FROM test ORDER BY id LIMIT 1 OFFSET 1), id) FROM test",
            publicSchema,
            hasChildThat(isInstanceOf(IgniteLimit.class)
                .and(input(isInstanceOf(IgniteExchange.class)
                    .and(e -> e.distribution() == IgniteDistributions.single())
                    .and(input(isInstanceOf(IgniteSort.class)
                        .and(s -> doubleFromRex(s.offset, -1) == 1)
                        .and(s -> doubleFromRex(s.fetch, -1) == 1)))))));

        publicSchema = createSchemaWithTable(IgniteDistributions.random(), 0);

        // Sort node is not required, since collation of the Limit node equals to the index collation.
        assertPlan("SELECT * FROM TEST ORDER BY ID LIMIT 10 OFFSET 10", publicSchema,
            isInstanceOf(IgniteLimit.class)
                .and(input(isInstanceOf(IgniteExchange.class)
                    .and(input(isInstanceOf(IgniteIndexScan.class)))))
                .and(hasChildThat(isInstanceOf(IgniteSort.class)).negate()));

        publicSchema = createSchemaWithTable(IgniteDistributions.random(), 0, 1);

        // Sort node is not required, since collation of the Limit node satisfies the index collation.
        assertPlan("SELECT * FROM TEST ORDER BY ID LIMIT 10 OFFSET 10", publicSchema,
            isInstanceOf(IgniteLimit.class)
                .and(input(isInstanceOf(IgniteExchange.class)
                    .and(input(isInstanceOf(IgniteIndexScan.class)))))
                .and(hasChildThat(isInstanceOf(IgniteSort.class)).negate()));

        publicSchema = createSchemaWithTable(IgniteDistributions.single());

        // Exchange node is not required, since distribution of the table is already "single".
        assertPlan("SELECT * FROM TEST ORDER BY ID LIMIT 10 OFFSET 10", publicSchema,
            isInstanceOf(IgniteLimit.class)
                .and(input(isInstanceOf(IgniteSort.class)))
                .and(hasChildThat(isInstanceOf(IgniteExchange.class)).negate()));
    }

    /**
     * Creates PUBLIC schema with one TEST table.
     */
    private IgniteSchema createSchemaWithTable(IgniteDistribution distr, int... indexedColumns) {
        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        RelDataType type = new RelDataTypeFactory.Builder(f)
            .add("ID", f.createJavaType(Integer.class))
            .add("VAL", f.createJavaType(String.class))
            .build();

        TestTable table = new TestTable("TEST", type, ROW_CNT) {
            @Override public IgniteDistribution distribution() {
                return distr;
            }
        };

        if (!F.isEmpty(indexedColumns))
            table.addIndex("test_idx", indexedColumns);

        return createSchema(table);
    }
}
