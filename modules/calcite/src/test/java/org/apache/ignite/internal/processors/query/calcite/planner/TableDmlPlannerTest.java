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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Spool;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTrimExchange;
import org.apache.ignite.internal.processors.query.calcite.rule.TableModifySingleNodeConverterRule;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.OTHER;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Table spool test.
 */
@SuppressWarnings({"TooBroadScope", "FieldCanBeLocal", "TypeMayBeWeakened"})
public class TableDmlPlannerTest extends AbstractPlannerTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void insertCachesTableScan() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("TEST", IgniteDistributions.random(), "VAL", Integer.class)
        );

        String sql = "insert into test select 2 * val from test";

        RelNode phys = physicalPlan(sql, schema, "LogicalIndexScanConverterRule");

        assertNotNull(phys);

        String invalidPlanMsg = "Invalid plan:\n" + RelOptUtil.toString(phys);

        IgniteTableModify modifyNode = findFirstNode(phys, byClass(IgniteTableModify.class));

        assertThat(invalidPlanMsg, modifyNode, notNullValue());
        assertThat(invalidPlanMsg, modifyNode.getInput(), instanceOf(Spool.class));

        Spool spool = (Spool)modifyNode.getInput();

        assertThat(invalidPlanMsg, spool.readType, equalTo(Spool.Type.EAGER));
        assertThat(invalidPlanMsg, findFirstNode(phys, byClass(IgniteTableScan.class)), notNullValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void insertCachesIndexScan() throws Exception {
        TestTable tbl = createTable("TEST", IgniteDistributions.random(), "VAL", Integer.class);

        tbl.addIndex("IDX", 0);

        IgniteSchema schema = createSchema(tbl);

        String sql = "insert into test select 2 * val from test";

        RelNode phys = physicalPlan(sql, schema, "LogicalTableScanConverterRule");

        assertNotNull(phys);

        String invalidPlanMsg = "Invalid plan:\n" + RelOptUtil.toString(phys);

        IgniteTableModify modifyNode = findFirstNode(phys, byClass(IgniteTableModify.class));

        assertThat(invalidPlanMsg, modifyNode, notNullValue());
        assertThat(invalidPlanMsg, modifyNode.getInput(), instanceOf(Spool.class));

        Spool spool = (Spool)modifyNode.getInput();

        assertThat(invalidPlanMsg, spool.readType, equalTo(Spool.Type.EAGER));
        assertThat(invalidPlanMsg, findFirstNode(phys, byClass(IgniteIndexScan.class)), notNullValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void updateNotCachesTableScan() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("TEST", IgniteDistributions.random(), "VAL", Integer.class)
        );

        String sql = "update test set val = 2 * val";

        RelNode phys = physicalPlan(sql, schema, "LogicalIndexScanConverterRule");

        assertNotNull(phys);

        String invalidPlanMsg = "Invalid plan:\n" + RelOptUtil.toString(phys);

        assertThat(invalidPlanMsg, findFirstNode(phys, byClass(Spool.class)), nullValue());
        assertThat(invalidPlanMsg, findFirstNode(phys, byClass(IgniteTableScan.class)), notNullValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void updateNotCachesNonDependentIndexScan() throws Exception {
        TestTable tbl = createTable("TEST", IgniteDistributions.random(), "VAL", Integer.class, "IDX_VAL", Integer.class);

        tbl.addIndex("IDX", 1);

        IgniteSchema schema = createSchema(tbl);

        String sql = "update test set val = 2 * val where idx_val between 2 and 10";

        RelNode phys = physicalPlan(sql, schema, "LogicalTableScanConverterRule");

        assertNotNull(phys);

        String invalidPlanMsg = "Invalid plan:\n" + RelOptUtil.toString(phys);

        assertThat(invalidPlanMsg, findFirstNode(phys, byClass(Spool.class)), nullValue());
        assertThat(invalidPlanMsg, findFirstNode(phys, byClass(IgniteIndexScan.class)), notNullValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void updateCachesDependentIndexScan() throws Exception {
        TestTable tbl = createTable("TEST", IgniteDistributions.random(), "VAL", Integer.class);

        tbl.addIndex("IDX", 0);

        IgniteSchema schema = createSchema(tbl);

        String sql = "update test set val = 2 * val where val between 2 and 10";

        RelNode phys = physicalPlan(sql, schema, "LogicalTableScanConverterRule");

        assertNotNull(phys);

        String invalidPlanMsg = "Invalid plan:\n" + RelOptUtil.toString(phys);

        IgniteTableModify modifyNode = findFirstNode(phys, byClass(IgniteTableModify.class));

        assertThat(invalidPlanMsg, modifyNode, notNullValue());
        assertThat(invalidPlanMsg, modifyNode.getInput(), instanceOf(Spool.class));

        Spool spool = (Spool)modifyNode.getInput();

        assertThat(invalidPlanMsg, spool.readType, equalTo(Spool.Type.EAGER));
        assertThat(invalidPlanMsg, findFirstNode(phys, byClass(IgniteIndexScan.class)), notNullValue());
    }

    /** Tests that queries with duplicated column names are correctly parsed. */
    @Test
    public void testDuplicatedColumnNames() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("CITY", IgniteDistributions.random(), "ID", INTEGER, "NAME", VARCHAR),
            createTable("STREET", IgniteDistributions.random(), "ID", INTEGER, "CITY_ID", INTEGER,
                "NAME", VARCHAR)
        );

        assertPlan("SELECT NAME, (SELECT NAME FROM CITY WHERE ID = S.CITY_ID LIMIT 1) AS NAME FROM STREET S ORDER BY ID",
            schema, hasColumns("NAME", "NAME1"));

        assertPlan("SELECT CITY_ID, NAME, NAME FROM STREET ORDER BY ID", schema,
            hasColumns("CITY_ID", "NAME", "NAME2"));

        assertPlan("SELECT CITY.NAME, STREET.NAME FROM STREET JOIN CITY ON STREET.CITY_ID = CITY.ID ORDER BY STREET.ID",
            schema, hasColumns("NAME", "NAME1"));
    }

    /** Tests that table modify can be executed on remote nodes. */
    @Test
    public void testDistributedTableModify() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("TEST", IgniteDistributions.affinity(3, "test", "hash"),
                QueryUtils.KEY_FIELD_NAME, OTHER,
                QueryUtils.VAL_FIELD_NAME, OTHER,
                "ID", INTEGER,
                "AFF_ID", INTEGER,
                "VAL", INTEGER
            ),
            createTable("TEST2", IgniteDistributions.affinity(2, "test2", "hash"),
                QueryUtils.KEY_FIELD_NAME, OTHER,
                QueryUtils.VAL_FIELD_NAME, OTHER,
                "ID", INTEGER,
                "AFF_ID", INTEGER,
                "VAL", INTEGER
            ),
            createTable("TEST3", IgniteDistributions.random(),
                QueryUtils.KEY_FIELD_NAME, OTHER,
                QueryUtils.VAL_FIELD_NAME, OTHER,
                "ID", INTEGER,
                "AFF_ID", INTEGER,
                "VAL", INTEGER
            ),
            createTable("TEST_REPL", IgniteDistributions.broadcast(),
                QueryUtils.KEY_FIELD_NAME, OTHER,
                QueryUtils.VAL_FIELD_NAME, OTHER,
                "ID", INTEGER,
                "AFF_ID", INTEGER,
                "VAL", INTEGER
            ),
            createTable("TEST_REPL2", IgniteDistributions.broadcast(),
                QueryUtils.KEY_FIELD_NAME, OTHER,
                QueryUtils.VAL_FIELD_NAME, OTHER,
                "ID", INTEGER,
                "AFF_ID", INTEGER,
                "VAL", INTEGER
            )
        );

        // Check INSERT statements.

        // partitioned <- values (broadcast).
        assertPlan("INSERT INTO test VALUES (?, ?, ?)", schema,
            isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.single())));

        // partitioned <- partitioned (same).
        assertPlan("INSERT INTO test SELECT * FROM test", schema,
            hasChildThat(isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.random()))
                .and(input(isInstanceOf(IgniteTableSpool.class)
                    .and(input(isTableScan("TEST")))))));

        // partitioned <- partitioned (same, affinity key change).
        assertPlan("INSERT INTO test SELECT id, aff_id + 1, val FROM test", schema,
            isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.single()))
                .and(input(isInstanceOf(IgniteTableSpool.class)
                    .and(input(isInstanceOf(IgniteExchange.class)
                        .and(input(isTableScan("TEST"))))))));

        // partitioned <- partitioned (another affinity).
        assertPlan("INSERT INTO test SELECT * FROM test2", schema,
            hasChildThat(isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.random()))
                .and(input(isTableScan("TEST2")))));

        // partitioned <- partitioned (another affinity, affinity key change).
        assertPlan("INSERT INTO test SELECT id, aff_id + 1, val FROM test2", schema,
            hasChildThat(isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.random()))
                .and(input(isTableScan("TEST2")))));

        // partitioned <- random.
        assertPlan("INSERT INTO test SELECT * FROM test3", schema,
            hasChildThat(isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.random()))
                .and(input(isTableScan("TEST3")))));

        // partitioned <- broadcast.
        assertPlan("INSERT INTO test SELECT * FROM test_repl", schema,
            isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.single()))
                .and(input(isTableScan("TEST_REPL"))));

        // partitioned <- broadcast (force distributed).
        assertPlan("INSERT INTO test SELECT * FROM test_repl", schema,
            hasChildThat(isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.random()))
                .and(input(isInstanceOf(IgniteTrimExchange.class)
                    .and(input(isTableScan("TEST_REPL")))))),
            TableModifySingleNodeConverterRule.class.getSimpleName()
        );

        // broadcast <- partitioned.
        assertPlan("INSERT INTO test_repl SELECT * FROM test", schema,
            hasChildThat(isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.random()))
                .and(input(isTableScan("TEST")))));

        // roadcast <- random.
        assertPlan("INSERT INTO test_repl SELECT * FROM test3", schema,
            hasChildThat(isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.random()))
                .and(input(isTableScan("TEST3")))));

        // broadcast <- broadcast.
        assertPlan("INSERT INTO test_repl SELECT * FROM test_repl2", schema,
            isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.single()))
                .and(input(isTableScan("TEST_REPL2"))));

        // broadcast <- broadcast (force distributed).
        assertPlan("INSERT INTO test_repl SELECT * FROM test_repl2", schema,
            hasChildThat(isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.random()))
                .and(input(isInstanceOf(IgniteTrimExchange.class)
                    .and(input(isTableScan("TEST_REPL2")))))),
            TableModifySingleNodeConverterRule.class.getSimpleName()
        );

        // broadcast <- broadcast (same).
        assertPlan("INSERT INTO test_repl SELECT * FROM test_repl", schema,
            isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.single()))
                .and(input(isInstanceOf(IgniteTableSpool.class)
                    .and(input(isTableScan("TEST_REPL"))))));

        // broadcast <- broadcast (same, force distributed).
        GridTestUtils.assertThrows(null, () -> {
                physicalPlan("INSERT INTO test_repl SELECT * FROM test_repl", schema,
                    TableModifySingleNodeConverterRule.class.getSimpleName());
            }, IgniteException.class, ""
        );

        // random <- partitioned.
        assertPlan("INSERT INTO test3 SELECT * FROM test", schema,
            hasChildThat(isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.random()))
                .and(input(isTableScan("TEST")))));

        // random <- broadcast.
        assertPlan("INSERT INTO test3 SELECT * FROM test_repl", schema,
            isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.single()))
                .and(input(isTableScan("TEST_REPL"))));

        // random <- broadcast (force distributed).
        assertPlan("INSERT INTO test3 SELECT * FROM test_repl", schema,
            hasChildThat(isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.random()))
                .and(input(isInstanceOf(IgniteTrimExchange.class)
                    .and(input(isTableScan("TEST_REPL")))))),
            TableModifySingleNodeConverterRule.class.getSimpleName()
        );

        // Check UPDATE statements.

        // partitioned.
        assertPlan("UPDATE test SET val = val + 1", schema,
            hasChildThat(isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.random()))
                .and(input(isTableScan("TEST")))));

        // broadcast.
        assertPlan("UPDATE test_repl SET val = val + 1", schema,
            isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.single()))
                .and(input(isTableScan("TEST_REPL"))));

        // broadcast (force distributed).
        assertPlan("UPDATE test_repl SET val = val + 1", schema,
            hasChildThat(isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.random()))
                .and(input(isInstanceOf(IgniteTrimExchange.class)
                    .and(input(isTableScan("TEST_REPL")))))),
            TableModifySingleNodeConverterRule.class.getSimpleName()
        );

        // random.
        assertPlan("UPDATE test3 SET val = val + 1", schema,
            hasChildThat(isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.random()))
                .and(input(isTableScan("TEST3")))));

        // Check DELETE statements.

        // partitioned.
        assertPlan("DELETE FROM test WHERE val = 10", schema,
            hasChildThat(isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.random()))
                .and(input(isTableScan("TEST")))));

        // broadcast.
        assertPlan("DELETE FROM test_repl WHERE val = 10", schema,
            isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.single()))
                .and(input(isTableScan("TEST_REPL"))));

        // broadcast (force distributed).
        assertPlan("DELETE FROM test_repl WHERE val = 10", schema,
            hasChildThat(isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.random()))
                .and(input(isInstanceOf(IgniteTrimExchange.class)
                    .and(input(isTableScan("TEST_REPL")))))),
            TableModifySingleNodeConverterRule.class.getSimpleName()
        );

        // random.
        assertPlan("DELETE FROM test3 WHERE val = 10", schema,
            hasChildThat(isInstanceOf(IgniteTableModify.class).and(hasDistribution(IgniteDistributions.random()))
                .and(input(isTableScan("TEST3")))));
    }
}
