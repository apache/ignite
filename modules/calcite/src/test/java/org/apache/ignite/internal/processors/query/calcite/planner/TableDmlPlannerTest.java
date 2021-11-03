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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Spool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.junit.jupiter.api.Test;

/**
 * Table spool test.
 */
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

        Spool spool = (Spool) modifyNode.getInput();

        assertThat(invalidPlanMsg, spool.readType, equalTo(Spool.Type.EAGER));
        assertThat(invalidPlanMsg, findFirstNode(phys, byClass(IgniteTableScan.class)), notNullValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void insertCachesIndexScan() throws Exception {
        TestTable tbl = createTable("TEST", IgniteDistributions.random(), "VAL", Integer.class);

        tbl.addIndex(new IgniteIndex(RelCollations.of(0), "IDX", tbl));

        IgniteSchema schema = createSchema(tbl);

        String sql = "insert into test select 2 * val from test";

        RelNode phys = physicalPlan(sql, schema, "LogicalTableScanConverterRule");

        assertNotNull(phys);

        String invalidPlanMsg = "Invalid plan:\n" + RelOptUtil.toString(phys);

        IgniteTableModify modifyNode = findFirstNode(phys, byClass(IgniteTableModify.class));

        assertThat(invalidPlanMsg, modifyNode, notNullValue());
        assertThat(invalidPlanMsg, modifyNode.getInput(), instanceOf(Spool.class));

        Spool spool = (Spool) modifyNode.getInput();

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

        tbl.addIndex(new IgniteIndex(RelCollations.of(1), "IDX", tbl));

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

        tbl.addIndex(new IgniteIndex(RelCollations.of(0), "IDX", tbl));

        IgniteSchema schema = createSchema(tbl);

        String sql = "update test set val = 2 * val where val between 2 and 10";

        RelNode phys = physicalPlan(sql, schema, "LogicalTableScanConverterRule");

        assertNotNull(phys);

        String invalidPlanMsg = "Invalid plan:\n" + RelOptUtil.toString(phys);

        IgniteTableModify modifyNode = findFirstNode(phys, byClass(IgniteTableModify.class));

        assertThat(invalidPlanMsg, modifyNode, notNullValue());
        assertThat(invalidPlanMsg, modifyNode.getInput(), instanceOf(Spool.class));

        Spool spool = (Spool) modifyNode.getInput();

        assertThat(invalidPlanMsg, spool.readType, equalTo(Spool.Type.EAGER));
        assertThat(invalidPlanMsg, findFirstNode(phys, byClass(IgniteIndexScan.class)), notNullValue());
    }
}
