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

package org.apache.ignite.internal.processors.query.calcite.planner.hints;

import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.planner.AbstractPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.TestTable;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.logging.log4j.Level;
import org.junit.Test;

/**
 * Planner test for force index hint.
 */
public class ForceIndexHintPlannerTest extends AbstractPlannerTest {
    /** */
    private IgniteSchema schema;

    /** */
    private TestTable tbl1;

    /** */
    private TestTable tbl2;

    /** {@inheritDoc} */
    @Override public void setup() {
        super.setup();

        // A tiny table.
        tbl1 = createTable("TBL1", 1, IgniteDistributions.single(), "ID", Integer.class,
            "VAL1", Integer.class, "VAL2", Integer.class, "VAL3", Integer.class)
            .addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0)
            .addIndex("IDX1_1", 1)
            .addIndex("IDX1_2", 2)
            .addIndex("IDX1_3", 3);

        // A large table. Has the same first inndex name 'IDX1' as of TBL1.
        tbl2 = createTable("TBL2", 10_000, IgniteDistributions.single(), "ID", Integer.class,
            "VAL21", Integer.class, "VAL22", Integer.class, "VAL23", Integer.class)
            .addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0)
            .addIndex("IDX2_1", 1)
            .addIndex("IDX2_2", 2)
            .addIndex("IDX2_3", 3);

        schema = createSchema(tbl1, tbl2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        ((GridTestLog4jLogger)log).setLevel(Level.INFO);
    }

    /** */
    @Test
    public void testBasicIndexSelection() throws Exception {
        assertPlan("SELECT /*+ FORCE_INDEX(IDX2_3) */ * FROM TBL2 WHERE val23=1 and val21=2 and val22=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3")));

        assertPlan("SELECT * FROM TBL2 /*+ FORCE_INDEX(IDX2_3) */ WHERE val23=1 and val21=2 and val22=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3")));

        // Table hint has a bigger priority.
        assertPlan("SELECT /*+ FORCE_INDEX(IDX2_3) */ * FROM TBL2 /*+ FORCE_INDEX(IDX2_2) */ WHERE val23=1 and " +
            "val21=2 and val22=3", schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX2_2")));

        // First table hint has a bigger priority.
        assertPlan("SELECT * FROM TBL2 /*+ FORCE_INDEX(IDX2_2), FORCE_INDEX(IDX2_3) */ WHERE val23=1 and " +
            "val21=2 and val22=3", schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX2_2")));

        assertPlan("SELECT /*+ FORCE_INDEX(UNEXISTING,IDX2_3,UNEXISTING) */ * FROM TBL2 WHERE val23=1 and val21=2 " +
            "and val22=3", schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3")));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX2_3,IDX2_1) */ * FROM TBL2 WHERE val23=1 and val21=2 " +
            "and val22=3", schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX2_1")
            .or(isIndexScan("TBL2", "IDX2_3"))));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX2_2,IDX2_3) */ * FROM TBL2 WHERE val23=1 and val21=2 and val22=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX2_2"))
                    .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3"))));

        assertPlan("SELECT * FROM TBL2 /*+ FORCE_INDEX(IDX2_2,IDX2_3) */ WHERE val23=1 and val21=2 and val22=3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX2_2"))
                .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3"))));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX2_3), FORCE_INDEX(IDX2_3) */ * FROM TBL2 WHERE val23=1 and val21=2 " +
            "and val22=3", schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3")));
    }
    
    /** */
    @Test
    public void testJoins() throws Exception {
        doTestJoins("LEFT");
        doTestJoins("RIGHT");
        doTestJoins("INNER");
    }

    /** */
    private void doTestJoins(String jt) throws Exception {
        assertPlan("SELECT /*+ FORCE_INDEX(IDX2_2) */ t1.val1, t2.val22 FROM TBL1 t1 " + jt + " JOIN TBL2 " +
                "t2 on t1.val3=t2.val23 and t1.val1=t2.val22", schema,
            nodeOrAnyChild(isIndexScan("TBL2", "IDX2_2")));

        assertPlan("SELECT t1.val1, t2.val22 FROM TBL1 /*+ FORCE_INDEX(IDX1_3) */ t1 " + jt
                + " JOIN TBL2 /*+ FORCE_INDEX(IDX2_2) */ t2 on t1.val3=t2.val23 and t1.val1=t2.val22", schema,
            nodeOrAnyChild(isIndexScan("TBL1", "IDX1_3"))
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX2_2"))));

        assertPlan("SELECT t1.val1, t2.val22 FROM TBL1 t1 " + jt + " JOIN TBL2 /*+ FORCE_INDEX */ t2 on " +
                "t1.val3=t2.val23 and t1.val1=t2.val22", schema,
            nodeOrAnyChild(isIndexScan("TBL2", "IDX2_2"))
                .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3"))));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX2_3) */ t1.val1, t2.val22 FROM TBL1 t1 " + jt + " JOIN TBL2 " +
                "t2 on t1.val3=t2.val23 and t1.val1=t2.val22", schema,
            nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3")));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX2_2,IDX2_3) */ t1.val1, t2.val22 FROM TBL1 t1 " + jt + " JOIN TBL2 " +
                "t2 on t1.val3=t2.val23 and t1.val1=t2.val22", schema,
            nodeOrAnyChild(isIndexScan("TBL2", "IDX2_2"))
                .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3"))));

        // With additional filter.
        assertPlan("SELECT /*+ FORCE_INDEX(IDX2_2,IDX2_3) */ t1.val1, t2.val22 FROM TBL1 t1 " + jt + " JOIN TBL2 " +
                "t2 on t1.val3=t2.val23 and t1.val1=t2.val22 where t2.val22=2 and t1.val3=3 and t2.val21=1", schema,
            nodeOrAnyChild(isIndexScan("TBL2", "IDX2_2"))
                .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3"))));
    }

    /** */
    @Test
    public void testOrderBy() throws Exception {
        assertPlan("SELECT /*+ FORCE_INDEX(IDX1_3) */ val2, val3 FROM TBL1 ORDER by val2, val1, val3", schema,
            nodeOrAnyChild(isIndexScan("TBL1", "IDX1_3")));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX1_2) */ val2, val3 FROM TBL1 ORDER by val2, val1, val3", schema,
            nodeOrAnyChild(isIndexScan("TBL1", "IDX1_2")));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX1_1) */ val2, val3 FROM TBL1 ORDER by val2, val1, val3", schema,
            nodeOrAnyChild(isIndexScan("TBL1", "IDX1_1")));
    }

    /** */
    @Test
    public void testAggregates() throws Exception {
        doTestAggregates("sum");
        doTestAggregates("avg");
        doTestAggregates("min");
        doTestAggregates("max");
    }
    
    /** */
    private void doTestAggregates(String op) throws Exception {
        assertPlan("SELECT /*+ FORCE_INDEX(IDX1_2) */ " + op + "(val1) FROM TBL1 where val1=1 group by val2", schema,
            nodeOrAnyChild(isIndexScan("TBL1", "IDX1_2")));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX1_2) */ " + op + "(val1) FROM TBL1 group by val2", schema,
            nodeOrAnyChild(isIndexScan("TBL1", "IDX1_2")));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX1_1) */ " + op + "(val1) FROM TBL1 where val1=1 group by val2", schema,
            nodeOrAnyChild(isIndexScan("TBL1", "IDX1_1")));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX1_2) */ " + op + "(val1) FROM TBL1 where val1=1 group by val2", schema,
            nodeOrAnyChild(isIndexScan("TBL1", "IDX1_2")));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX1_1,IDX1_2) */ " + op + "(val1) FROM TBL1 where val1=1 group by val2",
            schema, nodeOrAnyChild(isIndexScan("TBL1", "IDX1_2"))
                .or(nodeOrAnyChild(isIndexScan("TBL1", "IDX1_1"))));
    }

    /** */
    @Test
    public void testWithNoIndexHint() throws Exception {
        LogListener lsnr = LogListener.matches("Skipped hint 'NO_INDEX' with options 'IDX2_3'")
            .andMatches("Index 'IDX2_3' of table 'TBL2' has already been excluded").times(1).build();

        lsnrLog.registerListener(lsnr);

        ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

        assertPlan("SELECT /*+ NO_INDEX(IDX2_1), FORCE_INDEX(IDX2_3), NO_INDEX(IDX2_3) */ * FROM TBL2 where " +
            "val21=1 and val22=2 and val23=3", schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3")));

        assertTrue(lsnr.check());

        assertPlan("SELECT /*+ NO_INDEX(IDX2_1), FORCE_INDEX(IDX2_1), FORCE_INDEX(IDX2_3) */ * FROM TBL2 where " +
            "val21=1 and val22=2 and val23=3", schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3")));

        assertPlan("SELECT /*+ NO_INDEX */ t1.val1 FROM TBL1 t1 where t1.val2 = " +
            "(SELECT t2.val23 from TBL2 /*+ FORCE_INDEX(IDX2_3) */ t2 where t2.val21=10 and t2.val23=10 and " +
            "t2.val21=10)", schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3")));

        assertPlan("SELECT /*+ FORCE_INDEX(IDX2_3), NO_INDEX */ * FROM TBL2 where " +
            "val21=1 and val22=2 and val23=3", schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3")));

        // Table hint has a bigger priority.
        assertPlan("SELECT /*+ FORCE_INDEX(IDX2_1), FORCE_INDEX(IDX2_3) */ * FROM TBL2 /*+ NO_INDEX(IDX2_1) */ where " +
            "val21=1 and val22=2 and val23=3", schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3")));
        assertPlan("SELECT /*+ NO_INDEX */ * FROM TBL2 /*+ FORCE_INDEX(IDX2_3) */ where " +
            "val21=1 and val22=2 and val23=3", schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3")));
    }

    /** */
    @Test
    public void testSubquery() throws Exception {
        assertPlan("SELECT /*+ FORCE_INDEX(IDX1_2,IDX2_3) */ t1.val1 FROM TBL1 t1 where t1.val2 = " +
                "(SELECT t2.val23 from TBL2 t2 where t2.val21=10  and t2.val23=10 and t2.val21=10)", schema,
            nodeOrAnyChild(isIndexScan("TBL1", "IDX1_2"))
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3"))));

        assertPlan("SELECT t1.val1 FROM TBL1 t1 where t1.val2 = (SELECT /*+ FORCE_INDEX(IDX1_2,IDX2_3) */ " +
                "t2.val23 from TBL2 t2 where t2.val21=10 and t2.val23=10 and t2.val21=10)", schema,
            nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3")));

        assertPlan("SELECT t1.val1 FROM TBL1 t1 where t1.val2 = (SELECT t2.val23 from TBL2 " +
                "/*+ FORCE_INDEX(IDX2_3) */ t2 where t2.val21=10 and t2.val23=10 and t2.val21=10)", schema,
            nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3")));
    }

    /** */
    @Test
    public void testTwoTables() throws Exception {
        assertPlan("SELECT /*+ FORCE_INDEX(IDX1_1,IDX2_1,IDX2_2) */ val1 FROM TBL1, TBL2 WHERE val1=val21 and " +
            "val2=val22 and val3=val23", schema, nodeOrAnyChild(isIndexScan("TBL1", "IDX1_1"))
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX2_1")
                .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX2_2"))))));

        assertPlan("SELECT val1 FROM TBL1 /*+ FORCE_INDEX(IDX1_1) */, TBL2 /*+ FORCE_INDEX(IDX2_1,IDX2_2) */ WHERE " +
            "val1=val21 and val2=val22 and val3=val23", schema, nodeOrAnyChild(isIndexScan("TBL1", "IDX1_1"))
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX2_1")
                .or(nodeOrAnyChild(isIndexScan("TBL2", "IDX2_2"))))));
    }
}
