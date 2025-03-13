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
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.logging.log4j.Level;
import org.junit.Test;

/**
 * Planner test for index hints.
 */
public class NoIndexHintPlannerTest extends AbstractPlannerTest {
    /** */
    private IgniteSchema schema;

    /** */
    private TestTable tbl1;

    /** */
    private TestTable tbl2;

    /** {@inheritDoc} */
    @Override public void setup() {
        super.setup();

        tbl1 = createTable("TBL1", 100, IgniteDistributions.single(), "ID", Integer.class,
            "VAL1", String.class, "VAL2", String.class, "VAL3", String.class)
            .addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0)
            .addIndex("IDX1_1", 1)
            .addIndex("IDX1_23", 2, 3)
            .addIndex("IDX1_3", 3);

        tbl2 = createTable("TBL2", 100_000, IgniteDistributions.single(), "ID", Integer.class,
            "VAL1", String.class, "VAL2", String.class, "VAL3", String.class)
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
    public void testWrongParams() throws Exception {
        LogListener lsnr = LogListener.matches("Skipped hint 'NO_INDEX' with options 'IDX2_1','IDX2_1'")
            .times(1).build();

        lsnrLog.registerListener(lsnr);

        ((GridTestLog4jLogger)log).setLevel(Level.DEBUG);

        physicalPlan("SELECT /*+ NO_INDEX(IDX2_1,IDX2_1) */ * FROM TBL2 WHERE val2='v'", schema);

        assertTrue(lsnr.check());

        lsnrLog.clearListeners();

        lsnr = LogListener.matches("Skipped hint 'NO_INDEX' with options 'IDX2_1'").times(1).build();

        lsnrLog.registerListener(lsnr);

        physicalPlan("SELECT /*+ NO_INDEX, NO_INDEX(IDX2_1) */ * FROM TBL2 WHERE val2='v'", schema);

        assertTrue(lsnr.check());

        lsnrLog.clearListeners();

        lsnr = LogListener.matches("Skipped hint 'NO_INDEX' with options 'IDX2_1'").times(1).build();

        lsnrLog.registerListener(lsnr);

        // Table hint has a bigger priority.
        physicalPlan("SELECT /*+ NO_INDEX(IDX2_1) */ * FROM TBL2 /*+ NO_INDEX */ WHERE val2='v'", schema);

        assertTrue(lsnr.check());
    }

    /** */
    @Test
    public void testCertainIndex() throws Exception {
        assertNoAnyIndex("SELECT /*+ NO_INDEX */ * FROM TBL2 WHERE val2='v'");
        assertNoAnyIndex("SELECT * FROM TBL2 /*+ NO_INDEX */ WHERE val2='v'");

        // Checks lower-case idx name.
        assertCertainIndex("SELECT /*+ NO_INDEX('idx1_1') */ * FROM TBL1 WHERE val1='v'", "TBL1", "IDX1_1");
        assertCertainIndex("SELECT * FROM TBL1 /*+ NO_INDEX('idx1_1') */ WHERE val1='v'", "TBL1", "IDX1_1");

        // Without quotes, Calcite's parser makes lower-case upper.
        assertNoCertainIndex("SELECT /*+ NO_INDEX(idx1_1) */ * FROM TBL1 WHERE val1='v'", "TBL1", "IDX1_1");

        assertCertainIndex("SELECT /*+ NO_INDEX(" + QueryUtils.PRIMARY_KEY_INDEX +
            ") */ * FROM TBL1 WHERE id = 0", "TBL1", QueryUtils.PRIMARY_KEY_INDEX);
        assertNoCertainIndex("SELECT /*+ NO_INDEX('" + QueryUtils.PRIMARY_KEY_INDEX + "') */ * FROM TBL1 WHERE id = 0",
            "TBL1", QueryUtils.PRIMARY_KEY_INDEX);

        assertNoAnyIndex("SELECT /*+ NO_INDEX('idx1_1','IDX1_1','IDX1_23','IDX1_3') */ * FROM TBL1 WHERE val1='v' " +
            "and val2='v' and val3='v'");

        // Wrong names should be just skipped.
        assertNoAnyIndex("SELECT " +
            "/*+ NO_INDEX('UNEXISTING','idx1_1','UNEXISTING2','IDX1_1','UNEXISTING3','IDX1_23','IDX1_3') */ * " +
            "FROM TBL1 WHERE val1='v' and val2='v' and val3='v'");

        // Dedicated hint for each index.
        assertNoAnyIndex("SELECT /*+ NO_INDEX('idx1_1'), NO_INDEX(IDX1_1), NO_INDEX(IDX1_23), NO_INDEX(IDX1_3) */ * " +
            "FROM TBL1 WHERE val1='v' and val2='v' and val3='v'");
        assertNoAnyIndex("SELECT /*+ NO_INDEX('idx1_1'), NO_INDEX(IDX1_1) */ * FROM TBL1 " +
            "/*+ NO_INDEX(IDX1_23), NO_INDEX(IDX1_3) */  WHERE val1='v' and val2='v' and val3='v'");

        // Index of the second table.
        assertNoCertainIndex("SELECT /*+ NO_INDEX(IDX2_3) */ t1.val3, t2.val3 FROM TBL1 t1, TBL2 t2 WHERE " +
            "t1.val3='v' and t2.val3='v'", "TBL2", "IDX2_3");
        assertNoCertainIndex("SELECT t1.val3, t2.val3 FROM TBL1 t1, TBL2 /*+ NO_INDEX(IDX2_3) */ t2 WHERE " +
            "t1.val3='v' and t2.val3='v'", "TBL2", "IDX2_3");
        assertNoCertainIndex("SELECT /*+ NO_INDEX(IDX2_1) */ t1.val3, t2.val3 FROM TBL1  t1, " +
            "TBL2 /*+ NO_INDEX(IDX2_3) */ t2 WHERE t1.val3='v' and t2.val3='v' and t2.val1='v'", "TBL2", "IDX2_3");
    }

    /** */
    @Test
    public void testSecondQuery() throws Exception {
        assertNoCertainIndex("SELECT /*+ NO_INDEX(IDX1_23) */ * FROM TBL1 t1, (select * FROM TBL2 WHERE " +
            "val2='v') t2 WHERE t1.val2='v'", "TBL1", "IDX1_23");

        // Propagated, pushed-down hint.
        assertNoCertainIndex("SELECT /*+ NO_INDEX(IDX2_3) */ * FROM TBL1 t1, (select * FROM TBL2 WHERE " +
            "val3='v') t2 WHERE t1.val2='v'", "TBL2", "IDX2_3");

        // Not-root hint.
        assertNoCertainIndex("SELECT * FROM TBL1 t1, (select /*+ NO_INDEX(IDX2_3) */ * FROM TBL2 WHERE " +
            "val3='v') t2 WHERE t1.val2='v'", "TBL2", "IDX2_3");
        assertNoCertainIndex("SELECT * FROM TBL1 t1, (select * FROM TBL2 /*+ NO_INDEX(IDX2_3) */ WHERE " +
            "val3='v') t2 WHERE t1.val2='v'", "TBL2", "IDX2_3");
    }

    /** */
    @Test
    public void testCorrelatedSubquery() throws Exception {
        doTestDisabledInTable2Val3("t1.val3");
    }

    /** */
    @Test
    public void testSubquery() throws Exception {
        doTestDisabledInTable2Val3("'v'");
    }

    /** */
    @Test
    public void testOrderBy() throws Exception {
        assertNoCertainIndex("SELECT /*+ NO_INDEX(IDX1_23) */ val3 FROM TBL1 order by val2, val3", "TBL1", "IDX1_23");
        assertNoCertainIndex("SELECT val3 FROM TBL1 /*+ NO_INDEX(IDX1_23) */ order by val2, val3", "TBL1", "IDX1_23");
    }

    /** */
    @Test
    public void testAggregates() throws Exception {
        doTestAggregate("sum");
        doTestAggregate("avg");
        doTestAggregate("min");
        doTestAggregate("max");
    }

    /** */
    private void doTestAggregate(String op) throws Exception {
        assertNoCertainIndex("SELECT /*+ NO_INDEX(IDX2_3) */ " + op + "(val1) FROM TBL2 group by val3", "TBL2", "IDX2_3");

        assertNoCertainIndex("SELECT " + op + "(val1) FROM TBL2 /*+ NO_INDEX(IDX2_3) */ group by val3", "TBL2", "IDX2_3");

        assertNoAnyIndex("SELECT /*+ NO_INDEX */ " + op + "(val1) FROM TBL2 group by val3");

        assertNoAnyIndex("SELECT " + op + "(val1) FROM TBL2 /*+ NO_INDEX */ group by val3");
    }

    /** */
    @Test
    public void testJoins() throws Exception {
        assertNoAnyIndex("SELECT /*+ NO_INDEX */ t1.val1, t2.val2 FROM TBL1 t1, TBL2 t2 where " +
            "t2.val3=t1.val3");
        assertNoAnyIndex("SELECT t1.val1, t2.val2 FROM TBL1 /*+ NO_INDEX */ t1, TBL2 /*+ NO_INDEX */ t2 where " +
            "t2.val3=t1.val3");

        assertNoCertainIndex("SELECT /*+ NO_INDEX(IDX1_3,IDX2_3) */ t1.val1, t2.val2 FROM TBL1 t1, TBL2 t2 where " +
            "t2.val3=t1.val3", "TBL1", "IDX1_3");
        assertNoCertainIndex("SELECT /*+ NO_INDEX(IDX1_3,IDX2_3) */ t1.val1, t2.val2 FROM TBL1 t1, TBL2 t2 where " +
            "t2.val3=t1.val3", "TBL2", "IDX2_3");
        assertNoCertainIndex("SELECT /*+ NO_INDEX(IDX1_3) */ t1.val1, t2.val2 FROM TBL1 t1, " +
            "TBL2 /*+ NO_INDEX(IDX2_3) */ t2 where t2.val3=t1.val3", "TBL2", "IDX2_3");

        assertNoCertainIndex("SELECT /*+ NO_INDEX(IDX1_3) */ t1.val1, t2.val2 FROM TBL1 t1 JOIN TBL2 t2 on " +
            "t1.val3=t2.val3", "TBL1", "IDX1_3");

        assertNoCertainIndex("SELECT /*+ NO_INDEX(IDX2_3) */ t1.val1, t2.val2 FROM TBL1 t1 JOIN TBL2 t2 on " +
            "t1.val3=t2.val3", "TBL2", "IDX2_3");

        // With a filter
        assertNoCertainIndex("SELECT /*+ NO_INDEX(IDX2_2) */ t1.val1, t2.val2 FROM TBL1 t1, TBL2 t2 where t1.val2='v' " +
            "and t2.val2=t1.val2", "TBL2", "IDX2_2");
    }

    /** */
    @Test
    public void testUnion() throws Exception {
        doTestSetOps("UNION");
    }

    /** */
    @Test
    public void testIntersect() throws Exception {
        doTestSetOps("INTERSECT");
    }

    /** */
    private void doTestSetOps(String operation) throws Exception {
        assertPlan(String.format("SELECT /*+ NO_INDEX(IDX2_3) */ t1.* FROM TBL1 t1 where t1.val2='v' %s " +
                "SELECT t2.* FROM TBL2 t2 where t2.val3='v'", operation), schema,
            nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3"))
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1_23"))));

        assertPlan(String.format("SELECT /*+ NO_INDEX(IDX1_23) */ t1.* FROM TBL1 t1 where t1.val2='v' %s " +
                "SELECT t2.* FROM TBL2 t2 where t2.val3='v'", operation), schema,
            nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3"))
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1_23")).negate()));

        assertPlan(String.format("SELECT /*+ NO_INDEX */ t1.* FROM TBL1 t1 where t1.val2='v' %s " +
                "SELECT t2.* FROM TBL2 t2 where t2.val3='v'", operation), schema,
            nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3"))
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1_23")).negate()));

        assertPlan(String.format("SELECT t1.* FROM TBL1 /*+ NO_INDEX */ t1 where t1.val2='v' %s " +
                "SELECT t2.* FROM TBL2 t2 where t2.val3='v'", operation), schema,
            nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3"))
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1_23")).negate()));

        assertPlan(String.format("SELECT t1.* FROM TBL1 t1 where t1.val2='v' %s " +
                "SELECT /*+ NO_INDEX(IDX2_3) */ t2.* FROM TBL2 t2 where t2.val3='v'", operation), schema,
            nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1_23"))));

        assertPlan(String.format("SELECT t1.* FROM TBL1 t1 where t1.val2='v' %s " +
                "SELECT t2.* FROM TBL2 /*+ NO_INDEX(IDX2_3) */ t2 where t2.val3='v'", operation), schema,
            nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1_23"))));

        assertPlan(String.format("SELECT t1.* FROM TBL1 t1 where t1.val2='v' %s " +
                "SELECT /*+ NO_INDEX */ t2.* FROM TBL2 t2 where t2.val3='v'", operation), schema,
            nodeOrAnyChild(isIndexScan("TBL2", "IDX2_3")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1_23"))));

        assertNoCertainIndex(String.format("SELECT /*+ NO_INDEX(IDX1_23) */ t1.* FROM TBL1 t1 where t1.val2='v' %s " +
            "SELECT /*+ NO_INDEX(IDX2_3) */ t2.* FROM TBL2 t2 where t2.val3='v'", operation), "TBL1", "IDX1_23");
        assertNoCertainIndex(String.format("SELECT /*+ NO_INDEX(IDX1_23) */ t1.* FROM TBL1 t1 where t1.val2='v' %s " +
            "SELECT /*+ NO_INDEX(IDX2_3) */ t2.* FROM TBL2 t2 where t2.val3='v'", operation), "TBL2", "IDX2_3");
    }

    /**
     * Tests whether index 'IDX3' of table 'TBL2' in subquery of query to 'TBL1' is disabled.
     *
     * @param valueOfT2Val3 Value to use in 'WHERE TBL2.val2=' in the subquery. Can refer to 'TBL1'.
     */
    private void doTestDisabledInTable2Val3(String valueOfT2Val3) throws Exception {
        assertNoCertainIndex("SELECT /*+ NO_INDEX(IDX1_23) */ * FROM TBL1 t1 WHERE t1.val2 = " +
            "(SELECT val2 from TBL2 WHERE val3=" + valueOfT2Val3 + ')', "TBL1", "IDX1_23");

        assertNoCertainIndex("SELECT /*+ NO_INDEX(IDX2_3) */ * FROM TBL1 t1 WHERE t1.val2 = " +
            "(SELECT val2 from TBL2 WHERE val3=" + valueOfT2Val3 + ')', "TBL2", "IDX2_3");

        assertNoCertainIndex("SELECT * FROM TBL1 t1 WHERE t1.val2 = " +
            "(SELECT /*+ NO_INDEX(IDX2_3) */ val2 from TBL2 WHERE val3=" + valueOfT2Val3 + ')', "TBL2", "IDX2_3");

        assertCertainIndex("SELECT /*+ NO_NL_JOIN */ t2.val3 FROM TBL2 t2 WHERE t2.val2 = " +
            "(SELECT /*+ NO_INDEX(IDX2_2) */ t1.val2 from TBL1 t1 WHERE t1.val3=" + valueOfT2Val3 + ')', "TBL2", "IDX2_2");

        assertNoAnyIndex("SELECT /*+ NO_INDEX(IDX1_3), NO_INDEX(IDX2_3) */ * FROM TBL1 t1 WHERE t1.val3 = " +
            "(SELECT val2 from TBL2 WHERE val3=" + valueOfT2Val3 + ')');
        assertNoAnyIndex("SELECT /*+ NO_INDEX(IDX1_3) */ * FROM TBL1 t1 WHERE t1.val3 = " +
            "(SELECT val2 from TBL2 /*+ NO_INDEX */  WHERE val3=" + valueOfT2Val3 + ')');
        assertNoAnyIndex("SELECT /*+ NO_INDEX(IDX1_3, IDX2_3) */ * FROM TBL1 t1 WHERE t1.val3 = " +
            "(SELECT val2 from TBL2 WHERE val3=" + valueOfT2Val3 + ')');
        assertNoAnyIndex("SELECT /*+ NO_INDEX */ * FROM TBL1 t1 WHERE t1.val3 = " +
            "(SELECT val2 from TBL2 WHERE val3=" + valueOfT2Val3 + ')');
        assertNoAnyIndex("SELECT /*+ NO_INDEX(IDX1_3) */ * FROM TBL1 t1 WHERE t1.val3 = " +
            "(SELECT /*+ NO_INDEX(IDX2_3) */ val2 from TBL2 WHERE val3=" + valueOfT2Val3 + ')');
        assertNoAnyIndex("SELECT /*+ NO_INDEX(IDX1_3) */ * FROM TBL1 t1 WHERE t1.val3 = " +
            "(SELECT val2 from TBL2 /*+ NO_INDEX(IDX2_3) */ WHERE val3=" + valueOfT2Val3 + ')');
    }

    /** */
    private void assertNoAnyIndex(String sql) throws Exception {
        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteIndexScan.class)).negate());
    }

    /** */
    private void assertNoCertainIndex(String sql, String tblName, String idxName) throws Exception {
        assertPlan(sql, schema, nodeOrAnyChild(isIndexScan(tblName, idxName)).negate());
    }

    /** */
    private void assertCertainIndex(String sql, String tblName, String idxName) throws Exception {
        assertPlan(sql, schema, nodeOrAnyChild(isIndexScan(tblName, idxName)));
    }
}
