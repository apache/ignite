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
            .addIndex("idx1", 1)
            .addIndex("IDX1", 1)
            .addIndex("IDX2", 2)
            .addIndex("IDX3", 3);

        tbl2 = createTable("TBL2", 100_000, IgniteDistributions.single(), "ID", Integer.class,
            "VAL1", String.class, "VAL2", String.class, "VAL3", String.class)
            .addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0)
            .addIndex("idx1", 1)
            .addIndex("IDX1", 1)
            .addIndex("IDX2_2", 2)
            .addIndex("IDX3", 3);

        schema = createSchema(tbl1, tbl2);
    }

    /** */
    @Test
    public void testWithTableAndSchemaName() throws Exception {
        assertNoAnyIndex("SELECT /*+ NO_INDEX('PUBLIC.TBL1'='IDX2') */ * FROM TBL1 WHERE val2='v'");

        assertCertainIndex("SELECT /*+ NO_INDEX('PUB.TBL1'='IDX2') */ * FROM TBL1 WHERE val2='v'",
            "TBL1", "IDX2");

        assertNoAnyIndex("SELECT /*+ NO_INDEX('PUB.TBL1'='IDX2'), NO_INDEX('PUBLIC.TBL1'='IDX2') */ * FROM TBL1 " +
            "WHERE val2='v'");

        assertNoAnyIndex("SELECT /*+ NO_INDEX('PUBLIC.TBL1'='IDX2', 'PUBLIC.TBL2'='IDX3') */ t1.val2, t2.val3 " +
            "FROM TBL1 t1, TBL2 t2 WHERE t1.val2='v' and t2.val3='v'");
    }

    /** */
    @Test
    public void testCertainIndex() throws Exception {
        // Checks lower-case idx name.
        assertNoCertainIndex("SELECT /*+ NO_INDEX('idx1') */ * FROM TBL1 WHERE val1='v'", "TBL1", "idx1");
        assertCertainIndex("SELECT /*+ NO_INDEX('idx1') */ * FROM TBL1 WHERE val1='v'", "TBL1", "IDX1");

        // Checks primary index.
        assertNoCertainIndex("SELECT /*+ NO_INDEX('" + QueryUtils.PRIMARY_KEY_INDEX +
            "') */ * FROM TBL1 WHERE id = 0", "TBL1", QueryUtils.PRIMARY_KEY_INDEX);

        assertNoAnyIndex("SELECT /*+ NO_INDEX('idx1', 'IDX1', 'IDX2') */ * FROM TBL1 WHERE val1='v' and val2='v'");

        assertPlan("SELECT /*+ NO_INDEX(TBL1='IDX3') */ t1.val3, t2.val3 FROM TBL1 t1, TBL2 t2 WHERE " +
                "t1.val3='v' and t2.val3='v'", schema,
            nodeOrAnyChild(isIndexScan("TBL1", "IDX3")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX3"))));
    }

    /** */
    @Test
    public void testSecondQuery() throws Exception {
        assertNoAnyIndex("SELECT /*+ NO_INDEX */ * FROM TBL1 t1, (select * FROM TBL2 WHERE val3='v') t2" +
            " WHERE t1.val2='v'");

        assertPlan("SELECT /*+ NO_INDEX(TBL1='IDX2') */ * FROM TBL1 t1, (select * FROM TBL2 WHERE " +
                "val2='v') t2 WHERE t1.val2='v'", schema,
            nodeOrAnyChild(isIndexScan("TBL1", "IDX2")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX2_2"))));

        assertPlan("SELECT /*+ NO_INDEX(TBL2='IDX3') */ * FROM TBL1 t1, (select * FROM TBL2 WHERE " +
            "val3='v') t2 WHERE t1.val2='v'", schema, nodeOrAnyChild(isIndexScan("TBL1", "IDX2"))
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX3")).negate()));
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
    public void testJoins() throws Exception {
        assertNoAnyIndex("SELECT /*+ NO_INDEX */ t1.*, t2.* FROM TBL1 t1, TBL2 t2 where t1.val2='v' and " +
            "t2.val3=t1.val3 and t2.val2='v'");

        assertNoAnyIndex("SELECT /*+ NO_INDEX(IDX3) */ t1.*, t2.* FROM TBL1 t1, TBL2 t2 where t1.val3='v' and " +
            "t2.val3=t1.val3 and t2.val3='v'");

        assertPlan("SELECT /*+ NO_INDEX('IDX2_2') */ t1.*, t2.* FROM TBL1 t1, TBL2 t2 where t1.val2='v' and " +
            "t2.val2=t1.val2", schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX2_2")).negate()
            .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2"))));

        assertPlan("SELECT /*+ NO_INDEX('IDX2') */ t1.*, t2.* FROM TBL1 t1 LEFT JOIN TBL2 t2 on t1.val2=t2.val3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX3"))
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2")).negate()));

        assertPlan("SELECT /*+ NO_INDEX('IDX3') */ t1.*, t2.* FROM TBL1 t1 RIGHT JOIN TBL2 t2 on t1.val2=t2.val3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX3")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2"))));

        assertNoAnyIndex("SELECT /*+ NO_INDEX('IDX2', 'IDX3') */ t1.*, t2.* FROM TBL1 t1 INNER JOIN TBL2 t2 on " +
            "t1.val2=t2.val3");
    }

    /** */
    @Test
    public void testUnion() throws Exception {
        doTestUnions("UNION");
    }

    /** */
    @Test
    public void testIntersect() throws Exception {
        doTestUnions("INTERSECT");
    }

    /** */
    private void doTestUnions(String operation) throws Exception {
        assertNoAnyIndex(String.format("SELECT /*+ NO_INDEX */ t1.* FROM TBL1 t1 where t1.val2='v' %s " +
            "SELECT t2.* FROM TBL2 t2 where t2.val3='v'", operation));

        assertPlan(String.format("SELECT /*+ NO_INDEX('IDX3') */ t1.* FROM TBL1 t1 where t1.val2='v' %s " +
                "SELECT t2.* FROM TBL2 t2 where t2.val3='v'", operation), schema,
            nodeOrAnyChild(isIndexScan("TBL2", "IDX3")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2"))));
    }

    /**
     * Tests whether index 'IDX3' of table 'TBL2' in subquery of query to 'TBL1' is disabled.
     *
     * @param valueOfT2Val3 Value to use in 'WHERE TBL2.val2=' in the subquery. Can refer to 'TBL1'.
     */
    private void doTestDisabledInTable2Val3(String valueOfT2Val3) throws Exception {
        assertCertainIndex("SELECT * FROM TBL1 t1 WHERE t1.val2 = (SELECT val2 from TBL2 WHERE val3=" +
            valueOfT2Val3 + ')', "TBL2", "IDX3");

        assertNoCertainIndex("SELECT /*+ NO_INDEX */ * FROM TBL1 t1 WHERE t1.val2 = " +
            "(SELECT val2 from TBL2 WHERE val3=" + valueOfT2Val3 + ')', "TBL2", "IDX3");

        assertNoCertainIndex("SELECT /*+ NO_INDEX(TBL1='IDX2') */ * FROM TBL1 t1 WHERE t1.val2 = " +
            "(SELECT val2 from TBL2 WHERE val3=" + valueOfT2Val3 + ')', "TBL1", "IDX2");

        assertNoCertainIndex("SELECT /*+ NO_INDEX(TBL2='IDX3') */ * FROM TBL1 t1 WHERE t1.val2 = " +
            "(SELECT val2 from TBL2 WHERE val3=" + valueOfT2Val3 + ')', "TBL2", "IDX3");

        assertNoAnyIndex("SELECT /*+ NO_INDEX('IDX3') */ * FROM TBL1 t1 WHERE t1.val3 = " +
            "(SELECT val2 from TBL2 WHERE val3=" + valueOfT2Val3 + ')');
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
