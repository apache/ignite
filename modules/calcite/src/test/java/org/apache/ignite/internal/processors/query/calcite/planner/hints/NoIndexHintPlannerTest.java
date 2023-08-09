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
import org.junit.Ignore;
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

        tbl2 = createTable("TBL2", 100, IgniteDistributions.single(), "ID", Integer.class,
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
    public void testWithoutParams() throws Exception {
        assertNoAnyIndex("SELECT /*+ NO_INDEX */ * FROM TBL1 WHERE id = 0");
        assertNoAnyIndex("SELECT /*+ NO_INDEX */ * FROM TBL1 WHERE val1='v'");

        assertNoAnyIndex("SELECT /*+ NO_INDEX */ * FROM TBL1 WHERE id = 0 and val2='v'");

        assertNoAnyIndex("SELECT /*+ NO_INDEX */ t1.val1, t2.val1 FROM TBL1 t1, TBL2 t2 WHERE " +
            "t1.val2='v' AND t2.val1='v'");
    }

    /** */
    @Test
    public void testWithTableName() throws Exception {
        assertNoAnyIndex("SELECT /*+ NO_INDEX(TBL1='IDX2') */ * FROM TBL1 WHERE val2='v'");

        assertPlan("SELECT /*+ NO_INDEX(TBL1='idx1') */ * FROM TBL1 WHERE val1='v'",
            schema, nodeOrAnyChild(isIndexScan("TBL1", "IDX1"))
                .and(nodeOrAnyChild(isIndexScan("TBL1", "idx1")).negate()));

        assertPlan("SELECT /*+ NO_INDEX(TBL1='IDX3') */ t1.val3, t2.val3 FROM TBL1 t1, TBL2 t2 WHERE " +
            "t1.val3='v' and t2.val3='v'", schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX3"))
            .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX3")).negate()));

        assertNoAnyIndex("SELECT /*+ NO_INDEX(TBL1='IDX3'), NO_INDEX(TBL1='IDX2') */ * FROM TBL1 WHERE val2='v'");

        assertNoAnyIndex("SELECT /*+ NO_INDEX(TBL1='IDX3'), NO_INDEX */ * FROM TBL1 WHERE val2='v'");

        assertNoAnyIndex("SELECT /*+ NO_INDEX, NO_INDEX(TBL1='IDX3') */ * FROM TBL1 WHERE val2='v'");
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
        assertNoCertainIndex("SELECT /*+ NO_INDEX('idx1') */ * FROM TBL1 WHERE val1='v'", "TBL1", "idx1");

        assertNoCertainIndex("SELECT /*+ NO_INDEX('" + QueryUtils.PRIMARY_KEY_INDEX +
            "') */ * FROM TBL1 WHERE id = 0", "TBL1", QueryUtils.PRIMARY_KEY_INDEX);

        assertNoCertainIndex("SELECT /*+ NO_INDEX('IDX1') */ * FROM TBL1 WHERE val1='v'", "TBL1",
            "IDX1");
        assertCertainIndex("SELECT /*+ NO_INDEX('IDX1') */ * FROM TBL1 WHERE val1='v'", "TBL1", "idx1");

        assertNoCertainIndex("SELECT /*+ NO_INDEX('idx2') */ * FROM TBL1 WHERE val2='v'", "TBL1",
            "idx2");
        assertCertainIndex("SELECT /*+ NO_INDEX('idx2') */ * FROM TBL1 WHERE val2='v'", "TBL1", "IDX2");

        assertNoCertainIndex("SELECT /*+ NO_INDEX('idx1') */ * FROM TBL1 WHERE val1='v' and " +
            "val2='v'", "TBL1", "idx1");
        assertNoCertainIndex("SELECT /*+ NO_INDEX('idx1', 'IDX1') */ * FROM TBL1 WHERE val1='v' and " +
            "val2='v'", "TBL1", "IDX1");
        assertCertainIndex("SELECT /*+ NO_INDEX('idx1', 'IDX1') */ * FROM TBL1 WHERE val1='v' and " +
            "val2='v'", "TBL1", "IDX2");
    }

    /** */
    @Test
    public void testCertainIndexOtherTable() throws Exception {
        assertPlan("SELECT /*+ NO_INDEX('idx1') */ t1.val1, t2.val1 FROM TBL1 t1, TBL2 t2 WHERE " +
                "t1.val1='v' and t2.val1='v'", schema,
            nodeOrAnyChild(isIndexScan("TBL1", "idx1")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL2", "idx1")).negate())
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX1")))
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX1"))));

        assertPlan("SELECT /*+ NO_INDEX('IDX2') */ t1.val2, t2.val2 FROM TBL1 t1, TBL2 t2 WHERE " +
                "t1.val2='v' and t2.val2='v'", schema,
            nodeOrAnyChild(isIndexScan("TBL1", "IDX2")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX2_2"))));

        assertPlan("SELECT /*+ NO_INDEX(TBL1='IDX3') */ t1.val3, t2.val3 FROM TBL1 t1, TBL2 t2 WHERE " +
                "t1.val3='v' and t2.val3='v'", schema,
            nodeOrAnyChild(isIndexScan("TBL1", "IDX3")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX3"))));
    }

    /** */
    @Test
    public void testWithSubquery() throws Exception {
//        assertNoAnyIndex("SELECT /*+ NO_INDEX */ * FROM TBL1 t1, (select * FROM TBL2 WHERE val3='v') t2" +
//            " WHERE t1.val2='v'");
//
//        assertNoAnyIndex("SELECT /*+ NO_INDEX */ * FROM TBL1 t1, (select /*+ NO_INDEX(TBL2='IDX2') */ * FROM " +
//            "TBL2 WHERE val3='v') t2 WHERE t1.val2='v'");
//
//        assertPlan("SELECT /*+ NO_INDEX(TBL1='IDX2') */ * FROM TBL1 t1, (select * FROM TBL2 WHERE " +
//                "val3='v') t2 WHERE t1.val2='v'", schema,
//            nodeOrAnyChild(isIndexScan("TBL1", "IDX2")).negate()
//                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX3"))));
//
//        assertPlan("SELECT * FROM TBL1 t1, (select /*+ NO_INDEX */ * FROM TBL2 WHERE " +
//            "val3='v') t2 WHERE t1.val2='v'", schema, nodeOrAnyChild(isIndexScan("TBL1", "IDX2"))
//            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX3")).negate()));
//
//        assertPlan("SELECT * FROM TBL1 t1, (select /*+ NO_INDEX(TBL1='IDX2') */ * FROM TBL2 WHERE " +
//            "val3='v') t2 WHERE t1.val2='v'", schema, nodeOrAnyChild(isIndexScan("TBL1", "IDX2"))
//            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX3"))));
//
//        assertPlan("SELECT /*+ NO_INDEX(TBL2='IDX3') */ * FROM TBL1 t1, (select * FROM TBL2 WHERE " +
//            "val3='v') t2 WHERE t1.val2='v'", schema, nodeOrAnyChild(isIndexScan("TBL1", "IDX2"))
//            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX3")).negate()));
//
//        assertPlan("SELECT /*+ NO_INDEX(TBL2='IDX3') */ * FROM TBL1 t1, (select /*+ NO_INDEX(TBL2='IDX2_2') */ * " +
//                "FROM TBL2 WHERE val3='v') t2 WHERE t1.val2='v'", schema,
//            nodeOrAnyChild(isIndexScan("TBL1", "IDX2"))
//                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX3")).negate()));
//
//        assertPlan("SELECT /*+ NO_INDEX(TBL2='IDX3') */ * FROM TBL1 t1, (select /*+ NO_INDEX(TBL2='IDX2_2') */ * " +
//                "FROM TBL2 WHERE val3='v' and val2='v') t2 WHERE t1.val2='v'", schema,
//            nodeOrAnyChild(isIndexScan("TBL1", "IDX2"))
//                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX3")).negate())
//                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX2_2")).negate()));

        assertPlan("SELECT /*+ NO_INDEX */ * FROM TBL1 t1 WHERE t1.val2 in (SELECT /*+ NO_INDEX */ val2 from TBL2 " +
                "WHERE val3='v')", schema, nodeOrAnyChild(isIndexScan("TBL1", "IDX2"))
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX3")).negate()));
    }

    /** */
    @Test
    public void testWithSubqueryOfSameTable() throws Exception {
        assertNoAnyIndex("SELECT /*+ NO_INDEX */ * FROM TBL1 t1, (select * FROM TBL1 WHERE val3='v') t2" +
            " WHERE t1.val2='v'");

        assertNoAnyIndex("SELECT /*+ NO_INDEX */ * FROM TBL1 t1, (select /*+ NO_INDEX(TBL1='IDX2') */ * FROM " +
            "TBL1 WHERE val3='v') t2 WHERE t1.val2='v'");

        assertPlan("SELECT /*+ NO_INDEX(TBL1='IDX2') */ * FROM TBL1 t1, (select * FROM TBL1 WHERE " +
                "val3='v') t2 WHERE t1.val2='v'", schema,
            nodeOrAnyChild(isIndexScan("TBL1", "IDX2")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX3"))));

        assertPlan("SELECT * FROM TBL1 t1, (select /*+ NO_INDEX */ * FROM TBL1 WHERE " +
            "val3='v') t2 WHERE t1.val2='v'", schema, nodeOrAnyChild(isIndexScan("TBL1", "IDX2"))
            .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX3")).negate()));

        assertPlan("SELECT /*+ NO_INDEX(TBL1='IDX3') */ * FROM TBL1 t1, (select * FROM TBL1 WHERE " +
            "val3='v') t2 WHERE t1.val2='v'", schema, nodeOrAnyChild(isIndexScan("TBL1", "IDX2"))
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX3")).negate()));

        assertNoAnyIndex("SELECT /*+ NO_INDEX(TBL1='IDX2') */ * FROM TBL1 t1, " +
            "(select /*+ NO_INDEX(TBL1='IDX3') */ * FROM TBL1 WHERE val3='v') t2 WHERE t1.val2='v'");
    }

    /** */
    @Test
    @Ignore
    public void testWithCorrelated() throws Exception {
        assertNoAnyIndex("SELECT /*+ NO_INDEX */ val2, val3 FROM TBL1 t1 WHERE val1 = " +
            "(select /*+ NO_INDEX(TBL2='IDX3') */ t2.val2 FROM TBL2 t2 WHERE t2.val3=t1.val1)");

        assertNoAnyIndex("SELECT /*+ NO_INDEX */ t1.val2, (SELECT /*+ NO_INDEX */ val3 FROM TBL2 t2 where " +
            "val3=t1.val3) FROM TBL1 t1");
    }

    /** */
    @Test
    public void testUnion() throws Exception {
        assertNoAnyIndex("SELECT /*+ NO_INDEX */ t1.* FROM TBL1 t1 where t1.val2='v' UNION " +
            "SELECT /*+ NO_INDEX */ t2.* FROM TBL2 t2 where t2.val3='v'");

        assertNoAnyIndex("SELECT /*+ NO_INDEX(TBL1='IDX2') */ t1.* FROM TBL1 t1 where t1.val2='v' UNION " +
            "SELECT /*+ NO_INDEX(TBL2='IDX3') */ t2.* FROM TBL2 t2 where t2.val3='v'");

        assertPlan("SELECT /*+ NO_INDEX */ t1.* FROM TBL1 t1 where t1.val2='v' UNION " +
                "SELECT t2.* FROM TBL2 t2 where t2.val3='v'", schema,
            nodeOrAnyChild(isIndexScan("TBL2", "IDX3"))
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2")).negate()));

        assertPlan("SELECT t1.* FROM TBL1 t1 where t1.val2='v' UNION " +
                "SELECT /*+ NO_INDEX */ t2.* FROM TBL2 t2 where t2.val3='v'", schema,
            nodeOrAnyChild(isIndexScan("TBL2", "IDX3")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2"))));
    }

    /** */
    @Test
    public void testJoins() throws Exception {
        assertNoAnyIndex("SELECT /*+ NO_INDEX */ t1.*, t2.* FROM TBL1 t1, TBL2 t2 where t1.val2='v' and " +
            "t2.val3=t1.val3 and t2.val2='v'");

        assertPlan("SELECT /*+ NO_INDEX('IDX2') */ t1.*, t2.* FROM TBL1 t1, TBL2 t2 where t1.val2='v' and " +
            "t2.val3=t2.val3", schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX3"))
            .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2")).negate()));

        assertPlan("SELECT /*+ NO_INDEX('IDX2') */ t1.*, t2.* FROM TBL1 t1 LEFT JOIN TBL2 t2 on t1.val3=t2.val3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX3"))
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2")).negate()));

        assertPlan("SELECT /*+ NO_INDEX('IDX2') */ t1.*, t2.* FROM TBL1 t1 LEFT JOIN TBL2 t2 on t1.val3=t2.val3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX3"))
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2")).negate()));

        assertPlan("SELECT /*+ NO_INDEX('IDX2') */ t1.*, t2.* FROM TBL1 t1 INNER JOIN TBL2 t2 on t1.val3=t2.val3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX3"))
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2")).negate()));

        assertPlan("SELECT /*+ NO_INDEX('IDX2') */ t1.*, t2.* FROM TBL1 t1 RIGHT JOIN TBL2 t2 on t1.val3=t2.val3",
            schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX3"))
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2")).negate()));
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
