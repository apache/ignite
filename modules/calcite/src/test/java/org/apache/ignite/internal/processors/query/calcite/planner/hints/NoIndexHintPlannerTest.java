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
import org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition;
import org.apache.ignite.internal.processors.query.calcite.planner.AbstractPlannerTest;
import org.apache.ignite.internal.processors.query.calcite.planner.TestTable;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

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
            .addIndex("IDX2_3", 2, 3)
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
        assertNoAnyIndex("SELECT /*+ NO_INDEX('PUBLIC.TBL1'='IDX2_3') */ * FROM TBL1 WHERE val2='v'");

        // Wrong schema name.
        assertCertainIndex("SELECT /*+ NO_INDEX('PUB.TBL1'='IDX2_3') */ * FROM TBL1 WHERE val2='v'",
            "TBL1", "IDX2_3");

        // Wrong schema name, then correct schema name.
        assertNoAnyIndex("SELECT /*+ NO_INDEX('PUB.TBL1'='IDX2'), NO_INDEX('PUBLIC.TBL1'='IDX2_3') */ * " +
            "FROM TBL1 WHERE val2='v'");

        assertNoAnyIndex("SELECT /*+ NO_INDEX('PUBLIC.TBL1'='IDX2_3', 'PUBLIC.TBL2'='IDX3') */ t1.val2, t2.val3 " +
            "FROM TBL1 t1, TBL2 t2 WHERE t1.val2='v' and t2.val3='v'");
    }

    /** */
    @Test
    public void testWrongParams() {
        assertThrows(
            null,
            () -> assertPlan("SELECT /*+ NO_INDEX */ * FROM TBL1 WHERE val1='v'", schema, n -> true),
            Throwable.class,
            "Hint '" + HintDefinition.NO_INDEX.name() + "' needs at least one option."
        );
    }

    /** */
    @Test
    public void testCertainIndex() throws Exception {
        // Checks lower-case idx name.
        assertNoCertainIndex("SELECT /*+ NO_INDEX('idx1') */ * FROM TBL1 WHERE val1='v'", "TBL1", "idx1");
        assertCertainIndex("SELECT /*+ NO_INDEX('idx1') */ * FROM TBL1 WHERE val1='v'", "TBL1", "IDX1");

        // Without quotes, Calcite's parser makes lower-case upper.
        assertCertainIndex("SELECT /*+ NO_INDEX(idx1) */ * FROM TBL1 WHERE val1='v'", "TBL1", "idx1");
        assertNoCertainIndex("SELECT /*+ NO_INDEX(idx1) */ * FROM TBL1 WHERE val1='v'", "TBL1", "IDX1");
        assertCertainIndex("SELECT /*+ NO_INDEX(" + QueryUtils.PRIMARY_KEY_INDEX +
            ") */ * FROM TBL1 WHERE id = 0", "TBL1", QueryUtils.PRIMARY_KEY_INDEX);
        assertNoAnyIndex("SELECT /*+ NO_INDEX('" + QueryUtils.PRIMARY_KEY_INDEX + "') */ * FROM TBL1 WHERE id = 0");

        assertNoAnyIndex("SELECT /*+ NO_INDEX('idx1','IDX1','IDX2_3','IDX3') */ * FROM TBL1 WHERE val1='v' and " +
            "val2='v' and val3='v'");
        // Mixed with no-tbl-name hint.
        assertNoAnyIndex("SELECT /*+ NO_INDEX('idx1'), NO_INDEX(IDX1,IDX2_3,IDX3) */ * FROM TBL1 WHERE val1='v' " +
            "and val2='v' and val3='v'");
        // Dedicated hint for each index.
        assertNoAnyIndex("SELECT /*+ NO_INDEX('idx1'), NO_INDEX(IDX1), NO_INDEX(IDX2_3), NO_INDEX(IDX3) */ * " +
            "FROM TBL1 WHERE val1='v' and val2='v' and val3='v'");
        // Dedicated hint for each index with table name.
        assertNoAnyIndex("SELECT /*+ NO_INDEX(TBL1='idx1'), NO_INDEX(TBL1='IDX1'), NO_INDEX(TBL1='IDX2_3'), " +
            "NO_INDEX(TBL1='IDX3') */ * FROM TBL1 WHERE val1='v' and val2='v' and val3='v'");

        // HintOption recognizes dot-separated values.
        assertNoAnyIndex("SELECT /*+ NO_INDEX('idx1,IDX1,IDX2_3,IDX3') */ * FROM TBL1 WHERE val1='v' and " +
            "val2='v' and val3='v'");
        assertNoAnyIndex("SELECT /*+ NO_INDEX(TBL1='idx1,IDX1,IDX2_3,IDX3') */ * FROM TBL1 WHERE val1='v' and " +
            "val2='v' and val3='v'");
        assertNoAnyIndex("SELECT /*+ NO_INDEX(TBL1='idx1,IDX1'), NO_INDEX('IDX2_3,IDX3') */ * FROM TBL1 WHERE " +
            "val1='v' and val2='v' and val3='v'");

        assertPlan("SELECT /*+ NO_INDEX(TBL1='IDX3') */ t1.val3, t2.val3 FROM TBL1 t1, TBL2 t2 WHERE " +
                "t1.val3='v' and t2.val3='v'", schema,
            nodeOrAnyChild(isIndexScan("TBL1", "IDX3")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX3"))));
    }

    /** */
    @Test
    public void testSecondQuery() throws Exception {
        assertPlan("SELECT /*+ NO_INDEX(TBL1='IDX2_3') */ * FROM TBL1 t1, (select * FROM TBL2 WHERE " +
                "val2='v') t2 WHERE t1.val2='v'", schema,
            nodeOrAnyChild(isIndexScan("TBL1", "IDX2")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX2_2"))));

        assertPlan("SELECT /*+ NO_INDEX(TBL2='IDX3') */ * FROM TBL1 t1, (select * FROM TBL2 WHERE " +
            "val3='v') t2 WHERE t1.val2='v'", schema, nodeOrAnyChild(isIndexScan("TBL1", "IDX2_3"))
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
    public void testOrderBy() throws Exception {
        assertCertainIndex("SELECT * FROM TBL1 order by val3", "TBL1", "IDX3");

        assertNoAnyIndex("SELECT /*+ NO_INDEX(IDX2_3) */ * FROM TBL1 order by val2");

        assertNoAnyIndex("SELECT /*+ NO_INDEX(TBL1='IDX2_3') */ * FROM TBL1 order by val2");

        assertCertainIndex("SELECT * FROM TBL1 order by val2, val3", "TBL1", "IDX2_3");
        assertNoCertainIndex("SELECT /*+ NO_INDEX(IDX2_3) */ val3 FROM TBL1 order by val2, val3", "TBL1", "IDX2_3");
    }

    @Test
    /** */
    public void testAggregates() throws Exception {
        doTestAggregate("sum");
        doTestAggregate("avg");
        doTestAggregate("min");
        doTestAggregate("max");
    }

    /** */
    private void doTestAggregate(String op) throws Exception {
        assertCertainIndex("SELECT " + op + "(val1) FROM TBL2 group by val3", "TBL2", "IDX3");
        assertNoAnyIndex("SELECT /*+ NO_INDEX(IDX3) */ " + op + "(val1) FROM TBL2 group by val3");
        assertNoAnyIndex("SELECT /*+ NO_INDEX(TBL2='IDX3') */ " + op + "(val1) FROM TBL2 group by val3");

        assertCertainIndex("SELECT " + op + "(val1) FROM TBL1 group by val2, val3", "TBL1", "IDX2_3");
        assertNoAnyIndex("SELECT /*+ NO_INDEX(IDX2_3) */ " + op + "(val1) FROM TBL1 group by val2, val3");
        assertNoAnyIndex("SELECT /*+ NO_INDEX(TBL1='IDX2_3') */ " + op + "(val1) FROM TBL1 group by val2, val3");
    }

    @Test
    /** */
    public void testJoins() throws Exception {
        assertNoAnyIndex("SELECT /*+ NO_INDEX(IDX3) */ t1.val1, t2.val2 FROM TBL1 t1, TBL2 t2 where " +
            "t2.val3=t1.val3");

        assertNoAnyIndex("SELECT /*+ NO_INDEX(IDX3) */ t1.val1, t2.val2 FROM TBL1 t1 JOIN TBL2 t2 on " +
            "t1.val3=t2.val3");

        assertPlan("SELECT /*+ NO_INDEX(TBL1='IDX3') */ t1.val1, t2.val2 FROM TBL1 t1 JOIN TBL2 t2 on " +
            "t1.val3=t2.val3", schema, nodeOrAnyChild(isIndexScan("TBL1", "IDX3")).negate()
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX3"))));

        assertPlan("SELECT /*+ NO_INDEX(TBL2='IDX3') */ t1.val1, t2.val2 FROM TBL1 t1 JOIN TBL2 t2 on " +
            "t1.val3=t2.val3", schema, nodeOrAnyChild(isIndexScan("TBL1", "IDX3"))
            .and(nodeOrAnyChild(isIndexScan("TBL2", "IDX3")).negate()));
    }

    /** */
    @Test
    public void testJoinsWithOtherFilters() throws Exception {
        assertNoAnyIndex("SELECT /*+ NO_INDEX(IDX3) */ t1.val1, t2.val2 FROM TBL1 t1, TBL2 t2 where t1.val3='v' " +
            "and t2.val3=t1.val3 and t2.val3='v'");

        assertPlan("SELECT /*+ NO_INDEX('IDX2_2') */ t1.val1, t2.val2 FROM TBL1 t1, TBL2 t2 where t1.val2='v' " +
            "and t2.val2=t1.val2", schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX2_2")).negate()
            .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2_3"))));

        assertPlan("SELECT /*+ NO_INDEX('IDX2_3') */ t1.val1, t2.val2 FROM TBL1 t1 LEFT JOIN TBL2 t2 on " +
            "t1.val2=t2.val3", schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX3"))
            .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2_3")).negate()));

        assertPlan("SELECT /*+ NO_INDEX('IDX3') */ t1.val1, t2.val2 FROM TBL1 t1 RIGHT JOIN TBL2 t2 on " +
            "t1.val2=t2.val3", schema, nodeOrAnyChild(isIndexScan("TBL2", "IDX3")).negate()
            .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2_3"))));

        assertNoAnyIndex("SELECT /*+ NO_INDEX('IDX2_3', 'IDX3') */ t1.val1, t2.val2 FROM TBL1 t1 INNER JOIN " +
            "TBL2 t2 on  t1.val2=t2.val3");

        assertNoAnyIndex("SELECT /*+ NO_INDEX('IDX2_3'), NO_INDEX('IDX3') */ t1.val1, t2.val2 FROM TBL1 t1 " +
            "INNER JOIN TBL2 t2 on  t1.val2=t2.val3");

        assertNoAnyIndex("SELECT /*+ NO_INDEX(IDX2_3), NO_INDEX(TBL2='IDX3') */ t1.val1, t2.val2 FROM TBL1 t1 INNER " +
            "JOIN TBL2 t2 on  t1.val2=t2.val3");

        assertNoAnyIndex("SELECT /*+ NO_INDEX(TBL1='IDX2_3', TBL2='IDX3') */ t1.val1, t2.val2 FROM TBL1 t1 " +
            "INNER JOIN TBL2 t2 on  t1.val2=t2.val3");
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
        assertPlan(String.format("SELECT /*+ NO_INDEX(IDX3) */ t1.* FROM TBL1 t1 where t1.val2='v' %s " +
                "SELECT t2.* FROM TBL2 t2 where t2.val3='v'", operation), schema,
            nodeOrAnyChild(isIndexScan("TBL2", "IDX3")).negate()
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2_3"))));

        assertPlan(String.format("SELECT /*+ NO_INDEX(IDX2_3) */ t1.* FROM TBL1 t1 where t1.val2='v' %s " +
                "SELECT t2.* FROM TBL2 t2 where t2.val3='v'", operation), schema,
            nodeOrAnyChild(isIndexScan("TBL2", "IDX3"))
                .and(nodeOrAnyChild(isIndexScan("TBL1", "IDX2_3")).negate()));

        assertNoAnyIndex(String.format("SELECT /*+ NO_INDEX(IDX2_3,IDX3) */ t1.* FROM TBL1 t1 where t1.val2='v' %s " +
                "SELECT t2.* FROM TBL2 t2 where t2.val3='v'", operation));

        assertNoAnyIndex(String.format("SELECT /*+ NO_INDEX(TBL1='IDX2_3',TBL2='IDX3') */ t1.* FROM TBL1 t1 where " +
            "t1.val2='v' %s SELECT t2.* FROM TBL2 t2 where t2.val3='v'", operation));

        assertNoAnyIndex(String.format("SELECT /*+ NO_INDEX(IDX2_3), NO_INDEX(IDX3) */ t1.* FROM TBL1 " +
            "t1 where t1.val2='v' %s SELECT t2.* FROM TBL2 t2 where t2.val3='v'", operation));

        assertNoAnyIndex(String.format("SELECT /*+ NO_INDEX(TBL1='IDX2_3'), NO_INDEX(TBL2='IDX3') */ t1.* FROM TBL1 " +
            "t1 where t1.val2='v' %s SELECT t2.* FROM TBL2 t2 where t2.val3='v'", operation));
    }

    /**
     * Tests whether index 'IDX3' of table 'TBL2' in subquery of query to 'TBL1' is disabled.
     *
     * @param valueOfT2Val3 Value to use in 'WHERE TBL2.val2=' in the subquery. Can refer to 'TBL1'.
     */
    private void doTestDisabledInTable2Val3(String valueOfT2Val3) throws Exception {
        assertCertainIndex("SELECT * FROM TBL1 t1 WHERE t1.val2 = (SELECT val2 from TBL2 WHERE val3=" +
            valueOfT2Val3 + ')', "TBL2", "IDX3");

        assertNoCertainIndex("SELECT /*+ NO_INDEX(TBL1='IDX2_3') */ * FROM TBL1 t1 WHERE t1.val2 = " +
            "(SELECT val2 from TBL2 WHERE val3=" + valueOfT2Val3 + ')', "TBL1", "IDX2_3");

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
