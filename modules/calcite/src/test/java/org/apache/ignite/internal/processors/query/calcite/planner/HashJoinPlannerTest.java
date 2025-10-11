/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.util.List;
import org.apache.calcite.plan.RelOptPlanner.CannotPlanException;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** */
public class HashJoinPlannerTest extends AbstractPlannerTest {
    /** */
    private static final String[] DISABLED_RULES = {"NestedLoopJoinConverter", "CorrelatedNestedLoopJoin", "MergeJoinConverter",
        "JoinCommuteRule"};

    /** */
    private static final String[] JOIN_TYPES = {"LEFT", "RIGHT", "INNER", "FULL"};

    /** */
    @Test
    public void testHashJoinKeepsLeftCollation() throws Exception {
        TestTable tbl1 = createSimpleTable();
        TestTable tbl2 = createComplexTable();

        IgniteSchema schema = createSchema(tbl1, tbl2);

        String sql = "select t1.ID, t2.ID1 "
            + "from TEST_TBL_CMPLX t2 "
            + "join TEST_TBL t1 on t1.id = t2.id1 "
            + "order by t2.ID1 NULLS LAST, t2.ID2 NULLS LAST";

        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteHashJoin.class))
            .and(nodeOrAnyChild(isInstanceOf(IgniteSort.class)).negate()), DISABLED_RULES);
    }

    /** */
    @Test
    public void testHashJoinErasesRightCollation() throws Exception {
        TestTable tbl1 = createSimpleTable();
        TestTable tbl2 = createComplexTable();

        IgniteSchema schema = createSchema(tbl1, tbl2);

        String sql = "select t1.ID, t2.ID1 "
                + "from TEST_TBL t1 "
                + "join TEST_TBL_CMPLX t2 on t1.id = t2.id1 "
                + "order by t2.ID1 NULLS LAST, t2.ID2 NULLS LAST";

        assertPlan(sql, schema, nodeOrAnyChild(isInstanceOf(IgniteSort.class)
            .and(input(isInstanceOf(IgniteHashJoin.class)))), DISABLED_RULES);
    }

    /** */
    @Test
    public void testHashJoinWinsOnSkewedLeftInput() throws Exception {
        TestTable smallTbl = createSimpleTable("SMALL_TBL", 1000);
        TestTable largeTbl = createSimpleTable("LARGE_TBL", 500_000);

        IgniteSchema schema = createSchema(smallTbl, largeTbl);

        assertPlan(
            "select t1.ID, t1.INT_VAL, t2.ID, t2.INT_VAL from LARGE_TBL t1 join SMALL_TBL t2 on t1.INT_VAL = t2.INT_VAL",
            schema,
            nodeOrAnyChild(isInstanceOf(IgniteHashJoin.class)),
            "JoinCommuteRule"
        );

        assertPlan(
            "select t1.ID, t1.INT_VAL, t2.ID, t2.INT_VAL from SMALL_TBL t1 join LARGE_TBL t2 on t1.INT_VAL = t2.INT_VAL",
            schema,
            nodeOrAnyChild(isInstanceOf(IgniteHashJoin.class).negate()),
            "JoinCommuteRule"
        );

        // Merge join can consume less CPU resources.
        assertPlan(
            "select t1.ID, t1.INT_VAL, t2.ID, t2.INT_VAL from SMALL_TBL t1 join LARGE_TBL t2 on t1.ID = t2.ID",
            schema,
            nodeOrAnyChild(isInstanceOf(IgniteHashJoin.class).negate()),
            "JoinCommuteRule"
        );
    }

    /** */
    private static @Nullable IgniteSort sortOnTopOfJoin(IgniteRel root) {
        List<IgniteSort> sortNodes = findNodes(root, byClass(IgniteSort.class)
            .and(node -> node.getInputs().size() == 1 && node.getInput(0) instanceof Join));

        if (sortNodes.size() > 1)
            throw new IllegalStateException("Incorrect sort nodes number: expected 1, actual " + sortNodes.size());

        return sortNodes.isEmpty() ? null : sortNodes.get(0);
    }

    /** */
    @Test
    public void testHashJoinApplied() throws Exception {
        // Parms: request, can be planned, only INNER or SEMI join.
        List<List<Object>> testParams = F.asList(
            F.asList("select t1.c1 from t1 %s join t2 on t1.id = t2.id", true, false),
            F.asList("select t1.c1 from t1 %s join t2 on t1.id = t2.id and t1.c1=t2.c1", true, false),
            F.asList("select t1.c1 from t1 %s join t2 using(c1)", true, false),
            F.asList("select t1.c1 from t1 %s join t2 on t1.c1 = 1", false, false),
            F.asList("select t1.c1 from t1 %s join t2 ON t1.id is not distinct from t2.c1", true, false),
            F.asList("select t1.c1 from t1 %s join t2 ON t1.id is not distinct from t2.c1 and t1.c1 is not distinct from  t2.id",
                true, false),
            F.asList("select t1.c1 from t1 %s join t2 ON t1.id is not distinct from t2.c1 and t1.c1 = t2.id", false, false),
            F.asList("select t1.c1 from t1 %s join t2 ON t1.id is not distinct from t2.c1 and t1.c1 > t2.id", true, true),
            F.asList("select t1.c1 from t1 %s join t2 on t1.c1 = ?", false, false),
            F.asList("select t1.c1 from t1 %s join t2 on t1.c1 = OCTET_LENGTH('TEST')", false, false),
            F.asList("select t1.c1 from t1 %s join t2 on t1.c1 = LOG10(t1.c1)", false, false),
            F.asList("select t1.c1 from t1 %s join t2 on t1.c1 = t2.c1 and t1.ID > t2.ID", true, true),
            F.asList("select t1.c1 from t1 %s join t2 on t1.c1 = 1 and t2.c1 = 1", false, false)
        );

        for (List<Object> paramSet : testParams) {
            assert paramSet != null && paramSet.size() == 3;

            String sql = (String)paramSet.get(0);
            boolean canBePlanned = (Boolean)paramSet.get(1);
            boolean onlyInnerOrSemi = (Boolean)paramSet.get(2);

            TestTable tbl1 = createTable("T1", IgniteDistributions.single(), "ID", Integer.class, "C1", Integer.class);
            TestTable tbl2 = createTable("T2", IgniteDistributions.single(), "ID", Integer.class, "C1", Integer.class);

            IgniteSchema schema = createSchema(tbl1, tbl2);

            for (String joinType : JOIN_TYPES) {
                if (onlyInnerOrSemi && !joinType.equals("INNER") && !joinType.equals("SEMI"))
                    continue;

                String sql0 = String.format(sql, joinType);

                if (log.isInfoEnabled())
                    log.info("Testing query '" + sql0 + "' for join type " + joinType);

                if (canBePlanned)
                    assertPlan(sql0, schema, nodeOrAnyChild(isInstanceOf(IgniteHashJoin.class)), DISABLED_RULES);
                else {
                    assertThrows(null, () -> physicalPlan(sql0, schema, DISABLED_RULES), CannotPlanException.class,
                        "There are not enough rules");
                }
            }
        }
    }

    /** */
    private static TestTable createSimpleTable() {
        return createSimpleTable("TEST_TBL", DEFAULT_TBL_SIZE);
    }

    /** */
    private static TestTable createSimpleTable(String name, int size) {
        return createTable(
            name,
            size,
            IgniteDistributions.affinity(0, CU.cacheId("default"), 0),
            "ID", Integer.class,
            "INT_VAL", Integer.class,
            "STR_VAL", String.class
        ).addIndex(
            RelCollations.of(new RelFieldCollation(0, ASCENDING, RelFieldCollation.NullDirection.LAST)),
            "PK"
        );
    }

    /** */
    private static TestTable createComplexTable() {
        return createTable(
            "TEST_TBL_CMPLX",
            DEFAULT_TBL_SIZE,
            IgniteDistributions.affinity(ImmutableIntList.of(0, 1), CU.cacheId("default"), 0),
            "ID1", Integer.class,
            "ID2", Integer.class,
            "STR_VAL", String.class
        ).addIndex(
            RelCollations.of(
                new RelFieldCollation(0, ASCENDING, RelFieldCollation.NullDirection.LAST),
                new RelFieldCollation(1, ASCENDING, RelFieldCollation.NullDirection.LAST)
            ),
            "PK"
        );
    }
}
