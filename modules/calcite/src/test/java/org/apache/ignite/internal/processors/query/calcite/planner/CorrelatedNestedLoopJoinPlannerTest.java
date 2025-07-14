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

import java.util.List;
import java.util.function.Predicate;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.junit.Test;

import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

/**
 *
 */
@SuppressWarnings({"TooBroadScope", "FieldCanBeLocal", "TypeMayBeWeakened"})
public class CorrelatedNestedLoopJoinPlannerTest extends AbstractPlannerTest {
    /**
     * Check equi-join. CorrelatedNestedLoopJoinTest is applicable for it.
     */
    @Test
    public void testValidIndexExpressions() throws Exception {
        IgniteSchema publicSchema = createSchema(
            testTable("T0"),
            testTable("T1").addIndex("t1_jid_idx", 1, 0));

        String sql = "select * " +
            "from t0 " +
            "join t1 on t0.jid = t1.jid";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            "MergeJoinConverter", "NestedLoopJoinConverter"
        );

        System.out.println("+++ " + RelOptUtil.toString(phys));

        assertNotNull(phys);

        checkSplitAndSerialization(phys, publicSchema);

        IgniteIndexScan idxScan = findFirstNode(phys, byClass(IgniteIndexScan.class));

        List<SearchBounds> searchBounds = idxScan.searchBounds();

        assertNotNull("Invalid plan\n" + RelOptUtil.toString(phys), searchBounds);
        assertEquals(3, searchBounds.size());

        assertNull(searchBounds.get(0));
        assertTrue(searchBounds.get(1) instanceof ExactBounds);
        assertTrue(((ExactBounds)searchBounds.get(1)).bound() instanceof RexFieldAccess);
        assertNull(searchBounds.get(2));
    }

    /**
     * Check join with not equi condition.
     * Current implementation of the CorrelatedNestedLoopJoinTest is not applicable for such case.
     */
    @Test
    public void testInvalidIndexExpressions() throws Exception {
        IgniteSchema publicSchema = createSchema(
            testTable("T0").addIndex("t0_jid_idx", 1, 0),
            testTable("T1").addIndex("t1_jid_idx", 1, 0));

        String sql = "select * " +
            "from t0 " +
            "join t1 on t0.jid + 2 > t1.jid * 2";

        IgniteRel phys = physicalPlan(
            sql,
            publicSchema,
            "MergeJoinConverter", "NestedLoopJoinConverter", "FilterSpoolMergeRule"
        );

        assertNotNull(phys);

        checkSplitAndSerialization(phys, publicSchema);
    }

    /** */
    @Test
    public void testFilterPushDown() throws Exception {
        IgniteSchema publicSchema = createSchema(
            testTable("T0"),
            testTable("T1"));

        String sql = "select * " +
                "from t0 " +
                "where t0.val > 'value' and exists (select * from t1 where t0.jid = t1.jid);";

        Predicate<RelNode> check =
            hasChildThat(isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
                .and(input(0, isTableScan("T0").and(n -> n.condition() != null))))
            .and(hasChildThat(isInstanceOf(IgniteFilter.class)).negate());

        assertPlan(sql, publicSchema, check);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinPushExpressionRule() throws Exception {
        IgniteSchema publicSchema = createSchema(
            createTable("EMP", 100, IgniteDistributions.broadcast(),
                "ID", INTEGER, "NAME", VARCHAR, "DEPTNO", INTEGER),
            createTable("DEPT", 10, IgniteDistributions.broadcast(),
                "DEPTNO", INTEGER, "NAME", VARCHAR)
        );

        String sql = "select /*+ CNL_JOIN */ d.deptno, e.deptno " +
            "from dept d, emp e " +
            "where d.deptno + e.deptno = 2";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteCorrelatedNestedLoopJoin.class)
            .and(cnlj -> "=(+($0, $1), 2)".equals(cnlj.getCondition().toString()))
            .and(cnlj -> cnlj.getJoinType() == JoinRelType.INNER)
            .and(cnlj -> cnlj.getVariablesSet().size() == 1)
            .and(input(0, isTableScan("DEPT")
                .and(t -> t.requiredColumns().equals(ImmutableBitSet.of(0)))
            ))
            .and(input(1, isTableScan("EMP")
                .and(t -> t.requiredColumns().equals(ImmutableBitSet.of(2)))
                .and(t -> "=(+($cor1.DEPTNO, $t0), 2)".equals(t.condition().toString()))
            ))
        );
    }

    /** */
    private TestTable testTable(String name) {
        return createTable(name, IgniteDistributions.broadcast(), "ID", Integer.class, "JID", Integer.class, "VAL", String.class);
    }
}
