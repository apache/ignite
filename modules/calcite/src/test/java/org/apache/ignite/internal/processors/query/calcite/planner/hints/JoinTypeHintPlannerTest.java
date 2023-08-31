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
import org.apache.ignite.internal.processors.query.calcite.rel.AbstractIgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

/**
 * Planner test for index hints.
 */
public class JoinTypeHintPlannerTest extends AbstractPlannerTest {
    /** */
    private IgniteSchema schema;

    /** {@inheritDoc} */
    @Override public void setup() {
        super.setup();

        TestTable[] tables = new TestTable[5];

        for (int i = 0; i < tables.length; ++i) {
            tables[i] = createTable("TBL" + (i + 1), (int)Math.min(1_000_000, Math.pow(10, i)),
                IgniteDistributions.broadcast(), "ID", Integer.class, "V1", Integer.class, "V2", Integer.class,
                "V3", Integer.class).addIndex(QueryUtils.PRIMARY_KEY_INDEX, 0);
        }

        schema = createSchema(tables);
    }

    /** */
    @Test
    public void testDisableNCL() throws Exception {
        doTestDisableJoinTypeWith("TBL2", "TBL1", "INNER", IgniteCorrelatedNestedLoopJoin.class,
            HintDefinition.NO_CNL_JOIN);

        doTestDisableJoinTypeWith("TBL1", "TBL2", "LEFT", IgniteCorrelatedNestedLoopJoin.class,
            HintDefinition.NO_CNL_JOIN);

        // RIGHT-join is not supported by correlated nested loop. But Calcite replaces join inputs and join type.
        doTestDisableJoinTypeWith("TBL2", "TBL1", "RIGHT", IgniteCorrelatedNestedLoopJoin.class,
            HintDefinition.NO_CNL_JOIN);

        LogListener lsnr = LogListener.matches("Hint 'NO_CNL_JOIN' was skipped. Reason: Correlated nested " +
                "loop is not supported for join type 'FULL'").build();

        lsnrLog.registerListener(lsnr);

        physicalPlan("SELECT /*+ " + HintDefinition.NO_CNL_JOIN + " */ t1.v1, t2.v2 FROM TBL1 t1 FULL JOIN " +
            "TBL2 t2 on t1.v3=t2.v3", schema);

        assertTrue(lsnr.check());
    }

    /** */
    @Test
    public void testDisableNL() throws Exception {
        doTestDisableJoinTypeWith("TBL3", "TBL1", "LEFT", IgniteNestedLoopJoin.class,
            HintDefinition.NO_NL_JOIN);

        doTestDisableJoinTypeWith("TBL1", "TBL3", "RIGHT", IgniteNestedLoopJoin.class,
            HintDefinition.NO_NL_JOIN);

        doTestDisableJoinTypeWith("TBL5", "TBL4", "INNER", IgniteNestedLoopJoin.class,
            HintDefinition.NO_NL_JOIN, "MergeJoinConverter");

        doTestDisableJoinTypeWith("TBL1", "TBL2", "FULL", IgniteNestedLoopJoin.class,
            HintDefinition.NO_NL_JOIN);
    }

    /** */
    private void doTestDisableJoinTypeWith(String tbl1, String tbl2, String sqlJoinType,
        Class<? extends AbstractIgniteJoin> joinRel, HintDefinition hint, String... disabledRules) throws Exception {
        String sqlTpl = String.format("SELECT %%s t1.v1, t2.v2 FROM %s t1 %s JOIN %s t2 on t1.v3=t2.v3", tbl1,
            sqlJoinType, tbl2);

        String hintPref = "/*+ " + hint.name();

        // No hint. Ensure target join type.
        assertPlan(String.format(sqlTpl, ""), schema, nodeOrAnyChild(isInstanceOf(joinRel)), disabledRules);

        // Hint with no options.
        assertPlan(String.format(sqlTpl, hintPref + " */"), schema, nodeOrAnyChild(isInstanceOf(joinRel)).negate(),
            disabledRules);

        // Hint with tbl1.
        assertPlan(String.format(sqlTpl, hintPref + '(' + tbl1 + " ) */"), schema,
            nodeOrAnyChild(isInstanceOf(joinRel)).negate(), disabledRules);

        // Hint with tbl2.
        assertPlan(String.format(sqlTpl, hintPref + '(' + tbl2 + " ) */"), schema,
            nodeOrAnyChild(isInstanceOf(joinRel)).negate(), disabledRules);

        // Hint with both tables.
        assertPlan(String.format(sqlTpl, hintPref + '(' + tbl2 + ',' + tbl1 + " ) */"), schema,
            nodeOrAnyChild(isInstanceOf(joinRel)).negate(), disabledRules);

        // Hint with wrong table.
        assertPlan(String.format(sqlTpl, hintPref + "('UNEXISTING') */"), schema,
            nodeOrAnyChild(isInstanceOf(joinRel)), disabledRules);

        // Hint with correct and incorrect tbl.
        assertPlan(String.format(sqlTpl, hintPref + '(' + tbl1 + ",UNEXISTING) */"), schema,
            nodeOrAnyChild(isInstanceOf(joinRel)).negate(), disabledRules);
    }
}
