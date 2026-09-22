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

import java.math.BigDecimal;
import java.util.function.Predicate;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.RangeSet;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sarg;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.mockito.Mockito.mock;

/** {@link AbstractPlannerTest} inner utility methods test. */
public class AbstractPlannerUtilityTest extends AbstractPlannerTest {
    /** Tests correctness of {@link AbstractPlannerTest#satisfyCondition}. */
    @Test
    public void testFlatConditionNormalizer() {
        RelOptCluster cluster = Commons.emptyCluster();
        RexBuilder builder = cluster.getRexBuilder();

        RelDataType type = cluster.getTypeFactory().createJavaType(long.class);
        RexLiteral l0 = builder.makeExactLiteral(new BigDecimal(0));
        RexLocalRef lr0 = new RexLocalRef(0, type);

        RexLiteral l1 = builder.makeExactLiteral(new BigDecimal(0));
        RexLiteral l2 = builder.makeExactLiteral(new BigDecimal(2));
        RexLocalRef lr1 = new RexLocalRef(1, type);
        RexLocalRef lr100 = new RexLocalRef(100, type);

        RexLocalRef lr3 = new RexLocalRef(2, type);

        final RangeSet<@NotNull Integer> setNone = ImmutableRangeSet.of();
        final RangeSet<@NotNull Integer> setAll = setNone.complement();
        final Sarg<Integer> sarg =
            Sarg.of(RexUnknownAs.FALSE, setAll);
        RexNode rexNode =
            builder.makeCall(SqlStdOperatorTable.SEARCH, lr3,
                builder.makeSearchArgumentLiteral(sarg, cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER)));

        RexNode r0 = builder.makeCall(SqlStdOperatorTable.EQUALS, lr0, l0);
        RexNode r1 = builder.makeCall(SqlStdOperatorTable.EQUALS, lr1, l1);
        RexNode r2 = builder.makeCall(SqlStdOperatorTable.EQUALS, lr1, l2);

        {
            RexNode filter = builder.makeCall(SqlStdOperatorTable.OR, r1, r2, r0);
            assertEquals("OR(=($t1, 0), =($t1, 2), =($t0, 0))", filter.toString());

            ProjectableFilterableTableScan scan = buildNode(cluster, filter);

            Predicate<ProjectableFilterableTableScan> cond =
                satisfyCondition("OR(=($t0, 0), =($t1, 0), =($t1, 2))");

            boolean res = cond.test(scan);
            assertTrue(lastErrorMsg, res);
        }

        {
            RexNode filter = builder.makeCall(SqlStdOperatorTable.AND, r1, r0, rexNode);
            assertEquals("AND(=($t1, 0), =($t0, 0), SEARCH($t2, Sarg[IS NOT NULL]))", filter.toString());

            ProjectableFilterableTableScan scan = buildNode(cluster, filter);

            Predicate<ProjectableFilterableTableScan> cond =
                satisfyCondition("AND(=($t0, 0), =($t1, 0), SEARCH($t2, Sarg[IS NOT NULL]))");

            boolean res = cond.test(scan);
            assertTrue(lastErrorMsg, res);
        }

        {
            RexNode r100 = builder.makeCall(SqlStdOperatorTable.EQUALS, lr100, l1);
            RexNode filter = builder.makeCall(SqlStdOperatorTable.AND, r100, r0, rexNode);

            assertEquals("AND(=($t100, 0), =($t0, 0), SEARCH($t2, Sarg[IS NOT NULL]))", filter.toString());

            ProjectableFilterableTableScan scan = buildNode(cluster, filter);

            Predicate<ProjectableFilterableTableScan> cond =
                satisfyCondition("AND(=($t0, 0), =($t100, 0), SEARCH($t2, Sarg[IS NOT NULL]))");

            boolean res = cond.test(scan);
            assertTrue(lastErrorMsg, res);
        }

        {
            RexNode orOp = builder.makeCall(SqlStdOperatorTable.OR, r0, r1);
            RexNode filter = builder.makeCall(SqlStdOperatorTable.AND, orOp, rexNode);

            assertEquals("AND(OR(=($t0, 0), =($t1, 0)), SEARCH($t2, Sarg[IS NOT NULL]))", filter.toString());

            ProjectableFilterableTableScan scan = buildNode(cluster, filter);

            Predicate<ProjectableFilterableTableScan> cond =
                satisfyCondition("AND(OR(=($t0, 0), =($t1, 0)), SEARCH($t2, Sarg[IS NOT NULL]))");

            boolean res = cond.test(scan);

            assertTrue(lastErrorMsg, res);
        }
    }

    /** */
    private ProjectableFilterableTableScan buildNode(RelOptCluster cluster, RexNode cond) {
        RelDataType type = Commons.typeFactory().createType(int.class);

        return new IgniteIndexScan(
            cluster,
            RelTraitSet.createEmpty(),
            mock(RelOptTableImpl.class),
            null,
            type,
            null,
            cond,
            null,
            null,
            null
        );
    }
}
