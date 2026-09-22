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

package org.apache.ignite.internal.processors.query.calcite.rule;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.immutables.value.Value;

import static java.util.Objects.requireNonNull;

/**
 * Rewrites scalar subqueries in table function arguments to correlates.
 *
 * <p>This is a temporary backport of
 * <a href="https://issues.apache.org/jira/browse/CALCITE-7688">CALCITE-7688</a>.
 * Remove this rule and use Calcite's {@code CoreRules.TABLE_FUNCTION_SCAN_SCALAR_QUERY_TO_CORRELATE}
 * after upgrading to Calcite 1.43.
 */
@Value.Enclosing
public class TableFunctionScanScalarSubQueryRule
    extends RelRule<TableFunctionScanScalarSubQueryRule.Config> implements TransformationRule {
    /** */
    public static final TableFunctionScanScalarSubQueryRule INSTANCE = Config.DEFAULT.toRule();

    /** */
    private TableFunctionScanScalarSubQueryRule(Config cfg) {
        super(cfg);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        TableFunctionScan scan = call.rel(0);
        RexSubQuery subQry = requireNonNull(findScalarSubQuery(scan.getCall()));
        RelBuilder builder = call.builder();

        builder.push(subQry.rel);
        builder.aggregate(builder.groupKey(),
            builder.aggregateCall(SqlStdOperatorTable.SINGLE_VALUE, builder.field(0)));

        RelNode scalarVal = builder.build();
        CorrelationId correlationId = scan.getCluster().createCorrel();
        RexCorrelVariable correlationVar = (RexCorrelVariable)scan.getCluster().getRexBuilder()
            .makeCorrel(scalarVal.getRowType(), correlationId);
        RexNode target = scan.getCluster().getRexBuilder().makeFieldAccess(correlationVar, 0);
        RexNode newCall = scan.getCall().accept(new ReplaceSubQueryShuttle(subQry, target));
        TableFunctionScan newScan = (TableFunctionScan)scan.copy(
            scan.getTraitSet(),
            scan.getInputs(),
            newCall,
            scan.getElementType(),
            scan.getRowType(),
            scan.getColumnMappings()
        ).withHints(scan.getHints());

        RelNode correlate = LogicalCorrelate.create(
            scalarVal,
            newScan,
            ImmutableList.of(),
            correlationId,
            ImmutableBitSet.of(0),
            JoinRelType.INNER
        );

        builder.push(correlate);

        int scalarFieldCnt = scalarVal.getRowType().getFieldCount();

        builder.project(
            IntStream.range(0, scan.getRowType().getFieldCount())
                .mapToObj(i -> builder.field(scalarFieldCnt + i))
                .collect(Collectors.toList()),
            scan.getRowType().getFieldNames()
        );

        call.transformTo(builder.build());
    }

    /** Finds the first scalar subquery in the expression. */
    private static RexSubQuery findScalarSubQuery(RexNode node) {
        try {
            node.accept(ScalarSubQueryFinder.INSTANCE);

            return null;
        }
        catch (Util.FoundOne e) {
            return (RexSubQuery)e.getNode();
        }
    }

    /** Replaces one scalar subquery with a reference to the aggregate result. */
    private static class ReplaceSubQueryShuttle extends RexShuttle {
        /** Subquery to replace. */
        private final RexSubQuery subQry;

        /** Replacement expression. */
        private final RexNode replacement;

        /** */
        private ReplaceSubQueryShuttle(RexSubQuery subQry, RexNode replacement) {
            this.subQry = subQry;
            this.replacement = replacement;
        }

        /** {@inheritDoc} */
        @Override public RexNode visitSubQuery(RexSubQuery subQry) {
            return subQry.equals(this.subQry) ? replacement : subQry;
        }
    }

    /** Finds scalar subqueries without matching other subquery kinds. */
    private static class ScalarSubQueryFinder extends RexVisitorImpl<Void> {
        /** */
        private static final ScalarSubQueryFinder INSTANCE = new ScalarSubQueryFinder();

        /** */
        private ScalarSubQueryFinder() {
            super(true);
        }

        /** {@inheritDoc} */
        @Override public Void visitSubQuery(RexSubQuery subQry) {
            if (subQry.getKind() == SqlKind.SCALAR_QUERY)
                throw new Util.FoundOne(subQry);

            return super.visitSubQuery(subQry);
        }
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        /** */
        Config DEFAULT = ImmutableTableFunctionScanScalarSubQueryRule.Config.of()
            .withDescription("TableFunctionScanScalarSubQueryRule")
            .withOperandSupplier(b -> b.operand(TableFunctionScan.class)
                .predicate(scan -> findScalarSubQuery(scan.getCall()) != null)
                .anyInputs());

        /** {@inheritDoc} */
        @Override default TableFunctionScanScalarSubQueryRule toRule() {
            return new TableFunctionScanScalarSubQueryRule(this);
        }
    }
}
