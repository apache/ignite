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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.schema.TransientTable;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.IgniteScalarFunction;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;

import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.single;

/** Utilities shared by recursive CTE converter rules. */
final class RecursiveCteUtils {
    /** */
    private RecursiveCteUtils() {
        // No-op.
    }

    /** Returns whether the table is Calcite's query-local transient table. */
    static boolean isTransient(RelOptTable table) {
        return table != null && table.unwrap(TransientTable.class) != null;
    }

    /** Stable identifier preserved in the serialized physical plan. */
    static String stateId(RelOptPlanner planner, RelOptTable table) {
        PlanningContext ctx = planner.getContext().unwrap(PlanningContext.class);

        assert ctx != null;

        return ctx.recursiveCteStateId(table);
    }

    /** Counts scans of the recursive transient table. */
    static int referenceCount(RelNode rel, RelOptTable table) {
        rel = original(rel);

        int cnt = isRecursiveScan(rel, table) ? 1 : 0;

        for (RelNode input : rel.getInputs())
            cnt += referenceCount(input, table);

        return cnt;
    }

    /** Converts maximal inputs that do not depend on the current delta to coordinator-local rewindable execution. */
    static RelNode convertStaticInputs(RelNode rel, RelOptTable table, RelTraitSet traits) {
        rel = original(rel);

        if (isRecursiveScan(rel, table))
            return rel;

        List<RelNode> inputs = rel.getInputs();

        if (inputs.isEmpty())
            return rel;

        List<RelNode> newInputs = new ArrayList<>(inputs.size());

        for (RelNode input : inputs) {
            if (referenceCount(input, table) == 0 && isInvariant(input))
                newInputs.add(convertStaticInput(input, traits));
            else
                newInputs.add(convertStaticInputs(input, table, traits));
        }

        return rel.copy(rel.getTraitSet(), newInputs);
    }

    /** Converts an iteration-independent input to coordinator-local rewindable execution. */
    private static RelNode convertStaticInput(RelNode input, RelTraitSet traits) {
        IgniteDistribution inputDistribution =
            (IgniteDistribution)input.getCluster().getMetadataQuery().distribution(original(input));

        if (inputDistribution.satisfies(single()))
            return RelOptRule.convert(input, traits.replace(RewindabilityTrait.REWINDABLE));

        RelNode convertedInput = RelOptRule.convert(input, traits);

        return TraitUtils.convertRewindability(
            input.getCluster().getPlanner(),
            RewindabilityTrait.REWINDABLE,
            convertedInput
        );
    }

    /** Returns whether the subtree produces the same result on every recursive iteration. */
    private static boolean isInvariant(RelNode rel) {
        rel = original(rel);

        if (!RelOptUtil.getVariablesUsed(rel).isEmpty())
            return false;

        DeterminismChecker checker = new DeterminismChecker();

        rel.accept(checker);

        if (!checker.deterministic)
            return false;

        if (rel instanceof Aggregate) {
            for (AggregateCall call : ((Aggregate)rel).getAggCallList()) {
                if (!call.getAggregation().isDeterministic())
                    return false;
            }
        }

        for (RelNode input : rel.getInputs()) {
            if (!isInvariant(input))
                return false;
        }

        return true;
    }

    /**
     * Returns the logical expression represented by a Volcano subset. Converter rule inputs can be subsets whose own
     * input list is empty.
     */
    static RelNode original(RelNode rel) {
        while (rel instanceof RelSubset) {
            RelNode original = ((RelSubset)rel).getOriginal();

            if (original == null)
                return rel;

            rel = original;
        }

        return rel;
    }

    /** Returns whether both optimizer tables represent the same transient table instance. */
    static boolean sameTransientTable(RelOptTable first, RelOptTable second) {
        TransientTable firstTable = first == null ? null : first.unwrap(TransientTable.class);
        TransientTable secondTable = second == null ? null : second.unwrap(TransientTable.class);

        return firstTable != null && firstTable == secondTable;
    }

    /** */
    private static boolean isRecursiveScan(RelNode rel, RelOptTable table) {
        return rel instanceof TableScan
            && sameTransientTable(((TableScan)rel).getTable(), table);
    }

    /** Finds non-deterministic expressions in one relational node. */
    private static class DeterminismChecker extends RexShuttle {
        /** Whether all visited expressions are deterministic. */
        private boolean deterministic = true;

        /** {@inheritDoc} */
        @Override public RexNode visitCall(RexCall call) {
            if (!call.getOperator().isDeterministic() || isNonDeterministicUdf(call)) {
                deterministic = false;

                return call;
            }

            return super.visitCall(call);
        }

        /** {@inheritDoc} */
        @Override public RexNode visitSubQuery(RexSubQuery subQuery) {
            if (!isInvariant(subQuery.rel)) {
                deterministic = false;

                return subQuery;
            }

            return super.visitSubQuery(subQuery);
        }

        /** Returns whether the call targets an Ignite UDF declared as non-deterministic. */
        private static boolean isNonDeterministicUdf(RexCall call) {
            if (!(call.getOperator() instanceof SqlUserDefinedFunction))
                return false;

            Object function = ((SqlUserDefinedFunction)call.getOperator()).getFunction();

            return function instanceof IgniteScalarFunction
                && !((IgniteScalarFunction)function).isDeterministic();
        }
    }
}
