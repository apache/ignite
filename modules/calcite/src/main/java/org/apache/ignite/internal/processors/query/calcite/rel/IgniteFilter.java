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

package org.apache.ignite.internal.processors.query.calcite.rel;

import java.util.List;
import java.util.Set;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitsAwareIgniteRel;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;

import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;

/**
 * Relational expression that iterates over its input
 * and returns elements for which <code>condition</code> evaluates to
 * <code>true</code>.
 *
 * <p>If the condition allows nulls, then a null value is treated the same as
 * false.</p>
 */
public class IgniteFilter extends Filter implements TraitsAwareIgniteRel {
    /**
     * Creates a filter.
     *
     * @param cluster   Cluster that this relational expression belongs to
     * @param traits    the traits of this rel
     * @param input     input relational expression
     * @param condition boolean expression which determines whether a row is
     *                  allowed to pass
     */
    public IgniteFilter(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexNode condition) {
        super(cluster, traits, input, condition);
    }

    /** */
    public IgniteFilter(RelInput input) {
        super(changeTraits(input, IgniteConvention.INSTANCE));
    }

    /** {@inheritDoc} */
    @Override public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new IgniteFilter(getCluster(), traitSet, input, condition);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {
        if (!TraitUtils.rewindability(inTraits.get(0)).rewindable() && RexUtils.hasCorrelation(getCondition()))
            return ImmutableList.of();

        return ImmutableList.of(Pair.of(nodeTraits.replace(TraitUtils.rewindability(inTraits.get(0))),
            inTraits));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {
        return ImmutableList.of(Pair.of(nodeTraits.replace(TraitUtils.distribution(inTraits.get(0))),
            inTraits));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {
        return ImmutableList.of(Pair.of(nodeTraits.replace(TraitUtils.collation(inTraits.get(0))),
            inTraits));
    }

    /** */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughCorrelation(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {
        Set<CorrelationId> corrSet = RexUtils.extractCorrelationIds(getCondition());

        CorrelationTrait correlation = TraitUtils.correlation(nodeTraits);

        if (corrSet.isEmpty() || correlation.correlationIds().containsAll(corrSet))
            return Pair.of(nodeTraits, ImmutableList.of(inTraits.get(0).replace(correlation)));

        return null;
    }

    /** */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCorrelation(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {
        Set<CorrelationId> corrIds = RexUtils.extractCorrelationIds(getCondition());

        corrIds.addAll(TraitUtils.correlation(inTraits.get(0)).correlationIds());

        return ImmutableList.of(Pair.of(nodeTraits.replace(CorrelationTrait.correlations(corrIds)), inTraits));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughDistribution(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {
        IgniteDistribution distribution = TraitUtils.distribution(nodeTraits);

        if (distribution.function().correlated()) {
            // Check if filter contains condition with required correlate.
            DistributionFunction.CorrelatedDistribution func = (DistributionFunction.CorrelatedDistribution)distribution.function();

            RexVisitor<Void> visitor = new RexVisitorImpl<Void>(true) {
                @Override public Void visitCorrelVariable(RexCorrelVariable variable) {
                    if (variable.id.equals(func.correlationId()))
                        throw new Util.FoundOne(variable);

                    return null;
                }
            };

            try {
                condition.accept(visitor);
            }
            catch (Util.FoundOne corr) {
                // Found required correlate.
                IgniteDistribution corrDistr = func.target();

                assert corrDistr.getType() == RelDistribution.Type.HASH_DISTRIBUTED;

                // Remap correlate fields to input fields.
                int corrFieldsCnt = ((RexNode)corr.getNode()).getType().getFieldCount();
                int inputFieldsCnt = getRowType().getFieldCount();

                // Calcite during planning can add fields to the left hand of the correlated join, but without proper
                // RexCorrelVariable type change. Sometimes these fields can participate in hash distribution keys.
                // These keys can't be refered by RexFieldAccess and original hash distribution can't be restored,
                // so, just ignore correlated distribution with these keys.
                for (int i : corrDistr.getKeys()) {
                    if (i >= corrFieldsCnt)
                        return null;
                }

                Mapping mapping = Mappings.create(MappingType.PARTIAL_FUNCTION, corrFieldsCnt, inputFieldsCnt);

                List<RexNode> conds = RelOptUtil.conjunctions(RexUtil.toCnf(getCluster().getRexBuilder(), condition));

                for (RexNode cond : conds) {
                    if (cond instanceof RexCall && ((RexCall)cond).getOperator().getKind() == SqlKind.EQUALS) {
                        RexNode left = ((RexCall)cond).getOperands().get(0);
                        RexNode right = ((RexCall)cond).getOperands().get(1);

                        RexInputRef inputRef = left instanceof RexInputRef ? (RexInputRef)left :
                            right instanceof RexInputRef ? (RexInputRef)right : null;

                        RexFieldAccess fieldAccess = left instanceof RexFieldAccess ? (RexFieldAccess)left :
                            right instanceof RexFieldAccess ? (RexFieldAccess)right : null;

                        if (inputRef != null && fieldAccess != null &&
                            fieldAccess.getReferenceExpr() instanceof RexCorrelVariable &&
                            ((RexCorrelVariable)fieldAccess.getReferenceExpr()).id.equals(func.correlationId())
                        )
                            mapping.set(fieldAccess.getField().getIndex(), inputRef.getIndex());
                    }
                }

                IgniteDistribution inputDistr = corrDistr.apply(mapping);

                // Found all keys in filter conditions, replace correlated distribution with the real one.
                if (inputDistr != IgniteDistributions.random())
                    return Pair.of(nodeTraits, Commons.transform(inTraits, t -> t.replace(inputDistr)));
            }
        }

        return Pair.of(nodeTraits, Commons.transform(inTraits, t -> t.replace(distribution)));
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCnt = mq.getRowCount(getInput());

        return planner.getCostFactory().makeCost(rowCnt,
            rowCnt * (IgniteCost.ROW_COMPARISON_COST + IgniteCost.ROW_PASS_THROUGH_COST), 0);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteFilter(cluster, getTraitSet(), sole(inputs), getCondition());
    }
}
