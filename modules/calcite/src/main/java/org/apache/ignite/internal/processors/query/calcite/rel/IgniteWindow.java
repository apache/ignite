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
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.processors.query.calcite.externalize.RelInputEx;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost.AGG_CALL_MEM_COST;
import static org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost.AVERAGE_FIELD_SIZE;
import static org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost.ROW_COMPARISON_COST;
import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;
import static org.apache.ignite.internal.processors.query.calcite.util.Commons.maxPrefix;

/**
 * A relational expression representing a set of window aggregates.
 *
 * <p>A Window can handle several window aggregate functions, over one
 * partition, with pre- and post-expressions, and an optional post-filter.
 * Partitions is defined by a partition key (zero or more columns)
 * and a range (logical or physical). The partition expect the data to be
 * sorted correctly on input to the relational expression.
 *
 * <p>Each {@link Window.Group} has a set of
 * {@link org.apache.calcite.rex.RexOver} objects.
 */
public class IgniteWindow extends Window implements IgniteRel {
    /**  */
    private final Group grp;

    /**  */
    private final boolean streaming;

    /**  */
    public IgniteWindow(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        RelDataType rowType,
        Group grp,
        boolean streaming
    ) {
        super(cluster, traitSet, input, ImmutableList.of(), rowType, ImmutableList.of(grp));
        this.grp = grp;
        this.streaming = streaming;
        assert !grp.aggCalls.isEmpty();
    }

    /** */
    public IgniteWindow(RelInput input) {
        // Streaming flag required only on planning phase, and has not affect on execution.
        this(input.getCluster(),
            changeTraits(input, IgniteConvention.INSTANCE).getTraitSet().plus(input.getCollation()),
            input.getInput(),
            input.getRowType("rowType"),
            ((RelInputEx)input).getWindowGroup("group"),
            false);
    }

    /**  */
    public Group getGroup() {
        return grp;
    }

    /**  */
    public boolean isStreaming() {
        return streaming;
    }

    /** {@inheritDoc} */
    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new IgniteWindow(getCluster(), traitSet, sole(inputs), getRowType(), grp, streaming);
    }

    /** {@inheritDoc} */
    @Override public Window copy(List<RexLiteral> constants) {
        assert constants.isEmpty();
        return this;
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteWindow(cluster, getTraitSet(), sole(inputs), getRowType(), grp, streaming);
    }

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        return pw
            .input("input", getInput())
            .item("rowType", getRowType())
            .item("group", grp)
            .item("collation", collation());
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        IgniteCostFactory costFactory = (IgniteCostFactory)planner.getCostFactory();

        int aggCnt = grp.aggCalls.size();

        double rowCnt = mq.getRowCount(getInput());
        double cpuCost = rowCnt * ROW_COMPARISON_COST;
        double memCost = (getRowType().getFieldCount() * AVERAGE_FIELD_SIZE + aggCnt * AGG_CALL_MEM_COST) * (streaming ? 1.0 : rowCnt);

        RelOptCost cost = costFactory.makeCost(rowCnt, cpuCost, 0, memCost, 0);

        // Distributed processing is more preferable than processing on the single node.
        if (TraitUtils.distribution(traitSet).satisfies(IgniteDistributions.single()))
            cost = cost.plus(costFactory.makeTinyCost());

        return cost;
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        RelTraitSet traits = passThroughOrDerivedTraits(required, true);
        if (traits == null)
            return null;

        return Pair.of(traits, ImmutableList.of(traits));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        assert childId == 0;

        RelTraitSet traits = passThroughOrDerivedTraits(childTraits, false);
        if (traits == null)
            return null;

        return Pair.of(traits, ImmutableList.of(traits));
    }

    /**
     * Propagates the trait set from the parent to the child, or derives it from the child node.
     *
     * <p>The Window node cannot independently satisfy any traits. Therefore:
     * - Validate that collation and distribution traits are compatible with the Window node.
     * - If they are not, replace them with suitable traits.
     * - Request a new trait set from the input accordingly.
     */
    private @Nullable RelTraitSet passThroughOrDerivedTraits(RelTraitSet target, boolean passThrough) {
        if (target.getConvention() != IgniteConvention.INSTANCE)
            return null;

        RelTraitSet traits = target;
        RelCollation requiredCollation = TraitUtils.collation(target);
        if (!satisfiesCollationSansGroupFields(requiredCollation))
            traits = traits.replace(collation());
        else if (passThrough) {
            // In case of pass through, required collation can use fields outside of input row type.
            // So, we should truncate required collation to input row type.
            // We do not need any additional range checks, since current collation keys is a prefix to required collation keys.
            // Therefore, only additional keys in suffix can be removed here.
            ImmutableBitSet inputColls = ImmutableBitSet.range(input.getRowType().getFieldCount());

            List<Integer> newCollationColls = maxPrefix(requiredCollation.getKeys(), inputColls.asSet());
            List<RelFieldCollation> newCollationFields = requiredCollation.getFieldCollations()
                .stream().filter(k -> newCollationColls.contains(k.getFieldIndex())).collect(Collectors.toList());

            RelCollation newCollation = RelCollations.of(newCollationFields);

            traits = traits.replace(newCollation);
        }

        IgniteDistribution distribution = TraitUtils.distribution(target);
        if (!satisfiesDistribution(distribution))
            traits = traits.replace(distribution());

        if (traits == traitSet)
            // New traits equal to current traits of window.
            // No need to pass throught or derive any.
            return null;

        return traits;
    }

    /** Check input distribution satisfies collation of this window. */
    private boolean satisfiesDistribution(IgniteDistribution desiredDistribution) {
        if (desiredDistribution.satisfies(IgniteDistributions.single()) || desiredDistribution.function().correlated())
            return true;

        if (desiredDistribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED)
            return grp.keys.contains(ImmutableBitSet.of(desiredDistribution.getKeys()));

        return false;
    }

    /**
     * Check input collation satisfies collation of this window.
     * - Collations field indicies of the window should be a prefix for desired collation.
     * - Group fields sort direction can be changed to desired collation.
     * - Order fields sort direction should be the same as in desired collation.
     */
    private boolean satisfiesCollationSansGroupFields(RelCollation desiredCollation) {
        RelCollation collation = collation();
        if (desiredCollation.satisfies(collation))
            return true;

        if (desiredCollation.getFieldCollations().size() < collation.getFieldCollations().size())
            return false;

        int grpKeysSize = grp.keys.cardinality();

        // Check group keys (collation field order and direction meaningless).
        // Since window collation starts with group keys with 'default' sorting direction,
        // desired collation should start with same fields in any order / with any direction.
        ImmutableBitSet desiredGrpFields = ImmutableBitSet.of(Util.first(desiredCollation.getKeys(), grpKeysSize));
        if (!desiredGrpFields.equals(grp.keys))
            return false;

        // Check remaining collation (collation field order and direction meaningfull).
        List<RelFieldCollation> desiredFieldCollations = Util.skip(desiredCollation.getFieldCollations(), grpKeysSize);
        List<RelFieldCollation> fieldCollations = Util.skip(collation.getFieldCollations(), grpKeysSize);
        return Util.startsWith(desiredFieldCollations, fieldCollations);
    }
}
