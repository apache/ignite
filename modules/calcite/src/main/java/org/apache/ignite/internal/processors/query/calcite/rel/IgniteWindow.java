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
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.processors.query.calcite.externalize.RelInputEx;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost.AGG_CALL_MEM_COST;
import static org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost.AVERAGE_FIELD_SIZE;
import static org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost.ROW_COMPARISON_COST;
import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;

/**
 * A relational expression representing a set of window aggregates.
 *
 * <p>A Window can handle several window aggregate functions, over several
 * partitions, with pre- and post-expressions, and an optional post-filter.
 * Each of the partitions is defined by a partition key (zero or more columns)
 * and a range (logical or physical). The partitions expect the data to be
 * sorted correctly on input to the relational expression.
 *
 * <p>Each {@link Window.Group} has a set of
 * {@link org.apache.calcite.rex.RexOver} objects.
 */
public class IgniteWindow extends Window implements IgniteRel {

    /** */
    private final Group grp;

    /** */
    private final boolean streaming;

    /** */
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
        this(input.getCluster(),
            changeTraits(input, IgniteConvention.INSTANCE).getTraitSet(),
            input.getInput(),
            input.getRowType("rowType"),
            ((RelInputEx)input).getWindowGroup("group"),
            input.getBoolean("streaming", false));
    }

    /** */
    public Group getGroup() {
        return grp;
    }

    /** */
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
            .item("streaming", streaming);
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
        RelTraitSet traits = passThroughOrDerivedTraits(required);
        if (traits == null)
            return null;

        return Pair.of(traits, ImmutableList.of(traits));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        assert childId == 0;

        RelTraitSet traits = passThroughOrDerivedTraits(childTraits);
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
    private @Nullable RelTraitSet passThroughOrDerivedTraits(RelTraitSet target) {
        if (target.getConvention() != IgniteConvention.INSTANCE)
            return null;

        RelTraitSet traits = target;
        RelCollation requiredCollation = TraitUtils.collation(target);
        if (!satisfiesCollationSansGroupFields(requiredCollation)) {
            traits = traits.replace(collation());
        }

        IgniteDistribution distribution = TraitUtils.distribution(target);
        if (!satisfiesDistribution(distribution))
            traits = traits.replace(distribution());
        else if (distribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
            // Group set contains all distribution keys, shift distribution keys according to used columns.
            IgniteDistribution outDistribution = distribution.apply(Commons.mapping(grp.keys, rowType.getFieldCount()));
            traits = traits.replace(outDistribution);
        }

        if (traits == traitSet) {
            // new traits equal to current traits of window.
            // no need to pass throught or derive any.
            return null;
        }

        return traits;
    }

    /** Check input distribution satisfies collation of this window. */
    private boolean satisfiesDistribution(IgniteDistribution distribution) {
        if (distribution.satisfies(IgniteDistributions.single()) || distribution.function().correlated()) {
            return true;
        }

        if (distribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
            for (Integer key : distribution.getKeys()) {
                if (!grp.keys.get(key))
                    // can't derive distribution with fields unmatched to group keys
                    return false;
            }
            return true;
        }

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
        if (desiredCollation.satisfies(collation)) {
            return true;
        }

        if (!Util.startsWith(desiredCollation.getKeys(), collation.getKeys())) {
            return false;
        }

        int grpKeysSize = grp.keys.cardinality();
        // strip group keys
        List<RelFieldCollation> desiredFieldCollations = Util.skip(desiredCollation.getFieldCollations(), grpKeysSize);
        List<RelFieldCollation> fieldCollations = Util.skip(collation.getFieldCollations(), grpKeysSize);
        return Util.startsWith(desiredFieldCollations, fieldCollations);
    }
}
