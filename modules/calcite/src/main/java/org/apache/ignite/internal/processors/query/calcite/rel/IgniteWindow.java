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

import java.util.ArrayList;
import java.util.List;
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
import org.apache.ignite.internal.util.collection.IntHashMap;
import org.apache.ignite.internal.util.collection.IntMap;

import static org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost.AGG_CALL_MEM_COST;
import static org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost.AVERAGE_FIELD_SIZE;
import static org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost.ROW_COMPARISON_COST;
import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;

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
        // Streaming flag required only on planning phase, and has not affect on execution.
        this(input.getCluster(),
            changeTraits(input, IgniteConvention.INSTANCE).getTraitSet().plus(input.getCollation()),
            input.getInput(),
            input.getRowType("rowType"),
            ((RelInputEx)input).getWindowGroup("group"),
            false);
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
            .item("collation", collation());
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        if (required.getConvention() != IgniteConvention.INSTANCE)
            return null;

        if (!satisfiesDistribution(TraitUtils.distribution(required)))
            return null;

        RelCollation requiredCollation = TraitUtils.collation(required);
        RelCollation relCollation = TraitUtils.collation(traitSet);
        if (satisfiesCollationSansGroupFields(relCollation, requiredCollation))
            required = required.replace(adjustGroupFieldsCollation(relCollation, requiredCollation));
        else if (satisfiesCollationSansGroupFields(requiredCollation, relCollation))
            required = required.replace(truncateCollation(requiredCollation));
        else
            return null;

        return Pair.of(required, ImmutableList.of(required));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        assert childId == 0;

        if (childTraits.getConvention() != IgniteConvention.INSTANCE)
            return null;

        if (!satisfiesDistribution(TraitUtils.distribution(childTraits)))
            return null;

        RelCollation childCollation = TraitUtils.collation(childTraits);
        RelCollation relCollation = TraitUtils.collation(traitSet);
        if (satisfiesCollationSansGroupFields(relCollation, childCollation))
            childTraits = childTraits.replace(adjustGroupFieldsCollation(relCollation, childCollation));
        else if (!satisfiesCollationSansGroupFields(childCollation, relCollation))
            return null;

        return Pair.of(childTraits, ImmutableList.of(childTraits));
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

    /** Check input distribution satisfies collation of this window. */
    private boolean satisfiesDistribution(IgniteDistribution required) {
        if (required.satisfies(IgniteDistributions.single()) || required.function().correlated())
            return true;

        if (required.getType() == RelDistribution.Type.HASH_DISTRIBUTED)
            return grp.keys.contains(ImmutableBitSet.of(required.getKeys()));

        return false;
    }

    /**
     * Check input collation satisfies collation of this window.
     * - Collations field indicies of the window should be a prefix for desired collation.
     * - Group fields sort direction can be changed to desired collation.
     * - Order fields sort direction should be the same as in desired collation.
     */
    private boolean satisfiesCollationSansGroupFields(RelCollation left, RelCollation right) {
        if (left.satisfies(right))
            return true;

        int leftFldCnt = left.getFieldCollations().size();
        int rightFldCnt = right.getFieldCollations().size();
        if (leftFldCnt < rightFldCnt)
            return false;

        int grpKeysSize = grp.keys.cardinality();

        assert leftFldCnt >= grpKeysSize || rightFldCnt >= grpKeysSize;

        // Check group keys (collation field order and direction meaningless).
        // Since window collation starts with group keys with 'default' sorting direction,
        // desired collation should start with same fields in any order / with any direction.
        int rightGrpFldsCnt = Math.min(grpKeysSize, rightFldCnt);
        ImmutableBitSet leftGrpFlds = ImmutableBitSet.of(Util.first(left.getKeys(), grpKeysSize));
        ImmutableBitSet rightGrpFlds = ImmutableBitSet.of(Util.first(right.getKeys(), Math.min(grpKeysSize, rightGrpFldsCnt)));
        if (!leftGrpFlds.contains(rightGrpFlds))
            return false;
        else if (grpKeysSize >= rightGrpFldsCnt)
            return true;

        // Check remaining collation (collation field order and direction meaningfull).
        List<RelFieldCollation> leftFldCollations = Util.skip(left.getFieldCollations(), grpKeysSize);
        List<RelFieldCollation> rightFldCollations = Util.skip(right.getFieldCollations(), grpKeysSize);
        return Util.startsWith(rightFldCollations, leftFldCollations);
    }

    /** */
    private RelCollation adjustGroupFieldsCollation(RelCollation relCollation, RelCollation requiredCollation) {
        // Current collation satisfies required collation, but group fields in prefix may have invalid field collation.
        // So, we should replace it with field collation from required.
        List<RelFieldCollation> fldCollations = new ArrayList<>(relCollation.getFieldCollations().size());

        int grpFldCnt = grp.keys.cardinality();
        IntMap<RelFieldCollation> currGrpFldCollations = new IntHashMap<>(grpFldCnt);
        for (int i = 0; i < grpFldCnt; i++) {
            RelFieldCollation grpFldCollation = relCollation.getFieldCollations().get(i);
            currGrpFldCollations.put(grpFldCollation.getFieldIndex(), grpFldCollation);
        }

        int requiredGrpFldCnt = Math.min(requiredCollation.getFieldCollations().size(), grpFldCnt);
        for (int i = 0; i < requiredGrpFldCnt; i++) {
            RelFieldCollation requiredGrpFldCollation = requiredCollation.getFieldCollations().get(i);
            fldCollations.add(requiredGrpFldCollation);

            assert currGrpFldCollations.containsKey(requiredGrpFldCollation.getFieldIndex());
            currGrpFldCollations.remove(requiredGrpFldCollation.getFieldIndex());
        }

        // Add remaining group fields collation.
        fldCollations.addAll(currGrpFldCollations.values());

        // Add remaining fields collation.
        fldCollations.addAll(Util.skip(relCollation.getFieldCollations(), grpFldCnt));

        return RelCollations.of(fldCollations);
    }

    /** */
    private RelCollation truncateCollation(RelCollation requiredCollation) {
        // In case of pass through, required collation can use fields outside of input row type.
        // So, we should truncate required collation to input row type.
        // We do not need any additional range checks, since current collation keys is a prefix to required collation keys.
        // Therefore, only additional keys in suffix can be removed here.
        List<RelFieldCollation> requiredCollationFields = requiredCollation.getFieldCollations();
        for (int i = 0; i < requiredCollationFields.size(); i++) {
            if (requiredCollationFields.get(i).getFieldIndex() >= input.getRowType().getFieldCount()) {
                return RelCollations.of(requiredCollationFields.subList(0, i));
            }
        }
        return requiredCollation;
    }
}
