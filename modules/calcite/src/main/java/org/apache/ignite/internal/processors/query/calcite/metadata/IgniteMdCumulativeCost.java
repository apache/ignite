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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import java.util.Set;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;

import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.any;
import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.distribution;

/** */
@SuppressWarnings("unused") // actually all methods are used by runtime generated classes
public class IgniteMdCumulativeCost implements MetadataHandler<BuiltInMetadata.CumulativeCost> {
    /** */
    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(
        BuiltInMethod.CUMULATIVE_COST.method, new IgniteMdCumulativeCost());

    /** {@inheritDoc} */
    @Override public MetadataDef<BuiltInMetadata.CumulativeCost> getDef() {
        return BuiltInMetadata.CumulativeCost.DEF;
    }

    /** */
    public RelOptCost getCumulativeCost(RelSubset rel, RelMetadataQuery mq) {
        return VolcanoUtils.bestCost(rel);
    }

    /** */
    public RelOptCost getCumulativeCost(RelNode rel, RelMetadataQuery mq) {
        RelOptCost cost = nonCumulativeCost(rel, mq);

        if (cost.isInfinite())
            return cost;

        for (RelNode input : rel.getInputs()) {
            RelOptCost inputCost = mq.getCumulativeCost(input);
            if (inputCost.isInfinite())
                return inputCost;

            cost = cost.plus(inputCost);
        }

        return cost;
    }

    /** */
    public RelOptCost getCumulativeCost(IgniteCorrelatedNestedLoopJoin rel, RelMetadataQuery mq) {
        RelOptCost cost = nonCumulativeCost(rel, mq);

        if (cost.isInfinite())
            return cost;

        RelNode left = rel.getLeft();
        RelNode right = rel.getRight();

        Set<CorrelationId> corIds = rel.getVariablesSet();

        RelOptCost leftCost = mq.getCumulativeCost(left);
        if (leftCost.isInfinite())
            return leftCost;

        RelOptCost rightCost = mq.getCumulativeCost(right);
        if (rightCost.isInfinite())
            return rightCost;

        return cost.plus(leftCost).plus(rightCost.multiplyBy(left.estimateRowCount(mq) / corIds.size()));
    }

    /** */
    private static RelOptCost nonCumulativeCost(RelNode rel, RelMetadataQuery mq) {
        RelOptCost cost = mq.getNonCumulativeCost(rel);

        if (cost.isInfinite())
            return cost;

        RelOptCostFactory costFactory = rel.getCluster().getPlanner().getCostFactory();

        if (rel.getConvention() == Convention.NONE || distribution(rel) == any())
            return costFactory.makeInfiniteCost();

        return costFactory.makeZeroCost().isLt(cost) ? cost : costFactory.makeTinyCost();
    }
}
