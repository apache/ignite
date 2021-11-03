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

import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;

/**
 * Ignite sort operator.
 */
public class IgniteSort extends Sort implements IgniteRel {
    /**
     * Constructor.
     *
     * @param cluster   Cluster.
     * @param traits    Trait set.
     * @param child     Input node.
     * @param collation Collation.
     */
    public IgniteSort(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child,
            RelCollation collation) {
        super(cluster, traits, child, collation);
    }
    
    /**
     *
     */
    public IgniteSort(RelInput input) {
        super(changeTraits(input, IgniteConvention.INSTANCE));
    }
    
    /** {@inheritDoc} */
    @Override
    public Sort copy(
            RelTraitSet traitSet,
            RelNode newInput,
            RelCollation newCollation,
            RexNode offset,
            RexNode fetch
    ) {
        assert offset == null && fetch == null;
        
        return new IgniteSort(getCluster(), traitSet, newInput, newCollation);
    }
    
    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }
    
    /** {@inheritDoc} */
    @Override
    public RelCollation collation() {
        return collation;
    }
    
    /** {@inheritDoc} */
    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        if (isEnforcer() || required.getConvention() != IgniteConvention.INSTANCE) {
            return null;
        }
        
        RelCollation collation = TraitUtils.collation(required);
        
        return Pair.of(required.replace(collation), List.of(required.replace(RelCollations.EMPTY)));
    }
    
    /** {@inheritDoc} */
    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        assert childId == 0;
    
        if (isEnforcer() || childTraits.getConvention() != IgniteConvention.INSTANCE) {
            return null;
        }
        
        return Pair.of(childTraits.replace(collation()), List.of(childTraits));
    }
    
    /** {@inheritDoc} */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rows = mq.getRowCount(getInput());
        
        double cpuCost = rows * IgniteCost.ROW_PASS_THROUGH_COST + Util.nLogN(rows) * IgniteCost.ROW_COMPARISON_COST;
        double memory = rows * getRowType().getFieldCount() * IgniteCost.AVERAGE_FIELD_SIZE;
        
        IgniteCostFactory costFactory = (IgniteCostFactory) planner.getCostFactory();
        
        return costFactory.makeCost(rows, cpuCost, 0, memory, 0);
    }
    
    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteSort(cluster, getTraitSet(), sole(inputs), collation);
    }
}
