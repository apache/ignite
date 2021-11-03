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

package org.apache.ignite.internal.processors.query.calcite.rel.set;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitsAwareIgniteRel;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * Base class for physical MINUS (EXCEPT) set op.
 */
public abstract class IgniteMinusBase extends Minus implements TraitsAwareIgniteRel {
    /** Count of counter fields used to aggregate results. */
    protected static final int COUNTER_FIELDS_CNT = 2;
    
    /**
     *
     */
    IgniteMinusBase(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
        super(cluster, traits, inputs, all);
    }
    
    /** {@inheritDoc} */
    protected IgniteMinusBase(RelInput input) {
        super(TraitUtils.changeTraits(input, IgniteConvention.INSTANCE));
    }
    
    /** {@inheritDoc} */
    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughCollation(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // Operation erases collation.
        return Pair.of(nodeTraits.replace(RelCollations.EMPTY),
                Commons.transform(inputTraits, t -> t.replace(RelCollations.EMPTY)));
    }
    
    /** {@inheritDoc} */
    @Override
    public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // Operation erases collation.
        return List.of(Pair.of(nodeTraits.replace(RelCollations.EMPTY),
                Commons.transform(inputTraits, t -> t.replace(RelCollations.EMPTY))));
    }
    
    /** Gets count of fields for aggregation for this node. Required for memory consumption calculation. */
    protected abstract int aggregateFieldsCount();
    
    /** {@inheritDoc} */
    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        final List<RelNode> inputs = getInputs();
        
        double rows = mq.getRowCount(inputs.get(0));
    
        for (int i = 1; i < inputs.size(); i++) {
            rows -= 0.5 * Math.min(rows, mq.getRowCount(inputs.get(i)));
        }
        
        return rows;
    }
    
    /** {@inheritDoc} */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        IgniteCostFactory costFactory = (IgniteCostFactory) planner.getCostFactory();
        
        double rows = estimateRowCount(mq);
        
        double inputRows = 0;
    
        for (RelNode input : getInputs()) {
            inputRows += mq.getRowCount(input);
        }
        
        double mem = 0.5 * inputRows * aggregateFieldsCount() * IgniteCost.AVERAGE_FIELD_SIZE;
        
        return costFactory.makeCost(rows, inputRows * IgniteCost.ROW_PASS_THROUGH_COST, 0, mem, 0);
    }
}
