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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitsAwareIgniteRel;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * Base interface for physical set op node (MINUS, INTERSECT).
 */
public interface IgniteSetOp extends TraitsAwareIgniteRel {
    /** ALL flag of set op. */
    public boolean all();

    /** {@inheritDoc} */
    @Override public default Pair<RelTraitSet, List<RelTraitSet>> passThroughCollation(RelTraitSet nodeTraits,
        List<RelTraitSet> inputTraits) {
        // Operation erases collation.
        return Pair.of(nodeTraits.replace(RelCollations.EMPTY),
            Commons.transform(inputTraits, t -> t.replace(RelCollations.EMPTY)));
    }

    /** {@inheritDoc} */
    @Override public default List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(RelTraitSet nodeTraits,
        List<RelTraitSet> inputTraits) {
        // Operation erases collation.
        return ImmutableList.of(Pair.of(nodeTraits.replace(RelCollations.EMPTY),
            Commons.transform(inputTraits, t -> t.replace(RelCollations.EMPTY))));
    }

    /** Gets count of fields for aggregation for this node. Required for memory consumption calculation. */
    public int aggregateFieldsCount();

    /** Compute cost for set op. */
    public default RelOptCost computeSetOpCost(RelOptPlanner planner, RelMetadataQuery mq) {
        IgniteCostFactory costFactory = (IgniteCostFactory)planner.getCostFactory();

        double inputRows = 0;

        for (RelNode input : getInputs())
            inputRows += mq.getRowCount(input);

        double mem = 0.5 * inputRows * aggregateFieldsCount() * IgniteCost.AVERAGE_FIELD_SIZE;

        return costFactory.makeCost(inputRows, inputRows * IgniteCost.ROW_PASS_THROUGH_COST, 0, mem, 0);
    }

    /** Aggregate type. */
    public AggregateType aggregateType();
}
