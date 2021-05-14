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
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;

import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;

/**
 *
 */
public abstract class IgniteAggregate extends Aggregate implements IgniteRel {
    /** {@inheritDoc} */
    protected IgniteAggregate(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls
    ) {
        super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
    }

    /** {@inheritDoc} */
    protected IgniteAggregate(RelInput input) {
        super(changeTraits(input, IgniteConvention.INSTANCE));
    }

    /** */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        Double groupsCnt = mq.getDistinctRowCount(getInput(), groupSet, null);

        if (groupsCnt != null)
            return groupsCnt;

        // Estimation of the groups count is not available.
        // Use heuristic estimation for result rows count.
        return super.estimateRowCount(mq);
    }

    /** */
    public double estimateMemoryForGroup(RelMetadataQuery mq) {
        double mem = groupSet.cardinality() * IgniteCost.AVERAGE_FIELD_SIZE;

        if (!aggCalls.isEmpty()) {
            double grps = estimateRowCount(mq);
            double rows = input.estimateRowCount(mq);

            for (AggregateCall aggCall : aggCalls) {
                if (aggCall.isDistinct())
                    mem += IgniteCost.AGG_CALL_MEM_COST * rows / grps;
                else
                    mem += IgniteCost.AGG_CALL_MEM_COST;
            }
        }

        return mem;
    }

    /** */
    public RelOptCost computeSelfCostHash(RelOptPlanner planner, RelMetadataQuery mq) {
        IgniteCostFactory costFactory = (IgniteCostFactory)planner.getCostFactory();

        double inRows = mq.getRowCount(getInput());
        double groups = estimateRowCount(mq);

        double distinct = 1;
//        double distinct = aggCalls.stream().anyMatch(AggregateCall::isDistinct) ? 10000 : 1;

        return costFactory.makeCost(
            inRows,
            inRows * IgniteCost.ROW_PASS_THROUGH_COST,
            0,
            groups * estimateMemoryForGroup(mq),
            0
        ).multiplyBy(distinct);
    }

    /** */
    public RelOptCost computeSelfCostSort(RelOptPlanner planner, RelMetadataQuery mq) {
        IgniteCostFactory costFactory = (IgniteCostFactory)planner.getCostFactory();

        double inRows = mq.getRowCount(getInput());

        double distinct = 1;
//        double distinct = aggCalls.stream().anyMatch(AggregateCall::isDistinct) ? 10000 : 1;

        return costFactory.makeCost(
            inRows,
            inRows * IgniteCost.ROW_PASS_THROUGH_COST,
            0,
            estimateMemoryForGroup(mq),
            0
        ).multiplyBy(distinct);
    }
}


//IgniteProject(SUM(DISTINCT VAL0)=[$1], AVG(DISTINCT VAL1)=[$2]): rowcount = 1000000.0, cumulative cost = IgniteCost [rowCount=2.3E7, cpu=2.3E7, memory=24.0, io=0.0, network=8000000.0], id = 11261
//  IgniteReduceSortAggregate(rowType=[RecordType(JavaType(class java.lang.Integer) GRP0, JavaType(class java.lang.Integer) SUM(DISTINCT VAL0), JavaType(class java.lang.Integer) AVG(DISTINCT VAL1))], group=[{0}], SUM(DISTINCT VAL0)=[SUM(DISTINCT $1)], AVG(DISTINCT VAL1)=[AVG(DISTINCT $2)], collation=[[0]]): rowcount = 1000000.0, cumulative cost = IgniteCost [rowCount=2.2E7, cpu=2.2E7, memory=24.0, io=0.0, network=8000000.0], id = 11260
//    IgniteExchange(distribution=[single]): rowcount = 1000000.0, cumulative cost = IgniteCost [rowCount=2.1E7, cpu=2.1E7, memory=24.0, io=0.0, network=8000000.0], id = 11259
//      IgniteMapSortAggregate(group=[{0}], SUM(DISTINCT VAL0)=[SUM(DISTINCT $1)], AVG(DISTINCT VAL1)=[AVG(DISTINCT $2)], collation=[[0]]): rowcount = 1000000.0, cumulative cost = IgniteCost [rowCount=2.0E7, cpu=2.0E7, memory=24.0, io=0.0, network=0.0], id = 11258
//        IgniteIndexScan(table=[[PUBLIC, TEST]], index=[idx_val1], projects=[[$t2, $t0, $t1]], requiredColumns=[{1, 2, 3}]): rowcount = 1.0E7, cumulative cost = IgniteCost [rowCount=1.0E7, cpu=1.0E7, memory=0.0, io=0.0, network=0.0], id = 286


//    IgniteProject(SUM(DISTINCT VAL0)=[CASE(=($5, 0), null:JavaType(class java.lang.Integer), $4)], AVG(DISTINCT VAL1)=[CAST(/(CASE(=($2, 0), null:JavaType(class java.lang.Integer), $1), $2)):JavaType(class java.lang.Integer)]): rowcount = 25000.0, cumulative cost = IgniteCost [rowCount=6.2225E7, cpu=6.2825E7, memory=44.0, io=0.0, network=1.6E8], id = 11191
//  IgniteMergeJoin(condition=[IS NOT DISTINCT FROM($3, $0)], joinType=[inner], variablesSet=[[]], leftCollation=[[0]], rightCollation=[[0]]): rowcount = 25000.0, cumulative cost = IgniteCost [rowCount=6.22E7, cpu=6.28E7, memory=44.0, io=0.0, network=1.6E8], id = 11190
//    IgniteSingleSortAggregate(group=[{0}], agg#0=[$SUM0($1)], agg#1=[COUNT($1)], collation=[[0]]): rowcount = 100000.0, cumulative cost = IgniteCost [rowCount=3.1E7, cpu=3.1E7, memory=22.0, io=0.0, network=8.0E7], id = 11186
//      IgniteSingleSortAggregate(group=[{0, 1}], collation=[[0, 1]]): rowcount = 1000000.0, cumulative cost = IgniteCost [rowCount=3.0E7, cpu=3.0E7, memory=8.0, io=0.0, network=8.0E7], id = 11185
//        IgniteExchange(distribution=[single]): rowcount = 1.0E7, cumulative cost = IgniteCost [rowCount=2.0E7, cpu=2.0E7, memory=0.0, io=0.0, network=8.0E7], id = 11184
//          IgniteIndexScan(table=[[PUBLIC, TEST]], index=[idx_val1], projects=[[$t1, $t0]], requiredColumns=[{2, 3}]): rowcount = 1.0E7, cumulative cost = IgniteCost [rowCount=1.0E7, cpu=1.0E7, memory=0.0, io=0.0, network=0.0], id = 206
//    IgniteSingleSortAggregate(group=[{0}], SUM(DISTINCT VAL0)=[$SUM0($1)], agg#1=[COUNT($1)], collation=[[0]]): rowcount = 100000.0, cumulative cost = IgniteCost [rowCount=3.1E7, cpu=3.1E7, memory=22.0, io=0.0, network=8.0E7], id = 11189
//      IgniteSingleSortAggregate(group=[{0, 1}], collation=[[0, 1]]): rowcount = 1000000.0, cumulative cost = IgniteCost [rowCount=3.0E7, cpu=3.0E7, memory=8.0, io=0.0, network=8.0E7], id = 11188
//        IgniteExchange(distribution=[single]): rowcount = 1.0E7, cumulative cost = IgniteCost [rowCount=2.0E7, cpu=2.0E7, memory=0.0, io=0.0, network=8.0E7], id = 11187
//          IgniteIndexScan(table=[[PUBLIC, TEST]], index=[idx_val0], projects=[[$t1, $t0]], requiredColumns=[{1, 3}]): rowcount = 1.0E7, cumulative cost = IgniteCost [rowCount=1.0E7, cpu=1.0E7, memory=0.0, io=0.0, network=0.0], id = 501