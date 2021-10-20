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

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;

import static org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType.MAP;
import static org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType.REDUCE;
import static org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType.SINGLE;
import static org.apache.ignite.internal.util.CollectionUtils.first;

/** */
public class SortAggregateExecutionTest extends BaseAggregateTest {
    /** {@inheritDoc} */
    @Override protected SingleNode<Object[]> createSingleAggregateNodesChain(
        ExecutionContext<Object[]> ctx,
        List<ImmutableBitSet> grpSets,
        AggregateCall call,
        RelDataType inRowType,
        RelDataType aggRowType,
        RowHandler.RowFactory<Object[]> rowFactory,
        ScanNode<Object[]> scan
    ) {
        assert grpSets.size() == 1;

        ImmutableBitSet grpSet = first(grpSets);

        assert !grpSet.isEmpty() : "Not applicable for sort aggregate";

        RelCollation collation = RelCollations.of(ImmutableIntList.copyOf(grpSet.asList()));

        Comparator<Object[]> cmp = ctx.expressionFactory().comparator(collation);

        SortNode<Object[]> sort = new SortNode<>(ctx, inRowType, cmp);

        sort.register(scan);

        SortAggregateNode<Object[]> agg = new SortAggregateNode<>(
            ctx,
            aggRowType,
            SINGLE,
            grpSet,
            accFactory(ctx, call, SINGLE, inRowType),
            rowFactory,
            cmp
        );

        agg.register(sort);

        return agg;
    }

    /** {@inheritDoc} */
    @Override protected SingleNode<Object[]> createMapReduceAggregateNodesChain(
        ExecutionContext<Object[]> ctx,
        List<ImmutableBitSet> grpSets,
        AggregateCall call,
        RelDataType inRowType,
        RelDataType aggRowType,
        RowHandler.RowFactory<Object[]> rowFactory,
        ScanNode<Object[]> scan
    ) {
        assert grpSets.size() == 1;

        ImmutableBitSet grpSet = first(grpSets);

        assert !grpSet.isEmpty() : "Not applicable for sort aggregate";

        RelCollation collation = RelCollations.of(ImmutableIntList.copyOf(grpSet.asList()));

        Comparator<Object[]> cmp = ctx.expressionFactory().comparator(collation);

        SortNode<Object[]> sort = new SortNode<>(ctx, inRowType, cmp);

        sort.register(scan);

        SortAggregateNode<Object[]> aggMap = new SortAggregateNode<>(
            ctx,
            aggRowType,
            MAP,
            grpSet,
            accFactory(ctx, call, MAP, inRowType),
            rowFactory,
            cmp
        );

        aggMap.register(sort);

        // The group's fields placed on the begin of the output row (planner
        // does this by Projection node for aggregate input).
        // Hash aggregate doesn't use groups set on reducer because send GroupKey as object.
        ImmutableIntList reduceGrpFields = ImmutableIntList.copyOf(
            IntStream.range(0, grpSet.cardinality()).boxed().collect(Collectors.toList())
        );

        RelCollation rdcCollation = RelCollations.of(reduceGrpFields);

        Comparator<Object[]> rdcCmp = ctx.expressionFactory().comparator(rdcCollation);

        SortAggregateNode<Object[]> aggRdc = new SortAggregateNode<>(
            ctx,
            aggRowType,
            REDUCE,
            ImmutableBitSet.of(reduceGrpFields),
            accFactory(ctx, call, REDUCE, aggRowType),
            rowFactory,
            rdcCmp
        );

        aggRdc.register(aggMap);

        return aggRdc;
    }
}
