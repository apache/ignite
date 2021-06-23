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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;
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

/**
 *
 */
public class HashAggregateExecutionTest extends BaseAggregateTest {
    /** {@inheritDoc} */
    @Override protected SingleNode<Object[]> createSingleAggregateNodesChain(
        ExecutionContext<Object[]> ctx,
        ImmutableList<ImmutableBitSet> grpSets,
        AggregateCall call,
        RelDataType inRowType,
        RelDataType aggRowType,
        RowHandler.RowFactory<Object[]> rowFactory,
        ScanNode<Object[]> scan
    ) {
        assert grpSets.size() == 1 : "Test checks only simple GROUP BY";

        HashAggregateNode<Object[]> agg = new HashAggregateNode<>(
            ctx,
            aggRowType,
            SINGLE,
            grpSets,
            accFactory(ctx, call, SINGLE, inRowType),
            rowFactory
        );

        agg.register(scan);

        // Collation of the first fields emulates planner behavior:
        // The group's keys placed on the begin of the output row.
        RelCollation collation = RelCollations.of(
            ImmutableIntList.copyOf(
                IntStream.range(0, first(grpSets).cardinality()).boxed().collect(Collectors.toList())
            )
        );

        Comparator<Object[]> cmp = ctx.expressionFactory().comparator(collation);

        // Create sort node on the top to check sorted results
        SortNode<Object[]> sort = new SortNode<>(ctx, inRowType, cmp);

        sort.register(agg);

        return sort;
    }

    /** {@inheritDoc} */
    @Override protected SingleNode<Object[]> createMapReduceAggregateNodesChain(
        ExecutionContext<Object[]> ctx,
        ImmutableList<ImmutableBitSet> grpSets,
        AggregateCall call,
        RelDataType inRowType,
        RelDataType aggRowType,
        RowHandler.RowFactory<Object[]> rowFactory,
        ScanNode<Object[]> scan
    ) {
        assert grpSets.size() == 1 : "Test checks only simple GROUP BY";

        HashAggregateNode<Object[]> aggMap = new HashAggregateNode<>(
            ctx,
            aggRowType,
            MAP,
            grpSets,
            accFactory(ctx, call, MAP, inRowType),
            rowFactory
        );

        aggMap.register(scan);

        HashAggregateNode<Object[]> aggRdc = new HashAggregateNode<>(
            ctx,
            aggRowType,
            REDUCE,
            grpSets,
            accFactory(ctx, call, REDUCE, aggRowType),
            rowFactory
        );

        aggRdc.register(aggMap);

        // Collation of the first fields emulates planner behavior:
        // The group's keys placed on the begin of the output row.
        RelCollation collation = RelCollations.of(
            ImmutableIntList.copyOf(
                IntStream.range(0, first(grpSets).cardinality()).boxed().collect(Collectors.toList())
            )
        );

        Comparator<Object[]> cmp = ctx.expressionFactory().comparator(collation);

        // Create sort node on the top to check sorted results
        SortNode<Object[]> sort = new SortNode<>(ctx, aggRowType, cmp);

        sort.register(aggRdc);

        return sort;
    }
}
