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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.WithSystemProperty;

import static org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType.SINGLE;

/**
 *
 */
@SuppressWarnings("TypeMayBeWeakened")
@WithSystemProperty(key = "calcite.debug", value = "true")
public class SortAggregateTest extends BaseAggregateTest {
    /** {@inheritDoc} */
    @Override protected SingleNode<Object[]> createSingleAggregateNodesChain(
        ExecutionContext<Object[]> ctx,
        RelDataType aggType,
        ImmutableList<ImmutableBitSet> grpSets,
        AggregateCall call,
        RelDataType rowType,
        RowHandler.RowFactory<Object[]> rowFactory,
        ScanNode<Object[]> scan
    ) {
        assert grpSets.size() == 1;

        ImmutableBitSet grpSet = F.first(grpSets);

        assert !grpSet.isEmpty() : "Not applicable for sort aggregate";

        RelCollation collation = RelCollations.of(ImmutableIntList.copyOf(grpSet.asList()));

        Comparator<Object[]> cmp = ctx.expressionFactory().comparator(collation);

        SortNode<Object[]> sort = new SortNode<>(ctx, rowType, cmp);

        sort.register(scan);

        AggregateSortNode<Object[]> agg = new AggregateSortNode<>(
            ctx,
            aggType,
            SINGLE,
            grpSet,
            accFactory(ctx, call, SINGLE, rowType),
            rowFactory,
            cmp
        );

        agg.register(sort);

        return agg;
    }
}