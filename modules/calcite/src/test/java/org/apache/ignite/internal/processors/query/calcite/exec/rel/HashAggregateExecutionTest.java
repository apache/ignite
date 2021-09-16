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

import java.util.Collections;
import java.util.Comparator;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType.MAP;
import static org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType.REDUCE;
import static org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType.SINGLE;

/**
 *
 */
@SuppressWarnings("TypeMayBeWeakened")
@WithSystemProperty(key = "calcite.debug", value = "true")
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
                IntStream.range(0, F.first(grpSets).cardinality()).boxed().collect(Collectors.toList())
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
                IntStream.range(0, F.first(grpSets).cardinality()).boxed().collect(Collectors.toList())
            )
        );

        Comparator<Object[]> cmp = ctx.expressionFactory().comparator(collation);

        // Create sort node on the top to check sorted results
        SortNode<Object[]> sort = new SortNode<>(ctx, aggRowType, cmp);

        sort.register(aggRdc);

        return sort;
    }

    /**
     * Test verifies that after rewind all groups are properly initialized.
     */
    @Test
    public void countOfEmptyWithRewind() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Collections.emptyList());

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableIntList.of(),
            -1,
            null,
            RelCollations.EMPTY,
            tf.createJavaType(int.class),
            null
        );

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of());

        RelDataType aggRowType = TypeUtils.createRowType(tf, int.class);

        SingleNode<Object[]> aggChain = createAggregateNodesChain(
            ctx,
            grpSets,
            call,
            rowType,
            aggRowType,
            rowFactory(),
            scan
        );

        for (int i = 0; i < 2; i++) {
            RootNode<Object[]> root = new RootNode<Object[]>(ctx, aggRowType) {
                /** {@inheritDoc} */
                @Override protected void rewindInternal() {
                    // NO-OP
                }

                /** {@inheritDoc} */
                @Override public void close() {
                    // NO-OP
                }
            };

            root.register(aggChain);

            assertTrue(root.hasNext());
            Assert.assertArrayEquals(row(0), root.next());
            assertFalse(root.hasNext());

            aggChain.rewind();
        }
    }
}
