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

package org.apache.ignite.internal.sql.engine.exec.rel;

import static org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType.MAP;
import static org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType.REDUCE;
import static org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType.SINGLE;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * HashAggregateExecutionTest.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class HashAggregateExecutionTest extends BaseAggregateTest {
    /** {@inheritDoc} */
    @Override
    protected SingleNode<Object[]> createSingleAggregateNodesChain(
            ExecutionContext<Object[]> ctx,
            List<ImmutableBitSet> grpSets,
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

        RelCollation collation = createOutCollation(grpSets);

        Comparator<Object[]> cmp = ctx.expressionFactory().comparator(collation);

        // Create sort node on the top to check sorted results
        SortNode<Object[]> sort = new SortNode<>(ctx, inRowType, cmp);

        sort.register(agg);

        return sort;
    }

    private RelCollation createOutCollation(List<ImmutableBitSet> grpSets) {
        RelCollation collation;

        if (!grpSets.isEmpty() && grpSets.stream().anyMatch(set -> !set.isEmpty())) {
            // Sort by group to simplify compare results with expected results.
            collation = RelCollations.of(
                    ImmutableIntList.of(IntStream.range(0, Objects.requireNonNull(first(grpSets)).cardinality()).toArray()));
        } else {
            // Sort for the first column if there are no groups.
            collation = RelCollations.of(0);
        }

        return collation;
    }

    /** {@inheritDoc} */
    @Override
    protected SingleNode<Object[]> createMapReduceAggregateNodesChain(
            ExecutionContext<Object[]> ctx,
            List<ImmutableBitSet> grpSets,
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

        RelCollation collation = createOutCollation(grpSets);

        Comparator<Object[]> cmp = ctx.expressionFactory().comparator(collation);

        // Create sort node on the top to check sorted results
        SortNode<Object[]> sort = new SortNode<>(ctx, aggRowType, cmp);

        sort.register(aggRdc);

        return sort;
    }


    /**
     * Test verifies that after rewind all groups are properly initialized.
     */
    @ParameterizedTest
    @EnumSource
    public void countOfEmptyWithRewind(TestAggregateType testAgg) {
        ExecutionContext<Object[]> ctx = executionContext();
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
                testAgg,
                ctx,
                grpSets,
                call,
                rowType,
                aggRowType,
                rowFactory(),
                scan
        );

        for (int i = 0; i < 2; i++) {
            RootNode<Object[]> root = new RootNode<>(ctx, aggRowType) {
                /** {@inheritDoc} */
                @Override public void close() {
                    // NO-OP
                }
            };

            root.register(aggChain);

            assertTrue(root.hasNext());
            assertArrayEquals(row(0), root.next());
            assertFalse(root.hasNext());

            aggChain.rewind();
        }
    }
}
