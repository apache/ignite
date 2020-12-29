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

import java.util.Arrays;
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
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType.SINGLE;

/**
 *
 */
@SuppressWarnings("TypeMayBeWeakened")
@WithSystemProperty(key = "calcite.debug", value = "true")
public class HashAggregateTest extends BaseAggregateTest {
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
        assert grpSets.size() == 1 : "Test checks only simple GROUP BY";

        AggregateHashNode<Object[]> agg = new AggregateHashNode<>(
            ctx,
            aggType,
            SINGLE,
            grpSets,
            accFactory(ctx, call, SINGLE, rowType),
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
        SortNode<Object[]> sort = new SortNode<>(ctx, rowType, cmp);

        sort.register(agg);

        return sort;
    }

    /** */
    @Test
    public void singleCount() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, String.class, int.class);
        ScanNode<Object[]> scan = new ScanNode<>(ctx, rowType, Arrays.asList(
            row("Igor", 200),
            row("Roman", 300),
            row("Ivan", 1400),
            row("Alexey", 1000)
        ));

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableIntList.of(),
            -1,
            RelCollations.EMPTY,
            tf.createJavaType(int.class),
            null);

        ImmutableList<ImmutableBitSet> grpSets = ImmutableList.of(ImmutableBitSet.of());

        RelDataType aggType = TypeUtils.createRowType(tf, int.class);
        AggregateHashNode<Object[]> agg = new AggregateHashNode<>(
            ctx,
            aggType,
            SINGLE,
            grpSets,
            accFactory(ctx, call, SINGLE, rowType),
            rowFactory()
        );

        agg.register(scan);

        RootNode<Object[]> root = new RootNode<>(ctx, aggType);
        root.register(agg);

        assertTrue(root.hasNext());
        assertEquals(4, root.next()[0]);
        assertFalse(root.hasNext());
    }
}