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

import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.IterableAccumulator;

/**
 * Super class for aggregate nodes.
 */
public abstract class AggregateNode<Row> extends MemoryTrackingNode<Row> implements SingleNode<Row>, Downstream<Row> {
    /** */
    protected final AggregateType type;

    /** May be {@code null} when there are not accumulators (DISTINCT aggregate node). */
    protected final Supplier<List<AccumulatorWrapper<Row>>> accFactory;

    /** */
    protected final RowHandler.RowFactory<Row> rowFactory;

    /** */
    protected final boolean hasAggAccum;

    /** */
    protected AggregateNode(
        ExecutionContext<Row> ctx,
        RelDataType rowType,
        AggregateType type,
        Supplier<List<AccumulatorWrapper<Row>>> accFactory,
        RowHandler.RowFactory<Row> rowFactory,
        long rowOverhead
    ) {
        super(ctx, rowType, rowOverhead);
        this.type = type;
        this.accFactory = accFactory;
        this.rowFactory = rowFactory;
        hasAggAccum = hasAggAccumulators();
    }


    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** */
    protected boolean hasAccumulators() {
        return accFactory != null;
    }

    /** */
    private boolean hasAggAccumulators() {
        return accFactory != null &&
            accFactory.get().stream().anyMatch(aw -> aw.accumulator() instanceof IterableAccumulator);
    }
}
