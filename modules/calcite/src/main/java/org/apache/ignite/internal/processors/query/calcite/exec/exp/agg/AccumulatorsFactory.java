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

package org.apache.ignite.internal.processors.query.calcite.exec.exp.agg;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.jetbrains.annotations.NotNull;

/** */
public class AccumulatorsFactory<Row> extends AccumulatorsFactoryBase<Row> implements Supplier<List<AccumulatorWrapper<Row>>> {

    /** */
    private final ExecutionContext<Row> ctx;

    /** */
    private final AggregateType type;

    /** */
    private final RelDataType inputRowType;

    /** */
    private final List<WrapperPrototype> prototypes;

    /** */
    public AccumulatorsFactory(
        ExecutionContext<Row> ctx,
        AggregateType type,
        List<AggregateCall> aggCalls,
        RelDataType inputRowType
    ) {
        super(ctx);
        this.ctx = ctx;
        this.type = type;
        this.inputRowType = inputRowType;

        prototypes = Commons.transform(aggCalls, WrapperPrototype::new);
    }

    /** {@inheritDoc} */
    @Override public List<AccumulatorWrapper<Row>> get() {
        return Commons.transform(prototypes, WrapperPrototype::get);
    }

    /** */
    private final class WrapperPrototype implements Supplier<AccumulatorWrapper<Row>> {
        /** */
        private Supplier<Accumulator<Row>> accFactory;

        /** */
        private final AggregateCall call;

        /** */
        private Function<Row, Row> inAdapter;

        /** */
        private Function<Object, Object> outAdapter;

        /** */
        private WrapperPrototype(AggregateCall call) {
            this.call = call;
        }

        /** {@inheritDoc} */
        @Override public AccumulatorWrapper<Row> get() {
            Accumulator<Row> accumulator = accumulator();

            return new AccumulatorWrapperImpl(accumulator, call, inAdapter, outAdapter);
        }

        /** */
        @NotNull private Accumulator<Row> accumulator() {
            if (accFactory != null)
                return accFactory.get();

            // init factory and adapters
            accFactory = Accumulators.accumulatorFactory(call, ctx);
            Accumulator<Row> accumulator = accFactory.get();

            inAdapter = createInAdapter(accumulator);
            outAdapter = createOutAdapter(accumulator);

            return accumulator;
        }

        /** */
        @NotNull private Function<Row, Row> createInAdapter(Accumulator<Row> accumulator) {
            if (type == AggregateType.REDUCE)
                return Function.identity();

            List<RelDataType> outTypes = accumulator.argumentTypes(ctx.getTypeFactory());
            boolean createRow = StoringAccumulator.class.isAssignableFrom(accumulator.getClass());

            return AccumulatorsFactory.this.createInAdapter(call, inputRowType, outTypes, createRow);
        }

        /** */
        @NotNull private Function<Object, Object> createOutAdapter(Accumulator<Row> accumulator) {
            if (type == AggregateType.MAP)
                return Function.identity();

            RelDataType inType = accumulator.returnType(ctx.getTypeFactory());
            return AccumulatorsFactory.this.createOutAdapter(call, inType);
        }

        /** */
        private RelDataType nonNull(RelDataType type) {
            return ctx.getTypeFactory().createTypeWithNullability(type, false);
        }
    }

    /** */
    private final class AccumulatorWrapperImpl implements AccumulatorWrapper<Row> {
        /** */
        private final Accumulator<Row> accumulator;

        /** */
        private final Function<Row, Row> inAdapter;

        /** */
        private final Function<Object, Object> outAdapter;

        /** */
        private final int filterArg;

        /** */
        private final RowHandler<Row> handler;

        /** */
        AccumulatorWrapperImpl(
            Accumulator<Row> accumulator,
            AggregateCall call,
            Function<Row, Row> inAdapter,
            Function<Object, Object> outAdapter
        ) {
            this.accumulator = accumulator;
            this.inAdapter = inAdapter;
            this.outAdapter = outAdapter;

            filterArg = call.hasFilter() ? call.filterArg : -1;

            handler = ctx.rowHandler();
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            assert type != AggregateType.REDUCE;

            if (filterArg >= 0 && Boolean.TRUE != handler.get(filterArg, row))
                return;

            Row accRow = inAdapter.apply(row);

            if (accRow == null)
                return;

            accumulator.add(accRow);
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            assert type != AggregateType.MAP;

            return outAdapter.apply(accumulator.end());
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> accumulator) {
            assert type == AggregateType.REDUCE;

            this.accumulator.apply(accumulator);
        }

        /** {@inheritDoc} */
        @Override public Accumulator<Row> accumulator() {
            return accumulator;
        }
    }
}
