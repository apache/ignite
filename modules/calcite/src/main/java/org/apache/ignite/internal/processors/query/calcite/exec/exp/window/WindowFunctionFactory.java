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

package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorsFactoryBase;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.jetbrains.annotations.NotNull;

/** A factory class responsible for instantiating window functions. */
final class WindowFunctionFactory<Row> extends AccumulatorsFactoryBase<Row> implements Supplier<List<WindowFunctionWrapper<Row>>> {

    /** */
    private final List<WindowFunctionPrototype<Row>> prototypes;

    /** */
    private final RelDataType inputRowType;

    /** */
    private final ExecutionContext<Row> ctx;

    /** */
    WindowFunctionFactory(
        ExecutionContext<Row> ctx,
        Window.Group group,
        List<AggregateCall> aggCalls,
        RelDataType inputRowType
    ) {
        super(ctx);
        this.inputRowType = inputRowType;
        this.ctx = ctx;

        ImmutableList.Builder<WindowFunctionPrototype<Row>> prototypes = ImmutableList.builder();
        for (AggregateCall aggCall : aggCalls) {
            if (WindowFunctions.isWindowFunction(aggCall))
                prototypes.add(new WindowFunctionWrapperPrototype(aggCall));
            else {
                prototypes.add(new WindowFunctionAccumulatorAdapterPrototype(aggCall, group));
            }
        }

        this.prototypes = prototypes.build();
    }

    /** {@inheritDoc} */
    @Override public List<WindowFunctionWrapper<Row>> get() {
        return Commons.transform(prototypes, Supplier::get);
    }

    /** Checks whenether window functions can be streamed. */
    boolean isStreamable() {
        return prototypes.stream().allMatch(WindowFunctionPrototype::isStreamable);
    }

    /** */
    private interface WindowFunctionPrototype<Row> extends Supplier<WindowFunctionWrapper<Row>> {
        /** */
        boolean isStreamable();
    }

    /** */
    private final class WindowFunctionWrapperPrototype implements WindowFunctionPrototype<Row> {
        /** */
        private Supplier<WindowFunction<Row>> funcFactory;

        /** */
        private final AggregateCall call;

        /** */
        private Function<Row, Row> inAdapter;

        /** */
        private Function<Object, Object> outAdapter;

        /** */
        private final boolean supportsStreaming;

        /** */
        private WindowFunctionWrapperPrototype(AggregateCall call) {
            this.call = call;
            supportsStreaming = WindowFunctions.isStreamingFunction(call.getAggregation());
        }

        /** {@inheritDoc} */
        @Override public WindowFunctionWrapper<Row> get() {
            WindowFunction<Row> windowFunction = windowFunction();
            return new FunctionWrapper(windowFunction, inAdapter, outAdapter);
        }

        /** */
        @NotNull private WindowFunction<Row> windowFunction() {
            if (funcFactory != null)
                return funcFactory.get();

            // init factory and adapters
            funcFactory = WindowFunctions.windowFunctionFactory(call, ctx);
            WindowFunction<Row> windowFunction = funcFactory.get();

            inAdapter = createInAdapter(windowFunction);
            outAdapter = createOutAdapter(windowFunction);

            assert supportsStreaming == windowFunction instanceof StreamWindowFunction;

            return windowFunction;
        }

        /** */
        @NotNull private Function<Row, Row> createInAdapter(WindowFunction<Row> windowFunction) {
            List<RelDataType> outTypes = windowFunction.argumentTypes(ctx.getTypeFactory());
            return WindowFunctionFactory.this.createInAdapter(call, inputRowType, outTypes, false);
        }

        /** */
        @NotNull private Function<Object, Object> createOutAdapter(WindowFunction<Row> windowFunction) {
            RelDataType inType = windowFunction.returnType(ctx.getTypeFactory());
            return WindowFunctionFactory.this.createOutAdapter(call, inType);
        }

        /** {@inheritDoc} */
        @Override public boolean isStreamable() {
            return supportsStreaming;
        }
    }

    /** */
    private final class FunctionWrapper implements WindowFunctionWrapper<Row> {
        /** */
        private final WindowFunction<Row> windowFunction;

        /** */
        private final Function<Row, Row> inAdapter;

        /** */
        private final Function<Object, Object> outAdapter;

        /** */
        FunctionWrapper(
            WindowFunction<Row> windowFunction,
            Function<Row, Row> inAdapter,
            Function<Object, Object> outAdapter
        ) {
            this.windowFunction = windowFunction;
            this.inAdapter = inAdapter;
            this.outAdapter = outAdapter;
        }

        /** {@inheritDoc} */
        @Override public Object call(Row row, int rowIdx, int peerIdx) {
            assert windowFunction instanceof StreamWindowFunction;

            Row accRow = inAdapter.apply(row);
            assert accRow != null;

            Object result = ((StreamWindowFunction<Row>)windowFunction).call(accRow, rowIdx, peerIdx);
            return outAdapter.apply(result);
        }

        /** {@inheritDoc} */
        @Override public Object call(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame) {
            Row accRow = inAdapter.apply(row);
            assert accRow != null;

            Object result = windowFunction.call(accRow, rowIdx, peerIdx, frame);
            return outAdapter.apply(result);
        }
    }

    /** */
    private final class WindowFunctionAccumulatorAdapterPrototype implements WindowFunctionPrototype<Row> {

        /** */
        private final Supplier<org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper<Row>> factory;

        /** */
        private final boolean streamable;

        /** */
        private WindowFunctionAccumulatorAdapterPrototype(AggregateCall call, Window.Group group) {
            Supplier<List<org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper<Row>>> accFactory =
                ctx.expressionFactory().accumulatorsFactory(AggregateType.SINGLE, List.of(call), inputRowType);
            factory = () -> accFactory.get().get(0);
            streamable = group.isRows
                && group.lowerBound.isUnbounded()
                && group.upperBound.isCurrentRow();
        }

        /** {@inheritDoc} */
        @Override public WindowFunctionWrapper<Row> get() {
            return new AccumulatorWrapper<>(factory, streamable);
        }

        /** {@inheritDoc} */
        @Override public boolean isStreamable() {
            return streamable;
        }
    }

    /** */
    private static final class AccumulatorWrapper<Row> implements WindowFunctionWrapper<Row> {
        /** */
        private final Supplier<org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper<Row>> factory;

        /** */
        private final boolean streamable;

        /** */
        private org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper<Row> accHolder;

        /** */
        private int frameStart = -1;

        /** */
        private int frameEnd = -1;

        /** */
        private AccumulatorWrapper(
            Supplier<org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper<Row>> factory,
            boolean streamable) {
            this.factory = factory;
            this.streamable = streamable;
        }

        /** {@inheritDoc} */
        @Override public Object call(Row row, int rowIdx, int peerIdx) {
            assert streamable;
            org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper<Row> acc = accumulator();
            acc.add(row);
            return acc.end();
        }

        /** {@inheritDoc} */
        @Override public Object call(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame) {
            int start = frame.getFrameStart(row, rowIdx, peerIdx);
            int end = frame.getFrameEnd(row, rowIdx, peerIdx);
            org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper<Row> acc = accumulator();

            if (frameStart != start || frameEnd > end) {
                // recalculate accumulator if start idx changed.
                frameStart = start;
                accHolder = null;
                // recreate accumulator.
                acc = accumulator();
                for (int i = frameStart; i <= end; i++) {
                    Row valRow = frame.get(i);
                    acc.add(valRow);
                }
            }
            else if (frameEnd != end && end >= 0) {
                // append rows to accumulator
                for (int i = frameEnd + 1; i <= end; i++) {
                    Row valRow = frame.get(i);
                    acc.add(valRow);
                }
            }
            frameEnd = end;
            return acc.end();
        }

        /** */
        private org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper<Row> accumulator() {
            if (accHolder != null)
                return accHolder;
            accHolder = factory.get();
            return accHolder;
        }
    }
}
