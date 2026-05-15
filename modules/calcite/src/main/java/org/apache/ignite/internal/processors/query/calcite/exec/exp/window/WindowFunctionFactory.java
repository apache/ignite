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
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorsFactoryBase;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.jetbrains.annotations.NotNull;

/** A factory class responsible for instantiating window functions. */
final class WindowFunctionFactory<Row> extends AccumulatorsFactoryBase<Row> {
    /** */
    private final List<WindowFunctionPrototype<Row>> prototypes;

    /** */
    private final RelDataType inputRowType;

    /** */
    WindowFunctionFactory(
        ExecutionContext<Row> ctx,
        Window.Group grp,
        Function<Row, Row> project,
        Map<RexNode, Integer> rexToOrd,
        RelDataType inputRowType
    ) {
        super(ctx);
        this.inputRowType = inputRowType;

        ImmutableList.Builder<WindowFunctionPrototype<Row>> prototypes = ImmutableList.builder();
        for (Window.RexWinAggCall call : grp.aggCalls) {
            AggregateCall aggCall = convertToAggregateCall(call, rexToOrd);

            if (WindowFunctions.isWindowFunction(aggCall))
                prototypes.add(new WindowFunctionWrapperPrototype(aggCall, project));
            else
                prototypes.add(new WindowFunctionAccumulatorAdapterPrototype(aggCall, grp, project));
        }

        this.prototypes = prototypes.build();
    }

    /** */
    List<WindowFunctionWrapper<Row>> createWrappers() {
        return Commons.transform(prototypes, Supplier::get);
    }

    /** Checks whenether window functions can be streamed. */
    boolean isStreamable() {
        return prototypes.stream().allMatch(WindowFunctionPrototype::isStreamable);
    }

    /** Converts window agg call to aggregate call. */
    private AggregateCall convertToAggregateCall(Window.RexWinAggCall call, Map<RexNode, Integer> rexToOrd) {
        // see org.apache.calcite.rel.core.Window.Group.getAggregateCalls
        SqlAggFunction op = (SqlAggFunction)call.getOperator();
        List<Integer> argList = call.operands.stream()
            .map(it -> {
                Integer ord = rexToOrd.get(it);
                assert ord != null : "Unknown aggregate argument: " + it;
                return ord;
            })
            .collect(Collectors.toList());
        return AggregateCall.create(
            call.getParserPosition(),
            op,
            call.distinct,
            false,
            call.ignoreNulls,
            ImmutableList.of(),
            argList,
            -1,
            null,
            RelCollations.EMPTY,
            call.getType(),
            null
        );
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
        private final Function<Row, Row> project;

        /** */
        private Function<Row, Row> inAdapter;

        /** */
        private Function<Object, Object> outAdapter;

        /** */
        private final boolean supportsStreaming;

        /** */
        private WindowFunctionWrapperPrototype(AggregateCall call, Function<Row, Row> project) {
            this.call = call;
            this.project = project;
            supportsStreaming = WindowFunctions.isStreamingFunction(call.getAggregation());
        }

        /** {@inheritDoc} */
        @Override public WindowFunctionWrapper<Row> get() {
            WindowFunction<Row> windowFunction = windowFunction();
            return new FunctionWrapper(windowFunction, project.andThen(inAdapter), outAdapter);
        }

        /** */
        @NotNull private WindowFunction<Row> windowFunction() {
            if (funcFactory != null)
                return funcFactory.get();

            // Init factory and adapters.
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
        @Override public Object callStreaming(Row row, int rowIdx, int peerIdx) {
            assert windowFunction instanceof StreamWindowFunction;

            Row accRow = inAdapter.apply(row);
            assert accRow != null;

            Object result = ((StreamWindowFunction<Row>)windowFunction).call(accRow, rowIdx, peerIdx);
            return outAdapter.apply(result);
        }

        /** {@inheritDoc} */
        @Override public Object callBuffering(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame) {
            Row accRow = inAdapter.apply(row);
            assert accRow != null;

            Object result = windowFunction.call(accRow, rowIdx, peerIdx, frame);
            return outAdapter.apply(result);
        }
    }

    /** */
    private final class WindowFunctionAccumulatorAdapterPrototype implements WindowFunctionPrototype<Row> {

        /** */
        private final Supplier<AccumulatorWrapper<Row>> factory;

        /** */
        private final Function<Row, Row> project;

        /** */
        private final boolean streamable;

        /** */
        private WindowFunctionAccumulatorAdapterPrototype(AggregateCall call, Window.Group grp,
            Function<Row, Row> project) {
            Supplier<List<AccumulatorWrapper<Row>>> accFactory =
                ctx.expressionFactory().accumulatorsFactory(AggregateType.SINGLE, List.of(call), inputRowType);
            factory = () -> accFactory.get().get(0);
            this.project = project;
            streamable = grp.isRows && grp.lowerBound.isUnbounded() && grp.upperBound.isCurrentRow();
        }

        /** {@inheritDoc} */
        @Override public WindowFunctionWrapper<Row> get() {
            return new WindowAccumulatorWrapper<>(factory, project, streamable);
        }

        /** {@inheritDoc} */
        @Override public boolean isStreamable() {
            return streamable;
        }
    }

    /** */
    private static final class WindowAccumulatorWrapper<Row> implements WindowFunctionWrapper<Row> {
        /** */
        private final Supplier<AccumulatorWrapper<Row>> factory;

        /** */
        private final Function<Row, Row> inProject;

        /** */
        private final boolean streamable;

        /** */
        private AccumulatorWrapper<Row> accHolder;

        /** */
        private int frameStart = -1;

        /** */
        private int frameEnd = -1;

        /** */
        private WindowAccumulatorWrapper(Supplier<AccumulatorWrapper<Row>> factory,
            Function<Row, Row> inProject,
            boolean streamable) {
            this.factory = factory;
            this.inProject = inProject;
            this.streamable = streamable;
        }

        /** {@inheritDoc} */
        @Override public Object callStreaming(Row row, int rowIdx, int peerIdx) {
            assert streamable;
            AccumulatorWrapper<Row> acc = accumulator();
            Row inRow = inProject.apply(row);
            acc.add(inRow);
            return acc.end();
        }

        /** {@inheritDoc} */
        @Override public Object callBuffering(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame) {
            int start = frame.getFrameStart(rowIdx, peerIdx);
            int end = frame.getFrameEnd(rowIdx, peerIdx);
            AccumulatorWrapper<Row> acc = accumulator();

            if (frameStart != start || frameEnd > end) {
                // Recalculate accumulator if start idx changed.
                frameStart = start;
                accHolder = null;
                acc = accumulator();
                for (int i = frameStart; i <= end; i++) {
                    Row valRow = frame.getProjected(i);
                    acc.add(valRow);
                }
            }
            else if (frameEnd != end && end >= 0) {
                // Append rows to accumulator.
                for (int i = frameEnd + 1; i <= end; i++) {
                    Row valRow = frame.getProjected(i);
                    acc.add(valRow);
                }
            }
            frameEnd = end;
            return acc.end();
        }

        /** */
        private AccumulatorWrapper<Row> accumulator() {
            if (accHolder != null)
                return accHolder;
            accHolder = factory.get();
            return accHolder;
        }
    }
}
