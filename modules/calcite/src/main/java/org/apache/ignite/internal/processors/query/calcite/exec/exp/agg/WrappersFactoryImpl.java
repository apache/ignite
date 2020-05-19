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

import com.google.common.primitives.Primitives;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.ArrayExpressionFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.Scalar;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.AggregateNode.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;

/** */
@SuppressWarnings({"rawtypes", "unchecked"})
public class WrappersFactoryImpl implements Supplier<List<AccumulatorWrapper>> {
    /** */
    private static final Object[] EMPTY = new Object[0];

    /** */
    private final ExecutionContext ctx;

    /** */
    private final AggregateType type;

    /** */
    private final RowHandler handler;

    /** */
    private final RelDataType inputRowType;

    /** */
    private final List<WrapperPrototype> prototypes;

    /** */
    public WrappersFactoryImpl(ExecutionContext ctx, AggregateType type,
        List<AggregateCall> aggCalls, RelDataType inputRowType) {
        this.ctx = ctx;
        this.type = type;
        this.handler = ctx.planningContext().rowHandler();
        this.inputRowType = inputRowType;

        ArrayList<WrapperPrototype> prototypes = new ArrayList<>(aggCalls.size());

        for (AggregateCall aggCall : aggCalls)
            prototypes.add(new WrapperPrototype(aggCall));

        this.prototypes = prototypes;
    }

    /** {@inheritDoc} */
    @Override public List<AccumulatorWrapper> get() {
        return Commons.transform(prototypes, WrapperPrototype::get);
    }

    /** */
    private abstract class AbstractWrapper implements AccumulatorWrapper {
        /** */
        protected final Accumulator accumulator;

        /** */
        protected final Function<Object, Object> outAdapter;

        /** */
        private AbstractWrapper(Accumulator accumulator, Function<Object, Object> outAdapter) {
            this.accumulator = accumulator;
            this.outAdapter = outAdapter;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            assert type != AggregateType.MAP;

            return outAdapter.apply(accumulator.end());
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator accumulator) {
            assert type == AggregateType.REDUCE;

            this.accumulator.apply(accumulator);
        }

        /** {@inheritDoc} */
        @Override public Accumulator accumulator() {
            return accumulator;
        }
    }

    /** */
    private final class NoArgsWrapper extends AbstractWrapper {
        /** */
        private NoArgsWrapper(Accumulator accumulator, Function<Object, Object> outAdapter) {
            super(accumulator, outAdapter);
        }

        /** {@inheritDoc} */
        @Override public void add(Object row) {
            assert type != AggregateType.REDUCE;

            accumulator.add(EMPTY);
        }
    }

    /** */
    private final class Wrapper extends AbstractWrapper {
        /** */
        private final List<Integer> args;

        /** */
        private final boolean ignoreNulls;

        /** */
        private final Function<Object[], Object[]> inAdapter;

        /** */
        private Wrapper(Accumulator accumulator, List<Integer> args, boolean ignoreNulls,
            Function<Object[], Object[]> inAdapter, Function<Object, Object> outAdapter) {
            super(accumulator, outAdapter);

            this.args = args;
            this.ignoreNulls = ignoreNulls;
            this.inAdapter = inAdapter;
        }

        /** {@inheritDoc} */
        @Override public void add(Object row) {
            Object[] args0 = new Object[args.size()];

            for (int i = 0; i < args.size(); i++) {
                Object val = handler.get(args.get(i), row);

                if (ignoreNulls && val == null)
                    return;

                args0[i] = val;
            }

            accumulator.add(inAdapter.apply(args0));
        }
    }

    /** */
    private final class FilteringWrapper implements AccumulatorWrapper {
        /** */
        private final int filterArg;

        /** */
        private final AccumulatorWrapper delegate;

        /** */
        private FilteringWrapper(int filterArg, AccumulatorWrapper delegate) {
            this.filterArg = filterArg;
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public void add(Object row) {
            if (handler.get(filterArg, row) == Boolean.TRUE)
                delegate.add(row);
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return delegate.end();
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator accumulator) {
            delegate.apply(accumulator);
        }

        /** {@inheritDoc} */
        @Override public Accumulator accumulator() {
            return delegate.accumulator();
        }
    }

    /** */
    private static class InTypeAdapter implements Function<Object[], Object[]> {
        /** */
        private final ExecutionContext ctx;

        /** */
        private final Scalar scalar;

        /** */
        private InTypeAdapter(ExecutionContext ctx, Scalar scalar) {
            this.ctx = ctx;
            this.scalar = scalar;
        }

        /** {@inheritDoc} */
        @Override public Object[] apply(Object[] in) {
            return scalar.execute(ctx, in, new Object[in.length]);
        }
    }

    /** */
    private static class OutTypeAdapter implements Function<Object, Object> {
        /** */
        private final ExecutionContext ctx;

        /** */
        private final Scalar scalar;

        /** */
        private OutTypeAdapter(ExecutionContext ctx, Scalar scalar) {
            this.ctx = ctx;
            this.scalar = scalar;
        }

        /** {@inheritDoc} */
        @Override public Object apply(Object val) {
            return scalar.execute(ctx, new Object[] {val}, new Object[1])[0];
        }
    }

    /** */
    private final class WrapperPrototype implements Supplier<AccumulatorWrapper> {
        /** */
        private final AggregateCall call;

        /** */
        private Function<Object[], Object[]> inAdapter;

        /** */
        private Function<Object, Object> outAdapter;

        /** */
        private Supplier<Accumulator> accFactory;

        /** */
        private WrapperPrototype(AggregateCall call) {
            this.call = call;
        }

        /** {@inheritDoc} */
        @Override public AccumulatorWrapper get() {
            Accumulator accumulator = accumulator();

            if (type == AggregateType.REDUCE)
                return new NoArgsWrapper(accumulator, outAdapter);

            AccumulatorWrapper wrapper;

            if (F.isEmpty(call.getArgList()))
                wrapper = new NoArgsWrapper(accumulator, outAdapter);
            else
                wrapper = new Wrapper(accumulator, call.getArgList(), call.ignoreNulls(), inAdapter, outAdapter);

            if (call.filterArg >= 0)
                wrapper = new FilteringWrapper(call.filterArg, wrapper);

            return wrapper;
        }

        /** */
        public Accumulator accumulator() {
            if (accFactory != null)
                return accFactory.get();

            // init factory and adapters
            accFactory = Accumulators.accumulatorFactory(call);
            Accumulator accumulator = accFactory.get();

            IgniteTypeFactory typeFactory = ctx.planningContext().typeFactory();
            SqlConformance conformance = ctx.planningContext().conformance();

            ArrayExpressionFactory factory = new ArrayExpressionFactory(typeFactory, conformance);

            if (type != AggregateType.REDUCE && !F.isEmpty(call.getArgList()))
                inAdapter = createInAdapter(accumulator, factory);

            if (type != AggregateType.MAP)
                outAdapter = createOutAdapter(accumulator, factory);

            return accumulator;
        }

        /** */
        public Function<Object[], Object[]> createInAdapter(Accumulator accumulator, ArrayExpressionFactory factory) {
            IgniteTypeFactory typeFactory = factory.typeFactory();

            List<Integer> argList = call.getArgList();
            List<RelDataType> argTypes = accumulator.argumentTypes(typeFactory);

            assert !F.isEmpty(argTypes) && argTypes.size() == argList.size();

            List<RelDataType> rowTypes = new ArrayList<>(argList.size());
            List<RelDataTypeField> rowFields = inputRowType.getFieldList();

            for (Integer arg : argList)
                rowTypes.add(rowFields.get(arg).getType());

            RexBuilder rexBuilder = factory.rexBuilder();
            List<RexNode> rexNodes = new ArrayList<>(argList.size());

            boolean shouldCast = false;

            for (int i = 0; i < argList.size(); i++) {
                RelDataType rowType = rowTypes.get(i);
                RelDataType argType = argTypes.get(i);

                RexNode rexNode = rexBuilder.makeInputRef(rowType, i);

                if (shouldCast(typeFactory, rowType, argType)) {
                    rexNode = rexBuilder.makeCast(argType, rexNode);

                    shouldCast = true;
                }

                rexNodes.add(rexNode);
            }

            if (shouldCast) {
                RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);

                for (int i = 0; i < rowTypes.size(); i++)
                    builder.add("$EXP" + i, rowTypes.get(i));

                return new InTypeAdapter(ctx, factory.scalar(rexNodes, builder.build()));
            }

            return Function.identity();
        }

        /** */
        public Function<Object, Object> createOutAdapter(Accumulator accumulator, ArrayExpressionFactory factory) {
            IgniteTypeFactory typeFactory = factory.typeFactory();

            RelDataType inType = accumulator.returnType(typeFactory);
            RelDataType outType = call.getType();

            assert !inType.isStruct();
            assert !outType.isStruct();

            if (shouldCast(typeFactory, inType, outType)) {
                RexBuilder rexBuilder = factory.rexBuilder();

                RelDataType rowType =  new RelDataTypeFactory.Builder(typeFactory)
                    .add("$EXP0", inType).build();

                Scalar scalar = factory.scalar(
                    rexBuilder.makeCast(outType,
                        rexBuilder.makeInputRef(inType, 0)), rowType);

                return new OutTypeAdapter(ctx, scalar);
            }

            return Function.identity();
        }

        /** */
        private boolean shouldCast(IgniteTypeFactory typeFactory, RelDataType inDataType, RelDataType outDataType) {
            Type inType = typeFactory.getJavaClass(inDataType);

            if (!(inType instanceof Class))
                return false; // null or synthetic type, impossible to cast

            Type outType = typeFactory.getJavaClass(outDataType);

            if (!(outType instanceof Class))
                return false; // null or synthetic type, impossible to cast

            // since we can't use primitive types in rows
            // we have to box both types before comparison
            Class inBoxed = Primitives.wrap((Class) inType);
            Class outBoxed = Primitives.wrap((Class) outType);

            // the classes are expected to be basic types like
            // Integer or Long, so, it's OK to compare by links
            return inBoxed != outBoxed;
        }
    }
}
