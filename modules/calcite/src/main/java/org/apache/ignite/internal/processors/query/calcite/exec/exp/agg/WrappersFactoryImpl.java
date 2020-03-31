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
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.ExpressionFactory;
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
    public WrappersFactoryImpl(ExecutionContext ctx, AggregateType type, RowHandler handler,
        List<AggregateCall> aggCalls, RelDataType inputRowType) {
        this.ctx = ctx;
        this.type = type;
        this.handler = handler;
        this.inputRowType = inputRowType;

        ArrayList prototypes = new ArrayList(aggCalls.size());

        for (AggregateCall aggCall : aggCalls)
            prototypes.add(new WrapperPrototype(aggCall));

        this.prototypes = prototypes;
    }

    /** {@inheritDoc} */
    @Override public List<AccumulatorWrapper> get() {
        return Commons.transform(prototypes, WrapperPrototype::get);
    }

    /** */
    private final class InTypeAdapter implements Function<Object[], Object[]> {
        /** */
        private final Scalar scalar;

        /** */
        private InTypeAdapter(ExpressionFactory factory, RelDataType inType, RelDataType outType) {
            assert inType.isStruct();
            assert outType.isStruct();
            assert inType.getFieldCount() == outType.getFieldCount();

            int cnt = inType.getFieldCount();

            List<RelDataTypeField> inFields = inType.getFieldList();
            List<RelDataTypeField> outFields = outType.getFieldList();
            List<RexNode> rexNodes = new ArrayList<>(cnt);

            RexBuilder rexBuilder = factory.rexBuilder();

            for (int i = 0; i < cnt; i++) {
                rexNodes.add(
                    rexBuilder.makeCast(outFields.get(i).getType(),
                        rexBuilder.makeInputRef(inFields.get(i).getType(), i)));
            }

            scalar = factory.scalar(rexNodes, inType);
        }

        /** {@inheritDoc} */
        @Override public Object[] apply(Object[] in) {
            Object[] out = new Object[in.length];

            scalar.execute(ctx, in, out);

            return out;
        }
    }

    /** */
    private final class OutTypeAdapter implements Function<Object, Object> {
        /** */
        private final Scalar scalar;

        /** */
        private final Object[] in = new Object[1];

        /** */
        private final Object[] out = new Object[1];

        /** */
        private OutTypeAdapter(ExpressionFactory factory, RelDataType inType, RelDataType outType) {
            assert !inType.isStruct();
            assert !outType.isStruct();

            IgniteTypeFactory typeFactory = factory.typeFactory();

            RelDataType rowType = typeFactory.createStructType(F.asList(Pair.of("$EXP", inType)));

            RexBuilder rexBuilder = factory.rexBuilder();

            scalar = factory.scalar(
                rexBuilder.makeCast(outType,
                    rexBuilder.makeInputRef(inType, 0)), rowType);
        }

        /** {@inheritDoc} */
        @Override public Object apply(Object o) {
            try {
                in[0] = o;

                scalar.execute(ctx, in, out);

                return out[0];
            }
            finally {
                in[0] = null;
                out[0] = null;
            }
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
    private final class NoArgsWrapper extends AbstractWrapper {
        /** */
        private NoArgsWrapper(Accumulator accumulator, OutTypeAdapter outAdapter) {
            super(accumulator, outAdapter);
        }
    }

    /** */
    private final class Wrapper extends AbstractWrapper {
        /** */
        private final List<Integer> args;

        /** */
        private final boolean ignoreNulls;

        /** */
        private final InTypeAdapter inAdapter;

        /** */
        private Wrapper(Accumulator accumulator, List<Integer> args, boolean ignoreNulls, InTypeAdapter inAdapter, OutTypeAdapter outAdapter) {
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
    private abstract class AbstractWrapper implements AccumulatorWrapper {
        /** */
        protected final Accumulator accumulator;

        /** */
        protected final OutTypeAdapter outAdapter;

        /** */
        private AbstractWrapper(Accumulator accumulator, OutTypeAdapter outAdapter) {
            this.accumulator = accumulator;
            this.outAdapter = outAdapter;
        }

        /** {@inheritDoc} */
        @Override public void add(Object row) {
            assert type != AggregateType.REDUCE;

            accumulator.add(EMPTY);
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
    private final class WrapperPrototype implements Supplier<AccumulatorWrapper> {
        /** */
        private final AggregateCall call;

        /** */
        private InTypeAdapter inAdapter;

        /** */
        private OutTypeAdapter outAdapter;

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

            IgniteTypeFactory typeFactory = ctx.parent().typeFactory();
            SqlConformance conformance = ctx.parent().conformance();
            SqlOperatorTable opTable = ctx.parent().opTable();

            ExpressionFactory factory = new ExpressionFactory(typeFactory, conformance, opTable);

            if (type != AggregateType.REDUCE && !F.isEmpty(call.getArgList())) {
                List<Integer> argList = call.getArgList();
                List<RelDataType> argTypes = accumulator.argumentTypes(typeFactory);

                assert !F.isEmpty(argTypes) && argTypes.size() == argList.size();

                List<RelDataTypeField> rowFields = inputRowType.getFieldList();

                RelDataTypeFactory.Builder inBuilder = new RelDataTypeFactory.Builder(typeFactory);
                RelDataTypeFactory.Builder outBuilder = new RelDataTypeFactory.Builder(typeFactory);

                for (int i = 0; i < argList.size(); i++) {
                    inBuilder.add("$EXP" + i, rowFields.get(argList.get(i)).getType());
                    outBuilder.add("$EXP" + i, argTypes.get(i));
                }

                inAdapter = new InTypeAdapter(factory, inBuilder.build(), outBuilder.build());
            }

            if (type != AggregateType.MAP) {
                RelDataType inType = accumulator.returnType(typeFactory);
                RelDataType outType = call.getType();

                outAdapter = new OutTypeAdapter(factory, inType, outType);
            }

            return accumulator;
        }
    }
}
