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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.Pair;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.AggregateNode.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;

/** */
public class WrappersFactoryImpl<Row> implements Supplier<List<AccumulatorWrapper<Row>>> {
    private static final LoadingCache<Pair<RelDataType,RelDataType>, Function<Object,Object>> CACHE =
        CacheBuilder.newBuilder().build(CacheLoader.from(WrappersFactoryImpl::cast0));

    /** */
    public static interface CastFunction extends Function<Object, Object> {
        @Override Object apply(Object o);
    }

    /** */
    private static Function<Object, Object> cast(RelDataType from, RelDataType to) {
        assert !from.isStruct();
        assert !to.isStruct();

        return cast(Pair.of(from, to));
    }

    /** */
    private static Function<Object, Object> cast(Pair<RelDataType, RelDataType> types) {
        try {
            return CACHE.get(types);
        }
        catch (ExecutionException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    private static Function<Object, Object> cast0(Pair<RelDataType,RelDataType> types) {
        IgniteTypeFactory typeFactory = new IgniteTypeFactory();

        RelDataType from = types.left;
        RelDataType to = types.right;

        RelDataType rowType = typeFactory.createStructType(F.asList(types.left), F.asList("$EXPR"));

        ParameterExpression in_ = Expressions.parameter(Object.class, "in");

        RexToLixTranslator.InputGetter getter =
            new RexToLixTranslator.InputGetterImpl(
                ImmutableList.of(
                    Pair.of(in_,
                        PhysTypeImpl.of(typeFactory, rowType,
                            JavaRowFormat.SCALAR, false))));

        RexBuilder builder = new RexBuilder(typeFactory);
        RexProgramBuilder programBuilder = new RexProgramBuilder(rowType, builder);
        RexNode cast = builder.makeCast(to, builder.makeInputRef(from, 0));
        programBuilder.addProject(cast, null);
        RexProgram program = programBuilder.getProgram();
        BlockBuilder list = new BlockBuilder();
        List<Expression> projects = RexToLixTranslator.translateProjects(program, typeFactory, SqlConformanceEnum.DEFAULT,
            list, null, DataContext.ROOT, getter, null);
        list.add(projects.get(0));

        MethodDeclaration decl = Expressions.methodDecl(
            Modifier.PUBLIC, Object.class, "apply", ImmutableList.of(in_), list.toBlock());

        return Commons.compile(CastFunction.class, Expressions.toString(F.asList(decl), "\n", false));
    }

    /** */
    private final ExecutionContext<Row> ctx;

    /** */
    private final AggregateType type;

    /** */
    private final RelDataType inputRowType;

    /** */
    private final List<WrapperPrototype> prototypes;

    /** */
    public WrappersFactoryImpl(ExecutionContext<Row> ctx, AggregateType type,
        List<AggregateCall> aggCalls, RelDataType inputRowType) {
        this.ctx = ctx;
        this.type = type;
        this.inputRowType = inputRowType;

        ArrayList<WrapperPrototype> prototypes = new ArrayList<>(aggCalls.size());

        for (AggregateCall aggCall : aggCalls)
            prototypes.add(new WrapperPrototype(aggCall));

        this.prototypes = prototypes;
    }

    /** {@inheritDoc} */
    @Override public List<AccumulatorWrapper<Row>> get() {
        return Commons.transform(prototypes, WrapperPrototype::get);
    }

    /** */
    private abstract class AbstractWrapper implements AccumulatorWrapper<Row> {
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
        @Override public void add(Row row) {
            assert type != AggregateType.REDUCE;

            accumulator.add();
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
        @Override public void add(Row row) {
            Object[] args0 = new Object[args.size()];

            for (int i = 0; i < args.size(); i++) {
                Object val = ctx.rowHandler().get(args.get(i), row);

                if (ignoreNulls && val == null)
                    return;

                args0[i] = val;
            }

            accumulator.add(inAdapter.apply(args0));
        }
    }

    /** */
    private final class FilteringWrapper implements AccumulatorWrapper<Row> {
        /** */
        private final int filterArg;

        /** */
        private final AccumulatorWrapper<Row> delegate;

        /** */
        private FilteringWrapper(int filterArg, AccumulatorWrapper<Row> delegate) {
            this.filterArg = filterArg;
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            if (ctx.rowHandler().get(filterArg, row) == Boolean.TRUE)
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
    private final class WrapperPrototype implements Supplier<AccumulatorWrapper<Row>> {
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
        @Override public AccumulatorWrapper<Row> get() {
            Accumulator accumulator = accumulator();

            if (type == AggregateType.REDUCE)
                return new NoArgsWrapper(accumulator, outAdapter);

            AccumulatorWrapper<Row> wrapper;

            if (F.isEmpty(call.getArgList()))
                wrapper = new NoArgsWrapper(accumulator, outAdapter);
            else
                wrapper = new Wrapper(accumulator, call.getArgList(), call.ignoreNulls(), inAdapter, outAdapter);

            if (call.filterArg >= 0)
                wrapper = new FilteringWrapper(call.filterArg, wrapper);

            return wrapper;
        }

        /** */
        @NotNull public Accumulator accumulator() {
            if (accFactory != null)
                return accFactory.get();

            // init factory and adapters
            accFactory = Accumulators.accumulatorFactory(call);
            Accumulator accumulator = accFactory.get();

            if (type != AggregateType.REDUCE && !F.isEmpty(call.getArgList()))
                inAdapter = createInAdapter(accumulator);

            if (type != AggregateType.MAP)
                outAdapter = createOutAdapter(accumulator);

            return accumulator;
        }

        /** */
        @NotNull private Function<Object[], Object[]> createInAdapter(Accumulator accumulator) {
            List<RelDataType> inTypes = SqlTypeUtil.projectTypes(inputRowType, call.getArgList());
            List<RelDataType> outTypes = accumulator.argumentTypes(ctx.getTypeFactory());

            List<Function<Object, Object>> casts =
                Commons.transform(Pair.zip(inTypes, outTypes), WrappersFactoryImpl::cast);

            return new Function<Object[], Object[]>() {
                @Override public Object[] apply(Object[] args) {
                    for (int i = 0; i < args.length; i++)
                        args[i] = casts.get(i).apply(args[i]);
                    return args;
                }
            };
        }

        /** */
        @NotNull private Function<Object, Object> createOutAdapter(Accumulator accumulator) {
            RelDataType inType = accumulator.returnType(ctx.getTypeFactory());
            RelDataType outType = call.getType();

            return cast(inType, outType);
        }
    }
}
