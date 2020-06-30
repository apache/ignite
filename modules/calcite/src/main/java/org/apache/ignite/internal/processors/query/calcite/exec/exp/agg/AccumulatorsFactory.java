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
import com.google.common.primitives.Primitives;
import java.lang.reflect.Modifier;
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
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.AggregateNode.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.processors.query.calcite.util.TypeUtils.createRowType;

/** */
public class AccumulatorsFactory<Row> implements Supplier<List<AccumulatorWrapper<Row>>> {
    /** */
    private static final LoadingCache<Pair<RelDataType,RelDataType>, Function<Object,Object>> CACHE =
        CacheBuilder.newBuilder().build(CacheLoader.from(AccumulatorsFactory::cast0));

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
        IgniteTypeFactory typeFactory = PlanningContext.empty().typeFactory();

        RelDataType from = types.left;
        RelDataType to = types.right;

        Class<?> fromType = Primitives.wrap((Class<?>)typeFactory.getJavaClass(from));
        Class<?> toType = Primitives.wrap((Class<?>)typeFactory.getJavaClass(to));

        if (fromType == toType)
            return Function.identity();

        return compileCast(typeFactory, from, to);
    }

    /** */
    private static Function<Object, Object> compileCast(IgniteTypeFactory typeFactory, RelDataType from,
        RelDataType to) {
        RelDataType rowType = createRowType(typeFactory, from);
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
    public AccumulatorsFactory(ExecutionContext<Row> ctx, AggregateType type,
        List<AggregateCall> aggCalls, RelDataType inputRowType) {
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
        private Supplier<Accumulator> accFactory;

        /** */
        private final AggregateCall call;

        /** */
        private Function<Object[], Object[]> inAdapter;

        /** */
        private Function<Object, Object> outAdapter;

        /** */
        private WrapperPrototype(AggregateCall call) {
            this.call = call;
        }

        /** {@inheritDoc} */
        @Override public AccumulatorWrapper<Row> get() {
            Accumulator accumulator = accumulator();

            return new AccumulatorWrapperImpl(accumulator, call, inAdapter, outAdapter);
        }

        /** */
        @NotNull private Accumulator accumulator() {
            if (accFactory != null)
                return accFactory.get();

            // init factory and adapters
            accFactory = Accumulators.accumulatorFactory(call);
            Accumulator accumulator = accFactory.get();

            inAdapter = createInAdapter(accumulator);
            outAdapter = createOutAdapter(accumulator);

            return accumulator;
        }

        /** */
        @NotNull private Function<Object[], Object[]> createInAdapter(Accumulator accumulator) {
            if (type == AggregateType.REDUCE || F.isEmpty(call.getArgList()))
                return Function.identity();

            List<RelDataType> inTypes = SqlTypeUtil.projectTypes(inputRowType, call.getArgList());
            List<RelDataType> outTypes = accumulator.argumentTypes(ctx.getTypeFactory());

            if (call.ignoreNulls())
                inTypes = Commons.transform(inTypes, this::nonNull);

            List<Function<Object, Object>> casts =
                Commons.transform(Pair.zip(inTypes, outTypes), AccumulatorsFactory::cast);

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
            if (type == AggregateType.MAP)
                return Function.identity();

            RelDataType inType = accumulator.returnType(ctx.getTypeFactory());
            RelDataType outType = call.getType();

            return cast(inType, outType);
        }

        /** */
        private RelDataType nonNull(RelDataType type) {
            return ctx.getTypeFactory().createTypeWithNullability(type, false);
        }
    }

    /** */
    private final class AccumulatorWrapperImpl implements AccumulatorWrapper<Row> {
        /** */
        private final Accumulator accumulator;

        /** */
        private final Function<Object[], Object[]> inAdapter;

        /** */
        private final Function<Object, Object> outAdapter;

        /** */
        private final List<Integer> argList;

        /** */
        private final int filterArg;

        /** */
        private final boolean ignoreNulls;

        /** */
        private final RowHandler<Row> handler;

        /** */
        AccumulatorWrapperImpl(Accumulator accumulator, AggregateCall call,
            Function<Object[], Object[]> inAdapter,
            Function<Object, Object> outAdapter) {
            this.accumulator = accumulator;
            this.inAdapter = inAdapter;
            this.outAdapter = outAdapter;

            argList = call.getArgList();
            ignoreNulls = call.ignoreNulls();
            filterArg = call.hasFilter() ? call.filterArg : -1;

            handler = ctx.rowHandler();
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            assert type != AggregateType.REDUCE;

            if (filterArg >= 0 && Boolean.TRUE != handler.get(filterArg, row))
                return;

            Object[] args = new Object[argList.size()];
            for (int i = 0; i < argList.size(); i++) {
                args[i] = handler.get(argList.get(i), row);

                if (ignoreNulls && args[i] == null)
                    return;
            }

            accumulator.add(inAdapter.apply(args));
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
            assert type == AggregateType.MAP;

            return accumulator;
        }
    }
}
