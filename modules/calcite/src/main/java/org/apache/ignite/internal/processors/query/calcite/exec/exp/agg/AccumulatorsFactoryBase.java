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

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumUtils;
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
import org.apache.ignite.internal.processors.query.calcite.exec.exp.IgniteRexBuilder;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.processors.query.calcite.util.TypeUtils.createRowType;

/**
 * Base class for accumulators / windows function factory.
 * Provides common logic for agruments cast.
 * todo: rename/move to another package?
 */
public abstract class AccumulatorsFactoryBase<Row> {
    /** */
    private static final LoadingCache<Pair<RelDataType, RelDataType>, Function<Object, Object>> CACHE =
        CacheBuilder.newBuilder().build(CacheLoader.from(AccumulatorsFactoryBase::cast0));

    /** */
    public interface CastFunction extends Function<Object, Object> {
        /** {@inheritDoc} */
        @Override Object apply(Object o);
    }

    /** */
    static Function<Object, Object> cast(RelDataType from, RelDataType to) {
        assert !from.isStruct();
        assert !to.isStruct();

        return cast(Pair.of(from, to));
    }

    /** */
    static Function<Object, Object> cast(Pair<RelDataType, RelDataType> types) {
        try {
            return CACHE.get(types);
        }
        catch (ExecutionException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    private static Function<Object, Object> cast0(Pair<RelDataType, RelDataType> types) {
        IgniteTypeFactory typeFactory = Commons.typeFactory();

        RelDataType from = types.left;
        RelDataType to = types.right;

        Class<?> fromType = Primitives.wrap((Class<?>)typeFactory.getJavaClass(from));
        Class<?> toType = Primitives.wrap((Class<?>)typeFactory.getJavaClass(to));

        if (toType.isAssignableFrom(fromType))
            return Function.identity();

        if (Void.class == toType)
            return o -> null;

        return compileCast(typeFactory, from, to);
    }

    /** */
    private static Function<Object, Object> compileCast(IgniteTypeFactory typeFactory, RelDataType from,
        RelDataType to) {
        RelDataType rowType = createRowType(typeFactory, from);
        ParameterExpression in_ = Expressions.parameter(Object.class, "in");

        RexToLixTranslator.InputGetter getter =
            new RexToLixTranslator.InputGetterImpl(
                ImmutableMap.of(
                    EnumUtils.convert(in_, Object.class, typeFactory.getJavaClass(from)),
                        PhysTypeImpl.of(typeFactory, rowType,
                            JavaRowFormat.SCALAR, false)));

        RexBuilder builder = new IgniteRexBuilder(typeFactory);
        RexProgramBuilder programBuilder = new RexProgramBuilder(rowType, builder);
        RexNode cast = builder.makeCast(to, builder.makeInputRef(from, 0));
        programBuilder.addProject(cast, null);
        RexProgram program = programBuilder.getProgram();
        BlockBuilder list = new BlockBuilder();
        List<Expression> projects = RexToLixTranslator.translateProjects(program, typeFactory, SqlConformanceEnum.DEFAULT,
            list, null, null, DataContext.ROOT, getter, null);
        list.add(projects.get(0));

        MethodDeclaration decl = Expressions.methodDecl(
            Modifier.PUBLIC, Object.class, "apply", ImmutableList.of(in_), list.toBlock());

        return Commons.compile(CastFunction.class, Expressions.toString(F.asList(decl), "\n", false));
    }

    private final ExecutionContext<Row> ctx;

    /** */
    protected AccumulatorsFactoryBase(ExecutionContext<Row> ctx) {
        this.ctx = ctx;
    }

    /** */
    @NotNull protected Function<Row, Row> createInAdapter(
        AggregateCall call,
        RelDataType inputRowType,
        List<RelDataType> argTypes,
        boolean createRow
    ) {
        if (F.isEmpty(call.getArgList()))
            return Function.identity();

        List<RelDataType> inTypes = SqlTypeUtil.projectTypes(inputRowType, call.getArgList());

        if (call.getArgList().size() > argTypes.size()) {
            throw new AssertionError("Unexpected number of arguments: " +
                "expected=" + argTypes.size() + ", actual=" + inTypes.size());
        }

        if (call.ignoreNulls())
            inTypes = Commons.transform(inTypes, this::nonNull);

        List<Function<Object, Object>> casts =
            Commons.transform(Pair.zip(inTypes, argTypes), AccumulatorsFactory::cast);

        final boolean ignoreNulls = call.ignoreNulls();

        final int[] argMapping = new int[Collections.max(call.getArgList()) + 1];
        Arrays.fill(argMapping, -1);

        for (int i = 0; i < call.getArgList().size(); ++i)
            argMapping[call.getArgList().get(i)] = i;

        return new Function<Row, Row>() {
            final RowHandler<Row> hnd = ctx.rowHandler();

            final RowHandler.RowFactory<Row> rowFac = hnd.factory(ctx.getTypeFactory(), inputRowType);

            final Row reuseRow = createRow ? null : rowFac.create();

            @Override public Row apply(Row in) {
                Row out = createRow ? rowFac.create() : reuseRow;

                for (int i = 0; i < hnd.columnCount(in); ++i) {
                    Object val = hnd.get(i, in);

                    if (ignoreNulls && val == null)
                        return null;

                    int idx = i < argMapping.length ? argMapping[i] : -1;
                    if (idx != -1)
                        val = casts.get(idx).apply(val);

                    hnd.set(i, out, val);
                }

                return out;
            }
        };
    }

    /** */
    @NotNull protected Function<Object, Object> createOutAdapter(AggregateCall call, RelDataType inType) {
        RelDataType outType = call.getType();
        return cast(inType, outType);
    }

    /** */
    private RelDataType nonNull(RelDataType type) {
        return ctx.getTypeFactory().createTypeWithNullability(type, false);
    }
}
