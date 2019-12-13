/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.exec;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.Context;
import org.apache.calcite.interpreter.InterpreterUtils;
import org.apache.calcite.interpreter.JaninoRexCompiler;
import org.apache.calcite.interpreter.Scalar;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

/**
 *
 */
public class ScalarFactory {
    private final JaninoRexCompiler rexCompiler;
    private final RexBuilder builder;

    public ScalarFactory(RexBuilder builder) {
        rexCompiler = new JaninoRexCompiler(builder);
        this.builder = builder;
    }

    public <T> Predicate<T> filterPredicate(DataContext root, RexNode filter, RelDataType rowType) {
        Scalar scalar = rexCompiler.compile(ImmutableList.of(filter), rowType);
        Context ctx = InterpreterUtils.createContext(root);

        return new FilterPredicate<>(ctx, scalar);
    }

    public <T> Function<T, T> projectExpression(DataContext root, List<RexNode> projects, RelDataType rowType) {
        Scalar scalar = rexCompiler.compile(projects, rowType);
        Context ctx = InterpreterUtils.createContext(root);
        int count = projects.size();

        return new ProjectExpression<>(ctx, scalar, count);
    }

    public <T> BiFunction<T, T, T> joinExpression(DataContext root, RexNode expression, RelDataType leftType, RelDataType rightType) {
        RelDataType rowType = combinedType(leftType, rightType);

        Scalar scalar = rexCompiler.compile(ImmutableList.of(expression), rowType);
        Context ctx = InterpreterUtils.createContext(root);
        ctx.values = new Object[rowType.getFieldCount()];

        return new JoinExpression<>(ctx, scalar);
    }

    private RelDataType combinedType(RelDataType... types) {
        RelDataTypeFactory.Builder typeBuilder = new RelDataTypeFactory.Builder(builder.getTypeFactory());

        for (RelDataType type : types)
            typeBuilder.addAll(type.getFieldList());

        return typeBuilder.build();
    }

    private static class FilterPredicate<T> implements Predicate<T> {
        private final Context ctx;
        private final Scalar scalar;
        private final Object[] vals;

        private FilterPredicate(Context ctx, Scalar scalar) {
            this.ctx = ctx;
            this.scalar = scalar;

            vals = new Object[1];
        }

        @Override public boolean test(T r) {
            ctx.values = (Object[]) r;
            scalar.execute(ctx, vals);
            return (Boolean) vals[0];
        }
    }

    private static class JoinExpression<T> implements BiFunction<T, T, T> {
        private final Object[] vals;
        private final Context ctx;
        private final Scalar scalar;

        private Object[] left0;

        private JoinExpression(Context ctx, Scalar scalar) {
            this.ctx = ctx;
            this.scalar = scalar;

            vals = new Object[1];
        }

        @Override public T apply(T left, T right) {
            if (left0 != left) {
                left0 = (Object[]) left;
                System.arraycopy(left0, 0, ctx.values, 0, left0.length);
            }

            Object[] right0 = (Object[]) right;
            System.arraycopy(right0, 0, ctx.values, left0.length, right0.length);

            scalar.execute(ctx, vals);

            if ((Boolean) vals[0])
                return (T) Arrays.copyOf(ctx.values, ctx.values.length);

            return null;
        }
    }

    private static class ProjectExpression<T> implements Function<T, T> {
        private final Context ctx;
        private final Scalar scalar;
        private final int count;

        private ProjectExpression(Context ctx, Scalar scalar, int count) {
            this.ctx = ctx;
            this.scalar = scalar;
            this.count = count;
        }

        @Override public T apply(T r) {
            ctx.values = (Object[]) r;
            Object[] res = new Object[count];
            scalar.execute(ctx, res);

            return (T) res;
        }
    }
}
