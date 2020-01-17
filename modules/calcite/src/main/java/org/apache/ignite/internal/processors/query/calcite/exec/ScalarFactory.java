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

package org.apache.ignite.internal.processors.query.calcite.exec;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.Context;
import org.apache.calcite.interpreter.InterpreterUtils;
import org.apache.calcite.interpreter.JaninoRexCompiler;
import org.apache.calcite.interpreter.Scalar;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang.text.StrBuilder;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Implements rex expression into a function object. Uses JaninoRexCompiler under the hood.
 * Each expression compiles into a class and a wrapper over it is returned.
 */
public class ScalarFactory {
    /** */
    private static final int CACHE_SIZE = 1024;

    /** */
    private static final Map<String, Scalar> CACHE = new GridBoundedConcurrentLinkedHashMap<>(CACHE_SIZE);

    /** */
    private final ExecutionContext ctx;

    /** */
    private final JaninoRexCompiler rexCompiler;

    /** */
    private final ExceptionHandler handler;

    /** */
    public ScalarFactory(ExecutionContext ctx) {
        this.ctx = ctx;
        rexCompiler = new JaninoRexCompiler(new RexBuilder(ctx.getTypeFactory()));
        handler = new ExceptionHandler(
            ctx.parent().kernal().failure(),
            ctx.parent().logger().getLogger(ScalarFactory.class));
    }

    /**
     * Creates a comparator for given data type and collations. Mainly used for sorted exchange.
     *
     * @param root Execution context, holds a planner context, query and its parameters,
     *             execution specific variables (like queryId, current user, session, etc).
     * @param collations Collations.
     * @param rowType Input row type.
     * @return Row comparator.
     */
    public <T> Comparator<T> comparator(DataContext root, List<RelCollation> collations, RelDataType rowType) {
        return null; // TODO
    }

    /**
     * Creates a Filter predicate.
     * @param root Execution context, holds a planner context, query and its parameters,
     *             execution specific variables (like queryId, current user, session, etc).
     * @param filter Filter expression.
     * @param rowType Input row type.
     * @return Filter predicate.
     */
    public <T> Predicate<T> filterPredicate(DataContext root, RexNode filter, RelDataType rowType) {
        Scalar scalar = scalar(ImmutableList.of(filter), rowType);
        Context ctx = InterpreterUtils.createContext(root);

        return new FilterPredicate<>(ctx, scalar, handler);
    }

    /**
     * Creates a Project function. Resulting function returns a row with different fields,
     * fields order, fields types, etc.
     * @param root Execution context, holds a planner context, query and its parameters,
     *             execution specific variables (like queryId, current user, session, etc).
     * @param projects Projection expressions.
     * @param rowType Input row type.
     * @return Project function.
     */
    public <T> Function<T, T> projectExpression(DataContext root, List<RexNode> projects, RelDataType rowType) {
        Scalar scalar = scalar(projects, rowType);
        Context ctx = InterpreterUtils.createContext(root);
        int count = projects.size();

        return new ProjectExpression<>(ctx, scalar, count, handler);
    }

    /**
     * Creates a Join expression. Function consumes two rows and returns non null value in case the rows satisfy join condition.
     * @param root Execution context, holds a planner context, query and its parameters,
     *             execution specific variables (like queryId, current user, session, etc).
     * @param expression Join expression (condition).
     * @param leftType Left input row type.
     * @param rightType Right input row type.
     * @return Join function.
     */
    public <T> BiFunction<T, T, T> joinExpression(DataContext root, RexNode expression, RelDataType leftType, RelDataType rightType) {
        RelDataType rowType = combinedType(leftType, rightType);
        Scalar scalar = scalar(ImmutableList.of(expression), rowType);
        Context ctx = InterpreterUtils.createContext(root);
        ctx.values = new Object[rowType.getFieldCount()];

        return new JoinExpression<>(ctx, scalar, handler);
    }

    /** */
    private RelDataType combinedType(RelDataType... types) {
        RelDataTypeFactory.Builder typeBuilder = new RelDataTypeFactory.Builder(ctx.getTypeFactory());

        for (RelDataType type : types)
            typeBuilder.addAll(type.getFieldList());

        return typeBuilder.build();
    }

    /** */
    private Scalar scalar(List<RexNode> nodes, RelDataType type) {
        assert !F.isEmpty(nodes);

        return CACHE.computeIfAbsent(cacheKey(nodes, type), k -> rexCompiler.compile(nodes, type));
    }

    /** */
    private String cacheKey(List<RexNode> nodes, RelDataType type) {
        StrBuilder b = new StrBuilder("[").append(F.first(nodes));
        for (int i = 1; i < nodes.size(); i++)
            b.append(";").append(nodes.get(i));
        b.append("]:").append(type.getFullTypeString());

        return b.toString();
    }

    /** */
    private static class FilterPredicate<T> implements Predicate<T> {
        /** */
        private final Context ctx;

        /** */
        private final Scalar scalar;

        /** */
        private final ExceptionHandler handler;

        /** */
        private final Object[] vals;

        /**
         * @param ctx Interpreter context.
         * @param scalar Scalar.
         */
        private FilterPredicate(Context ctx, Scalar scalar, ExceptionHandler handler) {
            this.ctx = ctx;
            this.scalar = scalar;
            this.handler = handler;

            vals = new Object[1];
        }

        /** {@inheritDoc} */
        @Override public boolean test(T r) {
            try {
                ctx.values = (Object[]) r;
                scalar.execute(ctx, vals);
                return (Boolean) vals[0];
            }
            catch (Throwable e) {
                handler.onException(e);

                throw e;
            }
        }
    }

    /** */
    private static class JoinExpression<T> implements BiFunction<T, T, T> {
        /** */
        private final Object[] vals;

        /** */
        private final Context ctx;

        /** */
        private final Scalar scalar;

        /** */
        private final ExceptionHandler handler;

        /** */
        private Object[] left0;

        /**
         * @param ctx Interpreter context.
         * @param scalar Scalar.
         */
        private JoinExpression(Context ctx, Scalar scalar, ExceptionHandler handler) {
            this.ctx = ctx;
            this.scalar = scalar;
            this.handler = handler;

            vals = new Object[1];
        }

        /** {@inheritDoc} */
        @Override public T apply(T left, T right) {
            try {
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
            catch (Throwable e) {
                handler.onException(e);

                throw e;
            }
        }
    }

    /** */
    private static class ProjectExpression<T> implements Function<T, T> {
        /** */
        private final Context ctx;

        /** */
        private final Scalar scalar;

        /** */
        private final int count;

        /** */
        private final ExceptionHandler handler;

        /**
         * @param ctx Interpreter context.
         * @param scalar Scalar.
         * @param count Resulting columns count.
         */
        private ProjectExpression(Context ctx, Scalar scalar, int count, ExceptionHandler handler) {
            this.ctx = ctx;
            this.scalar = scalar;
            this.count = count;
            this.handler = handler;
        }

        /** {@inheritDoc} */
        @Override public T apply(T r) {
            try {
                ctx.values = (Object[]) r;
                Object[] res = new Object[count];
                scalar.execute(ctx, res);

                return (T) res;
            }
            catch (Throwable e) {
                handler.onException(e);

                throw e;
            }
        }
    }

    /** */
    private static class ExceptionHandler {
        /** */
        private final FailureProcessor failure;

        /** */
        private final IgniteLogger log;

        /** */
        private ExceptionHandler(@Nullable FailureProcessor failure, IgniteLogger log) {
            this.failure = failure;
            this.log = log;
        }

        /** */
        void onException(Throwable ex) {
            U.error(log, ex, ex);

            if (failure != null)
                failure.process(new FailureContext(FailureType.CRITICAL_ERROR, ex));
        }
    }
}
