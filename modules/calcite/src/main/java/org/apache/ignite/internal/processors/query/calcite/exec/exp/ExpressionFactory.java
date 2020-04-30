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

package org.apache.ignite.internal.processors.query.calcite.exec.exp;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import java.lang.reflect.Modifier;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.WrappersFactoryImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.AggregateNode;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMethod;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Implements rex expression into a function object. Uses JaninoRexCompiler under the hood.
 * Each expression compiles into a class and a wrapper over it is returned.
 */
@SuppressWarnings({"rawtypes"})
public class ExpressionFactory {
    /** */
    private static final Map<String, Scalar> SCALAR_CACHE = new GridBoundedConcurrentLinkedHashMap<>(1024);

    /** */
    private final IgniteTypeFactory typeFactory;

    /** */
    private final SqlConformance conformance;

    /** */
    private final RexBuilder rexBuilder;

    /** */
    private final RelDataType emptyType;

    /** */
    public ExpressionFactory(IgniteTypeFactory typeFactory, SqlConformance conformance) {
        this.typeFactory = typeFactory;
        this.conformance = conformance;

        rexBuilder = new RexBuilder(typeFactory);
        emptyType = new RelDataTypeFactory.Builder(typeFactory).build();
    }

    /** */
    public IgniteTypeFactory typeFactory() {
        return typeFactory;
    }

    /** */
    public RexBuilder rexBuilder() {
        return rexBuilder;
    }

    /** */
    public Supplier<List<AccumulatorWrapper>> wrappersFactory(ExecutionContext root, RowHandler handler, AggregateNode.AggregateType type, List<AggregateCall> calls, RelDataType rowType) {
        return new WrappersFactoryImpl(root, type, handler, calls, rowType);
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
    public <T> Comparator<T> comparator(ExecutionContext root, List<RelCollation> collations, RelDataType rowType) {
        return null; // TODO
    }

    /**
     * Creates a Filter predicate.
     * @param ctx Execution context, holds a planner context, query and its parameters,
     *             execution specific variables (like queryId, current user, session, etc).
     * @param filter Filter expression.
     * @param rowType Input row type.
     * @return Filter predicate.
     */
    public <T> Predicate<T> predicate(ExecutionContext ctx, RexNode filter, RelDataType rowType) {
        return new PredicateImpl<>(ctx, scalar(filter, rowType));
    }

    /**
     * Creates a Project function. Resulting function returns a row with different fields,
     * fields order, fields types, etc.
     * @param ctx Execution context, holds a planner context, query and its parameters,
     *             execution specific variables (like queryId, current user, session, etc).
     * @param projects Projection expressions.
     * @param rowType Input row type.
     * @return Project function.
     */
    public <T> Function<T, T> project(ExecutionContext ctx, List<RexNode> projects, RelDataType rowType) {
        return new ProjectImpl<>(ctx, scalar(projects, rowType), projects.size());
    }

    /**
     * Creates a Values relational node rows source.
     *
     * @param ctx Execution context, holds a planner context, query and its parameters,
     *             execution specific variables (like queryId, current user, session, etc).
     * @param values Values.
     * @param rowLen Row length.
     * @return Values relational node rows source.
     */
    public <T> Iterable<T> values(ExecutionContext ctx, List<RexLiteral> values, int rowLen) {
        Object[] out = new Object[values.size()];

        for (int i = 0; i < values.size(); i++) {
            RexLiteral literal = values.get(i);
            out[i] = literal.getValueAs(Primitives.wrap((Class<?>)typeFactory.getJavaClass(literal.getType())));
        }

        return () -> new ValuesIterator<>(out, rowLen);
    }

    /** */
    public Scalar scalar(RexNode node, RelDataType type) {
        return scalar(ImmutableList.of(node), type);
    }

    /** */
    public Scalar scalar(List<RexNode> nodes, RelDataType type) {
        return SCALAR_CACHE.computeIfAbsent(digest(nodes, type), k -> compile(nodes, type));
    }

    /** */
    private Scalar compile(List<RexNode> nodes, RelDataType type) {
        if (type == null)
            type = emptyType;

        ParameterExpression context_ =
            Expressions.parameter(ExecutionContext.class, "ctx");

        ParameterExpression inputValues_ =
            Expressions.parameter(Object[].class, "in");

        ParameterExpression outputValues_ =
            Expressions.parameter(Object[].class, "out");

        RexToLixTranslator.InputGetter inputGetter =
            new RexToLixTranslator.InputGetterImpl(
                ImmutableList.of(
                    Pair.of(inputValues_,
                        PhysTypeImpl.of(typeFactory, type,
                            JavaRowFormat.ARRAY, false))));

        RexProgramBuilder programBuilder = new RexProgramBuilder(type, rexBuilder);

        for (RexNode node : nodes)
            programBuilder.addProject(node, null);

        RexProgram program = programBuilder.getProgram();

        BlockBuilder builder = new BlockBuilder();

        List<org.apache.calcite.linq4j.tree.Expression> list = RexToLixTranslator.translateProjects(program,
            typeFactory, conformance, builder, null, context_, inputGetter, null);

        for (int i = 0; i < list.size(); i++) {
            builder.add(
                Expressions.statement(
                    Expressions.assign(
                        Expressions.arrayIndex(outputValues_,
                            Expressions.constant(i)),
                        list.get(i))));
        }

        builder.add(outputValues_); // return out

        MethodDeclaration declaration = Expressions.methodDecl(
            Modifier.PUBLIC, Object[].class, IgniteMethod.SCALAR_EXECUTE.method().getName(),
            ImmutableList.of(context_, inputValues_, outputValues_), builder.toBlock());

        return Commons.compile(Scalar.class, Expressions.toString(F.asList(declaration), "\n", false));
    }

    /** */
    private String digest(List<RexNode> nodes, RelDataType type) {
        StringBuilder b = new StringBuilder();

        b.append('[');

        for (int i = 0; i < nodes.size(); i++) {
            if (i > 0)
                b.append(';');

            b.append(nodes.get(i));
        }

        b.append(']');

        if (type != null)
            b.append(':').append(type.getFullTypeString());

        return b.toString();
    }

    /** */
    private static class PredicateImpl<T> implements Predicate<T> {
        /** */
        private final ExecutionContext ctx;

        /** */
        private final Scalar scalar;

        /** */
        private final Object[] out;

        /**
         * @param ctx Interpreter context.
         * @param scalar Scalar.
         */
        private PredicateImpl(ExecutionContext ctx, Scalar scalar) {
            this.ctx = ctx;
            this.scalar = scalar;

            out = new Object[1];
        }

        /** {@inheritDoc} */
        @Override public boolean test(T r) {
            return (Boolean) scalar.execute(ctx, (Object[]) r, out)[0];
        }
    }

    /** */
    private static class ProjectImpl<T> implements Function<T, T> {
        /** */
        private final ExecutionContext ctx;

        /** */
        private final Scalar scalar;

        /** */
        private final int count;

        /**
         * @param ctx Interpreter context.
         * @param scalar Scalar.
         * @param count Resulting columns count.
         */
        private ProjectImpl(ExecutionContext ctx, Scalar scalar, int count) {
            this.ctx = ctx;
            this.scalar = scalar;
            this.count = count;
        }

        /** {@inheritDoc} */
        @Override public T apply(T r) {
            return (T) scalar.execute(ctx, (Object[]) r, new Object[count]);
        }
    }

    /** */
    private static class ValuesIterator<T> implements Iterator<T> {
        /** */
        private final Object[] values;

        /** */
        private final int rowLen;

        /** */
        private int idx;

        /** */
        private ValuesIterator(Object[] values, int rowLen) {
            this.values = values;
            this.rowLen = rowLen;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return idx < values.length;
        }

        /** {@inheritDoc} */
        @Override public T next() {
            if (!hasNext())
                throw new NoSuchElementException();

            Object[] res = new Object[rowLen];
            System.arraycopy(values, idx, res, 0, rowLen);
            idx += rowLen;

            return (T) res;
        }
    }
}
