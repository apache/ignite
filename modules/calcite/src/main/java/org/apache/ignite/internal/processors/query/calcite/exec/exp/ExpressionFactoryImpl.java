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

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Primitives;
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator.InputGetter;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorsFactory;
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
public class ExpressionFactoryImpl<Row> implements ExpressionFactory<Row> {
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
    private final ExecutionContext<Row> ctx;

    /** */
    public ExpressionFactoryImpl(ExecutionContext<Row> ctx, IgniteTypeFactory typeFactory, SqlConformance conformance) {
        this.ctx = ctx;
        this.typeFactory = typeFactory;
        this.conformance = conformance;

        rexBuilder = new RexBuilder(this.typeFactory);
        emptyType = new RelDataTypeFactory.Builder(this.typeFactory).build();
    }

    /** {@inheritDoc} */
    @Override public Supplier<List<AccumulatorWrapper<Row>>> accumulatorsFactory(AggregateNode.AggregateType type,
        List<AggregateCall> calls, RelDataType rowType) {
        return new AccumulatorsFactory<>(ctx, type, calls, rowType);
    }

    /** {@inheritDoc} */
    @Override public Comparator<Row> comparator(RelCollation collation) {
        if (collation == null || collation.getFieldCollations().isEmpty())
            return null;
        else if (collation.getFieldCollations().size() == 1)
            return comparator(collation.getFieldCollations().get(0));
        return Ordering.compound(collation.getFieldCollations()
            .stream()
            .map(this::comparator)
            .collect(Collectors.toList()));
    }

    /** */
    @SuppressWarnings("rawtypes")
    private Comparator<Row> comparator(RelFieldCollation fieldCollation) {
        final int nullComparison = fieldCollation.nullDirection.nullComparison;
        final int x = fieldCollation.getFieldIndex();
        RowHandler<Row> handler = ctx.rowHandler();

        if (fieldCollation.direction == RelFieldCollation.Direction.ASCENDING) {
            return (o1, o2) -> {
                final Comparable c1 = (Comparable)handler.get(x, o1);
                final Comparable c2 = (Comparable)handler.get(x, o2);
                return RelFieldCollation.compare(c1, c2, nullComparison);
            };
        }

        return (o1, o2) -> {
            final Comparable c1 = (Comparable)handler.get(x, o1);
            final Comparable c2 = (Comparable)handler.get(x, o2);
            return RelFieldCollation.compare(c2, c1, -nullComparison);
        };
    }

    /** {@inheritDoc} */
    @Override public Predicate<Row> predicate(RexNode filter, RelDataType rowType) {
        return new PredicateImpl(scalar(filter, rowType));
    }

    /** {@inheritDoc} */
    @Override public Function<Row, Row> project(List<RexNode> projects, RelDataType rowType) {
        return new ProjectImpl(scalar(projects, rowType), ctx.rowHandler().factory(typeFactory, RexUtil.types(projects)));
    }

    /** {@inheritDoc} */
    @Override public Supplier<Row> rowSource(List<RexNode> values) {
        return new ValuesImpl(scalar(values, null), ctx.rowHandler().factory(typeFactory, RexUtil.types(values)));
    }

    /** {@inheritDoc} */
    @Override public Iterable<Row> values(List<RexLiteral> values, RelDataType rowType) {
        RowHandler<Row> handler = ctx.rowHandler();
        RowFactory<Row> factory = handler.factory(typeFactory, rowType);

        int columns = rowType.getFieldCount();
        assert values.size() % columns == 0;

        List<Class<?>> types = new ArrayList<>(columns);
        for (RelDataType type : RelOptUtil.getFieldTypeList(rowType))
            types.add(Primitives.wrap((Class<?>)typeFactory.getJavaClass(type)));

        List<Row> rows = new ArrayList<>(values.size() / columns);
        Row currRow = null;
        for (int i = 0; i < values.size(); i++) {
            int field = i % columns;

            if (field == 0)
                rows.add(currRow = factory.create());

            RexLiteral literal = values.get(i);

            handler.set(field, currRow, literal.getValueAs(types.get(field)));
        }

        return rows;
    }

    /** {@inheritDoc} */
    @Override public Scalar scalar(List<RexNode> nodes, RelDataType type) {
        return SCALAR_CACHE.computeIfAbsent(digest(nodes, type), k -> compile(nodes, type));
    }

    /** */
    private Scalar compile(Iterable<RexNode> nodes, RelDataType type) {
        if (type == null)
            type = emptyType;

        RexProgramBuilder programBuilder = new RexProgramBuilder(type, rexBuilder);

        for (RexNode node : nodes)
            programBuilder.addProject(node, null);

        RexProgram program = programBuilder.getProgram();

        BlockBuilder builder = new BlockBuilder();

        ParameterExpression ctx_ =
            Expressions.parameter(ExecutionContext.class, "ctx");

        ParameterExpression in_ =
            Expressions.parameter(Object.class, "in");

        ParameterExpression out_ =
            Expressions.parameter(Object.class, "out");

        Expression hnd_ = builder.append("hnd",
            Expressions.call(ctx_,
                IgniteMethod.CONTEXT_ROW_HANDLER.method()));

        InputGetter inputGetter = new FieldGetter(hnd_, in_, type);

        Function1<String, InputGetter> correlates = new CorrelatesBuilder(builder, ctx_, hnd_).build(nodes);

        List<Expression> projects = RexToLixTranslator.translateProjects(program, typeFactory, conformance,
            builder, null, ctx_, inputGetter, correlates);

        for (int i = 0; i < projects.size(); i++) {
            builder.add(
                Expressions.statement(
                    Expressions.call(hnd_,
                        IgniteMethod.ROW_HANDLER_SET.method(),
                            Expressions.constant(i), out_, projects.get(i))));
        }

        MethodDeclaration decl = Expressions.methodDecl(
            Modifier.PUBLIC, void.class, IgniteMethod.SCALAR_EXECUTE.method().getName(),
            ImmutableList.of(ctx_, in_, out_), builder.toBlock());

        return Commons.compile(Scalar.class, Expressions.toString(F.asList(decl), "\n", false));
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
    private class PredicateImpl implements Predicate<Row> {
        /** */
        private final Scalar scalar;

        /** */
        private final Row out;

        /** */
        private final RowHandler<Row> handler;

        /**
         * @param scalar Scalar.
         */
        private PredicateImpl(Scalar scalar) {
            this.scalar = scalar;
            handler = ctx.rowHandler();
            out = handler.factory(typeFactory, typeFactory.createJavaType(Boolean.class)).create();
        }

        /** {@inheritDoc} */
        @Override public boolean test(Row r) {
            scalar.execute(ctx, r, out);

            return Boolean.TRUE == handler.get(0, out);
        }
    }

    /** */
    private class ProjectImpl implements Function<Row, Row> {
        /** */
        private final Scalar scalar;

        /** */
        private final RowFactory<Row> factory;

        /**
         * @param scalar Scalar.
         * @param factory Row factory.
         */
        private ProjectImpl(Scalar scalar, RowFactory<Row> factory) {
            this.scalar = scalar;
            this.factory = factory;
        }

        /** {@inheritDoc} */
        @Override public Row apply(Row r) {
            Row res = factory.create();
            scalar.execute(ctx, r, res);

            return res;
        }
    }

    /** */
    private class ValuesImpl implements Supplier<Row> {
        /** */
        private final Scalar scalar;

        /** */
        private final RowFactory<Row> factory;

        /** */
        private ValuesImpl(Scalar scalar, RowFactory<Row> factory) {
            this.scalar = scalar;
            this.factory = factory;
        }

        /** {@inheritDoc} */
        @Override public Row get() {
            Row res = factory.create();
            scalar.execute(ctx, null, res);

            return res;
        }
    }

    /** */
    private class FieldGetter implements InputGetter {
        /** */
        private final Expression hnd_;

        /** */
        private final Expression row_;

        /** */
        private final RelDataType rowType;

        /** */
        private FieldGetter(Expression hnd_, Expression row_, RelDataType rowType) {
            this.hnd_ = hnd_;
            this.row_ = row_;
            this.rowType = rowType;
        }

        /** {@inheritDoc} */
        @Override public Expression field(BlockBuilder list, int index, Type desiredType) {
            Expression row_ = list.append("row", this.row_);

            Expression field = Expressions.call(hnd_,
                IgniteMethod.ROW_HANDLER_GET.method(),
                    Expressions.constant(index), row_);

            Type fieldType = typeFactory.getJavaClass(rowType.getFieldList().get(index).getType());

            if (desiredType == null) {
                desiredType = fieldType;
                fieldType = Object.class;
            } else if (fieldType != java.sql.Date.class
                && fieldType != java.sql.Time.class
                && fieldType != java.sql.Timestamp.class) {
                fieldType = Object.class;
            }

            return EnumUtils.convert(field, fieldType, desiredType);
        }
    }

    /** */
    private class CorrelatesBuilder extends RexShuttle {
        /** */
        private final BlockBuilder builder;

        /** */
        private final Expression ctx_;

        /** */
        private final Expression hnd_;

        /** */
        private Map<String, FieldGetter> correlates;

        /** */
        public CorrelatesBuilder(BlockBuilder builder, Expression ctx_, Expression hnd_) {
            this.builder = builder;
            this.hnd_ = hnd_;
            this.ctx_ = ctx_;
        }

        /** */
        public Function1<String, InputGetter> build(Iterable<RexNode> nodes) {
            try {
                for (RexNode node : nodes)
                    node.accept(this);

                return correlates == null ? null : correlates::get;
            }
            finally {
                correlates = null;
            }
        }

        /** {@inheritDoc} */
        @Override public RexNode visitCorrelVariable(RexCorrelVariable variable) {
            Expression corr_ = builder.append("corr",
                Expressions.call(ctx_, IgniteMethod.CONTEXT_GET_CORRELATED_VALUE.method(),
                    Expressions.constant(variable.id.getId())));

            if (correlates == null)
                correlates = new HashMap<>();

            correlates.put(variable.getName(), new FieldGetter(hnd_, corr_, variable.getType()));

            return variable;
        }
    }
}
