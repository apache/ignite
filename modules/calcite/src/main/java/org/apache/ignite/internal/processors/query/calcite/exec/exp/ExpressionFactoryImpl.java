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
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Primitives;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumUtils;
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
import org.apache.calcite.rex.RexFieldAccess;
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
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RexToLixTranslator.InputGetter;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorsFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMethod;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ValueCacheObject;
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
    @Override public Supplier<List<AccumulatorWrapper<Row>>> accumulatorsFactory(
        AggregateType type,
        List<AggregateCall> calls,
        RelDataType rowType
    ) {
        if (calls.isEmpty())
            return null;

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

    /** {@inheritDoc} */
    @Override public Comparator<Row> comparator(List<RelFieldCollation> left, List<RelFieldCollation> right) {
        if (F.isEmpty(left) || F.isEmpty(right) || left.size() != right.size())
            throw new IllegalArgumentException("Both inputs should be non-empty and have the same size: left="
                + (left != null ? left.size() : "null") + ", right=" + (right != null ? right.size() : "null"));

        List<Comparator<Row>> comparators = new ArrayList<>();

        for (int i = 0; i < left.size(); i++)
            comparators.add(comparator(left.get(i), right.get(i)));

        return Ordering.compound(comparators);
    }

    /** */
    @SuppressWarnings("rawtypes")
    private Comparator<Row> comparator(RelFieldCollation fieldCollation) {
        final int nullComparison = fieldCollation.nullDirection.nullComparison;
        final int x = fieldCollation.getFieldIndex();
        RowHandler<Row> handler = ctx.rowHandler();

        if (fieldCollation.direction == RelFieldCollation.Direction.ASCENDING) {
            return (o1, o2) -> {
                Object obj1 = handler.get(x, o1);
                Object obj2 = handler.get(x, o2);

                boolean o1Comparable = obj1 instanceof Comparable;
                boolean o2Comparable = obj2 instanceof Comparable;

                if (o1Comparable && o2Comparable) {
                    final Comparable c1 = (Comparable)obj1;
                    final Comparable c2 = (Comparable)obj2;
                    return RelFieldCollation.compare(c1, c2, nullComparison);
                }
                else {
                    if (obj1 == obj2)
                        return 0;
                    else if (obj1 == null)
                        return nullComparison;
                    else if (obj2 == null)
                        return -nullComparison;
                    else
                        return GridH2ValueCacheObject.compareHashOrBytes(obj1, obj2);
                }
            };
        }

        return (o1, o2) -> {
            final Comparable c1 = (Comparable)handler.get(x, o1);
            final Comparable c2 = (Comparable)handler.get(x, o2);
            return RelFieldCollation.compare(c2, c1, -nullComparison);
        };
    }

    /** */
    @SuppressWarnings("rawtypes")
    private Comparator<Row> comparator(RelFieldCollation left, RelFieldCollation right) {
        final int nullComparison = left.nullDirection.nullComparison;

        if (nullComparison != right.nullDirection.nullComparison)
            throw new IllegalArgumentException("Can't be compared: left=" + left + ", right=" + right);

        final int lIdx = left.getFieldIndex();
        final int rIdx = right.getFieldIndex();
        RowHandler<Row> handler = ctx.rowHandler();

        if (left.direction != right.direction)
            throw new IllegalArgumentException("Can't be compared: left=" + left + ", right=" + right);

        if (left.direction == RelFieldCollation.Direction.ASCENDING) {
            return (o1, o2) -> {
                final Comparable c1 = (Comparable)handler.get(lIdx, o1);
                final Comparable c2 = (Comparable)handler.get(rIdx, o2);
                return RelFieldCollation.compare(c1, c2, nullComparison);
            };
        }

        return (o1, o2) -> {
            final Comparable c1 = (Comparable)handler.get(lIdx, o1);
            final Comparable c2 = (Comparable)handler.get(rIdx, o2);
            return RelFieldCollation.compare(c2, c1, -nullComparison);
        };
    }

    /** {@inheritDoc} */
    @Override public Predicate<Row> predicate(RexNode filter, RelDataType rowType) {
        return new PredicateImpl(scalar(filter, rowType));
    }

    /** {@inheritDoc} */
    @Override public BiPredicate<Row, Row> biPredicate(RexNode filter, RelDataType rowType) {
        return new BiPredicateImpl(biScalar(filter, rowType));
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
    @Override public <T> Supplier<T> execute(RexNode node) {
        return new ValueImpl<T>(scalar(node, null), ctx.rowHandler().factory(typeFactory.getJavaClass(node.getType())));
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

    /**
     * Creates {@link SingleScalar}, a code-generated expressions evaluator.
     *
     * @param node Expression.
     * @param type Row type.
     * @return SingleScalar.
     */
    private SingleScalar scalar(RexNode node, RelDataType type) {
        return scalar(ImmutableList.of(node), type);
    }

    /**
     * Creates {@link SingleScalar}, a code-generated expressions evaluator.
     *
     * @param nodes Expressions.
     * @param type Row type.
     * @return SingleScalar.
     */
    public SingleScalar scalar(List<RexNode> nodes, RelDataType type) {
        return (SingleScalar)SCALAR_CACHE.computeIfAbsent(digest(nodes, type, false),
            k -> compile(nodes, type, false));
    }

    /**
     * Creates {@link BiScalar}, a code-generated expressions evaluator.
     *
     * @param node Expression.
     * @param type Row type.
     * @return BiScalar.
     */
    public BiScalar biScalar(RexNode node, RelDataType type) {
        ImmutableList<RexNode> nodes = ImmutableList.of(node);

        return (BiScalar)SCALAR_CACHE.computeIfAbsent(digest(nodes, type, true),
            k -> compile(nodes, type, true));
    }

    /** */
    private Scalar compile(Iterable<RexNode> nodes, RelDataType type, boolean biInParams) {
        if (type == null)
            type = emptyType;

        RexProgramBuilder programBuilder = new RexProgramBuilder(type, rexBuilder);

        for (RexNode node : nodes)
            programBuilder.addProject(node, null);

        RexProgram program = programBuilder.getProgram();

        BlockBuilder builder = new BlockBuilder();

        ParameterExpression ctx_ =
            Expressions.parameter(ExecutionContext.class, "ctx");

        ParameterExpression in1_ =
            Expressions.parameter(Object.class, "in1");

        ParameterExpression in2_ =
            Expressions.parameter(Object.class, "in2");

        ParameterExpression out_ =
            Expressions.parameter(Object.class, "out");

        builder.add(
            Expressions.declare(Modifier.FINAL, DataContext.ROOT, Expressions.convert_(ctx_, DataContext.class)));

        Expression hnd_ = builder.append("hnd",
            Expressions.call(ctx_,
                IgniteMethod.CONTEXT_ROW_HANDLER.method()));

        InputGetter inputGetter = biInParams ? new BiFieldGetter(hnd_, in1_, in2_, type) :
            new FieldGetter(hnd_, in1_, type);

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

        String methodName = biInParams ? IgniteMethod.BI_SCALAR_EXECUTE.method().getName() :
            IgniteMethod.SCALAR_EXECUTE.method().getName();

        ImmutableList<ParameterExpression> params = biInParams ? ImmutableList.of(ctx_, in1_, in2_, out_) :
            ImmutableList.of(ctx_, in1_, out_);

        MethodDeclaration decl = Expressions.methodDecl(
            Modifier.PUBLIC, void.class, methodName,
            params, builder.toBlock());

        Class<? extends Scalar> clazz = biInParams ? BiScalar.class : SingleScalar.class;

        return Commons.compile(clazz, Expressions.toString(F.asList(decl), "\n", false));
    }

    /** */
    private String digest(List<RexNode> nodes, RelDataType type, boolean biParam) {
        StringBuilder b = new StringBuilder();

        b.append('[');

        for (int i = 0; i < nodes.size(); i++) {
            if (i > 0)
                b.append(';');

            b.append(nodes.get(i));

            new RexShuttle() {
                @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
                    b.append(", fldIdx=").append(fieldAccess.getField().getIndex());

                    return super.visitFieldAccess(fieldAccess);
                }
            }.apply(nodes.get(i));
        }

        b.append(", biParam=").append(biParam);

        b.append(']');

        if (type != null)
            b.append(':').append(type.getFullTypeString());

        return b.toString();
    }

    /** */
    private abstract class AbstractScalarPredicate<T extends Scalar> {
        /** */
        protected final T scalar;

        /** */
        protected final Row out;

        /** */
        protected final RowHandler<Row> hnd;

        /**
         * @param scalar Scalar.
         */
        private AbstractScalarPredicate(T scalar) {
            this.scalar = scalar;
            hnd = ctx.rowHandler();
            out = hnd.factory(typeFactory, typeFactory.createJavaType(Boolean.class)).create();
        }
    }

    /** */
    private class PredicateImpl extends AbstractScalarPredicate<SingleScalar> implements Predicate<Row> {
        /**
         * @param scalar Scalar.
         */
        private PredicateImpl(SingleScalar scalar) {
            super(scalar);
        }

        /** {@inheritDoc} */
        @Override public boolean test(Row r) {
            scalar.execute(ctx, r, out);
            return Boolean.TRUE == hnd.get(0, out);
        }
    }

    /** */
    private class BiPredicateImpl extends AbstractScalarPredicate<BiScalar> implements BiPredicate<Row, Row> {
        /**
         * @param scalar Scalar.
         */
        private BiPredicateImpl(BiScalar scalar) {
            super(scalar);
        }

        /** {@inheritDoc} */
        @Override public boolean test(Row r1, Row r2) {
            scalar.execute(ctx, r1, r2, out);
            return Boolean.TRUE == hnd.get(0, out);
        }
    }

    /** */
    private class ProjectImpl implements Function<Row, Row> {
        /** */
        private final SingleScalar scalar;

        /** */
        private final RowFactory<Row> factory;

        /**
         * @param scalar Scalar.
         * @param factory Row factory.
         */
        private ProjectImpl(SingleScalar scalar, RowFactory<Row> factory) {
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
        private final SingleScalar scalar;

        /** */
        private final RowFactory<Row> factory;

        /** */
        private ValuesImpl(SingleScalar scalar, RowFactory<Row> factory) {
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
    private class ValueImpl<T> implements Supplier<T> {
        /** */
        private final SingleScalar scalar;

        /** */
        private final RowFactory<Row> factory;

        /** */
        private ValueImpl(SingleScalar scalar, RowFactory<Row> factory) {
            this.scalar = scalar;
            this.factory = factory;
        }

        /** {@inheritDoc} */
        @Override public T get() {
            Row res = factory.create();
            scalar.execute(ctx, null, res);

            return (T)ctx.rowHandler().get(0, res);
        }
    }

    /** */
    private class BiFieldGetter extends CommonFieldGetter {
        /** */
        private final Expression row2_;

        /** */
        private BiFieldGetter(Expression hnd_, Expression row1_, Expression row2_, RelDataType rowType) {
            super(hnd_, row1_, rowType);
            this.row2_ = row2_;
        }

        /** {@inheritDoc} */
        @Override protected Expression fillExpressions(BlockBuilder list, int index) {
            Expression row1_ = list.append("row1", this.row1_);
            Expression row2_ = list.append("row2", this.row2_);

            Expression field = Expressions.call(
                IgniteMethod.ROW_HANDLER_BI_GET.method(), hnd_,
                Expressions.constant(index), row1_, row2_);

            return field;
        }
    }

    /** */
    private class FieldGetter extends CommonFieldGetter {
        /** */
        private FieldGetter(Expression hnd_, Expression row1_, RelDataType rowType) {
            super(hnd_, row1_, rowType);
        }

        /** {@inheritDoc} */
        @Override protected Expression fillExpressions(BlockBuilder list, int index) {
            Expression row_ = list.append("row", this.row1_);

            Expression field = Expressions.call(hnd_,
                IgniteMethod.ROW_HANDLER_GET.method(),
                Expressions.constant(index), row_);

            return field;
        }
    }

    /** */
    private abstract class CommonFieldGetter implements InputGetter {
        /** */
        protected final Expression hnd_;

        /** */
        protected final Expression row1_;

        /** */
        protected final RelDataType rowType;

        /** */
        protected abstract Expression fillExpressions(BlockBuilder list, int index);

        /** */
        private CommonFieldGetter(Expression hnd_, Expression row_, RelDataType rowType) {
            this.hnd_ = hnd_;
            this.row1_ = row_;
            this.rowType = rowType;
        }

        /** {@inheritDoc} */
        @Override public Expression field(BlockBuilder list, int index, Type desiredType) {
            Expression fldExpression = fillExpressions(list, index);

            Type fieldType = typeFactory.getJavaClass(rowType.getFieldList().get(index).getType());

            if (desiredType == null) {
                desiredType = fieldType;
                fieldType = Object.class;
            } else if (fieldType != java.sql.Date.class
                && fieldType != java.sql.Time.class
                && fieldType != java.sql.Timestamp.class)
                fieldType = Object.class;

            return EnumUtils.convert(fldExpression, fieldType, desiredType);
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
