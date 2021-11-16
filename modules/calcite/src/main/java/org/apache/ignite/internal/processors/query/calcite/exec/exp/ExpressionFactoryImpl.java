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

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import com.github.benmanes.caffeine.cache.Caffeine;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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
import org.apache.ignite.internal.processors.query.calcite.util.Primitives;

/**
 * Implements rex expression into a function object. Uses JaninoRexCompiler under the hood. Each expression compiles
 * into a class and a wrapper over it is returned.
 */
public class ExpressionFactoryImpl<RowT> implements ExpressionFactory<RowT> {
    private static final int CACHE_SIZE = 1024;

    private static final ConcurrentMap<String, Scalar> SCALAR_CACHE = Caffeine.newBuilder()
            .maximumSize(CACHE_SIZE)
            .<String, Scalar>build()
            .asMap();

    private final IgniteTypeFactory typeFactory;

    private final SqlConformance conformance;

    private final RexBuilder rexBuilder;

    private final RelDataType emptyType;

    private final ExecutionContext<RowT> ctx;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public ExpressionFactoryImpl(ExecutionContext<RowT> ctx, IgniteTypeFactory typeFactory, SqlConformance conformance) {
        this.ctx = ctx;
        this.typeFactory = typeFactory;
        this.conformance = conformance;

        rexBuilder = new RexBuilder(this.typeFactory);
        emptyType = new RelDataTypeFactory.Builder(this.typeFactory).build();
    }

    /** {@inheritDoc} */
    @Override
    public Supplier<List<AccumulatorWrapper<RowT>>> accumulatorsFactory(
            AggregateType type,
            List<AggregateCall> calls,
            RelDataType rowType
    ) {
        if (calls.isEmpty()) {
            return null;
        }

        return new AccumulatorsFactory<>(ctx, type, calls, rowType);
    }

    /** {@inheritDoc} */
    @Override
    public Comparator<RowT> comparator(RelCollation collation) {
        if (collation == null || collation.getFieldCollations().isEmpty()) {
            return null;
        } else if (collation.getFieldCollations().size() == 1) {
            return comparator(collation.getFieldCollations().get(0));
        }

        return Commons.compoundComparator(collation.getFieldCollations()
                .stream()
                .map(this::comparator)
                .collect(Collectors.toList()));
    }

    /** {@inheritDoc} */
    @Override
    public Comparator<RowT> comparator(List<RelFieldCollation> left, List<RelFieldCollation> right) {
        if (nullOrEmpty(left) || nullOrEmpty(right) || left.size() != right.size()) {
            throw new IllegalArgumentException("Both inputs should be non-empty and have the same size: left="
                    + (left != null ? left.size() : "null") + ", right=" + (right != null ? right.size() : "null"));
        }

        List<Comparator<RowT>> comparators = new ArrayList<>();

        for (int i = 0; i < left.size(); i++) {
            comparators.add(comparator(left.get(i), right.get(i)));
        }

        return Commons.compoundComparator(comparators);
    }

    @SuppressWarnings("rawtypes")
    private Comparator<RowT> comparator(RelFieldCollation fieldCollation) {
        final int nullComparison = fieldCollation.nullDirection.nullComparison;
        final int x = fieldCollation.getFieldIndex();
        RowHandler<RowT> handler = ctx.rowHandler();

        if (fieldCollation.direction == RelFieldCollation.Direction.ASCENDING) {
            return (o1, o2) -> {
                final Comparable c1 = (Comparable) handler.get(x, o1);
                final Comparable c2 = (Comparable) handler.get(x, o2);
                return RelFieldCollation.compare(c1, c2, nullComparison);
            };
        }

        return (o1, o2) -> {
            final Comparable c1 = (Comparable) handler.get(x, o1);
            final Comparable c2 = (Comparable) handler.get(x, o2);
            return RelFieldCollation.compare(c2, c1, nullComparison);
        };
    }

    @SuppressWarnings("rawtypes")
    private Comparator<RowT> comparator(RelFieldCollation left, RelFieldCollation right) {
        final int nullComparison = left.nullDirection.nullComparison;

        if (nullComparison != right.nullDirection.nullComparison) {
            throw new IllegalArgumentException("Can't be compared: left=" + left + ", right=" + right);
        }

        final int lIdx = left.getFieldIndex();
        final int rIdx = right.getFieldIndex();
        RowHandler<RowT> handler = ctx.rowHandler();

        if (left.direction != right.direction) {
            throw new IllegalArgumentException("Can't be compared: left=" + left + ", right=" + right);
        }

        if (left.direction == RelFieldCollation.Direction.ASCENDING) {
            return (o1, o2) -> {
                final Comparable c1 = (Comparable) handler.get(lIdx, o1);
                final Comparable c2 = (Comparable) handler.get(rIdx, o2);
                return RelFieldCollation.compare(c1, c2, nullComparison);
            };
        }

        return (o1, o2) -> {
            final Comparable c1 = (Comparable) handler.get(lIdx, o1);
            final Comparable c2 = (Comparable) handler.get(rIdx, o2);
            return RelFieldCollation.compare(c2, c1, -nullComparison);
        };
    }

    /** {@inheritDoc} */
    @Override
    public Predicate<RowT> predicate(RexNode filter, RelDataType rowType) {
        return new PredicateImpl(scalar(filter, rowType));
    }

    /** {@inheritDoc} */
    @Override public BiPredicate<RowT, RowT> biPredicate(RexNode filter, RelDataType rowType) {
        return new BiPredicateImpl(biScalar(filter, rowType));
    }

    /** {@inheritDoc} */
    @Override
    public Function<RowT, RowT> project(List<RexNode> projects, RelDataType rowType) {
        return new ProjectImpl(scalar(projects, rowType), ctx.rowHandler().factory(typeFactory, RexUtil.types(projects)));
    }

    /** {@inheritDoc} */
    @Override
    public Supplier<RowT> rowSource(List<RexNode> values) {
        return new ValuesImpl(scalar(values, null), ctx.rowHandler().factory(typeFactory, RexUtil.types(values)));
    }

    /** {@inheritDoc} */
    @Override
    public <T> Supplier<T> execute(RexNode node) {
        return new ValueImpl<>(scalar(node, null), ctx.rowHandler().factory(typeFactory.getJavaClass(node.getType())));
    }

    /** {@inheritDoc} */
    @Override
    public Iterable<RowT> values(List<RexLiteral> values, RelDataType rowType) {
        RowHandler<RowT> handler = ctx.rowHandler();
        RowFactory<RowT> factory = handler.factory(typeFactory, rowType);

        int columns = rowType.getFieldCount();
        assert values.size() % columns == 0;

        List<Class<?>> types = new ArrayList<>(columns);
        for (RelDataType type : RelOptUtil.getFieldTypeList(rowType)) {
            types.add(Primitives.wrap((Class<?>) typeFactory.getJavaClass(type)));
        }

        List<RowT> rows = new ArrayList<>(values.size() / columns);
        RowT currRow = null;
        for (int i = 0; i < values.size(); i++) {
            int field = i % columns;

            if (field == 0) {
                rows.add(currRow = factory.create());
            }

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
        return scalar(List.of(node), type);
    }
    
    /**
     * Creates {@link SingleScalar}, a code-generated expressions evaluator.
     *
     * @param nodes Expressions.
     * @param type Row type.
     * @return SingleScalar.
     */
    public SingleScalar scalar(List<RexNode> nodes, RelDataType type) {
        return (SingleScalar) SCALAR_CACHE.computeIfAbsent(digest(nodes, type, false),
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
        List<RexNode> nodes = List.of(node);
        
        return (BiScalar) SCALAR_CACHE.computeIfAbsent(digest(nodes, type, true),
                k -> compile(nodes, type, true));
    }
    
    private Scalar compile(Iterable<RexNode> nodes, RelDataType type, boolean biInParams) {
        if (type == null) {
            type = emptyType;
        }

        RexProgramBuilder programBuilder = new RexProgramBuilder(type, rexBuilder);

        for (RexNode node : nodes) {
            programBuilder.addProject(node, null);
        }

        RexProgram program = programBuilder.getProgram();

        BlockBuilder builder = new BlockBuilder();

        ParameterExpression ctx =
                Expressions.parameter(ExecutionContext.class, "ctx");
    
        ParameterExpression in1 =
                Expressions.parameter(Object.class, "in1");
    
        ParameterExpression in2 =
                Expressions.parameter(Object.class, "in2");
    
        ParameterExpression out =
                Expressions.parameter(Object.class, "out");

        builder.add(
                Expressions.declare(Modifier.FINAL, DataContext.ROOT, Expressions.convert_(ctx, DataContext.class)));

        Expression hnd = builder.append("hnd",
                Expressions.call(ctx,
                        IgniteMethod.CONTEXT_ROW_HANDLER.method()));
    
        InputGetter inputGetter = biInParams ? new BiFieldGetter(hnd, in1, in2, type) :
                new FieldGetter(hnd, in1, type);

        Function1<String, InputGetter> correlates = new CorrelatesBuilder(builder, ctx, hnd).build(nodes);

        List<Expression> projects = RexToLixTranslator.translateProjects(program, typeFactory, conformance,
                builder, null, ctx, inputGetter, correlates);

        for (int i = 0; i < projects.size(); i++) {
            builder.add(
                    Expressions.statement(
                            Expressions.call(hnd,
                                    IgniteMethod.ROW_HANDLER_SET.method(),
                                    Expressions.constant(i), out, projects.get(i))));
        }

        String methodName = biInParams ? IgniteMethod.BI_SCALAR_EXECUTE.method().getName() :
                IgniteMethod.SCALAR_EXECUTE.method().getName();

        List<ParameterExpression> params = biInParams ? List.of(ctx, in1, in2, out) :
                List.of(ctx, in1, out);
    
        MethodDeclaration decl = Expressions.methodDecl(
                Modifier.PUBLIC, void.class, methodName,
                params, builder.toBlock());
    
        Class<? extends Scalar> clazz = biInParams ? BiScalar.class : SingleScalar.class;
    
        return Commons.compile(clazz, Expressions.toString(List.of(decl), "\n", false));
    }
    
    private String digest(List<RexNode> nodes, RelDataType type, boolean biParam) {
        StringBuilder b = new StringBuilder();

        b.append('[');

        for (int i = 0; i < nodes.size(); i++) {
            if (i > 0) {
                b.append(';');
            }

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

        if (type != null) {
            b.append(':').append(type.getFullTypeString());
        }

        return b.toString();
    }
    
    private abstract class AbstractScalarPredicate<T extends Scalar> {
        protected final T scalar;
    
        protected final RowT out;
    
        protected final RowHandler<RowT> hnd;

        /**
         * Constructor.
         *
         * @param scalar Scalar.
         */
        private AbstractScalarPredicate(T scalar) {
            this.scalar = scalar;
            hnd = ctx.rowHandler();
            out = hnd.factory(typeFactory, typeFactory.createJavaType(Boolean.class)).create();
        }
    }

    /**
     * Predicate implementation.
     */
    private class PredicateImpl extends AbstractScalarPredicate<SingleScalar> implements Predicate<RowT> {
        private PredicateImpl(SingleScalar scalar) {
            super(scalar);
        }

        /** {@inheritDoc} */
        @Override
        public boolean test(RowT r) {
            scalar.execute(ctx, r, out);
            
            return Boolean.TRUE == hnd.get(0, out);
        }
    }

    /**
     * Binary predicate implementation: check on two rows (used for join: left and right rows).
     */
    private class BiPredicateImpl extends AbstractScalarPredicate<BiScalar> implements BiPredicate<RowT, RowT> {
        private BiPredicateImpl(BiScalar scalar) {
            super(scalar);
        }

        /** {@inheritDoc} */
        @Override public boolean test(RowT r1, RowT r2) {
            scalar.execute(ctx, r1, r2, out);
            return Boolean.TRUE == hnd.get(0, out);
        }
    }

    private class ProjectImpl implements Function<RowT, RowT> {
        private final SingleScalar scalar;

        private final RowFactory<RowT> factory;

        /**
         * Constructor.
         *
         * @param scalar  Scalar.
         * @param factory Row factory.
         */
        private ProjectImpl(SingleScalar scalar, RowFactory<RowT> factory) {
            this.scalar = scalar;
            this.factory = factory;
        }

        /** {@inheritDoc} */
        @Override
        public RowT apply(RowT r) {
            RowT res = factory.create();
            scalar.execute(ctx, r, res);

            return res;
        }
    }

    private class ValuesImpl implements Supplier<RowT> {
        private final SingleScalar scalar;

        private final RowFactory<RowT> factory;

        /**
         * Constructor.
         */
        private ValuesImpl(SingleScalar scalar, RowFactory<RowT> factory) {
            this.scalar = scalar;
            this.factory = factory;
        }

        /** {@inheritDoc} */
        @Override
        public RowT get() {
            RowT res = factory.create();
            scalar.execute(ctx, null, res);

            return res;
        }
    }

    private class ValueImpl<T> implements Supplier<T> {
        private final SingleScalar scalar;

        private final RowFactory<RowT> factory;

        /**
         * Constructor.
         */
        private ValueImpl(SingleScalar scalar, RowFactory<RowT> factory) {
            this.scalar = scalar;
            this.factory = factory;
        }

        /** {@inheritDoc} */
        @Override
        public T get() {
            RowT res = factory.create();
            scalar.execute(ctx, null, res);

            return (T) ctx.rowHandler().get(0, res);
        }
    }
    
    private class BiFieldGetter extends CommonFieldGetter {
        private final Expression row2;
    
        private BiFieldGetter(Expression hnd, Expression row1, Expression row2, RelDataType rowType) {
            super(hnd, row1, rowType);
            this.row2 = row2;
        }
    
        /** {@inheritDoc} */
        @Override
        protected Expression fillExpressions(BlockBuilder list, int index) {
            Expression row1 = list.append("row1", this.row);
            Expression row2 = list.append("row2", this.row2);
        
            Expression field = Expressions.call(
                    IgniteMethod.ROW_HANDLER_BI_GET.method(), hnd,
                    Expressions.constant(index), row1, row2);
        
            return field;
        }
    }
    
    private class FieldGetter extends CommonFieldGetter {
        private FieldGetter(Expression hnd, Expression row, RelDataType rowType) {
            super(hnd, row, rowType);
        }
        
        /** {@inheritDoc} */
        @Override protected Expression fillExpressions(BlockBuilder list, int index) {
            Expression row = list.append("row", this.row);
            
            Expression field = Expressions.call(hnd,
                    IgniteMethod.ROW_HANDLER_GET.method(),
                    Expressions.constant(index), row);
            
            return field;
        }
    }
    
    private abstract class CommonFieldGetter implements InputGetter {
        protected final Expression hnd;
        
        protected final Expression row;
        
        protected final RelDataType rowType;
        
        private CommonFieldGetter(Expression hnd, Expression row, RelDataType rowType) {
            this.hnd = hnd;
            this.row = row;
            this.rowType = rowType;
        }
        
        protected abstract Expression fillExpressions(BlockBuilder list, int index);
        
        /** {@inheritDoc} */
        @Override
        public Expression field(BlockBuilder list, int index, Type desiredType) {
            Expression fldExpression = fillExpressions(list, index);
            
            Type fieldType = typeFactory.getJavaClass(rowType.getFieldList().get(index).getType());
            
            if (desiredType == null) {
                desiredType = fieldType;
                fieldType = Object.class;
            } else if (fieldType != java.sql.Date.class
                    && fieldType != java.sql.Time.class
                    && fieldType != java.sql.Timestamp.class) {
                fieldType = Object.class;
            }
            
            return EnumUtils.convert(fldExpression, fieldType, desiredType);
        }
    }
    
    
    private class CorrelatesBuilder extends RexShuttle {
        private final BlockBuilder builder;

        private final Expression ctx;

        private final Expression hnd;

        private Map<String, FieldGetter> correlates;

        private CorrelatesBuilder(BlockBuilder builder, Expression ctx, Expression hnd) {
            this.builder = builder;
            this.hnd = hnd;
            this.ctx = ctx;
        }

        public Function1<String, InputGetter> build(Iterable<RexNode> nodes) {
            try {
                for (RexNode node : nodes) {
                    node.accept(this);
                }

                return correlates == null ? null : correlates::get;
            } finally {
                correlates = null;
            }
        }

        /** {@inheritDoc} */
        @Override
        public RexNode visitCorrelVariable(RexCorrelVariable variable) {
            Expression corr = builder.append("corr",
                    Expressions.call(ctx, IgniteMethod.CONTEXT_GET_CORRELATED_VALUE.method(),
                            Expressions.constant(variable.id.getId())));

            if (correlates == null) {
                correlates = new HashMap<>();
            }

            correlates.put(variable.getName(), new FieldGetter(hnd, corr, variable.getType()));

            return variable;
        }
    }
}
