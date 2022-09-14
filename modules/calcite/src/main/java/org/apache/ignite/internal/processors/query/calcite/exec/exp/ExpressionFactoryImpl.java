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
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import com.google.common.collect.ImmutableList;
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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RexToLixTranslator.InputGetter;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorsFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.MultiBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.RangeBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMethod;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;

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
    private final RelDataType nullType;

    /** */
    private final ExecutionContext<Row> ctx;

    /** */
    public ExpressionFactoryImpl(
        ExecutionContext<Row> ctx,
        IgniteTypeFactory typeFactory,
        SqlConformance conformance,
        RexBuilder rexBuilder
    ) {
        this.ctx = ctx;
        this.typeFactory = typeFactory;
        this.conformance = conformance;
        this.rexBuilder = rexBuilder;

        emptyType = new RelDataTypeFactory.Builder(this.typeFactory).build();
        nullType = typeFactory.createSqlType(SqlTypeName.NULL);
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

        return new Comparator<Row>() {
            @Override public int compare(Row o1, Row o2) {
                RowHandler<Row> hnd = ctx.rowHandler();
                Object unspecifiedVal = ctx.unspecifiedValue();

                for (RelFieldCollation field : collation.getFieldCollations()) {
                    int fieldIdx = field.getFieldIndex();
                    int nullComparison = field.nullDirection.nullComparison;

                    Object c1 = hnd.get(fieldIdx, o1);
                    Object c2 = hnd.get(fieldIdx, o2);

                    // If filter for some field is unspecified, assume equality for this field and all subsequent fields.
                    if (c1 == unspecifiedVal || c2 == unspecifiedVal)
                        return 0;

                    int res = (field.direction == RelFieldCollation.Direction.ASCENDING) ?
                        ExpressionFactoryImpl.compare(c1, c2, nullComparison) :
                        ExpressionFactoryImpl.compare(c2, c1, -nullComparison);

                    if (res != 0)
                        return res;
                }

                return 0;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Comparator<Row> comparator(List<RelFieldCollation> left, List<RelFieldCollation> right) {
        if (F.isEmpty(left) || F.isEmpty(right) || left.size() != right.size())
            throw new IllegalArgumentException("Both inputs should be non-empty and have the same size: left="
                + (left != null ? left.size() : "null") + ", right=" + (right != null ? right.size() : "null"));

        // Check that collations is correct.
        for (int i = 0; i < left.size(); i++) {
            if (left.get(i).nullDirection.nullComparison != right.get(i).nullDirection.nullComparison)
                throw new IllegalArgumentException("Can't be compared: left=" + left.get(i) + ", right=" + right.get(i));

            if (left.get(i).direction != right.get(i).direction)
                throw new IllegalArgumentException("Can't be compared: left=" + left.get(i) + ", right=" + right.get(i));
        }

        return new Comparator<Row>() {
            @Override public int compare(Row o1, Row o2) {
                boolean hasNulls = false;
                RowHandler<Row> hnd = ctx.rowHandler();

                for (int i = 0; i < left.size(); i++) {
                    RelFieldCollation leftField = left.get(i);
                    RelFieldCollation rightField = right.get(i);

                    int lIdx = leftField.getFieldIndex();
                    int rIdx = rightField.getFieldIndex();

                    Object c1 = hnd.get(lIdx, o1);
                    Object c2 = hnd.get(rIdx, o2);

                    if (c1 == null && c2 == null) {
                        hasNulls = true;
                        continue;
                    }

                    int nullComparison = leftField.nullDirection.nullComparison;

                    int res = leftField.direction == RelFieldCollation.Direction.ASCENDING ?
                        ExpressionFactoryImpl.compare(c1, c2, nullComparison) :
                        ExpressionFactoryImpl.compare(c2, c1, -nullComparison);

                    if (res != 0)
                        return res;
                }

                // If compared rows contain NULLs, they shouldn't be treated as equals, since NULL <> NULL in SQL.
                return hasNulls ? 1 : 0;
            }
        };
    }

    /** */
    @SuppressWarnings("rawtypes")
    private static int compare(Object o1, Object o2, int nullComparison) {
        final Comparable c1 = (Comparable)o1;
        final Comparable c2 = (Comparable)o2;
        return RelFieldCollation.compare(c1, c2, nullComparison);
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
        return new ValuesImpl(scalar(values, null), ctx.rowHandler().factory(typeFactory,
            Commons.transform(values, v -> v != null ? v.getType() : nullType)));
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

    /** {@inheritDoc} */
    @Override public Iterable<BoundsValues<Row>> boundsValues(
        List<SearchBounds> searchBounds,
        RelCollation collation,
        RelDataType rowType
    ) {
        RowFactory<Row> rowFactory = ctx.rowHandler().factory(typeFactory, rowType);

        List<BoundsValues<Row>> boundsValues = new ArrayList<>();

        expandBounds(
            boundsValues,
            searchBounds,
            rowType,
            rowFactory,
            collation.getKeys(),
            0,
            Arrays.asList(new RexNode[searchBounds.size()]),
            Arrays.asList(new RexNode[searchBounds.size()]),
            true,
            true
        );

        Comparator<Row> comparator = comparator(collation);

        return new Iterable<BoundsValues<Row>>() {
            @NotNull @Override public Iterator<BoundsValues<Row>> iterator() {
                boundsValues.forEach(b -> ((BoundsValuesImpl)b).clearCache());

                if (boundsValues.size() == 1)
                    return boundsValues.iterator();

                // Sort bound values using collation comparator to produce sorted output. There should be no ranges
                // intersection between bounds.
                boundsValues.sort((o1, o2) -> comparator.compare(o1.lower(), o2.lower()));

                return boundsValues.iterator();
            }
        };
    }

    /**
     * Expand search bounds to list of tuples (lower row/upper row).
     *
     * @param boundsValues Bounds values.
     * @param searchBounds Search bounds.
     * @param rowType Row type.
     * @param rowFactory Row factory.
     * @param collationKeys Collation keys.
     * @param collationKeyIdx Current collation key index (field to process).
     * @param curLower Current lower row.
     * @param curUpper Current upper row.
     * @param lowerInclude Include current lower row.
     * @param upperInclude Include current upper row.
     */
    private void expandBounds(
        List<BoundsValues<Row>> boundsValues,
        List<SearchBounds> searchBounds,
        RelDataType rowType,
        RowFactory<Row> rowFactory,
        List<Integer> collationKeys,
        int collationKeyIdx,
        List<RexNode> curLower,
        List<RexNode> curUpper,
        boolean lowerInclude,
        boolean upperInclude
    ) {
        if ((collationKeyIdx >= collationKeys.size()) || (!lowerInclude && !upperInclude) ||
            searchBounds.get(collationKeys.get(collationKeyIdx)) == null) {
            boundsValues.add(new BoundsValuesImpl(
                scalar(curLower, rowType),
                scalar(curUpper, rowType),
                lowerInclude,
                upperInclude,
                rowFactory
            ));

            return;
        }

        int fieldIdx = collationKeys.get(collationKeyIdx);
        SearchBounds fieldBounds = searchBounds.get(fieldIdx);

        Collection<SearchBounds> fieldMultiBounds = fieldBounds instanceof MultiBounds ?
            ((MultiBounds)fieldBounds).bounds() : Collections.singleton(fieldBounds);

        for (SearchBounds fieldSingleBounds : fieldMultiBounds) {
            RexNode fieldLowerBound;
            RexNode fieldUpperBound;
            boolean fieldLowerInclude;
            boolean fieldUpperInclude;

            if (fieldSingleBounds instanceof ExactBounds) {
                fieldLowerBound = fieldUpperBound = ((ExactBounds)fieldSingleBounds).bound();
                fieldLowerInclude = fieldUpperInclude = true;
            }
            else if (fieldSingleBounds instanceof RangeBounds) {
                fieldLowerBound = ((RangeBounds)fieldSingleBounds).lowerBound();
                fieldUpperBound = ((RangeBounds)fieldSingleBounds).upperBound();
                fieldLowerInclude = ((RangeBounds)fieldSingleBounds).lowerInclude();
                fieldUpperInclude = ((RangeBounds)fieldSingleBounds).upperInclude();
            }
            else
                throw new IllegalStateException("Unexpected bounds: " + fieldSingleBounds);

            if (lowerInclude)
                curLower.set(fieldIdx, fieldLowerBound);

            if (upperInclude)
                curUpper.set(fieldIdx, fieldUpperBound);

            expandBounds(
                boundsValues,
                searchBounds,
                rowType,
                rowFactory,
                collationKeys,
                collationKeyIdx + 1,
                curLower,
                curUpper,
                lowerInclude && fieldLowerInclude,
                upperInclude && fieldUpperInclude
            );
        }

        curLower.set(fieldIdx, null);
        curLower.set(fieldIdx, null);
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
     * @param nodes Expressions. {@code Null} expressions will be evaluated to {@link ExecutionContext#unspecifiedValue()}.
     * @param type Row type.
     * @return SingleScalar.
     */
    private SingleScalar scalar(List<RexNode> nodes, RelDataType type) {
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
    private BiScalar biScalar(RexNode node, RelDataType type) {
        ImmutableList<RexNode> nodes = ImmutableList.of(node);

        return (BiScalar)SCALAR_CACHE.computeIfAbsent(digest(nodes, type, true),
            k -> compile(nodes, type, true));
    }

    /** */
    private Scalar compile(List<RexNode> nodes, RelDataType type, boolean biInParams) {
        if (type == null)
            type = emptyType;

        RexProgramBuilder programBuilder = new RexProgramBuilder(type, rexBuilder);

        BitSet unspecifiedValues = new BitSet(nodes.size());

        for (int i = 0; i < nodes.size(); i++) {
            RexNode node = nodes.get(i);

            if (node != null)
                programBuilder.addProject(node, null);
            else {
                unspecifiedValues.set(i);

                programBuilder.addProject(rexBuilder.makeNullLiteral(type == emptyType ?
                    nullType : type.getFieldList().get(i).getType()), null);
            }
        }

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

        assert nodes.size() == projects.size();

        for (int i = 0; i < projects.size(); i++) {
            Expression val = unspecifiedValues.get(i) ? Expressions.call(ctx_,
                IgniteMethod.CONTEXT_UNSPECIFIED_VALUE.method()) : projects.get(i);

            builder.add(
                Expressions.statement(
                    Expressions.call(hnd_,
                        IgniteMethod.ROW_HANDLER_SET.method(),
                        Expressions.constant(i), out_, val)));
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

            RexNode node = nodes.get(i);

            b.append(node);

            if (node == null)
                continue;

            b.append(':');
            b.append(node.getType().getFullTypeString());

            new RexShuttle() {
                @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
                    b.append(", fldIdx=").append(fieldAccess.getField().getIndex());

                    return super.visitFieldAccess(fieldAccess);
                }
            }.apply(node);
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
    private class BoundsValuesImpl implements BoundsValues<Row> {
        /** */
        private final SingleScalar lowerBound;

        /** */
        private final SingleScalar upperBound;

        /** */
        private final boolean lowerInclude;

        /** */
        private final boolean upperInclude;

        /** */
        private Row lowerRow;

        /** */
        private Row upperRow;

        /** */
        private final RowFactory<Row> factory;

        /** */
        private BoundsValuesImpl(
            SingleScalar lowerBound,
            SingleScalar upperBound,
            boolean lowerInclude,
            boolean upperInclude,
            RowFactory<Row> factory
        ) {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.lowerInclude = lowerInclude;
            this.upperInclude = upperInclude;
            this.factory = factory;
        }

        /** {@inheritDoc} */
        @Override public Row lower() {
            return lowerRow != null ? lowerRow : (lowerRow = getRow(lowerBound));
        }

        /** {@inheritDoc} */
        @Override public Row upper() {
            return upperRow != null ? upperRow : (upperRow = getRow(upperBound));
        }

        /** {@inheritDoc} */
        @Override public boolean lowerInclude() {
            return lowerInclude;
        }

        /** {@inheritDoc} */
        @Override public boolean upperInclude() {
            return upperInclude;
        }

        /** Compute row. */
        private Row getRow(SingleScalar scalar) {
            Row res = factory.create();
            scalar.execute(ctx, null, res);

            return res;
        }

        /** Clear cached rows. */
        public void clearCache() {
            lowerRow = upperRow = null;
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
            }
            else if (fieldType != java.sql.Date.class
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
                for (RexNode node : nodes) {
                    if (node != null)
                        node.accept(this);
                }

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
