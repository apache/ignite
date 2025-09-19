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
import java.util.stream.Collectors;
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
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RexToLixTranslator.InputGetter;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AccumulatorsFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.window.WindowPartition;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.window.WindowPartitionFactory;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.MultiBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.RangeBounds;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
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

    /** Placeholder for lowest possible value in range bound. This placeholder is only reqired to compare ranges. */
    private static final Object LOWEST_VALUE = new Object();

    /** Placeholder for highest possible value in range bound. This placeholder is only reqired to compare ranges. */
    private static final Object HIGHEST_VALUE = new Object();

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
    private final RelDataType booleanType;

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
        booleanType = typeFactory.createJavaType(Boolean.class);
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
    @Override public Supplier<WindowPartition<Row>> windowPartitionFactory(
        Window.Group group,
        List<AggregateCall> calls,
        RelDataType rowType,
        boolean streaming
    ) {
        assert !calls.isEmpty() : "Window aggregate calls should not be empty";

        return new WindowPartitionFactory<>(ctx, group, calls, rowType, streaming);
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

                    if (c1 == LOWEST_VALUE || c2 == LOWEST_VALUE) {
                        if (c1 != LOWEST_VALUE)
                            return 1;
                        else if (c2 != LOWEST_VALUE)
                            return -1;
                        else
                            return 0;
                    }

                    if (c1 == HIGHEST_VALUE || c2 == HIGHEST_VALUE) {
                        if (c1 != HIGHEST_VALUE)
                            return -1;
                        else if (c2 != HIGHEST_VALUE)
                            return 1;
                        else
                            return 0;
                    }

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
    @Override public Comparator<Row> comparator(List<RelFieldCollation> left, List<RelFieldCollation> right, boolean nullsEqual) {
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
                // Except cases with IS DISTINCT / IS NOT DISTINCT.
                return hasNulls && !nullsEqual ? 1 : 0;
            }
        };
    }

    /** */
    @SuppressWarnings("rawtypes")
    private static int compare(Object o1, Object o2, int nullComparison) {
        if (BinaryUtils.isBinaryObjectImpl(o1))
            return compareBinary(o1, o2, nullComparison);

        final Comparable c1 = (Comparable)o1;
        final Comparable c2 = (Comparable)o2;
        return RelFieldCollation.compare(c1, c2, nullComparison);
    }

    /** */
    private static int compareBinary(Object o1, Object o2, int nullComparison) {
        if (o1 == o2)
            return 0;
        else if (o1 == null)
            return nullComparison;
        else if (o2 == null)
            return -nullComparison;

        return BinaryUtils.compareForDml(o1, o2);
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
        RowHandler<Row> hnd = ctx.rowHandler();
        RowFactory<Row> factory = hnd.factory(typeFactory, rowType);

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

            hnd.set(field, currRow, literal.getValueAs(types.get(field)));
        }

        return rows;
    }

    /** {@inheritDoc} */
    @Override public RangeIterable<Row> ranges(
        List<SearchBounds> searchBounds,
        RelCollation collation,
        RelDataType rowType
    ) {
        RowFactory<Row> rowFactory = ctx.rowHandler().factory(typeFactory, rowType);

        List<RangeConditionImpl> ranges = new ArrayList<>();

        Comparator<Row> rowComparator = comparator(collation);

        expandBounds(
            ranges,
            searchBounds,
            rowType,
            rowFactory,
            collation.getKeys(),
            0,
            rowComparator,
            Arrays.asList(new RexNode[searchBounds.size()]),
            Arrays.asList(new RexNode[searchBounds.size()]),
            true,
            true
        );

        return new RangeIterableImpl(ranges);
    }

    /**
     * Expand column-oriented {@link SearchBounds} to a row-oriented list of ranges ({@link RangeCondition}).
     *
     * @param ranges List of ranges.
     * @param searchBounds Search bounds.
     * @param rowType Row type.
     * @param rowFactory Row factory.
     * @param collationKeys Collation keys.
     * @param collationKeyIdx Current collation key index (field to process).
     * @param rowComparator Row comparator.
     * @param curLower Current lower row.
     * @param curUpper Current upper row.
     * @param lowerInclude Include current lower row.
     * @param upperInclude Include current upper row.
     */
    private void expandBounds(
        List<RangeConditionImpl> ranges,
        List<SearchBounds> searchBounds,
        RelDataType rowType,
        RowFactory<Row> rowFactory,
        List<Integer> collationKeys,
        int collationKeyIdx,
        Comparator<Row> rowComparator,
        List<RexNode> curLower,
        List<RexNode> curUpper,
        boolean lowerInclude,
        boolean upperInclude
    ) {
        if ((collationKeyIdx >= collationKeys.size()) || (!lowerInclude && !upperInclude) ||
            searchBounds.get(collationKeys.get(collationKeyIdx)) == null) {
            ranges.add(new RangeConditionImpl(
                scalar(curLower, rowType),
                scalar(curUpper, rowType),
                lowerInclude,
                upperInclude,
                rowComparator,
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
                RangeBounds fieldRangeBounds = (RangeBounds)fieldSingleBounds;
                fieldLowerBound = fieldRangeBounds.lowerBound();
                fieldUpperBound = fieldRangeBounds.upperBound();
                fieldLowerInclude = fieldRangeBounds.lowerInclude();
                fieldUpperInclude = fieldRangeBounds.upperInclude();
            }
            else
                throw new IllegalStateException("Unexpected bounds: " + fieldSingleBounds);

            if (lowerInclude)
                curLower.set(fieldIdx, fieldLowerBound);

            if (upperInclude)
                curUpper.set(fieldIdx, fieldUpperBound);

            expandBounds(
                ranges,
                searchBounds,
                rowType,
                rowFactory,
                collationKeys,
                collationKeyIdx + 1,
                rowComparator,
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

        String code = Expressions.toString(F.asList(decl), "\n", false);

        return Commons.compile(clazz, code);
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
                    b.append(", fldType=").append(fieldAccess.getField().getType().getFullTypeString());

                    return super.visitFieldAccess(fieldAccess);
                }

                @Override public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
                    b.append(", paramType=").append(dynamicParam.getType().getFullTypeString());

                    return super.visitDynamicParam(dynamicParam);
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
            out = hnd.factory(typeFactory, booleanType).create();
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
    private class RangeConditionImpl implements RangeCondition<Row>, Comparable<RangeConditionImpl> {
        /** */
        private final SingleScalar lowerScalar;

        /** */
        private final SingleScalar upperScalar;

        /** */
        private final boolean lowerInclude;

        /** */
        private final boolean upperInclude;

        /** Lower row, to return to consumer (can contain ctx.unspecifiedValue()). */
        private Row lowerRow;

        /** Upper row, to return to consumer (can contain ctx.unspecifiedValue()). */
        private Row upperRow;

        /** Lower row, for comparison (can contain LOWEST_VALUE, HIGHEST_VALUE). */
        private Row lowerBound;

        /** Upper row, for comparison (can contain LOWEST_VALUE, HIGHEST_VALUE). */
        private Row upperBound;

        /** Cached skip range flag. */
        private Boolean skip;

        /** */
        private final Comparator<Row> rowComparator;

        /** */
        private final RowFactory<Row> factory;

        /** */
        private RangeConditionImpl(
            SingleScalar lowerScalar,
            SingleScalar upperScalar,
            boolean lowerInclude,
            boolean upperInclude,
            Comparator<Row> rowComparator,
            RowFactory<Row> factory
        ) {
            this.lowerScalar = lowerScalar;
            this.upperScalar = upperScalar;
            this.lowerInclude = lowerInclude;
            this.upperInclude = upperInclude;
            this.rowComparator = rowComparator;
            this.factory = factory;
        }

        /** {@inheritDoc} */
        @Override public Row lower() {
            return lowerRow != null ? lowerRow : (lowerRow = getRow(lowerScalar));
        }

        /** {@inheritDoc} */
        @Override public Row upper() {
            return upperRow != null ? upperRow : (upperRow = getRow(upperScalar));
        }

        /** {@inheritDoc} */
        @Override public boolean lowerInclude() {
            return lowerInclude;
        }

        /** {@inheritDoc} */
        @Override public boolean upperInclude() {
            return upperInclude;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(RangeConditionImpl o) {
            calcBounds();
            o.calcBounds();

            int res = rowComparator.compare(lowerBound, o.lowerBound);

            if (res == 0)
                return rowComparator.compare(upperBound, o.upperBound);
            else
                return res;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return o instanceof ExpressionFactoryImpl.RangeConditionImpl && compareTo((RangeConditionImpl)o) == 0;
        }

        /** */
        private void calcBounds() {
            if (lowerBound == null)
                lowerBound = calcBound(lower(), lowerInclude ? LOWEST_VALUE : HIGHEST_VALUE);

            if (upperBound == null)
                upperBound = calcBound(upper(), upperInclude ? HIGHEST_VALUE : LOWEST_VALUE);
        }

        /** */
        private Row calcBound(Row row, Object unspecifiedValReplacer) {
            RowHandler<Row> hnd = ctx.rowHandler();

            Object[] vals = new Object[hnd.columnCount(row)];

            for (int i = 0; i < vals.length; i++) {
                vals[i] = hnd.get(i, row);

                if (vals[i] == ctx.unspecifiedValue())
                    vals[i] = unspecifiedValReplacer;
            }

            return factory.create(vals);
        }

        /** Compute row. */
        private Row getRow(SingleScalar scalar) {
            Row res = factory.create();
            scalar.execute(ctx, null, res);

            RowHandler<Row> hnd = ctx.rowHandler();

            // Check bound for NULL values. If bound contains NULLs, the whole range should be skipped.
            // There is special placeholder for searchable NULLs, make this replacement here too.
            for (int i = 0; i < hnd.columnCount(res); i++) {
                Object fldVal = hnd.get(i, res);

                if (fldVal == null)
                    skip = Boolean.TRUE;

                if (fldVal == ctx.nullBound())
                    hnd.set(i, res, null);
            }

            return res;
        }

        /** Clear cached rows. */
        public void clearCache() {
            lowerRow = upperRow = null;
            lowerBound = upperBound = null;
            skip = null;
        }

        /** Skip this range. */
        public boolean skip() {
            if (skip == null) {
                // Precalculate skip flag.
                lower();
                upper();

                if (skip == null)
                    skip = Boolean.FALSE;
            }

            return skip;
        }

        /** Check if bounds is valid. */
        public boolean valid() {
            calcBounds();

            return rowComparator.compare(lowerBound, upperBound) <= 0;
        }

        /** Range intersects another range. */
        public boolean intersects(RangeConditionImpl o) {
            calcBounds();
            o.calcBounds();

            return rowComparator.compare(lowerBound, o.upperBound) <= 0
                && rowComparator.compare(o.lowerBound, upperBound) <= 0;
        }

        /** Merge two intersected ranges. */
        public RangeConditionImpl merge(RangeConditionImpl o) {
            calcBounds();
            o.calcBounds();

            SingleScalar newLowerScalar;
            Row newLowerRow;
            Row newLowerBound;
            boolean newLowerInclude;

            if (rowComparator.compare(lowerBound, o.lowerBound) <= 0) {
                newLowerScalar = lowerScalar;
                newLowerRow = lowerRow;
                newLowerBound = lowerBound;
                newLowerInclude = lowerInclude;
            }
            else {
                newLowerScalar = o.lowerScalar;
                newLowerRow = o.lowerRow;
                newLowerBound = o.lowerBound;
                newLowerInclude = o.lowerInclude;
            }

            SingleScalar newUpperScalar;
            Row newUpperRow;
            Row newUpperBound;
            boolean newUpperInclude;

            if (rowComparator.compare(upperBound, o.upperBound) >= 0) {
                newUpperScalar = upperScalar;
                newUpperRow = upperRow;
                newUpperBound = upperBound;
                newUpperInclude = upperInclude;
            }
            else {
                newUpperScalar = o.upperScalar;
                newUpperRow = o.upperRow;
                newUpperBound = o.upperBound;
                newUpperInclude = o.upperInclude;
            }

            RangeConditionImpl newRangeCondition = new RangeConditionImpl(newLowerScalar, newUpperScalar,
                newLowerInclude, newUpperInclude, rowComparator, factory);

            newRangeCondition.lowerRow = newLowerRow;
            newRangeCondition.upperRow = newUpperRow;
            newRangeCondition.lowerBound = newLowerBound;
            newRangeCondition.upperBound = newUpperBound;
            newRangeCondition.skip = Boolean.FALSE;

            return newRangeCondition;
        }
    }

    /** */
    private class RangeIterableImpl implements RangeIterable<Row> {
        /** */
        private List<RangeConditionImpl> ranges;

        /** */
        private boolean sorted;

        /** */
        public RangeIterableImpl(List<RangeConditionImpl> ranges) {
            this.ranges = ranges;
        }

        /** {@inheritDoc} */
        @Override public boolean multiBounds() {
            return ranges.size() > 1;
        }

        /** {@inheritDoc} */
        @Override public Iterator<RangeCondition<Row>> iterator() {
            ranges.forEach(RangeConditionImpl::clearCache);

            if (ranges.size() == 1) {
                if (ranges.get(0).skip())
                    return Collections.emptyIterator();
                else
                    return (Iterator)ranges.iterator();
            }

            // Sort ranges and remove intersections using collation comparator to produce sorted output.
            // Do not sort again if ranges already were sorted before, different values of correlated variables
            // should not affect ordering.
            if (!sorted) {
                ranges = ranges.stream().filter(r -> !r.skip()).sorted(RangeConditionImpl::compareTo)
                    .collect(Collectors.toList());

                List<RangeConditionImpl> ranges0 = new ArrayList<>(ranges.size());

                RangeConditionImpl prevRange = null;

                for (RangeConditionImpl range : ranges) {
                    if (!range.valid())
                        continue;

                    if (prevRange != null) {
                        if (prevRange.intersects(range))
                            range = prevRange.merge(range);
                        else
                            ranges0.add(prevRange);
                    }

                    prevRange = range;
                }

                if (prevRange != null)
                    ranges0.add(prevRange);

                ranges = ranges0;
                sorted = true;
            }

            return F.iterator(ranges.iterator(), r -> r, true, r -> !r.skip());
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
