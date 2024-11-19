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

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.UuidType;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.VARBINARY;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

/**
 *
 */
public class Accumulators {
    /** */
    public static <Row> Supplier<Accumulator<Row>> accumulatorFactory(AggregateCall call, ExecutionContext<Row> ctx) {
        Supplier<Accumulator<Row>> supplier = accumulatorFunctionFactory(call, ctx);

        if (call.isDistinct())
            return () -> new DistinctAccumulator<>(call, ctx.rowHandler(), supplier);

        return supplier;
    }

    /** */
    private static <Row> Supplier<Accumulator<Row>> accumulatorFunctionFactory(
        AggregateCall call,
        ExecutionContext<Row> ctx
    ) {
        RowHandler<Row> hnd = ctx.rowHandler();

        switch (call.getAggregation().getName()) {
            case "COUNT":
                return () -> new LongCount<>(call, hnd);
            case "AVG":
                return avgFactory(call, hnd);
            case "SUM":
                return sumFactory(call, hnd);
            case "$SUM0":
                return sumEmptyIsZeroFactory(call, hnd);
            case "MIN":
            case "EVERY":
                return minFactory(call, hnd);
            case "MAX":
            case "SOME":
                return maxFactory(call, hnd);
            case "SINGLE_VALUE":
                return () -> new SingleVal<>(call, hnd);
            case "LITERAL_AGG":
                return () -> new LiteralVal<>(call, hnd);
            case "ANY_VALUE":
                return () -> new AnyVal<>(call, hnd);
            case "LISTAGG":
            case "ARRAY_AGG":
            case "ARRAY_CONCAT_AGG":
                return listAggregateSupplier(call, ctx);
            case "BIT_AND":
            case "BIT_OR":
            case "BIT_XOR":
                return bitWiseAcc(call, hnd);
            default:
                throw new AssertionError(call.getAggregation().getName());
        }
    }

    /** */
    private static <Row> Supplier<Accumulator<Row>> listAggregateSupplier(
        AggregateCall call,
        ExecutionContext<Row> ctx
    ) {
        RowHandler<Row> hnd = ctx.rowHandler();

        Supplier<Accumulator<Row>> accSup;
        String aggName = call.getAggregation().getName();
        if ("LISTAGG".equals(aggName))
            accSup = () -> new ListAggAccumulator<>(call, hnd);
        else if ("ARRAY_CONCAT_AGG".equals(aggName))
            accSup = () -> new ArrayConcatAggregateAccumulator<>(call, hnd);
        else if ("ARRAY_AGG".equals(aggName))
            accSup = () -> new ArrayAggregateAccumulator<>(call, hnd);
        else
            throw new AssertionError(call.getAggregation().getName());

        if (call.getCollation() != null && !call.getCollation().getFieldCollations().isEmpty()) {
            Comparator<Row> cmp = ctx.expressionFactory().comparator(call.getCollation());

            return () -> new SortingAccumulator<>(accSup, cmp);
        }

        return accSup;
    }

    /** */
    private static <Row> Supplier<Accumulator<Row>> avgFactory(AggregateCall call, RowHandler<Row> hnd) {
        switch (call.type.getSqlTypeName()) {
            case ANY:
                throw new UnsupportedOperationException("AVG() is not supported for type '" + call.type + "'.");
            case BIGINT:
            case DECIMAL:
                return () -> new DecimalAvg<>(call, hnd);
            case DOUBLE:
            case REAL:
            case FLOAT:
            case INTEGER:
            default:
                return () -> new DoubleAvg<>(call, hnd);
        }
    }

    /** */
    private static <Row> Supplier<Accumulator<Row>> bitWiseAcc(AggregateCall call, RowHandler<Row> hnd) {
        switch (call.type.getSqlTypeName()) {
            case BIGINT:
            case INTEGER:
            case SMALLINT:
            case TINYINT:
                return () -> new BitWiseAcc<>(call, hnd);
            default:
                throw new UnsupportedOperationException(call.getName() + " is not supported for type '" + call.type + "'.");
        }
    }

    /** */
    private static <Row> Supplier<Accumulator<Row>> sumFactory(AggregateCall call, RowHandler<Row> hnd) {
        switch (call.type.getSqlTypeName()) {
            case ANY:
                throw new UnsupportedOperationException("SUM() is not supported for type '" + call.type + "'.");

            case BIGINT:
            case DECIMAL:
                return () -> new Sum<>(call, new DecimalSumEmptyIsZero<>(call, hnd), hnd);

            case DOUBLE:
            case REAL:
            case FLOAT:
                return () -> new Sum<>(call, new DoubleSumEmptyIsZero<>(call, hnd), hnd);

            case TINYINT:
            case SMALLINT:
            case INTEGER:
            default:
                return () -> new Sum<>(call, new LongSumEmptyIsZero<>(call, hnd), hnd);
        }
    }

    /** */
    private static <Row> Supplier<Accumulator<Row>> sumEmptyIsZeroFactory(AggregateCall call, RowHandler<Row> hnd) {
        switch (call.type.getSqlTypeName()) {
            case ANY:
                throw new UnsupportedOperationException("SUM() is not supported for type '" + call.type + "'.");

            case BIGINT:
            case DECIMAL:
                return () -> new DecimalSumEmptyIsZero<>(call, hnd);

            case DOUBLE:
            case REAL:
            case FLOAT:
                return () -> new DoubleSumEmptyIsZero<>(call, hnd);

            case TINYINT:
            case SMALLINT:
            case INTEGER:
            default:
                return () -> new LongSumEmptyIsZero<>(call, hnd);
        }
    }

    /** */
    private static <Row> Supplier<Accumulator<Row>> minFactory(AggregateCall call, RowHandler<Row> hnd) {
        switch (call.type.getSqlTypeName()) {
            case DOUBLE:
            case REAL:
            case FLOAT:
                return () -> new DoubleMinMax<>(call, hnd, true);
            case DECIMAL:
                return () -> new DecimalMinMax<>(call, hnd, true);
            case INTEGER:
                return () -> new IntMinMax<>(call, hnd, true);
            case CHAR:
            case VARCHAR:
                return () -> new VarCharMinMax<>(call, hnd, true);
            case BINARY:
            case VARBINARY:
                return () -> new ComparableMinMax<Row, ByteString>(call, hnd, true,
                    tf -> tf.createTypeWithNullability(tf.createSqlType(VARBINARY), true));
            case ANY:
                if (call.type instanceof UuidType) {
                    return () -> new ComparableMinMax<Row, UUID>(call, hnd, true,
                        tf -> tf.createTypeWithNullability(tf.createCustomType(UUID.class), true));
                }
                throw new UnsupportedOperationException("MIN() is not supported for type '" + call.type + "'.");
            case BIGINT:
            default:
                return () -> new LongMinMax<>(call, hnd, true);
        }
    }

    /** */
    private static <Row> Supplier<Accumulator<Row>> maxFactory(AggregateCall call, RowHandler<Row> hnd) {
        switch (call.type.getSqlTypeName()) {
            case DOUBLE:
            case REAL:
            case FLOAT:
                return () -> new DoubleMinMax<>(call, hnd, false);
            case DECIMAL:
                return () -> new DecimalMinMax<>(call, hnd, false);
            case INTEGER:
                return () -> new IntMinMax<>(call, hnd, false);
            case CHAR:
            case VARCHAR:
                return () -> new VarCharMinMax<>(call, hnd, false);
            case BINARY:
            case VARBINARY:
                return () -> new ComparableMinMax<Row, ByteString>(call, hnd, false,
                    tf -> tf.createTypeWithNullability(tf.createSqlType(VARBINARY), true));
            case ANY:
                if (call.type instanceof UuidType) {
                    return () -> new ComparableMinMax<Row, UUID>(call, hnd, false,
                        tf -> tf.createTypeWithNullability(tf.createCustomType(UUID.class), true));
                }
                throw new UnsupportedOperationException("MAX() is not supported for type '" + call.type + "'.");
            case BIGINT:
            default:
                return () -> new LongMinMax<>(call, hnd, false);
        }
    }

    /** */
    private abstract static class AbstractAccumulator<Row> implements Accumulator<Row> {
        /** */
        private final RowHandler<Row> hnd;

        /** */
        private final transient AggregateCall aggCall;

        /** */
        AbstractAccumulator(AggregateCall aggCall, RowHandler<Row> hnd) {
            this.aggCall = aggCall;
            this.hnd = hnd;
        }

        /** */
        <T> T get(int idx, Row row) {
            assert idx < arguments().size() : "idx=" + idx + "; arguments=" + arguments();

            return (T)hnd.get(arguments().get(idx), row);
        }

        /** */
        protected AggregateCall aggregateCall() {
            return aggCall;
        }

        /** */
        protected List<Integer> arguments() {
            return aggCall.getArgList();
        }

        /** */
        int columnCount(Row row) {
            return hnd.columnCount(row);
        }
    }

    /** */
    private static class SingleVal<Row> extends AnyVal<Row> {
        /** */
        private boolean touched;

        /** */
        SingleVal(AggregateCall aggCall, RowHandler<Row> hnd) {
            super(aggCall, hnd);
        }

        /** {@inheritDoc}  */
        @Override public void add(Row row) {
            if (touched)
                throw new IllegalArgumentException("Subquery returned more than 1 value.");

            touched = true;

            super.add(row);
        }

        /** {@inheritDoc}  */
        @Override public void apply(Accumulator<Row> other) {
            if (((SingleVal<Row>)other).touched) {
                if (touched)
                    throw new IllegalArgumentException("Subquery returned more than 1 value.");
                else
                    touched = true;
            }

            super.apply(other);
        }
    }

    /**
     * LITERAL_AGG accumulator, returns predefined literal value.
     * Calcite`s implementation RexImpTable#LiteralAggImplementor.
     */
    private static class LiteralVal<Row> extends AbstractAccumulator<Row> {
        /** */
        public LiteralVal(AggregateCall aggCall, RowHandler<Row> hnd) {
            super(aggCall, hnd);
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            assert !F.isEmpty(aggregateCall().rexList) : "aggregateCall().rexList is empty for LITERAL_AGG";

            RexNode rexNode = aggregateCall().rexList.get(0);

            assert rexNode instanceof RexLiteral;

            return ((RexLiteral)rexNode).getValue();
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return aggregateCall().getType();
        }
    }

    /** */
    private static class AnyVal<Row> extends AbstractAccumulator<Row> {
        /** */
        private Object holder;

        /** */
        AnyVal(AggregateCall aggCall, RowHandler<Row> hnd) {
            super(aggCall, hnd);
        }

        /** {@inheritDoc}  */
        @Override public void add(Row row) {
            if (holder == null)
                holder = get(0, row);
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            if (holder == null)
                holder = ((AnyVal<Row>)other).holder;
        }

        /** {@inheritDoc}  */
        @Override public Object end() {
            return holder;
        }

        /** {@inheritDoc}  */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), true));
        }

        /** {@inheritDoc}  */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createSqlType(ANY);
        }
    }

    /** */
    private static class BitWiseAcc<Row> extends AbstractAccumulator<Row> {
        /** */
        private long res;

        /** */
        private boolean updated;

        /** */
        private final SqlKind kind;

        /** */
        private BitWiseAcc(AggregateCall aggCall, RowHandler<Row> hnd) {
            super(aggCall, hnd);

            kind = aggCall.getAggregation().kind;

            assert kind == SqlKind.BIT_AND || kind == SqlKind.BIT_OR || kind == SqlKind.BIT_XOR;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            Number in = get(0, row);

            if (in == null)
                return;

            apply(in.longValue());
        }

        /** */
        private void apply(long val) {
            if (updated) {
                switch (kind) {
                    case BIT_AND:
                        res &= val;
                        break;
                    case BIT_OR:
                        res |= val;
                        break;
                    case BIT_XOR:
                        res ^= val;
                        break;
                }
            }
            else {
                res = val;

                updated = true;
            }
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            BitWiseAcc<Row> other0 = (BitWiseAcc<Row>)other;

            if (other0.updated)
                apply(other0.res);
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return updated ? res : null;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeFactory.createTypeWithNullability(typeFactory.createSqlType(BIGINT), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(BIGINT), true);
        }
    }

    /** */
    private static class DecimalAvg<Row> extends AbstractAccumulator<Row> {
        /** */
        private BigDecimal sum = BigDecimal.ZERO;

        /** */
        private BigDecimal cnt = BigDecimal.ZERO;

        /** */
        DecimalAvg(AggregateCall aggCall, RowHandler<Row> hnd) {
            super(aggCall, hnd);
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            BigDecimal in = get(0, row);

            if (in == null)
                return;

            sum = sum.add(in);
            cnt = cnt.add(BigDecimal.ONE);
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            DecimalAvg<Row> other0 = (DecimalAvg<Row>)other;

            sum = sum.add(other0.sum);
            cnt = cnt.add(other0.cnt);
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return cnt.compareTo(BigDecimal.ZERO) == 0 ? null : sum.divide(cnt, MathContext.DECIMAL64);
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeFactory.createTypeWithNullability(typeFactory.createSqlType(DECIMAL), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(DECIMAL), true);
        }
    }

    /** */
    private static class DoubleAvg<Row> extends AbstractAccumulator<Row> {
        /** */
        private double sum;

        /** */
        private long cnt;

        /** */
        DoubleAvg(AggregateCall aggCall, RowHandler<Row> hnd) {
            super(aggCall, hnd);
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            Double in = get(0, row);

            if (in == null)
                return;

            sum += in;
            cnt++;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            DoubleAvg<Row> other0 = (DoubleAvg<Row>)other;

            sum += other0.sum;
            cnt += other0.cnt;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return cnt > 0 ? sum / cnt : null;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeFactory.createTypeWithNullability(typeFactory.createSqlType(DOUBLE), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(DOUBLE), true);
        }
    }

    /** */
    private static class LongCount<Row> extends AbstractAccumulator<Row> {
        /** */
        private long cnt;

        /** */
        LongCount(AggregateCall aggCall, RowHandler<Row> hnd) {
            super(aggCall, hnd);
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            int argsCnt = arguments().size();

            assert argsCnt == 0 || argsCnt == 1;

            if (argsCnt == 0 || get(0, row) != null)
                cnt++;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            LongCount<Row> other0 = (LongCount<Row>)other;
            cnt += other0.cnt;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return cnt;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), false));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createSqlType(BIGINT);
        }
    }

    /** */
    private static class Sum<Row> extends AbstractAccumulator<Row> {
        /** */
        private final Accumulator<Row> acc;

        /** */
        private boolean empty = true;

        /** */
        public Sum(AggregateCall aggCall, Accumulator<Row> acc, RowHandler<Row> hnd) {
            super(aggCall, hnd);

            this.acc = acc;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            Object val = get(0, row);

            if (val == null)
                return;

            empty = false;
            acc.add(row);
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            Sum<Row> other0 = (Sum<Row>)other;

            if (other0.empty)
                return;

            empty = false;
            acc.apply(other0.acc);
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return empty ? null : acc.end();
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return acc.argumentTypes(typeFactory);
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return acc.returnType(typeFactory);
        }
    }

    /** */
    private static class DoubleSumEmptyIsZero<Row> extends AbstractAccumulator<Row> {
        /** */
        private double sum;

        /** */
        DoubleSumEmptyIsZero(AggregateCall aggCall, RowHandler<Row> hnd) {
            super(aggCall, hnd);
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            Double in = get(0, row);

            if (in == null)
                return;

            sum += in;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            DoubleSumEmptyIsZero<Row> other0 = (DoubleSumEmptyIsZero<Row>)other;

            sum += other0.sum;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return sum;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeFactory.createTypeWithNullability(typeFactory.createSqlType(DOUBLE), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(DOUBLE), true);
        }
    }

    /** */
    private static class LongSumEmptyIsZero<Row> extends AbstractAccumulator<Row> {
        /** */
        private long sum;

        /** */
        LongSumEmptyIsZero(AggregateCall aggCall, RowHandler<Row> hnd) {
            super(aggCall, hnd);
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            Long in = get(0, row);

            if (in == null)
                return;

            sum += in;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            LongSumEmptyIsZero<Row> other0 = (LongSumEmptyIsZero<Row>)other;

            sum += other0.sum;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return sum;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeFactory.createTypeWithNullability(typeFactory.createSqlType(BIGINT), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(BIGINT), true);
        }
    }

    /** */
    private static class DecimalSumEmptyIsZero<Row> extends AbstractAccumulator<Row> {
        /** */
        private BigDecimal sum;

        /** */
        DecimalSumEmptyIsZero(AggregateCall aggCall, RowHandler<Row> hnd) {
            super(aggCall, hnd);
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            BigDecimal in = get(0, row);

            if (in == null)
                return;

            sum = sum == null ? in : sum.add(in);
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            DecimalSumEmptyIsZero<Row> other0 = (DecimalSumEmptyIsZero<Row>)other;

            sum = sum == null ? other0.sum : sum.add(other0.sum);
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return sum != null ? sum : BigDecimal.ZERO;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeFactory.createTypeWithNullability(typeFactory.createSqlType(DECIMAL), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(DECIMAL), true);
        }
    }

    /** */
    private static class DoubleMinMax<Row> extends AbstractAccumulator<Row> {
        /** */
        private final boolean min;

        /** */
        private double val;

        /** */
        private boolean empty = true;

        /** */
        private DoubleMinMax(AggregateCall aggCall, RowHandler<Row> hnd, boolean min) {
            super(aggCall, hnd);
            this.min = min;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            Double in = get(0, row);

            if (in == null)
                return;

            val = empty ? in : min ? Math.min(val, in) : Math.max(val, in);
            empty = false;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            DoubleMinMax<Row> other0 = (DoubleMinMax<Row>)other;

            if (other0.empty)
                return;

            val = empty ? other0.val : min ? Math.min(val, other0.val) : Math.max(val, other0.val);
            empty = false;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return empty ? null : val;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeFactory.createTypeWithNullability(typeFactory.createSqlType(DOUBLE), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(DOUBLE), true);
        }
    }

    /** */
    private static class VarCharMinMax<Row> extends AbstractAccumulator<Row> {
        /** */
        private final boolean min;

        /** */
        private CharSequence val;

        /** */
        private boolean empty = true;

        /** */
        VarCharMinMax(AggregateCall aggCall, RowHandler<Row> hnd, boolean min) {
            super(aggCall, hnd);
            this.min = min;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            CharSequence in = get(0, row);

            if (in == null)
                return;

            val = empty ? in : min ?
                (CharSeqComparator.INSTANCE.compare(val, in) < 0 ? val : in) :
                (CharSeqComparator.INSTANCE.compare(val, in) < 0 ? in : val);

            empty = false;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            VarCharMinMax<Row> other0 = (VarCharMinMax<Row>)other;

            if (other0.empty)
                return;

            val = empty ? other0.val : min ?
                (CharSeqComparator.INSTANCE.compare(val, other0.val) < 0 ? val : other0.val) :
                (CharSeqComparator.INSTANCE.compare(val, other0.val) < 0 ? other0.val : val);

            empty = false;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return empty ? null : val;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeFactory.createTypeWithNullability(typeFactory.createSqlType(VARCHAR), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(VARCHAR), true);
        }

        /** */
        @SuppressWarnings("ComparatorNotSerializable")
        private static class CharSeqComparator implements Comparator<CharSequence> {
            /** */
            private static final CharSeqComparator INSTANCE = new CharSeqComparator();

            /** */
            @Override public int compare(CharSequence s1, CharSequence s2) {
                int len = Math.min(s1.length(), s2.length());

                // find the first difference and return
                for (int i = 0; i < len; i += 1) {
                    int cmp = Character.compare(s1.charAt(i), s2.charAt(i));
                    if (cmp != 0)
                        return cmp;
                }

                // if there are no differences, then the shorter seq is first
                return Integer.compare(s1.length(), s2.length());
            }
        }
    }

    /** */
    private static class IntMinMax<Row> extends AbstractAccumulator<Row> {
        /** */
        private final boolean min;

        /** */
        private int val;

        /** */
        private boolean empty = true;

        /** */
        private IntMinMax(AggregateCall aggCall, RowHandler<Row> hnd, boolean min) {
            super(aggCall, hnd);
            this.min = min;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            Integer in = get(0, row);

            if (in == null)
                return;

            val = empty ? in : min ? Math.min(val, in) : Math.max(val, in);
            empty = false;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            IntMinMax<Row> other0 = (IntMinMax<Row>)other;

            if (other0.empty)
                return;

            val = empty ? other0.val : min ? Math.min(val, other0.val) : Math.max(val, other0.val);
            empty = false;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return empty ? null : val;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeFactory.createTypeWithNullability(typeFactory.createSqlType(INTEGER), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(INTEGER), true);
        }
    }

    /** */
    private static class LongMinMax<Row> extends AbstractAccumulator<Row> {
        /** */
        private final boolean min;

        /** */
        private long val;

        /** */
        private boolean empty = true;

        /** */
        private LongMinMax(AggregateCall aggCall, RowHandler<Row> hnd, boolean min) {
            super(aggCall, hnd);

            this.min = min;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            Long in = get(0, row);

            if (in == null)
                return;

            val = empty ? in : min ? Math.min(val, in) : Math.max(val, in);
            empty = false;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            LongMinMax<Row> other0 = (LongMinMax<Row>)other;

            if (other0.empty)
                return;

            val = empty ? other0.val : min ? Math.min(val, other0.val) : Math.max(val, other0.val);
            empty = false;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return empty ? null : val;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeFactory.createTypeWithNullability(typeFactory.createSqlType(BIGINT), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(BIGINT), true);
        }
    }

    /** */
    private static class DecimalMinMax<Row> extends AbstractAccumulator<Row> {
        /** */
        private final boolean min;

        /** */
        private BigDecimal val;

        /** */
        private DecimalMinMax(AggregateCall aggCall, RowHandler<Row> hnd, boolean min) {
            super(aggCall, hnd);
            this.min = min;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            BigDecimal in = get(0, row);

            if (in == null)
                return;

            val = val == null ? in : min ? val.min(in) : val.max(in);
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            DecimalMinMax<Row> other0 = (DecimalMinMax<Row>)other;

            if (other0.val == null)
                return;

            val = val == null ? other0.val : min ? val.min(other0.val) : val.max(other0.val);
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeFactory.createTypeWithNullability(typeFactory.createSqlType(DECIMAL), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(DECIMAL), true);
        }
    }

    /** */
    private static class ComparableMinMax<Row, T extends Comparable<T>> extends AbstractAccumulator<Row> {
        /** */
        private final boolean min;

        /** */
        private final transient Function<IgniteTypeFactory, RelDataType> typeSupplier;

        /** */
        private T val;

        /** */
        private boolean empty = true;

        /** */
        private ComparableMinMax(
            AggregateCall aggCall,
            RowHandler<Row> hnd,
            boolean min,
            Function<IgniteTypeFactory, RelDataType> typeSupplier
        ) {
            super(aggCall, hnd);
            this.min = min;
            this.typeSupplier = typeSupplier;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            T in = get(0, row);

            if (in == null)
                return;

            val = empty ? in : min ?
                (val.compareTo(in) < 0 ? val : in) :
                (val.compareTo(in) < 0 ? in : val);

            empty = false;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            ComparableMinMax<Row, T> other0 = (ComparableMinMax<Row, T>)other;

            if (other0.empty)
                return;

            val = empty ? other0.val : min ?
                (val.compareTo(other0.val) < 0 ? val : other0.val) :
                (val.compareTo(other0.val) < 0 ? other0.val : val);

            empty = false;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return empty ? null : val;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeSupplier.apply(typeFactory));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeSupplier.apply(typeFactory);
        }
    }

    /** */
    private static class SortingAccumulator<Row> implements IterableAccumulator<Row> {
        /** */
        private final transient Comparator<Row> cmp;

        /** */
        private final List<Row> list;

        /** */
        private final Accumulator<Row> acc;

        /**
         * @param accSup Accumulator supplier.
         * @param cmp Comparator.
         */
        private SortingAccumulator(Supplier<Accumulator<Row>> accSup, Comparator<Row> cmp) {
            this.cmp = cmp;

            list = new ArrayList<>();
            acc = accSup.get();
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            list.add(row);
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            SortingAccumulator<Row> other1 = (SortingAccumulator<Row>)other;

            list.addAll(other1.list);
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            list.sort(cmp);

            for (Row row : list)
                acc.add(row);

            return acc.end();
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return acc.argumentTypes(typeFactory);
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return acc.returnType(typeFactory);
        }

        /** {@inheritDoc} */
        @Override public Iterator<Row> iterator() {
            return list.iterator();
        }
    }

    /** */
    private abstract static class AggAccumulator<Row> extends AbstractAccumulator<Row> implements IterableAccumulator<Row> {
        /** */
        private final List<Row> buf;

        /** */
        protected AggAccumulator(AggregateCall aggCall, RowHandler<Row> hnd) {
            super(aggCall, hnd);

            buf = new ArrayList<>();
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            if (row == null)
                return;

            buf.add(row);
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            AggAccumulator<Row> other0 = (AggAccumulator<Row>)other;

            buf.addAll(other0.buf);
        }

        /** {@inheritDoc} */
        @Override public Iterator<Row> iterator() {
            return buf.iterator();
        }

        /** */
        public boolean isEmpty() {
            return buf.isEmpty();
        }

        /** */
        public int size() {
            return buf.size();
        }
    }

    /** */
    private static class ListAggAccumulator<Row> extends AggAccumulator<Row> {
        /** Default separator. */
        private static final String DEFAULT_SEPARATOR = ",";

        /** */
        private final boolean isDfltSep;

        /** */
        public ListAggAccumulator(AggregateCall aggCall, RowHandler<Row> hnd) {
            super(aggCall, hnd);

            isDfltSep = arguments().size() <= 1;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            if (isEmpty())
                return null;

            StringBuilder builder = null;

            for (Row row: this) {
                Object val = get(0, row);

                if (val == null)
                    continue;

                if (builder == null)
                    builder = new StringBuilder();

                if (builder.length() != 0)
                    builder.append(extractSeparator(row));
                builder.append(val);
            }

            return builder != null ? builder.toString() : null;
        }

        /** */
        private String extractSeparator(Row row) {
            if (isDfltSep || columnCount(row) <= 1)
                return DEFAULT_SEPARATOR;

            Object rawSep = get(1, row);

            if (rawSep == null)
                return DEFAULT_SEPARATOR;

            return rawSep.toString();
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeFactory.createTypeWithNullability(typeFactory.createSqlType(VARCHAR), true),
                typeFactory.createTypeWithNullability(typeFactory.createSqlType(CHAR), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(VARCHAR), true);
        }
    }

    /** */
    private static class ArrayAggregateAccumulator<Row> extends AggAccumulator<Row> {
        /** */
        public ArrayAggregateAccumulator(AggregateCall aggCall, RowHandler<Row> hnd) {
            super(aggCall, hnd);
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            if (size() == 0)
                return null;

            List<Object> result = new ArrayList<>(size());
            for (Row row: this)
                result.add(get(0, row));

            return result;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createArrayType(
                typeFactory.createSqlType(ANY), -1), true);
        }
    }

    /** */
    private static class ArrayConcatAggregateAccumulator<Row> extends AggAccumulator<Row> {
        /** */
        public ArrayConcatAggregateAccumulator(AggregateCall aggCall, RowHandler<Row> hnd) {
            super(aggCall, hnd);
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            if (size() == 0)
                return null;

            List<Object> result = new ArrayList<>(size());

            for (Row row: this) {
                List<Object> arr = get(0, row);

                if (F.isEmpty(arr))
                    continue;

                result.addAll(arr);
            }

            if (result.isEmpty())
                return null;

            return result;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeFactory.createTypeWithNullability(typeFactory.createArrayType(
                typeFactory.createSqlType(ANY), -1), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createArrayType(
                typeFactory.createSqlType(ANY), -1), true);
        }
    }

    /** */
    private static class DistinctAccumulator<Row> extends AbstractAccumulator<Row> {
        /** */
        private final Accumulator<Row> acc;

        /** */
        private final Map<Object, Row> rows = new HashMap<>();

        /** */
        private final List<Integer> args;

        /** */
        private DistinctAccumulator(AggregateCall aggCall, RowHandler<Row> hnd, Supplier<Accumulator<Row>> accSup) {
            super(aggCall, hnd);

            acc = accSup.get();

            args = super.arguments().isEmpty() ? List.of(0) : super.arguments();
        }

        /** */
        @Override protected List<Integer> arguments() {
            return args;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            Object key;

            if (row == null || columnCount(row) == 0 || (key = get(0, row)) == null)
                return;

            rows.put(key, row);
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            DistinctAccumulator<Row> other0 = (DistinctAccumulator<Row>)other;

            rows.putAll(other0.rows);
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            for (Row row: rows.values())
                acc.add(row);

            return acc.end();
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return acc.argumentTypes(typeFactory);
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return acc.returnType(typeFactory);
        }
    }
}
