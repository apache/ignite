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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.mapping.Mappings;
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
            return () -> new DistinctAccumulator<>(supplier, ctx.rowHandler());

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
                return () -> new LongCount<>(hnd);
            case "AVG":
                return avgFactory(call, hnd);
            case "SUM":
                return sumFactory(call, hnd);
            case "$SUM0":
                return sumEmptyIsZeroFactory(call, hnd);
            case "MIN":
                return minFactory(call, hnd);
            case "MAX":
                return maxFactory(call, hnd);
            case "SINGLE_VALUE":
                return () -> new SingleVal<>(hnd);
            case "ANY_VALUE":
                return () -> new AnyVal<>(hnd);
            case "LISTAGG":
                return listAggregateSupplier(call, ctx);
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

        Supplier<Accumulator<Row>> accSup = () -> new ListAggAccumulator<>(call, hnd);

        RelCollation mapColl = getMappedCollation(call);

        Comparator<Row> cmp;
        if (mapColl != null) {
            cmp = ctx.expressionFactory().comparator(getMappedCollation(call));

            return () -> new SortingAccumulator<>(accSup, cmp);
        }

        return accSup;
    }

    /** */
    private static RelCollation getMappedCollation(AggregateCall call) {
        if (call.getCollation() == null || call.getCollation().getFieldCollations().isEmpty())
            return null;

        List<RelFieldCollation> collations = call.getCollation().getFieldCollations();
        List<Integer> argList = call.getArgList();

        // The target value will be accessed by field index in mapping array (targets[fieldIndex]),
        // so srcCnt should be "max_field_index + 1" to prevent IndexOutOfBoundsException.
        int srcCnt = Collections.max(collations, Comparator.comparingInt(RelFieldCollation::getFieldIndex))
            .getFieldIndex() + 1;

        Map<Integer, Integer> mapping = new HashMap<>();

        int collOff = 0;
        for (int i = 0; i < collations.size(); i++) {
            int idx = collations.get(i).getFieldIndex();

            int mapIdx = argList.indexOf(idx);
            if (mapIdx == -1) { //collation not found in arglist
                mapIdx = argList.size() + collOff;

                collOff++;
            }

            mapping.put(idx, mapIdx);
        }

        return mapping.isEmpty() ? call.getCollation() : call.getCollation()
            .apply(Mappings.target(mapping, srcCnt, srcCnt));
    }

    /** */
    private static <Row> Supplier<Accumulator<Row>> avgFactory(AggregateCall call, RowHandler<Row> hnd) {
        switch (call.type.getSqlTypeName()) {
            case ANY:
                throw new UnsupportedOperationException("AVG() is not supported for type '" + call.type + "'.");
            case BIGINT:
            case DECIMAL:
                return () -> new DecimalAvg<>(hnd);
            case DOUBLE:
            case REAL:
            case FLOAT:
            case INTEGER:
            default:
                return () -> new DoubleAvg<>(hnd);
        }
    }

    /** */
    private static <Row> Supplier<Accumulator<Row>> sumFactory(AggregateCall call, RowHandler<Row> hnd) {
        switch (call.type.getSqlTypeName()) {
            case ANY:
                throw new UnsupportedOperationException("SUM() is not supported for type '" + call.type + "'.");

            case BIGINT:
            case DECIMAL:
                return () -> new Sum<>(new DecimalSumEmptyIsZero<>(hnd), hnd);

            case DOUBLE:
            case REAL:
            case FLOAT:
                return () -> new Sum<>(new DoubleSumEmptyIsZero<>(hnd), hnd);

            case TINYINT:
            case SMALLINT:
            case INTEGER:
            default:
                return () -> new Sum<>(new LongSumEmptyIsZero<>(hnd), hnd);
        }
    }

    /** */
    private static <Row> Supplier<Accumulator<Row>> sumEmptyIsZeroFactory(AggregateCall call, RowHandler<Row> hnd) {
        switch (call.type.getSqlTypeName()) {
            case ANY:
                throw new UnsupportedOperationException("SUM() is not supported for type '" + call.type + "'.");

            case BIGINT:
            case DECIMAL:
                return () -> new DecimalSumEmptyIsZero<>(hnd);

            case DOUBLE:
            case REAL:
            case FLOAT:
                return () -> new DoubleSumEmptyIsZero<>(hnd);

            case TINYINT:
            case SMALLINT:
            case INTEGER:
            default:
                return () -> new LongSumEmptyIsZero<>(hnd);
        }
    }

    /** */
    private static <Row> Supplier<Accumulator<Row>> minFactory(AggregateCall call, RowHandler<Row> hnd) {
        switch (call.type.getSqlTypeName()) {
            case DOUBLE:
            case REAL:
            case FLOAT:
                return () -> new DoubleMinMax<>(true, hnd);
            case DECIMAL:
                return () -> new DecimalMinMax<>(true, hnd);
            case INTEGER:
                return () -> new IntMinMax<>(true, hnd);
            case CHAR:
            case VARCHAR:
                return () -> new VarCharMinMax<>(true, hnd);
            case BINARY:
            case VARBINARY:
                return () -> new ComparableMinMax<Row, ByteString>(true,
                    tf -> tf.createTypeWithNullability(tf.createSqlType(VARBINARY), true), hnd);
            case ANY:
                if (call.type instanceof UuidType) {
                    return () -> new ComparableMinMax<Row, UUID>(true,
                        tf -> tf.createTypeWithNullability(tf.createCustomType(UUID.class), true), hnd);
                }
                throw new UnsupportedOperationException("MIN() is not supported for type '" + call.type + "'.");
            case BIGINT:
            default:
                return () -> new LongMinMax<>(true, hnd);
        }
    }

    /** */
    private static <Row> Supplier<Accumulator<Row>> maxFactory(AggregateCall call, RowHandler<Row> hnd) {
        switch (call.type.getSqlTypeName()) {
            case DOUBLE:
            case REAL:
            case FLOAT:
                return () -> new DoubleMinMax<>(false, hnd);
            case DECIMAL:
                return () -> new DecimalMinMax<>(false, hnd);
            case INTEGER:
                return () -> new IntMinMax<>(false, hnd);
            case CHAR:
            case VARCHAR:
                return () -> new VarCharMinMax<>(false, hnd);
            case BINARY:
            case VARBINARY:
                return () -> new ComparableMinMax<Row, ByteString>(false,
                    tf -> tf.createTypeWithNullability(tf.createSqlType(VARBINARY), true), hnd);
            case ANY:
                if (call.type instanceof UuidType) {
                    return () -> new ComparableMinMax<Row, UUID>(false,
                        tf -> tf.createTypeWithNullability(tf.createCustomType(UUID.class), true), hnd);
                }
                throw new UnsupportedOperationException("MAX() is not supported for type '" + call.type + "'.");
            case BIGINT:
            default:
                return () -> new LongMinMax<>(false, hnd);
        }
    }

    /** */
    private abstract static class AbstractAccumulator<Row> implements Accumulator<Row> {
        /** */
        private final RowHandler<Row> hnd;

        /** */
        AbstractAccumulator(RowHandler<Row> hnd) {
            this.hnd = hnd;
        }

        /** */
        <T> T get(int idx, Row row) {
            return (T)hnd.get(idx, row);
        }

        /** */
        <T> void set(int idx, Row row, T val) {
            hnd.set(idx, row, val);
        }

        /** */
        int columnCount(Row row) {
            return hnd.columnCount(row);
        }

        /** */
        Row createRow(IgniteTypeFactory typeFactory, List<RelDataType> fieldTypes) {
            return hnd.factory(typeFactory, fieldTypes).create();
        }
    }

    /** */
    private static class SingleVal<Row> extends AnyVal<Row> {
        /** */
        private boolean touched;

        /** */
        SingleVal(RowHandler<Row> hnd) {
            super(hnd);
        }

        /** */
        @Override public void add(Row row) {
            if (touched)
                throw new IllegalArgumentException("Subquery returned more than 1 value.");

            touched = true;

            super.add(row);
        }

        /** */
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

    /** */
    private static class AnyVal<Row> extends AbstractAccumulator<Row> {
        /** */
        private Object holder;

        /** */
        AnyVal(RowHandler<Row> hnd) {
            super(hnd);
        }

        /** */
        @Override public void add(Row row) {
            assert columnCount(row) == 1 : "Column count " + columnCount(row);

            if (holder == null)
                holder = get(0, row);
        }

        /** */
        @Override public void apply(Accumulator<Row> other) {
            if (holder == null)
                holder = ((AnyVal<Row>)other).holder;
        }

        /** */
        @Override public Object end() {
            return holder;
        }

        /** */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList(typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), true));
        }

        /** */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createSqlType(ANY);
        }
    }

    /** */
    private static class DecimalAvg<Row> extends AbstractAccumulator<Row> {
        /** */
        private BigDecimal sum = BigDecimal.ZERO;

        /** */
        private BigDecimal cnt = BigDecimal.ZERO;

        /** */
        DecimalAvg(RowHandler<Row> hnd) {
            super(hnd);
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
        DoubleAvg(RowHandler<Row> hnd) {
            super(hnd);
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
        LongCount(RowHandler<Row> hnd) {
            super(hnd);
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            int colCnt = columnCount(row);

            assert colCnt == 0 || colCnt == 1;

            if (colCnt == 0 || get(0, row) != null)
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
        public Sum(Accumulator<Row> acc, RowHandler<Row> hnd) {
            super(hnd);

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
        DoubleSumEmptyIsZero(RowHandler<Row> hnd) {
            super(hnd);
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
        LongSumEmptyIsZero(RowHandler<Row> hnd) {
            super(hnd);
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
        DecimalSumEmptyIsZero(RowHandler<Row> hnd) {
            super(hnd);
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
        private DoubleMinMax(boolean min, RowHandler<Row> hnd) {
            super(hnd);
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
        VarCharMinMax(boolean min, RowHandler<Row> hnd) {
            super(hnd);
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
        private IntMinMax(boolean min, RowHandler<Row> hnd) {
            super(hnd);
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
        private LongMinMax(boolean min, RowHandler<Row> hnd) {
            super(hnd);

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
        private DecimalMinMax(boolean min, RowHandler<Row> hnd) {
            super(hnd);
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
        private final Function<IgniteTypeFactory, RelDataType> typeSupplier;

        /** */
        private T val;

        /** */
        private boolean empty = true;

        /** */
        private ComparableMinMax(boolean min, Function<IgniteTypeFactory, RelDataType> typeSupplier, RowHandler<Row> hnd) {
            super(hnd);
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
    private static class SortingAccumulator<Row> implements Accumulator<Row> {
        /** */
        private final transient Comparator<Row> cmp;

        /** */
        private final List<Row> list;

        /** */
        private final Accumulator<Row> acc;

        /**
         * @param accSup Acc support.
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
    }

    /** */
    private static class ListAggAccumulator<Row> extends AbstractAccumulator<Row> {
        /** Default separator. */
        private static final String DEFAULT_SEPARATOR = ",";

        /** */
        private final List<Row> list;

        /** */
        private final int sepIdx;

        /** */
        public ListAggAccumulator(AggregateCall call, RowHandler<Row> hnd) {
            super(hnd);

            sepIdx = call.getArgList().size() > 1 ? 1 : 0;

            list = new ArrayList<>();
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            if (row == null || get(0, row) == null)
                return;

            list.add(row);
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            ListAggAccumulator<Row> other0 = (ListAggAccumulator<Row>)other;

            list.addAll(other0.list);
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            if (list.isEmpty())
                return null;

            StringBuilder builder = new StringBuilder();

            for (Row row: list) {
                if (builder.length() != 0)
                    builder.append(extractSeparator(row));

                builder.append(Objects.toString(get(0, row)));
            }

            return builder.toString();
        }

        /** */
        private String extractSeparator(Row row) {
            if (sepIdx < 1 || columnCount(row) <= sepIdx)
                return DEFAULT_SEPARATOR;

            Object rawSep = get(sepIdx, row);

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
    private static class DistinctAccumulator<Row> extends AbstractAccumulator<Row> {
        /** */
        private final Accumulator<Row> acc;

        /** */
        private final Map<Object, Row> rows = new LinkedHashMap<>();

        /** */
        private DistinctAccumulator(Supplier<Accumulator<Row>> accSup, RowHandler<Row> hnd) {
            super(hnd);
            acc = accSup.get();
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            if (row == null || columnCount(row) == 0)
                return;

            rows.put(get(0, row), row);
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
