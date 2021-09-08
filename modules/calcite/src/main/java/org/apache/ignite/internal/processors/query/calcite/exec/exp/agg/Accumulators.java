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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

/**
 *
 */
public class Accumulators<Row> {
    /** */
    public static <Row> Supplier<Accumulator<Row>> accumulatorFactory(AggregateCall call, ExecutionContext<Row> ctx) {
        RowHandler<Row> hnd = ctx.rowHandler();

        Supplier<Accumulator<Row>> fac = accumulatorFunctionFactory(call, hnd);

        Comparator<Row> comp = ctx.expressionFactory().comparator(getMappedCollation(call));

        Supplier<Accumulator<Row>> supplier = getSortingAccSupplierIfNeeded(call, ctx, hnd, fac, comp);

        if (call.isDistinct())
            return () -> new DistinctAccumulator<>(supplier, hnd, ctx.getTypeFactory());

        return supplier;
    }

    /** */
    private static <Row> Supplier<Accumulator<Row>> getSortingAccSupplierIfNeeded(AggregateCall call,
        ExecutionContext<Row> ctx, RowHandler<Row> hnd, Supplier<Accumulator<Row>> fac, Comparator<Row> comp) {
        if (comp == null)
            return fac;

        int colFieldsCnt = (int) calculateOuterCollations(call);

        if (colFieldsCnt > 0)
            return () -> new SortingAccumulator<>(
                () -> new CollationExtracorAccumulator<>(fac, colFieldsCnt, hnd, ctx.getTypeFactory()),
                comp
            );

        return () -> new SortingAccumulator<>(fac, comp);
    }

    /** */
    private static long calculateOuterCollations(AggregateCall call) {
        List<RelFieldCollation> collations = call.getCollation().getFieldCollations();
        List<Integer> argList = call.getArgList();

        return collations.stream().map(RelFieldCollation::getFieldIndex)
            .map(argList::indexOf)
            .filter(index -> index == -1) //collation not found in arglist
            .count();
    }

    /** */
    private static RelCollation getMappedCollation(AggregateCall call) {
        if (call.getCollation() == null || call.getCollation().getFieldCollations().isEmpty())
            return null;

        List<RelFieldCollation> collations = call.getCollation().getFieldCollations();
        List<Integer> argList = call.getArgList();

        // The target value will be accessed by field index in mapping array (targets[fieldIndex]),
        // so srcCnt should be "max_field_index + 1" to prevent IndexOutOfBoundsException.
        final int srcCnt = collations.stream().map(RelFieldCollation::getFieldIndex)
            .max(Integer::compare).get() + 1;

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
        
        return mapping.isEmpty() ?
            call.getCollation() : call.getCollation().apply(Mappings.target(mapping, srcCnt, srcCnt));
    }

    /** */
    private static <Row> Supplier<Accumulator<Row>> accumulatorFunctionFactory(AggregateCall call, RowHandler<Row> hnd) {
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
            case "LISTAGG":
                return () -> new ListAggAccumulator<>(hnd);
            default:
                throw new AssertionError(call.getAggregation().getName());
        }
    }

    /** */
    private static <Row> Supplier<Accumulator<Row>> avgFactory(AggregateCall call, RowHandler<Row> hnd) {
        switch (call.type.getSqlTypeName()) {
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
            case DOUBLE:
            case REAL:
            case FLOAT:
                return () -> new DoubleSum<>(hnd);
            case DECIMAL:
                return () -> new DecimalSum<>(hnd);
            case INTEGER:
                return () -> new IntSum<>(hnd);
            case BIGINT:
            default:
                return () -> new LongSum<>(hnd);
        }
    }

    /** */
    private static <Row> Supplier<Accumulator<Row>> sumEmptyIsZeroFactory(AggregateCall call, RowHandler<Row> hnd) {
        switch (call.type.getSqlTypeName()) {
            case DOUBLE:
            case REAL:
            case FLOAT:
                return () -> new DoubleSumEmptyIsZero<>(hnd);
            case DECIMAL:
                return () -> new DecimalSumEmptyIsZero<>(hnd);
            case INTEGER:
                return () -> new IntSumEmptyIsZero<>(hnd);
            case BIGINT:
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
            case BIGINT:
            default:
                return () -> new LongMinMax<>(false, hnd);
        }
    }

    /** */
    private static class SingleVal<Row> implements Accumulator<Row> {
        /** */
        private Object holder;

        /** */
        private boolean touched;

        /** */
        private final RowHandler<Row> hnd;

        /** */
        private SingleVal(RowHandler<Row> hnd) {
            this.hnd = hnd;
        }

        /** */
        @Override public void add(Row row) {
            assert hnd.columnCount(row) == 1 : hnd.columnCount(row);

            if (touched)
                throw new IllegalArgumentException("Subquery returned more than 1 value.");

            touched = true;

            holder = hnd.get(0, row);
        }

        /** */
        @Override public void apply(Accumulator<Row> other) {
            assert holder == null : "sudden apply for: " + other + " on SingleVal";

            holder = ((SingleVal<Row>)other).holder;
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
    private static class DecimalAvg<Row> implements Accumulator<Row> {
        /** */
        private BigDecimal sum = BigDecimal.ZERO;

        /** */
        private BigDecimal cnt = BigDecimal.ZERO;

        /** */
        private final RowHandler<Row> hnd;

        /** */
        private DecimalAvg(RowHandler<Row> hnd) {
            this.hnd = hnd;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            BigDecimal in = (BigDecimal)hnd.get(0, row);

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
    private static class DoubleAvg<Row> implements Accumulator<Row> {
        /** */
        private double sum;

        /** */
        private long cnt;

        /** */
        private final RowHandler<Row> hnd;

        /** */
        private DoubleAvg(RowHandler<Row> hnd) {
            this.hnd = hnd;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            Double in = (Double)hnd.get(0, row);

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
    private static class LongCount<Row> implements Accumulator<Row> {
        /** */
        private long cnt;

        /** */
        private final RowHandler<Row> hnd;

        /** */
        private LongCount(RowHandler<Row> hnd) {
            this.hnd = hnd;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            assert row == null || hnd.columnCount(row) == 0 || hnd.columnCount(row) == 1;

            if (row == null || hnd.columnCount(row) == 0 || hnd.get(0, row) != null)
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
    private static class DoubleSum<Row> implements Accumulator<Row> {
        /** */
        private double sum;

        /** */
        private boolean empty = true;

        /** */
        private final RowHandler<Row> hnd;

        /** */
        private DoubleSum(RowHandler<Row> hnd) {
            this.hnd = hnd;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            Double in = (Double)hnd.get(0, row);

            if (in == null)
                return;

            empty = false;
            sum += in;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            DoubleSum<Row> other0 = (DoubleSum<Row>)other;

            if (other0.empty)
                return;

            empty = false;
            sum += other0.sum;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return empty ? null : sum;
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
    private static class IntSum<Row> implements Accumulator<Row> {
        /** */
        private int sum;

        /** */
        private boolean empty = true;

        /** */
        private final RowHandler<Row> hnd;

        /** */
        private IntSum(RowHandler<Row> hnd) {
            this.hnd = hnd;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            Integer in = (Integer)hnd.get(0, row);

            if (in == null)
                return;

            empty = false;
            sum += in;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            IntSum<Row> other0 = (IntSum<Row>)other;

            if (other0.empty)
                return;

            empty = false;
            sum += other0.sum;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return empty ? null : sum;
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
    private static class LongSum<Row> implements Accumulator<Row> {
        /** */
        private long sum;

        /** */
        private boolean empty = true;

        /** */
        private final RowHandler<Row> hnd;

        /** */
        private LongSum(RowHandler<Row> hnd) {
            this.hnd = hnd;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            Long in = (Long)hnd.get(0, row);

            if (in == null)
                return;

            empty = false;
            sum += in;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            LongSum<Row> other0 = (LongSum<Row>)other;

            if (other0.empty)
                return;

            empty = false;
            sum += other0.sum;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return empty ? null : sum;
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
    private static class DecimalSum<Row> implements Accumulator<Row> {
        /** */
        private BigDecimal sum;

        /** */
        private final RowHandler<Row> hnd;

        /** */
        private DecimalSum(RowHandler<Row> hnd) {
            this.hnd = hnd;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            BigDecimal in = (BigDecimal)hnd.get(0, row);

            if (in == null)
                return;

            sum = sum == null ? in : sum.add(in);
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            DecimalSum<Row> other0 = (DecimalSum<Row>)other;

            if (other0.sum == null)
                return;

            sum = sum == null ? other0.sum : sum.add(other0.sum);
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return sum;
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
    private static class DoubleSumEmptyIsZero<Row> implements Accumulator<Row> {
        /** */
        private double sum;

        /** */
        private final RowHandler<Row> hnd;

        /** */
        private DoubleSumEmptyIsZero(RowHandler<Row> hnd) {
            this.hnd = hnd;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            Double in = (Double)hnd.get(0, row);

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
    private static class IntSumEmptyIsZero<Row> implements Accumulator<Row> {
        /** */
        private int sum;

        /** */
        private final RowHandler<Row> hnd;

        /** */
        private IntSumEmptyIsZero(RowHandler<Row> hnd) {
            this.hnd = hnd;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            Integer in = (Integer)hnd.get(0, row);

            if (in == null)
                return;

            sum += in;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            IntSumEmptyIsZero<Row> other0 = (IntSumEmptyIsZero<Row>)other;

            sum += other0.sum;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return sum;
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
    private static class LongSumEmptyIsZero<Row> implements Accumulator<Row> {
        /** */
        private long sum;

        /** */
        private final RowHandler<Row> hnd;

        /** */
        private LongSumEmptyIsZero(RowHandler<Row> hnd) {
            this.hnd = hnd;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            Long in = (Long)hnd.get(0, row);

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
    private static class DecimalSumEmptyIsZero<Row> implements Accumulator<Row> {
        /** */
        private BigDecimal sum;

        /** */
        private final RowHandler<Row> hnd;

        /** */
        private DecimalSumEmptyIsZero(RowHandler<Row> hnd) {
            this.hnd = hnd;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            BigDecimal in = (BigDecimal)hnd.get(0, row);

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
    private static class DoubleMinMax<Row> implements Accumulator<Row> {
        /** */
        private final boolean min;

        /** */
        private double val;

        /** */
        private boolean empty = true;

        /** */
        private final RowHandler<Row> hnd;

        /** */
        private DoubleMinMax(boolean min, RowHandler<Row> hnd) {
            this.min = min;
            this.hnd = hnd;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            Double in = (Double)hnd.get(0, row);

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
    private static class VarCharMinMax<Row> implements Accumulator<Row> {
        /** */
        private final boolean min;

        /** */
        private CharSequence val;

        /** */
        private boolean empty = true;

        /** */
        private final RowHandler<Row> hnd;

        /** */
        private VarCharMinMax(boolean min, RowHandler<Row> hnd) {
            this.min = min;
            this.hnd = hnd;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            CharSequence in = (CharSequence)hnd.get(0, row);

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
    private static class IntMinMax<Row> implements Accumulator<Row> {
        /** */
        private final boolean min;

        /** */
        private int val;

        /** */
        private boolean empty = true;

        /** */
        private final RowHandler<Row> hnd;

        /** */
        private IntMinMax(boolean min, RowHandler<Row> hnd) {
            this.min = min;
            this.hnd = hnd;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            Integer in = (Integer)hnd.get(0, row);

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
    private static class LongMinMax<Row> implements Accumulator<Row> {
        /** */
        private final boolean min;

        /** */
        private long val;

        /** */
        private boolean empty = true;

        /** */
        private final RowHandler<Row> hnd;

        /** */
        private LongMinMax(boolean min, RowHandler<Row> hnd) {
            this.min = min;
            this.hnd = hnd;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            Long in = (Long)hnd.get(0, row);

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
    private static class DecimalMinMax<Row> implements Accumulator<Row> {
        /** */
        private final boolean min;

        /** */
        private BigDecimal val;

        /** */
        private final RowHandler<Row> hnd;
        
        /** */
        private DecimalMinMax(boolean min, RowHandler<Row> hnd) {
            this.hnd = hnd;
            this.min = min;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            BigDecimal in = (BigDecimal)hnd.get(0, row);

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
    private static class DistinctAccumulator<Row> implements Accumulator<Row> {
        /** */
        private final Accumulator<Row> acc;
        
        /** */
        private final RowHandler<Row> hnd;

        /** */
        private final IgniteTypeFactory factory;

        /** */
        private final Set<GroupKey> set = new LinkedHashSet<>();

        /** */
        private DistinctAccumulator(Supplier<Accumulator<Row>> accSup, RowHandler<Row> hnd,
            IgniteTypeFactory factory) {
            this.acc = accSup.get();
            this.hnd = hnd;
            this.factory = factory;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            if (row == null || hnd.columnCount(row) == 0)
                return;

            Object[] args = new Object[hnd.columnCount(row)];

            for (int i = 0; i < hnd.columnCount(row); i++)
                args[i] = hnd.get(i, row);

            set.add(new GroupKey(args));
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            DistinctAccumulator<Row> other0 = (DistinctAccumulator<Row>)other;

            set.addAll(other0.set);
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            for (GroupKey key : set) {

                Row row = hnd.factory(factory, acc.argumentTypes(factory)).create();

                for (int i = 0; i < key.fieldsCount(); i++)
                    hnd.set(i, row, key.field(i));

                acc.add(row);
            }

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
    private static class SortingAccumulator<Row> implements Accumulator<Row> {
        /** */
        private final transient Comparator<Row> comp;

        /** */
        private final List<Row> list;

        /** */
        private final Accumulator<Row> acc;

        /**
         * @param accSup Acc support.
         * @param comp Comparator.
         */
        private SortingAccumulator(Supplier<Accumulator<Row>> accSup, Comparator<Row> comp) {
            this.comp = comp;

            this.list = new ArrayList<>();
            this.acc = accSup.get();
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
            list.sort(comp);

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
    private static class ListAggAccumulator<Row> implements Accumulator<Row> {
        /** Default separator. */
        private static final String DEFAULT_SEPARATOR = ",";

        /** */
        private final List<Row> list;
        
        /** */
        private final RowHandler<Row> hnd;

        /** */
        public ListAggAccumulator(RowHandler<Row> hnd) {
            this.hnd = hnd;
            this.list = new ArrayList<>();
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            if (row == null || hnd.get(0, row) == null)
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
                    if (hnd.columnCount(row) > 1 && hnd.get(1, row) != null)
                        builder.append(hnd.get(1, row));
                    else
                        builder.append(DEFAULT_SEPARATOR);

                builder.append(hnd.get(0, row));
            }

            return builder.toString();
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
    private static class CollationExtracorAccumulator<Row> implements Accumulator<Row> {
        /** */
        private final Accumulator<Row> acc;

        /** Handler. */
        private final RowHandler<Row> hnd;

        /** */
        private final int outerCollsCnt;

        /** */
        private final IgniteTypeFactory factory;

        /**
         * @param accSup Accumulator<Row>.
         * @param outerCollsCnt Outer collations count.
         */
        public CollationExtracorAccumulator(Supplier<Accumulator<Row>> accSup, int outerCollsCnt, RowHandler<Row> hnd,
            IgniteTypeFactory factory) {
            this.outerCollsCnt = outerCollsCnt;
            this.acc = accSup.get();
            this.hnd = hnd;
            this.factory = factory;
        }

        /** {@inheritDoc} */
        @Override public void add(Row row) {
            int colCnt = hnd.columnCount(row) - outerCollsCnt;

            Row originalRow = hnd.factory(factory, acc.argumentTypes(factory)).create();

            for (int i = 0; i < colCnt; i++)
                hnd.set(i, originalRow, hnd.get(i, row));

            acc.add(originalRow);
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator<Row> other) {
            CollationExtracorAccumulator<Row> other1 = (CollationExtracorAccumulator<Row>)other;
            acc.apply(other1.acc);
        }

        /** {@inheritDoc} */
        @Override public Object end() {
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
