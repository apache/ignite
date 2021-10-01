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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.apache.ignite.internal.util.ArrayUtils.nullOrEmpty;

/**
 *
 */
public class Accumulators {
    /** */
    public static Supplier<Accumulator> accumulatorFactory(AggregateCall call) {
        if (!call.isDistinct())
            return accumulatorFunctionFactory(call);

        Supplier<Accumulator> fac = accumulatorFunctionFactory(call);

        return () -> new DistinctAccumulator(fac);
    }

    /** */
    public static Supplier<Accumulator> accumulatorFunctionFactory(AggregateCall call) {
        switch (call.getAggregation().getName()) {
            case "COUNT":
                return LongCount.FACTORY;
            case "AVG":
                return avgFactory(call);
            case "SUM":
                return sumFactory(call);
            case "$SUM0":
                return sumEmptyIsZeroFactory(call);
            case "MIN":
                return minFactory(call);
            case "MAX":
                return maxFactory(call);
            case "SINGLE_VALUE":
                return SingleVal.FACTORY;
            default:
                throw new AssertionError(call.getAggregation().getName());
        }
    }

    /** */
    private static Supplier<Accumulator> avgFactory(AggregateCall call) {
        switch (call.type.getSqlTypeName()) {
            case BIGINT:
            case DECIMAL:
                return DecimalAvg.FACTORY;
            case DOUBLE:
            case REAL:
            case FLOAT:
            case INTEGER:
            default:
                return DoubleAvg.FACTORY;
        }
    }

    /** */
    private static Supplier<Accumulator> sumFactory(AggregateCall call) {
        switch (call.type.getSqlTypeName()) {
            case DOUBLE:
            case REAL:
            case FLOAT:
                return DoubleSum.FACTORY;
            case DECIMAL:
                return DecimalSum.FACTORY;
            case INTEGER:
                return IntSum.FACTORY;
            case BIGINT:
            default:
                return LongSum.FACTORY;
        }
    }

    /** */
    private static Supplier<Accumulator> sumEmptyIsZeroFactory(AggregateCall call) {
        switch (call.type.getSqlTypeName()) {
            case DOUBLE:
            case REAL:
            case FLOAT:
                return DoubleSumEmptyIsZero.FACTORY;
            case DECIMAL:
                return DecimalSumEmptyIsZero.FACTORY;
            case INTEGER:
                return IntSumEmptyIsZero.FACTORY;
            case BIGINT:
            default:
                return LongSumEmptyIsZero.FACTORY;
        }
    }

    /** */
    private static Supplier<Accumulator> minFactory(AggregateCall call) {
        switch (call.type.getSqlTypeName()) {
            case DOUBLE:
            case REAL:
            case FLOAT:
                return DoubleMinMax.MIN_FACTORY;
            case DECIMAL:
                return DecimalMinMax.MIN_FACTORY;
            case INTEGER:
                return IntMinMax.MIN_FACTORY;
            case CHAR:
            case VARCHAR:
                return VarCharMinMax.MIN_FACTORY;
            case BIGINT:
            default:
                return LongMinMax.MIN_FACTORY;
        }
    }

    /** */
    private static Supplier<Accumulator> maxFactory(AggregateCall call) {
        switch (call.type.getSqlTypeName()) {
            case DOUBLE:
            case REAL:
            case FLOAT:
                return DoubleMinMax.MAX_FACTORY;
            case DECIMAL:
                return DecimalMinMax.MAX_FACTORY;
            case INTEGER:
                return IntMinMax.MAX_FACTORY;
            case CHAR:
            case VARCHAR:
                return VarCharMinMax.MAX_FACTORY;
            case BIGINT:
            default:
                return LongMinMax.MAX_FACTORY;
        }
    }

    /** */
    private static class SingleVal implements Accumulator {
        /** */
        private Object holder;

        /** */
        private boolean touched;

        /** */
        public static final Supplier<Accumulator> FACTORY = SingleVal::new;

        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            assert args.length == 1 : args.length;

            if (touched)
                throw new IllegalArgumentException("Subquery returned more than 1 value.");

            touched = true;

            holder = args[0];
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            assert other instanceof SingleVal;

            SingleVal otherSingle = (SingleVal)other;
            if (otherSingle.touched)
                add(otherSingle.holder);
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return holder;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createSqlType(ANY);
        }
    }

    /** */
    public static class DecimalAvg implements Accumulator {
        /** */
        public static final Supplier<Accumulator> FACTORY = DecimalAvg::new;
        
        /** */
        private BigDecimal sum = BigDecimal.ZERO;

        /** */
        private BigDecimal cnt = BigDecimal.ZERO;

        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            BigDecimal in = (BigDecimal)args[0];

            if (in == null)
                return;

            sum = sum.add(in);
            cnt = cnt.add(BigDecimal.ONE);
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            DecimalAvg other0 = (DecimalAvg)other;

            sum = sum.add(other0.sum);
            cnt = cnt.add(other0.cnt);
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return cnt.compareTo(BigDecimal.ZERO) == 0 ? null : sum.divide(cnt, MathContext.DECIMAL64);
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(DECIMAL), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(DECIMAL), true);
        }
    }

    /** */
    public static class DoubleAvg implements Accumulator {
        /** */
        public static final Supplier<Accumulator> FACTORY = DoubleAvg::new;

        /** */
        private double sum;

        /** */
        private long cnt;

        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            Double in = (Double)args[0];

            if (in == null)
                return;

            sum += in;
            cnt++;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            DoubleAvg other0 = (DoubleAvg)other;

            sum += other0.sum;
            cnt += other0.cnt;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return cnt > 0 ? sum / cnt : null;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(DOUBLE), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(DOUBLE), true);
        }
    }

    /** */
    private static class LongCount implements Accumulator {
        /** */
        public static final Supplier<Accumulator> FACTORY = LongCount::new;

        /** */
        private long cnt;

        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            assert nullOrEmpty(args) || args.length == 1;

            if (nullOrEmpty(args) || args[0] != null)
                cnt++;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            LongCount other0 = (LongCount)other;
            cnt += other0.cnt;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return cnt;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(ANY), false));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createSqlType(BIGINT);
        }
    }

    /** */
    private static class DoubleSum implements Accumulator {
        /** */
        public static final Supplier<Accumulator> FACTORY = DoubleSum::new;

        /** */
        private double sum;

        /** */
        private boolean empty = true;

        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            Double in = (Double)args[0];

            if (in == null)
                return;

            empty = false;
            sum += in;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            DoubleSum other0 = (DoubleSum)other;

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
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(DOUBLE), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(DOUBLE), true);
        }
    }

    /** */
    private static class IntSum implements Accumulator {
        /** */
        public static final Supplier<Accumulator> FACTORY = IntSum::new;

        /** */
        private int sum;

        /** */
        private boolean empty = true;

        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            Integer in = (Integer)args[0];

            if (in == null)
                return;

            empty = false;
            sum += in;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            IntSum other0 = (IntSum)other;

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
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(INTEGER), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(INTEGER), true);
        }
    }

    /** */
    private static class LongSum implements Accumulator {
        /** */
        public static final Supplier<Accumulator> FACTORY = LongSum::new;

        /** */
        private long sum;

        /** */
        private boolean empty = true;

        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            Long in = (Long)args[0];

            if (in == null)
                return;

            empty = false;
            sum += in;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            LongSum other0 = (LongSum)other;

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
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(BIGINT), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(BIGINT), true);
        }
    }

    /** */
    private static class DecimalSum implements Accumulator {
        /** */
        public static final Supplier<Accumulator> FACTORY = DecimalSum::new;

        /** */
        private BigDecimal sum;

        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            BigDecimal in = (BigDecimal)args[0];

            if (in == null)
                return;

            sum = sum == null ? in : sum.add(in);
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            DecimalSum other0 = (DecimalSum)other;

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
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(DECIMAL), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(DECIMAL), true);
        }
    }

    /** */
    private static class DoubleSumEmptyIsZero implements Accumulator {
        /** */
        public static final Supplier<Accumulator> FACTORY = DoubleSumEmptyIsZero::new;

        /** */
        private double sum;

        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            Double in = (Double)args[0];

            if (in == null)
                return;

            sum += in;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            DoubleSumEmptyIsZero other0 = (DoubleSumEmptyIsZero)other;

            sum += other0.sum;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return sum;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(DOUBLE), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(DOUBLE), true);
        }
    }

    /** */
    private static class IntSumEmptyIsZero implements Accumulator {
        /** */
        public static final Supplier<Accumulator> FACTORY = IntSumEmptyIsZero::new;

        /** */
        private int sum;

        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            Integer in = (Integer)args[0];

            if (in == null)
                return;

            sum += in;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            IntSumEmptyIsZero other0 = (IntSumEmptyIsZero)other;

            sum += other0.sum;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return sum;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(INTEGER), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(INTEGER), true);
        }
    }

    /** */
    private static class LongSumEmptyIsZero implements Accumulator {
        /** */
        public static final Supplier<Accumulator> FACTORY = LongSumEmptyIsZero::new;

        /** */
        private long sum;

        /** */
        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            Long in = (Long)args[0];

            if (in == null)
                return;

            sum += in;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            LongSumEmptyIsZero other0 = (LongSumEmptyIsZero)other;

            sum += other0.sum;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return sum;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(BIGINT), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(BIGINT), true);
        }
    }

    /** */
    private static class DecimalSumEmptyIsZero implements Accumulator {
        /** */
        public static final Supplier<Accumulator> FACTORY = DecimalSumEmptyIsZero::new;

        /** */
        private BigDecimal sum;

        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            BigDecimal in = (BigDecimal)args[0];

            if (in == null)
                return;

            sum = sum == null ? in : sum.add(in);
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            DecimalSumEmptyIsZero other0 = (DecimalSumEmptyIsZero)other;

            sum = sum == null ? other0.sum : sum.add(other0.sum);
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return sum != null ? sum : BigDecimal.ZERO;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(DECIMAL), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(DECIMAL), true);
        }
    }

    /** */
    private static class DoubleMinMax implements Accumulator {
        /** */
        public static final Supplier<Accumulator> MIN_FACTORY = () -> new DoubleMinMax(true);

        /** */
        public static final Supplier<Accumulator> MAX_FACTORY = () -> new DoubleMinMax(false);

        /** */
        private final boolean min;

        /** */
        private double val;

        /** */
        private boolean empty = true;

        /** */
        private DoubleMinMax(boolean min) {
            this.min = min;
        }

        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            Double in = (Double)args[0];

            if (in == null)
                return;

            val = empty ? in : min ? Math.min(val, in) : Math.max(val, in);
            empty = false;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            DoubleMinMax other0 = (DoubleMinMax)other;

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
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(DOUBLE), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(DOUBLE), true);
        }
    }

    /** */
    private static class VarCharMinMax implements Accumulator {
        /** */
        public static final Supplier<Accumulator> MIN_FACTORY = () -> new VarCharMinMax(true);

        /** */
        public static final Supplier<Accumulator> MAX_FACTORY = () -> new VarCharMinMax(false);

        /** */
        private final boolean min;

        /** */
        private CharSequence val;

        /** */
        private boolean empty = true;

        /** */
        private VarCharMinMax(boolean min) {
            this.min = min;
        }

        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            CharSequence in = (CharSequence)args[0];

            if (in == null)
                return;

            val = empty ? in : min ?
                (CharSeqComparator.INSTANCE.compare(val, in) < 0 ? val : in) :
                (CharSeqComparator.INSTANCE.compare(val, in) < 0 ? in : val);

            empty = false;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            VarCharMinMax other0 = (VarCharMinMax)other;

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
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(VARCHAR), true));
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
    private static class IntMinMax implements Accumulator {
        /** */
        public static final Supplier<Accumulator> MIN_FACTORY = () -> new IntMinMax(true);

        /** */
        public static final Supplier<Accumulator> MAX_FACTORY = () -> new IntMinMax(false);

        /** */
        private final boolean min;

        /** */
        private int val;

        /** */
        private boolean empty = true;

        /** */
        private IntMinMax(boolean min) {
            this.min = min;
        }

        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            Integer in = (Integer)args[0];

            if (in == null)
                return;

            val = empty ? in : min ? Math.min(val, in) : Math.max(val, in);
            empty = false;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            IntMinMax other0 = (IntMinMax)other;

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
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(INTEGER), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(INTEGER), true);
        }
    }

    /** */
    private static class LongMinMax implements Accumulator {
        /** */
        public static final Supplier<Accumulator> MIN_FACTORY = () -> new LongMinMax(true);

        /** */
        public static final Supplier<Accumulator> MAX_FACTORY = () -> new LongMinMax(false);

        /** */
        private final boolean min;

        /** */
        private long val;

        /** */
        private boolean empty = true;

        /** */
        private LongMinMax(boolean min) {
            this.min = min;
        }

        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            Long in = (Long)args[0];

            if (in == null)
                return;

            val = empty ? in : min ? Math.min(val, in) : Math.max(val, in);
            empty = false;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            LongMinMax other0 = (LongMinMax)other;

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
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(BIGINT), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(BIGINT), true);
        }
    }

    /** */
    private static class DecimalMinMax implements Accumulator {
        /** */
        public static final Supplier<Accumulator> MIN_FACTORY = () -> new DecimalMinMax(true);

        /** */
        public static final Supplier<Accumulator> MAX_FACTORY = () -> new DecimalMinMax(false);

        /** */
        private final boolean min;

        /** */
        private BigDecimal val;

        /** */
        private DecimalMinMax(boolean min) {
            this.min = min;
        }

        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            BigDecimal in = (BigDecimal)args[0];

            if (in == null)
                return;

            val = val == null ? in : min ? val.min(in) : val.max(in);
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            DecimalMinMax other0 = (DecimalMinMax)other;

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
            return List.of(typeFactory.createTypeWithNullability(typeFactory.createSqlType(DECIMAL), true));
        }

        /** {@inheritDoc} */
        @Override public RelDataType returnType(IgniteTypeFactory typeFactory) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(DECIMAL), true);
        }
    }

    /** */
    private static class DistinctAccumulator implements Accumulator {
        /** */
        private final Accumulator acc;

        /** */
        private final Set<Object> set = new HashSet<>();

        /** */
        private DistinctAccumulator(Supplier<Accumulator> accSup) {
            this.acc = accSup.get();
        }

        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            Object in = args[0];

            if (in == null)
                return;

            set.add(in);
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            DistinctAccumulator other0 = (DistinctAccumulator)other;

            set.addAll(other0.set);
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            for (Object o : set)
                acc.add(o);

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
