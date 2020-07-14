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
import java.util.List;
import java.util.function.Supplier;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

/**
 *
 */
public class Accumulators {
    /** */
    public static Supplier<Accumulator> accumulatorFactory(AggregateCall call) {
        switch (call.getAggregation().getName()) {
            case "COUNT":
                return LongCount.FACTORY;
            case "AVG":
                return avgFactory(call);
            case "SUM":
                return sumFactory(call);
            case "MIN":
                return minFactory(call);
            case "MAX":
                return maxFactory(call);
            default:
                throw new AssertionError();
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
            case BIGINT:
            default:
                return LongMinMax.MAX_FACTORY;
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
            BigDecimal in = (BigDecimal) args[0];

            if (in == null)
                return;

            sum = sum.add(in);
            cnt = cnt.add(BigDecimal.ONE);
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            DecimalAvg other0 = (DecimalAvg) other;

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
    public static class DoubleAvg implements Accumulator {
        /** */
        public static final Supplier<Accumulator> FACTORY = DoubleAvg::new;

        /** */
        private double sum;

        /** */
        private long cnt;

        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            Double in = (Double) args[0];

            if (in == null)
                return;

            sum += in;
            cnt++;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            DoubleAvg other0 = (DoubleAvg) other;

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
    private static class LongCount implements Accumulator {
        /** */
        public static final Supplier<Accumulator> FACTORY = LongCount::new;

        /** */
        private long cnt;

        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            cnt++;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            LongCount other0 = (LongCount) other;
            cnt += other0.cnt;
        }

        /** {@inheritDoc} */
        @Override public Object end() {
            return cnt;
        }

        /** {@inheritDoc} */
        @Override public List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory) {
            return F.asList();
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
            Double in = (Double) args[0];

            if (in == null)
                return;

            empty = false;
            sum += in;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            DoubleSum other0 = (DoubleSum) other;

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
    private static class IntSum implements Accumulator {
        /** */
        public static final Supplier<Accumulator> FACTORY = IntSum::new;

        /** */
        private int sum;

        /** */
        private boolean empty = true;

        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            Integer in = (Integer) args[0];

            if (in == null)
                return;

            empty = false;
            sum += in;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            IntSum other0 = (IntSum) other;

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
    private static class LongSum implements Accumulator {
        /** */
        public static final Supplier<Accumulator> FACTORY = LongSum::new;

        /** */
        private long sum;

        /** */
        private boolean empty = true;

        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            Long in = (Long) args[0];

            if (in == null)
                return;

            empty = false;
            sum += in;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            LongSum other0 = (LongSum) other;

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
    private static class DecimalSum implements Accumulator {
        /** */
        public static final Supplier<Accumulator> FACTORY = DecimalSum::new;

        /** */
        private BigDecimal sum;

        /** {@inheritDoc} */
        @Override public void add(Object... args) {
            BigDecimal in = (BigDecimal) args[0];

            if (in == null)
                return;

            sum = sum == null ? in : sum.add(in);
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            DecimalSum other0 = (DecimalSum) other;

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
            Double in = (Double) args[0];

            if (in == null)
                return;

            val = empty ? in : min ? Math.min(val, in) : Math.max(val, in);
            empty = false;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            DoubleMinMax other0 = (DoubleMinMax) other;

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
            Integer in = (Integer) args[0];

            if (in == null)
                return;

            val = empty ? in : min ? Math.min(val, in) : Math.max(val, in);
            empty = false;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            IntMinMax other0 = (IntMinMax) other;

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
            Long in = (Long) args[0];

            if (in == null)
                return;

            val = empty ? in : min ? Math.min(val, in) : Math.max(val, in);
            empty = false;
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            LongMinMax other0 = (LongMinMax) other;

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
            BigDecimal in = (BigDecimal) args[0];

            if (in == null)
                return;

            val = val == null ? in : min ? val.min(in) : val.max(in);
        }

        /** {@inheritDoc} */
        @Override public void apply(Accumulator other) {
            DecimalMinMax other0 = (DecimalMinMax) other;

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
}
