/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.exec.exp;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Ignite SQL functions.
 */
public class IgniteSqlFunctions {
    /** */
    public static final String NUMERIC_OVERFLOW_ERROR = "Numeric field overflow.";

    /** */
    private static final int DFLT_NUM_PRECISION = IgniteTypeSystem.INSTANCE.getDefaultPrecision(SqlTypeName.DECIMAL);

    /** */
    private static final RoundingMode NUMERIC_ROUNDING_MODE = RoundingMode.HALF_UP;

    /**
     * Default constructor.
     */
    private IgniteSqlFunctions() {
        // No-op.
    }

    /** SQL SYSTEM_RANGE(start, end) table function. */
    public static ScannableTable systemRange(Object rangeStart, Object rangeEnd) {
        return new RangeTable(rangeStart, rangeEnd, 1L);
    }

    /** SQL SYSTEM_RANGE(start, end, increment) table function. */
    public static ScannableTable systemRange(Object rangeStart, Object rangeEnd, Object increment) {
        return new RangeTable(rangeStart, rangeEnd, increment);
    }

    /** CAST(DECIMAL AS VARCHAR). */
    public static String toString(BigDecimal x) {
        return x == null ? null : x.toPlainString();
    }

    /** CAST(DOUBLE AS DECIMAL). */
    public static BigDecimal toBigDecimal(double val, int precision, int scale) {
        return removeDefaultScale(precision, scale, toBigDecimal(BigDecimal.valueOf(val), precision, scale));
    }

    /** CAST(FLOAT AS DECIMAL). */
    public static BigDecimal toBigDecimal(float val, int precision, int scale) {
        return removeDefaultScale(precision, scale, toBigDecimal(BigDecimal.valueOf(val), precision, scale));
    }

    /** Removes redundant scale in case of default DECIMAL (without passed precision and scale). */
    private static BigDecimal removeDefaultScale(int precision, int scale, BigDecimal val) {
        BigDecimal unscaled;

        if (precision == DFLT_NUM_PRECISION && scale == 0 && val.compareTo(unscaled = val.setScale(0, NUMERIC_ROUNDING_MODE)) == 0)
            return unscaled;

        return val;
    }

    /** CAST(java long AS DECIMAL). */
    public static BigDecimal toBigDecimal(long val, int precision, int scale) {
        return toBigDecimal(BigDecimal.valueOf(val), precision, scale);
    }

    /** CAST(INT AS DECIMAL). */
    public static BigDecimal toBigDecimal(int val, int precision, int scale) {
        return toBigDecimal(BigDecimal.valueOf(val), precision, scale);
    }

    /** CAST(java short AS DECIMAL). */
    public static BigDecimal toBigDecimal(short val, int precision, int scale) {
        return toBigDecimal(BigDecimal.valueOf(val), precision, scale);
    }

    /** CAST(java byte AS DECIMAL). */
    public static BigDecimal toBigDecimal(byte val, int precision, int scale) {
        return toBigDecimal(BigDecimal.valueOf(val), precision, scale);
    }

    /** CAST(BOOL AS DECIMAL). */
    public static BigDecimal toBigDecimal(boolean val, int precision, int scale) {
        throw new UnsupportedOperationException();
    }

    /** CAST(VARCHAR AS DECIMAL). */
    public static BigDecimal toBigDecimal(String s, int precision, int scale) {
        if (s == null)
            return null;

        return toBigDecimal(new BigDecimal(s.trim()), precision, scale);
    }

    /** Converts {@code val} to a {@link BigDecimal} with the given {@code precision} and {@code scale}. */
    public static BigDecimal toBigDecimal(Number val, int precision, int scale) {
        assert precision > 0 : "Invalid precision: " + precision;
        assert scale >= 0 : "Invalid scale: " + scale;

        if (val == null)
            return null;

        if (precision == DFLT_NUM_PRECISION)
            return convertToBigDecimal(val);

        BigDecimal dec = convertToBigDecimal(val);

        if (scale > precision || (dec.precision() - dec.scale() > precision - scale && !dec.unscaledValue().equals(BigInteger.ZERO)))
            throw new IllegalArgumentException(NUMERIC_OVERFLOW_ERROR);

        return dec.setScale(scale, NUMERIC_ROUNDING_MODE);
    }

    /** */
    private static BigDecimal convertToBigDecimal(Number value) {
        BigDecimal dec;

        if (value instanceof Float)
            dec = BigDecimal.valueOf(value.floatValue());
        else if (value instanceof Double)
            dec = BigDecimal.valueOf(value.doubleValue());
        else if (value instanceof BigDecimal)
            dec = (BigDecimal)value;
        else if (value instanceof BigInteger)
            dec = new BigDecimal((BigInteger)value);
        else
            dec = new BigDecimal(value.longValue());

        return dec;
    }

    /** Cast object depending on type to DECIMAL. */
    public static BigDecimal toBigDecimal(Object o, int precision, int scale) {
        if (o == null)
            return null;

        if (o instanceof Boolean)
            throw new UnsupportedOperationException();

        return o instanceof Number ? toBigDecimal((Number)o, precision, scale)
            : toBigDecimal(o.toString(), precision, scale);
    }

    /** CAST(VARCHAR AS VARBINARY). */
    public static ByteString toByteString(String s) {
        return s == null ? null : new ByteString(s.getBytes(Commons.typeFactory().getDefaultCharset()));
    }

    /** CAST(VARBINARY AS VARCHAR). */
    public static String toString(ByteString b) {
        return b == null ? null : new String(b.getBytes(), Commons.typeFactory().getDefaultCharset());
    }

    /** LEAST2. */
    public static Object least2(Object arg0, Object arg1) {
        return leastOrGreatest(true, arg0, arg1);
    }

    /** @return The second argument and ignores the first. */
    public static Object skipFirstArgument(Object v1, Object v2) {
        return v2;
    }

    /** */
    public static Number bitwise(SqlKind kind, Number v1, Number v2) {
        if (v1 == null)
            return v2;

        if (v2 == null)
            return v1;

        switch (kind) {
            case BIT_AND:
                return v1.longValue() & v2.longValue();
            case BIT_OR:
                return v1.longValue() | v2.longValue();
            case BIT_XOR:
                return v1.longValue() ^ v2.longValue();
            default:
                throw new IllegalArgumentException("Unexpected bitwise operation: " + kind);
        }
    }

    /** GREATEST2. */
    public static Object greatest2(Object arg0, Object arg1) {
        return leastOrGreatest(false, arg0, arg1);
    }

    /** */
    private static Object leastOrGreatest(boolean least, Object arg0, Object arg1) {
        if (arg0 == null || arg1 == null)
            return null;

        assert arg0 instanceof Comparable && arg1 instanceof Comparable :
            "Unexpected class [arg0=" + arg0.getClass().getName() + ", arg1=" + arg1.getClass().getName() + ']';

        if (((Comparable<Object>)arg0).compareTo(arg1) < 0)
            return least ? arg0 : arg1;
        else
            return least ? arg1 : arg0;
    }

    /** */
    private static class RangeTable implements ScannableTable {
        /** Start of the range. */
        private final Object rangeStart;

        /** End of the range. */
        private final Object rangeEnd;

        /** Increment. */
        private final Object increment;

        /**
         * Note: {@code Object} arguments required here due to:
         * 1. {@code NULL} arguments need to be supported, so we can't use {@code long} arguments type.
         * 2. {@code Integer} and other numeric classes can be converted to {@code long} type by java, but can't be
         *      converted to {@code Long} type, so we can't use {@code Long} arguments type either.
         * Instead, we accept {@code Object} arguments type and try to convert valid types to {@code long}.
         */
        RangeTable(Object rangeStart, Object rangeEnd, Object increment) {
            this.rangeStart = rangeStart;
            this.rangeEnd = rangeEnd;
            this.increment = increment;
        }

        /** {@inheritDoc} */
        @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder().add("X", SqlTypeName.BIGINT).build();
        }

        /** {@inheritDoc} */
        @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
            if (rangeStart == null || rangeEnd == null || increment == null)
                return Linq4j.emptyEnumerable();

            long rangeStart = convertToLongArg(this.rangeStart, "rangeStart");
            long rangeEnd = convertToLongArg(this.rangeEnd, "rangeEnd");
            long increment = convertToLongArg(this.increment, "increment");

            if (increment == 0L)
                throw new IllegalArgumentException("Increment can't be 0");

            return new AbstractEnumerable<@Nullable Object[]>() {
                @Override public Enumerator<@Nullable Object[]> enumerator() {
                    return new Enumerator<Object[]>() {
                        long cur = rangeStart - increment;

                        @Override public Object[] current() {
                            return new Object[] { cur };
                        }

                        @Override public boolean moveNext() {
                            cur += increment;

                            return increment > 0L ? cur <= rangeEnd : cur >= rangeEnd;
                        }

                        @Override public void reset() {
                            cur = rangeStart - increment;
                        }

                        @Override public void close() {
                            // No-op.
                        }
                    };
                }
            };
        }

        /** */
        private long convertToLongArg(Object val, String name) {
            if (val instanceof Byte || val instanceof Short || val instanceof Integer || val instanceof Long)
                return ((Number)val).longValue();

            throw new IllegalArgumentException("Unsupported argument type [arg=" + name +
                ", type=" + val.getClass().getSimpleName() + ']');
        }

        /** {@inheritDoc} */
        @Override public Statistic getStatistic() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public Schema.TableType getJdbcTableType() {
            return Schema.TableType.TABLE;
        }

        /** {@inheritDoc} */
        @Override public boolean isRolledUp(String column) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
            SqlNode parent, CalciteConnectionConfig cfg) {
            return true;
        }
    }
}
