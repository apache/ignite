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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Ignite SQL functions.
 */
public class IgniteSqlFunctions {
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

    /** */
    private static BigDecimal setScale(int precision, int scale, BigDecimal decimal) {
        return precision == IgniteTypeSystem.INSTANCE.getDefaultPrecision(SqlTypeName.DECIMAL)
            ? decimal : decimal.setScale(scale, RoundingMode.HALF_UP);
    }

    /** CAST(DOUBLE AS DECIMAL). */
    public static BigDecimal toBigDecimal(double val, int precision, int scale) {
        BigDecimal decimal = BigDecimal.valueOf(val);
        return setScale(precision, scale, decimal);
    }

    /** CAST(FLOAT AS DECIMAL). */
    public static BigDecimal toBigDecimal(float val, int precision, int scale) {
        BigDecimal decimal = new BigDecimal(String.valueOf(val));
        return setScale(precision, scale, decimal);
    }

    /** CAST(java long AS DECIMAL). */
    public static BigDecimal toBigDecimal(long val, int precision, int scale) {
        BigDecimal decimal = BigDecimal.valueOf(val);
        return setScale(precision, scale, decimal);
    }

    /** CAST(INT AS DECIMAL). */
    public static BigDecimal toBigDecimal(int val, int precision, int scale) {
        BigDecimal decimal = new BigDecimal(val);
        return setScale(precision, scale, decimal);
    }

    /** CAST(java short AS DECIMAL). */
    public static BigDecimal toBigDecimal(short val, int precision, int scale) {
        BigDecimal decimal = new BigDecimal(String.valueOf(val));
        return setScale(precision, scale, decimal);
    }

    /** CAST(java byte AS DECIMAL). */
    public static BigDecimal toBigDecimal(byte val, int precision, int scale) {
        BigDecimal decimal = new BigDecimal(String.valueOf(val));
        return setScale(precision, scale, decimal);
    }

    /** CAST(BOOL AS DECIMAL). */
    public static BigDecimal toBigDecimal(boolean val, int precision, int scale) {
        throw new UnsupportedOperationException();
    }

    /** CAST(VARCHAR AS DECIMAL). */
    public static BigDecimal toBigDecimal(String s, int precision, int scale) {
        if (s == null)
            return null;
        BigDecimal decimal = new BigDecimal(s.trim());
        return setScale(precision, scale, decimal);
    }

    /** CAST(REAL AS DECIMAL). */
    public static BigDecimal toBigDecimal(Number num, int precision, int scale) {
        if (num == null)
            return null;
        // There are some values of "long" that cannot be represented as "double".
        // Not so "int". If it isn't a long, go straight to double.
        BigDecimal decimal = num instanceof BigDecimal ? ((BigDecimal)num)
            : num instanceof BigInteger ? new BigDecimal((BigInteger)num)
            : num instanceof Long ? new BigDecimal(num.longValue())
            : BigDecimal.valueOf(num.doubleValue());
        return setScale(precision, scale, decimal);
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

    /** GREATEST2. */
    public static Object greatest2(Object arg0, Object arg1) {
        return leastOrGreatest(false, arg0, arg1);
    }

    /** */
    private static Object leastOrGreatest(boolean least, Object arg0, Object arg1) {
        if (arg0 == null || arg1 == null)
            return null;

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
