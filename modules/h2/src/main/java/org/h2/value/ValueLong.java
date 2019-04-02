/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;

/**
 * Implementation of the BIGINT data type.
 */
public class ValueLong extends Value {

    /**
     * The smallest {@code ValueLong} value.
     */
    public static final ValueLong MIN = get(Long.MIN_VALUE);

    /**
     * The largest {@code ValueLong} value.
     */
    public static final ValueLong MAX = get(Long.MAX_VALUE);

    /**
     * The largest Long value, as a BigInteger.
     */
    public static final BigInteger MAX_BI = BigInteger.valueOf(Long.MAX_VALUE);

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 19;

    /**
     * The maximum display size of a long.
     * Example: 9223372036854775808
     */
    public static final int DISPLAY_SIZE = 20;

    private static final int STATIC_SIZE = 100;
    private static final ValueLong[] STATIC_CACHE;

    private final long value;

    static {
        STATIC_CACHE = new ValueLong[STATIC_SIZE];
        for (int i = 0; i < STATIC_SIZE; i++) {
            STATIC_CACHE[i] = new ValueLong(i);
        }
    }

    private ValueLong(long value) {
        this.value = value;
    }

    @Override
    public Value add(Value v) {
        long x = value;
        long y = ((ValueLong) v).value;
        long result = x + y;
        /*
         * If signs of both summands are different from the sign of the sum there is an
         * overflow.
         */
        if (((x ^ result) & (y ^ result)) < 0) {
            throw getOverflow();
        }
        return ValueLong.get(result);
    }

    @Override
    public int getSignum() {
        return Long.signum(value);
    }

    @Override
    public Value negate() {
        if (value == Long.MIN_VALUE) {
            throw getOverflow();
        }
        return ValueLong.get(-value);
    }

    private DbException getOverflow() {
        return DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1,
                Long.toString(value));
    }

    @Override
    public Value subtract(Value v) {
        long x = value;
        long y = ((ValueLong) v).value;
        long result = x - y;
        /*
         * If minuend and subtrahend have different signs and minuend and difference
         * have different signs there is an overflow.
         */
        if (((x ^ y) & (x ^ result)) < 0) {
            throw getOverflow();
        }
        return ValueLong.get(result);
    }

    @Override
    public Value multiply(Value v) {
        long x = value;
        long y = ((ValueLong) v).value;
        long result = x * y;
        // Check whether numbers are large enough to overflow and second value != 0
        if ((Math.abs(x) | Math.abs(y)) >>> 31 != 0 && y != 0
                // Check with division
                && (result / y != x
                // Also check the special condition that is not handled above
                || x == Long.MIN_VALUE && y == -1)) {
            throw getOverflow();
        }
        return ValueLong.get(result);
    }

    @Override
    public Value divide(Value v) {
        long y = ((ValueLong) v).value;
        if (y == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        long x = value;
        if (x == Long.MIN_VALUE && y == -1) {
            throw getOverflow();
        }
        return ValueLong.get(x / y);
    }

    @Override
    public Value modulus(Value v) {
        ValueLong other = (ValueLong) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return ValueLong.get(this.value % other.value);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder) {
        return builder.append(value);
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_LONG;
    }

    @Override
    public int getValueType() {
        return LONG;
    }

    @Override
    public long getLong() {
        return value;
    }

    @Override
    public int compareTypeSafe(Value o, CompareMode mode) {
        return Long.compare(value, ((ValueLong) o).value);
    }

    @Override
    public String getString() {
        return Long.toString(value);
    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >> 32));
    }

    @Override
    public Object getObject() {
        return value;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setLong(parameterIndex, value);
    }

    /**
     * Get or create a long value for the given long.
     *
     * @param i the long
     * @return the value
     */
    public static ValueLong get(long i) {
        if (i >= 0 && i < STATIC_SIZE) {
            return STATIC_CACHE[(int) i];
        }
        return (ValueLong) Value.cache(new ValueLong(i));
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueLong && value == ((ValueLong) other).value;
    }

}
