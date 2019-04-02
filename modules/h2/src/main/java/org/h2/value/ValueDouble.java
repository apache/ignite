/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;

/**
 * Implementation of the DOUBLE data type.
 */
public class ValueDouble extends Value {

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 17;

    /**
     * The maximum display size of a double.
     * Example: -3.3333333333333334E-100
     */
    public static final int DISPLAY_SIZE = 24;

    /**
     * Double.doubleToLongBits(0d)
     */
    public static final long ZERO_BITS = 0L;

    /**
     * The value 0.
     */
    public static final ValueDouble ZERO = new ValueDouble(0d);

    /**
     * The value 1.
     */
    public static final ValueDouble ONE = new ValueDouble(1d);

    private static final ValueDouble NAN = new ValueDouble(Double.NaN);

    private final double value;

    private ValueDouble(double value) {
        this.value = value;
    }

    @Override
    public Value add(Value v) {
        ValueDouble v2 = (ValueDouble) v;
        return get(value + v2.value);
    }

    @Override
    public Value subtract(Value v) {
        ValueDouble v2 = (ValueDouble) v;
        return get(value - v2.value);
    }

    @Override
    public Value negate() {
        return get(-value);
    }

    @Override
    public Value multiply(Value v) {
        ValueDouble v2 = (ValueDouble) v;
        return get(value * v2.value);
    }

    @Override
    public Value divide(Value v) {
        ValueDouble v2 = (ValueDouble) v;
        if (v2.value == 0.0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return get(value / v2.value);
    }

    @Override
    public ValueDouble modulus(Value v) {
        ValueDouble other = (ValueDouble) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return get(value % other.value);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder) {
        if (value == Double.POSITIVE_INFINITY) {
            builder.append("POWER(0, -1)");
        } else if (value == Double.NEGATIVE_INFINITY) {
            builder.append("(-POWER(0, -1))");
        } else if (Double.isNaN(value)) {
            builder.append("SQRT(-1)");
        } else {
            builder.append(value);
        }
        return builder;
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_DOUBLE;
    }

    @Override
    public int getValueType() {
        return DOUBLE;
    }

    @Override
    public int compareTypeSafe(Value o, CompareMode mode) {
        return Double.compare(value, ((ValueDouble) o).value);
    }

    @Override
    public int getSignum() {
        return value == 0 ? 0 : (value < 0 ? -1 : 1);
    }

    @Override
    public double getDouble() {
        return value;
    }

    @Override
    public String getString() {
        return Double.toString(value);
    }

    @Override
    public int hashCode() {
        /*
         * NaNs are normalized in get() method, so it's safe to use
         * doubleToRawLongBits() instead of doubleToLongBits() here.
         */
        long hash = Double.doubleToRawLongBits(value);
        return (int) (hash ^ (hash >>> 32));
    }

    @Override
    public Object getObject() {
        return value;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setDouble(parameterIndex, value);
    }

    /**
     * Get or create double value for the given double.
     *
     * @param d the double
     * @return the value
     */
    public static ValueDouble get(double d) {
        if (d == 1.0) {
            return ONE;
        } else if (d == 0.0) {
            // -0.0 == 0.0, and we want to return 0.0 for both
            return ZERO;
        } else if (Double.isNaN(d)) {
            return NAN;
        }
        return (ValueDouble) Value.cache(new ValueDouble(d));
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ValueDouble)) {
            return false;
        }
        return compareTypeSafe((ValueDouble) other, null) == 0;
    }

}
