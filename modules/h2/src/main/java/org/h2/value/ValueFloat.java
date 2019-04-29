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
 * Implementation of the REAL data type.
 */
public class ValueFloat extends Value {

    /**
     * The precision in digits.
     */
    static final int PRECISION = 7;

    /**
     * The maximum display size of a float.
     * Example: -1.12345676E-20
     */
    static final int DISPLAY_SIZE = 15;

    /**
     * Float.floatToIntBits(0f).
     */
    public static final int ZERO_BITS = 0;

    /**
     * The value 0.
     */
    public static final ValueFloat ZERO = new ValueFloat(0f);

    /**
     * The value 1.
     */
    public static final ValueFloat ONE = new ValueFloat(1f);

    private static final ValueFloat NAN = new ValueFloat(Float.NaN);

    private final float value;

    private ValueFloat(float value) {
        this.value = value;
    }

    @Override
    public Value add(Value v) {
        ValueFloat v2 = (ValueFloat) v;
        return get(value + v2.value);
    }

    @Override
    public Value subtract(Value v) {
        ValueFloat v2 = (ValueFloat) v;
        return get(value - v2.value);
    }

    @Override
    public Value negate() {
        return get(-value);
    }

    @Override
    public Value multiply(Value v) {
        ValueFloat v2 = (ValueFloat) v;
        return get(value * v2.value);
    }

    @Override
    public Value divide(Value v) {
        ValueFloat v2 = (ValueFloat) v;
        if (v2.value == 0.0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return get(value / v2.value);
    }

    @Override
    public Value modulus(Value v) {
        ValueFloat other = (ValueFloat) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return get(value % other.value);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder) {
        if (value == Float.POSITIVE_INFINITY) {
            builder.append("POWER(0, -1)");
        } else if (value == Float.NEGATIVE_INFINITY) {
            builder.append("(-POWER(0, -1))");
        } else if (Float.isNaN(value)) {
            builder.append("SQRT(-1)");
        } else {
            builder.append(value);
        }
        return builder;
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_FLOAT;
    }

    @Override
    public int getValueType() {
        return FLOAT;
    }

    @Override
    public int compareTypeSafe(Value o, CompareMode mode) {
        return Float.compare(value, ((ValueFloat) o).value);
    }

    @Override
    public int getSignum() {
        return value == 0 ? 0 : (value < 0 ? -1 : 1);
    }

    @Override
    public float getFloat() {
        return value;
    }

    @Override
    public double getDouble() {
        return value;
    }

    @Override
    public String getString() {
        return Float.toString(value);
    }

    @Override
    public int hashCode() {
        /*
         * NaNs are normalized in get() method, so it's safe to use
         * floatToRawIntBits() instead of floatToIntBits() here.
         */
        return Float.floatToRawIntBits(value);
    }

    @Override
    public Object getObject() {
        return value;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setFloat(parameterIndex, value);
    }

    /**
     * Get or create float value for the given float.
     *
     * @param d the float
     * @return the value
     */
    public static ValueFloat get(float d) {
        if (d == 1.0F) {
            return ONE;
        } else if (d == 0.0F) {
            // -0.0 == 0.0, and we want to return 0.0 for both
            return ZERO;
        } else if (Float.isNaN(d)) {
            return NAN;
        }
        return (ValueFloat) Value.cache(new ValueFloat(d));
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ValueFloat)) {
            return false;
        }
        return compareTypeSafe((ValueFloat) other, null) == 0;
    }

}
