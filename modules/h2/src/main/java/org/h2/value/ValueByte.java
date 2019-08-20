/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;

/**
 * Implementation of the BYTE data type.
 */
public class ValueByte extends Value {

    /**
     * The precision in digits.
     */
    static final int PRECISION = 3;

    /**
     * The display size for a byte.
     * Example: -127
     */
    static final int DISPLAY_SIZE = 4;

    private final byte value;

    private ValueByte(byte value) {
        this.value = value;
    }

    @Override
    public Value add(Value v) {
        ValueByte other = (ValueByte) v;
        return checkRange(value + other.value);
    }

    private static ValueByte checkRange(int x) {
        if (x < Byte.MIN_VALUE || x > Byte.MAX_VALUE) {
            throw DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1,
                    Integer.toString(x));
        }
        return ValueByte.get((byte) x);
    }

    @Override
    public int getSignum() {
        return Integer.signum(value);
    }

    @Override
    public Value negate() {
        return checkRange(-(int) value);
    }

    @Override
    public Value subtract(Value v) {
        ValueByte other = (ValueByte) v;
        return checkRange(value - other.value);
    }

    @Override
    public Value multiply(Value v) {
        ValueByte other = (ValueByte) v;
        return checkRange(value * other.value);
    }

    @Override
    public Value divide(Value v) {
        ValueByte other = (ValueByte) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return ValueByte.get((byte) (value / other.value));
    }

    @Override
    public Value modulus(Value v) {
        ValueByte other = (ValueByte) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return ValueByte.get((byte) (value % other.value));
    }

    @Override
    public String getSQL() {
        return getString();
    }

    @Override
    public int getType() {
        return Value.BYTE;
    }

    @Override
    public byte getByte() {
        return value;
    }

    @Override
    public int getInt() {
        return value;
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        ValueByte v = (ValueByte) o;
        return Integer.compare(value, v.value);
    }

    @Override
    public String getString() {
        return String.valueOf(value);
    }

    @Override
    public long getPrecision() {
        return PRECISION;
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public Object getObject() {
        return value;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setByte(parameterIndex, value);
    }

    /**
     * Get or create byte value for the given byte.
     *
     * @param i the byte
     * @return the value
     */
    public static ValueByte get(byte i) {
        return (ValueByte) Value.cache(new ValueByte(i));
    }

    @Override
    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueByte && value == ((ValueByte) other).value;
    }

}
