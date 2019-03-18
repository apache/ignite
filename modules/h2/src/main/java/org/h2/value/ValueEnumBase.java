/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Base implementation of the ENUM data type.
 *
 * Currently, this class is used primarily for
 * client-server communication.
 */
public class ValueEnumBase extends Value {
    private static final int PRECISION = 10;
    private static final int DISPLAY_SIZE = 11;

    private final String label;
    private final int ordinal;

    protected ValueEnumBase(final String label, final int ordinal) {
        this.label = label;
        this.ordinal = ordinal;
    }

    @Override
    public Value add(final Value v) {
        final Value iv = v.convertTo(Value.INT);
        return convertTo(Value.INT).add(iv);
    }

    @Override
    protected int compareSecure(final Value v, final CompareMode mode) {
        return Integer.compare(getInt(), v.getInt());
    }

    @Override
    public Value divide(final Value v) {
        final Value iv = v.convertTo(Value.INT);
        return convertTo(Value.INT).divide(iv);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ValueEnumBase &&
            getInt() == ((ValueEnumBase) other).getInt();
    }

    /**
     * Get or create an enum value with the given label and ordinal.
     *
     * @param label the label
     * @param ordinal the ordinal
     * @return the value
     */
    public static ValueEnumBase get(final String label, final int ordinal) {
        return new ValueEnumBase(label, ordinal);
    }

    @Override
    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    @Override
    public int getInt() {
        return ordinal;
    }

    @Override
    public long getLong() {
        return ordinal;
    }

    @Override
    public Object getObject() {
        return ordinal;
    }

    @Override
    public long getPrecision() {
        return PRECISION;
    }

    @Override
    public int getSignum() {
        return Integer.signum(ordinal);
    }

    @Override
    public String getSQL() {
        return getString();
    }

    @Override
    public String getString() {
        return label;
    }

    @Override
    public int getType() {
        return Value.ENUM;
    }

    @Override
    public int hashCode() {
        int results = 31;
        results += getString().hashCode();
        results += getInt();
        return results;
    }

    @Override
    public Value modulus(final Value v) {
        final Value iv = v.convertTo(Value.INT);
        return convertTo(Value.INT).modulus(iv);
    }

    @Override
    public Value multiply(final Value v) {
        final Value iv = v.convertTo(Value.INT);
        return convertTo(Value.INT).multiply(iv);
    }


    @Override
    public void set(final PreparedStatement prep, final int parameterIndex)
            throws SQLException {
            prep.setInt(parameterIndex, ordinal);
    }

    @Override
    public Value subtract(final Value v) {
        final Value iv = v.convertTo(Value.INT);
        return convertTo(Value.INT).subtract(iv);
    }
}
