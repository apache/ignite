/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Implementation of the BOOLEAN data type.
 */
public class ValueBoolean extends Value {

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 1;

    /**
     * The maximum display size of a boolean.
     * Example: FALSE
     */
    public static final int DISPLAY_SIZE = 5;

    /**
     * TRUE value.
     */
    public static final ValueBoolean TRUE = new ValueBoolean(true);

    /**
     * FALSE value.
     */
    public static final ValueBoolean FALSE = new ValueBoolean(false);

    private final boolean value;

    private ValueBoolean(boolean value) {
        this.value = value;
    }

    @Override
    public int getType() {
        return Value.BOOLEAN;
    }

    @Override
    public String getSQL() {
        return getString();
    }

    @Override
    public String getString() {
        return value ? "TRUE" : "FALSE";
    }

    @Override
    public Value negate() {
        return value ? FALSE : TRUE;
    }

    @Override
    public boolean getBoolean() {
        return value;
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        ValueBoolean v = (ValueBoolean) o;
        return Boolean.compare(value, v.value);
    }

    @Override
    public long getPrecision() {
        return PRECISION;
    }

    @Override
    public int hashCode() {
        return value ? 1 : 0;
    }

    @Override
    public Object getObject() {
        return value;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setBoolean(parameterIndex, value);
    }

    /**
     * Get the boolean value for the given boolean.
     *
     * @param b the boolean
     * @return the value
     */
    public static ValueBoolean get(boolean b) {
        return b ? TRUE : FALSE;
    }

    @Override
    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    @Override
    public boolean equals(Object other) {
        // there are only ever two instances, so the instance must match
        return this == other;
    }

}
