/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

import org.h2.api.ErrorCode;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;

/**
 * Row value.
 */
public class ValueRow extends ValueCollectionBase {

    /**
     * Empty row.
     */
    private static final Object EMPTY = get(new Value[0]);

    private ValueRow(Value[] list) {
        super(list);
    }

    /**
     * Get or create a row value for the given value array.
     * Do not clone the data.
     *
     * @param list the value array
     * @return the value
     */
    public static ValueRow get(Value[] list) {
        return new ValueRow(list);
    }

    /**
     * Returns empty row.
     *
     * @return empty row
     */
    public static ValueRow getEmpty() {
        return (ValueRow) EMPTY;
    }

    @Override
    public int getValueType() {
        return ROW;
    }

    @Override
    public String getString() {
        StringBuilder builder = new StringBuilder("ROW (");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(values[i].getString());
        }
        return builder.append(')').toString();
    }

    @Override
    public int compareTypeSafe(Value o, CompareMode mode) {
        ValueRow v = (ValueRow) o;
        if (values == v.values) {
            return 0;
        }
        int len = values.length;
        if (len != v.values.length) {
            throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
        }
        for (int i = 0; i < len; i++) {
            Value v1 = values[i];
            Value v2 = v.values[i];
            int comp = v1.compareTo(v2, /* TODO */ null, mode);
            if (comp != 0) {
                return comp;
            }
        }
        return 0;
    }

    @Override
    public Object getObject() {
        int len = values.length;
        Object[] list = new Object[len];
        for (int i = 0; i < len; i++) {
            final Value value = values[i];
            if (!SysProperties.OLD_RESULT_SET_GET_OBJECT) {
                final int type = value.getValueType();
                if (type == Value.BYTE || type == Value.SHORT) {
                    list[i] = value.getInt();
                    continue;
                }
            }
            list[i] = value.getObject();
        }
        return list;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        throw getUnsupportedExceptionForOperation("PreparedStatement.set");
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder) {
        builder.append("ROW (");
        int length = values.length;
        for (int i = 0; i < length; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            values[i].getSQL(builder);
        }
        return builder.append(')');
    }

    @Override
    public String getTraceSQL() {
        StringBuilder builder = new StringBuilder("ROW (");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            Value v = values[i];
            builder.append(v == null ? "null" : v.getTraceSQL());
        }
        return builder.append(')').toString();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ValueRow)) {
            return false;
        }
        ValueRow v = (ValueRow) other;
        if (values == v.values) {
            return true;
        }
        int len = values.length;
        if (len != v.values.length) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            if (!values[i].equals(v.values[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Value convertPrecision(long precision, boolean force) {
        if (!force) {
            return this;
        }
        int length = values.length;
        Value[] newValues = new Value[length];
        int i = 0;
        boolean modified = false;
        for (; i < length; i++) {
            Value old = values[i];
            Value v = old.convertPrecision(precision, true);
            if (v != old) {
                modified = true;
            }
            // empty byte arrays or strings have precision 0
            // they count as precision 1 here
            precision -= Math.max(1, v.getType().getPrecision());
            if (precision < 0) {
                break;
            }
            newValues[i] = v;
        }
        if (i < length) {
            return get(Arrays.copyOf(newValues, i));
        }
        return modified ? get(newValues) : this;
    }

}
