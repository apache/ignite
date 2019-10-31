/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.h2.message.DbException;
import org.h2.tools.SimpleResultSet;
import org.h2.util.StatementBuilder;

/**
 * Implementation of the RESULT_SET data type.
 */
public class ValueResultSet extends Value {

    private final ResultSet result;

    private ValueResultSet(ResultSet rs) {
        this.result = rs;
    }

    /**
     * Create a result set value for the given result set.
     * The result set will be wrapped.
     *
     * @param rs the result set
     * @return the value
     */
    public static ValueResultSet get(ResultSet rs) {
        return new ValueResultSet(rs);
    }

    /**
     * Create a result set value for the given result set. The result set will
     * be fully read in memory. The original result set is not closed.
     *
     * @param rs the result set
     * @param maxrows the maximum number of rows to read (0 to just read the
     *            meta data)
     * @return the value
     */
    public static ValueResultSet getCopy(ResultSet rs, int maxrows) {
        try {
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            SimpleResultSet simple = new SimpleResultSet();
            simple.setAutoClose(false);
            ValueResultSet val = new ValueResultSet(simple);
            for (int i = 0; i < columnCount; i++) {
                String name = meta.getColumnLabel(i + 1);
                int sqlType = meta.getColumnType(i + 1);
                int precision = meta.getPrecision(i + 1);
                int scale = meta.getScale(i + 1);
                simple.addColumn(name, sqlType, precision, scale);
            }
            for (int i = 0; i < maxrows && rs.next(); i++) {
                Object[] list = new Object[columnCount];
                for (int j = 0; j < columnCount; j++) {
                    list[j] = rs.getObject(j + 1);
                }
                simple.addRow(list);
            }
            return val;
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public int getType() {
        return Value.RESULT_SET;
    }

    @Override
    public long getPrecision() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getDisplaySize() {
        // it doesn't make sense to calculate it
        return Integer.MAX_VALUE;
    }

    @Override
    public String getString() {
        try {
            StatementBuilder buff = new StatementBuilder("(");
            result.beforeFirst();
            ResultSetMetaData meta = result.getMetaData();
            int columnCount = meta.getColumnCount();
            for (int i = 0; result.next(); i++) {
                if (i > 0) {
                    buff.append(", ");
                }
                buff.append('(');
                buff.resetCount();
                for (int j = 0; j < columnCount; j++) {
                    buff.appendExceptFirst(", ");
                    int t = DataType.getValueTypeFromResultSet(meta, j + 1);
                    Value v = DataType.readValue(null, result, j + 1, t);
                    buff.append(v.getString());
                }
                buff.append(')');
            }
            result.beforeFirst();
            return buff.append(')').toString();
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    protected int compareSecure(Value v, CompareMode mode) {
        return this == v ? 0 : super.toString().compareTo(v.toString());
    }

    @Override
    public boolean equals(Object other) {
        return other == this;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public Object getObject() {
        return result;
    }

    @Override
    public ResultSet getResultSet() {
        return result;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) {
        throw throwUnsupportedExceptionForType("PreparedStatement.set");
    }

    @Override
    public String getSQL() {
        return "";
    }

    @Override
    public Value convertPrecision(long precision, boolean force) {
        if (!force) {
            return this;
        }
        SimpleResultSet rs = new SimpleResultSet();
        rs.setAutoClose(false);
        return ValueResultSet.get(rs);
    }

}
