/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import org.h2.engine.SessionInterface;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.SimpleResult;

/**
 * Implementation of the RESULT_SET data type.
 */
public class ValueResultSet extends Value {

    private final SimpleResult result;

    private ValueResultSet(SimpleResult result) {
        this.result = result;
    }

    /**
     * Create a result set value.
     *
     * @param result the result
     * @return the value
     */
    public static ValueResultSet get(SimpleResult result) {
        return new ValueResultSet(result);
    }

    /**
     * Create a result set value for the given result set. The result set will
     * be fully read in memory. The original result set is not closed.
     *
     * @param session the session
     * @param rs the result set
     * @param maxrows the maximum number of rows to read (0 to just read the
     *            meta data)
     * @return the value
     */
    public static ValueResultSet get(SessionInterface session, ResultSet rs, int maxrows) {
        try {
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            SimpleResult simple = new SimpleResult();
            for (int i = 0; i < columnCount; i++) {
                String alias = meta.getColumnLabel(i + 1);
                String name = meta.getColumnName(i + 1);
                int columnType = DataType.convertSQLTypeToValueType(meta.getColumnType(i + 1),
                        meta.getColumnTypeName(i + 1));
                int precision = meta.getPrecision(i + 1);
                int scale = meta.getScale(i + 1);
                simple.addColumn(alias, name, columnType, precision, scale);
            }
            for (int i = 0; i < maxrows && rs.next(); i++) {
                Value[] list = new Value[columnCount];
                for (int j = 0; j < columnCount; j++) {
                    list[j] = DataType.convertToValue(session, rs.getObject(j + 1),
                            simple.getColumnType(j).getValueType());
                }
                simple.addRow(list);
            }
            return new ValueResultSet(simple);
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    /**
     * Create a result set value for the given result. The result will be fully
     * read in memory. The original result is not closed.
     *
     * @param result result
     * @param maxrows the maximum number of rows to read (0 to just read the
     *            meta data)
     * @return the value
     */
    public static ValueResultSet get(ResultInterface result, int maxrows) {
        int columnCount = result.getVisibleColumnCount();
        SimpleResult simple = new SimpleResult();
        for (int i = 0; i < columnCount; i++) {
            simple.addColumn(result.getAlias(i), result.getColumnName(i), result.getColumnType(i));
        }
        result.reset();
        for (int i = 0; i < maxrows && result.next(); i++) {
            simple.addRow(Arrays.copyOf(result.currentRow(), columnCount));
        }
        return new ValueResultSet(simple);
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_RESULT_SET;
    }

    @Override
    public int getValueType() {
        return RESULT_SET;
    }

    @Override
    public int getMemory() {
        return result.getRowCount() * result.getVisibleColumnCount() * 32 + 400;
    }

    @Override
    public String getString() {
        StringBuilder buff = new StringBuilder("(");
        ResultInterface result = this.result.createShallowCopy(null);
        int columnCount = result.getVisibleColumnCount();
        for (int i = 0; result.next(); i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append('(');
            Value[] row = result.currentRow();
            for (int j = 0; j < columnCount; j++) {
                if (j > 0) {
                    buff.append(", ");
                }
                buff.append(row[j].getString());
            }
            buff.append(')');
        }
        return buff.append(')').toString();
    }

    @Override
    public int compareTypeSafe(Value v, CompareMode mode) {
        return this == v ? 0 : getString().compareTo(v.getString());
    }

    @Override
    public boolean equals(Object other) {
        return other == this;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    @Override
    public Object getObject() {
        return getString();
    }

    @Override
    public ResultInterface getResult() {
        return result.createShallowCopy(null);
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) {
        throw getUnsupportedExceptionForOperation("PreparedStatement.set");
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder) {
        return builder;
    }

    @Override
    public Value convertPrecision(long precision, boolean force) {
        if (!force) {
            return this;
        }
        return ValueResultSet.get(new SimpleResult());
    }

}
