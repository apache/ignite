/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.h2.api.ErrorCode;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.DbException;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * This class is used for updatable result sets. An updatable row provides
 * functions to update the current row in a result set.
 */
public class UpdatableRow {

    private final JdbcConnection conn;
    private final ResultInterface result;
    private final int columnCount;
    private String schemaName;
    private String tableName;
    private ArrayList<String> key;
    private boolean isUpdatable;

    /**
     * Construct a new object that is linked to the result set. The constructor
     * reads the database meta data to find out if the result set is updatable.
     *
     * @param conn the database connection
     * @param result the result
     */
    public UpdatableRow(JdbcConnection conn, ResultInterface result)
            throws SQLException {
        this.conn = conn;
        this.result = result;
        columnCount = result.getVisibleColumnCount();
        for (int i = 0; i < columnCount; i++) {
            String t = result.getTableName(i);
            String s = result.getSchemaName(i);
            if (t == null || s == null) {
                return;
            }
            if (tableName == null) {
                tableName = t;
            } else if (!tableName.equals(t)) {
                return;
            }
            if (schemaName == null) {
                schemaName = s;
            } else if (!schemaName.equals(s)) {
                return;
            }
        }
        final DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs = meta.getTables(null,
                StringUtils.escapeMetaDataPattern(schemaName),
                StringUtils.escapeMetaDataPattern(tableName),
                new String[] { "TABLE" });
        if (!rs.next()) {
            return;
        }
        if (rs.getString("SQL") == null) {
            // system table
            return;
        }
        String table = rs.getString("TABLE_NAME");
        // if the table name in the database meta data is lower case,
        // but the table in the result set meta data is not, then the column
        // in the database meta data is also lower case
        boolean toUpper = !table.equals(tableName) && table.equalsIgnoreCase(tableName);
        key = Utils.newSmallArrayList();
        rs = meta.getPrimaryKeys(null,
                StringUtils.escapeMetaDataPattern(schemaName),
                tableName);
        while (rs.next()) {
            String c = rs.getString("COLUMN_NAME");
            key.add(toUpper ? StringUtils.toUpperEnglish(c) : c);
        }
        if (isIndexUsable(key)) {
            isUpdatable = true;
            return;
        }
        key.clear();
        rs = meta.getIndexInfo(null,
                StringUtils.escapeMetaDataPattern(schemaName),
                tableName, true, true);
        while (rs.next()) {
            int pos = rs.getShort("ORDINAL_POSITION");
            if (pos == 1) {
                // check the last key if there was any
                if (isIndexUsable(key)) {
                    isUpdatable = true;
                    return;
                }
                key.clear();
            }
            String c = rs.getString("COLUMN_NAME");
            key.add(toUpper ? StringUtils.toUpperEnglish(c) : c);
        }
        if (isIndexUsable(key)) {
            isUpdatable = true;
            return;
        }
        key = null;
    }

    private boolean isIndexUsable(ArrayList<String> indexColumns) {
        if (indexColumns.isEmpty()) {
            return false;
        }
        for (String c : indexColumns) {
            if (findColumnIndex(c) < 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check if this result set is updatable.
     *
     * @return true if it is
     */
    public boolean isUpdatable() {
        return isUpdatable;
    }

    private int findColumnIndex(String columnName) {
        for (int i = 0; i < columnCount; i++) {
            String col = result.getColumnName(i);
            if (col.equals(columnName)) {
                return i;
            }
        }
        return -1;
    }

    private int getColumnIndex(String columnName) {
        int index = findColumnIndex(columnName);
        if (index < 0) {
            throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, columnName);
        }
        return index;
    }

    private void appendColumnList(StringBuilder builder, boolean set) {
        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                builder.append(',');
            }
            String col = result.getColumnName(i);
            StringUtils.quoteIdentifier(builder, col);
            if (set) {
                builder.append("=? ");
            }
        }
    }

    private void appendKeyCondition(StringBuilder builder) {
        builder.append(" WHERE ");
        for (int i = 0; i < key.size(); i++) {
            if (i > 0) {
                builder.append(" AND ");
            }
            StringUtils.quoteIdentifier(builder, key.get(i)).append("=?");
        }
    }

    private void setKey(PreparedStatement prep, int start, Value[] current)
            throws SQLException {
        for (int i = 0, size = key.size(); i < size; i++) {
            String col = key.get(i);
            int idx = getColumnIndex(col);
            Value v = current[idx];
            if (v == null || v == ValueNull.INSTANCE) {
                // rows with a unique key containing NULL are not supported,
                // as multiple such rows could exist
                throw DbException.get(ErrorCode.NO_DATA_AVAILABLE);
            }
            v.set(prep, start + i);
        }
    }

//    public boolean isRowDeleted(Value[] row) throws SQLException {
//        StringBuilder buff = new StringBuilder();
//        buff.append("SELECT COUNT(*) FROM ").
//               append(StringUtils.quoteIdentifier(tableName));
//        appendKeyCondition(buff);
//        PreparedStatement prep = conn.prepareStatement(buff.toString());
//        setKey(prep, 1, row);
//        ResultSet rs = prep.executeQuery();
//        rs.next();
//        return rs.getInt(1) == 0;
//    }

    private void appendTableName(StringBuilder builder) {
        if (schemaName != null && schemaName.length() > 0) {
            StringUtils.quoteIdentifier(builder, schemaName).append('.');
        }
        StringUtils.quoteIdentifier(builder, tableName);
    }

    /**
     * Re-reads a row from the database and updates the values in the array.
     *
     * @param row the values that contain the key
     * @return the row
     */
    public Value[] readRow(Value[] row) throws SQLException {
        StringBuilder builder = new StringBuilder("SELECT ");
        appendColumnList(builder, false);
        builder.append(" FROM ");
        appendTableName(builder);
        appendKeyCondition(builder);
        PreparedStatement prep = conn.prepareStatement(builder.toString());
        setKey(prep, 1, row);
        ResultSet rs = prep.executeQuery();
        if (!rs.next()) {
            throw DbException.get(ErrorCode.NO_DATA_AVAILABLE);
        }
        Value[] newRow = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            int type = result.getColumnType(i).getValueType();
            newRow[i] = DataType.readValue(conn.getSession(), rs, i + 1, type);
        }
        return newRow;
    }

    /**
     * Delete the given row in the database.
     *
     * @param current the row
     * @throws SQLException if this row has already been deleted
     */
    public void deleteRow(Value[] current) throws SQLException {
        StringBuilder builder = new StringBuilder("DELETE FROM ");
        appendTableName(builder);
        appendKeyCondition(builder);
        PreparedStatement prep = conn.prepareStatement(builder.toString());
        setKey(prep, 1, current);
        int count = prep.executeUpdate();
        if (count != 1) {
            // the row has already been deleted
            throw DbException.get(ErrorCode.NO_DATA_AVAILABLE);
        }
    }

    /**
     * Update a row in the database.
     *
     * @param current the old row
     * @param updateRow the new row
     * @throws SQLException if the row has been deleted
     */
    public void updateRow(Value[] current, Value[] updateRow) throws SQLException {
        StringBuilder builder = new StringBuilder("UPDATE ");
        appendTableName(builder);
        builder.append(" SET ");
        appendColumnList(builder, true);
        // TODO updatable result set: we could add all current values to the
        // where clause
        // - like this optimistic ('no') locking is possible
        appendKeyCondition(builder);
        PreparedStatement prep = conn.prepareStatement(builder.toString());
        int j = 1;
        for (int i = 0; i < columnCount; i++) {
            Value v = updateRow[i];
            if (v == null) {
                v = current[i];
            }
            v.set(prep, j++);
        }
        setKey(prep, j, current);
        int count = prep.executeUpdate();
        if (count != 1) {
            // the row has been deleted
            throw DbException.get(ErrorCode.NO_DATA_AVAILABLE);
        }
    }

    /**
     * Insert a new row into the database.
     *
     * @param row the new row
     * @throws SQLException if the row could not be inserted
     */
    public void insertRow(Value[] row) throws SQLException {
        StringBuilder builder = new StringBuilder("INSERT INTO ");
        appendTableName(builder);
        builder.append('(');
        appendColumnList(builder, false);
        builder.append(")VALUES(");
        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                builder.append(',');
            }
            Value v = row[i];
            if (v == null) {
                builder.append("DEFAULT");
            } else {
                builder.append('?');
            }
        }
        builder.append(')');
        PreparedStatement prep = conn.prepareStatement(builder.toString());
        for (int i = 0, j = 0; i < columnCount; i++) {
            Value v = row[i];
            if (v != null) {
                v.set(prep, j++ + 1);
            }
        }
        int count = prep.executeUpdate();
        if (count != 1) {
            throw DbException.get(ErrorCode.NO_DATA_AVAILABLE);
        }
    }

}
