/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;

import org.h2.engine.SessionInterface;
import org.h2.util.Utils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * Simple in-memory result.
 */
public class SimpleResult implements ResultInterface {

    /**
     *  Column info for the simple result.
     */
    static final class Column {
        /** Column alias. */
        final String alias;

        /** Column name. */
        final String columnName;

        /** Column type. */
        final TypeInfo columnType;

        Column(String alias, String columnName, int columnType, long columnPrecision, int columnScale) {
            if (alias == null || columnName == null) {
                throw new NullPointerException();
            }
            this.alias = alias;
            this.columnName = columnName;
            this.columnType = TypeInfo.getTypeInfo(columnType, columnPrecision, columnScale, null);
        }

        Column(String alias, String columnName, TypeInfo columnType) {
            if (alias == null || columnName == null) {
                throw new NullPointerException();
            }
            this.alias = alias;
            this.columnName = columnName;
            this.columnType = columnType;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + alias.hashCode();
            result = prime * result + columnName.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null || getClass() != obj.getClass())
                return false;
            Column other = (Column) obj;
            return alias.equals(other.alias) && columnName.equals(other.columnName);
        }

        @Override
        public String toString() {
            if (alias.equals(columnName)) {
                return columnName;
            }
            return columnName + ' ' + alias;
        }

    }

    private final ArrayList<Column> columns;

    private final ArrayList<Value[]> rows;

    private int rowId;

    /**
     * Creates new instance of simple result.
     */
    public SimpleResult() {
        this.columns = Utils.newSmallArrayList();
        this.rows = new ArrayList<>();
        this.rowId = -1;
    }

    private SimpleResult(ArrayList<Column> columns, ArrayList<Value[]> rows) {
        this.columns = columns;
        this.rows = rows;
        this.rowId = -1;
    }

    /**
     * Add column to the result.
     *
     * @param alias Column's alias.
     * @param columnName Column's name.
     * @param columnType Column's value type.
     * @param columnPrecision Column's  precision.
     * @param columnScale Column's scale.
     */
    public void addColumn(String alias, String columnName, int columnType, long columnPrecision, int columnScale) {
        addColumn(new Column(alias, columnName, columnType, columnPrecision, columnScale));
    }

    /**
     * Add column to the result.
     *
     * @param alias Column's alias.
     * @param columnName Column's name.
     * @param columnType Column's type.
     */
    public void addColumn(String alias, String columnName, TypeInfo columnType) {
        addColumn(new Column(alias, columnName, columnType));
    }

    /**
     * Add column to the result.
     *
     * @param column Column info.
     */
    void addColumn(Column column) {
        assert rows.isEmpty();
        columns.add(column);
    }

    /**
     * Add row to result.
     *
     * @param values Row's values.
     */
    public void addRow(Value... values) {
        assert values.length == columns.size();
        rows.add(values);
    }

    @Override
    public void reset() {
        rowId = -1;
    }

    @Override
    public Value[] currentRow() {
        return rows.get(rowId);
    }

    @Override
    public boolean next() {
        int count = rows.size();
        if (rowId < count) {
            return ++rowId < count;
        }
        return false;
    }

    @Override
    public int getRowId() {
        return rowId;
    }

    @Override
    public boolean isAfterLast() {
        return rowId >= rows.size();
    }

    @Override
    public int getVisibleColumnCount() {
        return columns.size();
    }

    @Override
    public int getRowCount() {
        return rows.size();
    }

    @Override
    public boolean hasNext() {
        return rowId < rows.size() - 1;
    }

    @Override
    public boolean needToClose() {
        return false;
    }

    @Override
    public void close() {
        // Do nothing for now
    }

    @Override
    public String getAlias(int i) {
        return columns.get(i).alias;
    }

    @Override
    public String getSchemaName(int i) {
        return "";
    }

    @Override
    public String getTableName(int i) {
        return "";
    }

    @Override
    public String getColumnName(int i) {
        return columns.get(i).columnName;
    }

    @Override
    public TypeInfo getColumnType(int i) {
        return columns.get(i).columnType;
    }

    @Override
    public boolean isAutoIncrement(int i) {
        return false;
    }

    @Override
    public int getNullable(int i) {
        return ResultSetMetaData.columnNullableUnknown;
    }

    @Override
    public void setFetchSize(int fetchSize) {
        // Ignored
    }

    @Override
    public int getFetchSize() {
        return 1;
    }

    @Override
    public boolean isLazy() {
        return false;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public ResultInterface createShallowCopy(SessionInterface targetSession) {
        return new SimpleResult(columns, rows);
    }

}
