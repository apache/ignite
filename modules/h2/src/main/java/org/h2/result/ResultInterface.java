/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.engine.SessionInterface;
import org.h2.value.Value;

/**
 * The result interface is used by the LocalResult and ResultRemote class.
 * A result may contain rows, or just an update count.
 */
public interface ResultInterface extends AutoCloseable {

    /**
     * Go to the beginning of the result, that means
     * before the first row.
     */
    void reset();

    /**
     * Get the current row.
     *
     * @return the row
     */
    Value[] currentRow();

    /**
     * Go to the next row.
     *
     * @return true if a row exists
     */
    boolean next();

    /**
     * Get the current row id, starting with 0.
     * -1 is returned when next() was not called yet.
     *
     * @return the row id
     */
    int getRowId();

    /**
     * Check if the current position is after last row.
     *
     * @return true if after last
     */
    boolean isAfterLast();

    /**
     * Get the number of visible columns.
     * More columns may exist internally for sorting or grouping.
     *
     * @return the number of columns
     */
    int getVisibleColumnCount();

    /**
     * Get the number of rows in this object.
     *
     * @return the number of rows
     */
    int getRowCount();

    /**
     * Check if this result has more rows to fetch.
     *
     * @return true if it has
     */
    boolean hasNext();

    /**
     * Check if this result set should be closed, for example because it is
     * buffered using a temporary file.
     *
     * @return true if close should be called.
     */
    boolean needToClose();

    /**
     * Close the result and delete any temporary files
     */
    @Override
    void close();

    /**
     * Get the column alias name for the column.
     *
     * @param i the column number (starting with 0)
     * @return the alias name
     */
    String getAlias(int i);

    /**
     * Get the schema name for the column, if one exists.
     *
     * @param i the column number (starting with 0)
     * @return the schema name or null
     */
    String getSchemaName(int i);

    /**
     * Get the table name for the column, if one exists.
     *
     * @param i the column number (starting with 0)
     * @return the table name or null
     */
    String getTableName(int i);

    /**
     * Get the column name.
     *
     * @param i the column number (starting with 0)
     * @return the column name
     */
    String getColumnName(int i);

    /**
     * Get the column data type.
     *
     * @param i the column number (starting with 0)
     * @return the column data type
     */
    int getColumnType(int i);

    /**
     * Get the precision for this column.
     *
     * @param i the column number (starting with 0)
     * @return the precision
     */
    long getColumnPrecision(int i);

    /**
     * Get the scale for this column.
     *
     * @param i the column number (starting with 0)
     * @return the scale
     */
    int getColumnScale(int i);

    /**
     * Get the display size for this column.
     *
     * @param i the column number (starting with 0)
     * @return the display size
     */
    int getDisplaySize(int i);

    /**
     * Check if this is an auto-increment column.
     *
     * @param i the column number (starting with 0)
     * @return true for auto-increment columns
     */
    boolean isAutoIncrement(int i);

    /**
     * Check if this column is nullable.
     *
     * @param i the column number (starting with 0)
     * @return Column.NULLABLE_*
     */
    int getNullable(int i);

    /**
     * Set the fetch size for this result set.
     *
     * @param fetchSize the new fetch size
     */
    void setFetchSize(int fetchSize);

    /**
     * Get the current fetch size for this result set.
     *
     * @return the fetch size
     */
    int getFetchSize();

    /**
     * Check if this a lazy execution result.
     *
     * @return true if it is a lazy result
     */
    boolean isLazy();

    /**
     * Check if this result set is closed.
     *
     * @return true if it is
     */
    boolean isClosed();

    /**
     * Create a shallow copy of the result set. The data and a temporary table
     * (if there is any) is not copied.
     *
     * @param targetSession the session of the copy
     * @return the copy if possible, or null if copying is not possible
     */
    ResultInterface createShallowCopy(SessionInterface targetSession);

    /**
     * Check if this result set contains the given row.
     *
     * @param values the row
     * @return true if the row exists
     */
    boolean containsDistinct(Value[] values);
}
