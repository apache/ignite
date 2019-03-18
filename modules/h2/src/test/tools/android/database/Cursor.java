/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package android.database;

import android.content.ContentResolver;
import android.net.Uri;
import android.os.Bundle;

/**
 * This interface allows to access the rows in a result set.
 */
public interface Cursor {

    /**
     * Get the row count.
     *
     * @return the row count
     */
    int getCount();

    /**
     * Deactivate the cursor. The cursor can be re-activated using requery().
     */
    void deactivate();

    /**
     * Get the column index. The first column is 0.
     *
     * @param columnName the name of the column
     * @return the column index, or -1 if the column was not found
     */
    int getColumnIndex(String columnName);

    /**
     * Close the cursor.
     */
    void close();

    /**
     * Get the column names.
     *
     * @return the column names
     */
    String[] getColumnNames();

    /**
     * Register a data set observer.
     *
     * @param observer the observer
     */
    void registerDataSetObserver(DataSetObserver observer);

    /**
     * Re-run the query.
     *
     * @return TODO
     */
    boolean requery();

    /**
     * Move the cursor by the given number of rows forward or backward.
     *
     * @param offset the row offset
     * @return true if the operation was successful
     */
    boolean move(int offset);

    /**
     * TODO
     */
    void copyStringToBuffer(int columnIndex, CharArrayBuffer buffer);

    /**
     * Get the value from the current row.
     *
     * @param columnIndex the column index (0, 1,...)
     * @return the value
     */
    byte[] getBlob(int columnIndex);

    /**
     * Get the number of columns in the result.
     *
     * @return the column count
     */
    int getColumnCount();

    /**
     * Get the column index for the given column name, or throw an exception if
     * not found.
     *
     * @param columnName the column name
     * @return the index
     */
    int getColumnIndexOrThrow(String columnName);

    /**
     * Get the name of the given column.
     *
     * @param columnIndex the column index (0, 1,...)
     * @return the name
     */
    String getColumnName(int columnIndex);

    /**
     * Get the value from the current row.
     *
     * @param columnIndex the column index (0, 1,...)
     * @return the value
     */
    double getDouble(int columnIndex);

    /**
     * TODO
     *
     * @return TODO
     */
    Bundle getExtras();

    /**
     * Get the value from the current row.
     *
     * @param columnIndex the column index (0, 1,...)
     * @return the value
     */
    float getFloat(int columnIndex);

    /**
     * Get the value from the current row.
     *
     * @param columnIndex the column index (0, 1,...)
     * @return the value
     */
    int getInt(int columnIndex);

    /**
     * Get the value from the current row.
     *
     * @param columnIndex the column index (0, 1,...)
     * @return the value
     */
    long getLong(int columnIndex);

    /**
     * Get the current row number
     *
     * @return the row number TODO 0, 1,...
     */
    int getPosition();

    /**
     * Get the value from the current row.
     *
     * @param columnIndex the column index (0, 1,...)
     * @return the value
     */
    short getShort(int columnIndex);

    /**
     * Get the value from the current row.
     *
     * @param columnIndex the column index (0, 1,...)
     * @return the value
     */
    String getString(int columnIndex);

    /**
     * The method onMove is only called if this method returns true.
     *
     * @return true if calling onMove is required
     */
    boolean getWantsAllOnMoveCalls();

    /**
     * Check if the current position is past the last row.
     *
     * @return true if it is
     */
    boolean isAfterLast();

    /**
     * Check if the current position is before the first row.
     *
     * @return true if it is
     */
    boolean isBeforeFirst();

    /**
     * Check if the cursor is closed.
     *
     * @return true if it is
     */
    boolean isClosed();

    /**
     * Check if the current position is on the first row.
     *
     * @return true if it is
     */
    boolean isFirst();

    /**
     * Check if the current position is on the last row.
     *
     * @return true if it is
     */
    boolean isLast();

    /**
     * Check if the value of the current row is null.
     *
     * @param columnIndex the column index (0, 1,...)
     * @return true if it is
     */
    boolean isNull(int columnIndex);

    /**
     * Move to the first row.
     *
     * @return TODO
     */
    boolean moveToFirst();

    /**
     * Move to the last row.
     *
     * @return TODO
     */
    boolean moveToLast();

    /**
     * Move to the next row.
     *
     * @return TODO
     */
    boolean moveToNext();

    /**
     * Move to the given row.
     *
     * @param position TODO
     * @return TODO
     */
    boolean moveToPosition(int position);

    /**
     * Move to the previous row.
     *
     * @return TODO
     */
    boolean moveToPrevious();

    /**
     * TODO
     *
     * @param observer TODO
     */
    void registerContentObserver(ContentObserver observer);

    /**
     * TODO
     *
     * @param extras TODO
     * @return TODO
     */
    Bundle respond(Bundle extras);

    /**
     * TODO
     *
     * @param cr TODO
     * @param uri TODO
     */
    void setNotificationUri(ContentResolver cr, Uri uri);

    /**
     * TODO
     *
     * @param observer TODO
     */
    void unregisterContentObserver(ContentObserver observer);

    /**
     * TODO
     *
     * @param observer TODO
     */
    void unregisterDataSetObserver(DataSetObserver observer);

}
