/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.android;

import org.h2.result.ResultInterface;
import android.content.ContentResolver;
import android.database.AbstractWindowedCursor;
import android.database.CharArrayBuffer;
import android.database.ContentObserver;
import android.database.CursorWindow;
import android.database.DataSetObserver;
import android.net.Uri;
import android.os.Bundle;

/**
 * A cursor implementation.
 */
@SuppressWarnings("unused")
public class H2Cursor extends AbstractWindowedCursor {

    private H2Database database;
    private ResultInterface result;

    H2Cursor(H2Database db, H2CursorDriver driver, String editTable,
            H2Query query) {
        this.database = db;
        // TODO
    }

    H2Cursor(ResultInterface result) {
        this.result = result;
    }

    @Override
    public void close() {
        result.close();
    }

    @Override
    public void deactivate() {
        // TODO
    }

    @Override
    public int getColumnIndex(String columnName) {
        return 0;
    }

    @Override
    public String[] getColumnNames() {
        return null;
    }

    @Override
    public int getCount() {
        return result.isLazy() ? -1 : result.getRowCount();
    }

    /**
     * Get the database that created this cursor.
     *
     * @return the database
     */
    public H2Database getDatabase() {
        return database;
    }

    /**
     * The cursor moved to a new position.
     *
     * @param oldPosition the previous position
     * @param newPosition the new position
     * @return TODO
     */
    public boolean onMove(int oldPosition, int newPosition) {
        return false;
    }

    @Override
    public void registerDataSetObserver(DataSetObserver observer) {
        // TODO
    }

    @Override
    public boolean requery() {
        return false;
    }

    /**
     * Set the parameter values.
     *
     * @param selectionArgs the parameter values
     */
    public void setSelectionArguments(String[] selectionArgs) {
        // TODO
    }

    /**
     * TODO
     *
     * @param window the window
     */
    public void setWindow(CursorWindow window) {
        // TODO
    }

    @Override
    public boolean move(int offset) {
        if (offset == 1) {
            return result.next();
        }
        throw H2Database.unsupported();
    }

    @Override
    public void copyStringToBuffer(int columnIndex, CharArrayBuffer buffer) {
        // TODO

    }

    @Override
    public byte[] getBlob(int columnIndex) {
        // TODO
        return null;
    }

    @Override
    public int getColumnCount() {
        // TODO
        return 0;
    }

    @Override
    public int getColumnIndexOrThrow(String columnName) {
        // TODO
        return 0;
    }

    @Override
    public String getColumnName(int columnIndex) {
        // TODO
        return null;
    }

    @Override
    public double getDouble(int columnIndex) {
        // TODO
        return 0;
    }

    @Override
    public Bundle getExtras() {
        // TODO
        return null;
    }

    @Override
    public float getFloat(int columnIndex) {
        // TODO
        return 0;
    }

    @Override
    public int getInt(int columnIndex) {
        return result.currentRow()[columnIndex].getInt();
    }

    @Override
    public long getLong(int columnIndex) {
        return result.currentRow()[columnIndex].getLong();
    }

    @Override
    public int getPosition() {
        // TODO
        return 0;
    }

    @Override
    public short getShort(int columnIndex) {
        // TODO
        return 0;
    }

    @Override
    public String getString(int columnIndex) {
        return result.currentRow()[columnIndex].getString();
    }

    @Override
    public boolean getWantsAllOnMoveCalls() {
        // TODO
        return false;
    }

    @Override
    public boolean isAfterLast() {
        // TODO
        return false;
    }

    @Override
    public boolean isBeforeFirst() {
        // TODO
        return false;
    }

    @Override
    public boolean isClosed() {
        // TODO
        return false;
    }

    @Override
    public boolean isFirst() {
        // TODO
        return false;
    }

    @Override
    public boolean isLast() {
        // TODO
        return false;
    }

    @Override
    public boolean isNull(int columnIndex) {
        // TODO
        return false;
    }

    @Override
    public boolean moveToFirst() {
        // TODO
        return false;
    }

    @Override
    public boolean moveToLast() {
        // TODO
        return false;
    }

    @Override
    public boolean moveToNext() {
        // TODO
        return false;
    }

    @Override
    public boolean moveToPosition(int position) {
        // TODO
        return false;
    }

    @Override
    public boolean moveToPrevious() {
        // TODO
        return false;
    }

    @Override
    public void registerContentObserver(ContentObserver observer) {
        // TODO

    }

    @Override
    public Bundle respond(Bundle extras) {
        // TODO
        return null;
    }

    @Override
    public void setNotificationUri(ContentResolver cr, Uri uri) {
        // TODO

    }

    @Override
    public void unregisterContentObserver(ContentObserver observer) {
        // TODO

    }

    @Override
    public void unregisterDataSetObserver(DataSetObserver observer) {
        // TODO

    }

}
