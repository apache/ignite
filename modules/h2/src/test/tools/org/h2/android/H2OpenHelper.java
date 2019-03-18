/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.android;

import android.content.Context;

/**
 * This helper class helps creating and managing databases. A subclass typically
 * implements the "on" methods.
 */
@SuppressWarnings("unused")
public abstract class H2OpenHelper {

    /**
     * Construct a new instance.
     *
     * @param context the context to use
     * @param name the name of the database (use null for an in-memory database)
     * @param factory the cursor factory to use
     * @param version the expected database version
     */
    H2OpenHelper(Context context, String name,
            H2Database.CursorFactory factory, int version) {
        // TODO
    }

    /**
     * Close the connection.
     */
    public synchronized void close() {
        // TODO
    }

    /**
     * Open a read-only connection.
     *
     * @return a new read-only connection
     */
    public synchronized H2Database getReadableDatabase() {
        return null;
    }

    /**
     * Open a read-write connection.
     *
     * @return a new read-write connection
     */
    public synchronized H2Database getWritableDatabase() {
        return null;
    }

    /**
     * This method is called when the database did not already exist.
     *
     * @param db the connection
     */
    public abstract void onCreate(H2Database db);

    /**
     * This method is called after opening the database.
     *
     * @param db the connection
     */
    public void onOpen(H2Database db) {
        // TODO
    }

    /**
     * This method is called when the version stored in the database file does
     * not match the expected value.
     *
     * @param db the connection
     * @param oldVersion the current version
     * @param newVersion the expected version
     */
    public abstract void onUpgrade(H2Database db, int oldVersion, int newVersion);

}
