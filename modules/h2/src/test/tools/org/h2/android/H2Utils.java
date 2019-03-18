/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.android;

/**
 * Utility methods.
 */
public class H2Utils {

    /**
     * A replacement for Context.openOrCreateDatabase.
     *
     * @param name the database name
     * @param mode the access mode
     * @param factory the cursor factory to use
     * @return the database connection
     */
    public static H2Database openOrCreateDatabase(String name, @SuppressWarnings("unused") int mode,
            H2Database.CursorFactory factory) {
        return H2Database.openOrCreateDatabase(name, factory);
    }

}
