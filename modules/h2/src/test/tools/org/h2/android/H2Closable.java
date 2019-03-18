/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.android;

import android.database.sqlite.SQLiteClosable;

/**
 * An object that can be closed.
 */
public abstract class H2Closable extends SQLiteClosable {

    /**
     * TODO
     */
    public void acquireReference() {
        // TODO
    }

    /**
     * TODO
     */
    public void releaseReference() {
        // TODO
    }

    /**
     * TODO
     */
    public void releaseReferenceFromContainer() {
        // TODO
    }

    /**
     * TODO
     */
    protected abstract void onAllReferencesReleased();

    /**
     * TODO
     */
    protected void onAllReferencesReleasedFromContainer() {
        // TODO
    }

}
