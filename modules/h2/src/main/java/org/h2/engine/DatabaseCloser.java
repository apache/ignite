/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.lang.ref.WeakReference;

import org.h2.message.Trace;

/**
 * This class is responsible to close a database if the application did not
 * close a connection. A database closer object only exists if there is no user
 * connected to the database.
 */
class DatabaseCloser extends Thread {

    private final boolean shutdownHook;
    private final Trace trace;
    private volatile WeakReference<Database> databaseRef;
    private int delayInMillis;

    DatabaseCloser(Database db, int delayInMillis, boolean shutdownHook) {
        this.databaseRef = new WeakReference<>(db);
        this.delayInMillis = delayInMillis;
        this.shutdownHook = shutdownHook;
        trace = db.getTrace(Trace.DATABASE);
    }

    /**
     * Stop and disable the database closer. This method is called after the
     * database has been closed, or after a session has been created.
     */
    void reset() {
        synchronized (this) {
            databaseRef = null;
        }
    }

    @Override
    public void run() {
        while (delayInMillis > 0) {
            try {
                int step = 100;
                Thread.sleep(step);
                delayInMillis -= step;
            } catch (Exception e) {
                // ignore InterruptedException
            }
            if (databaseRef == null) {
                return;
            }
        }
        Database database = null;
        synchronized (this) {
            if (databaseRef != null) {
                database = databaseRef.get();
            }
        }
        if (database != null) {
            try {
                database.close(shutdownHook);
            } catch (RuntimeException e) {
                // this can happen when stopping a web application,
                // if loading classes is no longer allowed
                // it would throw an IllegalStateException
                try {
                    trace.error(e, "could not close the database");
                    // if this was successful, we ignore the exception
                    // otherwise not
                } catch (Throwable e2) {
                    e.addSuppressed(e2);
                    throw e;
                }
            }
        }
    }

}
