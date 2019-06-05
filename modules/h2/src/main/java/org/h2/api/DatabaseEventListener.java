/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.api;

import java.sql.SQLException;
import java.util.EventListener;

/**
 * A class that implements this interface can get notified about exceptions
 * and other events. A database event listener can be registered when
 * connecting to a database. Example database URL:
 * jdbc:h2:test;DATABASE_EVENT_LISTENER='com.acme.DbListener'
 */
public interface DatabaseEventListener extends EventListener {

    /**
     * This state is used when scanning the database file.
     */
    int STATE_SCAN_FILE = 0;

    /**
     * This state is used when re-creating an index.
     */
    int STATE_CREATE_INDEX = 1;

    /**
     * This state is used when re-applying the transaction log or rolling back
     * uncommitted transactions.
     */
    int STATE_RECOVER = 2;

    /**
     * This state is used during the BACKUP command.
     */
    int STATE_BACKUP_FILE = 3;

    /**
     * This state is used after re-connecting to a database (if auto-reconnect
     * is enabled).
     */
    int STATE_RECONNECTED = 4;

    /**
     * This state is used when a query starts.
     */
    int STATE_STATEMENT_START = 5;

    /**
     * This state is used when a query ends.
     */
    int STATE_STATEMENT_END = 6;

    /**
     * This state is used for periodic notification during long-running queries.
     */
    int STATE_STATEMENT_PROGRESS = 7;

    /**
     * This method is called just after creating the object.
     * This is done when opening the database if the listener is specified
     * in the database URL, but may be later if the listener is set at
     * runtime with the SET SQL statement.
     *
     * @param url - the database URL
     */
    void init(String url);

    /**
     * This method is called after the database has been opened. It is save to
     * connect to the database and execute statements at this point.
     */
    void opened();

    /**
     * This method is called if an exception occurred.
     *
     * @param e the exception
     * @param sql the SQL statement
     */
    void exceptionThrown(SQLException e, String sql);

    /**
     * This method is called for long running events, such as recovering,
     * scanning a file or building an index.
     * <p>
     * More states might be added in future versions, therefore implementations
     * should silently ignore states that they don't understand.
     * </p>
     *
     * @param state the state
     * @param name the object name
     * @param x the current position
     * @param max the highest possible value (might be 0)
     */
    void setProgress(int state, String name, int x, int max);

    /**
     * This method is called before the database is closed normally. It is save
     * to connect to the database and execute statements at this point, however
     * the connection must be closed before the method returns.
     */
    void closingDatabase();

}
