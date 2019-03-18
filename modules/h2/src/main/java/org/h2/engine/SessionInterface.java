/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.io.Closeable;
import java.util.ArrayList;
import org.h2.command.CommandInterface;
import org.h2.message.Trace;
import org.h2.store.DataHandler;
import org.h2.value.Value;

/**
 * A local or remote session. A session represents a database connection.
 */
public interface SessionInterface extends Closeable {

    /**
     * Get the list of the cluster servers for this session.
     *
     * @return A list of "ip:port" strings for the cluster servers in this
     *         session.
     */
    ArrayList<String> getClusterServers();

    /**
     * Parse a command and prepare it for execution.
     *
     * @param sql the SQL statement
     * @param fetchSize the number of rows to fetch in one step
     * @return the prepared command
     */
    CommandInterface prepareCommand(String sql, int fetchSize);

    /**
     * Roll back pending transactions and close the session.
     */
    @Override
    void close();

    /**
     * Get the trace object
     *
     * @return the trace object
     */
    Trace getTrace();

    /**
     * Check if close was called.
     *
     * @return if the session has been closed
     */
    boolean isClosed();

    /**
     * Get the number of disk operations before power failure is simulated.
     * This is used for testing. If not set, 0 is returned
     *
     * @return the number of operations, or 0
     */
    int getPowerOffCount();

    /**
     * Set the number of disk operations before power failure is simulated.
     * To disable the countdown, use 0.
     *
     * @param i the number of operations
     */
    void setPowerOffCount(int i);

    /**
     * Get the data handler object.
     *
     * @return the data handler
     */
    DataHandler getDataHandler();

    /**
     * Check whether this session has a pending transaction.
     *
     * @return true if it has
     */
    boolean hasPendingTransaction();

    /**
     * Cancel the current or next command (called when closing a connection).
     */
    void cancel();

    /**
     * Check if the database changed and therefore reconnecting is required.
     *
     * @param write if the next operation may be writing
     * @return true if reconnecting is required
     */
    boolean isReconnectNeeded(boolean write);

    /**
     * Close the connection and open a new connection.
     *
     * @param write if the next operation may be writing
     * @return the new connection
     */
    SessionInterface reconnect(boolean write);

    /**
     * Called after writing has ended. It needs to be called after
     * isReconnectNeeded(true) returned false.
     */
    void afterWriting();

    /**
     * Check if this session is in auto-commit mode.
     *
     * @return true if the session is in auto-commit mode
     */
    boolean getAutoCommit();

    /**
     * Set the auto-commit mode. This call doesn't commit the current
     * transaction.
     *
     * @param autoCommit the new value
     */
    void setAutoCommit(boolean autoCommit);

    /**
     * Add a temporary LOB, which is closed when the session commits.
     *
     * @param v the value
     */
    void addTemporaryLob(Value v);

    /**
     * Check if this session is remote or embedded.
     *
     * @return true if this session is remote
     */
    boolean isRemote();

    /**
     * Set current schema.
     *
     * @param schema the schema name
     */
    void setCurrentSchemaName(String schema);

    /**
     * Get current schema.
     *
     * @return the current schema name
     */
    String getCurrentSchemaName();

    /**
     * Returns is this session supports generated keys.
     *
     * @return {@code true} if generated keys are supported, {@code false} if only
     *         {@code SCOPE_IDENTITY()} is supported
     */
    boolean isSupportsGeneratedKeys();

}
