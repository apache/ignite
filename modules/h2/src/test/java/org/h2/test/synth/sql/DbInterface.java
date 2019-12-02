/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth.sql;

import java.sql.SQLException;

/**
 * Represents a connection to a (real or simulated) database.
 */
public interface DbInterface {

    /**
     * Drop all objects in the database.
     */
    void reset() throws SQLException;

    /**
     * Connect to the database.
     */
    void connect() throws Exception;

    /**
     * Disconnect from the database.
     */
    void disconnect() throws SQLException;

    /**
     * Close the connection and the database.
     */
    void end() throws SQLException;

    /**
     * Create the specified table.
     *
     * @param table the table to create
     */
    void createTable(Table table) throws SQLException;

    /**
     * Drop the specified table.
     *
     * @param table the table to drop
     */
    void dropTable(Table table) throws SQLException;

    /**
     * Create an index.
     *
     * @param index the index to create
     */
    void createIndex(Index index) throws SQLException;

    /**
     * Drop an index.
     *
     * @param index the index to drop
     */
    void dropIndex(Index index) throws SQLException;

    /**
     * Insert a row into a table.
     *
     * @param table the table
     * @param c the column list
     * @param v the values
     * @return the result
     */
    Result insert(Table table, Column[] c, Value[] v) throws SQLException;

    /**
     * Execute a query.
     *
     * @param sql the SQL statement
     * @return the result
     */
    Result select(String sql) throws SQLException;

    /**
     * Delete a number of rows.
     *
     * @param table the table
     * @param condition the condition
     * @return the result
     */
    Result delete(Table table, String condition) throws SQLException;

    /**
     * Update the given table with the new values.
     *
     * @param table the table
     * @param columns the columns to update
     * @param values the new values
     * @param condition the condition
     * @return the result of the update
     */
    Result update(Table table, Column[] columns, Value[] values,
            String condition) throws SQLException;

    /**
     * Enable or disable autocommit.
     *
     * @param b the new value
     */
    void setAutoCommit(boolean b) throws SQLException;

    /**
     * Commit a pending transaction.
     */
    void commit() throws SQLException;

    /**
     * Roll back a pending transaction.
     */
    void rollback() throws SQLException;
}
