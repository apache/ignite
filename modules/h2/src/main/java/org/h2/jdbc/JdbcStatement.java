/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.SessionInterface;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.message.TraceObject;
import org.h2.result.ResultInterface;
import org.h2.result.ResultWithGeneratedKeys;
import org.h2.tools.SimpleResultSet;
import org.h2.util.New;
import org.h2.util.ParserUtil;
import org.h2.util.StringUtils;

/**
 * Represents a statement.
 */
public class JdbcStatement extends TraceObject implements Statement, JdbcStatementBackwardsCompat {

    protected JdbcConnection conn;
    protected SessionInterface session;
    protected JdbcResultSet resultSet;
    protected int maxRows;
    protected int fetchSize = SysProperties.SERVER_RESULT_SET_FETCH_SIZE;
    protected int updateCount;
    protected JdbcResultSet generatedKeys;
    protected final int resultSetType;
    protected final int resultSetConcurrency;
    protected final boolean closedByResultSet;
    private volatile CommandInterface executingCommand;
    private int lastExecutedCommandType;
    private ArrayList<String> batchCommands;
    private boolean escapeProcessing = true;
    private volatile boolean cancelled;

    JdbcStatement(JdbcConnection conn, int id, int resultSetType,
            int resultSetConcurrency, boolean closeWithResultSet) {
        this.conn = conn;
        this.session = conn.getSession();
        setTrace(session.getTrace(), TraceObject.STATEMENT, id);
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.closedByResultSet = closeWithResultSet;
    }

    /**
     * Executes a query (select statement) and returns the result set.
     * If another result set exists for this statement, this will be closed
     * (even if this statement fails).
     *
     * @param sql the SQL statement to execute
     * @return the result set
     */
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        try {
            int id = getNextId(TraceObject.RESULT_SET);
            if (isDebugEnabled()) {
                debugCodeAssign("ResultSet", TraceObject.RESULT_SET, id,
                        "executeQuery(" + quote(sql) + ")");
            }
            synchronized (session) {
                checkClosed();
                closeOldResultSet();
                sql = JdbcConnection.translateSQL(sql, escapeProcessing);
                CommandInterface command = conn.prepareCommand(sql, fetchSize);
                ResultInterface result;
                boolean lazy = false;
                boolean scrollable = resultSetType != ResultSet.TYPE_FORWARD_ONLY;
                boolean updatable = resultSetConcurrency == ResultSet.CONCUR_UPDATABLE;
                setExecutingStatement(command);
                try {
                    result = command.executeQuery(maxRows, scrollable);
                    lazy = result.isLazy();
                } finally {
                    if (!lazy) {
                        setExecutingStatement(null);
                    }
                }
                if (!lazy) {
                    command.close();
                }
                resultSet = new JdbcResultSet(conn, this, command, result, id,
                        closedByResultSet, scrollable, updatable);
            }
            return resultSet;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement (insert, update, delete, create, drop)
     * and returns the update count.
     * If another result set exists for this statement, this will be closed
     * (even if this statement fails).
     *
     * If auto commit is on, this statement will be committed.
     * If the statement is a DDL statement (create, drop, alter) and does not
     * throw an exception, the current transaction (if any) is committed after
     * executing the statement.
     *
     * @param sql the SQL statement
     * @return the update count (number of row affected by an insert,
     *         update or delete, or 0 if no rows or the statement was a
     *         create, drop, commit or rollback)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    @Override
    public int executeUpdate(String sql) throws SQLException {
        try {
            debugCodeCall("executeUpdate", sql);
            return executeUpdateInternal(sql, false);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement (insert, update, delete, create, drop)
     * and returns the update count.
     * If another result set exists for this statement, this will be closed
     * (even if this statement fails).
     *
     * If auto commit is on, this statement will be committed.
     * If the statement is a DDL statement (create, drop, alter) and does not
     * throw an exception, the current transaction (if any) is committed after
     * executing the statement.
     *
     * @param sql the SQL statement
     * @return the update count (number of row affected by an insert,
     *         update or delete, or 0 if no rows or the statement was a
     *         create, drop, commit or rollback)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        try {
            debugCodeCall("executeLargeUpdate", sql);
            return executeUpdateInternal(sql, false);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private int executeUpdateInternal(String sql, Object generatedKeysRequest) throws SQLException {
        checkClosedForWrite();
        try {
            closeOldResultSet();
            sql = JdbcConnection.translateSQL(sql, escapeProcessing);
            CommandInterface command = conn.prepareCommand(sql, fetchSize);
            synchronized (session) {
                setExecutingStatement(command);
                try {
                    ResultWithGeneratedKeys result = command.executeUpdate(
                            conn.scopeGeneratedKeys() ? false : generatedKeysRequest);
                    updateCount = result.getUpdateCount();
                    ResultInterface gk = result.getGeneratedKeys();
                    if (gk != null) {
                        int id = getNextId(TraceObject.RESULT_SET);
                        generatedKeys = new JdbcResultSet(conn, this, command, gk, id,
                                false, true, false);
                    }
                } finally {
                    setExecutingStatement(null);
                }
            }
            command.close();
            return updateCount;
        } finally {
            afterWriting();
        }
    }

    /**
     * Executes an arbitrary statement. If another result set exists for this
     * statement, this will be closed (even if this statement fails).
     *
     * If the statement is a create or drop and does not throw an exception, the
     * current transaction (if any) is committed after executing the statement.
     * If auto commit is on, and the statement is not a select, this statement
     * will be committed.
     *
     * @param sql the SQL statement to execute
     * @return true if a result set is available, false if not
     */
    @Override
    public boolean execute(String sql) throws SQLException {
        try {
            debugCodeCall("execute", sql);
            return executeInternal(sql, false);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private boolean executeInternal(String sql, Object generatedKeysRequest) throws SQLException {
        int id = getNextId(TraceObject.RESULT_SET);
        checkClosedForWrite();
        try {
            closeOldResultSet();
            sql = JdbcConnection.translateSQL(sql, escapeProcessing);
            CommandInterface command = conn.prepareCommand(sql, fetchSize);
            boolean lazy = false;
            boolean returnsResultSet;
            synchronized (session) {
                setExecutingStatement(command);
                try {
                    if (command.isQuery()) {
                        returnsResultSet = true;
                        boolean scrollable = resultSetType != ResultSet.TYPE_FORWARD_ONLY;
                        boolean updatable = resultSetConcurrency == ResultSet.CONCUR_UPDATABLE;
                        ResultInterface result = command.executeQuery(maxRows, scrollable);
                        lazy = result.isLazy();
                        resultSet = new JdbcResultSet(conn, this, command, result, id,
                                closedByResultSet, scrollable, updatable);
                    } else {
                        returnsResultSet = false;
                        ResultWithGeneratedKeys result = command.executeUpdate(
                                conn.scopeGeneratedKeys() ? false : generatedKeysRequest);
                        updateCount = result.getUpdateCount();
                        ResultInterface gk = result.getGeneratedKeys();
                        if (gk != null) {
                            generatedKeys = new JdbcResultSet(conn, this, command, gk, id,
                                    false, true, false);
                        }
                    }
                } finally {
                    if (!lazy) {
                        setExecutingStatement(null);
                    }
                }
            }
            if (!lazy) {
                command.close();
            }
            return returnsResultSet;
        } finally {
            afterWriting();
        }
    }

    /**
     * Returns the last result set produces by this statement.
     *
     * @return the result set
     */
    @Override
    public ResultSet getResultSet() throws SQLException {
        try {
            checkClosed();
            if (resultSet != null) {
                int id = resultSet.getTraceId();
                debugCodeAssign("ResultSet", TraceObject.RESULT_SET, id, "getResultSet()");
            } else {
                debugCodeCall("getResultSet");
            }
            return resultSet;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the last update count of this statement.
     *
     * @return the update count (number of row affected by an insert, update or
     *         delete, or 0 if no rows or the statement was a create, drop,
     *         commit or rollback; -1 if the statement was a select).
     * @throws SQLException if this object is closed or invalid
     */
    @Override
    public int getUpdateCount() throws SQLException {
        try {
            debugCodeCall("getUpdateCount");
            checkClosed();
            return updateCount;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the last update count of this statement.
     *
     * @return the update count (number of row affected by an insert, update or
     *         delete, or 0 if no rows or the statement was a create, drop,
     *         commit or rollback; -1 if the statement was a select).
     * @throws SQLException if this object is closed or invalid
     */
    @Override
    public long getLargeUpdateCount() throws SQLException {
        try {
            debugCodeCall("getLargeUpdateCount");
            checkClosed();
            return updateCount;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Closes this statement.
     * All result sets that where created by this statement
     * become invalid after calling this method.
     */
    @Override
    public void close() throws SQLException {
        try {
            debugCodeCall("close");
            synchronized (session) {
                closeOldResultSet();
                if (conn != null) {
                    conn = null;
                }
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the connection that created this object.
     *
     * @return the connection
     */
    @Override
    public Connection getConnection() {
        debugCodeCall("getConnection");
        return conn;
    }

    /**
     * Gets the first warning reported by calls on this object.
     * This driver does not support warnings, and will always return null.
     *
     * @return null
     */
    @Override
    public SQLWarning getWarnings() throws SQLException {
        try {
            debugCodeCall("getWarnings");
            checkClosed();
            return null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Clears all warnings. As this driver does not support warnings,
     * this call is ignored.
     */
    @Override
    public void clearWarnings() throws SQLException {
        try {
            debugCodeCall("clearWarnings");
            checkClosed();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the name of the cursor. This call is ignored.
     *
     * @param name ignored
     * @throws SQLException if this object is closed
     */
    @Override
    public void setCursorName(String name) throws SQLException {
        try {
            debugCodeCall("setCursorName", name);
            checkClosed();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the fetch direction.
     * This call is ignored by this driver.
     *
     * @param direction ignored
     * @throws SQLException if this object is closed
     */
    @Override
    public void setFetchDirection(int direction) throws SQLException {
        try {
            debugCodeCall("setFetchDirection", direction);
            checkClosed();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the fetch direction.
     *
     * @return FETCH_FORWARD
     * @throws SQLException if this object is closed
     */
    @Override
    public int getFetchDirection() throws SQLException {
        try {
            debugCodeCall("getFetchDirection");
            checkClosed();
            return ResultSet.FETCH_FORWARD;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the maximum number of rows for a ResultSet.
     *
     * @return the number of rows where 0 means no limit
     * @throws SQLException if this object is closed
     */
    @Override
    public int getMaxRows() throws SQLException {
        try {
            debugCodeCall("getMaxRows");
            checkClosed();
            return maxRows;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the maximum number of rows for a ResultSet.
     *
     * @return the number of rows where 0 means no limit
     * @throws SQLException if this object is closed
     */
    @Override
    public long getLargeMaxRows() throws SQLException {
        try {
            debugCodeCall("getLargeMaxRows");
            checkClosed();
            return maxRows;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the maximum number of rows for a ResultSet.
     *
     * @param maxRows the number of rows where 0 means no limit
     * @throws SQLException if this object is closed
     */
    @Override
    public void setMaxRows(int maxRows) throws SQLException {
        try {
            debugCodeCall("setMaxRows", maxRows);
            checkClosed();
            if (maxRows < 0) {
                throw DbException.getInvalidValueException("maxRows", maxRows);
            }
            this.maxRows = maxRows;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the maximum number of rows for a ResultSet.
     *
     * @param maxRows the number of rows where 0 means no limit
     * @throws SQLException if this object is closed
     */
    @Override
    public void setLargeMaxRows(long maxRows) throws SQLException {
        try {
            debugCodeCall("setLargeMaxRows", maxRows);
            checkClosed();
            if (maxRows < 0) {
                throw DbException.getInvalidValueException("maxRows", maxRows);
            }
            this.maxRows = maxRows <= Integer.MAX_VALUE ? (int) maxRows : 0;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the number of rows suggested to read in one step.
     * This value cannot be higher than the maximum rows (setMaxRows)
     * set by the statement or prepared statement, otherwise an exception
     * is throws. Setting the value to 0 will set the default value.
     * The default value can be changed using the system property
     * h2.serverResultSetFetchSize.
     *
     * @param rows the number of rows
     * @throws SQLException if this object is closed
     */
    @Override
    public void setFetchSize(int rows) throws SQLException {
        try {
            debugCodeCall("setFetchSize", rows);
            checkClosed();
            if (rows < 0 || (rows > 0 && maxRows > 0 && rows > maxRows)) {
                throw DbException.getInvalidValueException("rows", rows);
            }
            if (rows == 0) {
                rows = SysProperties.SERVER_RESULT_SET_FETCH_SIZE;
            }
            fetchSize = rows;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the number of rows suggested to read in one step.
     *
     * @return the current fetch size
     * @throws SQLException if this object is closed
     */
    @Override
    public int getFetchSize() throws SQLException {
        try {
            debugCodeCall("getFetchSize");
            checkClosed();
            return fetchSize;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the result set concurrency created by this object.
     *
     * @return the concurrency
     */
    @Override
    public int getResultSetConcurrency() throws SQLException {
        try {
            debugCodeCall("getResultSetConcurrency");
            checkClosed();
            return resultSetConcurrency;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the result set type.
     *
     * @return the type
     * @throws SQLException if this object is closed
     */
    @Override
    public int getResultSetType()  throws SQLException {
        try {
            debugCodeCall("getResultSetType");
            checkClosed();
            return resultSetType;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the maximum number of bytes for a result set column.
     *
     * @return always 0 for no limit
     * @throws SQLException if this object is closed
     */
    @Override
    public int getMaxFieldSize() throws SQLException {
        try {
            debugCodeCall("getMaxFieldSize");
            checkClosed();
            return 0;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the maximum number of bytes for a result set column.
     * This method does currently do nothing for this driver.
     *
     * @param max the maximum size - ignored
     * @throws SQLException if this object is closed
     */
    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        try {
            debugCodeCall("setMaxFieldSize", max);
            checkClosed();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Enables or disables processing or JDBC escape syntax.
     * See also Connection.nativeSQL.
     *
     * @param enable - true (default) or false (no conversion is attempted)
     * @throws SQLException if this object is closed
     */
    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setEscapeProcessing("+enable+");");
            }
            checkClosed();
            escapeProcessing = enable;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Cancels a currently running statement.
     * This method must be called from within another
     * thread than the execute method.
     * Operations on large objects are not interrupted,
     * only operations that process many rows.
     *
     * @throws SQLException if this object is closed
     */
    @Override
    public void cancel() throws SQLException {
        try {
            debugCodeCall("cancel");
            checkClosed();
            // executingCommand can be reset  by another thread
            CommandInterface c = executingCommand;
            try {
                if (c != null) {
                    c.cancel();
                    cancelled = true;
                }
            } finally {
                setExecutingStatement(null);
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Check whether the statement was cancelled.
     *
     * @return true if yes
     */
    public boolean isCancelled() {
        return cancelled;
    }

    /**
     * Gets the current query timeout in seconds.
     * This method will return 0 if no query timeout is set.
     * The result is rounded to the next second.
     * For performance reasons, only the first call to this method
     * will query the database. If the query timeout was changed in another
     * way than calling setQueryTimeout, this method will always return
     * the last value.
     *
     * @return the timeout in seconds
     * @throws SQLException if this object is closed
     */
    @Override
    public int getQueryTimeout() throws SQLException {
        try {
            debugCodeCall("getQueryTimeout");
            checkClosed();
            return conn.getQueryTimeout();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the current query timeout in seconds.
     * Changing the value will affect all statements of this connection.
     * This method does not commit a transaction,
     * and rolling back a transaction does not affect this setting.
     *
     * @param seconds the timeout in seconds - 0 means no timeout, values
     *        smaller 0 will throw an exception
     * @throws SQLException if this object is closed
     */
    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        try {
            debugCodeCall("setQueryTimeout", seconds);
            checkClosed();
            if (seconds < 0) {
                throw DbException.getInvalidValueException("seconds", seconds);
            }
            conn.setQueryTimeout(seconds);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Adds a statement to the batch.
     *
     * @param sql the SQL statement
     */
    @Override
    public void addBatch(String sql) throws SQLException {
        try {
            debugCodeCall("addBatch", sql);
            checkClosed();
            sql = JdbcConnection.translateSQL(sql, escapeProcessing);
            if (batchCommands == null) {
                batchCommands = New.arrayList();
            }
            batchCommands.add(sql);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Clears the batch.
     */
    @Override
    public void clearBatch() throws SQLException {
        try {
            debugCodeCall("clearBatch");
            checkClosed();
            batchCommands = null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes the batch.
     * If one of the batched statements fails, this database will continue.
     *
     * @return the array of update counts
     */
    @Override
    public int[] executeBatch() throws SQLException {
        try {
            debugCodeCall("executeBatch");
            checkClosedForWrite();
            try {
                if (batchCommands == null) {
                    // TODO batch: check what other database do if no commands
                    // are set
                    batchCommands = New.arrayList();
                }
                int size = batchCommands.size();
                int[] result = new int[size];
                boolean error = false;
                SQLException next = null;
                for (int i = 0; i < size; i++) {
                    String sql = batchCommands.get(i);
                    try {
                        result[i] = executeUpdateInternal(sql, false);
                    } catch (Exception re) {
                        SQLException e = logAndConvert(re);
                        if (next == null) {
                            next = e;
                        } else {
                            e.setNextException(next);
                            next = e;
                        }
                        result[i] = Statement.EXECUTE_FAILED;
                        error = true;
                    }
                }
                batchCommands = null;
                if (error) {
                    throw new JdbcBatchUpdateException(next, result);
                }
                return result;
            } finally {
                afterWriting();
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes the batch.
     * If one of the batched statements fails, this database will continue.
     *
     * @return the array of update counts
     */
    @Override
    public long[] executeLargeBatch() throws SQLException {
        int[] intResult = executeBatch();
        int count = intResult.length;
        long[] longResult = new long[count];
        for (int i = 0; i < count; i++) {
            longResult[i] = intResult[i];
        }
        return longResult;
    }

    /**
     * Return a result set that contains the last generated auto-increment key
     * for this connection, if there was one. If no key was generated by the
     * last modification statement, then an empty result set is returned.
     * The returned result set only contains the data for the very last row.
     *
     * @return the result set with one row and one column containing the key
     * @throws SQLException if this object is closed
     */
    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        try {
            int id = getNextId(TraceObject.RESULT_SET);
            if (isDebugEnabled()) {
                debugCodeAssign("ResultSet", TraceObject.RESULT_SET, id, "getGeneratedKeys()");
            }
            checkClosed();
            if (!conn.scopeGeneratedKeys()) {
                if (generatedKeys != null) {
                    return generatedKeys;
                }
                if (session.isSupportsGeneratedKeys()) {
                    return new SimpleResultSet();
                }
            }
            // Compatibility mode or an old server, so use SCOPE_IDENTITY()
            return conn.getGeneratedKeys(this, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves to the next result set - however there is always only one result
     * set. This call also closes the current result set (if there is one).
     * Returns true if there is a next result set (that means - it always
     * returns false).
     *
     * @return false
     * @throws SQLException if this object is closed.
     */
    @Override
    public boolean getMoreResults() throws SQLException {
        try {
            debugCodeCall("getMoreResults");
            checkClosed();
            closeOldResultSet();
            return false;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Move to the next result set.
     * This method always returns false.
     *
     * @param current Statement.CLOSE_CURRENT_RESULT,
     *          Statement.KEEP_CURRENT_RESULT,
     *          or Statement.CLOSE_ALL_RESULTS
     * @return false
     */
    @Override
    public boolean getMoreResults(int current) throws SQLException {
        try {
            debugCodeCall("getMoreResults", current);
            switch (current) {
            case Statement.CLOSE_CURRENT_RESULT:
            case Statement.CLOSE_ALL_RESULTS:
                checkClosed();
                closeOldResultSet();
                break;
            case Statement.KEEP_CURRENT_RESULT:
                // nothing to do
                break;
            default:
                throw DbException.getInvalidValueException("current", current);
            }
            return false;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns the update count.
     *
     * @param sql the SQL statement
     * @param autoGeneratedKeys
     *            {@link Statement.RETURN_GENERATED_KEYS} if generated keys should
     *            be available for retrieval, {@link Statement.NO_GENERATED_KEYS} if
     *            generated keys should not be available
     * @return the update count (number of row affected by an insert,
     *         update or delete, or 0 if no rows or the statement was a
     *         create, drop, commit or rollback)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("executeUpdate("+quote(sql)+", "+autoGeneratedKeys+");");
            }
            return executeUpdateInternal(sql, autoGeneratedKeys == RETURN_GENERATED_KEYS);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns the update count.
     *
     * @param sql the SQL statement
     * @param autoGeneratedKeys
     *            {@link Statement.RETURN_GENERATED_KEYS} if generated keys should
     *            be available for retrieval, {@link Statement.NO_GENERATED_KEYS} if
     *            generated keys should not be available
     * @return the update count (number of row affected by an insert,
     *         update or delete, or 0 if no rows or the statement was a
     *         create, drop, commit or rollback)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("executeLargeUpdate("+quote(sql)+", "+autoGeneratedKeys+");");
            }
            return executeUpdateInternal(sql, autoGeneratedKeys == RETURN_GENERATED_KEYS);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns the update count.
     *
     * @param sql the SQL statement
     * @param columnIndexes
     *            an array of column indexes indicating the columns with generated
     *            keys that should be returned from the inserted row
     * @return the update count (number of row affected by an insert,
     *         update or delete, or 0 if no rows or the statement was a
     *         create, drop, commit or rollback)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("executeUpdate("+quote(sql)+", "+quoteIntArray(columnIndexes)+");");
            }
            return executeUpdateInternal(sql, columnIndexes);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns the update count.
     *
     * @param sql the SQL statement
     * @param columnIndexes
     *            an array of column indexes indicating the columns with generated
     *            keys that should be returned from the inserted row
     * @return the update count (number of row affected by an insert,
     *         update or delete, or 0 if no rows or the statement was a
     *         create, drop, commit or rollback)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    @Override
    public long executeLargeUpdate(String sql, int columnIndexes[]) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("executeLargeUpdate("+quote(sql)+", "+quoteIntArray(columnIndexes)+");");
            }
            return executeUpdateInternal(sql, columnIndexes);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns the update count.
     *
     * @param sql the SQL statement
     * @param columnNames
     *            an array of column names indicating the columns with generated
     *            keys that should be returned from the inserted row
     * @return the update count (number of row affected by an insert,
     *         update or delete, or 0 if no rows or the statement was a
     *         create, drop, commit or rollback)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("executeUpdate("+quote(sql)+", "+quoteArray(columnNames)+");");
            }
            return executeUpdateInternal(sql, columnNames);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns the update count.
     *
     * @param sql the SQL statement
     * @param columnNames
     *            an array of column names indicating the columns with generated
     *            keys that should be returned from the inserted row
     * @return the update count (number of row affected by an insert,
     *         update or delete, or 0 if no rows or the statement was a
     *         create, drop, commit or rollback)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    @Override
    public long executeLargeUpdate(String sql, String columnNames[]) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("executeLargeUpdate("+quote(sql)+", "+quoteArray(columnNames)+");");
            }
            return executeUpdateInternal(sql, columnNames);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns the update count.
     *
     * @param sql the SQL statement
     * @param autoGeneratedKeys
     *            {@link Statement.RETURN_GENERATED_KEYS} if generated keys should
     *            be available for retrieval, {@link Statement.NO_GENERATED_KEYS} if
     *            generated keys should not be available
     * @return the update count (number of row affected by an insert,
     *         update or delete, or 0 if no rows or the statement was a
     *         create, drop, commit or rollback)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("execute("+quote(sql)+", "+autoGeneratedKeys+");");
            }
            return executeInternal(sql, autoGeneratedKeys == RETURN_GENERATED_KEYS);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns the update count.
     *
     * @param sql the SQL statement
     * @param columnIndexes
     *            an array of column indexes indicating the columns with generated
     *            keys that should be returned from the inserted row
     * @return the update count (number of row affected by an insert,
     *         update or delete, or 0 if no rows or the statement was a
     *         create, drop, commit or rollback)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("execute("+quote(sql)+", "+quoteIntArray(columnIndexes)+");");
            }
            return executeInternal(sql, columnIndexes);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement and returns the update count.
     *
     * @param sql the SQL statement
     * @param columnNames
     *            an array of column names indicating the columns with generated
     *            keys that should be returned from the inserted row
     * @return the update count (number of row affected by an insert,
     *         update or delete, or 0 if no rows or the statement was a
     *         create, drop, commit or rollback)
     * @throws SQLException if a database error occurred or a
     *         select statement was executed
     */
    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("execute("+quote(sql)+", "+quoteArray(columnNames)+");");
            }
            return executeInternal(sql, columnNames);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the result set holdability.
     *
     * @return the holdability
     */
    @Override
    public int getResultSetHoldability() throws SQLException {
        try {
            debugCodeCall("getResultSetHoldability");
            checkClosed();
            return ResultSet.HOLD_CURSORS_OVER_COMMIT;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    @Override
    public void closeOnCompletion() {
        // not supported
    }

    /**
     * [Not supported]
     */
    @Override
    public boolean isCloseOnCompletion() {
        return true;
    }

    // =============================================================

    /**
     * Check if this connection is closed.
     * The next operation is a read request.
     *
     * @return true if the session was re-connected
     * @throws DbException if the connection or session is closed
     */
    boolean checkClosed() {
        return checkClosed(false);
    }

    /**
     * Check if this connection is closed.
     * The next operation may be a write request.
     *
     * @return true if the session was re-connected
     * @throws DbException if the connection or session is closed
     */
    boolean checkClosedForWrite() {
        return checkClosed(true);
    }

    /**
     * INTERNAL.
     * Check if the statement is closed.
     *
     * @param write if the next operation is possibly writing
     * @return true if a reconnect was required
     * @throws DbException if it is closed
     */
    protected boolean checkClosed(boolean write) {
        if (conn == null) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        }
        conn.checkClosed(write);
        SessionInterface s = conn.getSession();
        if (s != session) {
            session = s;
            trace = session.getTrace();
            return true;
        }
        return false;
    }

    /**
     * Called after each write operation.
     */
    void afterWriting() {
        if (conn != null) {
            conn.afterWriting();
        }
    }

    /**
     * INTERNAL.
     * Close and old result set if there is still one open.
     */
    protected void closeOldResultSet() throws SQLException {
        try {
            if (!closedByResultSet) {
                if (resultSet != null) {
                    resultSet.closeInternal();
                }
                if (generatedKeys != null) {
                    generatedKeys.closeInternal();
                }
            }
        } finally {
            cancelled = false;
            resultSet = null;
            updateCount = -1;
            generatedKeys = null;
        }
    }

    /**
     * INTERNAL.
     * Set the statement that is currently running.
     *
     * @param c the command
     */
    protected void setExecutingStatement(CommandInterface c) {
        if (c == null) {
            conn.setExecutingStatement(null);
        } else {
            conn.setExecutingStatement(this);
            lastExecutedCommandType = c.getCommandType();
        }
        executingCommand = c;
    }

    /**
     * Called when the result set is closed.
     *
     * @param command the command
     * @param closeCommand whether to close the command
     */
    void onLazyResultSetClose(CommandInterface command, boolean closeCommand) {
        setExecutingStatement(null);
        command.stop();
        if (closeCommand) {
            command.close();
        }
    }

    /**
     * INTERNAL.
     * Get the command type of the last executed command.
     */
    public int getLastExecutedCommandType() {
        return lastExecutedCommandType;
    }

    /**
     * Returns whether this statement is closed.
     *
     * @return true if the statement is closed
     */
    @Override
    public boolean isClosed() throws SQLException {
        try {
            debugCodeCall("isClosed");
            return conn == null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Return an object of this class if possible.
     *
     * @param iface the class
     * @return this
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            if (isWrapperFor(iface)) {
                return (T) this;
            }
            throw DbException.getInvalidValueException("iface", iface);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if unwrap can return an object of this class.
     *
     * @param iface the class
     * @return whether or not the interface is assignable from this class
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(getClass());
    }

    /**
     * Returns whether this object is poolable.
     * @return false
     */
    @Override
    public boolean isPoolable() {
        debugCodeCall("isPoolable");
        return false;
    }

    /**
     * Requests that this object should be pooled or not.
     * This call is ignored.
     *
     * @param poolable the requested value
     */
    @Override
    public void setPoolable(boolean poolable) {
        if (isDebugEnabled()) {
            debugCode("setPoolable("+poolable+");");
        }
    }

    /**
     * @param identifier
     *            identifier to quote if required
     * @param alwaysQuote
     *            if {@code true} identifier will be quoted unconditionally
     * @return specified identifier quoted if required or explicitly requested
     */
    @Override
    public String enquoteIdentifier(String identifier, boolean alwaysQuote) throws SQLException {
        if (alwaysQuote || !isSimpleIdentifier(identifier)) {
            return StringUtils.quoteIdentifier(identifier);
        }
        return identifier;
    }

    /**
     * @param identifier
     *            identifier to check
     * @return is specified identifier may be used without quotes
     */
    @Override
    public boolean isSimpleIdentifier(String identifier) throws SQLException {
        return ParserUtil.isSimpleIdentifier(identifier, true);
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {
        return getTraceObjectName();
    }

}

