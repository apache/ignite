/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.jdbc.thin;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryExecuteResult;

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.FETCH_FORWARD;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

/**
 * JDBC statement implementation.
 */
public class JdbcThinStatement implements Statement {
    /** Default queryPage size. */
    private static final int DFLT_PAGE_SIZE = SqlQuery.DFLT_PAGE_SIZE;

    /** Ignite endpoint and I/O protocol implementation. */
    private JdbcThinConnection conn;

    /** Closed flag. */
    private boolean closed;

    /** Rows limit. */
    private int maxRows;

    /** Query timeout. */
    private int timeout;

    /** Current result set. */
    protected JdbcThinResultSet rs;

    /** Fetch size. */
    private int pageSize = DFLT_PAGE_SIZE;

    /** */
    private boolean alreadyRead;

    /**
     * Creates new statement.
     *
     * @param conn JDBC connection.
     */
    JdbcThinStatement(JdbcThinConnection conn) {
        assert conn != null;

        this.conn = conn;
    }

    /** {@inheritDoc} */
    @Override public ResultSet executeQuery(String sql) throws SQLException {
        execute0(sql, null);

        ResultSet rs = getResultSet();

        if (rs == null)
            throw new SQLException("The query isn't SELECT query: " + sql);

        return rs;
    }

    /**
     * @param sql Sql query.
     * @param args Query parameters.
     *
     * @throws SQLException Onj error.
     */
    protected void execute0(String sql, List<Object> args) throws SQLException {
        ensureNotClosed();

        if (rs != null) {
            rs.close();

            rs = null;
        }

        alreadyRead = false;

        if (sql == null || sql.isEmpty())
            throw new SQLException("SQL query is empty.");

        try {
            JdbcQueryExecuteResult res = conn.io().queryExecute(conn.getSchema(), pageSize, maxRows,
                sql, args);

            assert res != null;

            rs = new JdbcThinResultSet(this, res.getQueryId(), pageSize, res.last(), res.items(),
                res.isQuery(), res.updateCount());
        }
        catch (IOException e) {
            conn.close();

            throw new SQLException("Failed to query Ignite.", e);
        }
        catch (IgniteCheckedException e) {
            throw new SQLException("Failed to query Ignite.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate(String sql) throws SQLException {
        execute0(sql, null);

        int res = getUpdateCount();

        if (res == -1)
            throw new SQLException("The query is not DML statememt: " + sql);

        return res;
    }

    /** {@inheritDoc} */
    @Override public void close() throws SQLException {
        if (isClosed())
            return;

        if (rs != null)
            rs.close();

        closed = true;
    }

    /** {@inheritDoc} */
    @Override public int getMaxFieldSize() throws SQLException {
        ensureNotClosed();

        return 0;
    }

    /** {@inheritDoc} */
    @Override public void setMaxFieldSize(int max) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Field size limitation is not supported.");
    }

    /** {@inheritDoc} */
    @Override public int getMaxRows() throws SQLException {
        ensureNotClosed();

        return maxRows;
    }

    /** {@inheritDoc} */
    @Override public void setMaxRows(int maxRows) throws SQLException {
        ensureNotClosed();

        this.maxRows = maxRows;
    }

    /** {@inheritDoc} */
    @Override public void setEscapeProcessing(boolean enable) throws SQLException {
        ensureNotClosed();
    }

    /** {@inheritDoc} */
    @Override public int getQueryTimeout() throws SQLException {
        ensureNotClosed();

        return timeout / 1000;
    }

    /** {@inheritDoc} */
    @Override public void setQueryTimeout(int timeout) throws SQLException {
        ensureNotClosed();

        this.timeout = timeout * 1000;
    }

    /** {@inheritDoc} */
    @Override public void cancel() throws SQLException {
        ensureNotClosed();
    }

    /** {@inheritDoc} */
    @Override public SQLWarning getWarnings() throws SQLException {
        ensureNotClosed();

        return null;
    }

    /** {@inheritDoc} */
    @Override public void clearWarnings() throws SQLException {
        ensureNotClosed();
    }

    /** {@inheritDoc} */
    @Override public void setCursorName(String name) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public boolean execute(String sql) throws SQLException {
        ensureNotClosed();

        execute0(sql, null);

        return rs.isQuery();
    }

    /** {@inheritDoc} */
    @Override public ResultSet getResultSet() throws SQLException {
        JdbcThinResultSet rs = lastResultSet();

        ResultSet res = rs == null || !rs.isQuery() ? null : rs;

        if (res != null)
            alreadyRead = true;

        return res;
    }

    /** {@inheritDoc} */
    @Override public int getUpdateCount() throws SQLException {
        JdbcThinResultSet rs = lastResultSet();

        int res = rs == null || rs.isQuery() ? -1 : (int)rs.updatedCount();

        if (res != -1)
            alreadyRead = true;

        return res;
    }

    /**
     * Get last result set if any.
     *
     * @return Result set or null.
     * @throws SQLException If failed.
     */
    private JdbcThinResultSet lastResultSet() throws SQLException {
        ensureNotClosed();

        if (rs == null || alreadyRead)
            return null;

        return rs;
    }

    /** {@inheritDoc} */
    @Override public boolean getMoreResults() throws SQLException {
        ensureNotClosed();

        return false;
    }

    /** {@inheritDoc} */
    @Override public void setFetchDirection(int direction) throws SQLException {
        ensureNotClosed();

        if (direction != FETCH_FORWARD)
            throw new SQLFeatureNotSupportedException("Only forward direction is supported");
    }

    /** {@inheritDoc} */
    @Override public int getFetchDirection() throws SQLException {
        ensureNotClosed();

        return FETCH_FORWARD;
    }

    /** {@inheritDoc} */
    @Override public void setFetchSize(int fetchSize) throws SQLException {
        ensureNotClosed();

        if (fetchSize <= 0)
            throw new SQLException("Fetch size must be greater than zero.");

        this.pageSize = fetchSize;
    }

    /** {@inheritDoc} */
    @Override public int getFetchSize() throws SQLException {
        ensureNotClosed();

        return pageSize;
    }

    /** {@inheritDoc} */
    @Override public int getResultSetConcurrency() throws SQLException {
        ensureNotClosed();

        return CONCUR_READ_ONLY;
    }

    /** {@inheritDoc} */
    @Override public int getResultSetType() throws SQLException {
        ensureNotClosed();

        return TYPE_FORWARD_ONLY;
    }

    /** {@inheritDoc} */
    @Override public void addBatch(String sql) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void clearBatch() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public int[] executeBatch() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public Connection getConnection() throws SQLException {
        ensureNotClosed();

        return conn;
    }

    /** {@inheritDoc} */
    @Override public boolean getMoreResults(int curr) throws SQLException {
        ensureNotClosed();

        if (curr == KEEP_CURRENT_RESULT || curr == CLOSE_ALL_RESULTS)
            throw new SQLFeatureNotSupportedException("Multiple open results are not supported.");

        return false;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getGeneratedKeys() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Auto-generated columns are not supported .");
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Auto-generated columns are not supported .");
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate(String sql, int[] colIndexes) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Auto-generated columns are not supported .");
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate(String sql, String[] colNames) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Auto-generated columns are not supported .");
    }

    /** {@inheritDoc} */
    @Override public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        ensureNotClosed();

        if (autoGeneratedKeys == RETURN_GENERATED_KEYS)
            throw new SQLFeatureNotSupportedException("Auto-generated columns are not supported .");

        return execute(sql);
    }

    /** {@inheritDoc} */
    @Override public boolean execute(String sql, int[] colIndexes) throws SQLException {
        ensureNotClosed();

        if (colIndexes != null && colIndexes.length > 0)
            throw new SQLFeatureNotSupportedException("Auto-generated columns are not supported .");

        return execute(sql);
    }

    /** {@inheritDoc} */
    @Override public boolean execute(String sql, String[] colNames) throws SQLException {
        ensureNotClosed();

        if (colNames != null && colNames.length > 0)
            throw new SQLFeatureNotSupportedException("Auto-generated columns are not supported .");

        return execute(sql);
    }

    /** {@inheritDoc} */
    @Override public int getResultSetHoldability() throws SQLException {
        ensureNotClosed();

        return HOLD_CURSORS_OVER_COMMIT;
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() throws SQLException {
        return conn.isClosed() || closed;
    }

    /** {@inheritDoc} */
    @Override public void setPoolable(boolean poolable) throws SQLException {
        ensureNotClosed();

        if (poolable)
            throw new SQLFeatureNotSupportedException("Pooling is not supported.");
    }

    /** {@inheritDoc} */
    @Override public boolean isPoolable() throws SQLException {
        ensureNotClosed();

        return false;
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(iface))
            throw new SQLException("Statement is not a wrapper for " + iface.getName());

        return (T)this;
    }

    /** {@inheritDoc} */
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(JdbcThinStatement.class);
    }

    /** {@inheritDoc} */
    @Override public void closeOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException("closeOnCompletion is not supported.");
    }

    /** {@inheritDoc} */
    @Override public boolean isCloseOnCompletion() throws SQLException {
        ensureNotClosed();

        return false;
    }

    /**
     * Sets timeout in milliseconds.
     *
     * @param timeout Timeout.
     */
    void timeout(int timeout) {
        this.timeout = timeout;
    }

    /**
     * @return Connection.
     */
    JdbcThinConnection connection() {
        return conn;
    }

    /**
     * Ensures that statement is not closed.
     *
     * @throws SQLException If statement is closed.
     */
    protected void ensureNotClosed() throws SQLException {
        if (isClosed())
            throw new SQLException("Statement is closed.");
    }
}
