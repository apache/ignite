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

package org.apache.ignite.internal.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.FETCH_FORWARD;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

/**
 * JDBC statement implementation.
 */
public class JdbcStatement implements Statement {
    /** Task name. */
    private static final String TASK_NAME =
        "org.apache.ignite.internal.processors.cache.query.jdbc.GridCacheQueryJdbcTask";

    /** Default fetch size. */
    private static final int DFLT_FETCH_SIZE = 1024;

    /** Connection. */
    private final JdbcConnection conn;

    /** Closed flag. */
    private boolean closed;

    /** Rows limit. */
    private int maxRows;

    /** Query timeout. */
    private int timeout;

    /** Current result set. */
    private ResultSet rs;

    /** Query arguments. */
    protected Object[] args;

    /** Fetch size. */
    private int fetchSize = DFLT_FETCH_SIZE;

    /**
     * Creates new statement.
     *
     * @param conn Connection.
     */
    JdbcStatement(JdbcConnection conn) {
        assert conn != null;

        this.conn = conn;
    }

    /** {@inheritDoc} */
    @Override public ResultSet executeQuery(String sql) throws SQLException {
        ensureNotClosed();

        rs = null;

        if (sql == null || sql.isEmpty())
            throw new SQLException("SQL query is empty");

        try {
            byte[] packet = conn.client().compute().execute(TASK_NAME,
                JdbcUtils.marshalArgument(JdbcUtils.taskArgument(conn.nodeId(), conn.cacheName(), sql,
                    timeout, args, fetchSize, maxRows)));

            byte status = packet[0];
            byte[] data = new byte[packet.length - 1];

            U.arrayCopy(packet, 1, data, 0, data.length);

            if (status == 1)
                throw JdbcUtils.unmarshalError(data);
            else {
                List<?> msg = JdbcUtils.unmarshal(data);

                assert msg.size() == 7;

                UUID nodeId = (UUID)msg.get(0);
                UUID futId = (UUID)msg.get(1);
                List<String> tbls = (List<String>)msg.get(2);
                List<String> cols = (List<String>)msg.get(3);
                List<String> types = (List<String>)msg.get(4);
                Collection<List<Object>> fields = (Collection<List<Object>>)msg.get(5);
                boolean finished = (Boolean)msg.get(6);

                return new JdbcResultSet(this, nodeId, futId, tbls, cols, types, fields, finished, fetchSize);
            }
        }
        catch (GridClientException e) {
            throw new SQLException("Failed to query Ignite.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate(String sql) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void close() throws SQLException {
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

        return timeout;
    }

    /** {@inheritDoc} */
    @Override public void setQueryTimeout(int timeout) throws SQLException {
        ensureNotClosed();

        this.timeout = timeout * 1000;
    }

    /** {@inheritDoc} */
    @Override public void cancel() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Cancellation is not supported.");
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

        rs = executeQuery(sql);

        return true;
    }

    /** {@inheritDoc} */
    @Override public ResultSet getResultSet() throws SQLException {
        ensureNotClosed();

        ResultSet rs0 = rs;

        rs = null;

        return rs0;
    }

    /** {@inheritDoc} */
    @Override public int getUpdateCount() throws SQLException {
        ensureNotClosed();

        return -1;
    }

    /** {@inheritDoc} */
    @Override public boolean getMoreResults() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Multiple open results are not supported.");
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

        this.fetchSize = fetchSize;
    }

    /** {@inheritDoc} */
    @Override public int getFetchSize() throws SQLException {
        ensureNotClosed();

        return fetchSize;
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

        throw new SQLFeatureNotSupportedException("Multiple open results are not supported.");
    }

    /** {@inheritDoc} */
    @Override public ResultSet getGeneratedKeys() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate(String sql, int[] colIndexes) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate(String sql, String[] colNames) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        ensureNotClosed();

        if (autoGeneratedKeys == RETURN_GENERATED_KEYS)
            throw new SQLFeatureNotSupportedException("Updates are not supported.");

        return execute(sql);
    }

    /** {@inheritDoc} */
    @Override public boolean execute(String sql, int[] colIndexes) throws SQLException {
        ensureNotClosed();

        if (colIndexes != null && colIndexes.length > 0)
            throw new SQLFeatureNotSupportedException("Updates are not supported.");

        return execute(sql);
    }

    /** {@inheritDoc} */
    @Override public boolean execute(String sql, String[] colNames) throws SQLException {
        ensureNotClosed();

        if (colNames != null && colNames.length > 0)
            throw new SQLFeatureNotSupportedException("Updates are not supported.");

        return execute(sql);
    }

    /** {@inheritDoc} */
    @Override public int getResultSetHoldability() throws SQLException {
        ensureNotClosed();

        return HOLD_CURSORS_OVER_COMMIT;
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() throws SQLException {
        return closed;
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
        return iface != null && iface == Statement.class;
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
    JdbcConnection connection() {
        return conn;
    }

    /**
     * Ensures that statement is not closed.
     *
     * @throws SQLException If statement is closed.
     */
    protected void ensureNotClosed() throws SQLException {
        if (closed)
            throw new SQLException("Statement is closed.");
    }
}