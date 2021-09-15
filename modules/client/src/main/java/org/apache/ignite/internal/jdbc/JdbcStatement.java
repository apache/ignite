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

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.client.proto.query.IgniteQueryErrorCode;
import org.apache.ignite.client.proto.query.SqlStateCode;
import org.apache.ignite.client.proto.query.event.BatchExecuteRequest;
import org.apache.ignite.client.proto.query.event.BatchExecuteResult;
import org.apache.ignite.client.proto.query.event.Query;
import org.apache.ignite.client.proto.query.event.QueryExecuteRequest;
import org.apache.ignite.client.proto.query.event.QueryExecuteResult;
import org.apache.ignite.client.proto.query.event.QuerySingleResult;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.CollectionUtils;

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.FETCH_FORWARD;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

/**
 * Jdbc statement implementation.
 */
public class JdbcStatement implements Statement {
    /** Default queryPage size. */
    private static final int DFLT_PAGE_SIZE = 1024;

    /** JDBC Connection implementation. */
    private final JdbcConnection conn;

    /** Result set holdability. */
    private final int resHoldability;

    /** Schema name. */
    private final String schema;

    /** Closed flag. */
    private volatile boolean closed;

    /** Query timeout. */
    private int timeout;

    /** Rows limit. */
    private int maxRows;

    /** Fetch size. */
    private int pageSize = DFLT_PAGE_SIZE;

    /** Result sets. */
    private volatile List<JdbcResultSet> resSets;

    /** Batch. */
    private List<Query> batch;

    /** Close on completion. */
    private boolean closeOnCompletion;

    /** Current result index. */
    private int curRes;

    /**
     * Creates new statement.
     *
     * @param conn JDBC connection.
     * @param resHoldability Result set holdability.
     * @param schema Schema name.
     */
    JdbcStatement(JdbcConnection conn, int resHoldability, String schema) {
        assert conn != null;

        this.conn = conn;
        this.resHoldability = resHoldability;
        this.schema = schema;
    }

    /** {@inheritDoc} */
    @Override public ResultSet executeQuery(String sql) throws SQLException {
        execute0(Objects.requireNonNull(sql), null);

        ResultSet rs = getResultSet();

        if (rs == null)
            throw new SQLException("The query isn't SELECT query: " + sql, SqlStateCode.PARSING_EXCEPTION);

        return rs;
    }

    /**
     * Execute the query with given parameters.
     *
     * @param sql Sql query.
     * @param args Query parameters.
     *
     * @throws SQLException Onj error.
     */
    protected void execute0(String sql, List<Object> args) throws SQLException {
        ensureNotClosed();

        closeResults();

        if (sql == null || sql.isEmpty())
            throw new SQLException("SQL query is empty.");

        QueryExecuteRequest req = new QueryExecuteRequest(schema, pageSize, maxRows, sql,
            args == null ? ArrayUtils.OBJECT_EMPTY_ARRAY : args.toArray());

        QueryExecuteResult res = conn.handler().query(req);

        if (!res.hasResults())
            throw IgniteQueryErrorCode.createJdbcSqlException(res.err(), res.status());

        for (QuerySingleResult jdbcRes : res.results()) {
            if (!jdbcRes.hasResults())
                throw IgniteQueryErrorCode.createJdbcSqlException(jdbcRes.err(), jdbcRes.status());
        }

        resSets = new ArrayList<>(res.results().size());

        for (QuerySingleResult jdbcRes : res.results()) {
            resSets.add(new JdbcResultSet(this, jdbcRes.cursorId(), pageSize,
                jdbcRes.last(), jdbcRes.items(), jdbcRes.isQuery(), false, jdbcRes.updateCount(),
                closeOnCompletion, conn.handler()));
        }

        assert !resSets.isEmpty() : "At least one results set is expected";
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate(String sql) throws SQLException {
        execute0(Objects.requireNonNull(sql), null);

        int res = getUpdateCount();

        if (res == -1) {
            closeResults();
            throw new SQLException("The query is not DML statement: " + sql);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void close() throws SQLException {
        if (isClosed())
            return;

        try {
            closeResults();

            conn.removeStatement(this);
        }
        finally {
            closed = true;
        }
    }

    /** {@inheritDoc} */
    @Override public int getMaxFieldSize() throws SQLException {
        ensureNotClosed();

        return 0;
    }

    /** {@inheritDoc} */
    @Override public void setMaxFieldSize(int max) throws SQLException {
        ensureNotClosed();

        if (max < 0)
            throw new SQLException("Invalid field limit.");

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

        if (maxRows < 0)
            throw new SQLException("Invalid max rows value.");

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

        if (timeout < 0)
            throw new SQLException("Invalid timeout value.");

        //The timeout value of 0 will be converted to Integer.MAX_VALUE timeout to avoid further checks to 0.
        //This is because zero means there is no timeout limit.
        timeout(timeout * 1000 > timeout ? timeout * 1000 : Integer.MAX_VALUE);
    }

    /** {@inheritDoc} */
    @Override public void cancel() throws SQLException {
        ensureNotClosed();

        throw new SQLException("Cancellation is not supported.");
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

        execute0(Objects.requireNonNull(sql), null);

        return isQuery();
    }

    /** {@inheritDoc} */
    @Override public ResultSet getResultSet() throws SQLException {
        ensureNotClosed();

        if (resSets == null || curRes >= resSets.size())
            return null;

        JdbcResultSet rs = resSets.get(curRes);

        if (!rs.isQuery())
            return null;

        return rs;
    }

    /** {@inheritDoc} */
    @Override public int getUpdateCount() throws SQLException {
        ensureNotClosed();

        if (resSets == null || curRes >= resSets.size())
            return -1;

        JdbcResultSet rs = resSets.get(curRes);

        if (rs.isQuery())
            return -1;

        return (int)rs.updatedCount();
    }

    /** {@inheritDoc} */
    @Override public boolean getMoreResults() throws SQLException {
        return getMoreResults(CLOSE_CURRENT_RESULT);
    }

    /** {@inheritDoc} */
    @Override public void setFetchDirection(int direction) throws SQLException {
        ensureNotClosed();

        if (direction != FETCH_FORWARD)
            throw new SQLFeatureNotSupportedException("Only forward direction is supported.");
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

        pageSize = fetchSize;
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

        if (batch == null)
            batch = new ArrayList<>();

        batch.add(new Query(sql, null));
    }

    /** {@inheritDoc} */
    @Override public void clearBatch() throws SQLException {
        ensureNotClosed();

        batch = null;
    }

    /** {@inheritDoc} */
    @Override public int[] executeBatch() throws SQLException {
        ensureNotClosed();

        closeResults();

        if (CollectionUtils.nullOrEmpty(batch))
            return new int[0];

        BatchExecuteRequest req = new BatchExecuteRequest(conn.getSchema(), batch, conn.getAutoCommit());

        try {
            BatchExecuteResult res = conn.handler().batch(req);

            if (!res.hasResults())
                throw new BatchUpdateException(res.err(),
                    IgniteQueryErrorCode.codeToSqlState(res.status()),
                    res.status(),
                    res.updateCounts());

            return res.updateCounts();
        }
        finally {
            batch = null;
        }
    }

    /** {@inheritDoc} */
    @Override public Connection getConnection() throws SQLException {
        ensureNotClosed();

        return conn;
    }

    /** {@inheritDoc} */
    @Override public boolean getMoreResults(int curr) throws SQLException {
        ensureNotClosed();

        if (resSets == null || curRes >= resSets.size())
            return false;

        curRes++;

        if (resSets != null) {
            assert curRes <= resSets.size() : "Invalid results state: [resultsCount=" + resSets.size() +
                ", curRes=" + curRes + ']';

            switch (curr) {
                case CLOSE_CURRENT_RESULT:
                    if (curRes > 0)
                        resSets.get(curRes - 1).close0();

                    break;

                case CLOSE_ALL_RESULTS:
                case KEEP_CURRENT_RESULT:
                    throw new SQLFeatureNotSupportedException("Multiple open results is not supported.");

                default:
                    throw new SQLException("Invalid 'current' parameter.");
            }
        }

        return (resSets != null && curRes < resSets.size());
    }

    /** {@inheritDoc} */
    @Override public ResultSet getGeneratedKeys() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Auto-generated columns are not supported.");
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        ensureNotClosed();

        switch (autoGeneratedKeys) {
            case Statement.RETURN_GENERATED_KEYS:
                throw new SQLFeatureNotSupportedException("Auto-generated columns are not supported.");

            case Statement.NO_GENERATED_KEYS:
                return executeUpdate(sql);

            default:
                throw new SQLException("Invalid autoGeneratedKeys value");
        }
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate(String sql, int[] colIndexes) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Auto-generated columns are not supported.");
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate(String sql, String[] colNames) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Auto-generated columns are not supported.");
    }

    /** {@inheritDoc} */
    @Override public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        ensureNotClosed();

        switch (autoGeneratedKeys) {
            case Statement.RETURN_GENERATED_KEYS:
                throw new SQLFeatureNotSupportedException("Auto-generated columns are not supported.");

            case Statement.NO_GENERATED_KEYS:
                return execute(sql);

            default:
                throw new SQLException("Invalid autoGeneratedKeys value.");
        }
    }

    /** {@inheritDoc} */
    @Override public boolean execute(String sql, int[] colIndexes) throws SQLException {
        ensureNotClosed();

        if (colIndexes != null && colIndexes.length > 0)
            throw new SQLFeatureNotSupportedException("Auto-generated columns are not supported.");

        return execute(sql);
    }

    /** {@inheritDoc} */
    @Override public boolean execute(String sql, String[] colNames) throws SQLException {
        ensureNotClosed();

        if (colNames != null && colNames.length > 0)
            throw new SQLFeatureNotSupportedException("Auto-generated columns are not supported.");

        return execute(sql);
    }

    /** {@inheritDoc} */
    @Override public int getResultSetHoldability() throws SQLException {
        ensureNotClosed();

        return resHoldability;
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
    @Override public void closeOnCompletion() throws SQLException {
        ensureNotClosed();

        closeOnCompletion = true;

        if (resSets != null) {
            for (JdbcResultSet rs : resSets)
                rs.closeStatement(true);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isCloseOnCompletion() throws SQLException {
        ensureNotClosed();

        return closeOnCompletion;
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(Objects.requireNonNull(iface)))
            throw new SQLException("Statement is not a wrapper for " + iface.getName());

        return (T)this;
    }

    /** {@inheritDoc} */
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(JdbcStatement.class);
    }

    /**
     * Adds a set of parameters to batch of commands.
     *
     * @throws SQLException If statement is closed.
     */
    protected void addBatch(String sql, ArrayList<Object> args) throws SQLException {
        ensureNotClosed();

        if (batch == null) {
            batch = new ArrayList<>();

            batch.add(new Query(sql, args.toArray()));
        }
        else
            batch.add(new Query(null, args.toArray()));
    }

    /**
     * Gets the isQuery flag from the first result.
     *
     * @return isQuery flag.
     */
    protected boolean isQuery() {
        return resSets.get(0).isQuery();
    }

    /**
     * Ensures that statement not closed.
     *
     * @throws SQLException If statement is closed.
     */
    void ensureNotClosed() throws SQLException {
        if (isClosed())
            throw new SQLException("Statement is closed.");
    }

    /**
     * Close results.
     *
     * @throws SQLException On error.
     */
    private void closeResults() throws SQLException {
        if (resSets != null) {
            for (JdbcResultSet rs : resSets)
                rs.close0();

            resSets = null;
            curRes = 0;
        }
    }

    /**
     * Used by statement on closeOnCompletion mode.
     *
     * @throws SQLException On error.
     */
    void closeIfAllResultsClosed() throws SQLException {
        if (isClosed())
            return;

        boolean allRsClosed = true;

        if (resSets != null) {
            for (JdbcResultSet rs : resSets) {
                if (!rs.isClosed())
                    allRsClosed = false;
            }
        }

        if (allRsClosed)
            close();
    }

    /**
     * Sets timeout in milliseconds.
     *
     * For test purposes.
     *
     * @param timeout Timeout.
     * @throws SQLException If timeout condition is not satisfied.
     */
    public final void timeout(int timeout) throws SQLException {
        if (timeout < 0)
            throw new SQLException("Condition timeout >= 0 is not satisfied.");

        this.timeout = timeout;
    }
}
