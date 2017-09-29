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

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcBatchExecuteRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcBatchExecuteResult;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQuery;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryExecuteMultipleStatementsResult;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryExecuteRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryExecuteResult;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcStatementType;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcResult;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcResultInfo;

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.FETCH_FORWARD;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

/**
 * JDBC statement implementation.
 */
public class JdbcThinStatement implements Statement {
    /** Default queryPage size. */
    private static final int DFLT_PAGE_SIZE = SqlQuery.DFLT_PAGE_SIZE;

    /** JDBC Connection implementation. */
    protected JdbcThinConnection conn;

    /** Closed flag. */
    private boolean closed;

    /** Rows limit. */
    private int maxRows;

    /** Query timeout. */
    private int timeout;

    /** Fetch size. */
    private int pageSize = DFLT_PAGE_SIZE;

    /** Result set  holdability*/
    private final int resHoldability;

    /** Batch. */
    protected List<JdbcQuery> batch;

    /** Close this statement on result set close. */
    private boolean closeOnCompletion;

    /** Result sets. */
    protected List<JdbcThinResultSet> resultSets;

    /** Current result index. */
    protected int curRes;

    /**
     * Creates new statement.
     *
     * @param conn JDBC connection.
     * @param resHoldability Result set holdability.
     */
    JdbcThinStatement(JdbcThinConnection conn, int resHoldability) {
        assert conn != null;

        this.conn = conn;
        this.resHoldability = resHoldability;
    }

    /** {@inheritDoc} */
    @Override public ResultSet executeQuery(String sql) throws SQLException {
        execute0(JdbcStatementType.SELECT_STATEMENT_TYPE, sql, null);

        ResultSet rs = getResultSet();

        if (rs == null)
            throw new SQLException("The query isn't SELECT query: " + sql, SqlStateCode.PARSING_EXCEPTION);

        return rs;
    }

    /**
     * @param stmtType Expected statement type.
     * @param sql Sql query.
     * @param args Query parameters.
     *
     * @throws SQLException Onj error.
     */
    protected void execute0(JdbcStatementType stmtType, String sql, List<Object> args) throws SQLException {
        ensureNotClosed();

        closeResults();

        if (sql == null || sql.isEmpty())
            throw new SQLException("SQL query is empty.");

        JdbcResult res0 = conn.sendRequest(new JdbcQueryExecuteRequest(stmtType, conn.getSchema(), pageSize,
            maxRows, sql, args == null ? null : args.toArray(new Object[args.size()])));

        assert res0 != null;

        if (res0 instanceof JdbcQueryExecuteResult) {
            JdbcQueryExecuteResult res = (JdbcQueryExecuteResult)res0;

            resultSets = Collections.singletonList(new JdbcThinResultSet(this, res.getQueryId(), pageSize,
                res.last(), res.items(), res.isQuery(), conn.autoCloseServerCursor(), res.updateCount(),
                closeOnCompletion));
        }
        else if (res0 instanceof JdbcQueryExecuteMultipleStatementsResult) {
            JdbcQueryExecuteMultipleStatementsResult res = (JdbcQueryExecuteMultipleStatementsResult)res0;

            List<JdbcResultInfo> resInfos = res.results();

            resultSets = new ArrayList<>(resInfos.size());

            boolean firstRes = true;

            for(JdbcResultInfo rsInfo : resInfos) {
                if (!rsInfo.isQuery()) {
                    resultSets.add(new JdbcThinResultSet(this, -1, pageSize,
                        true, Collections.<List<Object>>emptyList(), false,
                        conn.autoCloseServerCursor(), rsInfo.updateCount(), closeOnCompletion));
                }
                else {
                    if (firstRes) {
                        firstRes = false;

                        resultSets.add(new JdbcThinResultSet(this, rsInfo.queryId(), pageSize,
                            res.isLast(), res.items(), true,
                            conn.autoCloseServerCursor(), -1, closeOnCompletion));
                    }
                    else {
                        resultSets.add(new JdbcThinResultSet(this, rsInfo.queryId(), pageSize,
                            false, null, true,
                            conn.autoCloseServerCursor(), -1, closeOnCompletion));
                    }
                }
            }
        }
        else
            throw new SQLException("Unexpected result [res=" + res0 + ']');

        assert resultSets.size() > 0 : "At least one results set is expected";
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate(String sql) throws SQLException {
        execute0(JdbcStatementType.UPDATE_STMT_TYPE, sql, null);

        int res = getUpdateCount();

        if (res == -1)
            throw new SQLException("The query is not DML statememt: " + sql);

        return res;
    }

    /** {@inheritDoc} */
    @Override public void close() throws SQLException {
        if (isClosed())
            return;

        closeResults();

        closed = true;
    }

    /**
     * Close results.
     * @throws SQLException On error.
     */
    private void closeResults() throws SQLException {
        if (resultSets != null) {
            for (JdbcThinResultSet rs : resultSets)
                rs.close0();

            resultSets = null;
            curRes = 0;
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

        execute0(JdbcStatementType.ANY_STATEMENT_TYPE, sql, null);

        return resultSets.get(0).isQuery();
    }

    /** {@inheritDoc} */
    @Override public ResultSet getResultSet() throws SQLException {
        JdbcThinResultSet rs = nextResultSet();

        if (rs == null)
            return null;

        if (!rs.isQuery()) {
            curRes--;

            return null;
        }

        return rs;
    }

    /** {@inheritDoc} */
    @Override public int getUpdateCount() throws SQLException {
        JdbcThinResultSet rs = nextResultSet();

        if (rs == null)
            return -1;

        if (rs.isQuery()) {
            curRes--;

            return -1;
        }

        return (int)rs.updatedCount();
    }

    /**
     * Get last result set if any.
     *
     * @return Result set or null.
     * @throws SQLException If failed.
     */
    private JdbcThinResultSet nextResultSet() throws SQLException {
        ensureNotClosed();

        if (resultSets == null || curRes >= resultSets.size())
            return null;
        else
            return resultSets.get(curRes++);
    }

    /** {@inheritDoc} */
    @Override public boolean getMoreResults() throws SQLException {
        ensureNotClosed();

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

        if (batch == null)
            batch = new ArrayList<>();

        batch.add(new JdbcQuery(sql, null));
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

        if (batch == null || batch.isEmpty())
            throw new SQLException("Batch is empty.");

        try {
            JdbcBatchExecuteResult res = conn.sendRequest(new JdbcBatchExecuteRequest(conn.getSchema(), batch));

            if (res.errorCode() != ClientListenerResponse.STATUS_SUCCESS) {
                throw new BatchUpdateException(res.errorMessage(), IgniteQueryErrorCode.codeToSqlState(res.errorCode()),
                    res.errorCode(), res.updateCounts());
            }

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

        if (resultSets != null) {
            assert curRes <= resultSets.size() : "Invalid results state: [resultsCount=" + resultSets.size() +
                ", curRes=" + curRes + ']';

            switch (curr) {
                case CLOSE_CURRENT_RESULT:
                    if (curRes > 0)
                        resultSets.get(curRes - 1).close0();

                    break;

                case CLOSE_ALL_RESULTS:
                    for (int i = 0; i < curRes; ++i)
                        resultSets.get(i).close0();

                    break;

                case KEEP_CURRENT_RESULT:
                    break;

                default:
                    throw new SQLException("Invalid 'current' parameter.");
            }
        }

        return (resultSets != null && curRes < resultSets.size());
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
    @SuppressWarnings("unchecked")
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
        ensureNotClosed();

        closeOnCompletion = true;

        if (resultSets != null) {
            for (JdbcThinResultSet rs : resultSets)
                rs.closeStatement(true);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isCloseOnCompletion() throws SQLException {
        ensureNotClosed();

        return closeOnCompletion;
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

    /**
     * Used by statement on closeOnCompletion mode.
     * @throws SQLException On error.
     */
    void closeIfAllResultsClosed() throws SQLException {
        if (isClosed())
            return;

        boolean allRsClosed = true;

        if (resultSets != null) {
            for (JdbcThinResultSet rs : resultSets) {
                if (!rs.isClosed())
                    allRsClosed = false;
            }
        }

        if (allRsClosed)
            close();
    }
}
