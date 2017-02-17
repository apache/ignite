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

package org.apache.ignite.internal.jdbc2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.F;

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.FETCH_FORWARD;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

/**
 * JDBC statement implementation.
 */
public class JdbcStatement implements Statement {
    /** Default fetch size. */
    private static final int DFLT_FETCH_SIZE = 1024;

    /** Connection. */
    protected final JdbcConnection conn;

    /** Closed flag. */
    private boolean closed;

    /** Rows limit. */
    private int maxRows;

    /** Current result set. */
    protected ResultSet rs;

    /** Query arguments. */
    protected ArrayList<Object> args;

    /** Fetch size. */
    private int fetchSize = DFLT_FETCH_SIZE;

    /** Result sets. */
    final Set<JdbcResultSet> resSets = new HashSet<>();

    /** Fields indexes. */
    Map<String, Integer> fieldsIdxs = new HashMap<>();

    /** Current updated items count. */
    long updateCnt = -1;

    /** Batch statements. */
    private List<String> batch;

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
    @SuppressWarnings("deprecation")
    @Override public ResultSet executeQuery(String sql) throws SQLException {
        ensureNotClosed();

        rs = null;

        updateCnt = -1;

        if (F.isEmpty(sql))
            throw new SQLException("SQL query is empty");

        Ignite ignite = conn.ignite();

        UUID nodeId = conn.nodeId();

        UUID uuid = UUID.randomUUID();

        boolean loc = nodeId == null;

        JdbcQueryTask qryTask = new JdbcQueryTask(loc ? ignite : null, conn.cacheName(), sql, loc, getArgs(),
            fetchSize, uuid, conn.isLocalQuery(), conn.isCollocatedQuery(), conn.isDistributedJoins());

        try {
            JdbcQueryTask.QueryResult res =
                loc ? qryTask.call() : ignite.compute(ignite.cluster().forNodeId(nodeId)).call(qryTask);

            JdbcResultSet rs = new JdbcResultSet(uuid, this, res.getTbls(), res.getCols(), res.getTypes(),
                res.getRows(), res.isFinished());

            rs.setFetchSize(fetchSize);

            resSets.add(rs);

            return rs;
        }
        catch (IgniteSQLException e) {
            throw e.toJdbcException();
        }
        catch (Exception e) {
            throw new SQLException("Failed to query Ignite.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate(String sql) throws SQLException {
        ensureNotClosed();

        rs = null;

        updateCnt = -1;

        return doUpdate(sql, getArgs());
    }

    /**
     * Run update query.
     * @param sql SQL query.
     * @param args Update arguments.
     * @return Number of affected items.
     * @throws SQLException
     */
    int doUpdate(String sql, Object[] args) throws SQLException {
        if (F.isEmpty(sql))
            throw new SQLException("SQL query is empty");

        Ignite ignite = conn.ignite();

        UUID nodeId = conn.nodeId();

        UUID uuid = UUID.randomUUID();

        boolean loc = nodeId == null;

        if (!conn.isDmlSupported())
            throw new SQLException("Failed to query Ignite: DML operations are supported in versions 1.8.0 and newer");

        JdbcQueryTaskV2 qryTask = new JdbcQueryTaskV2(loc ? ignite : null, conn.cacheName(), sql, false, loc, args,
            fetchSize, uuid, conn.isLocalQuery(), conn.isCollocatedQuery(), conn.isDistributedJoins());

        try {
            JdbcQueryTaskV2.QueryResult qryRes =
                loc ? qryTask.call() : ignite.compute(ignite.cluster().forNodeId(nodeId)).call(qryTask);

            Long res = updateCounterFromQueryResult(qryRes.getRows());

            updateCnt = res;

            return res.intValue();
        }
        catch (IgniteSQLException e) {
            throw e.toJdbcException();
        }
        catch (SQLException e) {
            throw e;
        }
        catch (Exception e) {
            throw new SQLException("Failed to query Ignite.", e);
        }
    }

    /**
     * @param rows query result.
     * @return update counter, if found
     * @throws SQLException if getting an update counter from result proved to be impossible.
     */
    private static Long updateCounterFromQueryResult(List<List<?>> rows) throws SQLException {
         if (F.isEmpty(rows))
            return 0L;

        if (rows.size() != 1)
            throw new SQLException("Expected number of rows of 1 for update operation");

        List<?> row = rows.get(0);

        if (row.size() != 1)
            throw new SQLException("Expected row size of 1 for update operation");

        Object objRes = row.get(0);

        if (!(objRes instanceof Long))
            throw new SQLException("Unexpected update result type");

        return (Long) objRes;
    }

    /** {@inheritDoc} */
    @Override public void close() throws SQLException {
        conn.statements.remove(this);

        closeInternal();
    }

    /**
     * Marks statement as closed and closes all result sets.
     */
    void closeInternal() throws SQLException {
        for (Iterator<JdbcResultSet> it = resSets.iterator(); it.hasNext(); ) {
            JdbcResultSet rs = it.next();

            rs.closeInternal();

            it.remove();
        }

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

        throw new SQLFeatureNotSupportedException("Query timeout is not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setQueryTimeout(int timeout) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Query timeout is not supported.");
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
        if (!conn.isDmlSupported()) {
            // We attempt to run a query without any checks as long as server does not support DML anyway,
            // so it simply will throw an exception when given a DML statement instead of a query.
            rs = executeQuery(sql);

            return true;
        }

        ensureNotClosed();

        rs = null;

        updateCnt = -1;

        if (F.isEmpty(sql))
            throw new SQLException("SQL query is empty");

        Ignite ignite = conn.ignite();

        UUID nodeId = conn.nodeId();

        UUID uuid = UUID.randomUUID();

        boolean loc = nodeId == null;

        JdbcQueryTaskV2 qryTask = new JdbcQueryTaskV2(loc ? ignite : null, conn.cacheName(), sql, null, loc, getArgs(),
            fetchSize, uuid, conn.isLocalQuery(), conn.isCollocatedQuery(), conn.isDistributedJoins());

        try {
            JdbcQueryTaskV2.QueryResult res =
                loc ? qryTask.call() : ignite.compute(ignite.cluster().forNodeId(nodeId)).call(qryTask);

            if (res.isQuery()) {
                JdbcResultSet rs = JdbcResultSet.resultSetForQueryTaskV2(uuid, this, res.getTbls(), res.getCols(),
                    res.getTypes(), res.getRows(), res.isFinished());

                rs.setFetchSize(fetchSize);

                resSets.add(rs);

                this.rs = rs;
            }
            else
                updateCnt = updateCounterFromQueryResult(res.getRows());

            return res.isQuery();
        }
        catch (IgniteSQLException e) {
            throw e.toJdbcException();
        }
        catch (Exception e) {
            throw new SQLException("Failed to query Ignite.", e);
        }
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

        long res = updateCnt;

        updateCnt = -1;

        return Long.valueOf(res).intValue();
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

        if (fetchSize < 0)
            throw new SQLException("Fetch size must be greater or equal zero.");

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

        if (F.isEmpty(sql))
            throw new SQLException("SQL query is empty");

        if (batch == null)
            batch = new ArrayList<>();

        batch.add(sql);
    }

    /** {@inheritDoc} */
    @Override public void clearBatch() throws SQLException {
        ensureNotClosed();

        batch = null;
    }

    /** {@inheritDoc} */
    @Override public int[] executeBatch() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Batch statements are not supported yet.");
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

        throw new SQLFeatureNotSupportedException("Auto generated keys are not supported.");
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        ensureNotClosed();

        if (autoGeneratedKeys == RETURN_GENERATED_KEYS)
            throw new SQLFeatureNotSupportedException("Auto generated keys are not supported.");

        return executeUpdate(sql);
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate(String sql, int[] colIndexes) throws SQLException {
        ensureNotClosed();

        if (!F.isEmpty(colIndexes))
            throw new SQLFeatureNotSupportedException("Auto generated keys are not supported.");

        return executeUpdate(sql);
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate(String sql, String[] colNames) throws SQLException {
        ensureNotClosed();

        if (!F.isEmpty(colNames))
            throw new SQLFeatureNotSupportedException("Auto generated keys are not supported.");

        return executeUpdate(sql);
    }

    /** {@inheritDoc} */
    @Override public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        ensureNotClosed();

        if (autoGeneratedKeys == RETURN_GENERATED_KEYS)
            throw new SQLFeatureNotSupportedException("Auto generated keys are not supported.");

        return execute(sql);
    }

    /** {@inheritDoc} */
    @Override public boolean execute(String sql, int[] colIndexes) throws SQLException {
        ensureNotClosed();

        if (!F.isEmpty(colIndexes))
            throw new SQLFeatureNotSupportedException("Auto generated keys are not supported.");

        return execute(sql);
    }

    /** {@inheritDoc} */
    @Override public boolean execute(String sql, String[] colNames) throws SQLException {
        ensureNotClosed();

        if (!F.isEmpty(colNames))
            throw new SQLFeatureNotSupportedException("Auto generated keys are not supported.");

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
    @SuppressWarnings("unchecked")
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
     * @return Args for current statement
     */
    protected final Object[] getArgs() {
        return args != null ? args.toArray() : null;
    }

    /**
     * Ensures that statement is not closed.
     *
     * @throws SQLException If statement is closed.
     */
    void ensureNotClosed() throws SQLException {
        if (closed)
            throw new SQLException("Statement is closed.");
    }
}
