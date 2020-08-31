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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.util.typedef.F;

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.FETCH_FORWARD;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.apache.ignite.internal.jdbc2.JdbcUtils.convertToSqlException;

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

    /** Query arguments. */
    protected ArrayList<Object> args;

    /** Fetch size. */
    private int fetchSize = DFLT_FETCH_SIZE;

    /** Result sets. */
    final Set<JdbcResultSet> resSets = new HashSet<>();

    /** Fields indexes. */
    Map<String, Integer> fieldsIdxs = new HashMap<>();

    /** Batch of statements. */
    private List<String> batch;

    /** Results. */
    protected List<JdbcResultSet> results;

    /** Current result set index. */
    protected int curRes = 0;

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
        execute0(sql, true);

        return getResultSet();
    }

    /**
     * @param sql SQL query.
     * @param isQuery Expected type of statements are contained in the query.
     * @throws SQLException On error.
     */
    private void executeMultipleStatement(String sql, Boolean isQuery) throws SQLException {
        ensureNotClosed();

        closeResults();

        if (F.isEmpty(sql))
            throw new SQLException("SQL query is empty");

        Ignite ignite = conn.ignite();

        UUID nodeId = conn.nodeId();

        boolean loc = nodeId == null;
        JdbcQueryMultipleStatementsTask qryTask;

        if (!conn.isMultipleStatementsAllowed() && conn.isMultipleStatementsTaskV2Supported()) {
            qryTask = new JdbcQueryMultipleStatementsNotAllowTask(loc ? ignite : null, conn.schemaName(),
                sql, isQuery, loc, getArgs(), fetchSize, conn.isLocalQuery(), conn.isCollocatedQuery(),
                conn.isDistributedJoins(), conn.isEnforceJoinOrder(), conn.isLazy());
        }
        else {
            qryTask = new JdbcQueryMultipleStatementsTask(loc ? ignite : null, conn.schemaName(),
                sql, isQuery, loc, getArgs(), fetchSize, conn.isLocalQuery(), conn.isCollocatedQuery(),
                conn.isDistributedJoins(), conn.isEnforceJoinOrder(), conn.isLazy());
        }

        try {
            List<JdbcStatementResultInfo> rsInfos =
                loc ? qryTask.call() : ignite.compute(ignite.cluster().forNodeId(nodeId)).call(qryTask);

            results = new ArrayList<>(rsInfos.size());

            for (JdbcStatementResultInfo rsInfo : rsInfos) {
                if (rsInfo.isQuery())
                    results.add(new JdbcResultSet(true, rsInfo.queryId(), this, null, null, null, null, false));
                else
                    results.add(new JdbcResultSet(this, rsInfo.updateCount()));
            }
        }
        catch (Exception e) {
            throw convertToSqlException(e, "Failed to query Ignite.");
        }
    }

    /**
     * @param sql SQL query.
     * @param isQuery Expected type of statements are contained in the query.
     * @throws SQLException On error.
     */
    private void executeSingle(String sql, Boolean isQuery) throws SQLException {
        ensureNotClosed();

        Ignite ignite = conn.ignite();

        UUID nodeId = conn.nodeId();

        UUID uuid = UUID.randomUUID();

        boolean loc = nodeId == null;

        if (!conn.isDmlSupported())
            if (isQuery != null && !isQuery)
                throw new SQLException("Failed to query Ignite: DML operations are supported in versions 1.8.0 and newer");
            else
                isQuery = true;

        JdbcQueryTask qryTask = JdbcQueryTaskV3.createTask(loc ? ignite : null, conn.cacheName(), conn.schemaName(),
            sql, isQuery, loc, getArgs(), fetchSize, uuid, conn.isLocalQuery(), conn.isCollocatedQuery(),
            conn.isDistributedJoins(), conn.isEnforceJoinOrder(), conn.isLazy(), false, conn.skipReducerOnUpdate());

        try {
            JdbcQueryTaskResult qryRes =
                loc ? qryTask.call() : ignite.compute(ignite.cluster().forNodeId(nodeId)).call(qryTask);

            JdbcResultSet rs = new JdbcResultSet(qryRes.isQuery(), uuid, this, qryRes.getTbls(), qryRes.getCols(),
                qryRes.getTypes(), qryRes.getRows(), qryRes.isFinished());

            rs.setFetchSize(fetchSize);

            results = Collections.singletonList(rs);
            curRes = 0;
        }
        catch (Exception e) {
            throw convertToSqlException(e, "Failed to query Ignite.");
        }

    }

    /**
     * Sets parameters to query object. Logic should be consistent with {@link JdbcQueryTask#call()} and {@link
     * JdbcQueryMultipleStatementsTask#call()}. Parameters are determined from context: connection state and this
     * statement state.
     *
     * @param qry query which parameters to set up
     */
    protected void setupQuery(SqlFieldsQuery qry) {
        qry.setPageSize(fetchSize);
        qry.setLocal(conn.nodeId() == null);
        qry.setCollocated(conn.isCollocatedQuery());
        qry.setDistributedJoins(conn.isDistributedJoins());
        qry.setEnforceJoinOrder(conn.isEnforceJoinOrder());
        qry.setLazy(conn.isLazy());
        qry.setSchema(conn.schemaName());
    }

    /**
     * @param sql SQL query.
     * @param isQuery Expected type of statements are contained in the query.
     * @throws SQLException On error.
     */
    protected void execute0(String sql, Boolean isQuery) throws SQLException {
        if (conn.isMultipleStatementsSupported())
            executeMultipleStatement(sql, isQuery);
        else
            executeSingle(sql, isQuery);
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate(String sql) throws SQLException {
        execute0(sql, false);

        return getUpdateCount();
    }

    /** {@inheritDoc} */
    @Override public void close() throws SQLException {
        conn.statements.remove(this);

        closeInternal();
    }

    /**
     * Marks statement as closed and closes all result sets.
     * @throws SQLException On error.
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
        execute0(sql, null);

        return results.get(0).isQuery();
    }

    /** {@inheritDoc} */
    @Override public ResultSet getResultSet() throws SQLException {
        JdbcResultSet rs = nextResultSet();

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
        JdbcResultSet rs = nextResultSet();

        if (rs == null)
            return -1;

        if (rs.isQuery()) {
            curRes--;

            return -1;
        }

        return (int)rs.updateCount();
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

        List<String> batch = this.batch;

        this.batch = null;

        return doBatchUpdate(null, batch, null);
    }

    /**
     * Runs batch of update commands.
     *
     * @param command SQL command.
     * @param batch Batch of SQL commands.
     * @param batchArgs Batch of SQL parameters.
     * @return Number of affected rows.
     * @throws SQLException If failed.
     */
    protected int[] doBatchUpdate(String command, List<String> batch, List<List<Object>> batchArgs)
        throws SQLException {

        closeResults();

        if ((F.isEmpty(command) || F.isEmpty(batchArgs)) && F.isEmpty(batch))
            throw new SQLException("Batch is empty.");

        Ignite ignite = conn.ignite();

        UUID nodeId = conn.nodeId();

        boolean loc = nodeId == null;

        if (!conn.isDmlSupported())
            throw new SQLException("Failed to query Ignite: DML operations are supported in versions 1.8.0 and newer");

        JdbcBatchUpdateTask task = new JdbcBatchUpdateTask(loc ? ignite : null, conn.cacheName(),
            conn.schemaName(), command, batch, batchArgs, loc, getFetchSize(), conn.isLocalQuery(),
            conn.isCollocatedQuery(), conn.isDistributedJoins());

        try {
            int[] res = loc ? task.call() : ignite.compute(ignite.cluster().forNodeId(nodeId)).call(task);

            long updateCnt = F.isEmpty(res) ? -1 : res[res.length - 1];

            results = Collections.singletonList(new JdbcResultSet(this, updateCnt));

            curRes = 0;

            return res;
        }
        catch (Exception e) {
            throw convertToSqlException(e, "Failed to query Ignite.");
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

        if (results != null) {
            assert curRes <= results.size() : "Invalid results state: [resultsCount=" + results.size() +
                ", curRes=" + curRes + ']';

            switch (curr) {
                case CLOSE_CURRENT_RESULT:
                    if (curRes > 0)
                        results.get(curRes - 1).close();

                    break;

                case CLOSE_ALL_RESULTS:
                    for (int i = 0; i < curRes; ++i)
                        results.get(i).close();

                    break;

                case KEEP_CURRENT_RESULT:
                    break;

                default:
                    throw new SQLException("Invalid 'current' parameter.");
            }
        }

        return (results != null && curRes < results.size());
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
            throw new SQLException("Connection is closed.", SqlStateCode.CONNECTION_CLOSED);
    }

    /**
     * Get last result set if any.
     *
     * @return Result set or null.
     * @throws SQLException If failed.
     */
    private JdbcResultSet nextResultSet() throws SQLException {
        ensureNotClosed();

        if (results == null || curRes >= results.size())
            return null;
        else
            return results.get(curRes++);
    }

    /**
     * Close results.
     *
     * @throws SQLException On error.
     */
    private void closeResults() throws SQLException {
        if (results != null) {
            for (JdbcResultSet rs : results)
                rs.close();

            results = null;
            curRes = 0;
        }
    }

}
