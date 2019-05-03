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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLPermission;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.internal.jdbc2.JdbcUtils;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadBatchRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcCachePartitionsRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcCachePartitionsResult;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcOrderedBatchExecuteRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcOrderedBatchExecuteResult;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQuery;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryCancelRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryExecuteRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryExecuteResult;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcResponse;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcResultWithIo;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcStatementType;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.sql.command.SqlSetStreamingCommand;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionClientContext;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionResult;
import org.apache.ignite.internal.util.HostAndPortRange;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

/**
 * JDBC connection implementation.
 *
 * See documentation of {@link org.apache.ignite.IgniteJdbcThinDriver} for details.
 */
public class JdbcThinConnection implements Connection {
    /** Logger. */
    private static final Logger LOG = Logger.getLogger(JdbcThinConnection.class.getName());

    /** Request timeout period. */
    private static final int REQUEST_TIMEOUT_PERIOD = 1_000;

    /** Network timeout permission */
    private static final String SET_NETWORK_TIMEOUT_PERM = "setNetworkTimeout";

    /** Zero timeout as query timeout means no timeout. */
    static final int NO_TIMEOUT = 0;

    /** Index generator. */
    private static final AtomicLong IDX_GEN = new AtomicLong();

    /** Affinity awareness enabled flag. */
    private final boolean affinityAwareness;

    /** Statements modification mutex. */
    private final Object stmtsMux = new Object();

    /** Schema name. */
    private String schema;

    /** Closed flag. */
    private volatile boolean closed;

    /** Current transaction isolation. */
    private int txIsolation;

    /** Auto-commit flag. */
    private boolean autoCommit;

    /** Read-only flag. */
    private boolean readOnly;

    /** Streaming flag. */
    private volatile StreamState streamState;

    /** Current transaction holdability. */
    private int holdability;

    /** Jdbc metadata. Cache the JDBC object on the first access */
    private JdbcThinDatabaseMetadata metadata;

    /** Connection properties. */
    private final ConnectionProperties connProps;

    /** Connected. */
    private volatile boolean connected;

    /** Tracked statements to close on disconnect. */
    private final Set<JdbcThinStatement> stmts = Collections.newSetFromMap(new IdentityHashMap<>());

    /** Query timeout timer */
    private final Timer timer;

    /** Affinity cache. */
    private AffinityCache affinityCache;

    /** Ignite endpoint. */
    private volatile JdbcThinTcpIo singleIo;

    /** Node Ids tp ignite endpoints. */
    private final Map<UUID, JdbcThinTcpIo> ios = new ConcurrentHashMap<>();

    /** Ignite endpoints to use for better performance in case of random access. */
    private JdbcThinTcpIo[] iosArr;

    /** Server index. */
    private int srvIdx;

    /** Ignite server version. */
    private Thread ownThread;

    /** Mutex. */
    private final Object mux = new Object();

    /** Ignite endpoint to use within transactional context. */
    private volatile JdbcThinTcpIo txIo;

    /** Random generator. */
    private static final Random RND = new Random(System.currentTimeMillis());

    /** Network timeout. */
    private int netTimeout;

    /**
     * Creates new connection.
     *
     * @param connProps Connection properties.
     * @throws SQLException In case Ignite client failed to start.
     */
    public JdbcThinConnection(ConnectionProperties connProps) throws SQLException {
        this.connProps = connProps;

        holdability = HOLD_CURSORS_OVER_COMMIT;
        autoCommit = true;
        txIsolation = Connection.TRANSACTION_NONE;

        schema = JdbcUtils.normalizeSchema(connProps.getSchema());

        timer = new Timer("query-timeout-timer");

        affinityAwareness = connProps.isAffinityAwareness();

        ensureConnected();
    }

    /**
     * @throws SQLException On connection error.
     */
    private void ensureConnected() throws SQLException {
        if (connected)
            return;

        assert !closed;

        assert ios.isEmpty();

        assert iosArr == null;

        HostAndPortRange[] srvs = connProps.getAddresses();

        if (affinityAwareness)
            connectInAffinityAwarenessMode(srvs);
        else
            connectInCommonMode(srvs);
    }

    /**
     * @return Whether this connection is streamed or not.
     */
    boolean isStream() {
        return streamState != null;
    }

    /**
     * @param sql Statement.
     * @param cmd Parsed form of {@code sql}.
     * @param stmt Jdbc thin statement.
     * @throws SQLException if failed.
     */
    void executeNative(String sql, SqlCommand cmd, JdbcThinStatement stmt) throws SQLException {
        if (cmd instanceof SqlSetStreamingCommand) {
            SqlSetStreamingCommand cmd0 = (SqlSetStreamingCommand)cmd;

            // If streaming is already on, we have to close it first.
            if (streamState != null) {
                streamState.close();

                streamState = null;
            }

            boolean newVal = ((SqlSetStreamingCommand)cmd).isTurnOn();

            ensureConnected();

            JdbcThinTcpIo cliIo = cliIo(null);

            // Actual ON, if needed.
            if (newVal) {
                if (!cmd0.isOrdered() && !cliIo.isUnorderedStreamSupported()) {
                    throw new SQLException("Streaming without order doesn't supported by server [remoteNodeVer="
                        + cliIo.igniteVersion() + ']', SqlStateCode.INTERNAL_ERROR);
                }

                streamState = new StreamState((SqlSetStreamingCommand)cmd, cliIo);

                sendRequest(new JdbcQueryExecuteRequest(JdbcStatementType.ANY_STATEMENT_TYPE,
                    schema, 1, 1, autoCommit, sql, null), stmt, cliIo);

                streamState.start();
            }
        }
        else
            throw IgniteQueryErrorCode.createJdbcSqlException("Unsupported native statement: " + sql,
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /**
     * Add another query for batched execution.
     *
     * @param sql Query.
     * @param args Arguments.
     * @throws SQLException On error.
     */
    void addBatch(String sql, List<Object> args) throws SQLException {
        assert isStream();

        streamState.addBatch(sql, args);
    }

    /** {@inheritDoc} */
    @Override public Statement createStatement() throws SQLException {
        return createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT);
    }

    /** {@inheritDoc} */
    @Override public Statement createStatement(int resSetType, int resSetConcurrency) throws SQLException {
        return createStatement(resSetType, resSetConcurrency, HOLD_CURSORS_OVER_COMMIT);
    }

    /** {@inheritDoc} */
    @Override public Statement createStatement(int resSetType, int resSetConcurrency,
        int resSetHoldability) throws SQLException {
        ensureNotClosed();

        checkCursorOptions(resSetType, resSetConcurrency);

        JdbcThinStatement stmt = new JdbcThinStatement(this, resSetHoldability, schema);

        synchronized (stmtsMux) {
            stmts.add(stmt);
        }

        return stmt;
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT);
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement prepareStatement(String sql, int resSetType,
        int resSetConcurrency) throws SQLException {
        return prepareStatement(sql, resSetType, resSetConcurrency, HOLD_CURSORS_OVER_COMMIT);
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement prepareStatement(String sql, int resSetType, int resSetConcurrency,
        int resSetHoldability) throws SQLException {
        ensureNotClosed();

        checkCursorOptions(resSetType, resSetConcurrency);

        if (sql == null)
            throw new SQLException("SQL string cannot be null.");

        JdbcThinPreparedStatement stmt = new JdbcThinPreparedStatement(this, sql, resSetHoldability, schema);

        synchronized (stmtsMux) {
            stmts.add(stmt);
        }

        return stmt;
    }

    /**
     * @param resSetType Cursor option.
     * @param resSetConcurrency Cursor option.
     * @throws SQLException If options unsupported.
     */
    private void checkCursorOptions(int resSetType, int resSetConcurrency) throws SQLException {
        if (resSetType != TYPE_FORWARD_ONLY)
            throw new SQLFeatureNotSupportedException("Invalid result set type (only forward is supported).");

        if (resSetConcurrency != CONCUR_READ_ONLY)
            throw new SQLFeatureNotSupportedException("Invalid concurrency (updates are not supported).");
    }

    /** {@inheritDoc} */
    @Override public CallableStatement prepareCall(String sql) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Callable functions are not supported.");
    }

    /** {@inheritDoc} */
    @Override public CallableStatement prepareCall(String sql, int resSetType, int resSetConcurrency)
        throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Callable functions are not supported.");
    }

    /** {@inheritDoc} */
    @Override public String nativeSQL(String sql) throws SQLException {
        ensureNotClosed();

        if (sql == null)
            throw new SQLException("SQL string cannot be null.");

        return sql;
    }

    /** {@inheritDoc} */
    @Override public void setAutoCommit(boolean autoCommit) throws SQLException {
        ensureNotClosed();

        // Do nothing if resulting value doesn't actually change.
        if (autoCommit != this.autoCommit) {
            doCommit();

            this.autoCommit = autoCommit;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean getAutoCommit() throws SQLException {
        ensureNotClosed();

        return autoCommit;
    }

    /** {@inheritDoc} */
    @Override public void commit() throws SQLException {
        ensureNotClosed();

        if (autoCommit)
            throw new SQLException("Transaction cannot be committed explicitly in auto-commit mode.");

        doCommit();
    }

    /** {@inheritDoc} */
    @Override public void rollback() throws SQLException {
        ensureNotClosed();

        if (autoCommit)
            throw new SQLException("Transaction cannot be rolled back explicitly in auto-commit mode.");

        try (Statement s = createStatement()) {
            s.execute("ROLLBACK");
        }
    }

    /**
     * Send to the server {@code COMMIT} command.
     *
     * @throws SQLException if failed.
     */
    private void doCommit() throws SQLException {
        try (Statement s = createStatement()) {
            s.execute("COMMIT");
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws SQLException {
        if (isClosed())
            return;

        if (streamState != null) {
            streamState.close();

            streamState = null;
        }

        synchronized (stmtsMux) {
            stmts.clear();
        }

        SQLException err = null;

        closed = true;

        if (affinityAwareness) {
            for (JdbcThinTcpIo clioIo : ios.values())
                clioIo.close();

            ios.clear();

            iosArr = null;
        }
        else {
            if (singleIo != null)
                singleIo.close();
        }

        timer.cancel();

        if (err != null)
            throw err;
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        return closed;
    }

    /** {@inheritDoc} */
    @Override public DatabaseMetaData getMetaData() throws SQLException {
        ensureNotClosed();

        if (metadata == null)
            metadata = new JdbcThinDatabaseMetadata(this);

        return metadata;
    }

    /** {@inheritDoc} */
    @Override public void setReadOnly(boolean readOnly) throws SQLException {
        ensureNotClosed();

        this.readOnly = readOnly;
    }

    /** {@inheritDoc} */
    @Override public boolean isReadOnly() throws SQLException {
        ensureNotClosed();

        return readOnly;
    }

    /** {@inheritDoc} */
    @Override public void setCatalog(String catalog) throws SQLException {
        ensureNotClosed();
    }

    /** {@inheritDoc} */
    @Override public String getCatalog() throws SQLException {
        ensureNotClosed();

        return null;
    }

    /** {@inheritDoc} */
    @Override public void setTransactionIsolation(int level) throws SQLException {
        ensureNotClosed();

        switch (level) {
            case Connection.TRANSACTION_READ_UNCOMMITTED:
            case Connection.TRANSACTION_READ_COMMITTED:
            case Connection.TRANSACTION_REPEATABLE_READ:
            case Connection.TRANSACTION_SERIALIZABLE:
            case Connection.TRANSACTION_NONE:
                break;

            default:
                throw new SQLException("Invalid transaction isolation level.", SqlStateCode.INVALID_TRANSACTION_LEVEL);
        }

        txIsolation = level;
    }

    /** {@inheritDoc} */
    @Override public int getTransactionIsolation() throws SQLException {
        ensureNotClosed();

        return txIsolation;
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
    @Override public Map<String, Class<?>> getTypeMap() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Types mapping is not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Types mapping is not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setHoldability(int holdability) throws SQLException {
        ensureNotClosed();

        if (holdability != HOLD_CURSORS_OVER_COMMIT && holdability != CLOSE_CURSORS_AT_COMMIT)
            throw new SQLException("Invalid result set holdability value.");

        this.holdability = holdability;
    }

    /** {@inheritDoc} */
    @Override public int getHoldability() throws SQLException {
        ensureNotClosed();

        return holdability;
    }

    /** {@inheritDoc} */
    @Override public Savepoint setSavepoint() throws SQLException {
        ensureNotClosed();

        if (autoCommit)
            throw new SQLException("Savepoint cannot be set in auto-commit mode.");

        throw new SQLFeatureNotSupportedException("Savepoints are not supported.");
    }

    /** {@inheritDoc} */
    @Override public Savepoint setSavepoint(String name) throws SQLException {
        ensureNotClosed();

        if (name == null)
            throw new SQLException("Savepoint name cannot be null.");

        if (autoCommit)
            throw new SQLException("Savepoint cannot be set in auto-commit mode.");

        throw new SQLFeatureNotSupportedException("Savepoints are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void rollback(Savepoint savepoint) throws SQLException {
        ensureNotClosed();

        if (savepoint == null)
            throw new SQLException("Invalid savepoint.");

        if (autoCommit)
            throw new SQLException("Auto-commit mode.");

        throw new SQLFeatureNotSupportedException("Savepoints are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        ensureNotClosed();

        if (savepoint == null)
            throw new SQLException("Savepoint cannot be null.");

        throw new SQLFeatureNotSupportedException("Savepoints are not supported.");
    }

    /** {@inheritDoc} */
    @Override public CallableStatement prepareCall(String sql, int resSetType, int resSetConcurrency,
        int resSetHoldability) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Callable functions are not supported.");
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Auto generated keys are not supported.");
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement prepareStatement(String sql, int[] colIndexes) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Auto generated keys are not supported.");
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement prepareStatement(String sql, String[] colNames) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Auto generated keys are not supported.");
    }

    /** {@inheritDoc} */
    @Override public Clob createClob() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public Blob createBlob() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public NClob createNClob() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public SQLXML createSQLXML() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0)
            throw new SQLException("Invalid timeout: " + timeout);

        return !closed;
    }

    /** {@inheritDoc} */
    @Override public void setClientInfo(String name, String val) throws SQLClientInfoException {
        if (closed)
            throw new SQLClientInfoException("Connection is closed.", null);
    }

    /** {@inheritDoc} */
    @Override public void setClientInfo(Properties props) throws SQLClientInfoException {
        if (closed)
            throw new SQLClientInfoException("Connection is closed.", null);
    }

    /** {@inheritDoc} */
    @Override public String getClientInfo(String name) throws SQLException {
        ensureNotClosed();

        return null;
    }

    /** {@inheritDoc} */
    @Override public Properties getClientInfo() throws SQLException {
        ensureNotClosed();

        return new Properties();
    }

    /** {@inheritDoc} */
    @Override public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        ensureNotClosed();

        if (typeName == null)
            throw new SQLException("Type name cannot be null.");

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public Struct createStruct(String typeName, Object[] attrs) throws SQLException {
        ensureNotClosed();

        if (typeName == null)
            throw new SQLException("Type name cannot be null.");

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(iface))
            throw new SQLException("Connection is not a wrapper for " + iface.getName());

        return (T)this;
    }

    /** {@inheritDoc} */
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(JdbcThinConnection.class);
    }

    /** {@inheritDoc} */
    @Override public void setSchema(String schema) throws SQLException {
        ensureNotClosed();

        this.schema = JdbcUtils.normalizeSchema(schema);
    }

    /** {@inheritDoc} */
    @Override public String getSchema() throws SQLException {
        ensureNotClosed();

        return schema;
    }

    /** {@inheritDoc} */
    @Override public void abort(Executor executor) throws SQLException {
        if (executor == null)
            throw new SQLException("Executor cannot be null.");

        close();
    }

    /** {@inheritDoc} */
    @Override public void setNetworkTimeout(Executor executor, int ms) throws SQLException {
        ensureNotClosed();

        if (ms < 0)
            throw new SQLException("Network timeout cannot be negative.");

        SecurityManager secMgr = System.getSecurityManager();

        if (secMgr != null)
            secMgr.checkPermission(new SQLPermission(SET_NETWORK_TIMEOUT_PERM));

        netTimeout = ms;

        if (affinityAwareness) {
            for (JdbcThinTcpIo clioIo : ios.values())
                clioIo.timeout(ms);
        }
        else
            singleIo.timeout(ms);
    }

    /** {@inheritDoc} */
    @Override public int getNetworkTimeout() throws SQLException {
        ensureNotClosed();

        return netTimeout;
    }

    /**
     * Ensures that connection is not closed.
     *
     * @throws SQLException If connection is closed.
     */
    public void ensureNotClosed() throws SQLException {
        if (closed)
            throw new SQLException("Connection is closed.", SqlStateCode.CONNECTION_CLOSED);
    }

    /**
     * @return Ignite server version.
     */
    IgniteProductVersion igniteVersion() {
        // TODO: IGNITE-11321: JDBC Thin: implement nodes multi version support.
        return cliIo(null).igniteVersion();
    }

    /**
     * @return Auto close server cursors flag.
     */
    boolean autoCloseServerCursor() {
        return connProps.isAutoCloseServerCursor();
    }

    /**
     * Send request for execution via corresponding singleIo from {@link #ios}.
     *
     * @param req Request.
     * @return Server response.
     * @throws SQLException On any error.
     */
    JdbcResultWithIo sendRequest(JdbcRequest req) throws SQLException {
        return sendRequest(req, null, null);
    }

    /**
     * Send request for execution via corresponding singleIo from {@link #ios} or sticky singleIo.
     *
     * @param req Request.
     * @param stmt Jdbc thin statement.
     * @param stickyIo Sticky ignite endpoint.
     * @return Server response.
     * @throws SQLException On any error.
     */
    JdbcResultWithIo sendRequest(JdbcRequest req, JdbcThinStatement stmt, @Nullable JdbcThinTcpIo stickyIo)
        throws SQLException {
        ensureConnected();

        RequestTimeoutTimerTask reqTimeoutTimerTask = null;

        synchronized (mux) {
            if (ownThread != null) {
                throw new SQLException("Concurrent access to JDBC connection is not allowed"
                    + " [ownThread=" + ownThread.getName()
                    + ", curThread=" + Thread.currentThread().getName(), SqlStateCode.CONNECTION_FAILURE);
            }

            ownThread = Thread.currentThread();
        }
        try {
            try {
                JdbcThinTcpIo cliIo = stickyIo == null ? cliIo(calculateNodeIds(req)) : stickyIo;

                if (stmt != null && stmt.requestTimeout() != NO_TIMEOUT) {
                    reqTimeoutTimerTask = new RequestTimeoutTimerTask(
                        req instanceof JdbcBulkLoadBatchRequest ? stmt.currentRequestId() : req.requestId(),
                        cliIo,
                        stmt.requestTimeout());

                    timer.schedule(reqTimeoutTimerTask, 0, REQUEST_TIMEOUT_PERIOD);
                }

                JdbcQueryExecuteRequest qryReq = null;

                if (req instanceof JdbcQueryExecuteRequest)
                    qryReq = (JdbcQueryExecuteRequest)req;

                JdbcResponse res = cliIo.sendRequest(req, stmt);

                txIo = res.activeTransaction() ? cliIo : null;

                if (res.status() == IgniteQueryErrorCode.QUERY_CANCELED && stmt != null &&
                    stmt.requestTimeout() != NO_TIMEOUT && reqTimeoutTimerTask != null && reqTimeoutTimerTask.expired.get()) {

                    throw new SQLTimeoutException(QueryCancelledException.ERR_MSG, SqlStateCode.QUERY_CANCELLED,
                        IgniteQueryErrorCode.QUERY_CANCELED);
                }
                else if (res.status() != ClientListenerResponse.STATUS_SUCCESS)
                    throw new SQLException(res.error(), IgniteQueryErrorCode.codeToSqlState(res.status()), res.status());

                updateAffinityCache(qryReq, res);

                return new JdbcResultWithIo(res.response(), cliIo);
            }
            catch (SQLException e) {
                throw e;
            }
            catch (Exception e) {
                onDisconnect();

                if (e instanceof SocketTimeoutException)
                    throw new SQLException("Connection timed out.", SqlStateCode.CONNECTION_FAILURE, e);
                else
                    throw new SQLException("Failed to communicate with Ignite cluster.", SqlStateCode.CONNECTION_FAILURE, e);
            }
            finally {
                if (stmt != null && stmt.requestTimeout() != NO_TIMEOUT && reqTimeoutTimerTask != null)
                    reqTimeoutTimerTask.cancel();
            }
        }
        finally {
            synchronized (mux) {
                ownThread = null;
            }
        }
    }

    /**
     * Calculate node UUIDs.
     *
     * @param req Jdbc request for which we'll try to calculate node id.
     * @return node UUID or null if failed to calculate.
     * @throws IOException If Exception occured during the network partiton destribution retrieval.
     * @throws SQLException If Failed to calculate derived partitions.
     */
    @Nullable private List<UUID> calculateNodeIds(JdbcRequest req) throws IOException, SQLException {
        if (!affinityAwareness || !(req instanceof JdbcQueryExecuteRequest))
            return null;

        JdbcQueryExecuteRequest qry = (JdbcQueryExecuteRequest)req;

        if (affinityCache == null) {
            qry.partitionResponseRequest(true);

            return null;
        }

        JdbcThinPartitionResultDescriptor partResDesc = affinityCache.partitionResult(
            new QualifiedSQLQuery(qry.schemaName(), qry.sqlQuery()));

        // Value is empty.
        if (partResDesc == JdbcThinPartitionResultDescriptor.EMPTY_DESCRIPTOR)
            return null;

        // Key is missing.
        if (partResDesc == null) {
            qry.partitionResponseRequest(true);

            return null;
        }

        Collection<Integer> parts = calculatePartitions(partResDesc, qry.arguments());

        if (parts == null || parts.isEmpty())
            return null;

        UUID[] cacheDistr = retrieveCacheDistribution(partResDesc.cacheId(),
            partResDesc.partitionResult().partitionsCount());

        if (parts.size() == 1)
            return Collections.singletonList(cacheDistr[parts.iterator().next()]);
        else {
            List<UUID> affinityAwarenessNodeIds = new ArrayList<>();

            for (int part : parts)
                affinityAwarenessNodeIds.add(cacheDistr[part]);

            return affinityAwarenessNodeIds;
        }
    }

    /**
     * Retrieve cache destribution for specified cache Id.
     *
     * @param cacheId Cache Id.
     * @param partCnt Partitons count.
     * @return Partitions cache distribution.
     * @throws IOException If Exception occured during the network partiton destribution retrieval.
     */
    private UUID[] retrieveCacheDistribution(int cacheId, int partCnt) throws IOException {
        UUID[] cacheDistr = affinityCache.cacheDistribution(cacheId);

        if (cacheDistr != null)
            return cacheDistr;

        JdbcResponse res;

        res = cliIo(null).sendRequest(new JdbcCachePartitionsRequest(Collections.singleton(cacheId)), null);

        assert res.status() == ClientListenerResponse.STATUS_SUCCESS;

        AffinityTopologyVersion resAffinityVer = res.affinityVersion();

        if (affinityCache.version().compareTo(resAffinityVer) < 0)
            affinityCache = new AffinityCache(resAffinityVer);
        else if (affinityCache.version().compareTo(resAffinityVer) > 0) {
            // Jdbc thin affinity cache is binded to the newer affinity topology version, so we should ignore retrieved
            // partition destribution. Given situation might occur in case of concurrent race and is not
            // possible in single-threaded jdbc thin client, so it's a reserve for the future.
            return null;
        }

        List<JdbcThinAffinityAwarenessMappingGroup> mappings =
            ((JdbcCachePartitionsResult)res.response()).getMappings();

        // Despite the fact that, at this moment, we request partition destribution only for one cache,
        // we might retrieve multiple caches but exactly with same distribution.
        assert mappings.size() == 1;

        JdbcThinAffinityAwarenessMappingGroup mappingGrp = mappings.get(0);

        cacheDistr = mappingGrp.revertMappings(partCnt);

        for (int mpCacheId : mappingGrp.cacheIds())
            affinityCache.addCacheDistribution(mpCacheId, cacheDistr);

        return cacheDistr;
    }

    /**
     * Calculate partitions for the query.
     *
     * @param partResDesc Partition result descriptor.
     * @param args Arguments.
     * @return Calculated partitions or {@code null} if failed to calculate and there should be a broadcast.
     * @throws SQLException If Failed to calculate derived partitions.
     */
    public static Collection<Integer> calculatePartitions(JdbcThinPartitionResultDescriptor partResDesc, Object[] args)
        throws SQLException {
        PartitionResult derivedParts = partResDesc.partitionResult();

        if (derivedParts != null) {
            try {
                return derivedParts.tree().apply(partResDesc.partitionClientContext(), args);
            }
            catch (IgniteCheckedException e) {
                throw new SQLException("Failed to calculate derived partitions for query.", SqlStateCode.INTERNAL_ERROR);
            }
        }

        return null;
    }

    /**
     * Send request for execution via corresponding singleIo from {@link #ios} or sticky singleIo.
     * Response is waited at the separate thread (see {@link StreamState#asyncRespReaderThread}).
     *
     * @param req Request.
     * @throws SQLException On any error.
     */
    void sendQueryCancelRequest(JdbcQueryCancelRequest req, JdbcThinTcpIo cliIo) throws SQLException {
        if (!connected)
            throw new SQLException("Failed to communicate with Ignite cluster.", SqlStateCode.CONNECTION_FAILURE);

        assert cliIo != null;

        try {
            cliIo.sendCancelRequest(req);
        }
        catch (Exception e) {
            throw new SQLException("Failed to communicate with Ignite cluster.", SqlStateCode.CONNECTION_FAILURE, e);
        }
    }

    /**
     * Send request for execution via corresponding singleIo from {@link #ios} or sticky singleIo.
     * Response is waited at the separate thread (see {@link StreamState#asyncRespReaderThread}).
     *
     * @param req Request.
     * @param stickyIO Sticky ignite endpoint.
     * @throws SQLException On any error.
     */
    private void sendRequestNotWaitResponse(JdbcOrderedBatchExecuteRequest req, JdbcThinTcpIo stickyIO) throws SQLException {
        ensureConnected();

        synchronized (mux) {
            if (ownThread != null) {
                throw new SQLException("Concurrent access to JDBC connection is not allowed"
                    + " [ownThread=" + ownThread.getName()
                    + ", curThread=" + Thread.currentThread().getName(), SqlStateCode.CONNECTION_FAILURE);
            }

            ownThread = Thread.currentThread();
        }

        try {
            stickyIO.sendBatchRequestNoWaitResponse(req);
        }
        catch (SQLException e) {
            throw e;
        }
        catch (Exception e) {
            onDisconnect();

            if (e instanceof SocketTimeoutException)
                throw new SQLException("Connection timed out.", SqlStateCode.CONNECTION_FAILURE, e);
            else
                throw new SQLException("Failed to communicate with Ignite cluster.", SqlStateCode.CONNECTION_FAILURE, e);
        }
        finally {
            synchronized (mux) {
                ownThread = null;
            }
        }
    }

    /**
     * @return Connection URL.
     */
    public String url() {
        return connProps.getUrl();
    }

    /**
     * Called on IO disconnect: close the client IO and opened statements.
     */
    private void onDisconnect() {
        if (!connected)
            return;

        if (affinityAwareness) {
            for (JdbcThinTcpIo clioIo : ios.values())
                clioIo.close();

            ios.clear();

            iosArr = null;
        }
        else {
            if (singleIo != null)
                singleIo.close();
        }

        connected = false;

        if (streamState != null) {
            streamState.close0();

            streamState = null;
        }

        synchronized (stmtsMux) {
            for (JdbcThinStatement s : stmts)
                s.closeOnDisconnect();

            stmts.clear();
        }

        timer.cancel();
    }

    /**
     * @param stmt Statement to close.
     */
    void closeStatement(JdbcThinStatement stmt) {
        synchronized (stmtsMux) {
            stmts.remove(stmt);
        }
    }

    /**
     * Streamer state and
     */
    private class StreamState {
        /** Maximum requests count that may be sent before any responses. */
        private static final int MAX_REQUESTS_BEFORE_RESPONSE = 10;

        /** Batch size for streaming. */
        private int streamBatchSize;

        /** Batch for streaming. */
        private List<JdbcQuery> streamBatch;

        /** Last added query to recognize batches. */
        private String lastStreamQry;

        /** Keep request order on execution. */
        private long order;

        /** Async response reader thread. */
        private Thread asyncRespReaderThread;

        /** Async response error. */
        private volatile Exception err;

        /** The order of the last batch request at the stream. */
        private long lastRespOrder = -1;

        /** Last response future. */
        private final GridFutureAdapter<Void> lastRespFut = new GridFutureAdapter<>();

        /** Response semaphore sem. */
        private Semaphore respSem = new Semaphore(MAX_REQUESTS_BEFORE_RESPONSE);

        /** Streaming sticky ignite endpoint. */
        private final JdbcThinTcpIo streamingStickyIo;

        /**
         * @param cmd Stream cmd.
         * @param stickyIo Sticky ignite endpoint.
         */
        StreamState(SqlSetStreamingCommand cmd, JdbcThinTcpIo stickyIo) {
            streamBatchSize = cmd.batchSize();

            asyncRespReaderThread = new Thread(this::readResponses);

            streamingStickyIo = stickyIo;
        }

        /**
         * Start reader.
         */
        void start() {
            asyncRespReaderThread.start();
        }

        /**
         * Add another query for batched execution.
         *
         * @param sql Query.
         * @param args Arguments.
         * @throws SQLException On error.
         */
        void addBatch(String sql, List<Object> args) throws SQLException {
            checkError();

            boolean newQry = (args == null || !F.eq(lastStreamQry, sql));

            // Providing null as SQL here allows for recognizing subbatches on server and handling them more efficiently.
            JdbcQuery q = new JdbcQuery(newQry ? sql : null, args != null ? args.toArray() : null);

            if (streamBatch == null)
                streamBatch = new ArrayList<>(streamBatchSize);

            streamBatch.add(q);

            // Null args means "addBatch(String)" was called on non-prepared Statement,
            // we don't want to remember its query string.
            lastStreamQry = (args != null ? sql : null);

            if (streamBatch.size() == streamBatchSize)
                executeBatch(false);
        }

        /**
         * @param lastBatch Whether open data streamers must be flushed and closed after this batch.
         * @throws SQLException if failed.
         */
        private void executeBatch(boolean lastBatch) throws SQLException {
            checkError();

            if (lastBatch)
                lastRespOrder = order;

            try {
                respSem.acquire();

                sendRequestNotWaitResponse(
                    new JdbcOrderedBatchExecuteRequest(schema, streamBatch, autoCommit, lastBatch, order),
                    streamingStickyIo);

                streamBatch = null;

                lastStreamQry = null;

                if (lastBatch) {
                    try {
                        lastRespFut.get();
                    }
                    catch (IgniteCheckedException ignored) {
                        // No-op.
                        // No exceptions are expected here.
                    }

                    checkError();
                }
                else
                    order++;
            }
            catch (InterruptedException e) {
                throw new SQLException("Streaming operation was interrupted", SqlStateCode.INTERNAL_ERROR, e);
            }
        }

        /**
         * Throws at the user thread exception that was thrown at the {@link #asyncRespReaderThread} thread.
         *
         * @throws SQLException Saved exception.
         */
        void checkError() throws SQLException {
            if (err != null) {
                Exception err0 = err;

                err = null;

                if (err0 instanceof SQLException)
                    throw (SQLException)err0;
                else {
                    onDisconnect();

                    if (err0 instanceof SocketTimeoutException)
                        throw new SQLException("Connection timed out.", SqlStateCode.CONNECTION_FAILURE, err0);
                    throw new SQLException("Failed to communicate with Ignite cluster on JDBC streaming.",
                        SqlStateCode.CONNECTION_FAILURE, err0);
                }
            }
        }

        /**
         * @throws SQLException On error.
         */
        void close() throws SQLException {
            close0();

            checkError();
        }

        /**
         */
        void close0() {
            if (connected) {
                try {
                    executeBatch(true);
                }
                catch (SQLException e) {
                    err = e;

                    LOG.log(Level.WARNING, "Exception during batch send on streamed connection close", e);
                }
            }

            if (asyncRespReaderThread != null)
                asyncRespReaderThread.interrupt();
        }

        /**
         *
         */
        void readResponses() {
            try {
                while (true) {
                    JdbcResponse resp = streamingStickyIo.readResponse();

                    if (resp.response() instanceof JdbcOrderedBatchExecuteResult) {
                        JdbcOrderedBatchExecuteResult res = (JdbcOrderedBatchExecuteResult)resp.response();

                        respSem.release();

                        if (res.errorCode() != ClientListenerResponse.STATUS_SUCCESS) {
                            err = new BatchUpdateException(res.errorMessage(),
                                IgniteQueryErrorCode.codeToSqlState(res.errorCode()),
                                res.errorCode(), res.updateCounts());
                        }

                        // Receive the response for the last request.
                        if (res.order() == lastRespOrder) {
                            lastRespFut.onDone();

                            break;
                        }
                    }
                    else if (resp.status() != ClientListenerResponse.STATUS_SUCCESS)
                        err = new SQLException(resp.error(), IgniteQueryErrorCode.codeToSqlState(resp.status()));
                    else
                        assert false : "Invalid response: " + resp;
                }
            }
            catch (Exception e) {
                err = e;
            }
        }
    }

    /**
     * @return True if query cancellation supported, false otherwise.
     */
    boolean isQueryCancellationSupported() {
        // TODO: IGNITE-11321: JDBC Thin: implement nodes multi version support.
        return cliIo(null).isQueryCancellationSupported();
    }

    /**
     * @param nodeIds Set of node's UUIDs.
     * @return Ignite endpoint to use for request/response transferring.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    private JdbcThinTcpIo cliIo(List<UUID> nodeIds) {
        if (!affinityAwareness)
            return singleIo;

        if (txIo != null)
            return txIo;

        if (nodeIds == null || nodeIds.isEmpty())
            return iosArr[RND.nextInt(iosArr.length)];

        JdbcThinTcpIo io = null;

        if (nodeIds.size() == 1)
            io = ios.get(nodeIds.iterator().next());
        else {
            int initNodeId = RND.nextInt(nodeIds.size());

            int iterCnt = 0;

            while (io == null) {
                io = ios.get(nodeIds.get(initNodeId));

                initNodeId = initNodeId == nodeIds.size() ? 0 : initNodeId + 1;

                iterCnt++;

                if (iterCnt == nodeIds.size())
                    break;
            }
        }

        return io != null ? io : iosArr[RND.nextInt(iosArr.length)];
    }

    /**
     * @return Current server index.
     */
    public int serverIndex() {
        return srvIdx;
    }

    /**
     * Get next server index.
     *
     * @param len Number of servers.
     * @return Index of the next server to connect to.
     */
    private static int nextServerIndex(int len) {
        if (len == 1)
            return 0;
        else {
            long nextIdx = IDX_GEN.getAndIncrement();

            return (int)(nextIdx % len);
        }
    }

    /**
     * Establishes a connection to ignite endpoint, trying all specified hosts and ports one by one.
     * Stops as soon as any connection is established.
     *
     * @param srvs Ignite endpoints addresses.
     * @throws SQLException If failed to connect to ignite cluster.
     */
    private void connectInCommonMode(HostAndPortRange[] srvs) throws SQLException {
        List<Exception> exceptions = null;

        for (int i = 0; i < srvs.length; i++) {
            srvIdx = nextServerIndex(srvs.length);

            HostAndPortRange srv = srvs[srvIdx];

            try {
                InetAddress[] addrs = InetAddress.getAllByName(srv.host());

                for (InetAddress addr : addrs) {
                    for (int port = srv.portFrom(); port <= srv.portTo(); ++port) {
                        try {
                            JdbcThinTcpIo cliIo = new JdbcThinTcpIo(connProps, new InetSocketAddress(addr, port),
                                0);

                            cliIo.timeout(netTimeout);

                            singleIo = cliIo;

                            connected = true;

                            return;
                        }
                        catch (Exception exception) {
                            if (exceptions == null)
                                exceptions = new ArrayList<>();

                            exceptions.add(exception);
                        }

                    }
                }
            }
            catch (Exception exception) {
                if (exceptions == null)
                    exceptions = new ArrayList<>();

                exceptions.add(exception);
            }
        }

        handleConnectExceptions(exceptions);
    }

    /**
     * Prepare and throw general {@code SQLException} with all specified exceptions as suppressed items.
     *
     * @param exceptions Exceptions list.
     * @throws SQLException Umbrella exception.
     */
    private void handleConnectExceptions(List<Exception> exceptions) throws SQLException {
        if (!connected && exceptions != null) {
            close();

            if (exceptions.size() == 1) {
                Exception ex = exceptions.get(0);

                if (ex instanceof SQLException)
                    throw (SQLException)ex;
                else if (ex instanceof IOException)
                    throw new SQLException("Failed to connect to Ignite cluster [url=" + connProps.getUrl() + ']',
                        SqlStateCode.CLIENT_CONNECTION_FAILED, ex);
            }

            SQLException e = new SQLException("Failed to connect to server [url=" + connProps.getUrl() + ']',
                SqlStateCode.CLIENT_CONNECTION_FAILED);

            for (Exception ex : exceptions)
                e.addSuppressed(ex);

            throw e;
        }
    }

    /**
     * Establishes a connection to ignite endpoint, trying all specified hosts and ports one by one.
     * Stops as soon as all iosArr are established.
     *
     * @param srvs Ignite endpoints addresses.
     * @throws SQLException If failed to connect to at least one ignite endpoint,
     * or if endpoints versions are not the same.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    private void connectInAffinityAwarenessMode(HostAndPortRange[] srvs) throws SQLException {
        List<Exception> exceptions = null;

        IgniteProductVersion prevIgniteEnpointVer = null;

        for (int i = 0; i < srvs.length; i++) {
            HostAndPortRange srv = srvs[i];

            try {
                InetAddress[] addrs = InetAddress.getAllByName(srv.host());

                for (InetAddress addr : addrs) {
                    for (int port = srv.portFrom(); port <= srv.portTo(); ++port) {
                        try {
                            JdbcThinTcpIo cliIo =
                                new JdbcThinTcpIo(connProps, new InetSocketAddress(addr, port), 0);

                            if (!cliIo.isAffinityAwarenessSupported()) {
                                throw new SQLException("Failed to connect to Ignite node [url=" +
                                    connProps.getUrl() + "]. address = [" + addr + ':' + port + "]." +
                                    "Node doesn't support best affort affinity mode.",
                                    SqlStateCode.INTERNAL_ERROR);
                            }

                            if (prevIgniteEnpointVer != null && !prevIgniteEnpointVer.equals(cliIo.igniteVersion())) {
                                // TODO: 13.02.19 IGNITE-11321 JDBC Thin: implement nodes multi version support.
                                throw new SQLException("Failed to connect to Ignite node [url=" +
                                    connProps.getUrl() + "]. address = [" + addr + ':' + port + "]." +
                                    "Different versions of nodes are not supported in affinity awareness mode.",
                                    SqlStateCode.INTERNAL_ERROR);
                            }

                            cliIo.timeout(netTimeout);

                            JdbcThinTcpIo ioToSameNode = ios.get(cliIo.nodeId());

                            // This can happen if the same node has several IPs.
                            if (ioToSameNode != null)
                                ioToSameNode.close();

                            ios.put(cliIo.nodeId(), cliIo);

                            connected = true;

                            prevIgniteEnpointVer = cliIo.igniteVersion();
                        }
                        catch (Exception exception) {
                            if (exceptions == null)
                                exceptions = new ArrayList<>();

                            exceptions.add(exception);
                        }
                    }
                }
            }
            catch (Exception exception) {
                if (exceptions == null)
                    exceptions = new ArrayList<>();

                exceptions.add(exception);
            }
        }

        handleConnectExceptions(exceptions);

        iosArr = ios.values().toArray(new JdbcThinTcpIo[0]);
    }

    /**
     * Request Timeout Timer Task
     */
    private class RequestTimeoutTimerTask extends TimerTask {
        /** Request id. */
        private final long reqId;

        /** Sticky singleIo. */
        private final JdbcThinTcpIo stickyIO;

        /** Remaining query timeout. */
        private int remainingQryTimeout;

        /** Flag that shows whether TimerTask was expired or not. */
        private AtomicBoolean expired;

        /**
         * @param reqId Request Id to cancel in case of timeout
         * @param initReqTimeout Initial request timeout
         */
        RequestTimeoutTimerTask(long reqId, JdbcThinTcpIo stickyIO, int initReqTimeout) {
            this.reqId = reqId;

            this.stickyIO = stickyIO;

            remainingQryTimeout = initReqTimeout;

            expired = new AtomicBoolean(false);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                if (remainingQryTimeout <= 0) {
                    expired.set(true);

                    sendQueryCancelRequest(new JdbcQueryCancelRequest(reqId), stickyIO);

                    cancel();
                }

                remainingQryTimeout -= REQUEST_TIMEOUT_PERIOD;
            }
            catch (SQLException e) {
                LOG.log(Level.WARNING,
                    "Request timeout processing failure: unable to cancel request [reqId=" + reqId + ']', e);

                cancel();
            }
        }
    }

    /**
     * Recreates affinity cache if affinity topology version was changed and adds partition result to sql cache.
     *
     * @param qryReq Query request.
     * @param res Jdbc Response.
     */
    private void updateAffinityCache(JdbcQueryExecuteRequest qryReq, JdbcResponse res) {
        if (affinityAwareness) {
            AffinityTopologyVersion resAffVer = res.affinityVersion();

            if (resAffVer != null && (affinityCache == null || affinityCache.version().compareTo(resAffVer) < 0))
                affinityCache = new AffinityCache(resAffVer);

            // Partition result was requested.
            if (res.response() instanceof JdbcQueryExecuteResult && qryReq.partitionResponseRequest()) {
                PartitionResult partRes = ((JdbcQueryExecuteResult)res.response()).partitionResult();

                if (partRes == null || affinityCache.version().equals(partRes.topologyVersion())) {
                    int cacheId = (partRes != null && partRes.tree() != null) ?
                        GridCacheUtils.cacheId(partRes.cacheName()) :
                        -1;

                    PartitionClientContext partClientCtx = partRes != null ?
                        new PartitionClientContext(partRes.partitionsCount()) :
                        null;

                    QualifiedSQLQuery qry = new QualifiedSQLQuery(qryReq.schemaName(), qryReq.sqlQuery());

                    JdbcThinPartitionResultDescriptor partResDescr =
                        new JdbcThinPartitionResultDescriptor(partRes, cacheId, partClientCtx);

                    affinityCache.addSqlQuery(qry, partResDescr);
                }
            }
        }
    }
}
