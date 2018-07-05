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
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcOrderedBatchExecuteRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcOrderedBatchExecuteResult;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQuery;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryExecuteRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcResponse;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcResult;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcStatementType;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.sql.command.SqlSetStreamingCommand;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteProductVersion;

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

    /** Statements modification mutex. */
    final private Object stmtsMux = new Object();

    /** Schema name. */
    private String schema;

    /** Closed flag. */
    private boolean closed;

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

    /** Timeout. */
    private int timeout;

    /** Ignite endpoint. */
    private JdbcThinTcpIo cliIo;

    /** Jdbc metadata. Cache the JDBC object on the first access */
    private JdbcThinDatabaseMetadata metadata;

    /** Connection properties. */
    private ConnectionProperties connProps;

    /** Connected. */
    private boolean connected;

    /** Tracked statements to close on disconnect. */
    private final ArrayList<JdbcThinStatement> stmts = new ArrayList<>();

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

        schema = normalizeSchema(connProps.getSchema());

        cliIo = new JdbcThinTcpIo(connProps);

        ensureConnected();
    }

    /**
     * @throws SQLException On connection error.
     */
    private synchronized void ensureConnected() throws SQLException {
        try {
            if (connected)
                return;

            assert !closed;

            cliIo.start();

            connected = true;
        }
        catch (SQLException e) {
            close();

            throw e;
        }
        catch (Exception e) {
            close();

            throw new SQLException("Failed to connect to Ignite cluster [url=" + connProps.getUrl() + ']',
                SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }
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
     * @throws SQLException if failed.
     */
    void executeNative(String sql, SqlCommand cmd) throws SQLException {
        if (cmd instanceof SqlSetStreamingCommand) {
            SqlSetStreamingCommand cmd0 = (SqlSetStreamingCommand)cmd;

            // If streaming is already on, we have to close it first.
            if (streamState != null) {
                streamState.close();

                streamState = null;
            }

            boolean newVal = ((SqlSetStreamingCommand)cmd).isTurnOn();

            // Actual ON, if needed.
            if (newVal) {
                if (!cmd0.isOrdered()  && !cliIo.isUnorderedStreamSupported()) {
                    throw new SQLException("Streaming without order doesn't supported by server [remoteNodeVer="
                        + cliIo.igniteVersion() + ']', SqlStateCode.INTERNAL_ERROR);
                }

                sendRequest(new JdbcQueryExecuteRequest(JdbcStatementType.ANY_STATEMENT_TYPE,
                    schema, 1, 1, sql, null));

                streamState = new StreamState((SqlSetStreamingCommand)cmd);
            }
        }
        else
            throw IgniteQueryErrorCode.createJdbcSqlException("Unsupported native statement: " + sql,
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /**
     * Add another query for batched execution.
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

        checkCursorOptions(resSetType, resSetConcurrency, resSetHoldability);

        JdbcThinStatement stmt  = new JdbcThinStatement(this, resSetHoldability, schema);

        if (timeout > 0)
            stmt.timeout(timeout);

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

        checkCursorOptions(resSetType, resSetConcurrency, resSetHoldability);

        if (sql == null)
            throw new SQLException("SQL string cannot be null.");

        JdbcThinPreparedStatement stmt = new JdbcThinPreparedStatement(this, sql, resSetHoldability, schema);

        if (timeout > 0)
            stmt.timeout(timeout);

        synchronized (stmtsMux) {
            stmts.add(stmt);
        }

        return stmt;
    }

    /**
     * @param resSetType Cursor option.
     * @param resSetConcurrency Cursor option.
     * @param resSetHoldability Cursor option.
     * @throws SQLException If options unsupported.
     */
    private void checkCursorOptions(int resSetType, int resSetConcurrency,
        int resSetHoldability) throws SQLException {
        if (resSetType != TYPE_FORWARD_ONLY)
            throw new SQLFeatureNotSupportedException("Invalid result set type (only forward is supported).");

        if (resSetConcurrency != CONCUR_READ_ONLY)
            throw new SQLFeatureNotSupportedException("Invalid concurrency (updates are not supported).");

        if (resSetHoldability != HOLD_CURSORS_OVER_COMMIT)
            LOG.warning("Transactions are not supported.");
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

        this.autoCommit = autoCommit;

        if (!autoCommit)
            LOG.warning("Transactions are not supported.");
    }

    /** {@inheritDoc} */
    @Override public boolean getAutoCommit() throws SQLException {
        ensureNotClosed();

        if (!autoCommit)
            LOG.warning("Transactions are not supported.");

        return autoCommit;
    }

    /** {@inheritDoc} */
    @Override public void commit() throws SQLException {
        ensureNotClosed();

        if (autoCommit)
            throw new SQLException("Transaction cannot be committed explicitly in auto-commit mode.");

        LOG.warning("Transactions are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void rollback() throws SQLException {
        ensureNotClosed();

        if (autoCommit)
            throw new SQLException("Transaction cannot rollback in auto-commit mode.");

        LOG.warning("Transactions are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void close() throws SQLException {
        if (isClosed())
            return;

        if (streamState != null) {
            streamState.close();

            streamState = null;
        }

        closed = true;

        cliIo.close();
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() throws SQLException {
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
                LOG.warning("Transactions are not supported.");

                break;

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

        LOG.warning("Transactions are not supported.");

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

        if (holdability != HOLD_CURSORS_OVER_COMMIT)
            LOG.warning("Transactions are not supported.");

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
    @SuppressWarnings("unchecked")
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

        this.schema = normalizeSchema(schema);
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

        if (executor == null)
            throw new SQLException("Executor cannot be null.");

        if (ms < 0)
            throw new SQLException("Network timeout cannot be negative.");

        timeout = ms;
    }

    /** {@inheritDoc} */
    @Override public int getNetworkTimeout() throws SQLException {
        ensureNotClosed();

        return timeout;
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
        return cliIo.igniteVersion();
    }

    /**
     * @return Auto close server cursors flag.
     */
    boolean autoCloseServerCursor() {
        return connProps.isAutoCloseServerCursor();
    }

    /**
     * Send request for execution via {@link #cliIo}.
     * @param req Request.
     * @return Server response.
     * @throws SQLException On any error.
     */
    @SuppressWarnings("unchecked")
    <R extends JdbcResult> R sendRequest(JdbcRequest req) throws SQLException {
        ensureConnected();

        try {
            JdbcResponse res = cliIo.sendRequest(req);

            if (res.status() != ClientListenerResponse.STATUS_SUCCESS)
                throw new SQLException(res.error(), IgniteQueryErrorCode.codeToSqlState(res.status()));

            return (R)res.response();
        }
        catch (SQLException e) {
            throw e;
        }
        catch (Exception e) {
            onDisconnect();

            throw new SQLException("Failed to communicate with Ignite cluster.", SqlStateCode.CONNECTION_FAILURE, e);
        }
    }

    /**
     * Send request for execution via {@link #cliIo}. Response is waited at the separate thread
     *     (see {@link StreamState#asyncRespReaderThread}).
     * @param req Request.
     * @throws SQLException On any error.
     */
    private void sendRequestNotWaitResponse(JdbcOrderedBatchExecuteRequest req) throws SQLException {
        ensureConnected();

        try {
            cliIo.sendBatchRequestNoWaitResponse(req);
        }
        catch (SQLException e) {
            throw e;
        }
        catch (Exception e) {
            onDisconnect();

            throw new SQLException("Failed to communicate with Ignite cluster.", SqlStateCode.CONNECTION_FAILURE, e);
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

        cliIo.close();

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
    }

    /**
     * Normalize schema name. If it is quoted - unquote and leave as is, otherwise - convert to upper case.
     *
     * @param schemaName Schema name.
     * @return Normalized schema name.
     */
    private static String normalizeSchema(String schemaName) {
        if (F.isEmpty(schemaName))
            return QueryUtils.DFLT_SCHEMA;

        String res;

        if (schemaName.startsWith("\"") && schemaName.endsWith("\""))
            res = schemaName.substring(1, schemaName.length() - 1);
        else
            res = schemaName.toUpperCase();

        return res;
    }

    /**
     * Streamer state and
     */
    private class StreamState {
        /** Maximum requests count that may be sent before any responses. */
        private static final int MAX_REQUESTS_BEFORE_RESPONSE = 10;

        /** Wait timeout. */
        private static final long WAIT_TIMEOUT = 1;

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

        /**
         * @param cmd Stream cmd.
         */
        StreamState(SqlSetStreamingCommand cmd) {
            streamBatchSize = cmd.batchSize();

            asyncRespReaderThread = new Thread(this::readResponses);

            asyncRespReaderThread.start();
        }

        /**
         * Add another query for batched execution.
         * @param sql Query.
         * @param args Arguments.
         * @throws SQLException On error.
         */
        void addBatch(String sql, List<Object> args) throws SQLException {
            checkError();

            boolean newQry = (args == null || !F.eq(lastStreamQry, sql));

            // Providing null as SQL here allows for recognizing subbatches on server and handling them more efficiently.
            JdbcQuery q  = new JdbcQuery(newQry ? sql : null, args != null ? args.toArray() : null);

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
                    new JdbcOrderedBatchExecuteRequest(schema, streamBatch, lastBatch, order));

                streamBatch = null;

                lastStreamQry = null;

                if (lastBatch) {
                    try {
                        lastRespFut.get();
                    }
                    catch (IgniteCheckedException e) {
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
        void readResponses () {
            try {
                while (true) {
                    JdbcResponse resp = cliIo.readResponse();

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

                    if (resp.status() != ClientListenerResponse.STATUS_SUCCESS)
                        err = new SQLException(resp.error(), IgniteQueryErrorCode.codeToSqlState(resp.status()));
                }
            }
            catch (Exception e) {
                err = e;
            }
        }
    }
}