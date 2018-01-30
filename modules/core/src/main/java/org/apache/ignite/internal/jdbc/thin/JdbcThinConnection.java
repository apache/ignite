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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Logger;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcResponse;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcResult;
import org.apache.ignite.internal.processors.query.QueryUtils;
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

    /** Connection URL. */
    private String url;

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

    /**
     * Creates new connection.
     *
     * @param url Connection URL.
     * @param schema Schema name.
     * @param props Connection properties.
     * @throws SQLException In case Ignite client failed to start.
     */
    public JdbcThinConnection(String url, String schema, Properties props) throws SQLException {
        assert url != null;

        this.url = url;

        connProps = new ConnectionPropertiesImpl();

        ((ConnectionPropertiesImpl)connProps).init(props);

        holdability = HOLD_CURSORS_OVER_COMMIT;
        autoCommit = true;
        txIsolation = Connection.TRANSACTION_NONE;

        this.schema = normalizeSchema(schema);

        try {
            cliIo = new JdbcThinTcpIo(connProps);

            cliIo.start();
        }
        catch (Exception e) {
            cliIo.close();

            throw new SQLException("Failed to connect to Ignite cluster [host=" + connProps.getHost() +
                ", port=" + connProps.getPort() + ']', SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }
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
            close();

            throw new SQLException("Failed to communicate with Ignite cluster.", SqlStateCode.CONNECTION_FAILURE, e);
        }
    }

    /**
     * @return Connection URL.
     */
    public String url() {
        return url;
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
}