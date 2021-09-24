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
import java.sql.SQLPermission;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.ShardingKey;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.proto.query.JdbcQueryEventHandler;
import org.apache.ignite.client.proto.query.SqlStateCode;
import org.apache.ignite.internal.client.HostAndPortRange;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.internal.client.query.JdbcClientQueryEventHandler;
import org.apache.ignite.schema.definition.TableDefinition;
import org.jetbrains.annotations.Nullable;

import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.apache.ignite.client.proto.query.SqlStateCode.CONNECTION_CLOSED;

/**
 * JDBC connection implementation.
 */
public class JdbcConnection implements Connection {
    /** Network timeout permission. */
    private static final String SET_NETWORK_TIMEOUT_PERM = "setNetworkTimeout";

    /** Statements modification mutex. */
    private final Object stmtsMux = new Object();

    /** Handler. */
    private final JdbcQueryEventHandler handler;

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

    /** Current transaction holdability. */
    private int holdability;

    /** Connection properties. */
    private final ConnectionProperties connProps;

    /** Tracked statements to close on disconnect. */
    private final Set<JdbcStatement> stmts = Collections.newSetFromMap(new IdentityHashMap<>());

    /** Network timeout. */
    private int netTimeout;

    /** Query timeout. */
    private final @Nullable Integer qryTimeout;

    /** Ignite remote client. */
    private final TcpIgniteClient client;

    /** Jdbc metadata. Cache the JDBC object on the first access */
    private JdbcDatabaseMetadata metadata;

    /**
     * Constructor.
     *
     * @param handler Handler.
     * @param props Properties.
     */
    public JdbcConnection(JdbcQueryEventHandler handler, ConnectionProperties props) {
        this.connProps = props;
        this.handler = handler;

        autoCommit = true;

        netTimeout = connProps.getConnectionTimeout();
        qryTimeout = connProps.getQueryTimeout();

        holdability = HOLD_CURSORS_OVER_COMMIT;

        schema = TableDefinition.DEFAULT_DATABASE_SCHEMA_NAME;

        client = null;
    }

    /**
     * Creates new connection.
     *
     * @param props Connection properties.
     */
    public JdbcConnection(ConnectionProperties props) {
        this.connProps = props;
        autoCommit = true;

        String[] addrs = Arrays.stream(props.getAddresses()).map(this::createStrAddress)
            .toArray(String[]::new);

        netTimeout = connProps.getConnectionTimeout();
        qryTimeout = connProps.getQueryTimeout();

        int retryLimit = connProps.getRetryLimit();
        long reconnectThrottlingPeriod = connProps.getReconnectThrottlingPeriod();
        int reconnectThrottlingRetries = connProps.getReconnectThrottlingRetries();

        client = ((TcpIgniteClient)IgniteClient
            .builder()
            .addresses(addrs)
            .connectTimeout(netTimeout)
            .retryLimit(retryLimit)
            .reconnectThrottlingPeriod(reconnectThrottlingPeriod)
            .reconnectThrottlingRetries(reconnectThrottlingRetries)
            .build());

        this.handler = new JdbcClientQueryEventHandler(client);

        txIsolation = Connection.TRANSACTION_NONE;

        schema = normalizeSchema(connProps.getSchema());

        holdability = HOLD_CURSORS_OVER_COMMIT;
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

        JdbcStatement stmt = new JdbcStatement(this, resSetHoldability, schema);

        if (qryTimeout != null)
            stmt.setQueryTimeout(qryTimeout);

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

        Objects.requireNonNull(sql);

        JdbcPreparedStatement stmt = new JdbcPreparedStatement(this, sql, resSetHoldability, schema);

        synchronized (stmtsMux) {
            stmts.add(stmt);
        }

        return stmt;
    }

    /** {@inheritDoc} */
    @Override public String nativeSQL(String sql) throws SQLException {
        ensureNotClosed();

        Objects.requireNonNull(sql);

        return sql;
    }

    /** {@inheritDoc} */
    @Override public void setAutoCommit(boolean autoCommit) throws SQLException {
        ensureNotClosed();

        if (autoCommit != this.autoCommit) {
            this.autoCommit = autoCommit;
            
            doCommit(); // Specification requires to commit current tx if 'autoCommit' state was changed.
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

    /** {@inheritDoc} */
    @Override public void close() throws SQLException {
        if (isClosed())
            return;

        closed = true;

        synchronized (stmtsMux) {
            stmts.clear();
        }

        if (client == null)
            return;

        try {
            client.close();
        }
        catch (Exception e) {
            throw new SQLException(e);
        }
    }

    /**
     * Ensures that connection is not closed.
     *
     * @throws SQLException If connection is closed.
     */
    public void ensureNotClosed() throws SQLException {
        if (closed)
            throw new SQLException("Connection is closed.", CONNECTION_CLOSED);
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() throws SQLException {
        return closed;
    }

    /** {@inheritDoc} */
    @Override public DatabaseMetaData getMetaData() throws SQLException {
        ensureNotClosed();

        if (metadata == null)
            metadata = new JdbcDatabaseMetadata(this);

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

        if (ms < 0)
            throw new SQLException("Network timeout cannot be negative.");

        SecurityManager secMgr = System.getSecurityManager();

        if (secMgr != null)
            secMgr.checkPermission(new SQLPermission(SET_NETWORK_TIMEOUT_PERM));

        netTimeout = ms;
    }

    /** {@inheritDoc} */
    @Override public int getNetworkTimeout() throws SQLException {
        ensureNotClosed();

        return netTimeout;
    }

    /** {@inheritDoc} */
    @Override public void beginRequest() throws SQLException {
        ensureNotClosed();

        Connection.super.beginRequest();
    }

    /** {@inheritDoc} */
    @Override public void endRequest() throws SQLException {
        ensureNotClosed();

        Connection.super.endRequest();
    }

    /** {@inheritDoc} */
    @Override public boolean setShardingKeyIfValid(ShardingKey shardingKey, ShardingKey superShardingKey,
        int timeout) throws SQLException {
        ensureNotClosed();

        return Connection.super.setShardingKeyIfValid(shardingKey, superShardingKey, timeout);
    }

    /** {@inheritDoc} */
    @Override public boolean setShardingKeyIfValid(ShardingKey shardingKey, int timeout) throws SQLException {
        ensureNotClosed();

        return Connection.super.setShardingKeyIfValid(shardingKey, timeout);
    }

    /** {@inheritDoc} */
    @Override public void setShardingKey(ShardingKey shardingKey, ShardingKey superShardingKey) throws SQLException {
        ensureNotClosed();

        Connection.super.setShardingKey(shardingKey, superShardingKey);
    }

    /** {@inheritDoc} */
    @Override public void setShardingKey(ShardingKey shardingKey) throws SQLException {
        ensureNotClosed();

        Connection.super.setShardingKey(shardingKey);
    }

    /**
     * Get the query event handler.
     *
     * @return Handler.
     */
    public JdbcQueryEventHandler handler() {
        return handler;
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(iface))
            throw new SQLException("Connection is not a wrapper for " + iface.getName());

        return (T)this;
    }

    /** {@inheritDoc} */
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(JdbcConnection.class);
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

    /**
     * Remove statement from statements set.
     *
     * @param stmt Statement to remove.
     */
    void removeStatement(JdbcStatement stmt) {
        synchronized (stmtsMux) {
            stmts.remove(stmt);
        }
    }

    /**
     * Check cursor options.
     *
     * @param resSetType Cursor option.
     * @param resSetConcurrency Cursor option.
     * @throws SQLFeatureNotSupportedException If options unsupported.
     */
    private void checkCursorOptions(int resSetType, int resSetConcurrency) throws SQLFeatureNotSupportedException {
        if (resSetType != TYPE_FORWARD_ONLY)
            throw new SQLFeatureNotSupportedException("Invalid result set type (only forward is supported).");

        if (resSetConcurrency != CONCUR_READ_ONLY)
            throw new SQLFeatureNotSupportedException("Invalid concurrency (updates are not supported).");
    }

    /**
     * Creates address string from HostAndPortRange object.
     *
     * @param range HostAndPortRange.
     * @return Address string with host and port range.
     */
    private String createStrAddress(HostAndPortRange range) {
        String host = range.host();
        int portFrom = range.portFrom();
        int portTo = range.portTo();

        boolean ipV6 = host.contains(":");

        if (ipV6)
            host = "[" + host + "]";

        return host + ":" + (portFrom == portTo ? portFrom : portFrom + ".." + portTo);
    }

    /**
     * Normalize schema name. If it is quoted - unquote and leave as is, otherwise - convert to upper case.
     *
     * @param schemaName Schema name.
     * @return Normalized schema name.
     */
    public static String normalizeSchema(String schemaName) {
        if (schemaName == null || schemaName.isEmpty())
            return TableDefinition.DEFAULT_DATABASE_SCHEMA_NAME;

        String res;

        if (schemaName.startsWith("\"") && schemaName.endsWith("\""))
            res = schemaName.substring(1, schemaName.length() - 1);
        else
            res = schemaName.toUpperCase();

        return res;
    }

    /**
     * For test purposes.
     *
     * @return Connection properties.
     */
    public ConnectionProperties connectionProperties() {
        return connProps;
    }

    /**
     * Gets connection url.
     *
     * @return Connection URL.
     */
    public String url() {
        return connProps.getUrl();
    }
}
