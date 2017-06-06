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

import org.apache.ignite.internal.util.typedef.F;

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

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

import static org.apache.ignite.internal.jdbc.thin.JdbcThinUtils.PROP_HOST;
import static org.apache.ignite.internal.jdbc.thin.JdbcThinUtils.PROP_PORT;
import static org.apache.ignite.internal.jdbc.thin.JdbcThinUtils.PROP_DISTRIBUTED_JOINS;
import static org.apache.ignite.internal.jdbc.thin.JdbcThinUtils.PROP_ENFORCE_JOIN_ORDER;
import static org.apache.ignite.internal.jdbc.thin.JdbcThinUtils.PROP_COLLOCATED;
import static org.apache.ignite.internal.jdbc.thin.JdbcThinUtils.PROP_REPLICATED_ONLY;
import static org.apache.ignite.internal.jdbc.thin.JdbcThinUtils.PROP_SOCK_SND_BUF;
import static org.apache.ignite.internal.jdbc.thin.JdbcThinUtils.PROP_SOCK_RCV_BUF;
import static org.apache.ignite.internal.jdbc.thin.JdbcThinUtils.PROP_TCP_NO_DELAY;

/**
 * JDBC connection implementation.
 *
 * See documentation of {@link org.apache.ignite.IgniteJdbcThinDriver} for details.
 */
public class JdbcThinConnection implements Connection {
    /** Logger. */
    private static final Logger LOG = Logger.getLogger(JdbcThinConnection.class.getName());

    /** Schema name. */
    private String schemaName;

    /** Closed flag. */
    private boolean closed;

    /** Current transaction isolation. */
    private int txIsolation;

    /** Auto commit flag. */
    private boolean autoCommit;

    /** Current transaction holdability. */
    private int holdability;

    /** Timeout. */
    private int timeout;

    /** Ignite endpoint. */
    private JdbcThinTcpIo cliIo;

    /**
     * Creates new connection.
     *
     * @param url Connection URL.
     * @param props Additional properties.
     * @throws SQLException In case Ignite client failed to start.
     */
    public JdbcThinConnection(String url, Properties props) throws SQLException {
        assert url != null;
        assert props != null;

        holdability = HOLD_CURSORS_OVER_COMMIT;
        autoCommit = true;
        txIsolation = Connection.TRANSACTION_NONE;

        String host = extractHost(props);
        int port = extractPort(props);

        boolean distributedJoins = extractBoolean(props, PROP_DISTRIBUTED_JOINS, false);
        boolean enforceJoinOrder = extractBoolean(props, PROP_ENFORCE_JOIN_ORDER, false);
        boolean collocated = extractBoolean(props, PROP_COLLOCATED, false);
        boolean replicatedOnly = extractBoolean(props, PROP_REPLICATED_ONLY, false);

        int sockSndBuf = extractIntNonNegative(props, PROP_SOCK_SND_BUF, 0);
        int sockRcvBuf = extractIntNonNegative(props, PROP_SOCK_RCV_BUF, 0);

        boolean tcpNoDelay  = extractBoolean(props, PROP_TCP_NO_DELAY, true);

        try {
            cliIo = new JdbcThinTcpIo(host, port, distributedJoins, enforceJoinOrder, collocated, replicatedOnly,
                sockSndBuf, sockRcvBuf, tcpNoDelay);

            cliIo.start();
        }
        catch (Exception e) {
            cliIo.close();

            throw new SQLException("Failed to connect to Ignite node [host=" + host + ", port=" + port + ']', e);
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

        JdbcThinStatement stmt  = new JdbcThinStatement(this);

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

        JdbcThinPreparedStatement stmt = new JdbcThinPreparedStatement(this, sql);

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
            throw new SQLFeatureNotSupportedException("Invalid result set type (only forward is supported.)");

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

        LOG.warning("Transactions are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void rollback() throws SQLException {
        ensureNotClosed();

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

        return null;
    }

    /** {@inheritDoc} */
    @Override public void setReadOnly(boolean readOnly) throws SQLException {
        ensureNotClosed();
    }

    /** {@inheritDoc} */
    @Override public boolean isReadOnly() throws SQLException {
        ensureNotClosed();

        return true;
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

        LOG.warning("Transactions are not supported.");

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

        throw new SQLFeatureNotSupportedException("Savepoints are not supported.");
    }

    /** {@inheritDoc} */
    @Override public Savepoint setSavepoint(String name) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Savepoints are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void rollback(Savepoint savepoint) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Savepoints are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        ensureNotClosed();

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
        throw new UnsupportedOperationException("Client info is not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setClientInfo(Properties props) throws SQLClientInfoException {
        throw new UnsupportedOperationException("Client info is not supported.");
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

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public Struct createStruct(String typeName, Object[] attrs) throws SQLException {
        ensureNotClosed();

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
        schemaName = schema;
    }

    /** {@inheritDoc} */
    @Override public String getSchema() throws SQLException {
        return schemaName;
    }

    /** {@inheritDoc} */
    @Override public void abort(Executor executor) throws SQLException {
        close();
    }

    /** {@inheritDoc} */
    @Override public void setNetworkTimeout(Executor executor, int ms) throws SQLException {
        if (ms < 0)
            throw new IllegalArgumentException("Timeout is below zero: " + ms);

        timeout = ms;
    }

    /** {@inheritDoc} */
    @Override public int getNetworkTimeout() throws SQLException {
        return timeout;
    }

    /**
     * Ensures that connection is not closed.
     *
     * @throws SQLException If connection is closed.
     */
    private void ensureNotClosed() throws SQLException {
        if (closed)
            throw new SQLException("Connection is closed.");
    }

    /**
     * @return Ignite endpoint and I/O protocol.
     */
    public JdbcThinTcpIo io() {
        return cliIo;
    }

    /**
     * Extract host.
     *
     * @param props Properties.
     * @return Host.
     * @throws SQLException If failed.
     */
    private static String extractHost(Properties props) throws SQLException {
        String host = props.getProperty(PROP_HOST);

        if (host != null)
            host = host.trim();

        if (F.isEmpty(host))
            throw new SQLException("Host name is empty.");

        return host;
    }

    /**
     * Extract port.
     *
     * @param props Properties.
     * @return Port.
     * @throws SQLException If failed.
     */
    private static int extractPort(Properties props) throws SQLException {
        String portStr = props.getProperty(PROP_PORT);

        if (portStr == null)
            return JdbcThinUtils.DFLT_PORT;

        int port;

        try {
            port = Integer.parseInt(portStr);

            if (port <= 0 || port > 0xFFFF)
                throw new SQLException("Invalid port: " + portStr);
        }
        catch (NumberFormatException e) {
            throw new SQLException("Invalid port: " + portStr, e);
        }

        return port;
    }

    /**
     * Extract boolean property.
     *
     * @param props Properties.
     * @param propName Property name.
     * @param dfltVal Default value.
     * @return Value.
     * @throws SQLException If failed.
     */
    private static boolean extractBoolean(Properties props, String propName, boolean dfltVal) throws SQLException {
        String strVal = props.getProperty(propName);

        if (strVal == null)
            return dfltVal;

        if (Boolean.TRUE.toString().equalsIgnoreCase(strVal))
            return true;
        else if (Boolean.FALSE.toString().equalsIgnoreCase(strVal))
            return false;
        else
            throw new SQLException("Failed to parse boolean property [name=" + JdbcThinUtils.trimPrefix(propName) +
                    ", value=" + strVal + ']');
    }

    /**
     * Extract non-negative int property.
     *
     * @param props Properties.
     * @param propName Property name.
     * @param dfltVal Default value.
     * @return Value.
     * @throws SQLException If failed.
     */
    private static int extractIntNonNegative(Properties props, String propName, int dfltVal) throws SQLException {
        int res = extractInt(props, propName, dfltVal);

        if (res < 0)
            throw new SQLException("Property cannot be negative [name=" + JdbcThinUtils.trimPrefix(propName) +
                ", value=" + res + ']');

        return res;
    }

    /**
     * Extract int property.
     *
     * @param props Properties.
     * @param propName Property name.
     * @param dfltVal Default value.
     * @return Value.
     * @throws SQLException If failed.
     */
    private static int extractInt(Properties props, String propName, int dfltVal) throws SQLException {
        String strVal = props.getProperty(propName);

        if (strVal == null)
            return dfltVal;

        try {
            return Integer.parseInt(strVal);
        }
        catch (NumberFormatException e) {
            throw new SQLException("Failed to parse int property [name=" + JdbcThinUtils.trimPrefix(propName) +
                ", value=" + strVal + ']');
        }
    }
}