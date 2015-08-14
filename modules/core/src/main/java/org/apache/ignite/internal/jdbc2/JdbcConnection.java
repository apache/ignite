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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.discovery.*;
import org.apache.ignite.internal.processors.resource.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.sql.ResultSet.*;
import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.IgniteJdbcDriver.*;

/**
 * JDBC connection implementation.
 */
public class JdbcConnection implements Connection {
    /**
     * Ignite nodes cache.
     *
     * The key is result of concatenation of the following properties:
     * <ol>
     *     <li>{@link IgniteJdbcDriver#PROP_CFG}</li>
     * </ol>
     */
    private static final ConcurrentMap<String, IgniteNodeFuture> NODES = new ConcurrentHashMap<>();

    /** Ignite ignite. */
    private final Ignite ignite;

    /** Node key. */
    private final String nodeKey;

    /** Cache name. */
    private String cacheName;

    /** Closed flag. */
    private boolean closed;

    /** URL. */
    private String url;

    /** Node ID. */
    private UUID nodeId;

    /**
     * Creates new connection.
     *
     * @param url Connection URL.
     * @param props Additional properties.
     * @throws SQLException In case Ignite node failed to start.
     */
    public JdbcConnection(String url, Properties props) throws SQLException {
        assert url != null;
        assert props != null;

        this.url = url;

        this.cacheName = props.getProperty(PROP_CACHE);

        String nodeIdProp = props.getProperty(PROP_NODE_ID);

        if (nodeIdProp != null)
            this.nodeId = UUID.fromString(nodeIdProp);

        this.nodeKey = nodeKey(props);

        try {
            ignite = getIgnite(props.getProperty(PROP_CFG));

            if (!isValid(2))
                throw new SQLException("Client is invalid. Probably cache name is wrong.");
        }
        catch (Exception e) {
            close();

            if (e instanceof SQLException)
                throw (SQLException)e;
            else
                throw new SQLException("Failed to start Ignite node.", e);
        }
    }

    /**
     * @param cfgUrl Config url.
     */
    private Ignite getIgnite(String cfgUrl) throws IgniteCheckedException {
        while (true) {
            IgniteNodeFuture fut = NODES.get(nodeKey);

            if (fut == null) {
                fut = new IgniteNodeFuture();

                IgniteNodeFuture old = NODES.putIfAbsent(nodeKey, fut);

                if (old != null)
                    fut = old;
                else {
                    try {
                        Ignite ignite = Ignition.start(loadConfiguration(cfgUrl));

                        fut.onDone(ignite);
                    }
                    catch (IgniteException e) {
                        fut.onDone(e);
                    }

                    return fut.get();
                }
            }

            if (fut.acquire())
                return fut.get();
            else
                NODES.remove(nodeKey, fut);
        }
    }

    /**
     * @param cfgUrl Config URL.
     */
    private IgniteConfiguration loadConfiguration(String cfgUrl) {
        try {
            IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> cfgMap =
                IgnitionEx.loadConfigurations(cfgUrl);

            IgniteConfiguration cfg = F.first(cfgMap.get1());

            if (cfg.getGridName() == null)
                cfg.setGridName("ignite-jdbc-driver-" + UUID.randomUUID().toString());

            return cfg;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Statement createStatement() throws SQLException {
        return createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT);
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement prepareStatement(String sql) throws SQLException {
        ensureNotClosed();

        return prepareStatement(sql, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT);
    }

    /** {@inheritDoc} */
    @Override public CallableStatement prepareCall(String sql) throws SQLException {
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

        if (!autoCommit)
            throw new SQLFeatureNotSupportedException("Transactions are not supported.");
    }

    /** {@inheritDoc} */
    @Override public boolean getAutoCommit() throws SQLException {
        ensureNotClosed();

        return true;
    }

    /** {@inheritDoc} */
    @Override public void commit() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Transactions are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void rollback() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Transactions are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void close() throws SQLException {
        if (closed)
            return;

        closed = true;

        IgniteNodeFuture fut = NODES.get(nodeKey);

        if (fut != null && fut.release()) {
                NODES.remove(nodeKey);

                ignite.close();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() throws SQLException {
        return closed;
    }

    /** {@inheritDoc} */
    @Override public DatabaseMetaData getMetaData() throws SQLException {
        ensureNotClosed();

        return new JdbcDatabaseMetadata(this);
    }

    /** {@inheritDoc} */
    @Override public void setReadOnly(boolean readOnly) throws SQLException {
        ensureNotClosed();

        if (!readOnly)
            throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public boolean isReadOnly() throws SQLException {
        ensureNotClosed();

        return true;
    }

    /** {@inheritDoc} */
    @Override public void setCatalog(String catalog) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Catalogs are not supported.");
    }

    /** {@inheritDoc} */
    @Override public String getCatalog() throws SQLException {
        ensureNotClosed();

        return null;
    }

    /** {@inheritDoc} */
    @Override public void setTransactionIsolation(int level) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Transactions are not supported.");
    }

    /** {@inheritDoc} */
    @Override public int getTransactionIsolation() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Transactions are not supported.");
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
    @Override public Statement createStatement(int resSetType, int resSetConcurrency) throws SQLException {
        return createStatement(resSetType, resSetConcurrency, HOLD_CURSORS_OVER_COMMIT);
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement prepareStatement(String sql, int resSetType,
        int resSetConcurrency) throws SQLException {
        ensureNotClosed();

        return prepareStatement(sql, resSetType, resSetConcurrency, HOLD_CURSORS_OVER_COMMIT);
    }

    /** {@inheritDoc} */
    @Override public CallableStatement prepareCall(String sql, int resSetType,
        int resSetConcurrency) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Callable functions are not supported.");
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
            throw new SQLFeatureNotSupportedException("Invalid holdability (transactions are not supported).");
    }

    /** {@inheritDoc} */
    @Override public int getHoldability() throws SQLException {
        ensureNotClosed();

        return HOLD_CURSORS_OVER_COMMIT;
    }

    /** {@inheritDoc} */
    @Override public Savepoint setSavepoint() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Transactions are not supported.");
    }

    /** {@inheritDoc} */
    @Override public Savepoint setSavepoint(String name) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Transactions are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void rollback(Savepoint savepoint) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Transactions are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Transactions are not supported.");
    }

    /** {@inheritDoc} */
    @Override public Statement createStatement(int resSetType, int resSetConcurrency,
        int resSetHoldability) throws SQLException {
        ensureNotClosed();

        if (resSetType != TYPE_FORWARD_ONLY)
            throw new SQLFeatureNotSupportedException("Invalid result set type (only forward is supported.)");

        if (resSetConcurrency != CONCUR_READ_ONLY)
            throw new SQLFeatureNotSupportedException("Invalid concurrency (updates are not supported).");

        if (resSetHoldability != HOLD_CURSORS_OVER_COMMIT)
            throw new SQLFeatureNotSupportedException("Invalid holdability (transactions are not supported).");

        return new JdbcStatement(this);
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement prepareStatement(String sql, int resSetType, int resSetConcurrency,
        int resSetHoldability) throws SQLException {
        ensureNotClosed();

        if (resSetType != TYPE_FORWARD_ONLY)
            throw new SQLFeatureNotSupportedException("Invalid result set type (only forward is supported.)");

        if (resSetConcurrency != CONCUR_READ_ONLY)
            throw new SQLFeatureNotSupportedException("Invalid concurrency (updates are not supported).");

        if (resSetHoldability != HOLD_CURSORS_OVER_COMMIT)
            throw new SQLFeatureNotSupportedException("Invalid holdability (transactions are not supported).");

        return new JdbcPreparedStatement(this, sql);
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

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement prepareStatement(String sql, int[] colIndexes) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement prepareStatement(String sql, String[] colNames) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
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
        ensureNotClosed();

        if (timeout < 0)
            throw new SQLException("Invalid timeout: " + timeout);

        try {
            if (nodeId != null) {
                IgniteCompute compute = ignite.compute().withAsync();

                compute.call(new JdbcConnectionValidationTask(cacheName));

                return compute.<Boolean>future().get(timeout, SECONDS);
            }
            else
                return ignite.cache(cacheName) != null;
        }
        catch (IgniteClientDisconnectedException | ComputeTaskTimeoutException e) {
            throw new SQLException("Failed to establish connection.", e);
        }
        catch (IgniteException ignored) {
            return false;
        }
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
        return iface != null && iface == Connection.class;
    }

    /** {@inheritDoc} */
    @Override public void setSchema(String schema) throws SQLException {
        cacheName = schema;
    }

    /** {@inheritDoc} */
    @Override public String getSchema() throws SQLException {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public void abort(Executor executor) throws SQLException {
        close();
    }

    /** {@inheritDoc} */
    @Override public void setNetworkTimeout(Executor executor, int ms) throws SQLException {
        throw new SQLFeatureNotSupportedException("Network timeout is not supported.");
    }

    /** {@inheritDoc} */
    @Override public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException("Network timeout is not supported.");
    }

    /**
     * @return Ignite node.
     */
    Ignite ignite() {
        return ignite;
    }

    /**
     * @return Cache name.
     */
    String cacheName() {
        return cacheName;
    }

    /**
     * @return URL.
     */
    String url() {
        return url;
    }

    /**
     * @return Node ID.
     */
    UUID nodeId() {
        return nodeId;
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
     * @return Internal statement.
     * @throws SQLException In case of error.
     */
    JdbcStatement createStatement0() throws SQLException {
        return (JdbcStatement)createStatement();
    }

    /**
     * @param props Properties.
     * @return Concatenated properties.
     */
    private static String nodeKey(Properties props) {
        return props.getProperty(PROP_CFG);
    }

    /**
     * JDBC connection validation task.
     */
    private static class JdbcConnectionValidationTask implements IgniteCallable<Boolean> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Cache name. */
        private final String cacheName;

        /** Ignite. */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * @param cacheName Cache name.
         */
        public JdbcConnectionValidationTask(String cacheName) {
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() {
            GridDiscoveryManager discoMgr = ((IgniteKernal)ignite).context().discovery();

            for (ClusterNode n : ignite.cluster().nodes())
                if (discoMgr.cacheNode(n, cacheName))
                    return true;

            return false;
        }
    }

    /**
     *
     */
    private static class IgniteNodeFuture extends GridFutureAdapter<Ignite> {
        /** Reference count. */
        private final AtomicInteger refCnt = new AtomicInteger(1);

        /**
         *
         */
        public boolean acquire() {
            while (true) {
                int cur = refCnt.get();

                if (cur == 0)
                    return false;

                if (refCnt.compareAndSet(cur, cur + 1))
                    return true;
            }
        }

        /**
         *
         */
        public boolean release() {
            while (true) {
                int cur = refCnt.get();

                assert cur > 0;

                if (refCnt.compareAndSet(cur, cur - 1))
                    // CASed to 0.
                    return cur == 1;
            }
        }
    }
}
