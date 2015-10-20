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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteJdbcDriver;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.compute.ComputeTaskTimeoutException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.IgniteJdbcDriver.PROP_CACHE;
import static org.apache.ignite.IgniteJdbcDriver.PROP_CFG;
import static org.apache.ignite.IgniteJdbcDriver.PROP_COLLOCATED;
import static org.apache.ignite.IgniteJdbcDriver.PROP_LOCAL;
import static org.apache.ignite.IgniteJdbcDriver.PROP_NODE_ID;

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
    private final String cfg;

    /** Cache name. */
    private String cacheName;

    /** Closed flag. */
    private boolean closed;

    /** URL. */
    private String url;

    /** Node ID. */
    private UUID nodeId;

    /** Local query flag. */
    private boolean locQry;

    /** Collocated query flag. */
    private boolean collocatedQry;

    /** Statements. */
    final Set<JdbcStatement> statements = new HashSet<>();

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
        this.locQry = Boolean.parseBoolean(props.getProperty(PROP_LOCAL));
        this.collocatedQry = Boolean.parseBoolean(props.getProperty(PROP_COLLOCATED));

        String nodeIdProp = props.getProperty(PROP_NODE_ID);

        if (nodeIdProp != null)
            this.nodeId = UUID.fromString(nodeIdProp);

        try {
            cfg = props.getProperty(PROP_CFG);

            ignite = getIgnite(cfg);

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
            IgniteNodeFuture fut = NODES.get(cfg);

            if (fut == null) {
                fut = new IgniteNodeFuture();

                IgniteNodeFuture old = NODES.putIfAbsent(cfg, fut);

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
                NODES.remove(cfg, fut);
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

        IgniteNodeFuture fut = NODES.get(cfg);

        if (fut != null && fut.release()) {
            NODES.remove(cfg);

            if (ignite != null)
                ignite.close();
        }

        for (Iterator<JdbcStatement> it = statements.iterator(); it.hasNext();) {
            JdbcStatement stmt = it.next();

            stmt.closeInternal();

            it.remove();
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

        JdbcStatement stmt = new JdbcStatement(this);

        statements.add(stmt);

        return stmt;
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

        JdbcPreparedStatement stmt = new JdbcPreparedStatement(this, sql);

        statements.add(stmt);

        return stmt;
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
            JdbcConnectionValidationTask task = new JdbcConnectionValidationTask(cacheName,
                nodeId == null ? ignite : null);

            if (nodeId != null) {
                ClusterGroup grp = ignite.cluster().forServers().forNodeId(nodeId);

                if (grp.nodes().isEmpty())
                    throw new SQLException("Failed to establish connection with node (is it a server node?): " +
                        nodeId);

                assert grp.nodes().size() == 1;

                if (grp.node().isDaemon())
                    throw new SQLException("Failed to establish connection with node (is it a server node?): " +
                        nodeId);

                IgniteCompute compute = ignite.compute(grp).withAsync();

                compute.call(task);

                return compute.<Boolean>future().get(timeout, SECONDS);
            }
            else
                return task.call();
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
     * @return Local query flag.
     */
    boolean isLocalQuery() {
        return locQry;
    }

    /**
     * @return Collocated query flag.
     */
    boolean isCollocatedQuery() {
        return collocatedQry;
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
         * @param ignite Ignite instance.
         */
        public JdbcConnectionValidationTask(String cacheName, Ignite ignite) {
            this.cacheName = cacheName;
            this.ignite = ignite;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() {
            return ignite.cache(cacheName) != null;
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
