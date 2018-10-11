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

import java.net.URL;
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
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteJdbcDriver;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.compute.ComputeTaskTimeoutException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
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
import static org.apache.ignite.IgniteJdbcDriver.PROP_DISTRIBUTED_JOINS;
import static org.apache.ignite.IgniteJdbcDriver.PROP_ENFORCE_JOIN_ORDER;
import static org.apache.ignite.IgniteJdbcDriver.PROP_LAZY;
import static org.apache.ignite.IgniteJdbcDriver.PROP_LOCAL;
import static org.apache.ignite.IgniteJdbcDriver.PROP_MULTIPLE_STMTS;
import static org.apache.ignite.IgniteJdbcDriver.PROP_NODE_ID;
import static org.apache.ignite.IgniteJdbcDriver.PROP_STREAMING;
import static org.apache.ignite.IgniteJdbcDriver.PROP_STREAMING_ALLOW_OVERWRITE;
import static org.apache.ignite.IgniteJdbcDriver.PROP_STREAMING_FLUSH_FREQ;
import static org.apache.ignite.IgniteJdbcDriver.PROP_STREAMING_PER_NODE_BUF_SIZE;
import static org.apache.ignite.IgniteJdbcDriver.PROP_STREAMING_PER_NODE_PAR_OPS;
import static org.apache.ignite.IgniteJdbcDriver.PROP_TX_ALLOWED;
import static org.apache.ignite.IgniteJdbcDriver.PROP_SKIP_REDUCER_ON_UPDATE;
import static org.apache.ignite.internal.jdbc2.JdbcUtils.convertToSqlException;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.createJdbcSqlException;

/**
 * JDBC connection implementation.
 */
public class JdbcConnection implements Connection {
    /** Null stub. */
    private static final String NULL = "null";

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
    private final String cacheName;

    /** Schema name. */
    private String schemaName;

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

    /** Distributed joins flag. */
    private boolean distributedJoins;

    /** Enforced join order flag. */
    private boolean enforceJoinOrder;

    /** Lazy query execution flag. */
    private boolean lazy;

    /** Transactions allowed flag. */
    private boolean txAllowed;

    /** Current transaction isolation. */
    private int txIsolation;

    /** Make this connection streaming oriented, and prepared statements - data streamer aware. */
    private final boolean stream;

    /** Auto flush frequency for streaming. */
    private final long streamFlushTimeout;

    /** Node buffer size for data streamer. */
    private final int streamNodeBufSize;

    /** Parallel ops count per node for data streamer. */
    private final int streamNodeParOps;

    /** Allow overwrites for duplicate keys on streamed {@code INSERT}s. */
    private final boolean streamAllowOverwrite;

    /** Allow queries with multiple statements. */
    private final boolean multipleStmts;

    /** Skip reducer on update flag. */
    private final boolean skipReducerOnUpdate;

    /** Statements. */
    final Set<JdbcStatement> statements = new HashSet<>();

    /**
     * Creates new connection.
     *
     * @param url Connection URL.
     * @param props Additional properties.
     * @throws SQLException In case Ignite node failed to start.
     */
    @SuppressWarnings("unchecked")
    public JdbcConnection(String url, Properties props) throws SQLException {
        assert url != null;
        assert props != null;

        this.url = url;

        cacheName = props.getProperty(PROP_CACHE);
        locQry = Boolean.parseBoolean(props.getProperty(PROP_LOCAL));
        collocatedQry = Boolean.parseBoolean(props.getProperty(PROP_COLLOCATED));
        distributedJoins = Boolean.parseBoolean(props.getProperty(PROP_DISTRIBUTED_JOINS));
        enforceJoinOrder = Boolean.parseBoolean(props.getProperty(PROP_ENFORCE_JOIN_ORDER));
        lazy = Boolean.parseBoolean(props.getProperty(PROP_LAZY));
        txAllowed = Boolean.parseBoolean(props.getProperty(PROP_TX_ALLOWED));

        stream = Boolean.parseBoolean(props.getProperty(PROP_STREAMING));

        if (stream && cacheName == null) {
            throw new SQLException("Cache name cannot be null when streaming is enabled.",
                SqlStateCode.CLIENT_CONNECTION_FAILED);
        }

        streamAllowOverwrite = Boolean.parseBoolean(props.getProperty(PROP_STREAMING_ALLOW_OVERWRITE));
        streamFlushTimeout = Long.parseLong(props.getProperty(PROP_STREAMING_FLUSH_FREQ, "0"));
        streamNodeBufSize = Integer.parseInt(props.getProperty(PROP_STREAMING_PER_NODE_BUF_SIZE,
            String.valueOf(IgniteDataStreamer.DFLT_PER_NODE_BUFFER_SIZE)));
        // If value is zero, server data-streamer pool size multiplied
        // by IgniteDataStreamer.DFLT_PARALLEL_OPS_MULTIPLIER will be used
        streamNodeParOps = Integer.parseInt(props.getProperty(PROP_STREAMING_PER_NODE_PAR_OPS, "0"));

        multipleStmts = Boolean.parseBoolean(props.getProperty(PROP_MULTIPLE_STMTS));
        skipReducerOnUpdate = Boolean.parseBoolean(props.getProperty(PROP_SKIP_REDUCER_ON_UPDATE));

        String nodeIdProp = props.getProperty(PROP_NODE_ID);

        if (nodeIdProp != null)
            nodeId = UUID.fromString(nodeIdProp);

        try {
            String cfgUrl = props.getProperty(PROP_CFG);

            cfg = cfgUrl == null || cfgUrl.isEmpty() ? NULL : cfgUrl;

            ignite = getIgnite(cfg);

            if (!isValid(2)) {
                throw new SQLException("Client is invalid. Probably cache name is wrong.",
                    SqlStateCode.CLIENT_CONNECTION_FAILED);
            }

            if (cacheName != null) {
                DynamicCacheDescriptor cacheDesc = ignite().context().cache().cacheDescriptor(cacheName);

                if (cacheDesc == null) {
                    throw createJdbcSqlException("Cache doesn't exist: " + cacheName,
                        IgniteQueryErrorCode.CACHE_NOT_FOUND);
                }

                schemaName = QueryUtils.normalizeSchemaName(cacheName, cacheDesc.cacheConfiguration().getSqlSchema());
            }
            else
                schemaName = QueryUtils.DFLT_SCHEMA;
        }
        catch (Exception e) {
            close();

            throw convertToSqlException(e, "Failed to start Ignite node. " + e.getMessage(), SqlStateCode.CLIENT_CONNECTION_FAILED);
        }
    }

    /**
     * @param cfgUrl Config url.
     * @return Ignite client node.
     * @throws IgniteCheckedException On error.
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
                        final IgniteBiTuple<IgniteConfiguration, ? extends GridSpringResourceContext> cfgAndCtx;

                        String jdbcName = "ignite-jdbc-driver-" + UUID.randomUUID().toString();

                        if (NULL.equals(cfg)) {
                            URL url = U.resolveIgniteUrl(IgnitionEx.DFLT_CFG);

                            if (url != null)
                                cfgAndCtx = loadConfiguration(IgnitionEx.DFLT_CFG, jdbcName);
                            else {
                                U.warn(null, "Default Spring XML file not found (is IGNITE_HOME set?): "
                                    + IgnitionEx.DFLT_CFG);

                                IgniteConfiguration cfg = new IgniteConfiguration()
                                    .setIgniteInstanceName(jdbcName)
                                    .setClientMode(true);

                                cfgAndCtx = new IgniteBiTuple<>(cfg, null);
                            }
                        }
                        else
                            cfgAndCtx = loadConfiguration(cfgUrl, jdbcName);

                        fut.onDone(IgnitionEx.start(cfgAndCtx.get1(), cfgAndCtx.get2()));
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
     * @param jdbcName Appended to instance name or used as default.
     * @return Ignite config and Spring context.
     */
    private IgniteBiTuple<IgniteConfiguration, ? extends GridSpringResourceContext> loadConfiguration(String cfgUrl,
        String jdbcName) {
        try {
            IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> cfgMap =
                IgnitionEx.loadConfigurations(cfgUrl);

            IgniteConfiguration cfg = F.first(cfgMap.get1());

            if (cfg.getIgniteInstanceName() == null)
                cfg.setIgniteInstanceName(jdbcName);
            else
                cfg.setIgniteInstanceName(cfg.getIgniteInstanceName() + "-" + jdbcName);

            cfg.setClientMode(true); // Force client mode.

            return new IgniteBiTuple<>(cfg, cfgMap.getValue());
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

        if (!txAllowed && !autoCommit)
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

        if (!txAllowed)
            throw new SQLFeatureNotSupportedException("Transactions are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void rollback() throws SQLException {
        ensureNotClosed();

        if (!txAllowed)
            throw new SQLFeatureNotSupportedException("Transactions are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void close() throws SQLException {
        if (closed)
            return;

        closed = true;

        for (Iterator<JdbcStatement> it = statements.iterator(); it.hasNext();) {
            JdbcStatement stmt = it.next();

            stmt.closeInternal();

            it.remove();
        }

        IgniteNodeFuture fut = NODES.get(cfg);

        if (fut != null && fut.release()) {
            NODES.remove(cfg);

            if (ignite != null)
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

        if (txAllowed)
            txIsolation = level;
        else
            throw new SQLFeatureNotSupportedException("Transactions are not supported.");
    }

    /** {@inheritDoc} */
    @Override public int getTransactionIsolation() throws SQLException {
        ensureNotClosed();

        if (txAllowed)
            return txIsolation;
        else
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

        if (!txAllowed && holdability != HOLD_CURSORS_OVER_COMMIT)
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
    @Override public Statement createStatement(int resSetType, int resSetConcurrency,
        int resSetHoldability) throws SQLException {
        ensureNotClosed();

        if (resSetType != TYPE_FORWARD_ONLY)
            throw new SQLFeatureNotSupportedException("Invalid result set type (only forward is supported.)");

        if (resSetConcurrency != CONCUR_READ_ONLY)
            throw new SQLFeatureNotSupportedException("Invalid concurrency (updates are not supported).");

        if (!txAllowed && resSetHoldability != HOLD_CURSORS_OVER_COMMIT)
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

        if (!txAllowed && resSetHoldability != HOLD_CURSORS_OVER_COMMIT)
            throw new SQLFeatureNotSupportedException("Invalid holdability (transactions are not supported).");

        JdbcPreparedStatement stmt;

        if (!stream)
            stmt = new JdbcPreparedStatement(this, sql);
        else {
            GridQueryIndexing idx = ignite().context().query().getIndexing();

            PreparedStatement nativeStmt = prepareNativeStatement(sql);

            try {
                idx.checkStatementStreamable(nativeStmt);
            }
            catch (IgniteSQLException e) {
                throw e.toJdbcException();
            }

            IgniteDataStreamer streamer = ignite().dataStreamer(cacheName);

            streamer.autoFlushFrequency(streamFlushTimeout);
            streamer.allowOverwrite(streamAllowOverwrite);

            if (streamNodeBufSize > 0)
                streamer.perNodeBufferSize(streamNodeBufSize);

            if (streamNodeParOps > 0)
                streamer.perNodeParallelOperations(streamNodeParOps);

            stmt = new JdbcStreamedPreparedStatement(this, sql, streamer, nativeStmt);
        }

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

        return new JdbcBlob(new byte[0]);
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

                return ignite.compute(grp).callAsync(task).get(timeout, SECONDS);
            }
            else
                return task.call();
        }
        catch (IgniteClientDisconnectedException | ComputeTaskTimeoutException e) {
            throw new SQLException("Failed to establish connection.", SqlStateCode.CONNECTION_FAILURE, e);
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
    @Override public void setSchema(String schemaName) throws SQLException {
        this.schemaName = schemaName;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public String getSchema() throws SQLException {
        return schemaName;
    }

    /**
     * @return Normalized schema name.
     */
    public String schemaName() {
        return F.isEmpty(schemaName) ? QueryUtils.DFLT_SCHEMA : schemaName;
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
    IgniteKernal ignite() {
        return (IgniteKernal)ignite;
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
     * @return {@code true} if target node has DML support, {@code false} otherwise.
     */
    boolean isDmlSupported() {
        return ignite.version().greaterThanEqual(1, 8, 0);
    }

    /**
     * @return {@code true} if multiple statements allowed, {@code false} otherwise.
     */
    boolean isMultipleStatementsAllowed() {
        return multipleStmts;
    }

    /**
     * @return {@code true} if update on server is enabled, {@code false} otherwise.
     */
    boolean skipReducerOnUpdate() {
        return skipReducerOnUpdate;
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
     * @return Distributed joins flag.
     */
    boolean isDistributedJoins() {
        return distributedJoins;
    }

    /**
     * @return Enforce join order flag.
     */
    boolean isEnforceJoinOrder() {
        return enforceJoinOrder;
    }

    /**
     * @return Lazy query execution flag.
     */
    boolean isLazy() {
        return lazy;
    }

    /**
     * Ensures that connection is not closed.
     *
     * @throws SQLException If connection is closed.
     */
    private void ensureNotClosed() throws SQLException {
        if (closed)
            throw new SQLException("Connection is closed.", SqlStateCode.CONNECTION_CLOSED);
    }

    /**
     * @return Internal statement.
     * @throws SQLException In case of error.
     */
    JdbcStatement createStatement0() throws SQLException {
        return (JdbcStatement)createStatement();
    }

    /**
     * @param sql Query.
     * @return {@link PreparedStatement} from underlying engine to supply metadata to Prepared - most likely H2.
     * @throws SQLException On error.
     */
    PreparedStatement prepareNativeStatement(String sql) throws SQLException {
        return ignite().context().query().prepareNativeStatement(schemaName(), sql);
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
            return cacheName == null || ignite.cache(cacheName) != null;
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
