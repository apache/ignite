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

package org.apache.ignite.internal.processors.query.h2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2DefaultTableEngine;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2PlainRowFactory;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.jdbc.JdbcStatement;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_INDEXING_CACHE_CLEANUP_PERIOD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_INDEXING_CACHE_THREAD_USAGE_TIMEOUT;

/**
 * H2 connection manager.
 */
public class ConnectionManager {
    /** Default DB options. */
    private static final String DB_OPTIONS = ";LOCK_MODE=3;MULTI_THREADED=1;DB_CLOSE_ON_EXIT=FALSE" +
        ";DEFAULT_LOCK_TIMEOUT=10000;FUNCTIONS_IN_SCHEMA=true;OPTIMIZE_REUSE_RESULTS=0;QUERY_CACHE_SIZE=0" +
        ";MAX_OPERATION_MEMORY=0;BATCH_JOINS=1" +
        ";ROW_FACTORY=\"" + H2PlainRowFactory.class.getName() + "\"" +
        ";DEFAULT_TABLE_ENGINE=" + GridH2DefaultTableEngine.class.getName();

    /** The period of clean up the {@link #threadConns}. */
    private static final Long CONN_CLEANUP_PERIOD = 2000L;

    /** The period of clean up the statement cache. */
    @SuppressWarnings("FieldCanBeLocal")
    private final Long stmtCleanupPeriod = Long.getLong(IGNITE_H2_INDEXING_CACHE_CLEANUP_PERIOD, 10_000);

    /** The timeout to remove entry from the statement cache if the thread doesn't perform any queries. */
    private final Long stmtTimeout = Long.getLong(IGNITE_H2_INDEXING_CACHE_THREAD_USAGE_TIMEOUT, 600 * 1000);

    /*
     * Initialize system properties for H2.
     */
    static {
        System.setProperty("h2.objectCache", "false");
        System.setProperty("h2.serializeJavaObject", "false");
        System.setProperty("h2.objectCacheMaxPerElementSize", "0"); // Avoid ValueJavaObject caching.
        System.setProperty("h2.optimizeTwoEquals", "false"); // Makes splitter fail on subqueries in WHERE.
        System.setProperty("h2.dropRestrict", "false"); // Drop schema with cascade semantics.
    }

    /** Shared connection pool. */
    private final ThreadLocalObjectPool<H2ConnectionWrapper> connPool =
        new ThreadLocalObjectPool<>(
            5,
            this::newConnectionWrapper,
            this::closeDetachedConnection,
            this::addConnectionToThreaded);

    /** Per-thread connections. */
    private final ConcurrentMap<Thread, ConcurrentMap<H2ConnectionWrapper, Boolean>> threadConns = new ConcurrentHashMap<>();

    /** Track detached connections to close on node stop. */
    private final ConcurrentMap<H2ConnectionWrapper, Boolean> detachedConns = new ConcurrentHashMap<>();

    /** Connection cache. */
    private final ThreadLocal<ThreadLocalObjectPool<H2ConnectionWrapper>.Reusable> threadConn =
        new ThreadLocal<ThreadLocalObjectPool<H2ConnectionWrapper>.Reusable>() {
        @Override public ThreadLocalObjectPool<H2ConnectionWrapper>.Reusable get() {
            ThreadLocalObjectPool<H2ConnectionWrapper>.Reusable reusable = super.get();

            boolean reconnect = true;

            try {
                reconnect = reusable == null || reusable.object().connection().isClosed();
            }
            catch (SQLException e) {
                U.warn(log, "Failed to check connection status.", e);
            }

            if (reconnect) {
                reusable = initialValue();

                set(reusable);
            }

            return reusable;
        }

        @Override protected ThreadLocalObjectPool<H2ConnectionWrapper>.Reusable initialValue() {
            ThreadLocalObjectPool<H2ConnectionWrapper>.Reusable reusableConnection = connPool.borrow();

            addConnectionToThreaded(reusableConnection.object());

            return reusableConnection;
        }
    };

    /** Database URL. */
    private final String dbUrl;

    /** Connection cleanup task. */
    private final GridTimeoutProcessor.CancelableTask connCleanupTask;

    /** Statement cleanup task. */
    private final GridTimeoutProcessor.CancelableTask stmtCleanupTask;

    /** H2 connection for INFORMATION_SCHEMA. Holds H2 open until node is stopped. */
    private volatile Connection sysConn;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    public ConnectionManager(GridKernalContext ctx) {
        dbUrl = "jdbc:h2:mem:" + ctx.localNodeId() + DB_OPTIONS;

        log = ctx.log(ConnectionManager.class);

        org.h2.Driver.load();

        sysConn = connectionNoCache(QueryUtils.SCHEMA_INFORMATION);

        stmtCleanupTask = ctx.timeout().schedule(this::cleanupStatements, stmtCleanupPeriod, stmtCleanupPeriod);
        connCleanupTask = ctx.timeout().schedule(this::cleanupConnections, CONN_CLEANUP_PERIOD, CONN_CLEANUP_PERIOD);
    }

    /**
     * @return H2 connection wrapper.
     */
    public H2ConnectionWrapper connectionForThread() {
        return threadConn.get().object();
    }

    /**
     * @return Per-thread connections (for testing purposes only).
     */
    public Map<Thread, ConcurrentMap<H2ConnectionWrapper, Boolean>> connectionsForThread() {
        return threadConns;
    }

    /**
     * Removes from cache and returns associated with current thread connection.
     *
     * @return Connection associated with current thread.
     */
    public ThreadLocalObjectPool<H2ConnectionWrapper>.Reusable detachThreadConnection() {
        Thread key = Thread.currentThread();

        ThreadLocalObjectPool<H2ConnectionWrapper>.Reusable reusableConn = threadConn.get();

        ConcurrentMap<H2ConnectionWrapper, Boolean> connSet = threadConns.get(key);

        assert connSet != null;

        Boolean rmv = connSet.remove(reusableConn.object());

        assert rmv != null;

        threadConn.remove();

        detachedConns.putIfAbsent(reusableConn.object(), false);

        return reusableConn;
    }

    /**
     * Get connection without cache.
     *
     * @param schema Schema name.
     * @return Connection.
     * @throws IgniteSQLException On error.
     */
    public Connection connectionNoCache(String schema) throws IgniteSQLException {
        try {
            Connection conn = DriverManager.getConnection(dbUrl);

            conn.setSchema(schema);

            return conn;
        }
        catch (SQLException e) {
            throw new IgniteSQLException("Failed to initialize system DB connection: " + dbUrl, e);
        }
    }

    /**
     * @return {@link H2StatementCache} associated with current thread.
     */
    public H2StatementCache statementCacheForThread() {
        H2StatementCache statementCache = threadConn.get().object().statementCache();

        statementCache.updateLastUsage();

        return statementCache;
    }

    /**
     * Execute SQL statement on specific schema.
     *
     * @param schema Schema
     * @param sql SQL statement.
     * @throws IgniteCheckedException If failed.
     */
    public void executeStatement(String schema, String sql) throws IgniteCheckedException {
        Statement stmt = null;

        Connection c = null;

        try {
            c = connectionForThread().connection(schema);

            stmt = c.createStatement();

            stmt.executeUpdate(sql);
        }
        catch (SQLException e) {
            onSqlException(c);

            throw new IgniteCheckedException("Failed to execute statement: " + sql, e);
        }
        finally {
            U.close(stmt, log);
        }
    }

    /**
     * Execute statement on H2 INFORMATION_SCHEMA.
     *
     * @param sql SQL statement.
     * @throws IgniteCheckedException On error.
     */
    public void executeSystemStatement(String sql) throws IgniteCheckedException {
        Statement stmt = null;

        try {
            stmt = sysConn.createStatement();

            stmt.executeUpdate(sql);
        }
        catch (SQLException e) {
            U.close(sysConn, log);

            throw new IgniteCheckedException("Failed to execute system statement: " + sql, e);
        }
        finally {
            U.close(stmt, log);
        }
    }

    /**
     * Get cached prepared statement (if any).
     *
     * @param c Connection.
     * @param sql SQL.
     * @return Prepared statement or {@code null}.
     * @throws SQLException On error.
     */
    @Nullable public PreparedStatement cachedPreparedStatement(Connection c, String sql) throws SQLException {
        H2StatementCache cache = statementCacheForThread();

        H2CachedStatementKey key = new H2CachedStatementKey(c.getSchema(), sql);

        PreparedStatement stmt = cache.get(key);

        // Nothing found.
        if (stmt == null)
            return null;

        // TODO: Remove thread local caching at all. Just keep per-connection statement cache.
        // TODO: https://issues.apache.org/jira/browse/IGNITE-11211
        // Statement is not from the given connection.
        if (stmt.getConnection() != c)
            return null;

        // Is statement still valid?
        if (
            stmt.isClosed() ||                                 // Closed.
            stmt.unwrap(JdbcStatement.class).isCancelled() ||  // Cancelled.
            GridSqlQueryParser.prepared(stmt).needRecompile() // Outdated (schema has been changed concurrently).
        )
            return null;

        return stmt;
    }

    /**
     * Prepare statement caching it if needed.
     *
     * @param c Connection.
     * @param sql SQL.
     * @return Prepared statement.
     * @throws SQLException If failed.
     */
    public PreparedStatement prepareStatement(Connection c, String sql) throws SQLException {
        PreparedStatement stmt = cachedPreparedStatement(c, sql);

        if (stmt == null) {
            H2StatementCache cache = statementCacheForThread();

            H2CachedStatementKey key = new H2CachedStatementKey(c.getSchema(), sql);

            stmt = prepareStatementNoCache(c, sql);

            cache.put(key, stmt);
        }

        return stmt;
    }

    /**
     * Get prepared statement without caching.
     *
     * @param c Connection.
     * @param sql SQL.
     * @return Prepared statement.
     * @throws SQLException If failed.
     */
    public PreparedStatement prepareStatementNoCache(Connection c, String sql) throws SQLException {
        boolean insertHack = GridH2Table.insertHackRequired(sql);

        if (insertHack) {
            GridH2Table.insertHack(true);

            try {
                return c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            }
            finally {
                GridH2Table.insertHack(false);
            }
        }
        else
            return c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }

    /**
     * Clear statement cache when cache is unregistered..
     */
    public void onCacheDestroyed() {
        threadConns.values().forEach(set -> set.keySet().forEach(H2ConnectionWrapper::clearStatementCache));
    }

    /**
     * Close all connections.
     */
    private void closeConnections() {
        threadConns.values().forEach(set -> set.keySet().forEach(U::closeQuiet));
        detachedConns.keySet().forEach(U::closeQuiet);

        threadConns.clear();
        detachedConns.clear();
    }

    /**
     * Cancel all queries.
     */
    public void onKernalStop() {
        closeConnections();
    }

    /**
     * Close executor.
     */
    public void stop() {
        if (stmtCleanupTask != null)
            stmtCleanupTask.close();

        if (connCleanupTask != null)
            connCleanupTask.close();

        // Needs to be released before SHUTDOWN.
        closeConnections();

        try (Connection c = connectionNoCache(QueryUtils.SCHEMA_INFORMATION); Statement s = c.createStatement()) {
            s.execute("SHUTDOWN");
        }
        catch (SQLException e) {
            U.error(log, "Failed to shutdown database.", e);
        }

        if (sysConn != null) {
            U.close(sysConn, log);

            sysConn = null;
        }
    }

    /**
     * Handles SQL exception.
     * @param c Connection to close.
     */
    public void onSqlException(Connection c) {
        H2ConnectionWrapper conn = threadConn.get().object();

        // Clear thread local cache if connection not detached.
        if (conn.connection() == c)
            threadConn.remove();

        if (c != null) {
            threadConns.remove(Thread.currentThread());

            // Reset connection to receive new one at next call.
            U.close(c, log);
        }
    }

    /**
     * Create new connection wrapper.
     *
     * @return Connection wrapper.
     */
    private H2ConnectionWrapper newConnectionWrapper() {
        try {
            return new H2ConnectionWrapper(DriverManager.getConnection(dbUrl));
        }
        catch (SQLException e) {
            throw new IgniteSQLException("Failed to initialize DB connection: " + dbUrl, e);
        }
    }

    /**
     * Called by connection bool on connection recycle.
     *
     * @param conn recycled connection.
     */
    private void addConnectionToThreaded(H2ConnectionWrapper conn) {
        Thread cur = Thread.currentThread();

        ConcurrentMap<H2ConnectionWrapper, Boolean> setConn = threadConns.get(cur);

        if (setConn == null) {
            setConn = new ConcurrentHashMap<>();

            threadConns.putIfAbsent(cur, setConn);
        }

        setConn.put(conn, false);

        detachedConns.remove(conn);
    }

    /**
     * Called by connection bool on connection close.
     *
     * @param conn closed connection.
     */
    private void closeDetachedConnection(H2ConnectionWrapper conn) {
        U.close(conn, log);

        detachedConns.remove(conn);
    }

    /**
     * Called periodically to cleanup connections.
     */
    private void cleanupConnections() {
        threadConns.entrySet().removeIf(e -> {
            Thread t = e.getKey();

            if (t.getState() == Thread.State.TERMINATED) {
                e.getValue().keySet().forEach(c -> U.close(c, log));

                return true;
            }

            return false;
        });
    }

    /**
     * Called periodically to clean up the statement cache.
     */
    private void cleanupStatements() {
        long now = U.currentTimeMillis();

        threadConns.values().forEach(set -> set.keySet().forEach(c ->{
            if (now - c.statementCache().lastUsage() > stmtTimeout)
                c.clearStatementCache();
        }));
    }
}
