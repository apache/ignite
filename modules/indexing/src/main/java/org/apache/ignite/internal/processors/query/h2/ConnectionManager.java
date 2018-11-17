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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2DefaultTableEngine;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2PlainRowFactory;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.server.web.WebServer;
import org.h2.tools.Server;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_DEBUG_CONSOLE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_DEBUG_CONSOLE_PORT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_INDEXING_CACHE_CLEANUP_PERIOD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_INDEXING_CACHE_THREAD_USAGE_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.IgniteSystemProperties.getString;

/**
 * H2 connection manager.
 */
public class ConnectionManager {
    /** Default DB options. */
    private static final String DB_OPTIONS = ";LOCK_MODE=3;MULTI_THREADED=1;DB_CLOSE_ON_EXIT=FALSE" +
        ";DEFAULT_LOCK_TIMEOUT=10000;FUNCTIONS_IN_SCHEMA=true;OPTIMIZE_REUSE_RESULTS=0;QUERY_CACHE_SIZE=0" +
        ";MAX_OPERATION_MEMORY=0;BATCH_JOINS=1" +
        ";ROW_FACTORY=\"" + GridH2PlainRowFactory.class.getName() + "\"" +
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
        new ThreadLocalObjectPool<>(this::newConnectionWrapper, 5);

    /** Per-thread connections. */
    private final ConcurrentMap<Thread, H2ConnectionWrapper> threadConns = new ConcurrentHashMap<>();

    /** Connection cache. */
    private final ThreadLocal<ThreadLocalObjectPool.Reusable<H2ConnectionWrapper>> threadConn =
        new ThreadLocal<ThreadLocalObjectPool.Reusable<H2ConnectionWrapper>>() {
        @Override public ThreadLocalObjectPool.Reusable<H2ConnectionWrapper> get() {
            ThreadLocalObjectPool.Reusable<H2ConnectionWrapper> reusable = super.get();

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

        @Override protected ThreadLocalObjectPool.Reusable<H2ConnectionWrapper> initialValue() {
            ThreadLocalObjectPool.Reusable<H2ConnectionWrapper> reusableConnection = connPool.borrow();

            threadConns.put(Thread.currentThread(), reusableConnection.object());

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
    public ConnectionManager(GridKernalContext ctx) throws IgniteCheckedException {
        dbUrl = "jdbc:h2:mem:" + ctx.localNodeId() + DB_OPTIONS;

        log = ctx.log(ConnectionManager.class);

        org.h2.Driver.load();

        sysConn = connectionNoCache(QueryUtils.SCHEMA_INFORMATION);

        stmtCleanupTask = ctx.timeout().schedule(new Runnable() {
            @Override public void run() {
                cleanupStatements();
            }
        }, stmtCleanupPeriod, stmtCleanupPeriod);

        connCleanupTask = ctx.timeout().schedule(new Runnable() {
            @Override public void run() {
                cleanupConnections();
            }
        }, CONN_CLEANUP_PERIOD, CONN_CLEANUP_PERIOD);

        startDebugConsole();
    }

    /**
     * Gets DB connection.
     *
     * @param schema Whether to set schema for connection or not.
     * @return DB connection.
     */
    public Connection connectionForThread(@Nullable String schema) {
        H2ConnectionWrapper c = threadConn.get().object();

        if (c == null)
            throw new IgniteSQLException("Failed to get DB connection for thread (check log for details).");

        if (schema != null && !F.eq(c.schema(), schema)) {
            try {
                c.connection().setSchema(schema);
                c.schema(schema);

                if (log.isDebugEnabled())
                    log.debug("Set schema: " + schema);
            }
            catch (SQLException e) {
                throw new IgniteSQLException("Failed to set schema for DB connection for thread [schema=" +
                    schema + "]", e);
            }
        }

        return c.connection();
    }

    /**
     * @return Per-thread connections (for testing purposes only).
     */
    public Map<Thread, H2ConnectionWrapper> connectionsForThread() {
        return threadConns;
    }

    /**
     * Removes from cache and returns associated with current thread connection.
     *
     * @return Connection associated with current thread.
     */
    public ThreadLocalObjectPool.Reusable<H2ConnectionWrapper> detachThreadConnection() {
        Thread key = Thread.currentThread();

        ThreadLocalObjectPool.Reusable<H2ConnectionWrapper> reusableConnection = threadConn.get();

        H2ConnectionWrapper connection = threadConns.remove(key);

        threadConn.remove();

        assert reusableConnection.object().connection() == connection.connection();

        return reusableConnection;
    }

    /**
     * Get connection without cache.
     *
     * @param schema Schema name.
     * @return Connection.
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

        try {
            Connection c = connectionForThread(schema);

            stmt = c.createStatement();

            stmt.executeUpdate(sql);
        }
        catch (SQLException e) {
            onSqlException();

            throw new IgniteSQLException("Failed to execute statement: " + sql, e);
        }
        finally {
            U.close(stmt, log);
        }
    }

    /**
     * Execute statement on H2 INFORMATION_SCHEMA.
     *
     * @param sql SQL statement.
     */
    public void executeSystemStatement(String sql) {
        Statement stmt = null;

        try {
            stmt = sysConn.createStatement();

            stmt.executeUpdate(sql);
        }
        catch (SQLException e) {
            onSqlException();

            throw new IgniteSQLException("Failed to execute system statement: " + sql, e);
        }
        finally {
            U.close(stmt, log);
        }
    }

    /**
     * Clear statement cache when cache is unregistered..
     */
    public void onCacheUnregistered() {
        threadConns.values().forEach(H2ConnectionWrapper::clearStatementCache);
    }

    /**
     * Cancel all queries.
     */
    public void onKernalStop() {
        for (H2ConnectionWrapper c : threadConns.values())
            U.close(c, log);
    }

    /**
     * Close executor.
     */
    public void stop() {
        for (H2ConnectionWrapper c : threadConns.values())
            U.close(c, log);

        threadConns.clear();

        try (Connection c = connectionNoCache(QueryUtils.SCHEMA_INFORMATION); Statement s = c.createStatement()) {
            s.execute("SHUTDOWN");
        }
        catch (SQLException e) {
            U.error(log, "Failed to shutdown database.", e);
        }

        if (stmtCleanupTask != null)
            stmtCleanupTask.close();

        if (connCleanupTask != null)
            connCleanupTask.close();

        if (sysConn != null) {
            U.close(sysConn, log);

            sysConn = null;
        }
    }

    /**
     * Handles SQL exception.
     */
    public void onSqlException() {
        Connection conn = threadConn.get().object().connection();

        threadConn.set(null);

        if (conn != null) {
            threadConns.remove(Thread.currentThread());

            // Reset connection to receive new one at next call.
            U.close(conn, log);
        }
    }

    /**
     * Start debug console if needed.
     *
     * @throws IgniteCheckedException If failed.
     */
    private void startDebugConsole() throws IgniteCheckedException {
        try {
            if (getString(IGNITE_H2_DEBUG_CONSOLE) != null) {
                Connection c = DriverManager.getConnection(dbUrl);

                int port = getInteger(IGNITE_H2_DEBUG_CONSOLE_PORT, 0);

                WebServer webSrv = new WebServer();
                Server web = new Server(webSrv, "-webPort", Integer.toString(port));
                web.start();
                String url = webSrv.addSession(c);

                U.quietAndInfo(log, "H2 debug console URL: " + url);

                try {
                    Server.openBrowser(url);
                }
                catch (Exception e) {
                    U.warn(log, "Failed to open browser: " + e.getMessage());
                }
            }
        }
        catch (SQLException e) {
            throw new IgniteCheckedException(e);
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
     * Called periodically to cleanup connections.
     */
    private void cleanupConnections() {
        for (Iterator<Map.Entry<Thread, H2ConnectionWrapper>> it = threadConns.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Thread, H2ConnectionWrapper> entry = it.next();

            Thread t = entry.getKey();

            if (t.getState() == Thread.State.TERMINATED) {
                U.close(entry.getValue(), log);

                it.remove();
            }
        }
    }

    /**
     * Called periodically to clean up the statement cache.
     */
    private void cleanupStatements() {
        long now = U.currentTimeMillis();

        for (Iterator<Map.Entry<Thread, H2ConnectionWrapper>> it = threadConns.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Thread, H2ConnectionWrapper> entry = it.next();

            Thread t = entry.getKey();

            if (t.getState() == Thread.State.TERMINATED) {
                U.close(entry.getValue(), log);

                it.remove();
            }
            else if (now - entry.getValue().statementCache().lastUsage() > stmtTimeout)
                entry.getValue().clearStatementCache();
        }
    }
}
