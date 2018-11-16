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
import org.apache.ignite.IgniteException;
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
 * H2 query executor.
 */
public class H2QueryExecutor {
    /** Default DB options. */
    private static final String DB_OPTIONS = ";LOCK_MODE=3;MULTI_THREADED=1;DB_CLOSE_ON_EXIT=FALSE" +
        ";DEFAULT_LOCK_TIMEOUT=10000;FUNCTIONS_IN_SCHEMA=true;OPTIMIZE_REUSE_RESULTS=0;QUERY_CACHE_SIZE=0" +
        ";MAX_OPERATION_MEMORY=0;BATCH_JOINS=1" +
        ";ROW_FACTORY=\"" + GridH2PlainRowFactory.class.getName() + "\"" +
        ";DEFAULT_TABLE_ENGINE=" + GridH2DefaultTableEngine.class.getName();

    /** The period of clean up the {@link #threadConns}. */
    private static final Long CONN_CLEANUP_PERIOD = 2000L;

    /** The period of clean up the statement cache. */
    private static final Long STMT_CLEANUP_PERIOD = Long.getLong(IGNITE_H2_INDEXING_CACHE_CLEANUP_PERIOD, 10_000);

    /** The timeout to remove entry from the statement cache if the thread doesn't perform any queries. */
    private static final Long STMT_TIMEOUT = Long.getLong(IGNITE_H2_INDEXING_CACHE_THREAD_USAGE_TIMEOUT, 600 * 1000);

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
    // TODO: No anonymous stuff.
    private final ThreadLocal<ThreadLocalObjectPool.Reusable<H2ConnectionWrapper>> threadConn = new ThreadLocal<ThreadLocalObjectPool.Reusable<H2ConnectionWrapper>>() {
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
    public H2QueryExecutor(GridKernalContext ctx) throws IgniteCheckedException {
        dbUrl = "jdbc:h2:mem:" + ctx.localNodeId() + DB_OPTIONS;

        log = ctx.log(H2QueryExecutor.class);

        org.h2.Driver.load();

        sysConn = connectionNoCache(QueryUtils.SCHEMA_IS);

        startDebugConsole();

        stmtCleanupTask = ctx.timeout().schedule(new Runnable() {
            @Override public void run() {
                cleanupStatementCache();
            }
        }, STMT_CLEANUP_PERIOD, STMT_CLEANUP_PERIOD);

        connCleanupTask = ctx.timeout().schedule(new Runnable() {
            @Override public void run() {
                cleanupConnections();
            }
        }, CONN_CLEANUP_PERIOD, CONN_CLEANUP_PERIOD);
    }

    /** */
    private H2ConnectionWrapper newConnectionWrapper() {
        try {
            return new H2ConnectionWrapper(DriverManager.getConnection(dbUrl));
        }
        catch (SQLException e) {
            throw new IgniteSQLException("Failed to initialize DB connection: " + dbUrl, e);
        }
    }

    /**
     * Cancel all queries.
     */
    public void cancelAllQueries() {
        for (H2ConnectionWrapper c : threadConns.values())
            U.close(c, log);
    }

    /**
     * @return Per-thread connections (for testing purposes only).
     */
    public Map<Thread, ?> perThreadConnections() {
        return threadConns;
    }

    /**
     * Called periodically by {@link GridTimeoutProcessor} to clean up the {@link #threadConns}.
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
     * Called periodically by {@link GridTimeoutProcessor} to clean up the statement cache.
     */
    private void cleanupStatementCache() {
        long now = U.currentTimeMillis();

        for (Iterator<Map.Entry<Thread, H2ConnectionWrapper>> it = threadConns.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Thread, H2ConnectionWrapper> entry = it.next();

            Thread t = entry.getKey();

            if (t.getState() == Thread.State.TERMINATED) {
                U.close(entry.getValue(), log);

                it.remove();
            }
            else if (now - entry.getValue().statementCache().lastUsage() > STMT_TIMEOUT)
                entry.getValue().clearStatementCache();
        }
    }

    /**
     * @return {@link H2StatementCache} associated with current thread.
     */
    public H2StatementCache getStatementsCacheForCurrentThread() {
        H2StatementCache statementCache = threadConn.get().object().statementCache();

        statementCache.updateLastUsage();

        return statementCache;
    }

    /**
     * Gets DB connection.
     *
     * @param schema Whether to set schema for connection or not.
     * @return DB connection.
     * @throws IgniteCheckedException In case of error.
     */
    public Connection connectionForThread(@Nullable String schema) throws IgniteCheckedException {
        H2ConnectionWrapper c = threadConn.get().object();

        if (c == null)
            throw new IgniteCheckedException("Failed to get DB connection for thread (check log for details).");

        if (schema != null && !F.eq(c.schema(), schema)) {
            Statement stmt = null;

            try {
                stmt = c.connection().createStatement();

                // TODO: Could we use c.connection().schema() instead?
                stmt.executeUpdate("SET SCHEMA " + H2Utils.withQuotes(schema));

                if (log.isDebugEnabled())
                    log.debug("Set schema: " + schema);

                c.schema(schema);
            }
            catch (SQLException e) {
                throw new IgniteSQLException("Failed to set schema for DB connection for thread [schema=" +
                    schema + "]", e);
            }
            finally {
                U.close(stmt, log);
            }
        }

        return c.connection();
    }

    /**
     * @param schema Schema.
     * @return Connection.
     */
    public Connection connectionForSchema(String schema) {
        try {
            return connectionForThread(schema);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @return H2 JDBC connection to INFORMATION_SCHEMA.
     */
    public Connection systemConnection() {
        // TODO: Initialize in ctor
        if (sysConn == null)
            sysConn = connectionNoCache(QueryUtils.SCHEMA_IS);

        return sysConn;
    }

    /**
     * Execute statement on H2 INFORMATION_SCHEMA.
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
     * Removes from cache and returns associated with current thread connection.
     * @return Connection associated with current thread.
     */
    public ThreadLocalObjectPool.Reusable<H2ConnectionWrapper> detach() {
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

    public void clearStatementCache() {
        threadConns.values().forEach(H2ConnectionWrapper::clearStatementCache);
    }

    public void closeConnections() {
        for (H2ConnectionWrapper c : threadConns.values())
            U.close(c, log);

        threadConns.clear();
    }

    /**
     * Close executor.
     */
    public void close() {
        try (Connection c = DriverManager.getConnection(dbUrl); Statement s = c.createStatement()) {
            s.execute("SHUTDOWN");
        }
        catch (SQLException e) {
            U.error(log, "Failed to shutdown database.", e);
        }

        if (stmtCleanupTask != null)
            stmtCleanupTask.close();

        if (connCleanupTask != null)
            connCleanupTask.close();
    }

    /**
     * Close system connection.
     */
    public void closeSystemConnection() {
        // TODO: Merge.
        if (sysConn != null) {
            U.close(sysConn, log);

            sysConn = null;
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
}
