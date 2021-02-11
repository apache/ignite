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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2DefaultTableEngine;
import org.apache.ignite.internal.processors.query.h2.opt.H2PlainRowFactory;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.api.JavaObjectSerializer;
import org.h2.engine.Database;
import org.h2.jdbc.JdbcConnection;
import org.h2.store.DataHandler;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_INDEXING_CACHE_CLEANUP_PERIOD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_INDEXING_CACHE_THREAD_USAGE_TIMEOUT;
import static org.apache.ignite.internal.processors.query.h2.H2Utils.setter;

/**
 * H2 connection manager.
 */
public class ConnectionManager {
    /** Default DB options. */
    private static final String DEFAULT_DB_OPTIONS = ";LOCK_MODE=3;MULTI_THREADED=1;DB_CLOSE_ON_EXIT=FALSE" +
        ";DEFAULT_LOCK_TIMEOUT=10000;FUNCTIONS_IN_SCHEMA=true;OPTIMIZE_REUSE_RESULTS=0;QUERY_CACHE_SIZE=0" +
        ";MAX_OPERATION_MEMORY=0;BATCH_JOINS=1" +
        ";ROW_FACTORY=\"" + H2PlainRowFactory.class.getName() + "\"" +
        ";DEFAULT_TABLE_ENGINE=" + GridH2DefaultTableEngine.class.getName();

    /** Default maximum size of connection pool. */
    private static final int DFLT_CONNECTION_POOL_SIZE = 32;

    /** */
    private static final H2Utils.Setter<Database, JavaObjectSerializer> DB_JOBJ_SERIALIZER
        = setter(Database.class, "javaObjectSerializer");

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

    /** The period of clean up the statement cache. */
    @SuppressWarnings("FieldCanBeLocal")
    private final Long stmtCleanupPeriod = Long.getLong(IGNITE_H2_INDEXING_CACHE_CLEANUP_PERIOD, 10_000);

    /** The timeout to remove entry from the statement cache if the thread doesn't perform any queries. */
    private final Long stmtTimeout = Long.getLong(IGNITE_H2_INDEXING_CACHE_THREAD_USAGE_TIMEOUT, 600 * 1000)
        * 1_000_000; // convert millis to nanos

    /** Database URL. */
    private final String dbUrl;

    /** Statement cleanup task. */
    private final GridTimeoutProcessor.CancelableTask stmtCleanupTask;

    /** Logger. */
    private final IgniteLogger log;

    /** Used connections set. */
    private final Set<H2Connection> usedConns = Collections.newSetFromMap(new ConcurrentHashMap<>());

    /** Connection pool. */
    private final ConcurrentStripedPool<H2Connection> connPool;

    /** H2 connection for INFORMATION_SCHEMA. Holds H2 open until node is stopped. */
    private volatile Connection sysConn;

    /** H2 data handler. Primarily used for serialization. */
    private final DataHandler dataNhd;

    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    public ConnectionManager(GridKernalContext ctx) {
        connPool = new ConcurrentStripedPool<>(ctx.config().getQueryThreadPoolSize(), DFLT_CONNECTION_POOL_SIZE);

        dbUrl = "jdbc:h2:mem:" + ctx.localNodeId() + DEFAULT_DB_OPTIONS;

        log = ctx.log(ConnectionManager.class);

        org.h2.Driver.load();

        try {
            sysConn = DriverManager.getConnection(dbUrl);

            sysConn.setSchema(QueryUtils.SCHEMA_INFORMATION);

            assert sysConn instanceof JdbcConnection : sysConn;

            JdbcConnection conn = (JdbcConnection)sysConn;

            dataNhd = conn.getSession().getDataHandler();
        }
        catch (SQLException e) {
            throw new IgniteSQLException("Failed to initialize DB connection: " + dbUrl, e);
        }

        stmtCleanupTask = ctx.timeout().schedule(this::cleanupStatements, stmtCleanupPeriod, stmtCleanupPeriod);
    }

    /**
     * Execute SQL statement on specific schema.
     *
     * @param schema Schema
     * @param sql SQL statement.
     * @throws IgniteCheckedException If failed.
     */
    public void executeStatement(String schema, String sql) throws IgniteCheckedException {
        try (H2PooledConnection conn = connection(schema)) {
            Connection c = conn.connection();

            try (Statement stmt = c.createStatement()) {
                stmt.executeUpdate(sql);
            }
        }
        catch (SQLException e) {
            throw new IgniteCheckedException("Failed to execute statement: " + sql, e);
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
     * Clear statement cache when cache is unregistered..
     */
    public void onCacheDestroyed() {
        connPool.forEach(H2Connection::clearStatementCache);
    }

    /**
     * Close all connections.
     */
    private void closeConnections() {
        connPool.forEach(c -> U.close(c.connection(), log));
        connPool.clear();

        usedConns.forEach(c -> U.close(c.connection(), log));
        usedConns.clear();
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

        // Needs to be released before SHUTDOWN.
        closeConnections();

        try (Statement s = sysConn.createStatement()) {
            s.execute("SHUTDOWN");
        }
        catch (SQLException e) {
            U.error(log, "Failed to shutdown database.", e);
        }

        U.close(sysConn, log);
    }

    /**
     * Called periodically to clean up the statement cache.
     */
    private void cleanupStatements() {
        connPool.forEach(c -> {
            if (c.statementCache().inactiveFor(stmtTimeout))
                c.clearStatementCache();
        });
    }

    /**
     * @param schema Schema name.
     * @return Connection with setup schema.
     */
    public H2PooledConnection connection(String schema) {
        H2PooledConnection conn = connection();

        try {
            conn.schema(schema);

            return conn;
        }
        catch (IgniteSQLException e) {
            U.closeQuiet(conn);

            throw e;
        }
    }

    /**
     * Resize the connection pool.
     *
     * @param size New size the connection pool.
     */
    void poolSize(int size) {
        if (size <= 0)
            throw new IllegalArgumentException("Invalid connection pool size: " + size);

        connPool.resize(size);
    }

    /**
     * @return H2 connection wrapper.
     */
    public H2PooledConnection connection() {
        try {
            H2Connection conn = connPool.borrow();

            if (conn == null)
                conn = newConnection();

            H2PooledConnection connWrp = new H2PooledConnection(conn, this);

            usedConns.add(conn);

            assert !conn.connection().isClosed() : "Connection is closed [conn=" + conn + ']';

            return connWrp;
        }
        catch (SQLException e) {
            throw new IgniteSQLException("Failed to initialize DB connection: " + dbUrl, e);
        }
    }

    /**
     * Create new connection wrapper.
     *
     * @return Connection wrapper.
     */
    private H2Connection newConnection() {
        try {
            return new H2Connection(DriverManager.getConnection(dbUrl), log);
        }
        catch (SQLException e) {
            throw new IgniteSQLException("Failed to initialize DB connection: " + dbUrl, e);
        }
    }

    /**
     * Return connection to pool or close if the pool size is bigger then maximum.
     *
     * @param conn Connection.
     */
    void recycle(H2Connection conn) {
        boolean rmv = usedConns.remove(conn);

        assert rmv : "Connection isn't tracked [conn=" + conn + ']';

        if (!connPool.recycle(conn))
            conn.close();
    }

    /**
     * @return Data handler.
     */
    public DataHandler dataHandler() {
        return dataNhd;
    }

    /**
     * Sets internal H2 serializer.
     *
     * @param serializer Serializer.
     */
    void setH2Serializer(JavaObjectSerializer serializer) {
        if (dataNhd != null && dataNhd instanceof Database)
            DB_JOBJ_SERIALIZER.set((Database)dataNhd, serializer);
    }
}
