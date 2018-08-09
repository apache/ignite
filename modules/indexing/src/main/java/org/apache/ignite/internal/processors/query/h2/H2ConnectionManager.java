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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectUtils;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.query.CacheQueryPartitionInfo;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMarshallable;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.CacheQueryObjectValueContext;
import org.apache.ignite.internal.processors.query.GridQueryCacheObjectsIterator;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResult;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResultAdapter;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.SqlClientContext;
import org.apache.ignite.internal.processors.query.h2.database.H2RowFactory;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.ddl.DdlStatementsProcessor;
import org.apache.ignite.internal.processors.query.h2.dml.DmlUtils;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlan;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2DefaultTableEngine;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2PlainRowFactory;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuery;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuerySplitter;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.apache.ignite.internal.processors.query.h2.sys.SqlSystemTableEngine;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemView;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemViewNodes;
import org.apache.ignite.internal.processors.query.h2.twostep.GridMapQueryExecutor;
import org.apache.ignite.internal.processors.query.h2.twostep.GridReduceQueryExecutor;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorImpl;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.sql.SqlParseException;
import org.apache.ignite.internal.sql.SqlParser;
import org.apache.ignite.internal.sql.SqlStrictParseException;
import org.apache.ignite.internal.sql.command.SqlAlterTableCommand;
import org.apache.ignite.internal.sql.command.SqlAlterUserCommand;
import org.apache.ignite.internal.sql.command.SqlBulkLoadCommand;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.sql.command.SqlCreateIndexCommand;
import org.apache.ignite.internal.sql.command.SqlCreateUserCommand;
import org.apache.ignite.internal.sql.command.SqlDropIndexCommand;
import org.apache.ignite.internal.sql.command.SqlDropUserCommand;
import org.apache.ignite.internal.sql.command.SqlSetStreamingCommand;
import org.apache.ignite.internal.util.GridEmptyCloseableIterator;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilterImpl;
import org.h2.api.ErrorCode;
import org.h2.api.JavaObjectSerializer;
import org.h2.command.Prepared;
import org.h2.command.dml.NoOperation;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.index.Index;
import org.h2.jdbc.JdbcStatement;
import org.h2.server.web.WebServer;
import org.h2.table.IndexColumn;
import org.h2.tools.Server;
import org.h2.util.JdbcUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_DEBUG_CONSOLE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_DEBUG_CONSOLE_PORT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_INDEXING_CACHE_CLEANUP_PERIOD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_INDEXING_CACHE_THREAD_USAGE_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.IgniteSystemProperties.getString;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL_FIELDS;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.TEXT;
import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.VAL_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.VER_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.h2.opt.DistributedJoinMode.OFF;
import static org.apache.ignite.internal.processors.query.h2.opt.DistributedJoinMode.distributedJoinMode;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.LOCAL;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.PREPARE;

/**
 * Manage H@ connections and statements.
 */
public class H2ConnectionManager {
    /** Default DB options. */
    private static final String DB_OPTIONS = ";LOCK_MODE=3;MULTI_THREADED=1;DB_CLOSE_ON_EXIT=FALSE" +
        ";DEFAULT_LOCK_TIMEOUT=10000;FUNCTIONS_IN_SCHEMA=true;OPTIMIZE_REUSE_RESULTS=0;QUERY_CACHE_SIZE=0" +
        ";RECOMPILE_ALWAYS=1;MAX_OPERATION_MEMORY=0;NESTED_JOINS=0;BATCH_JOINS=1" +
        ";ROW_FACTORY=\"" + GridH2PlainRowFactory.class.getName() + "\"" +
        ";DEFAULT_TABLE_ENGINE=" + GridH2DefaultTableEngine.class.getName();

    /** The period of clean up the statement cache. */
    private final Long CLEANUP_STMT_CACHE_PERIOD = Long.getLong(IGNITE_H2_INDEXING_CACHE_CLEANUP_PERIOD, 10_000);

    /** The period of clean up the {@link #conns}. */
    @SuppressWarnings("FieldCanBeLocal")
    private final Long CLEANUP_CONNECTIONS_PERIOD = 2000L;

    /** The timeout to remove entry from the statement cache if the thread doesn't perform any queries. */
    private final Long STATEMENT_CACHE_THREAD_USAGE_TIMEOUT =
        Long.getLong(IGNITE_H2_INDEXING_CACHE_THREAD_USAGE_TIMEOUT, 600 * 1000);

    /** Database URL. */
    private final String dbUrl;

    /** */
    private GridTimeoutProcessor.CancelableTask stmtCacheCleanupTask;

    /** */
    private GridTimeoutProcessor.CancelableTask connCleanupTask;

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** */
    private final ConcurrentMap<Thread, H2ConnectionWrapper> conns = new ConcurrentHashMap<>();

    /** */
    private final ThreadLocalObjectPool<H2ConnectionWrapper> connectionPool = new ThreadLocalObjectPool<>(
        H2ConnectionManager.this::newConnectionWrapper, 5);

    /** */
    private final ThreadLocal<ThreadLocalObjectPool.Reusable<H2ConnectionWrapper>> connCache
        = new ThreadLocal<ThreadLocalObjectPool.Reusable<H2ConnectionWrapper>>() {
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
            ThreadLocalObjectPool.Reusable<H2ConnectionWrapper> reusableConnection = connectionPool.borrow();

            conns.put(Thread.currentThread(), reusableConnection.object());

            return reusableConnection;
        }
    };

    /** */
    protected volatile GridKernalContext ctx;

    /** H2 JDBC connection for INFORMATION_SCHEMA. Holds H2 open until node is stopped. */
    private Connection sysConn;

    /**
     * @param ctx Kernal context.
     * @throws IgniteCheckedException On error.
     */
    H2ConnectionManager(GridKernalContext ctx) throws IgniteCheckedException {
        this.ctx=ctx;
        log = ctx.log(H2ConnectionManager.class);

        String dbName = (ctx != null ? ctx.localNodeId() : UUID.randomUUID()).toString();

        this.dbUrl = "jdbc:h2:mem:" + dbName + DB_OPTIONS;

        org.h2.Driver.load();

        try {
            if (getString(IGNITE_H2_DEBUG_CONSOLE) != null) {
                Connection c = DriverManager.getConnection(this.dbUrl);

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

        stmtCacheCleanupTask = ctx.timeout().schedule(new Runnable() {
            @Override public void run() {
                cleanupStatementCache();
            }
        }, CLEANUP_STMT_CACHE_PERIOD, CLEANUP_STMT_CACHE_PERIOD);

        connCleanupTask = ctx.timeout().schedule(new Runnable() {
            @Override public void run() {
                cleanupConnections();
            }
        }, CLEANUP_CONNECTIONS_PERIOD, CLEANUP_CONNECTIONS_PERIOD);
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
    synchronized private Connection systemConnection() {
        if (sysConn == null) {
            try {
                sysConn = DriverManager.getConnection(dbUrl);

                sysConn.setSchema("INFORMATION_SCHEMA");
            }
            catch (SQLException e) {
                throw new IgniteSQLException("Failed to initialize system DB connection: " + dbUrl, e);
            }
        }

        return sysConn;
    }

    /**
     * @return Connection wrapper.
     */
    private H2ConnectionWrapper newConnectionWrapper() {
        try {
            return new H2ConnectionWrapper(DriverManager.getConnection(dbUrl));
        } catch (SQLException e) {
            throw new IgniteSQLException("Failed to initialize DB connection: " + dbUrl, e);
        }
    }

    /**
     * @param c Connection.
     * @param sql SQL.
     * @return <b>Cached</b> prepared statement.
     */
    @SuppressWarnings("ConstantConditions")
    @Nullable PreparedStatement cachedStatement(Connection c, String sql) {
        try {
            return prepareStatement(c, sql, true, true);
        }
        catch (SQLException e) {
            // We actually don't except anything SQL related here as we're supposed to work with cache only.
            throw new AssertionError(e);
        }
    }

    /**
     * @param c Connection.
     * @param sql SQL.
     * @param useStmtCache If {@code true} uses statement cache.
     * @return Prepared statement.
     * @throws SQLException If failed.
     */
    @SuppressWarnings("ConstantConditions")
    @NotNull PreparedStatement prepareStatement(Connection c, String sql, boolean useStmtCache)
        throws SQLException {
        return prepareStatement(c, sql, useStmtCache, false);
    }

    /**
     * @param c Connection.
     * @param sql SQL.
     * @param useStmtCache If {@code true} uses statement cache.
     * @param cachedOnly Whether parsing should be avoided if statement has not been found in cache.
     * @return Prepared statement.
     * @throws SQLException If failed.
     */
    @Nullable PreparedStatement prepareStatement(Connection c, String sql, boolean useStmtCache,
        boolean cachedOnly) throws SQLException {
        // We can't avoid parsing and avoid using cache at the same time.
        assert useStmtCache || !cachedOnly;

        if (useStmtCache) {
            H2StatementCache cache = getStatementsCacheForCurrentThread();

            H2CachedStatementKey key = new H2CachedStatementKey(c.getSchema(), sql);

            PreparedStatement stmt = cache.get(key);

            if (stmt != null && !stmt.isClosed() && !stmt.unwrap(JdbcStatement.class).isCancelled() &&
                !GridSqlQueryParser.prepared(stmt).needRecompile()) {
                assert stmt.getConnection() == c;

                return stmt;
            }

            if (cachedOnly)
                return null;

            stmt = prepare0(c, sql);

            cache.put(key, stmt);

            assert stmt.getConnection() == c;

            return stmt;
        }
        else
            return prepare0(c, sql);
    }

    /**
     * Prepare statement.
     *
     * @param c Connection.
     * @param sql SQL.
     * @return Prepared statement.
     * @throws SQLException If failed.
     */
    private PreparedStatement prepare0(Connection c, String sql) throws SQLException {
        boolean insertHack = GridH2Table.insertHackRequired(sql);

        if (insertHack) {
            GridH2Table.insertHack(true);

            try {
                return c.prepareStatement(sql);
            }
            finally {
                GridH2Table.insertHack(false);
            }
        }
        else
            return c.prepareStatement(sql);
    }

    /**
     * @return {@link H2StatementCache} associated with current thread.
     */
    @NotNull private H2StatementCache getStatementsCacheForCurrentThread() {
        H2StatementCache statementCache = connCache.get().object().statementCache();

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
    private Connection connectionForThread(@Nullable String schema) throws IgniteCheckedException {
        H2ConnectionWrapper c = connCache.get().object();

        if (c == null)
            throw new IgniteCheckedException("Failed to get DB connection for thread (check log for details).");

        if (schema != null && !F.eq(c.schema(), schema)) {
            Statement stmt = null;

            try {
                stmt = c.connection().createStatement();

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
     * Called periodically by {@link GridTimeoutProcessor} to clean up the statement cache.
     */
    private void cleanupStatementCache() {
        long now = U.currentTimeMillis();

        for (Iterator<Map.Entry<Thread, H2ConnectionWrapper>> it = conns.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Thread, H2ConnectionWrapper> entry = it.next();

            Thread t = entry.getKey();

            if (t.getState() == Thread.State.TERMINATED) {
                U.close(entry.getValue(), log);

                it.remove();
            }
            else if (now - entry.getValue().statementCache().lastUsage() > STATEMENT_CACHE_THREAD_USAGE_TIMEOUT)
                entry.getValue().clearStatementCache();
        }
    }

    /**
     * Called periodically by {@link GridTimeoutProcessor} to clean up the {@link #conns}.
     */
    private void cleanupConnections() {
        for (Iterator<Map.Entry<Thread, H2ConnectionWrapper>> it = conns.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Thread, H2ConnectionWrapper> entry = it.next();

            Thread t = entry.getKey();

            if (t.getState() == Thread.State.TERMINATED) {
                U.close(entry.getValue(), log);

                it.remove();
            }
        }
    }

    /**
     * Removes from cache and returns associated with current thread connection.
     * @return Connection associated with current thread.
     */
    public ThreadLocalObjectPool.Reusable<H2ConnectionWrapper> detach() {
        Thread key = Thread.currentThread();

        ThreadLocalObjectPool.Reusable<H2ConnectionWrapper> reusableConnection = connCache.get();

        H2ConnectionWrapper connection = conns.remove(key);

        connCache.remove();

        assert reusableConnection.object().connection() == connection.connection();

        return reusableConnection;
    }

    /**
     * @param ctx Context.
     * @return Predefined system views.
     */
    public Collection<SqlSystemView> systemViews(GridKernalContext ctx) {
        Collection<SqlSystemView> views = new ArrayList<>();

        views.add(new SqlSystemViewNodes(ctx));

        return views;
    }

    /**
     * @return Value object context.
     */
    public CacheObjectValueContext objectContext() {
        return ctx.query().objectContext();
    }


    /**
     * Close all connections.
     */
    public synchronized void close() {
        if (log.isDebugEnabled())
            log.debug("Stopping cache query index...");

        for (H2ConnectionWrapper c : conns.values())
            U.close(c, log);

        conns.clear();

        try (Connection c = DriverManager.getConnection(dbUrl); Statement s = c.createStatement()) {
            s.execute("SHUTDOWN");
        }
        catch (SQLException e) {
            U.error(log, "Failed to shutdown database.", e);
        }

        if (stmtCacheCleanupTask != null)
            stmtCacheCleanupTask.close();

        if (connCleanupTask != null)
            connCleanupTask.close();

        GridH2QueryContext.clearLocalNodeStop(ctx.localNodeId());

        if (log.isDebugEnabled())
            log.debug("Cache query index stopped.");

        // Close system H2 connection to INFORMATION_SCHEMA
        if (sysConn != null) {
            U.close(sysConn, log);

            sysConn = null;
        }
    }

    /**
     *
     */
    public void closeAllConnections() {
        for (H2ConnectionWrapper c : conns.values())
            U.close(c, log);
    }

    /**
     * @return Per-thread connections.
     */
    public Map<Thread, ?> perThreadConnections() {
        return conns;
    }
}
