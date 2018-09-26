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
import java.util.regex.Pattern;
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
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxSelectForUpdateFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.TxTopologyVersionFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccQueryTracker;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.query.CacheQueryPartitionInfo;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMarshallable;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxAdapter;
import org.apache.ignite.internal.processors.query.CacheQueryObjectValueContext;
import org.apache.ignite.internal.processors.query.GridQueryCacheObjectsIterator;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResult;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResultAdapter;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.NestedTxMode;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.SqlClientContext;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.internal.processors.query.h2.database.H2RowFactory;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2InnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2MvccInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2MvccLeafIO;
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
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAlias;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuery;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuerySplitter;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlTable;
import org.apache.ignite.internal.processors.query.h2.sys.SqlSystemTableEngine;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemView;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemViewBaselineNodes;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemViewNodeAttributes;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemViewNodeMetrics;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemViewNodes;
import org.apache.ignite.internal.processors.query.h2.twostep.GridMapQueryExecutor;
import org.apache.ignite.internal.processors.query.h2.twostep.GridReduceQueryExecutor;
import org.apache.ignite.internal.processors.query.h2.twostep.MapQueryLazyWorker;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorImpl;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.sql.SqlParseException;
import org.apache.ignite.internal.sql.SqlParser;
import org.apache.ignite.internal.sql.SqlStrictParseException;
import org.apache.ignite.internal.sql.command.SqlAlterTableCommand;
import org.apache.ignite.internal.sql.command.SqlAlterUserCommand;
import org.apache.ignite.internal.sql.command.SqlBeginTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlBulkLoadCommand;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.sql.command.SqlCommitTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlCreateIndexCommand;
import org.apache.ignite.internal.sql.command.SqlCreateUserCommand;
import org.apache.ignite.internal.sql.command.SqlDropIndexCommand;
import org.apache.ignite.internal.sql.command.SqlDropUserCommand;
import org.apache.ignite.internal.sql.command.SqlRollbackTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlSetStreamingCommand;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
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
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
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
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.checkActive;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.mvccEnabled;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.tx;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.txStart;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL_FIELDS;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.TEXT;
import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.VAL_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.VER_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.h2.PreparedStatementEx.MVCC_CACHE_ID;
import static org.apache.ignite.internal.processors.query.h2.PreparedStatementEx.MVCC_STATE;
import static org.apache.ignite.internal.processors.query.h2.opt.DistributedJoinMode.OFF;
import static org.apache.ignite.internal.processors.query.h2.opt.DistributedJoinMode.distributedJoinMode;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.LOCAL;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.PREPARE;

/**
 * Indexing implementation based on H2 database engine. In this implementation main query language is SQL,
 * fulltext indexing can be performed using Lucene.
 * <p>
 * For each registered {@link GridQueryTypeDescriptor} this SPI will create respective SQL table with
 * {@code '_key'} and {@code '_val'} fields for key and value, and fields from
 * {@link GridQueryTypeDescriptor#fields()}.
 * For each table it will create indexes declared in {@link GridQueryTypeDescriptor#indexes()}.
 */
public class IgniteH2Indexing implements GridQueryIndexing {
    /** A pattern for commands having internal implementation in Ignite. */
    public static final Pattern INTERNAL_CMD_RE = Pattern.compile(
        "^(create|drop)\\s+index|^alter\\s+table|^copy|^set|^begin|^commit|^rollback|^(create|alter|drop)\\s+user",
        Pattern.CASE_INSENSITIVE);

    /*
     * Register IO for indexes.
     */
    static {
        PageIO.registerH2(H2InnerIO.VERSIONS, H2LeafIO.VERSIONS, H2MvccInnerIO.VERSIONS, H2MvccLeafIO.VERSIONS);
        H2ExtrasInnerIO.register();
        H2ExtrasLeafIO.register();

        // Initialize system properties for H2.
        System.setProperty("h2.objectCache", "false");
        System.setProperty("h2.serializeJavaObject", "false");
        System.setProperty("h2.objectCacheMaxPerElementSize", "0"); // Avoid ValueJavaObject caching.
        System.setProperty("h2.optimizeTwoEquals", "false"); // Makes splitter fail on subqueries in WHERE.
    }

    /** Default DB options. */
    private static final String DB_OPTIONS = ";LOCK_MODE=3;MULTI_THREADED=1;DB_CLOSE_ON_EXIT=FALSE" +
        ";DEFAULT_LOCK_TIMEOUT=10000;FUNCTIONS_IN_SCHEMA=true;OPTIMIZE_REUSE_RESULTS=0;QUERY_CACHE_SIZE=0" +
        ";MAX_OPERATION_MEMORY=0;BATCH_JOINS=1" +
        ";ROW_FACTORY=\"" + GridH2PlainRowFactory.class.getName() + "\"" +
        ";DEFAULT_TABLE_ENGINE=" + GridH2DefaultTableEngine.class.getName();

        // Uncomment this setting to get debug output from H2 to sysout.
//        ";TRACE_LEVEL_SYSTEM_OUT=3";

    /** Dummy metadata for update result. */
    public static final List<GridQueryFieldMetadata> UPDATE_RESULT_META = Collections.<GridQueryFieldMetadata>
        singletonList(new H2SqlFieldMetadata(null, null, "UPDATED", Long.class.getName(), -1, -1));

    /** */
    private static final int TWO_STEP_QRY_CACHE_SIZE = 1024;

    /** The period of clean up the statement cache. */
    private final Long CLEANUP_STMT_CACHE_PERIOD = Long.getLong(IGNITE_H2_INDEXING_CACHE_CLEANUP_PERIOD, 10_000);

    /** The period of clean up the {@link #conns}. */
    @SuppressWarnings("FieldCanBeLocal")
    private final Long CLEANUP_CONNECTIONS_PERIOD = 2000L;

    /** The timeout to remove entry from the statement cache if the thread doesn't perform any queries. */
    private final Long STATEMENT_CACHE_THREAD_USAGE_TIMEOUT =
        Long.getLong(IGNITE_H2_INDEXING_CACHE_THREAD_USAGE_TIMEOUT, 600 * 1000);

    /** */
    private GridTimeoutProcessor.CancelableTask stmtCacheCleanupTask;

    /** */
    private GridTimeoutProcessor.CancelableTask connCleanupTask;

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Node ID. */
    private UUID nodeId;

    /** */
    private Marshaller marshaller;

    /** Collection of schemaNames and registered tables. */
    private final ConcurrentMap<String, H2Schema> schemas = new ConcurrentHashMap<>();

    /** */
    private String dbUrl = "jdbc:h2:mem:";

    /** */
    // TODO https://issues.apache.org/jira/browse/IGNITE-9062
    private final ConcurrentMap<Thread, H2ConnectionWrapper> conns = new ConcurrentHashMap<>();

    /** */
    private GridMapQueryExecutor mapQryExec;

    /** */
    private GridReduceQueryExecutor rdcQryExec;

    /** Cache name -> schema name */
    private final Map<String, String> cacheName2schema = new ConcurrentHashMap<>();

    /** */
    private AtomicLong qryIdGen;

    /** */
    private GridSpinBusyLock busyLock;

    /** */
    private final Object schemaMux = new Object();

    /** */
    private final ConcurrentMap<Long, GridRunningQueryInfo> runs = new ConcurrentHashMap<>();

    /** Row cache. */
    private final H2RowCacheRegistry rowCache = new H2RowCacheRegistry();

    /** */
    // TODO https://issues.apache.org/jira/browse/IGNITE-9062
    private final ThreadLocalObjectPool<H2ConnectionWrapper> connectionPool = new ThreadLocalObjectPool<>(IgniteH2Indexing.this::newConnectionWrapper, 5);

    /** */
    // TODO https://issues.apache.org/jira/browse/IGNITE-9062
    private final ThreadLocal<ThreadLocalObjectPool.Reusable<H2ConnectionWrapper>> connCache = new ThreadLocal<ThreadLocalObjectPool.Reusable<H2ConnectionWrapper>>() {
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

    /** Cache object value context. */
    protected CacheQueryObjectValueContext valCtx;

    /** */
    private DmlStatementsProcessor dmlProc;

    /** */
    private DdlStatementsProcessor ddlProc;

    /** */
    private final ConcurrentMap<QueryTable, GridH2Table> dataTables = new ConcurrentHashMap<>();

    /** */
    private volatile GridBoundedConcurrentLinkedHashMap<H2TwoStepCachedQueryKey, H2TwoStepCachedQuery> twoStepCache =
        new GridBoundedConcurrentLinkedHashMap<>(TWO_STEP_QRY_CACHE_SIZE);

    /** */
    private final IgniteInClosure<? super IgniteInternalFuture<?>> logger = new IgniteInClosure<IgniteInternalFuture<?>>() {
        @Override public void apply(IgniteInternalFuture<?> fut) {
            try {
                fut.get();
            }
            catch (IgniteCheckedException e) {
                U.error(log, e.getMessage(), e);
            }
        }
    };

    /** H2 JDBC connection for INFORMATION_SCHEMA. Holds H2 open until node is stopped. */
    private Connection sysConn;

    /**
     * @return Kernal context.
     */
    public GridKernalContext kernalContext() {
        return ctx;
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
    private Connection systemConnection() {
        assert Thread.holdsLock(schemaMux);

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

    /** */
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
    @Nullable private PreparedStatement cachedStatement(Connection c, String sql) {
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
    @NotNull public PreparedStatement prepareStatement(Connection c, String sql, boolean useStmtCache)
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
    @Nullable private PreparedStatement prepareStatement(Connection c, String sql, boolean useStmtCache,
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

            cache.put(key, stmt = PreparedStatementExImpl.wrap(prepare0(c, sql)));

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
     * @return {@link H2StatementCache} associated with current thread.
     */
    @NotNull private H2StatementCache getStatementsCacheForCurrentThread() {
        H2StatementCache statementCache = connCache.get().object().statementCache();

        statementCache.updateLastUsage();

        return statementCache;
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement prepareNativeStatement(String schemaName, String sql) {
        Connection conn = connectionForSchema(schemaName);

        return prepareStatementAndCaches(conn, sql);
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
     * Create and register schema if needed.
     *
     * @param schemaName Schema name.
     * @param predefined Whether this is predefined schema.
     */
    private void createSchemaIfNeeded(String schemaName, boolean predefined) {
        assert Thread.holdsLock(schemaMux);

        if (!predefined)
            predefined = isSchemaPredefined(schemaName);

        H2Schema schema = new H2Schema(schemaName, predefined);

        H2Schema oldSchema = schemas.putIfAbsent(schemaName, schema);

        if (oldSchema == null)
            createSchema0(schemaName);
        else
            schema = oldSchema;

        schema.incrementUsageCount();
    }

    /**
     * Check if schema is predefined.
     *
     * @param schemaName Schema name.
     * @return {@code True} if predefined.
     */
    private boolean isSchemaPredefined(String schemaName) {
        if (F.eq(QueryUtils.DFLT_SCHEMA, schemaName))
            return true;

        for (H2Schema schema : schemas.values()) {
            if (F.eq(schema.schemaName(), schemaName) && schema.predefined())
                return true;
        }

        return false;
    }

    /**
     * Creates DB schema if it has not been created yet.
     *
     * @param schema Schema name.
     */
    private void createSchema0(String schema) {
        executeSystemStatement("CREATE SCHEMA IF NOT EXISTS " + H2Utils.withQuotes(schema));

        if (log.isDebugEnabled())
            log.debug("Created H2 schema for index database: " + schema);
    }

    /**
     * Creates DB schema if it has not been created yet.
     *
     * @param schema Schema name.
     */
    private void dropSchema(String schema) {
        executeSystemStatement("DROP SCHEMA IF EXISTS " + H2Utils.withQuotes(schema));

        if (log.isDebugEnabled())
            log.debug("Dropped H2 schema for index database: " + schema);
    }

    /**
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
     * @param sql SQL statement.
     */
    public void executeSystemStatement(String sql) {
        assert Thread.holdsLock(schemaMux);

        Statement stmt = null;

        try {
            stmt = systemConnection().createStatement();

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
     * Binds object to prepared statement.
     *
     * @param stmt SQL statement.
     * @param idx Index.
     * @param obj Value to store.
     * @throws IgniteCheckedException If failed.
     */
    private void bindObject(PreparedStatement stmt, int idx, @Nullable Object obj) throws IgniteCheckedException {
        try {
            if (obj == null)
                stmt.setNull(idx, Types.VARCHAR);
            else if (obj instanceof BigInteger)
                stmt.setObject(idx, obj, Types.JAVA_OBJECT);
            else if (obj instanceof BigDecimal)
                stmt.setObject(idx, obj, Types.DECIMAL);
            else
                stmt.setObject(idx, obj);
        }
        catch (SQLException e) {
            throw new IgniteCheckedException("Failed to bind parameter [idx=" + idx + ", obj=" + obj + ", stmt=" +
                stmt + ']', e);
        }
    }

    /**
     * Handles SQL exception.
     */
    private void onSqlException() {
        Connection conn = connCache.get().object().connection();

        connCache.set(null);

        if (conn != null) {
            conns.remove(Thread.currentThread());

            // Reset connection to receive new one at next call.
            U.close(conn, log);
        }
    }

    /** {@inheritDoc} */
    @Override public void store(GridCacheContext cctx,
        GridQueryTypeDescriptor type,
        CacheDataRow row,
        @Nullable CacheDataRow prevRow,
        boolean prevRowAvailable) throws IgniteCheckedException
    {
        String cacheName = cctx.name();

        H2TableDescriptor tbl = tableDescriptor(schema(cacheName), cacheName, type.name());

        if (tbl == null)
            return; // Type was rejected.

        tbl.table().update(row, prevRow,  prevRowAvailable);

        if (tbl.luceneIndex() != null) {
            long expireTime = row.expireTime();

            if (expireTime == 0L)
                expireTime = Long.MAX_VALUE;

            tbl.luceneIndex().store(row.key(), row.value(), row.version(), expireTime);
        }
    }

    /** {@inheritDoc} */
    @Override public void remove(GridCacheContext cctx, GridQueryTypeDescriptor type, CacheDataRow row)
        throws IgniteCheckedException
    {
        if (log.isDebugEnabled()) {
            log.debug("Removing key from cache query index [locId=" + nodeId +
                ", key=" + row.key() +
                ", val=" + row.value() + ']');
        }

        String cacheName = cctx.name();

        H2TableDescriptor tbl = tableDescriptor(schema(cacheName), cacheName, type.name());

        if (tbl == null)
            return;

        if (tbl.table().remove(row)) {
            if (tbl.luceneIndex() != null)
                tbl.luceneIndex().remove(row.key());
        }
    }

    /**
     * Drops table form h2 database and clear all related indexes (h2 text, lucene).
     *
     * @param tbl Table to unregister.
     * @throws IgniteCheckedException If failed to unregister.
     */
    private void dropTable(H2TableDescriptor tbl) throws IgniteCheckedException {
        assert tbl != null;

        if (log.isDebugEnabled())
            log.debug("Removing query index table: " + tbl.fullTableName());

        Connection c = connectionForThread(tbl.schemaName());

        Statement stmt = null;

        try {
            stmt = c.createStatement();

            String sql = "DROP TABLE IF EXISTS " + tbl.fullTableName();

            if (log.isDebugEnabled())
                log.debug("Dropping database index table with SQL: " + sql);

            stmt.executeUpdate(sql);
        }
        catch (SQLException e) {
            onSqlException();

            throw new IgniteSQLException("Failed to drop database index table [type=" + tbl.type().name() +
                ", table=" + tbl.fullTableName() + "]", IgniteQueryErrorCode.TABLE_DROP_FAILED, e);
        }
        finally {
            U.close(stmt, log);
        }
    }

    /**
     * Add initial user index.
     *
     * @param schemaName Schema name.
     * @param desc Table descriptor.
     * @param h2Idx User index.
     * @throws IgniteCheckedException If failed.
     */
    private void addInitialUserIndex(String schemaName, H2TableDescriptor desc, GridH2IndexBase h2Idx)
        throws IgniteCheckedException {
        GridH2Table h2Tbl = desc.table();

        h2Tbl.proposeUserIndex(h2Idx);

        try {
            String sql = H2Utils.indexCreateSql(desc.fullTableName(), h2Idx, false);

            executeSql(schemaName, sql);
        }
        catch (Exception e) {
            // Rollback and re-throw.
            h2Tbl.rollbackUserIndex(h2Idx.getName());

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void dynamicIndexCreate(final String schemaName, final String tblName,
        final QueryIndexDescriptorImpl idxDesc, boolean ifNotExists, SchemaIndexCacheVisitor cacheVisitor)
        throws IgniteCheckedException {
        // Locate table.
        H2Schema schema = schemas.get(schemaName);

        H2TableDescriptor desc = (schema != null ? schema.tableByName(tblName) : null);

        if (desc == null)
            throw new IgniteCheckedException("Table not found in internal H2 database [schemaName=" + schemaName +
                ", tblName=" + tblName + ']');

        GridH2Table h2Tbl = desc.table();

        // Create index.
        final GridH2IndexBase h2Idx = desc.createUserIndex(idxDesc);

        h2Tbl.proposeUserIndex(h2Idx);

        try {
            // Populate index with existing cache data.
            final GridH2RowDescriptor rowDesc = h2Tbl.rowDescriptor();

            SchemaIndexCacheVisitorClosure clo = new SchemaIndexCacheVisitorClosure() {
                @Override public void apply(CacheDataRow row) throws IgniteCheckedException {
                    GridH2Row h2Row = rowDesc.createRow(row);

                    h2Idx.putx(h2Row);
                }
            };

            cacheVisitor.visit(clo);

            // At this point index is in consistent state, promote it through H2 SQL statement, so that cached
            // prepared statements are re-built.
            String sql = H2Utils.indexCreateSql(desc.fullTableName(), h2Idx, ifNotExists);

            executeSql(schemaName, sql);
        }
        catch (Exception e) {
            // Rollback and re-throw.
            h2Tbl.rollbackUserIndex(h2Idx.getName());

            throw e;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    @Override public void dynamicIndexDrop(final String schemaName, String idxName, boolean ifExists)
        throws IgniteCheckedException{
        String sql = H2Utils.indexDropSql(schemaName, idxName, ifExists);

        executeSql(schemaName, sql);
    }

    /** {@inheritDoc} */
    @Override public void dynamicAddColumn(String schemaName, String tblName, List<QueryField> cols,
        boolean ifTblExists, boolean ifColNotExists) throws IgniteCheckedException {
        // Locate table.
        H2Schema schema = schemas.get(schemaName);

        H2TableDescriptor desc = (schema != null ? schema.tableByName(tblName) : null);

        if (desc == null) {
            if (!ifTblExists)
                throw new IgniteCheckedException("Table not found in internal H2 database [schemaName=" + schemaName +
                    ", tblName=" + tblName + ']');
            else
                return;
        }

        desc.table().addColumns(cols, ifColNotExists);

        clearCachedQueries();
    }

    /** {@inheritDoc} */
    @Override public void dynamicDropColumn(String schemaName, String tblName, List<String> cols, boolean ifTblExists,
        boolean ifColExists) throws IgniteCheckedException {
        // Locate table.
        H2Schema schema = schemas.get(schemaName);

        H2TableDescriptor desc = (schema != null ? schema.tableByName(tblName) : null);

        if (desc == null) {
            if (!ifTblExists)
                throw new IgniteCheckedException("Table not found in internal H2 database [schemaName=" + schemaName +
                    ",tblName=" + tblName + ']');
            else
                return;
        }

        desc.table().dropColumns(cols, ifColExists);

        clearCachedQueries();
    }

    /**
     * Execute DDL command.
     *
     * @param schemaName Schema name.
     * @param sql SQL.
     * @throws IgniteCheckedException If failed.
     */
    private void executeSql(String schemaName, String sql) throws IgniteCheckedException {
        try {
            Connection conn = connectionForSchema(schemaName);

            try (PreparedStatement stmt = prepareStatement(conn, sql, false)) {
                stmt.execute();
            }
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to execute SQL statement on internal H2 database: " + sql, e);
        }
    }

    /**
     * Create sorted index.
     *
     * @param name Index name,
     * @param tbl Table.
     * @param pk Primary key flag.
     * @param cols Columns.
     * @param inlineSize Index inline size.
     * @return Index.
     */
    GridH2IndexBase createSortedIndex(String name, GridH2Table tbl, boolean pk, List<IndexColumn> cols,
        int inlineSize) {
        try {
            GridCacheContext cctx = tbl.cache();

            if (log.isDebugEnabled())
                log.debug("Creating cache index [cacheId=" + cctx.cacheId() + ", idxName=" + name + ']');

            final int segments = tbl.rowDescriptor().context().config().getQueryParallelism();

            H2RowCache cache = rowCache.forGroup(cctx.groupId());

            return new H2TreeIndex(cctx, cache, tbl, name, pk, cols, inlineSize, segments);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocalText(String schemaName,
        String cacheName, String qry, String typeName, IndexingQueryFilter filters) throws IgniteCheckedException {
        H2TableDescriptor tbl = tableDescriptor(schemaName, cacheName, typeName);

        if (tbl != null && tbl.luceneIndex() != null) {
            GridRunningQueryInfo run = new GridRunningQueryInfo(qryIdGen.incrementAndGet(), qry, TEXT, schemaName,
                U.currentTimeMillis(), null, true);

            try {
                runs.put(run.id(), run);

                return tbl.luceneIndex().query(qry.toUpperCase(), filters);
            }
            finally {
                runs.remove(run.id());
            }
        }

        return new GridEmptyCloseableIterator<>();
    }

    /**
     * Queries individual fields (generally used by JDBC drivers).
     *
     * @param schemaName Schema name.
     * @param qry Query.
     * @param params Query parameters.
     * @param filter Cache name and key filter.
     * @param enforceJoinOrder Enforce join order of tables in the query.
     * @param startTx Start transaction flag.
     * @param timeout Query timeout in milliseconds.
     * @param cancel Query cancel.
     * @return Query result.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public GridQueryFieldsResult queryLocalSqlFields(String schemaName, String qry, @Nullable Collection<Object> params,
        IndexingQueryFilter filter, boolean enforceJoinOrder, boolean startTx, int timeout,
        GridQueryCancel cancel) throws IgniteCheckedException {
        return queryLocalSqlFields(schemaName, qry, params, filter, enforceJoinOrder, startTx, timeout, cancel, null);
    }

    /**
     * Queries individual fields (generally used by JDBC drivers).
     *
     * @param schemaName Schema name.
     * @param qry Query.
     * @param params Query parameters.
     * @param filter Cache name and key filter.
     * @param enforceJoinOrder Enforce join order of tables in the query.
     * @param startTx Start transaction flag.
     * @param qryTimeout Query timeout in milliseconds.
     * @param cancel Query cancel.
     * @param mvccTracker Query tracker.
     * @return Query result.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    GridQueryFieldsResult queryLocalSqlFields(final String schemaName, String qry,
        @Nullable final Collection<Object> params, final IndexingQueryFilter filter, boolean enforceJoinOrder,
        boolean startTx, int qryTimeout, final GridQueryCancel cancel,
        MvccQueryTracker mvccTracker) throws IgniteCheckedException {

        GridNearTxLocal tx = null;

        boolean mvccEnabled = mvccEnabled(kernalContext());

        assert mvccEnabled || mvccTracker == null;

        try {
            final Connection conn = connectionForSchema(schemaName);

            H2Utils.setupConnection(conn, false, enforceJoinOrder);

            PreparedStatement stmt = preparedStatementWithParams(conn, qry, params, true);

            if (GridSqlQueryParser.checkMultipleStatements(stmt))
                throw new IgniteSQLException("Multiple statements queries are not supported for local queries");

            Prepared p = GridSqlQueryParser.prepared(stmt);

            if (DmlStatementsProcessor.isDmlStatement(p)) {
                SqlFieldsQuery fldsQry = new SqlFieldsQuery(qry);

                if (params != null)
                    fldsQry.setArgs(params.toArray());

                fldsQry.setEnforceJoinOrder(enforceJoinOrder);
                fldsQry.setTimeout(qryTimeout, TimeUnit.MILLISECONDS);

                return dmlProc.updateSqlFieldsLocal(schemaName, conn, p, fldsQry, filter, cancel);
            }
            else if (DdlStatementsProcessor.isDdlStatement(p)) {
                throw new IgniteSQLException("DDL statements are supported for the whole cluster only.",
                    IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
            }

            final GridH2QueryContext ctx = new GridH2QueryContext(nodeId, nodeId, 0, LOCAL)
                .filter(filter).distributedJoinMode(OFF);

            boolean forUpdate = GridSqlQueryParser.isForUpdateQuery(p);

            if (forUpdate && !mvccEnabled)
                throw new IgniteSQLException("SELECT FOR UPDATE query requires transactional " +
                    "cache with MVCC enabled.", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

            GridNearTxSelectForUpdateFuture sfuFut = null;

            int opTimeout = qryTimeout;

            if (mvccEnabled) {
                if (mvccTracker == null)
                    mvccTracker = mvccTracker(stmt, startTx);

                if (mvccTracker != null) {
                    ctx.mvccSnapshot(mvccTracker.snapshot());

                    tx = checkActive(tx(this.ctx));

                    opTimeout = operationTimeout(opTimeout, tx);
                }

                if (forUpdate) {
                    if (mvccTracker == null)
                        throw new IgniteSQLException("SELECT FOR UPDATE query requires transactional " +
                            "cache with MVCC enabled.", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

                    GridSqlStatement stmt0 = new GridSqlQueryParser(false).parse(p);

                    qry = GridSqlQueryParser.rewriteQueryForUpdateIfNeeded(stmt0, forUpdate = tx != null);

                    stmt = preparedStatementWithParams(conn, qry, params, true);

                    if (forUpdate) {
                        GridCacheContext cctx = mvccTracker.context();

                        try {
                            if (tx.topologyVersionSnapshot() == null)
                                new TxTopologyVersionFuture(tx, cctx).get();
                        }
                        catch (Exception e) {
                            throw new IgniteSQLException("Failed to lock topology for SELECT FOR UPDATE query.", e);
                        }

                        sfuFut = new GridNearTxSelectForUpdateFuture(cctx, tx, opTimeout);

                        sfuFut.initLocal();
                    }
                }
            }

            List<GridQueryFieldMetadata> meta;

            try {
                meta = H2Utils.meta(stmt.getMetaData());

                if (forUpdate) {
                    assert meta.size() >= 1;

                    meta = meta.subList(0, meta.size() - 1);
                }
            }
            catch (SQLException e) {
                throw new IgniteCheckedException("Cannot prepare query metadata", e);
            }

            GridNearTxLocal tx0 = tx;
            MvccQueryTracker mvccTracker0 = mvccTracker;
            GridNearTxSelectForUpdateFuture sfuFut0 = sfuFut;
            PreparedStatement stmt0 = stmt;
            String qry0 = qry;
            int timeout0 = opTimeout;

            return new GridQueryFieldsResultAdapter(meta, null) {
                @Override public GridCloseableIterator<List<?>> iterator() throws IgniteCheckedException {
                    assert GridH2QueryContext.get() == null;

                    GridH2QueryContext.set(ctx);

                    GridRunningQueryInfo run = new GridRunningQueryInfo(qryIdGen.incrementAndGet(), qry0,
                        SQL_FIELDS, schemaName, U.currentTimeMillis(), cancel, true);

                    runs.putIfAbsent(run.id(), run);

                    try {
                        ResultSet rs = executeSqlQueryWithTimer(stmt0, conn, qry0, params, timeout0, cancel);

                        if (sfuFut0 != null) {
                            assert tx0.mvccSnapshot() != null;

                            ResultSetEnlistFuture enlistFut = ResultSetEnlistFuture.future(
                                IgniteH2Indexing.this.ctx.localNodeId(),
                                tx0.nearXidVersion(),
                                tx0.mvccSnapshot(),
                                tx0.threadId(),
                                IgniteUuid.randomUuid(),
                                -1,
                                null,
                                tx0,
                                timeout0,
                                sfuFut0.cache(),
                                rs
                            );

                            enlistFut.listen(new IgniteInClosure<IgniteInternalFuture<Long>>() {
                                @Override public void apply(IgniteInternalFuture<Long> fut) {
                                    if (fut.error() != null)
                                        sfuFut0.onResult(IgniteH2Indexing.this.ctx.localNodeId(), 0L, false, fut.error());
                                    else
                                        sfuFut0.onResult(IgniteH2Indexing.this.ctx.localNodeId(), fut.result(), false, null);
                                }
                            });

                            enlistFut.init();

                            try {
                                sfuFut0.get();

                                rs.beforeFirst();
                            }
                            catch (Exception e) {
                                U.closeQuiet(rs);

                                throw new IgniteSQLException("Failed to obtain locks on result of SELECT FOR UPDATE.",
                                    e);
                            }
                        }

                        return new H2FieldsIterator(rs, mvccTracker0, sfuFut0 != null);
                    }
                    catch (IgniteCheckedException | RuntimeException | Error e) {
                        try {
                            if (mvccTracker0 != null)
                                mvccTracker0.onDone();
                        }
                        catch (Exception e0) {
                            e.addSuppressed(e0);
                        }

                        throw e;
                    }
                    finally {
                        GridH2QueryContext.clearThreadLocal();

                        runs.remove(run.id());
                    }
                }
            };
        }
        catch (IgniteCheckedException | RuntimeException | Error e) {
            if (mvccEnabled && (tx != null || (tx = tx(ctx)) != null))
                tx.setRollbackOnly();

            throw e;
        }
    }

    /**
     * @param qryTimeout Query timeout in milliseconds.
     * @param tx Transaction.
     * @return Timeout for operation in milliseconds based on query and tx timeouts.
     */
    public static int operationTimeout(int qryTimeout, IgniteTxAdapter tx) {
        if (tx != null) {
            int tm1 = (int)tx.remainingTime(), tm2 = qryTimeout;

            return tm1 > 0 && tm2 > 0 ? Math.min(tm1, tm2) : Math.max(tm1, tm2);
        }

        return qryTimeout;
    }

    /** {@inheritDoc} */
    @Override public long streamUpdateQuery(String schemaName, String qry,
        @Nullable Object[] params, IgniteDataStreamer<?, ?> streamer) throws IgniteCheckedException {
        final Connection conn = connectionForSchema(schemaName);

        final PreparedStatement stmt;

        try {
            stmt = prepareStatement(conn, qry, true);
        }
        catch (SQLException e) {
            throw new IgniteSQLException(e);
        }

        return dmlProc.streamUpdateQuery(schemaName, streamer, stmt, params);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override public List<Long> streamBatchedUpdateQuery(String schemaName, String qry, List<Object[]> params,
        SqlClientContext cliCtx) throws IgniteCheckedException {
        if (cliCtx == null || !cliCtx.isStream()) {
            U.warn(log, "Connection is not in streaming mode.");

            return zeroBatchedStreamedUpdateResult(params.size());
        }

        final Connection conn = connectionForSchema(schemaName);

        final PreparedStatement stmt = prepareStatementAndCaches(conn, qry);

        if (GridSqlQueryParser.checkMultipleStatements(stmt))
            throw new IgniteSQLException("Multiple statements queries are not supported for streaming mode.",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        checkStatementStreamable(stmt);

        Prepared p = GridSqlQueryParser.prepared(stmt);

        UpdatePlan plan = dmlProc.getPlanForStatement(schemaName, conn, p, null, true, null);

        IgniteDataStreamer<?, ?> streamer = cliCtx.streamerForCache(plan.cacheContext().name());

        assert streamer != null;

        List<Long> res = new ArrayList<>(params.size());

        for (int i = 0; i < params.size(); i++)
            res.add(dmlProc.streamUpdateQuery(schemaName, streamer, stmt, params.get(i)));

        return res;
    }

    /**
     * @param size Result size.
     * @return List of given size filled with 0Ls.
     */
    private static List<Long> zeroBatchedStreamedUpdateResult(int size) {
        Long[] res = new Long[size];

        Arrays.fill(res, 0L);

        return Arrays.asList(res);
    }

    /**
     * Prepares sql statement.
     *
     * @param conn Connection.
     * @param sql Sql.
     * @param params Params.
     * @param useStmtCache If {@code true} use stmt cache.
     * @return Prepared statement with set parameters.
     * @throws IgniteCheckedException If failed.
     */
    public PreparedStatement preparedStatementWithParams(Connection conn, String sql, Collection<Object> params,
        boolean useStmtCache) throws IgniteCheckedException {
        final PreparedStatement stmt;

        try {
            stmt = prepareStatement(conn, sql, useStmtCache);
        }
        catch (SQLException e) {
            throw new IgniteCheckedException("Failed to parse SQL query: " + sql, e);
        }

        bindParameters(stmt, params);

        return stmt;
    }

    /**
     * Executes sql query statement.
     *
     * @param conn Connection,.
     * @param stmt Statement.
     * @param timeoutMillis Query timeout.
     * @param cancel Query cancel.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    private ResultSet executeSqlQuery(final Connection conn, final PreparedStatement stmt,
        int timeoutMillis, @Nullable GridQueryCancel cancel) throws IgniteCheckedException {
        final MapQueryLazyWorker lazyWorker = MapQueryLazyWorker.currentWorker();

        if (cancel != null) {
            cancel.set(new Runnable() {
                @Override public void run() {
                    if (lazyWorker != null) {
                        lazyWorker.submit(new Runnable() {
                            @Override public void run() {
                                cancelStatement(stmt);
                            }
                        });
                    }
                    else
                        cancelStatement(stmt);
                }
            });
        }

        Session ses = H2Utils.session(conn);

        if (timeoutMillis > 0)
            ses.setQueryTimeout(timeoutMillis);

        if (lazyWorker != null)
            ses.setLazyQueryExecution(true);

        try {
            return stmt.executeQuery();
        }
        catch (SQLException e) {
            // Throw special exception.
            if (e.getErrorCode() == ErrorCode.STATEMENT_WAS_CANCELED)
                throw new QueryCancelledException();

            throw new IgniteCheckedException("Failed to execute SQL query. " + e.getMessage(), e);
        }
        finally {
            if (timeoutMillis > 0)
                ses.setQueryTimeout(0);

            if (lazyWorker != null)
                ses.setLazyQueryExecution(false);
        }
    }

    /**
     * Cancel prepared statement.
     *
     * @param stmt Statement.
     */
    private static void cancelStatement(PreparedStatement stmt) {
        try {
            stmt.cancel();
        }
        catch (SQLException ignored) {
            // No-op.
        }
    }

    /**
     * Executes sql query and prints warning if query is too slow..
     *
     * @param conn Connection,
     * @param sql Sql query.
     * @param params Parameters.
     * @param useStmtCache If {@code true} uses stmt cache.
     * @param timeoutMillis Query timeout.
     * @param cancel Query cancel.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    public ResultSet executeSqlQueryWithTimer(Connection conn, String sql, @Nullable Collection<Object> params,
        boolean useStmtCache, int timeoutMillis, @Nullable GridQueryCancel cancel) throws IgniteCheckedException {
        return executeSqlQueryWithTimer(preparedStatementWithParams(conn, sql, params, useStmtCache),
            conn, sql, params, timeoutMillis, cancel);
    }

    /**
     * Executes sql query and prints warning if query is too slow.
     *
     * @param stmt Prepared statement for query.
     * @param conn Connection.
     * @param sql Sql query.
     * @param params Parameters.
     * @param timeoutMillis Query timeout.
     * @param cancel Query cancel.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    public ResultSet executeSqlQueryWithTimer(PreparedStatement stmt, Connection conn, String sql,
        @Nullable Collection<Object> params, int timeoutMillis, @Nullable GridQueryCancel cancel)
        throws IgniteCheckedException {
        long start = U.currentTimeMillis();

        try {
            ResultSet rs = executeSqlQuery(conn, stmt, timeoutMillis, cancel);

            long time = U.currentTimeMillis() - start;

            long longQryExecTimeout = ctx.config().getLongQueryWarningTimeout();

            if (time > longQryExecTimeout) {
                ResultSet plan = executeSqlQuery(conn, preparedStatementWithParams(conn, "EXPLAIN " + sql,
                    params, false), 0, null);

                plan.next();

                // Add SQL explain result message into log.
                String msg = "Query execution is too long [time=" + time + " ms, sql='" + sql + '\'' +
                    ", plan=" + U.nl() + plan.getString(1) + U.nl() + ", parameters=" +
                    (params == null ? "[]" : Arrays.deepToString(params.toArray())) + "]";

                LT.warn(log, msg);
            }

            return rs;
        }
        catch (SQLException e) {
            onSqlException();

            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Binds parameters to prepared statement.
     *
     * @param stmt Prepared statement.
     * @param params Parameters collection.
     * @throws IgniteCheckedException If failed.
     */
    public void bindParameters(PreparedStatement stmt,
        @Nullable Collection<Object> params) throws IgniteCheckedException {
        if (!F.isEmpty(params)) {
            int idx = 1;

            for (Object arg : params)
                bindObject(stmt, idx++, arg);
        }
    }

    /** {@inheritDoc} */
    @Override public FieldsQueryCursor<List<?>> queryLocalSqlFields(String schemaName, SqlFieldsQuery qry,
        final boolean keepBinary, IndexingQueryFilter filter, GridQueryCancel cancel) throws IgniteCheckedException {
        String sql = qry.getSql();
        List<Object> params = F.asList(qry.getArgs());
        boolean enforceJoinOrder = qry.isEnforceJoinOrder(), startTx = autoStartTx(qry);
        int timeout = qry.getTimeout();

        final GridQueryFieldsResult res = queryLocalSqlFields(schemaName, sql, params, filter,
            enforceJoinOrder, startTx, timeout, cancel);

        QueryCursorImpl<List<?>> cursor = new QueryCursorImpl<>(new Iterable<List<?>>() {
            @SuppressWarnings("NullableProblems")
            @Override public Iterator<List<?>> iterator() {
                try {
                    return new GridQueryCacheObjectsIterator(res.iterator(), objectContext(), keepBinary);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        }, cancel);

        cursor.fieldsMeta(res.metaData());

        return cursor;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> QueryCursor<Cache.Entry<K,V>> queryLocalSql(String schemaName, String cacheName,
        final SqlQuery qry, final IndexingQueryFilter filter, final boolean keepBinary) throws IgniteCheckedException {
        String type = qry.getType();
        String sqlQry = qry.getSql();
        String alias = qry.getAlias();
        Object[] params = qry.getArgs();

        GridQueryCancel cancel = new GridQueryCancel();

        final GridCloseableIterator<IgniteBiTuple<K, V>> i = queryLocalSql(schemaName, cacheName, sqlQry, alias,
            F.asList(params), type, filter, cancel);

        return new QueryCursorImpl<>(new Iterable<Cache.Entry<K, V>>() {
            @SuppressWarnings("NullableProblems")
            @Override public Iterator<Cache.Entry<K, V>> iterator() {
                return new ClIter<Cache.Entry<K, V>>() {
                    @Override public void close() throws Exception {
                        i.close();
                    }

                    @Override public boolean hasNext() {
                        return i.hasNext();
                    }

                    @Override public Cache.Entry<K, V> next() {
                        IgniteBiTuple<K, V> t = i.next();

                        K key = (K)CacheObjectUtils.unwrapBinaryIfNeeded(objectContext(), t.get1(), keepBinary, false);
                        V val = (V)CacheObjectUtils.unwrapBinaryIfNeeded(objectContext(), t.get2(), keepBinary, false);

                        return new CacheEntryImpl<>(key, val);
                    }

                    @Override public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        }, cancel);
    }

    /**
     * Executes regular query.
     *
     * @param schemaName Schema name.
     * @param cacheName Cache name.
     * @param qry Query.
     * @param alias Table alias.
     * @param params Query parameters.
     * @param type Query return type.
     * @param filter Cache name and key filter.
     * @param cancel Cancel object.
     * @return Queried rows.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocalSql(String schemaName, String cacheName,
        final String qry, String alias, @Nullable final Collection<Object> params, String type,
        final IndexingQueryFilter filter, GridQueryCancel cancel) throws IgniteCheckedException {
        final H2TableDescriptor tbl = tableDescriptor(schemaName, cacheName, type);

        if (tbl == null)
            throw new IgniteSQLException("Failed to find SQL table for type: " + type,
                IgniteQueryErrorCode.TABLE_NOT_FOUND);

        String sql = generateQuery(qry, alias, tbl);

        Connection conn = connectionForThread(tbl.schemaName());

        H2Utils.setupConnection(conn, false, false);

        GridH2QueryContext qctx = new GridH2QueryContext(nodeId, nodeId, 0, LOCAL).filter(filter)
            .distributedJoinMode(OFF);

        PreparedStatement stmt = preparedStatementWithParams(conn, sql, params, true);

        MvccQueryTracker mvccTracker = mvccTracker(stmt, false);

        if (mvccTracker != null)
            qctx.mvccSnapshot(mvccTracker.snapshot());

        GridH2QueryContext.set(qctx);

        GridRunningQueryInfo run = new GridRunningQueryInfo(qryIdGen.incrementAndGet(), qry, SQL, schemaName,
            U.currentTimeMillis(), null, true);

        runs.put(run.id(), run);

        try {
            ResultSet rs = executeSqlQueryWithTimer(stmt, conn, sql, params, 0, cancel);

            return new H2KeyValueIterator(rs);
        }
        finally {
            GridH2QueryContext.clearThreadLocal();

            if (mvccTracker != null)
                mvccTracker.onDone();

            runs.remove(run.id());
        }
    }

    /**
     * Initialises MVCC filter and returns MVCC query tracker if needed.
     * @param stmt Prepared statement.
     * @param startTx Start transaction flag.
     * @return MVCC query tracker or {@code null} if MVCC is disabled for involved caches.
     */
    private MvccQueryTracker mvccTracker(PreparedStatement stmt, boolean startTx) throws IgniteCheckedException {
        boolean mvccEnabled;

        GridCacheContext mvccCacheCtx = null;

        try {
            if (stmt.isWrapperFor(PreparedStatementEx.class)) {
                PreparedStatementEx stmtEx = stmt.unwrap(PreparedStatementEx.class);

                Boolean mvccState = stmtEx.meta(MVCC_STATE);

                mvccEnabled = mvccState != null ? mvccState : checkMvcc(stmt);

                if (mvccEnabled) {
                    Integer cacheId = stmtEx.meta(MVCC_CACHE_ID);

                    assert cacheId != null;

                    mvccCacheCtx = ctx.cache().context().cacheContext(cacheId);

                    assert mvccCacheCtx != null;
                }
            }
            else
                mvccEnabled = checkMvcc(stmt);
        }
        catch (SQLException e) {
            throw new IgniteSQLException(e);
        }

        assert !mvccEnabled || mvccCacheCtx != null;

        return mvccEnabled ? MvccUtils.mvccTracker(mvccCacheCtx, startTx) : null;
    }

    /**
     * Checks if statement uses MVCC caches. If it does, additional metadata is added to statement.
     *
     * @param stmt Statement to check.
     * @return {@code True} if there MVCC cache involved in statement.
     * @throws SQLException If parser failed.
     */
    private static Boolean checkMvcc(PreparedStatement stmt) throws SQLException {
        GridSqlQueryParser parser = new GridSqlQueryParser(false);

        parser.parse(GridSqlQueryParser.prepared(stmt));

        Boolean mvccEnabled = null;
        Integer mvccCacheId = null;
        GridCacheContext ctx0 = null;

        for (Object o : parser.objectsMap().values()) {
            if (o instanceof GridSqlAlias)
                o = GridSqlAlias.unwrap((GridSqlAst) o);
            if (o instanceof GridSqlTable && ((GridSqlTable) o).dataTable() != null) {
                GridCacheContext cctx = ((GridSqlTable) o).dataTable().cache();

                if (mvccEnabled == null) {
                    mvccEnabled = cctx.mvccEnabled();
                    mvccCacheId = cctx.cacheId();
                    ctx0 = cctx;
                }
                else if (mvccEnabled != cctx.mvccEnabled())
                    MvccUtils.throwAtomicityModesMismatchException(ctx0, cctx);
            }
        }

        if (mvccEnabled == null)
            return false;

        // Remember mvccEnabled flag to avoid further additional parsing if statement obtained from the statement cache.
        if (stmt.isWrapperFor(PreparedStatementEx.class)) {
            PreparedStatementEx stmtEx = stmt.unwrap(PreparedStatementEx.class);

            if (mvccEnabled) {
                assert mvccCacheId != null;

                stmtEx.putMeta(MVCC_CACHE_ID, mvccCacheId);
                stmtEx.putMeta(MVCC_STATE, Boolean.TRUE);
            }
            else
                stmtEx.putMeta(MVCC_STATE, Boolean.FALSE);
        }

        return mvccEnabled;
    }

    /**
     * @param schemaName Schema name.
     * @param qry Query.
     * @param keepCacheObj Flag to keep cache object.
     * @param enforceJoinOrder Enforce join order of tables.
     * @param startTx Start transaction flag.
     * @param qryTimeout Query timeout.
     * @param cancel Cancel object.
     * @param params Query parameters.
     * @param parts Partitions.
     * @param lazy Lazy query execution flag.
     * @param mvccTracker Query tracker.
     * @return Iterable result.
     */
    private Iterable<List<?>> runQueryTwoStep(
        final String schemaName,
        final GridCacheTwoStepQuery qry,
        final boolean keepCacheObj,
        final boolean enforceJoinOrder,
        boolean startTx,
        final int qryTimeout,
        final GridQueryCancel cancel,
        final Object[] params,
        final int[] parts,
        final boolean lazy,
        MvccQueryTracker mvccTracker) {
        assert !qry.mvccEnabled() || !F.isEmpty(qry.cacheIds());

        try {
            final MvccQueryTracker tracker = mvccTracker == null && qry.mvccEnabled() ?
                MvccUtils.mvccTracker(ctx.cache().context().cacheContext(qry.cacheIds().get(0)), startTx) : mvccTracker;

            GridNearTxLocal tx = tx(ctx);

            if (qry.forUpdate())
                qry.forUpdate(checkActive(tx) != null);

            int opTimeout = operationTimeout(qryTimeout, tx);

            return new Iterable<List<?>>() {
                @SuppressWarnings("NullableProblems")
                @Override public Iterator<List<?>> iterator() {
                    return rdcQryExec.query(schemaName, qry, keepCacheObj, enforceJoinOrder, opTimeout,
                        cancel, params, parts, lazy, tracker);
                }
            };
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
    }

    /**
     * Run DML on remote nodes.
     *
     * @param schemaName Schema name.
     * @param fieldsQry Initial update query.
     * @param cacheIds Cache identifiers.
     * @param isReplicatedOnly Whether query uses only replicated caches.
     * @param cancel Cancel state.
     * @return Update result.
     */
    UpdateResult runDistributedUpdate(
        String schemaName,
        SqlFieldsQuery fieldsQry,
        List<Integer> cacheIds,
        boolean isReplicatedOnly,
        GridQueryCancel cancel) {
        return rdcQryExec.update(schemaName, cacheIds, fieldsQry.getSql(), fieldsQry.getArgs(),
            fieldsQry.isEnforceJoinOrder(), fieldsQry.getPageSize(), fieldsQry.getTimeout(),
            fieldsQry.getPartitions(), isReplicatedOnly, cancel);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> QueryCursor<Cache.Entry<K, V>> queryDistributedSql(String schemaName, String cacheName,
        SqlQuery qry, boolean keepBinary) {
        String type = qry.getType();

        H2TableDescriptor tblDesc = tableDescriptor(schemaName, cacheName, type);

        if (tblDesc == null)
            throw new IgniteSQLException("Failed to find SQL table for type: " + type,
                IgniteQueryErrorCode.TABLE_NOT_FOUND);

        String sql;

        try {
            sql = generateQuery(qry.getSql(), qry.getAlias(), tblDesc);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        SqlFieldsQuery fqry = new SqlFieldsQuery(sql);

        fqry.setArgs(qry.getArgs());
        fqry.setPageSize(qry.getPageSize());
        fqry.setDistributedJoins(qry.isDistributedJoins());
        fqry.setPartitions(qry.getPartitions());
        fqry.setLocal(qry.isLocal());

        if (qry.getTimeout() > 0)
            fqry.setTimeout(qry.getTimeout(), TimeUnit.MILLISECONDS);

        final QueryCursor<List<?>> res =
            querySqlFields(schemaName, fqry, null, keepBinary, true, null, null).get(0);

        final Iterable<Cache.Entry<K, V>> converted = new Iterable<Cache.Entry<K, V>>() {
            @Override public Iterator<Cache.Entry<K, V>> iterator() {
                final Iterator<List<?>> iter0 = res.iterator();

                return new Iterator<Cache.Entry<K, V>>() {
                    @Override public boolean hasNext() {
                        return iter0.hasNext();
                    }

                    @Override public Cache.Entry<K, V> next() {
                        List<?> l = iter0.next();

                        return new CacheEntryImpl<>((K)l.get(0), (V)l.get(1));
                    }

                    @Override public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };

        // No metadata for SQL queries.
        return new QueryCursorImpl<Cache.Entry<K, V>>(converted) {
            @Override public void close() {
                res.close();
            }
        };
    }

    /**
     * Try executing query using native facilities.
     *
     * @param schemaName Schema name.
     * @param qry Query.
     * @param cliCtx Client context, or {@code null} if not applicable.
     * @return Result or {@code null} if cannot parse/process this query.
     */
    @SuppressWarnings({"ConstantConditions", "StatementWithEmptyBody"})
    private List<FieldsQueryCursor<List<?>>> tryQueryDistributedSqlFieldsNative(String schemaName, SqlFieldsQuery qry,
        @Nullable SqlClientContext cliCtx) {
        // Heuristic check for fast return.
        if (!INTERNAL_CMD_RE.matcher(qry.getSql().trim()).find())
            return null;

        // Parse.
        SqlCommand cmd;

        try {
            SqlParser parser = new SqlParser(schemaName, qry.getSql());

            cmd = parser.nextCommand();

            // No support for multiple commands for now.
            if (parser.nextCommand() != null)
                return null;

            if (!(cmd instanceof SqlCreateIndexCommand
                || cmd instanceof SqlDropIndexCommand
                || cmd instanceof SqlBeginTransactionCommand
                || cmd instanceof SqlCommitTransactionCommand
                || cmd instanceof SqlRollbackTransactionCommand
                || cmd instanceof SqlBulkLoadCommand
                || cmd instanceof SqlAlterTableCommand
                || cmd instanceof SqlSetStreamingCommand
                || cmd instanceof SqlCreateUserCommand
                || cmd instanceof SqlAlterUserCommand
                || cmd instanceof SqlDropUserCommand))
                return null;
        }
        catch (SqlStrictParseException e) {
            throw new IgniteSQLException(e.getMessage(), IgniteQueryErrorCode.PARSING, e);
        }
        catch (Exception e) {
            // Cannot parse, return.
            if (log.isDebugEnabled())
                log.debug("Failed to parse SQL with native parser [qry=" + qry.getSql() + ", err=" + e + ']');

            if (!IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK))
                return null;

            int code = IgniteQueryErrorCode.PARSING;

            if (e instanceof SqlParseException)
                code = ((SqlParseException)e).code();

            throw new IgniteSQLException("Failed to parse DDL statement: " + qry.getSql() + ": " + e.getMessage(),
                code, e);
        }

        // Execute.
        try {
            if (cmd instanceof SqlCreateIndexCommand
                || cmd instanceof SqlDropIndexCommand
                || cmd instanceof SqlAlterTableCommand
                || cmd instanceof SqlCreateUserCommand
                || cmd instanceof SqlAlterUserCommand
                || cmd instanceof SqlDropUserCommand)
                return Collections.singletonList(ddlProc.runDdlStatement(qry.getSql(), cmd));
            else if (cmd instanceof SqlBulkLoadCommand)
                return Collections.singletonList(dmlProc.runNativeDmlStatement(qry.getSql(), cmd));
            else if (cmd instanceof SqlSetStreamingCommand) {
                if (cliCtx == null)
                    throw new IgniteSQLException("SET STREAMING command can only be executed from JDBC or ODBC driver.");

                SqlSetStreamingCommand setCmd = (SqlSetStreamingCommand)cmd;

                if (setCmd.isTurnOn())
                    cliCtx.enableStreaming(setCmd.allowOverwrite(), setCmd.flushFrequency(),
                        setCmd.perNodeBufferSize(), setCmd.perNodeParallelOperations(), setCmd.isOrdered());
                else
                    cliCtx.disableStreaming();
            }
            else
                processTxCommand(cmd, qry);

            return Collections.singletonList(H2Utils.zeroCursor());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSQLException("Failed to execute DDL statement [stmt=" + qry.getSql() + ']', e);
        }
    }

    /**
     * Check expected statement type (when it is set by JDBC) and given statement type.
     *
     * @param qry Query.
     * @param isQry {@code true} for select queries, otherwise (DML/DDL queries) {@code false}.
     */
    private void checkQueryType(SqlFieldsQuery qry, boolean isQry) {
        Boolean qryFlag = qry instanceof SqlFieldsQueryEx ? ((SqlFieldsQueryEx) qry).isQuery() : null;

        if (qryFlag != null && qryFlag != isQry)
            throw new IgniteSQLException("Given statement type does not match that declared by JDBC driver",
                IgniteQueryErrorCode.STMT_TYPE_MISMATCH);
    }

    /**
     * Process transactional command.
     * @param cmd Command.
     * @param qry Query.
     * @throws IgniteCheckedException if failed.
     */
    private void processTxCommand(SqlCommand cmd, SqlFieldsQuery qry) throws IgniteCheckedException {
        if (!mvccEnabled(ctx))
            throw new IgniteSQLException("MVCC must be enabled in order to invoke transactional operation: " +
                qry.getSql(), IgniteQueryErrorCode.MVCC_DISABLED);

        NestedTxMode nestedTxMode = qry instanceof SqlFieldsQueryEx ? ((SqlFieldsQueryEx)qry).getNestedTxMode() :
            NestedTxMode.DEFAULT;

        GridNearTxLocal tx = tx(ctx);

        if (cmd instanceof SqlBeginTransactionCommand) {
            if (tx != null) {
                if (nestedTxMode == null)
                    nestedTxMode = NestedTxMode.DEFAULT;

                switch (nestedTxMode) {
                    case COMMIT:
                        doCommit(tx);

                        txStart(ctx, qry.getTimeout());

                        break;

                    case IGNORE:
                        log.warning("Transaction has already been started, ignoring BEGIN command.");

                        break;

                    case ERROR:
                        throw new IgniteSQLException("Transaction has already been started.",
                            IgniteQueryErrorCode.TRANSACTION_EXISTS);

                    default:
                        throw new IgniteSQLException("Unexpected nested transaction handling mode: " +
                            nestedTxMode.name());
                }
            }
            else
                txStart(ctx, qry.getTimeout());
        }
        else if (cmd instanceof SqlCommitTransactionCommand) {
            // Do nothing if there's no transaction.
            if (tx != null)
                doCommit(tx);
        }
        else {
            assert cmd instanceof SqlRollbackTransactionCommand;

            // Do nothing if there's no transaction.
            if (tx != null)
                doRollback(tx);
        }
    }

    /**
     * Commit and properly close transaction.
     * @param tx Transaction.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("ThrowFromFinallyBlock")
    private void doCommit(@NotNull GridNearTxLocal tx) throws IgniteCheckedException {
        try {
            if (!tx.isRollbackOnly())
                tx.commit();
        }
        finally {
            closeTx(tx);
        }
    }

    /**
     * Rollback and properly close transaction.
     * @param tx Transaction.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("ThrowFromFinallyBlock")
    private void doRollback(@NotNull GridNearTxLocal tx) throws IgniteCheckedException {
        try {
            tx.rollback();
        }
        finally {
            closeTx(tx);
        }
    }

    /**
     * Properly close transaction.
     * @param tx Transaction.
     * @throws IgniteCheckedException if failed.
     */
    private void closeTx(@NotNull GridNearTxLocal tx) throws IgniteCheckedException {
        try {
            tx.close();
        }
        finally {
            ctx.cache().context().tm().resetContext();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"StringEquality", "unchecked"})
    @Override public List<FieldsQueryCursor<List<?>>> querySqlFields(String schemaName, SqlFieldsQuery qry,
        @Nullable SqlClientContext cliCtx, boolean keepBinary, boolean failOnMultipleStmts, MvccQueryTracker tracker,
        GridQueryCancel cancel) {
        boolean mvccEnabled = mvccEnabled(ctx), startTx = autoStartTx(qry);

        try {
            List<FieldsQueryCursor<List<?>>> res = tryQueryDistributedSqlFieldsNative(schemaName, qry, cliCtx);

            if (res != null)
                return res;

            {
                // First, let's check if we already have a two-step query for this statement...
                H2TwoStepCachedQueryKey cachedQryKey = new H2TwoStepCachedQueryKey(schemaName, qry.getSql(),
                    qry.isCollocated(), qry.isDistributedJoins(), qry.isEnforceJoinOrder(), qry.isLocal());

                H2TwoStepCachedQuery cachedQry;

                if ((cachedQry = twoStepCache.get(cachedQryKey)) != null) {
                    checkQueryType(qry, true);

                    GridCacheTwoStepQuery twoStepQry = cachedQry.query().copy();

                    List<GridQueryFieldMetadata> meta = cachedQry.meta();

                    res = Collections.singletonList(doRunDistributedQuery(schemaName, qry, twoStepQry, meta, keepBinary,
                        startTx, tracker, cancel));

                    if (!twoStepQry.explain())
                        twoStepCache.putIfAbsent(cachedQryKey, new H2TwoStepCachedQuery(meta, twoStepQry.copy()));

                    return res;
                }
            }

            {
                // Second, let's check if we already have a parsed statement...
                PreparedStatement cachedStmt;

                if ((cachedStmt = cachedStatement(connectionForSchema(schemaName), qry.getSql())) != null) {
                    Prepared prepared = GridSqlQueryParser.prepared(cachedStmt);

                    // We may use this cached statement only for local queries and non queries.
                    if (qry.isLocal() || !prepared.isQuery())
                        return (List<FieldsQueryCursor<List<?>>>)doRunPrepared(schemaName, prepared, qry, null, null,
                            keepBinary, startTx, tracker, cancel);
                }
            }

            res = new ArrayList<>(1);

            int firstArg = 0;

            String remainingSql = qry.getSql();

            while (remainingSql != null) {
                ParsingResult parseRes = parseAndSplit(schemaName,
                    remainingSql != qry.getSql() ? cloneFieldsQuery(qry).setSql(remainingSql) : qry, firstArg);

                // Let's avoid second reflection getter call by returning Prepared object too
                Prepared prepared = parseRes.prepared();

                GridCacheTwoStepQuery twoStepQry = parseRes.twoStepQuery();

                List<GridQueryFieldMetadata> meta = parseRes.meta();

                SqlFieldsQuery newQry = parseRes.newQuery();

                remainingSql = parseRes.remainingSql();

                if (remainingSql != null && failOnMultipleStmts)
                    throw new IgniteSQLException("Multiple statements queries are not supported");

                firstArg += prepared.getParameters().size();

                res.addAll(doRunPrepared(schemaName, prepared, newQry, twoStepQry, meta, keepBinary, startTx, tracker,
                    cancel));

                if (parseRes.twoStepQuery() != null && parseRes.twoStepQueryKey() != null &&
                    !parseRes.twoStepQuery().explain())
                    twoStepCache.putIfAbsent(parseRes.twoStepQueryKey(), new H2TwoStepCachedQuery(meta,
                        twoStepQry.copy()));
            }

            return res;
        }
        catch (RuntimeException | Error e) {
            GridNearTxLocal tx;

            if (mvccEnabled && (tx = tx(ctx)) != null)
                tx.setRollbackOnly();

            throw e;
        }
    }

    /**
     * Execute an all-ready {@link SqlFieldsQuery}.
     * @param schemaName Schema name.
     * @param prepared H2 command.
     * @param qry Fields query with flags.
     * @param twoStepQry Two-step query if this query must be executed in a distributed way.
     * @param meta Metadata for {@code twoStepQry}.
     * @param keepBinary Whether binary objects must not be deserialized automatically.
     * @param startTx Start transactionq flag.
     * @param tracker MVCC tracker.
     * @param cancel Query cancel state holder.
     * @return Query result.
     */
    @SuppressWarnings("unchecked")
    private List<? extends FieldsQueryCursor<List<?>>> doRunPrepared(String schemaName, Prepared prepared,
        SqlFieldsQuery qry, GridCacheTwoStepQuery twoStepQry, List<GridQueryFieldMetadata> meta, boolean keepBinary,
        boolean startTx, MvccQueryTracker tracker, GridQueryCancel cancel) {
        String sqlQry = qry.getSql();

        boolean loc = qry.isLocal();

        IndexingQueryFilter filter = (loc ? backupFilter(null, qry.getPartitions()) : null);

        if (!prepared.isQuery()) {
            if (DmlStatementsProcessor.isDmlStatement(prepared)) {
                try {
                    Connection conn = connectionForSchema(schemaName);

                    if (!loc)
                        return dmlProc.updateSqlFieldsDistributed(schemaName, conn, prepared, qry, cancel);
                    else {
                        final GridQueryFieldsResult updRes =
                            dmlProc.updateSqlFieldsLocal(schemaName, conn, prepared, qry, filter, cancel);

                        return Collections.singletonList(new QueryCursorImpl<>(new Iterable<List<?>>() {
                            @SuppressWarnings("NullableProblems")
                            @Override public Iterator<List<?>> iterator() {
                                try {
                                    return new GridQueryCacheObjectsIterator(updRes.iterator(), objectContext(),
                                        true);
                                }
                                catch (IgniteCheckedException e) {
                                    throw new IgniteException(e);
                                }
                            }
                        }, cancel));
                    }
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteSQLException("Failed to execute DML statement [stmt=" + sqlQry +
                        ", params=" + Arrays.deepToString(qry.getArgs()) + "]", e);
                }
            }

            if (DdlStatementsProcessor.isDdlStatement(prepared)) {
                if (loc)
                    throw new IgniteSQLException("DDL statements are not supported for LOCAL caches",
                        IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

                return Collections.singletonList(ddlProc.runDdlStatement(sqlQry, prepared));
            }

            if (prepared instanceof NoOperation) {
                QueryCursorImpl<List<?>> resCur = (QueryCursorImpl<List<?>>)new QueryCursorImpl(
                    Collections.singletonList(Collections.singletonList(0L)), null, false);

                resCur.fieldsMeta(UPDATE_RESULT_META);

                return Collections.singletonList(resCur);
            }

            throw new IgniteSQLException("Unsupported DDL/DML operation: " + prepared.getClass().getName(),
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        if (twoStepQry != null) {
            if (log.isDebugEnabled())
                log.debug("Parsed query: `" + sqlQry + "` into two step query: " + twoStepQry);

            checkQueryType(qry, true);

            return Collections.singletonList(doRunDistributedQuery(schemaName, qry, twoStepQry, meta, keepBinary,
                startTx, tracker, cancel));
        }

        // We've encountered a local query, let's just run it.
        try {
            return Collections.singletonList(queryLocalSqlFields(schemaName, qry, keepBinary, filter, cancel));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSQLException("Failed to execute local statement [stmt=" + sqlQry +
                ", params=" + Arrays.deepToString(qry.getArgs()) + "]", e);
        }
    }

    /**
     * Parse and split query if needed, cache either two-step query or statement.
     * @param schemaName Schema name.
     * @param qry Query.
     * @param firstArg Position of the first argument of the following {@code Prepared}.
     * @return Result: prepared statement, H2 command, two-step query (if needed),
     *     metadata for two-step query (if needed), evaluated query local execution flag.
     */
    private ParsingResult parseAndSplit(String schemaName, SqlFieldsQuery qry, int firstArg) {
        Connection c = connectionForSchema(schemaName);

        // For queries that are explicitly local, we rely on the flag specified in the query
        // because this parsing result will be cached and used for queries directly.
        // For other queries, we enforce join order at this stage to avoid premature optimizations
        // (and therefore longer parsing) as long as there'll be more parsing at split stage.
        boolean enforceJoinOrderOnParsing = (!qry.isLocal() || qry.isEnforceJoinOrder());

        H2Utils.setupConnection(c, /*distributedJoins*/false, /*enforceJoinOrder*/enforceJoinOrderOnParsing);

        boolean loc = qry.isLocal();

        PreparedStatement stmt = prepareStatementAndCaches(c, qry.getSql());

        if (loc && GridSqlQueryParser.checkMultipleStatements(stmt))
            throw new IgniteSQLException("Multiple statements queries are not supported for local queries.",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        GridSqlQueryParser.PreparedWithRemaining prep = GridSqlQueryParser.preparedWithRemaining(stmt);

        Prepared prepared = prep.prepared();

        checkQueryType(qry, prepared.isQuery());

        String remainingSql = prep.remainingSql();

        int paramsCnt = prepared.getParameters().size();

        Object[] argsOrig = qry.getArgs();

        Object[] args = null;

        if (!DmlUtils.isBatched(qry) && paramsCnt > 0) {
            if (argsOrig == null || argsOrig.length < firstArg + paramsCnt) {
                throw new IgniteException("Invalid number of query parameters. " +
                    "Cannot find " + (argsOrig != null ? argsOrig.length + 1 - firstArg : 1) + " parameter.");
            }

            args = Arrays.copyOfRange(argsOrig, firstArg, firstArg + paramsCnt);
        }

       if (prepared.isQuery()) {
            try {
                bindParameters(stmt, F.asList(args));
            }
            catch (IgniteCheckedException e) {
                U.closeQuiet(stmt);

                throw new IgniteSQLException("Failed to bind parameters: [qry=" + prepared.getSQL() + ", params=" +
                    Arrays.deepToString(args) + "]", IgniteQueryErrorCode.PARSING, e);
            }

            GridSqlQueryParser parser = null;

            if (!loc) {
                parser = new GridSqlQueryParser(false);

                GridSqlStatement parsedStmt = parser.parse(prepared);

                // Legit assertion - we have H2 query flag above.
                assert parsedStmt instanceof GridSqlQuery;

                loc = parser.isLocalQuery(qry.isReplicatedOnly());
            }

            if (loc) {
                if (parser == null) {
                    parser = new GridSqlQueryParser(false);

                    parser.parse(prepared);
                }

                GridCacheContext cctx = parser.getFirstPartitionedCache();

                if (cctx != null && cctx.config().getQueryParallelism() > 1) {
                    loc = false;

                    qry.setDistributedJoins(true);
                }
            }
        }

        SqlFieldsQuery newQry = cloneFieldsQuery(qry).setSql(prepared.getSQL()).setArgs(args);

        boolean hasTwoStep = !loc && prepared.isQuery();

        // Let's not cache multiple statements and distributed queries as whole two step query will be cached later on.
        if (remainingSql != null || hasTwoStep)
            getStatementsCacheForCurrentThread().remove(schemaName, qry.getSql());

        if (!hasTwoStep)
            return new ParsingResult(prepared, newQry, remainingSql, null, null, null);

        final UUID locNodeId = ctx.localNodeId();

        // Now we're sure to have a distributed query. Let's try to get a two-step plan from the cache, or perform the
        // split if needed.
        H2TwoStepCachedQueryKey cachedQryKey = new H2TwoStepCachedQueryKey(schemaName, qry.getSql(),
            qry.isCollocated(), qry.isDistributedJoins(), qry.isEnforceJoinOrder(), qry.isLocal());

        H2TwoStepCachedQuery cachedQry;

        if ((cachedQry = twoStepCache.get(cachedQryKey)) != null) {
            checkQueryType(qry, true);

            GridCacheTwoStepQuery twoStepQry = cachedQry.query().copy();

            List<GridQueryFieldMetadata> meta = cachedQry.meta();

            return new ParsingResult(prepared, newQry, remainingSql, twoStepQry, cachedQryKey, meta);
        }

        try {
            GridH2QueryContext.set(new GridH2QueryContext(locNodeId, locNodeId, 0, PREPARE)
                .distributedJoinMode(distributedJoinMode(qry.isLocal(), qry.isDistributedJoins())));

            try {
                GridCacheTwoStepQuery twoStepQry = split(prepared, newQry);

                return new ParsingResult(prepared, newQry, remainingSql, twoStepQry,
                    cachedQryKey, H2Utils.meta(stmt.getMetaData()));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteSQLException("Failed to bind parameters: [qry=" + newQry.getSql() + ", params=" +
                    Arrays.deepToString(newQry.getArgs()) + "]", IgniteQueryErrorCode.PARSING, e);
            }
            catch (SQLException e) {
                throw new IgniteSQLException(e);
            }
            finally {
                U.close(stmt, log);
            }
        }
        finally {
            GridH2QueryContext.clearThreadLocal();
        }
    }

    /**
     * Make a copy of {@link SqlFieldsQuery} with all flags and preserving type.
     * @param oldQry Query to copy.
     * @return Query copy.
     */
    private SqlFieldsQuery cloneFieldsQuery(SqlFieldsQuery oldQry) {
        return oldQry.copy().setLocal(oldQry.isLocal()).setPageSize(oldQry.getPageSize());
    }

    /**
     * Split query into two-step query.
     * @param prepared JDBC prepared statement.
     * @param qry Original fields query.
     * @return Two-step query.
     * @throws IgniteCheckedException in case of error inside {@link GridSqlQuerySplitter}.
     * @throws SQLException in case of error inside {@link GridSqlQuerySplitter}.
     */
    private GridCacheTwoStepQuery split(Prepared prepared, SqlFieldsQuery qry) throws IgniteCheckedException,
        SQLException {
        GridCacheTwoStepQuery res = GridSqlQuerySplitter.split(connectionForThread(qry.getSchema()), prepared,
            qry.getArgs(), qry.isCollocated(), qry.isDistributedJoins(), qry.isEnforceJoinOrder(), this);

        List<Integer> cacheIds = collectCacheIds(null, res);

        if (!F.isEmpty(cacheIds) && hasSystemViews(res)) {
            throw new IgniteSQLException("Normal tables and system views cannot be used in the same query.",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        if (F.isEmpty(cacheIds))
            res.local(true);
        else {
            res.cacheIds(cacheIds);
            res.local(qry.isLocal());
        }

        res.pageSize(qry.getPageSize());

        return res;
    }

    /**
     * @param qry Sql fields query.autoStartTx(qry)
     * @return {@code True} if need to start transaction.
     */
    public boolean autoStartTx(SqlFieldsQuery qry) {
        if (!mvccEnabled(ctx))
            return false;

        return qry instanceof SqlFieldsQueryEx && !((SqlFieldsQueryEx)qry).isAutoCommit() && tx(ctx) == null;
    }

    /** {@inheritDoc} */
    @Override public UpdateSourceIterator<?> prepareDistributedUpdate(GridCacheContext<?, ?> cctx, int[] ids,
        int[] parts, String schema, String qry, Object[] params, int flags,
        int pageSize, int timeout, AffinityTopologyVersion topVer,
        MvccSnapshot mvccSnapshot, GridQueryCancel cancel) throws IgniteCheckedException {

        SqlFieldsQuery fldsQry = new SqlFieldsQuery(qry);

        if (params != null)
            fldsQry.setArgs(params);

        fldsQry.setEnforceJoinOrder(isFlagSet(flags, GridH2QueryRequest.FLAG_ENFORCE_JOIN_ORDER));
        fldsQry.setTimeout(timeout, TimeUnit.MILLISECONDS);
        fldsQry.setPageSize(pageSize);
        fldsQry.setLocal(true);

        boolean loc = true;

        final boolean replicated = isFlagSet(flags, GridH2QueryRequest.FLAG_REPLICATED);

        GridCacheContext<?, ?> cctx0;

        if (!replicated
            && !F.isEmpty(ids)
            && (cctx0 = CU.firstPartitioned(cctx.shared(), ids)) != null
            && cctx0.config().getQueryParallelism() > 1) {
            fldsQry.setDistributedJoins(true);

            loc = false;
        }

        Connection conn = connectionForSchema(schema);

        H2Utils.setupConnection(conn, false, fldsQry.isEnforceJoinOrder());

        PreparedStatement stmt = preparedStatementWithParams(conn, fldsQry.getSql(),
            F.asList(fldsQry.getArgs()), true);

        return dmlProc.prepareDistributedUpdate(schema, conn, stmt, fldsQry, backupFilter(topVer, parts), cancel, loc,
            topVer, mvccSnapshot);
    }

    private boolean isFlagSet(int flags, int flag) {
        return (flags & flag) == flag;
    }

    /**
     * Run distributed query on detected set of partitions.
     * @param schemaName Schema name.
     * @param qry Original query.
     * @param twoStepQry Two-step query.
     * @param meta Metadata to set to cursor.
     * @param keepBinary Keep binary flag.
     * @param startTx Start transaction flag.
     * @param mvccTracker Query tracker.
     * @param cancel Cancel handler.
     * @return Cursor representing distributed query result.
     */
    private FieldsQueryCursor<List<?>> doRunDistributedQuery(String schemaName, SqlFieldsQuery qry,
        GridCacheTwoStepQuery twoStepQry, List<GridQueryFieldMetadata> meta, boolean keepBinary,
        boolean startTx, MvccQueryTracker mvccTracker, GridQueryCancel cancel) {
        if (log.isDebugEnabled())
            log.debug("Parsed query: `" + qry.getSql() + "` into two step query: " + twoStepQry);

        twoStepQry.pageSize(qry.getPageSize());

        if (cancel == null)
            cancel = new GridQueryCancel();

        int partitions[] = qry.getPartitions();

        if (partitions == null && twoStepQry.derivedPartitions() != null) {
            try {
                partitions = calculateQueryPartitions(twoStepQry.derivedPartitions(), qry.getArgs());
            }
            catch (IgniteCheckedException e) {
                throw new CacheException("Failed to calculate derived partitions: [qry=" + qry.getSql() + ", params=" +
                    Arrays.deepToString(qry.getArgs()) + "]", e);
            }
        }

        QueryCursorImpl<List<?>> cursor = new QueryCursorImpl<>(
            runQueryTwoStep(schemaName, twoStepQry, keepBinary, qry.isEnforceJoinOrder(), startTx, qry.getTimeout(),
                cancel, qry.getArgs(), partitions, qry.isLazy(), mvccTracker), cancel);

        cursor.fieldsMeta(meta);

        return cursor;
    }

    /**
     * Do initial parsing of the statement and create query caches, if needed.
     * @param c Connection.
     * @param sqlQry Query.
     * @return H2 prepared statement.
     */
    private PreparedStatement prepareStatementAndCaches(Connection c, String sqlQry) {
        boolean cachesCreated = false;

        while (true) {
            try {
                return prepareStatement(c, sqlQry, true);
            }
            catch (SQLException e) {
                if (!cachesCreated && (
                        e.getErrorCode() == ErrorCode.SCHEMA_NOT_FOUND_1 ||
                            e.getErrorCode() == ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1 ||
                            e.getErrorCode() == ErrorCode.INDEX_NOT_FOUND_1)
                        ) {
                    try {
                        ctx.cache().createMissingQueryCaches();
                    }
                    catch (IgniteCheckedException ignored) {
                        throw new CacheException("Failed to create missing caches.", e);
                    }

                    cachesCreated = true;
                }
                else
                    throw new IgniteSQLException("Failed to parse query. " + e.getMessage(),
                        IgniteQueryErrorCode.PARSING, e);
            }
        }
    }

    /**
     * Run DML request from other node.
     *
     * @param schemaName Schema name.
     * @param fldsQry Query.
     * @param filter Filter.
     * @param cancel Cancel state.
     * @param local Locality flag.
     * @return Update result.
     * @throws IgniteCheckedException if failed.
     */
    public UpdateResult mapDistributedUpdate(String schemaName, SqlFieldsQuery fldsQry, IndexingQueryFilter filter,
        GridQueryCancel cancel, boolean local) throws IgniteCheckedException {
        Connection conn = connectionForSchema(schemaName);

        H2Utils.setupConnection(conn, false, fldsQry.isEnforceJoinOrder());

        PreparedStatement stmt = preparedStatementWithParams(conn, fldsQry.getSql(),
            Arrays.asList(fldsQry.getArgs()), true);

        return dmlProc.mapDistributedUpdate(schemaName, stmt, fldsQry, filter, cancel, local);
    }

    /**
     * @param cacheIds Cache IDs.
     * @param twoStepQry Query.
     * @throws IllegalStateException if segmented indices used with non-segmented indices.
     */
    private void processCaches(List<Integer> cacheIds, GridCacheTwoStepQuery twoStepQry) {
        if (cacheIds.isEmpty())
            return; // Nothing to check

        GridCacheSharedContext sharedCtx = ctx.cache().context();

        int expectedParallelism = 0;
        GridCacheContext cctx0 = null;

        boolean mvccEnabled = false;

        for (int i = 0; i < cacheIds.size(); i++) {
            Integer cacheId = cacheIds.get(i);

            GridCacheContext cctx = sharedCtx.cacheContext(cacheId);

            assert cctx != null;

            if (i == 0) {
                mvccEnabled = cctx.mvccEnabled();
                cctx0 = cctx;
            }
            else if (cctx.mvccEnabled() != mvccEnabled)
                MvccUtils.throwAtomicityModesMismatchException(cctx0, cctx);

            if (!cctx.isPartitioned())
                continue;

            if (expectedParallelism == 0)
                expectedParallelism = cctx.config().getQueryParallelism();
            else if (cctx.config().getQueryParallelism() != expectedParallelism) {
                throw new IllegalStateException("Using indexes with different parallelism levels in same query is " +
                    "forbidden.");
            }
        }

        twoStepQry.mvccEnabled(mvccEnabled);

        if (twoStepQry.forUpdate()) {
            if (cacheIds.size() != 1)
                throw new IgniteSQLException("SELECT FOR UPDATE is supported only for queries " +
                    "that involve single transactional cache.");

            if (!mvccEnabled)
                throw new IgniteSQLException("SELECT FOR UPDATE query requires transactional cache " +
                    "with MVCC enabled.", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }
    }

    /**
     * Prepares statement for query.
     *
     * @param qry Query string.
     * @param tableAlias table alias.
     * @param tbl Table to use.
     * @return Prepared statement.
     * @throws IgniteCheckedException In case of error.
     */
    private String generateQuery(String qry, String tableAlias, H2TableDescriptor tbl) throws IgniteCheckedException {
        assert tbl != null;

        final String qry0 = qry;

        String t = tbl.fullTableName();

        String from = " ";

        qry = qry.trim();

        String upper = qry.toUpperCase();

        if (upper.startsWith("SELECT")) {
            qry = qry.substring(6).trim();

            final int star = qry.indexOf('*');

            if (star == 0)
                qry = qry.substring(1).trim();
            else if (star > 0) {
                if (F.eq('.', qry.charAt(star - 1))) {
                    t = qry.substring(0, star - 1);

                    qry = qry.substring(star + 1).trim();
                }
                else
                    throw new IgniteCheckedException("Invalid query (missing alias before asterisk): " + qry0);
            }
            else
                throw new IgniteCheckedException("Only queries starting with 'SELECT *' and 'SELECT alias.*' " +
                    "are supported (rewrite your query or use SqlFieldsQuery instead): " + qry0);

            upper = qry.toUpperCase();
        }

        if (!upper.startsWith("FROM"))
            from = " FROM " + t + (tableAlias != null ? " as " + tableAlias : "") +
                (upper.startsWith("WHERE") || upper.startsWith("ORDER") || upper.startsWith("LIMIT") ?
                    " " : " WHERE ");

        if(tableAlias != null)
            t = tableAlias;

        qry = "SELECT " + t + "." + KEY_FIELD_NAME + ", " + t + "." + VAL_FIELD_NAME + from + qry;

        return qry;
    }

    /**
     * Registers new class description.
     *
     * This implementation doesn't support type reregistration.
     *
     * @param type Type description.
     * @throws IgniteCheckedException In case of error.
     */
    @Override public boolean registerType(GridCacheContext cctx, GridQueryTypeDescriptor type)
        throws IgniteCheckedException {
        validateTypeDescriptor(type);

        String schemaName = schema(cctx.name());

        H2Schema schema = schemas.get(schemaName);

        H2TableDescriptor tbl = new H2TableDescriptor(this, schema, type, cctx);

        try {
            Connection conn = connectionForThread(schemaName);

            createTable(schemaName, schema, tbl, conn);

            schema.add(tbl);
        }
        catch (SQLException e) {
            onSqlException();

            throw new IgniteCheckedException("Failed to register query type: " + type, e);
        }

        return true;
    }

    /**
     * Validates properties described by query types.
     *
     * @param type Type descriptor.
     * @throws IgniteCheckedException If validation failed.
     */
    @SuppressWarnings("CollectionAddAllCanBeReplacedWithConstructor")
    private void validateTypeDescriptor(GridQueryTypeDescriptor type)
        throws IgniteCheckedException {
        assert type != null;

        Collection<String> names = new HashSet<>();

        names.addAll(type.fields().keySet());

        if (names.size() < type.fields().size())
            throw new IgniteCheckedException("Found duplicated properties with the same name [keyType=" +
                type.keyClass().getName() + ", valueType=" + type.valueClass().getName() + "]");

        String ptrn = "Name ''{0}'' is reserved and cannot be used as a field name [type=" + type.name() + "]";

        for (String name : names) {
            if (name.equalsIgnoreCase(KEY_FIELD_NAME) ||
                name.equalsIgnoreCase(VAL_FIELD_NAME) ||
                name.equalsIgnoreCase(VER_FIELD_NAME))
                throw new IgniteCheckedException(MessageFormat.format(ptrn, name));
        }
    }

    /**
     * Create db table by using given table descriptor.
     *
     * @param schemaName Schema name.
     * @param schema Schema.
     * @param tbl Table descriptor.
     * @param conn Connection.
     * @throws SQLException If failed to create db table.
     * @throws IgniteCheckedException If failed.
     */
    private void createTable(String schemaName, H2Schema schema, H2TableDescriptor tbl, Connection conn)
        throws SQLException, IgniteCheckedException {
        assert schema != null;
        assert tbl != null;

        GridQueryProperty keyProp = tbl.type().property(KEY_FIELD_NAME);
        GridQueryProperty valProp = tbl.type().property(VAL_FIELD_NAME);

        String keyType = dbTypeFromClass(tbl.type().keyClass(),
            keyProp == null ? -1 : keyProp.precision(),
            keyProp == null ? -1 : keyProp.scale());

        String valTypeStr = dbTypeFromClass(tbl.type().valueClass(),
            valProp == null ? -1 : valProp.precision(),
            valProp == null ? -1 : valProp.scale());

        SB sql = new SB();

        String keyValVisibility = tbl.type().fields().isEmpty() ? " VISIBLE" : " INVISIBLE";

        sql.a("CREATE TABLE ").a(tbl.fullTableName()).a(" (")
            .a(KEY_FIELD_NAME).a(' ').a(keyType).a(keyValVisibility).a(" NOT NULL");

        sql.a(',').a(VAL_FIELD_NAME).a(' ').a(valTypeStr).a(keyValVisibility);
        sql.a(',').a(VER_FIELD_NAME).a(" OTHER INVISIBLE");

        for (Map.Entry<String, Class<?>> e : tbl.type().fields().entrySet()) {
            GridQueryProperty prop = tbl.type().property(e.getKey());

            sql.a(',')
                .a(H2Utils.withQuotes(e.getKey()))
                .a(' ')
                .a(dbTypeFromClass(e.getValue(), prop.precision(), prop.scale()))
                .a(prop.notNull() ? " NOT NULL" : "");
        }

        sql.a(')');

        if (log.isDebugEnabled())
            log.debug("Creating DB table with SQL: " + sql);

        GridH2RowDescriptor rowDesc = new GridH2RowDescriptor(this, tbl, tbl.type());

        H2RowFactory rowFactory = tbl.rowFactory(rowDesc);

        GridH2Table h2Tbl = H2TableEngine.createTable(conn, sql.toString(), rowDesc, rowFactory, tbl);

        for (GridH2IndexBase usrIdx : tbl.createUserIndexes())
            addInitialUserIndex(schemaName, tbl, usrIdx);

        if (dataTables.putIfAbsent(h2Tbl.identifier(), h2Tbl) != null)
            throw new IllegalStateException("Table already exists: " + h2Tbl.identifierString());
    }

    /**
     * Find table by name in given schema.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @return Table or {@code null} if none found.
     */
    public GridH2Table dataTable(String schemaName, String tblName) {
        return dataTable(new QueryTable(schemaName, tblName));
    }

    /**
     * Find table by it's identifier.
     *
     * @param tbl Identifier.
     * @return Table or {@code null} if none found.
     */
    public GridH2Table dataTable(QueryTable tbl) {
        return dataTables.get(tbl);
    }

    /**
     * @param h2Tbl Remove data table.
     */
    public void removeDataTable(GridH2Table h2Tbl) {
        dataTables.remove(h2Tbl.identifier(), h2Tbl);
    }

    /**
     * Find table for index.
     *
     * @param schemaName Schema name.
     * @param idxName Index name.
     * @return Table or {@code null} if index is not found.
     */
    public GridH2Table dataTableForIndex(String schemaName, String idxName) {
        for (Map.Entry<QueryTable, GridH2Table> dataTableEntry : dataTables.entrySet()) {
            if (F.eq(dataTableEntry.getKey().schema(), schemaName)) {
                GridH2Table h2Tbl = dataTableEntry.getValue();

                if (h2Tbl.containsUserIndex(idxName))
                    return h2Tbl;
            }
        }

        return null;
    }

    /**
     * Gets corresponding DB type from java class.
     *
     * @param cls Java class.
     * @param precision Field precision.
     * @param scale Field scale.
     * @return DB type name.
     */
    private String dbTypeFromClass(Class<?> cls, int precision, int scale) {
        String dbType = H2DatabaseType.fromClass(cls).dBTypeAsString();

        if (precision != -1 && dbType.equalsIgnoreCase(H2DatabaseType.VARCHAR.dBTypeAsString()))
            return dbType + "(" + precision + ")";

        return dbType;
    }

    /**
     * Get table descriptor.
     *
     * @param schemaName Schema name.
     * @param cacheName Cache name.
     * @param type Type name.
     * @return Descriptor.
     */
    @Nullable private H2TableDescriptor tableDescriptor(String schemaName, String cacheName, String type) {
        H2Schema schema = schemas.get(schemaName);

        if (schema == null)
            return null;

        return schema.tableByTypeName(cacheName, type);
    }

    /** {@inheritDoc} */
    @Override public String schema(String cacheName) {
        String res = cacheName2schema.get(cacheName);

        if (res == null)
            res = "";

        return res;
    }

    /**
     * Gets collection of table for given schema name.
     *
     * @param cacheName Cache name.
     * @return Collection of table descriptors.
     */
    Collection<H2TableDescriptor> tables(String cacheName) {
        H2Schema s = schemas.get(schema(cacheName));

        if (s == null)
            return Collections.emptySet();

        List<H2TableDescriptor> tbls = new ArrayList<>();

        for (H2TableDescriptor tbl : s.tables()) {
            if (F.eq(tbl.cache().name(), cacheName))
                tbls.add(tbl);
        }

        return tbls;
    }

    /** {@inheritDoc} */
    @Override public void checkStatementStreamable(PreparedStatement nativeStmt) {
        if (!GridSqlQueryParser.isStreamableInsertStatement(nativeStmt))
            throw new IgniteSQLException("Streaming mode supports only INSERT commands without subqueries.",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /** {@inheritDoc} */
    @Override public GridQueryRowCacheCleaner rowCacheCleaner(int grpId) {
        return rowCache.forGroup(grpId);
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
     * Rebuild indexes from hash index.
     *
     * @param cacheName Cache name.
     * @throws IgniteCheckedException If failed.
     */
    @Override public void rebuildIndexesFromHash(String cacheName) throws IgniteCheckedException {
        int cacheId = CU.cacheId(cacheName);

        GridCacheContext cctx = ctx.cache().context().cacheContext(cacheId);

        final GridCacheQueryManager qryMgr = cctx.queries();

        SchemaIndexCacheVisitor visitor = new SchemaIndexCacheVisitorImpl(cctx);

        visitor.visit(new RebuildIndexFromHashClosure(qryMgr, cctx.mvccEnabled()));

        for (H2TableDescriptor tblDesc : tables(cacheName))
            tblDesc.table().markRebuildFromHashInProgress(false);
    }

    /** {@inheritDoc} */
    @Override public void markForRebuildFromHash(String cacheName) {
        for (H2TableDescriptor tblDesc : tables(cacheName)) {
            assert tblDesc.table() != null;

            tblDesc.table().markRebuildFromHashInProgress(true);
        }
    }

    /**
     * @return Busy lock.
     */
    public GridSpinBusyLock busyLock() {
        return busyLock;
    }

    /**
     * @return Map query executor.
     */
    public GridMapQueryExecutor mapQueryExecutor() {
        return mapQryExec;
    }

    /**
     * @return Reduce query executor.
     */
    public GridReduceQueryExecutor reduceQueryExecutor() {
        return rdcQryExec;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"NonThreadSafeLazyInitialization", "deprecation"})
    @Override public void start(GridKernalContext ctx, GridSpinBusyLock busyLock) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Starting cache query index...");

        this.busyLock = busyLock;

        qryIdGen = new AtomicLong();

        if (SysProperties.serializeJavaObject) {
            U.warn(log, "Serialization of Java objects in H2 was enabled.");

            SysProperties.serializeJavaObject = false;
        }

        String dbName = (ctx != null ? ctx.localNodeId() : UUID.randomUUID()).toString();

        dbUrl = "jdbc:h2:mem:" + dbName + DB_OPTIONS;

        org.h2.Driver.load();

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

        if (ctx == null) {
            // This is allowed in some tests.
            nodeId = UUID.randomUUID();
            marshaller = new JdkMarshaller();
        }
        else {
            this.ctx = ctx;

            // Register PUBLIC schema which is always present.
            schemas.put(QueryUtils.DFLT_SCHEMA, new H2Schema(QueryUtils.DFLT_SCHEMA, true));

            // Register additional schemas.
            String[] additionalSchemas = ctx.config().getSqlSchemas();

            if (!F.isEmpty(additionalSchemas)) {
                synchronized (schemaMux) {
                    for (String schema : additionalSchemas) {
                        if (F.isEmpty(schema))
                            continue;

                        schema = QueryUtils.normalizeSchemaName(null, schema);

                        createSchemaIfNeeded(schema, true);
                    }
                }
            }

            valCtx = new CacheQueryObjectValueContext(ctx);

            nodeId = ctx.localNodeId();
            marshaller = ctx.config().getMarshaller();

            mapQryExec = new GridMapQueryExecutor(busyLock);
            rdcQryExec = new GridReduceQueryExecutor(qryIdGen, busyLock);

            mapQryExec.start(ctx, this);
            rdcQryExec.start(ctx, this);

            stmtCacheCleanupTask = ctx.timeout().schedule(new Runnable() {
                @Override public void run() {
                    cleanupStatementCache();
                }
            }, CLEANUP_STMT_CACHE_PERIOD, CLEANUP_STMT_CACHE_PERIOD);

            dmlProc = new DmlStatementsProcessor();
            ddlProc = new DdlStatementsProcessor();

            dmlProc.start(ctx, this);
            ddlProc.start(ctx, this);

            boolean sysViewsEnabled =
                !IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_SQL_DISABLE_SYSTEM_VIEWS);

            if (sysViewsEnabled) {
                try {
                    synchronized (schemaMux) {
                        createSchema0(QueryUtils.SCHEMA_SYS);
                    }

                    Connection c = connectionForSchema(QueryUtils.SCHEMA_SYS);

                    for (SqlSystemView view : systemViews(ctx))
                        SqlSystemTableEngine.registerView(c, view);
                }
                catch (SQLException e) {
                    throw new IgniteCheckedException("Failed to register system view.", e);
                }

                // Caching this connection in ThreadLocal may lead to memory leaks.
                connCache.set(null);
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("SQL system views will not be created because they are disabled (see " +
                        IgniteSystemProperties.IGNITE_SQL_DISABLE_SYSTEM_VIEWS + " system property)");
            }
        }

        if (JdbcUtils.serializer != null)
            U.warn(log, "Custom H2 serialization is already configured, will override.");

        JdbcUtils.serializer = h2Serializer();

        assert ctx != null;

        connCleanupTask = ctx.timeout().schedule(new Runnable() {
            @Override public void run() {
                cleanupConnections();
            }
        }, CLEANUP_CONNECTIONS_PERIOD, CLEANUP_CONNECTIONS_PERIOD);
    }

    /**
     * @param ctx Context.
     * @return Predefined system views.
     */
    public Collection<SqlSystemView> systemViews(GridKernalContext ctx) {
        Collection<SqlSystemView> views = new ArrayList<>();

        views.add(new SqlSystemViewNodes(ctx));
        views.add(new SqlSystemViewNodeAttributes(ctx));
        views.add(new SqlSystemViewBaselineNodes(ctx));
        views.add(new SqlSystemViewNodeMetrics(ctx));

        return views;
    }

    /**
     * @return Value object context.
     */
    public CacheObjectValueContext objectContext() {
        return ctx.query().objectContext();
    }

    /**
     * @param topic Topic.
     * @param topicOrd Topic ordinal for {@link GridTopic}.
     * @param nodes Nodes.
     * @param msg Message.
     * @param specialize Optional closure to specialize message for each node.
     * @param locNodeHnd Handler for local node.
     * @param plc Policy identifying the executor service which will process message.
     * @param runLocParallel Run local handler in parallel thread.
     * @return {@code true} If all messages sent successfully.
     */
    public boolean send(
        Object topic,
        int topicOrd,
        Collection<ClusterNode> nodes,
        Message msg,
        @Nullable IgniteBiClosure<ClusterNode, Message, Message> specialize,
        @Nullable final IgniteInClosure2X<ClusterNode, Message> locNodeHnd,
        byte plc,
        boolean runLocParallel
    ) {
        boolean ok = true;

        if (specialize == null && msg instanceof GridCacheQueryMarshallable)
            ((GridCacheQueryMarshallable)msg).marshall(marshaller);

        ClusterNode locNode = null;

        for (ClusterNode node : nodes) {
            if (node.isLocal()) {
                if (locNode != null)
                    throw new IllegalStateException();

                locNode = node;

                continue;
            }

            try {
                if (specialize != null) {
                    msg = specialize.apply(node, msg);

                    if (msg instanceof GridCacheQueryMarshallable)
                        ((GridCacheQueryMarshallable)msg).marshall(marshaller);
                }

                ctx.io().sendGeneric(node, topic, topicOrd, msg, plc);
            }
            catch (IgniteCheckedException e) {
                ok = false;

                U.warn(log, "Failed to send message [node=" + node + ", msg=" + msg +
                    ", errMsg=" + e.getMessage() + "]");
            }
        }

        // Local node goes the last to allow parallel execution.
        if (locNode != null) {
            assert locNodeHnd != null;

            if (specialize != null)
                msg = specialize.apply(locNode, msg);

            if (runLocParallel) {
                final ClusterNode finalLocNode = locNode;
                final Message finalMsg = msg;

                try {
                    // We prefer runLocal to runLocalSafe, because the latter can produce deadlock here.
                    ctx.closure().runLocal(new GridPlainRunnable() {
                        @Override public void run() {
                            if (!busyLock.enterBusy())
                                return;

                            try {
                                locNodeHnd.apply(finalLocNode, finalMsg);
                            }
                            finally {
                                busyLock.leaveBusy();
                            }
                        }
                    }, plc).listen(logger);
                }
                catch (IgniteCheckedException e) {
                    ok = false;

                    U.error(log, "Failed to execute query locally.", e);
                }
            }
            else
                locNodeHnd.apply(locNode, msg);
        }

        return ok;
    }

    /**
     * @return Serializer.
     */
    private JavaObjectSerializer h2Serializer() {
        return new JavaObjectSerializer() {
            @Override public byte[] serialize(Object obj) throws Exception {
                return U.marshal(marshaller, obj);
            }

            @Override public Object deserialize(byte[] bytes) throws Exception {
                ClassLoader clsLdr = ctx != null ? U.resolveClassLoader(ctx.config()) : null;

                return U.unmarshal(marshaller, bytes, clsLdr);
            }
        };
    }

    /**
     * Registers SQL functions.
     *
     * @param schema Schema.
     * @param clss Classes.
     * @throws IgniteCheckedException If failed.
     */
    private void createSqlFunctions(String schema, Class<?>[] clss) throws IgniteCheckedException {
        if (F.isEmpty(clss))
            return;

        for (Class<?> cls : clss) {
            for (Method m : cls.getDeclaredMethods()) {
                QuerySqlFunction ann = m.getAnnotation(QuerySqlFunction.class);

                if (ann != null) {
                    int modifiers = m.getModifiers();

                    if (!Modifier.isStatic(modifiers) || !Modifier.isPublic(modifiers))
                        throw new IgniteCheckedException("Method " + m.getName() + " must be public static.");

                    String alias = ann.alias().isEmpty() ? m.getName() : ann.alias();

                    String clause = "CREATE ALIAS IF NOT EXISTS " + alias + (ann.deterministic() ?
                        " DETERMINISTIC FOR \"" :
                        " FOR \"") +
                        cls.getName() + '.' + m.getName() + '"';

                    executeStatement(schema, clause);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        if (log.isDebugEnabled())
            log.debug("Stopping cache query index...");

        mapQryExec.cancelLazyWorkers();

        for (H2ConnectionWrapper c : conns.values())
            U.close(c, log);

        conns.clear();
        schemas.clear();
        cacheName2schema.clear();

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

        GridH2QueryContext.clearLocalNodeStop(nodeId);

        if (log.isDebugEnabled())
            log.debug("Cache query index stopped.");

        // Close system H2 connection to INFORMATION_SCHEMA
        synchronized (schemaMux) {
            if (sysConn != null) {
                U.close(sysConn, log);

                sysConn = null;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onClientDisconnect() throws IgniteCheckedException {
        if (!mvccEnabled(ctx))
            return;

        GridNearTxLocal tx = tx(ctx);

        if (tx != null)
            doRollback(tx);
    }

    /** {@inheritDoc} */
    @Override public void registerCache(String cacheName, String schemaName, GridCacheContext<?, ?> cctx)
        throws IgniteCheckedException {
        rowCache.onCacheRegistered(cctx);

        synchronized (schemaMux) {
            createSchemaIfNeeded(schemaName, false);
        }

        cacheName2schema.put(cacheName, schemaName);

        createSqlFunctions(schemaName, cctx.config().getSqlFunctionClasses());
    }

    /** {@inheritDoc} */
    @Override public void unregisterCache(GridCacheContext cctx, boolean rmvIdx) {
        rowCache.onCacheUnregistered(cctx);

        String cacheName = cctx.name();

        String schemaName = schema(cacheName);

        H2Schema schema = schemas.get(schemaName);

        if (schema != null) {
            mapQryExec.onCacheStop(cacheName);
            dmlProc.onCacheStop(cacheName);

            // Remove this mapping only after callback to DML proc - it needs that mapping internally
            cacheName2schema.remove(cacheName);

            // Drop tables.
            Collection<H2TableDescriptor> rmvTbls = new HashSet<>();

            for (H2TableDescriptor tbl : schema.tables()) {
                if (F.eq(tbl.cache().name(), cacheName)) {
                    try {
                        tbl.table().setRemoveIndexOnDestroy(rmvIdx);

                        dropTable(tbl);
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to drop table on cache stop (will ignore): " + tbl.fullTableName(), e);
                    }

                    schema.drop(tbl);

                    rmvTbls.add(tbl);
                }
            }

            synchronized (schemaMux) {
                if (schema.decrementUsageCount()) {
                    schemas.remove(schemaName);

                    try {
                        dropSchema(schemaName);
                    }
                    catch (IgniteException e) {
                        U.error(log, "Failed to drop schema on cache stop (will ignore): " + cacheName, e);
                    }
                }
            }

            conns.values().forEach(H2ConnectionWrapper::clearStatementCache);

            for (H2TableDescriptor tbl : rmvTbls) {
                for (Index idx : tbl.table().getIndexes())
                    idx.close(null);
            }

            int cacheId = CU.cacheId(cacheName);

            for (Iterator<Map.Entry<H2TwoStepCachedQueryKey, H2TwoStepCachedQuery>> it =
                twoStepCache.entrySet().iterator(); it.hasNext();) {
                Map.Entry<H2TwoStepCachedQueryKey, H2TwoStepCachedQuery> e = it.next();

                GridCacheTwoStepQuery qry = e.getValue().query();

                if (!F.isEmpty(qry.cacheIds()) && qry.cacheIds().contains(cacheId))
                    it.remove();
            }
        }
    }

    /**
     * Remove all cached queries from cached two-steps queries.
     */
    private void clearCachedQueries() {
        twoStepCache = new GridBoundedConcurrentLinkedHashMap<>(TWO_STEP_QRY_CACHE_SIZE);
    }

    /** {@inheritDoc} */
    @Override public IndexingQueryFilter backupFilter(@Nullable final AffinityTopologyVersion topVer,
        @Nullable final int[] parts) {
        return new IndexingQueryFilterImpl(ctx, topVer, parts);
    }

    /**
     * @return Ready topology version.
     */
    public AffinityTopologyVersion readyTopologyVersion() {
        return ctx.cache().context().exchange().readyAffinityVersion();
    }

    /**
     * @param readyVer Ready topology version.
     *
     * @return {@code true} If pending distributed exchange exists because server topology is changed.
     */
    public boolean serverTopologyChanged(AffinityTopologyVersion readyVer) {
        GridDhtPartitionsExchangeFuture fut = ctx.cache().context().exchange().lastTopologyFuture();

        if (fut.isDone())
            return false;

        AffinityTopologyVersion initVer = fut.initialVersion();

        return initVer.compareTo(readyVer) > 0 && !fut.firstEvent().node().isClient();
    }

    /**
     * @param topVer Topology version.
     * @throws IgniteCheckedException If failed.
     */
    public void awaitForReadyTopologyVersion(AffinityTopologyVersion topVer) throws IgniteCheckedException {
        IgniteInternalFuture<?> fut = ctx.cache().context().exchange().affinityReadyFuture(topVer);

        if (fut != null)
            fut.get();
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {
        rdcQryExec.onDisconnected(reconnectFut);
    }

    /**
     * Bind query parameters and calculate partitions derived from the query.
     *
     * @param partInfoList Collection of query derived partition info.
     * @param params Query parameters.
     * @return Partitions.
     * @throws IgniteCheckedException, If fails.
     */
    private int[] calculateQueryPartitions(CacheQueryPartitionInfo[] partInfoList, Object[] params)
        throws IgniteCheckedException {

        ArrayList<Integer> list = new ArrayList<>(partInfoList.length);

        for (CacheQueryPartitionInfo partInfo: partInfoList) {
            int partId = (partInfo.partition() >= 0) ? partInfo.partition() :
                bindPartitionInfoParameter(partInfo, params);

            int i = 0;

            while (i < list.size() && list.get(i) < partId)
                i++;

            if (i < list.size()) {
                if (list.get(i) > partId)
                    list.add(i, partId);
            }
            else
                list.add(partId);
        }

        int[] result = new int[list.size()];

        for (int i = 0; i < list.size(); i++)
            result[i] = list.get(i);

        return result;
    }

    /**
     * Bind query parameter to partition info and calculate partition.
     *
     * @param partInfo Partition Info.
     * @param params Query parameters.
     * @return Partition.
     * @throws IgniteCheckedException, If fails.
     */
    private int bindPartitionInfoParameter(CacheQueryPartitionInfo partInfo, Object[] params)
        throws IgniteCheckedException {
        assert partInfo != null;
        assert partInfo.partition() < 0;

        GridH2RowDescriptor desc = dataTable(schema(partInfo.cacheName()), partInfo.tableName()).rowDescriptor();

        Object param = H2Utils.convert(params[partInfo.paramIdx()],
                desc, partInfo.dataType());

        return kernalContext().affinity().partition(partInfo.cacheName(), param);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRunningQueryInfo> runningQueries(long duration) {
        Collection<GridRunningQueryInfo> res = new ArrayList<>();

        res.addAll(runs.values());
        res.addAll(rdcQryExec.longRunningQueries(duration));

        return res;
    }

    /** {@inheritDoc} */
    @Override public void cancelQueries(Collection<Long> queries) {
        if (!F.isEmpty(queries)) {
            for (Long qryId : queries) {
                GridRunningQueryInfo run = runs.get(qryId);

                if (run != null)
                    run.cancel();
            }

            rdcQryExec.cancelQueries(queries);
        }
    }

    /** {@inheritDoc} */
    @Override public void cancelAllQueries() {
        mapQryExec.cancelLazyWorkers();

        for (H2ConnectionWrapper c : conns.values())
            U.close(c, log);
    }

    /**
     * @return Per-thread connections.
     */
    public Map<Thread, ?> perThreadConnections() {
        return conns;
    }

    /**
     * Collect cache identifiers from two-step query.
     *
     * @param mainCacheId Id of main cache.
     * @param twoStepQry Two-step query.
     * @return Result.
     */
    @Nullable public List<Integer> collectCacheIds(@Nullable Integer mainCacheId, GridCacheTwoStepQuery twoStepQry) {
        LinkedHashSet<Integer> caches0 = new LinkedHashSet<>();

        int tblCnt = twoStepQry.tablesCount();

        if (mainCacheId != null)
            caches0.add(mainCacheId);

        if (tblCnt > 0) {
            for (QueryTable tblKey : twoStepQry.tables()) {
                GridH2Table tbl = dataTable(tblKey);

                if (tbl != null) {
                    int cacheId = tbl.cacheId();

                    caches0.add(cacheId);
                }
            }
        }

        if (caches0.isEmpty())
            return null;
        else {
            //Prohibit usage indices with different numbers of segments in same query.
            List<Integer> cacheIds = new ArrayList<>(caches0);

            processCaches(cacheIds, twoStepQry);

            return cacheIds;
        }
    }

    /**
     * @return {@code True} is system views exist.
     */
    private boolean hasSystemViews(GridCacheTwoStepQuery twoStepQry) {
        if (twoStepQry.tablesCount() > 0) {
            for (QueryTable tbl : twoStepQry.tables()) {
                if (QueryUtils.SCHEMA_SYS.equals(tbl.schema()))
                    return true;
            }
        }

        return false;
    }

    /**
     * Closeable iterator.
     */
    private interface ClIter<X> extends AutoCloseable, Iterator<X> {
        // No-op.
    }
}
