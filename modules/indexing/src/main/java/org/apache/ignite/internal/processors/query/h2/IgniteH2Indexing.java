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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.jdbc2.JdbcSqlFieldsQuery;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMarshallable;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryCacheObjectsIterator;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResult;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResultAdapter;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.h2.ddl.DdlStatementsProcessor;
import org.apache.ignite.internal.processors.query.h2.opt.DistributedJoinMode;
import org.apache.ignite.internal.processors.query.h2.database.H2PkHashIndex;
import org.apache.ignite.internal.processors.query.h2.database.H2RowFactory;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2InnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2DefaultTableEngine;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2SystemIndexFactory;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOffheap;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowFactory;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ValueCacheObject;
import org.apache.ignite.internal.processors.query.h2.opt.GridLuceneIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuerySplitter;
import org.apache.ignite.internal.processors.query.h2.twostep.GridMapQueryExecutor;
import org.apache.ignite.internal.processors.query.h2.twostep.GridReduceQueryExecutor;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.GridEmptyCloseableIterator;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeGuard;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.api.ErrorCode;
import org.h2.api.JavaObjectSerializer;
import org.h2.api.TableEngine;
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.command.ddl.CreateTableData;
import org.h2.command.dml.Insert;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.jdbc.JdbcConnection;
import org.h2.jdbc.JdbcPreparedStatement;
import org.h2.jdbc.JdbcStatement;
import org.h2.message.DbException;
import org.h2.mvstore.cache.CacheLongKeyLIRS;
import org.h2.result.SearchRow;
import org.h2.result.SimpleRow;
import org.h2.result.SortOrder;
import org.h2.server.web.WebServer;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableBase;
import org.h2.tools.Server;
import org.h2.util.JdbcUtils;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueByte;
import org.h2.value.ValueBytes;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueGeometry;
import org.h2.value.ValueInt;
import org.h2.value.ValueJavaObject;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueShort;
import org.h2.value.ValueString;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueUuid;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

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
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2AbstractKeyValueRow.DEFAULT_COLUMNS_COUNT;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2AbstractKeyValueRow.KEY_COL;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2AbstractKeyValueRow.VAL_COL;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2AbstractKeyValueRow.VER_COL;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.LOCAL;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.PREPARE;

/**
 * Indexing implementation based on H2 database engine. In this implementation main query language is SQL,
 * fulltext indexing can be performed using Lucene. For each registered space
 * the SPI will create respective schema, for default space (where space name is null) schema
 * with name {@code ""} will be used. To avoid name conflicts user should not explicitly name
 * a schema {@code ""}.
 * <p>
 * For each registered {@link GridQueryTypeDescriptor} this SPI will create respective SQL table with
 * {@code '_key'} and {@code '_val'} fields for key and value, and fields from
 * {@link GridQueryTypeDescriptor#fields()}.
 * For each table it will create indexes declared in {@link GridQueryTypeDescriptor#indexes()}.
 */
@SuppressWarnings({"UnnecessaryFullyQualifiedName", "NonFinalStaticVariableUsedInClassInitialization"})
public class IgniteH2Indexing implements GridQueryIndexing {
    /*
     * Register IO for indexes.
     */
    static {
        PageIO.registerH2(H2InnerIO.VERSIONS, H2LeafIO.VERSIONS);
        H2ExtrasInnerIO.register();
        H2ExtrasLeafIO.register();
    }

    /** Spatial index class name. */
    private static final String SPATIAL_IDX_CLS =
        "org.apache.ignite.internal.processors.query.h2.opt.GridH2SpatialIndex";

    /** Default DB options. */
    private static final String DB_OPTIONS = ";LOCK_MODE=3;MULTI_THREADED=1;DB_CLOSE_ON_EXIT=FALSE" +
        ";DEFAULT_LOCK_TIMEOUT=10000;FUNCTIONS_IN_SCHEMA=true;OPTIMIZE_REUSE_RESULTS=0;QUERY_CACHE_SIZE=0" +
        ";RECOMPILE_ALWAYS=1;MAX_OPERATION_MEMORY=0;NESTED_JOINS=0;BATCH_JOINS=1" +
        ";ROW_FACTORY=\"" + GridH2RowFactory.class.getName() + "\"" +
        ";DEFAULT_TABLE_ENGINE=" + GridH2DefaultTableEngine.class.getName();

        // Uncomment this setting to get debug output from H2 to sysout.
//        ";TRACE_LEVEL_SYSTEM_OUT=3";

    /** Dummy metadata for update result. */
    public static final List<GridQueryFieldMetadata> UPDATE_RESULT_META = Collections.<GridQueryFieldMetadata>
        singletonList(new SqlFieldMetadata(null, null, "UPDATED", Long.class.getName()));

    /** */
    private static final int PREPARED_STMT_CACHE_SIZE = 256;

    /** */
    private static final int TWO_STEP_QRY_CACHE_SIZE = 1024;

    /** */
    private static final Field COMMAND_FIELD;

    /** */
    private static final char ESC_CH = '\"';

    /** */
    private static final String ESC_STR = ESC_CH + "" + ESC_CH;

    /** The period of clean up the {@link #stmtCache}. */
    private final Long CLEANUP_STMT_CACHE_PERIOD = Long.getLong(IGNITE_H2_INDEXING_CACHE_CLEANUP_PERIOD, 10_000);

    /** The timeout to remove entry from the {@link #stmtCache} if the thread doesn't perform any queries. */
    private final Long STATEMENT_CACHE_THREAD_USAGE_TIMEOUT =
        Long.getLong(IGNITE_H2_INDEXING_CACHE_THREAD_USAGE_TIMEOUT, 600 * 1000);

    /** */
    private GridTimeoutProcessor.CancelableTask stmtCacheCleanupTask;

    /*
     * Command in H2 prepared statement.
     */
    static {
        // Initialize system properties for H2.
        System.setProperty("h2.objectCache", "false");
        System.setProperty("h2.serializeJavaObject", "false");
        System.setProperty("h2.objectCacheMaxPerElementSize", "0"); // Avoid ValueJavaObject caching.

        try {
            COMMAND_FIELD = JdbcPreparedStatement.class.getDeclaredField("command");

            COMMAND_FIELD.setAccessible(true);
        }
        catch (NoSuchFieldException e) {
            throw new IllegalStateException("Check H2 version in classpath.", e);
        }
    }

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Node ID. */
    private UUID nodeId;

    /** */
    private Marshaller marshaller;

    /** Collection of schemaNames and registered tables. */
    private final ConcurrentMap<String, Schema> schemas = new ConcurrentHashMap8<>();

    /** */
    private String dbUrl = "jdbc:h2:mem:";

    /** */
    private final Collection<Connection> conns = Collections.synchronizedCollection(new ArrayList<Connection>());

    /** */
    private GridMapQueryExecutor mapQryExec;

    /** */
    private GridReduceQueryExecutor rdcQryExec;

    /** space name -> schema name */
    private final Map<String, String> space2schema = new ConcurrentHashMap8<>();

    /** */
    private AtomicLong qryIdGen;

    /** */
    private GridSpinBusyLock busyLock;

    /** */
    private final ConcurrentMap<Long, GridRunningQueryInfo> runs = new ConcurrentHashMap8<>();

    /** */
    private final ThreadLocal<ConnectionWrapper> connCache = new ThreadLocal<ConnectionWrapper>() {
        @Nullable @Override public ConnectionWrapper get() {
            ConnectionWrapper c = super.get();

            boolean reconnect = true;

            try {
                reconnect = c == null || c.connection().isClosed();
            }
            catch (SQLException e) {
                U.warn(log, "Failed to check connection status.", e);
            }

            if (reconnect) {
                c = initialValue();

                set(c);

                // Reset statement cache when new connection is created.
                stmtCache.remove(Thread.currentThread());
            }

            return c;
        }

        @Nullable @Override protected ConnectionWrapper initialValue() {
            Connection c;

            try {
                c = DriverManager.getConnection(dbUrl);
            }
            catch (SQLException e) {
                throw new IgniteSQLException("Failed to initialize DB connection: " + dbUrl, e);
            }

            conns.add(c);

            return new ConnectionWrapper(c);
        }
    };

    /** */
    protected volatile GridKernalContext ctx;

    /** */
    private DmlStatementsProcessor dmlProc;

    /** */
    private DdlStatementsProcessor ddlProc;

    /** */
    private final ConcurrentMap<QueryTable, GridH2Table> dataTables = new ConcurrentHashMap8<>();

    /** Statement cache. */
    private final ConcurrentHashMap<Thread, StatementCache> stmtCache = new ConcurrentHashMap<>();

    /** */
    private final GridBoundedConcurrentLinkedHashMap<TwoStepCachedQueryKey, TwoStepCachedQuery> twoStepCache =
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

    /**
     * @return Kernal context.
     */
    public GridKernalContext kernalContext() {
        return ctx;
    }

    /**
     * @param space Space.
     * @return Connection.
     */
    public Connection connectionForSpace(String space) {
        try {
            return connectionForThread(schema(space));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param c Connection.
     * @param sql SQL.
     * @param useStmtCache If {@code true} uses statement cache.
     * @return Prepared statement.
     * @throws SQLException If failed.
     */
    private PreparedStatement prepareStatement(Connection c, String sql, boolean useStmtCache) throws SQLException {
        if (useStmtCache) {
            Thread curThread = Thread.currentThread();

            StatementCache cache = stmtCache.get(curThread);

            if (cache == null) {
                StatementCache cache0 = new StatementCache(PREPARED_STMT_CACHE_SIZE);

                cache = stmtCache.putIfAbsent(curThread, cache0);

                if (cache == null)
                    cache = cache0;
            }

            cache.updateLastUsage();

            PreparedStatement stmt = cache.get(sql);

            if (stmt != null && !stmt.isClosed() && !((JdbcStatement)stmt).isCancelled()) {
                assert stmt.getConnection() == c;

                return stmt;
            }

            stmt = c.prepareStatement(sql);

            cache.put(sql, stmt);

            return stmt;
        }
        else
            return c.prepareStatement(sql);
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement prepareNativeStatement(String space, String sql) throws SQLException {
        return prepareStatement(connectionForSpace(space), sql, true);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteDataStreamer<?, ?> createStreamer(String spaceName, PreparedStatement nativeStmt,
        long autoFlushFreq, int nodeBufSize, int nodeParOps, boolean allowOverwrite) {
        Prepared prep = GridSqlQueryParser.prepared(nativeStmt);

        if (!(prep instanceof Insert))
            throw new IgniteSQLException("Only INSERT operations are supported in streaming mode",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        IgniteDataStreamer streamer = ctx.grid().dataStreamer(spaceName);

        streamer.autoFlushFrequency(autoFlushFreq);

        streamer.allowOverwrite(allowOverwrite);

        if (nodeBufSize > 0)
            streamer.perNodeBufferSize(nodeBufSize);

        if (nodeParOps > 0)
            streamer.perNodeParallelOperations(nodeParOps);

        return streamer;
    }

    /**
     * Gets DB connection.
     *
     * @param schema Whether to set schema for connection or not.
     * @return DB connection.
     * @throws IgniteCheckedException In case of error.
     */
    private Connection connectionForThread(@Nullable String schema) throws IgniteCheckedException {
        ConnectionWrapper c = connCache.get();

        if (c == null)
            throw new IgniteCheckedException("Failed to get DB connection for thread (check log for details).");

        if (schema != null && !F.eq(c.schema(), schema)) {
            Statement stmt = null;

            try {
                stmt = c.connection().createStatement();

                stmt.executeUpdate("SET SCHEMA " + schema);

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
     * Creates DB schema if it has not been created yet.
     *
     * @param schema Schema name.
     * @throws IgniteCheckedException If failed to create db schema.
     */
    private void createSchema(String schema) throws IgniteCheckedException {
        executeStatement("INFORMATION_SCHEMA", "CREATE SCHEMA IF NOT EXISTS " + schema);

        if (log.isDebugEnabled())
            log.debug("Created H2 schema for index database: " + schema);
    }

    /**
     * Creates DB schema if it has not been created yet.
     *
     * @param schema Schema name.
     * @throws IgniteCheckedException If failed to create db schema.
     */
    private void dropSchema(String schema) throws IgniteCheckedException {
        executeStatement("INFORMATION_SCHEMA", "DROP SCHEMA IF EXISTS " + schema);

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
        Connection conn = connCache.get().connection();

        connCache.set(null);

        if (conn != null) {
            conns.remove(conn);

            // Reset connection to receive new one at next call.
            U.close(conn, log);
        }
    }

    /** {@inheritDoc} */
    @Override public void store(String spaceName,
        String typeName,
        KeyCacheObject k,
        int partId,
        CacheObject v,
        GridCacheVersion ver,
        long expirationTime,
        long link) throws IgniteCheckedException {
        TableDescriptor tbl = tableDescriptor(typeName, spaceName);

        if (tbl == null)
            return; // Type was rejected.

        if (expirationTime == 0)
            expirationTime = Long.MAX_VALUE;

        tbl.tbl.update(k, partId, v, ver, expirationTime, false, link);

        if (tbl.luceneIdx != null)
            tbl.luceneIdx.store(k, v, ver, expirationTime);
    }

    /**
     * @param o Object.
     * @return {@code true} If it is a binary object.
     */
    private boolean isBinary(CacheObject o) {
        if (ctx == null)
            return false;

        return ctx.cacheObjects().isBinaryObject(o);
    }

    /**
     * @param space Space.
     * @return Cache object context.
     */
    private CacheObjectContext objectContext(String space) {
        if (ctx == null)
            return null;

        return ctx.cache().internalCache(space).context().cacheObjectContext();
    }

    /**
     * @param space Space.
     * @return Cache object context.
     */
    private GridCacheContext cacheContext(String space) {
        if (ctx == null)
            return null;

        return ctx.cache().internalCache(space).context();
    }

    /** {@inheritDoc} */
    @Override public void remove(String spaceName,
        GridQueryTypeDescriptor type,
        KeyCacheObject key,
        int partId,
        CacheObject val,
        GridCacheVersion ver) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Removing key from cache query index [locId=" + nodeId + ", key=" + key + ", val=" + val + ']');

        TableDescriptor tbl = tableDescriptor(type.name(), spaceName);

        if (tbl == null)
            return;

        if (tbl.tbl.update(key, partId, val, ver, 0, true, 0)) {
            if (tbl.luceneIdx != null)
                tbl.luceneIdx.remove(key);
        }
    }

    /**
     * Drops table form h2 database and clear all related indexes (h2 text, lucene).
     *
     * @param tbl Table to unregister.
     * @throws IgniteCheckedException If failed to unregister.
     */
    private void removeTable(TableDescriptor tbl) throws IgniteCheckedException {
        assert tbl != null;

        if (log.isDebugEnabled())
            log.debug("Removing query index table: " + tbl.fullTableName());

        Connection c = connectionForThread(tbl.schemaName());

        Statement stmt = null;

        try {
            // NOTE: there is no method dropIndex() for lucene engine correctly working.
            // So we have to drop all lucene index.
            // FullTextLucene.dropAll(c); TODO: GG-4015: fix this

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

        tbl.onDrop();

        tbl.schema.tbls.remove(tbl.typeName());
    }

    /**
     * Add initial user index.
     *
     * @param spaceName Space name.
     * @param desc Table descriptor.
     * @param h2Idx User index.
     * @throws IgniteCheckedException If failed.
     */
    private void addInitialUserIndex(String spaceName, TableDescriptor desc, GridH2IndexBase h2Idx)
        throws IgniteCheckedException {
        GridH2Table h2Tbl = desc.tbl;

        h2Tbl.proposeUserIndex(h2Idx);

        try {
            String sql = indexCreateSql(desc.fullTableName(), h2Idx, false, desc.schema.escapeAll());

            executeSql(spaceName, sql);
        }
        catch (Exception e) {
            // Rollback and re-throw.
            h2Tbl.rollbackUserIndex(h2Idx.getName());

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void dynamicIndexCreate(final String spaceName, final String tblName,
        final QueryIndexDescriptorImpl idxDesc, boolean ifNotExists, SchemaIndexCacheVisitor cacheVisitor)
        throws IgniteCheckedException {
        // Locate table.
        String schemaName = schema(spaceName);

        Schema schema = schemas.get(schemaName);

        TableDescriptor desc = (schema != null ? schema.tbls.get(tblName) : null);

        if (desc == null)
            throw new IgniteCheckedException("Table not found in internal H2 database [schemaName=" + schemaName +
                ", tblName=" + tblName + ']');

        GridH2Table h2Tbl = desc.tbl;

        // Create index.
        final GridH2IndexBase h2Idx = desc.createUserIndex(idxDesc);

        h2Tbl.proposeUserIndex(h2Idx);

        try {
            // Populate index with existing cache data.
            final GridH2RowDescriptor rowDesc = h2Tbl.rowDescriptor();

            SchemaIndexCacheVisitorClosure clo = new SchemaIndexCacheVisitorClosure() {
                @Override public void apply(KeyCacheObject key, int part, CacheObject val, GridCacheVersion ver,
                    long expTime, long link) throws IgniteCheckedException {
                    if (expTime == 0L)
                        expTime = Long.MAX_VALUE;

                    GridH2Row row = rowDesc.createRow(key, part, val, ver, expTime);

                    row.link(link);

                    h2Idx.put(row);
                }
            };

            cacheVisitor.visit(clo);

            // At this point index is in consistent state, promote it through H2 SQL statement, so that cached
            // prepared statements are re-built.
            String sql = indexCreateSql(desc.fullTableName(), h2Idx, ifNotExists, schema.escapeAll());

            executeSql(spaceName, sql);
        }
        catch (Exception e) {
            // Rollback and re-throw.
            h2Tbl.rollbackUserIndex(h2Idx.getName());

            throw e;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    @Override public void dynamicIndexDrop(final String spaceName, String idxName, boolean ifExists)
        throws IgniteCheckedException{
        String schemaName = schema(spaceName);

        Schema schema = schemas.get(schemaName);

        String sql = indexDropSql(schemaName, idxName, ifExists, schema.escapeAll());

        executeSql(spaceName, sql);
    }

    /**
     * Execute DDL command.
     *
     * @param spaceName Space name.
     * @param sql SQL.
     * @throws IgniteCheckedException If failed.
     */
    private void executeSql(String spaceName, String sql) throws IgniteCheckedException {
        try {
            Connection conn = connectionForSpace(spaceName);

            try (PreparedStatement stmt = prepareStatement(conn, sql, false)) {
                stmt.execute();
            }
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to execute SQL statement on internal H2 database: " + sql, e);
        }
    }

    /**
     * Generate {@code CREATE INDEX} SQL statement for given params.
     * @param fullTblName Fully qualified table name.
     * @param h2Idx H2 index.
     * @param ifNotExists Quietly skip index creation if it exists.
     * @return Statement string.
     */
    private static String indexCreateSql(String fullTblName, GridH2IndexBase h2Idx, boolean ifNotExists,
        boolean escapeAll) {
        boolean spatial = F.eq(SPATIAL_IDX_CLS, h2Idx.getClass().getName());

        GridStringBuilder sb = new SB("CREATE ")
            .a(spatial ? "SPATIAL " : "")
            .a("INDEX ")
            .a(ifNotExists ? "IF NOT EXISTS " : "")
            .a(escapeName(h2Idx.getName(), escapeAll))
            .a(" ON ")
            .a(fullTblName)
            .a(" (");

        boolean first = true;

        for (IndexColumn col : h2Idx.getIndexColumns()) {
            if (first)
                first = false;
            else
                sb.a(", ");

            sb.a("\"" + col.columnName + "\"").a(" ").a(col.sortType == SortOrder.ASCENDING ? "ASC" : "DESC");
        }

        sb.a(')');

        return sb.toString();
    }

    /**
     * Generate {@code CREATE INDEX} SQL statement for given params.
     * @param schemaName <b>Quoted</b> schema name.
     * @param idxName Index name.
     * @param ifExists Quietly skip index drop if it exists.
     * @param escapeAll Escape flag.
     * @return Statement string.
     */
    private static String indexDropSql(String schemaName, String idxName, boolean ifExists, boolean escapeAll) {
        return "DROP INDEX " + (ifExists ? "IF EXISTS " : "") + schemaName + '.' + escapeName(idxName, escapeAll);
    }

    /**
     * Create sorted index.
     *
     * @param schema Schema.
     * @param name Index name,
     * @param tbl Table.
     * @param pk Primary key flag.
     * @param cols Columns.
     * @return Index.
     */
    private GridH2IndexBase createSortedIndex(Schema schema, String name, GridH2Table tbl, boolean pk,
        List<IndexColumn> cols, int inlineSize) {
        try {
            GridCacheContext cctx = schema.cacheContext();

            if (log.isDebugEnabled())
                log.debug("Creating cache index [cacheId=" + cctx.cacheId() + ", idxName=" + name + ']');

            final int segments = tbl.rowDescriptor().configuration().getQueryParallelism();

            return new H2TreeIndex(cctx, tbl, name, pk, cols, inlineSize, segments);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Create spatial index.
     *
     * @param tbl Table.
     * @param idxName Index name.
     * @param cols Columns.
     */
    private GridH2IndexBase createSpatialIndex(GridH2Table tbl, String idxName, IndexColumn[] cols
    ) {
        try {
            Class<?> cls = Class.forName(SPATIAL_IDX_CLS);

            Constructor<?> ctor = cls.getConstructor(
                GridH2Table.class,
                String.class,
                Integer.TYPE,
                IndexColumn[].class);

            if (!ctor.isAccessible())
                ctor.setAccessible(true);

            final int segments = tbl.rowDescriptor().configuration().getQueryParallelism();

            return (GridH2IndexBase)ctor.newInstance(tbl, idxName, segments, cols);
        }
        catch (Exception e) {
            throw new IgniteException("Failed to instantiate: " + SPATIAL_IDX_CLS, e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocalText(
        String spaceName, String qry, String typeName,
        IndexingQueryFilter filters) throws IgniteCheckedException {
        TableDescriptor tbl = tableDescriptor(typeName, spaceName);

        if (tbl != null && tbl.luceneIdx != null) {
            GridRunningQueryInfo run = new GridRunningQueryInfo(qryIdGen.incrementAndGet(), qry, TEXT, spaceName,
                U.currentTimeMillis(), null, true);

            try {
                runs.put(run.id(), run);

                return tbl.luceneIdx.query(qry, filters);
            }
            finally {
                runs.remove(run.id());
            }
        }

        return new GridEmptyCloseableIterator<>();
    }

    /** {@inheritDoc} */
    @Override public void unregisterType(String spaceName, String typeName)
        throws IgniteCheckedException {
        TableDescriptor tbl = tableDescriptor(typeName, spaceName);

        if (tbl != null)
            removeTable(tbl);
    }

    /**
     * Queries individual fields (generally used by JDBC drivers).
     *
     * @param spaceName Space name.
     * @param qry Query.
     * @param params Query parameters.
     * @param filter Space name and key filter.
     * @param enforceJoinOrder Enforce join order of tables in the query.
     * @param timeout Query timeout in milliseconds.
     * @param cancel Query cancel.
     * @return Query result.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public GridQueryFieldsResult queryLocalSqlFields(final String spaceName, final String qry,
        @Nullable final Collection<Object> params, final IndexingQueryFilter filter, boolean enforceJoinOrder,
        final int timeout, final GridQueryCancel cancel)
        throws IgniteCheckedException {
        final Connection conn = connectionForSpace(spaceName);

        setupConnection(conn, false, enforceJoinOrder);

        final PreparedStatement stmt = preparedStatementWithParams(conn, qry, params, true);

        Prepared p = GridSqlQueryParser.prepared(stmt);

        if (DmlStatementsProcessor.isDmlStatement(p)) {
            SqlFieldsQuery fldsQry = new SqlFieldsQuery(qry);

            if (params != null)
                fldsQry.setArgs(params.toArray());

            fldsQry.setEnforceJoinOrder(enforceJoinOrder);
            fldsQry.setTimeout(timeout, TimeUnit.MILLISECONDS);

            return dmlProc.updateLocalSqlFields(spaceName, stmt, fldsQry, filter, cancel);
        }
        else if (DdlStatementsProcessor.isDdlStatement(p))
            throw new IgniteSQLException("DDL statements are supported for the whole cluster only",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        List<GridQueryFieldMetadata> meta;

        try {
            meta = meta(stmt.getMetaData());
        }
        catch (SQLException e) {
            throw new IgniteCheckedException("Cannot prepare query metadata", e);
        }

        final GridH2QueryContext ctx = new GridH2QueryContext(nodeId, nodeId, 0, LOCAL)
            .filter(filter).distributedJoinMode(OFF);

        return new GridQueryFieldsResultAdapter(meta, null) {
            @Override public GridCloseableIterator<List<?>> iterator() throws IgniteCheckedException {
                assert GridH2QueryContext.get() == null;

                GridH2QueryContext.set(ctx);

                GridRunningQueryInfo run = new GridRunningQueryInfo(qryIdGen.incrementAndGet(), qry, SQL_FIELDS,
                    spaceName, U.currentTimeMillis(), cancel, true);

                runs.putIfAbsent(run.id(), run);

                try {
                    ResultSet rs = executeSqlQueryWithTimer(spaceName, stmt, conn, qry, params, timeout, cancel);

                    return new FieldsIterator(rs);
                }
                finally {
                    GridH2QueryContext.clearThreadLocal();

                    runs.remove(run.id());
                }
            }
        };
    }

    /** {@inheritDoc} */
    @Override public long streamUpdateQuery(String spaceName, String qry,
        @Nullable Object[] params, IgniteDataStreamer<?, ?> streamer) throws IgniteCheckedException {
        final Connection conn = connectionForSpace(spaceName);

        final PreparedStatement stmt;

        try {
            stmt = prepareStatement(conn, qry, true);
        }
        catch (SQLException e) {
            throw new IgniteSQLException(e);
        }

        return dmlProc.streamUpdateQuery(streamer, stmt, params);
    }

    /**
     * @param rsMeta Metadata.
     * @return List of fields metadata.
     * @throws SQLException If failed.
     */
    private static List<GridQueryFieldMetadata> meta(ResultSetMetaData rsMeta) throws SQLException {
        List<GridQueryFieldMetadata> meta = new ArrayList<>(rsMeta.getColumnCount());

        for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
            String schemaName = rsMeta.getSchemaName(i);
            String typeName = rsMeta.getTableName(i);
            String name = rsMeta.getColumnLabel(i);
            String type = rsMeta.getColumnClassName(i);

            if (type == null) // Expression always returns NULL.
                type = Void.class.getName();

            meta.add(new SqlFieldMetadata(schemaName, typeName, name, type));
        }

        return meta;
    }

    /**
     * @param stmt Prepared statement.
     * @return Command type.
     */
    private static int commandType(PreparedStatement stmt) {
        try {
            return ((CommandInterface)COMMAND_FIELD.get(stmt)).getCommandType();
        }
        catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Stores rule for constructing schemaName according to cache configuration.
     *
     * @param ccfg Cache configuration.
     * @return Proper schema name according to ANSI-99 standard.
     */
    private static String schemaNameFromCacheConf(CacheConfiguration<?, ?> ccfg) {
        if (ccfg.getSqlSchema() == null)
            return escapeName(ccfg.getName(), true);

        if (ccfg.getSqlSchema().charAt(0) == ESC_CH)
            return ccfg.getSqlSchema();

        return ccfg.isSqlEscapeAll() ? escapeName(ccfg.getSqlSchema(), true) : ccfg.getSqlSchema().toUpperCase();
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
    private PreparedStatement preparedStatementWithParams(Connection conn, String sql, Collection<Object> params,
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
     * @param cancel Query cancel.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    private ResultSet executeSqlQuery(final Connection conn, final PreparedStatement stmt,
        int timeoutMillis, @Nullable GridQueryCancel cancel)
        throws IgniteCheckedException {

        if (cancel != null) {
            cancel.set(new Runnable() {
                @Override public void run() {
                    try {
                        stmt.cancel();
                    }
                    catch (SQLException ignored) {
                        // No-op.
                    }
                }
            });
        }

        if (timeoutMillis > 0)
            session(conn).setQueryTimeout(timeoutMillis);

        try {
            return stmt.executeQuery();
        }
        catch (SQLException e) {
            // Throw special exception.
            if (e.getErrorCode() == ErrorCode.STATEMENT_WAS_CANCELED)
                throw new QueryCancelledException();

            throw new IgniteCheckedException("Failed to execute SQL query.", e);
        }
        finally {
            if (timeoutMillis > 0)
                session(conn).setQueryTimeout(0);
        }
    }

    /**
     * Executes sql query and prints warning if query is too slow..
     *
     * @param space Space name.
     * @param conn Connection,.
     * @param sql Sql query.
     * @param params Parameters.
     * @param useStmtCache If {@code true} uses stmt cache.
     * @param cancel Query cancel.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    public ResultSet executeSqlQueryWithTimer(String space,
        Connection conn,
        String sql,
        @Nullable Collection<Object> params,
        boolean useStmtCache,
        int timeoutMillis,
        @Nullable GridQueryCancel cancel) throws IgniteCheckedException {
        return executeSqlQueryWithTimer(space, preparedStatementWithParams(conn, sql, params, useStmtCache),
            conn, sql, params, timeoutMillis, cancel);
    }

    /**
     * Executes sql query and prints warning if query is too slow.
     *
     * @param space Space name.
     * @param stmt Prepared statement for query.
     * @param conn Connection.
     * @param sql Sql query.
     * @param params Parameters.
     * @param cancel Query cancel.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    private ResultSet executeSqlQueryWithTimer(String space, PreparedStatement stmt,
        Connection conn,
        String sql,
        @Nullable Collection<Object> params,
        int timeoutMillis,
        @Nullable GridQueryCancel cancel) throws IgniteCheckedException {
        long start = U.currentTimeMillis();

        try {
            ResultSet rs = executeSqlQuery(conn, stmt, timeoutMillis, cancel);

            long time = U.currentTimeMillis() - start;

            long longQryExecTimeout = schemas.get(schema(space)).ccfg.getLongQueryWarningTimeout();

            if (time > longQryExecTimeout) {
                String msg = "Query execution is too long (" + time + " ms): " + sql;

                ResultSet plan = executeSqlQuery(conn, preparedStatementWithParams(conn, "EXPLAIN " + sql,
                    params, false), 0, null);

                plan.next();

                // Add SQL explain result message into log.
                String longMsg = "Query execution is too long [time=" + time + " ms, sql='" + sql + '\'' +
                    ", plan=" + U.nl() + plan.getString(1) + U.nl() + ", parameters=" + params + "]";

                LT.warn(log, longMsg, msg);
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

    /**
     * @param conn Connection to use.
     * @param distributedJoins If distributed joins are enabled.
     * @param enforceJoinOrder Enforce join order of tables.
     */
    public static void setupConnection(Connection conn, boolean distributedJoins, boolean enforceJoinOrder) {
        Session s = session(conn);

        s.setForceJoinOrder(enforceJoinOrder);
        s.setJoinBatchEnabled(distributedJoins);
    }

    /** {@inheritDoc} */
    @Override public QueryCursor<List<?>> queryLocalSqlFields(final GridCacheContext<?, ?> cctx,
        final SqlFieldsQuery qry, final IndexingQueryFilter filter, final GridQueryCancel cancel)
        throws IgniteCheckedException {

        if (cctx.config().getQueryParallelism() > 1) {
            qry.setDistributedJoins(true);

            assert qry.isLocal();

            return queryDistributedSqlFields(cctx, qry, cancel);
        }
        else {
            final boolean keepBinary = cctx.keepBinary();

            final String space = cctx.name();
            final String sql = qry.getSql();
            final Object[] args = qry.getArgs();

            final GridQueryFieldsResult res = queryLocalSqlFields(space, sql, F.asList(args), filter,
                qry.isEnforceJoinOrder(), qry.getTimeout(), cancel);

            QueryCursorImpl<List<?>> cursor = new QueryCursorImpl<>(new Iterable<List<?>>() {
                @Override public Iterator<List<?>> iterator() {
                    try {
                        return new GridQueryCacheObjectsIterator(res.iterator(), cctx, keepBinary);
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
            }, cancel);

            cursor.fieldsMeta(res.metaData());

            return cursor;
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> QueryCursor<Cache.Entry<K,V>> queryLocalSql(final GridCacheContext<?, ?> cctx,
        final SqlQuery qry, final IndexingQueryFilter filter, final boolean keepBinary) throws IgniteCheckedException {
        if (cctx.config().getQueryParallelism() > 1) {
            qry.setDistributedJoins(true);

            assert qry.isLocal();

            return queryDistributedSql(cctx, qry);
        }
        else {
            String space = cctx.name();
            String type = qry.getType();
            String sqlQry = qry.getSql();
            String alias = qry.getAlias();
            Object[] params = qry.getArgs();

            GridQueryCancel cancel = new GridQueryCancel();

            final GridCloseableIterator<IgniteBiTuple<K, V>> i = queryLocalSql(space, sqlQry, alias,
                F.asList(params), type, filter, cancel);

            return new QueryCursorImpl<Cache.Entry<K, V>>(new Iterable<Cache.Entry<K, V>>() {
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

                            return new CacheEntryImpl<>(
                                (K)cctx.unwrapBinaryIfNeeded(t.get1(), keepBinary, false),
                                (V)cctx.unwrapBinaryIfNeeded(t.get2(), keepBinary, false));
                        }

                        @Override public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
            }, cancel);
        }
    }

    /**
     * Executes regular query.
     *
     * @param spaceName Space name.
     * @param qry Query.
     * @param alias Table alias.
     * @param params Query parameters.
     * @param type Query return type.
     * @param filter Space name and key filter.
     * @return Queried rows.
     * @throws IgniteCheckedException If failed.
     */
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocalSql(String spaceName,
        final String qry, String alias, @Nullable final Collection<Object> params, String type,
        final IndexingQueryFilter filter, GridQueryCancel cancel) throws IgniteCheckedException {
        final TableDescriptor tbl = tableDescriptor(type, spaceName);

        if (tbl == null)
            throw new IgniteSQLException("Failed to find SQL table for type: " + type,
                IgniteQueryErrorCode.TABLE_NOT_FOUND);

        String sql = generateQuery(qry, alias, tbl);

        Connection conn = connectionForThread(tbl.schemaName());

        setupConnection(conn, false, false);

        GridH2QueryContext.set(new GridH2QueryContext(nodeId, nodeId, 0, LOCAL).filter(filter)
            .distributedJoinMode(OFF));

        GridRunningQueryInfo run = new GridRunningQueryInfo(qryIdGen.incrementAndGet(), qry, SQL, spaceName,
            U.currentTimeMillis(), null, true);

        runs.put(run.id(), run);

        try {
            ResultSet rs = executeSqlQueryWithTimer(spaceName, conn, sql, params, true, 0, cancel);

            return new KeyValIterator(rs);
        }
        finally {
            GridH2QueryContext.clearThreadLocal();

            runs.remove(run.id());
        }
    }

    /**
     * @param cctx Cache context.
     * @param qry Query.
     * @param keepCacheObj Flag to keep cache object.
     * @param enforceJoinOrder Enforce join order of tables.
     * @param parts Partitions.
     * @return Iterable result.
     */
    private Iterable<List<?>> runQueryTwoStep(
        final GridCacheContext<?,?> cctx,
        final GridCacheTwoStepQuery qry,
        final boolean keepCacheObj,
        final boolean enforceJoinOrder,
        final int timeoutMillis,
        final GridQueryCancel cancel,
        final Object[] params,
        final int[] parts
    ) {
        return new Iterable<List<?>>() {
            @Override public Iterator<List<?>> iterator() {
                return rdcQryExec.query(cctx, qry, keepCacheObj, enforceJoinOrder, timeoutMillis, cancel, params, parts);
            }
        };
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> QueryCursor<Cache.Entry<K, V>> queryDistributedSql(GridCacheContext<?, ?> cctx, SqlQuery qry) {
        String type = qry.getType();
        String space = cctx.name();

        TableDescriptor tblDesc = tableDescriptor(type, space);

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

        final QueryCursor<List<?>> res = queryDistributedSqlFields(cctx, fqry, null);

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
     * @param c Connection.
     * @return Session.
     */
    public static Session session(Connection c) {
        return (Session)((JdbcConnection)c).getSession();
    }

    /** {@inheritDoc} */
    @Override public QueryCursor<List<?>> queryDistributedSqlFields(GridCacheContext<?, ?> cctx, SqlFieldsQuery qry,
        GridQueryCancel cancel) {
        final String space = cctx.name();
        final String sqlQry = qry.getSql();

        Connection c = connectionForSpace(space);

        final boolean enforceJoinOrder = qry.isEnforceJoinOrder();
        final boolean distributedJoins = qry.isDistributedJoins();
        final boolean grpByCollocated = qry.isCollocated();

        final DistributedJoinMode distributedJoinMode = distributedJoinMode(qry.isLocal(), distributedJoins);

        GridCacheTwoStepQuery twoStepQry = null;
        List<GridQueryFieldMetadata> meta;

        final TwoStepCachedQueryKey cachedQryKey = new TwoStepCachedQueryKey(space, sqlQry, grpByCollocated,
            distributedJoins, enforceJoinOrder, qry.isLocal());
        TwoStepCachedQuery cachedQry = twoStepCache.get(cachedQryKey);

        if (cachedQry != null) {
            twoStepQry = cachedQry.twoStepQry.copy();
            meta = cachedQry.meta;
        }
        else {
            final UUID locNodeId = ctx.localNodeId();

            // Here we will just parse the statement, no need to optimize it at all.
            setupConnection(c, /*distributedJoins*/false, /*enforceJoinOrder*/true);

            GridH2QueryContext.set(new GridH2QueryContext(locNodeId, locNodeId, 0, PREPARE)
                .distributedJoinMode(distributedJoinMode));

            PreparedStatement stmt = null;
            Prepared prepared;

            boolean cachesCreated = false;

            try {
                try {
                    while (true) {
                        try {
                            // Do not cache this statement because the whole two step query object will be cached later on.
                            stmt = prepareStatement(c, sqlQry, false);

                            break;
                        }
                        catch (SQLException e) {
                            if (!cachesCreated && e.getErrorCode() == ErrorCode.SCHEMA_NOT_FOUND_1) {
                                try {
                                    ctx.cache().createMissingQueryCaches();
                                }
                                catch (IgniteCheckedException ignored) {
                                    throw new CacheException("Failed to create missing caches.", e);
                                }

                                cachesCreated = true;
                            }
                            else
                                throw new IgniteSQLException("Failed to parse query: " + sqlQry,
                                    IgniteQueryErrorCode.PARSING, e);
                        }
                    }


                    prepared = GridSqlQueryParser.prepared(stmt);

                    if (qry instanceof JdbcSqlFieldsQuery && ((JdbcSqlFieldsQuery) qry).isQuery() != prepared.isQuery())
                        throw new IgniteSQLException("Given statement type does not match that declared by JDBC driver",
                            IgniteQueryErrorCode.STMT_TYPE_MISMATCH);

                    if (prepared.isQuery()) {
                        bindParameters(stmt, F.asList(qry.getArgs()));

                        twoStepQry = GridSqlQuerySplitter.split((JdbcPreparedStatement)stmt, qry.getArgs(), grpByCollocated,
                            distributedJoins, enforceJoinOrder, this);

                        assert twoStepQry != null;
                    }
                }
                finally {
                    GridH2QueryContext.clearThreadLocal();
                }

                // It is a DML statement if we did not create a twoStepQuery.
                if (twoStepQry == null) {
                    if (DmlStatementsProcessor.isDmlStatement(prepared)) {
                        try {
                            return dmlProc.updateSqlFieldsTwoStep(cctx.name(), stmt, qry, cancel);
                        }
                        catch (IgniteCheckedException e) {
                            throw new IgniteSQLException("Failed to execute DML statement [stmt=" + sqlQry +
                                ", params=" + Arrays.deepToString(qry.getArgs()) + "]", e);
                        }
                    }

                    if (DdlStatementsProcessor.isDdlStatement(prepared)) {
                        try {
                            return ddlProc.runDdlStatement(sqlQry, stmt);
                        }
                        catch (IgniteCheckedException e) {
                            throw new IgniteSQLException("Failed to execute DDL statement [stmt=" + sqlQry + ']', e);
                        }
                    }
                }

                LinkedHashSet<Integer> caches0 = new LinkedHashSet<>();

                // Setup spaces from schemas.
                assert twoStepQry != null;

                int tblCnt = twoStepQry.tablesCount();

                if (tblCnt > 0) {
                    caches0.add(cctx.cacheId());

                    for (QueryTable table : twoStepQry.tables()) {
                        String cacheName = cacheNameForSchemaAndTable(table.schema(), table.table());

                        int cacheId = CU.cacheId(cacheName);

                        caches0.add(cacheId);
                    }
                }
                else
                    caches0.add(cctx.cacheId());

                //Prohibit usage indices with different numbers of segments in same query.
                List<Integer> cacheIds = new ArrayList<>(caches0);

                checkCacheIndexSegmentation(cacheIds);

                twoStepQry.cacheIds(cacheIds);
                twoStepQry.local(qry.isLocal());

                meta = meta(stmt.getMetaData());
            }
            catch (IgniteCheckedException e) {
                throw new CacheException("Failed to bind parameters: [qry=" + sqlQry + ", params=" +
                    Arrays.deepToString(qry.getArgs()) + "]", e);
            }
            catch (SQLException e) {
                throw new IgniteSQLException(e);
            }
            finally {
                U.close(stmt, log);
            }
        }

        if (log.isDebugEnabled())
            log.debug("Parsed query: `" + sqlQry + "` into two step query: " + twoStepQry);

        twoStepQry.pageSize(qry.getPageSize());

        if (cancel == null)
            cancel = new GridQueryCancel();

        QueryCursorImpl<List<?>> cursor = new QueryCursorImpl<>(
            runQueryTwoStep(cctx, twoStepQry, cctx.keepBinary(), enforceJoinOrder, qry.getTimeout(), cancel,
                    qry.getArgs(), qry.getPartitions()),
            cancel);

        cursor.fieldsMeta(meta);

        if (cachedQry == null && !twoStepQry.explain()) {
            cachedQry = new TwoStepCachedQuery(meta, twoStepQry.copy());
            twoStepCache.putIfAbsent(cachedQryKey, cachedQry);
        }

        return cursor;
    }

    /**
     * Get cache for schema and table.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @return Cache name.
     */
    private String cacheNameForSchemaAndTable(String schemaName, String tblName) {
        // TODO: This need to be changed.
        return space(schemaName);
    }

    /**
     * @throws IllegalStateException if segmented indices used with non-segmented indices.
     */
    private void checkCacheIndexSegmentation(List<Integer> caches) {
        if (caches.isEmpty())
            return; // Nothing to check

        GridCacheSharedContext sharedCtx = ctx.cache().context();

        int expectedParallelism = 0;

        for (int i = 0; i < caches.size(); i++) {
            GridCacheContext cctx = sharedCtx.cacheContext(caches.get(i));

            assert cctx != null;

            if (!cctx.isPartitioned())
                continue;

            if (expectedParallelism == 0)
                expectedParallelism = cctx.config().getQueryParallelism();
            else if (cctx.config().getQueryParallelism() != expectedParallelism)
                throw new IllegalStateException("Using indexes with different parallelism levels in same query is forbidden.");
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
    private String generateQuery(String qry, String tableAlias, TableDescriptor tbl) throws IgniteCheckedException {
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
    @Override public boolean registerType(String spaceName, GridQueryTypeDescriptor type)
        throws IgniteCheckedException {
        validateTypeDescriptor(type);

        String schemaName = schema(spaceName);

        Schema schema = schemas.get(schemaName);

        TableDescriptor tbl = new TableDescriptor(schema, type);

        try {
            Connection conn = connectionForThread(schemaName);

            createTable(spaceName, schema, tbl, conn);

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

        if (type.keyFieldName() != null && !type.fields().containsKey(type.keyFieldName())) {
            throw new IgniteCheckedException(
                    MessageFormat.format("Name ''{0}'' must be amongst fields since it is configured as ''keyFieldName'' [type=" +
                            type.name() + "]", type.keyFieldName()));
        }

        if (type.valueFieldName() != null && !type.fields().containsKey(type.valueFieldName())) {
            throw new IgniteCheckedException(
                    MessageFormat.format("Name ''{0}'' must be amongst fields since it is configured as ''valueFieldName'' [type=" +
                            type.name() + "]", type.valueFieldName()));
        }
    }

    /**
     * Returns empty string, if {@code nullableString} is empty.
     *
     * @param nullableString String for convertion. Could be null.
     * @return Non null string. Could be empty.
     */
    private static String emptyIfNull(String nullableString) {
        return nullableString == null ? "" : nullableString;
    }

    /**
     * Escapes name to be valid SQL identifier. Currently just replaces '.' and '$' sign with '_'.
     *
     * @param name Name.
     * @param escapeAll Escape flag.
     * @return Escaped name.
     */
    public static String escapeName(String name, boolean escapeAll) {
        if (name == null) // It is possible only for a cache name.
            return ESC_STR;

        if (escapeAll)
            return ESC_CH + name + ESC_CH;

        SB sb = null;

        for (int i = 0; i < name.length(); i++) {
            char ch = name.charAt(i);

            if (!Character.isLetter(ch) && !Character.isDigit(ch) && ch != '_' &&
                !(ch == '"' && (i == 0 || i == name.length() - 1)) && ch != '-') {
                // Class name can also contain '$' or '.' - these should be escaped.
                assert ch == '$' || ch == '.';

                if (sb == null)
                    sb = new SB();

                sb.a(name.substring(sb.length(), i));

                // Replace illegal chars with '_'.
                sb.a('_');
            }
        }

        if (sb == null)
            return name;

        sb.a(name.substring(sb.length(), name.length()));

        return sb.toString();
    }

    /**
     * Create db table by using given table descriptor.
     *
     * @param spaceName Space name.
     * @param schema Schema.
     * @param tbl Table descriptor.
     * @param conn Connection.
     * @throws SQLException If failed to create db table.
     * @throws IgniteCheckedException If failed.
     */
    private void createTable(String spaceName, Schema schema, TableDescriptor tbl, Connection conn)
        throws SQLException, IgniteCheckedException {
        assert schema != null;
        assert tbl != null;

        boolean escapeAll = schema.escapeAll();

        String keyType = dbTypeFromClass(tbl.type().keyClass());
        String valTypeStr = dbTypeFromClass(tbl.type().valueClass());

        SB sql = new SB();

        String keyValVisibility = tbl.type().fields().isEmpty() ? " VISIBLE" : " INVISIBLE";

        sql.a("CREATE TABLE ").a(tbl.fullTableName()).a(" (")
            .a(KEY_FIELD_NAME).a(' ').a(keyType).a(keyValVisibility).a(" NOT NULL");

        sql.a(',').a(VAL_FIELD_NAME).a(' ').a(valTypeStr).a(keyValVisibility);
        sql.a(',').a(VER_FIELD_NAME).a(" OTHER INVISIBLE");

        for (Map.Entry<String, Class<?>> e : tbl.type().fields().entrySet())
            sql.a(',').a(escapeName(e.getKey(), escapeAll)).a(' ').a(dbTypeFromClass(e.getValue()));

        sql.a(')');

        if (log.isDebugEnabled())
            log.debug("Creating DB table with SQL: " + sql);

        GridH2RowDescriptor rowDesc = new RowDescriptor(tbl.type(), schema);

        H2RowFactory rowFactory = tbl.rowFactory(rowDesc);

        GridH2Table h2Tbl = H2TableEngine.createTable(conn, sql.toString(), rowDesc, rowFactory, tbl);

        for (GridH2IndexBase usrIdx : tbl.createUserIndexes())
            addInitialUserIndex(spaceName, tbl, usrIdx);

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
     * Gets corresponding DB type from java class.
     *
     * @param cls Java class.
     * @return DB type name.
     */
    private String dbTypeFromClass(Class<?> cls) {
        return DBTypeEnum.fromClass(cls).dBTypeAsString();
    }

    /**
     * Gets table descriptor by type and space names.
     *
     * @param type Type name.
     * @param space Space name.
     * @return Table descriptor.
     */
    @Nullable private TableDescriptor tableDescriptor(String type, String space) {
        Schema s = schemas.get(schema(space));

        if (s == null)
            return null;

        return s.tbls.get(type);
    }

    /**
     * Gets collection of table for given schema name.
     *
     * @param schema Schema name.
     * @return Collection of table descriptors.
     */
    private Collection<TableDescriptor> tables(String schema) {
        Schema s = schemas.get(schema);

        if (s == null)
            return Collections.emptySet();

        return s.tbls.values();
    }

    /**
     * Gets database schema from space.
     *
     * @param space Space name. {@code null} would be converted to an empty string.
     * @return Schema name. Should not be null since we should not fail for an invalid space name.
     */
    private String schema(String space) {
        return emptyIfNull(space2schema.get(emptyIfNull(space)));
    }

    /**
     * Called periodically by {@link GridTimeoutProcessor} to clean up the {@link #stmtCache}.
     */
    private void cleanupStatementCache() {
        long cur = U.currentTimeMillis();

        for (Iterator<Map.Entry<Thread, StatementCache>> it = stmtCache.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Thread, StatementCache> entry = it.next();

            Thread t = entry.getKey();

            if (t.getState() == Thread.State.TERMINATED
                || cur - entry.getValue().lastUsage() > STATEMENT_CACHE_THREAD_USAGE_TIMEOUT)
                it.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public String space(String schemaName) {
        assert schemaName != null;

        Schema schema = schemas.get(schemaName);

        // For the compatibility with conversion from """" to "" inside h2 lib
        if (schema == null) {
            assert schemaName.isEmpty() || schemaName.charAt(0) != ESC_CH;

            schema = schemas.get(escapeName(schemaName, true));
        }

        return schema.spaceName;
    }

    /**
     * Rebuild indexes from hash index.
     *
     * @param spaceName Space name.
     * @param type Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    @Override public void rebuildIndexesFromHash(String spaceName,
        GridQueryTypeDescriptor type) throws IgniteCheckedException {
        TableDescriptor tbl = tableDescriptor(type.name(), spaceName);

        if (tbl == null)
            return;

        assert tbl.tbl != null;

        assert tbl.tbl.rebuildFromHashInProgress();

        H2PkHashIndex hashIdx = tbl.pkHashIdx;

        Cursor cursor = hashIdx.find((Session)null, null, null);

        int cacheId = CU.cacheId(tbl.schema.ccfg.getName());

        GridCacheContext cctx = ctx.cache().context().cacheContext(cacheId);

        while (cursor.next()) {
            CacheDataRow dataRow = (CacheDataRow)cursor.get();

            boolean done = false;

            while (!done) {
                GridCacheEntryEx entry = cctx.cache().entryEx(dataRow.key());

                try {
                    synchronized (entry) {
                        // TODO : How to correctly get current value and link here?

                        GridH2Row row = tbl.tbl.rowDescriptor().createRow(entry.key(), entry.partition(),
                            dataRow.value(), entry.version(), entry.expireTime());

                        row.link(dataRow.link());

                        List<Index> indexes = tbl.tbl.getAllIndexes();

                        for (int i = 2; i < indexes.size(); i++) {
                            Index idx = indexes.get(i);

                            if (idx instanceof H2TreeIndex)
                                ((H2TreeIndex)idx).put(row);
                        }

                        done = true;
                    }
                }
                catch (GridCacheEntryRemovedException e) {
                    // No-op
                }
            }

        }

        tbl.tbl.markRebuildFromHashInProgress(false);
    }

    /** {@inheritDoc} */
    @Override public void markForRebuildFromHash(String spaceName, GridQueryTypeDescriptor type) {
        TableDescriptor tbl = tableDescriptor(type.name(), spaceName);

        if (tbl == null)
            return;

        assert tbl.tbl != null;

        tbl.tbl.markRebuildFromHashInProgress(true);
    }

    /**
     * Gets size (for tests only).
     *
     * @param spaceName Space name.
     * @param typeName Type name.
     * @return Size.
     * @throws IgniteCheckedException If failed or {@code -1} if the type is unknown.
     */
    long size(String spaceName, String typeName) throws IgniteCheckedException {
        TableDescriptor tbl = tableDescriptor(typeName, spaceName);

        if (tbl == null)
            return -1;

        Connection conn = connectionForSpace(spaceName);

        setupConnection(conn, false, false);

        try {
            ResultSet rs = executeSqlQuery(conn, prepareStatement(conn, "SELECT COUNT(*) FROM " + tbl.fullTableName(), false),
                0, null);

            if (!rs.next())
                throw new IllegalStateException();

            return rs.getLong(1);
        }
        catch (SQLException e) {
            throw new IgniteCheckedException(e);
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
    @SuppressWarnings("NonThreadSafeLazyInitialization")
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
        }

        if (JdbcUtils.serializer != null)
            U.warn(log, "Custom H2 serialization is already configured, will override.");

        JdbcUtils.serializer = h2Serializer();

        // TODO https://issues.apache.org/jira/browse/IGNITE-2139
        // registerMBean(igniteInstanceName, this, GridH2IndexingSpiMBean.class);
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
    @Override public void stop() throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Stopping cache query index...");

//        unregisterMBean(); TODO https://issues.apache.org/jira/browse/IGNITE-2139
        if (ctx != null && !ctx.cache().context().database().persistenceEnabled()) {
            for (Schema schema : schemas.values())
                schema.onDrop();
        }

        for (Connection c : conns)
            U.close(c, log);

        conns.clear();
        schemas.clear();
        space2schema.clear();

        try (Connection c = DriverManager.getConnection(dbUrl);
             Statement s = c.createStatement()) {
            s.execute("SHUTDOWN");
        }
        catch (SQLException e) {
            U.error(log, "Failed to shutdown database.", e);
        }

        if (stmtCacheCleanupTask != null)
            stmtCacheCleanupTask.close();

        GridH2QueryContext.clearLocalNodeStop(nodeId);

        if (log.isDebugEnabled())
            log.debug("Cache query index stopped.");
    }

    /** {@inheritDoc} */
    @Override public void registerCache(String spaceName, GridCacheContext<?, ?> cctx, CacheConfiguration<?, ?> ccfg)
        throws IgniteCheckedException {
        String schema = schemaNameFromCacheConf(ccfg);

        if (schemas.putIfAbsent(schema, new Schema(spaceName, schema, cctx, ccfg)) != null)
            throw new IgniteCheckedException("Cache already registered: " + U.maskName(spaceName));

        space2schema.put(emptyIfNull(spaceName), schema);

        createSchema(schema);

        createSqlFunctions(schema, ccfg.getSqlFunctionClasses());
    }

    /** {@inheritDoc} */
    @Override public void unregisterCache(String spaceName) {
        String schema = schema(spaceName);
        Schema rmv = schemas.remove(schema);

        if (rmv != null) {
            space2schema.remove(emptyIfNull(rmv.spaceName));
            mapQryExec.onCacheStop(spaceName);
            dmlProc.onCacheStop(spaceName);

            rmv.onDrop();

            try {
                dropSchema(schema);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to drop schema on cache stop (will ignore): " + U.maskName(spaceName), e);
            }

            for (TableDescriptor tblDesc : rmv.tbls.values())
                for (Index idx : tblDesc.tbl.getIndexes())
                    idx.close(null);

            for (Iterator<Map.Entry<TwoStepCachedQueryKey, TwoStepCachedQuery>> it = twoStepCache.entrySet().iterator();
                it.hasNext(); ) {
                Map.Entry<TwoStepCachedQueryKey, TwoStepCachedQuery> e = it.next();

                if (F.eq(e.getKey().space, spaceName))
                    it.remove();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public IndexingQueryFilter backupFilter(
        @Nullable final AffinityTopologyVersion topVer,
        @Nullable final int[] parts
    ) {
        final AffinityTopologyVersion topVer0 = topVer != null ? topVer : AffinityTopologyVersion.NONE;

        return new IndexingQueryFilter() {
            @Nullable @Override public <K, V> IgniteBiPredicate<K, V> forSpace(String spaceName) {
                final GridCacheAdapter<Object, Object> cache = ctx.cache().internalCache(spaceName);

                if (cache.context().isReplicated())
                    return null;

                final GridCacheAffinityManager aff = cache.context().affinity();

                if (parts != null) {
                    if (parts.length < 64) { // Fast scan for small arrays.
                        return new IgniteBiPredicate<K, V>() {
                            @Override public boolean apply(K k, V v) {
                                int p = aff.partition(k);

                                for (int p0 : parts) {
                                    if (p0 == p)
                                        return true;

                                    if (p0 > p) // Array is sorted.
                                        return false;
                                }

                                return false;
                            }
                        };
                    }

                    return new IgniteBiPredicate<K, V>() {
                        @Override public boolean apply(K k, V v) {
                            int p = aff.partition(k);

                            return Arrays.binarySearch(parts, p) >= 0;
                        }
                    };
                }

                final ClusterNode locNode = ctx.discovery().localNode();

                return new IgniteBiPredicate<K, V>() {
                    @Override public boolean apply(K k, V v) {
                        return aff.primaryByKey(locNode, k, topVer0);
                    }
                };
            }

            @Override public boolean isValueRequired() {
                return false;
            }

            @Override public String toString() {
                return "IndexingQueryFilter [ver=" + topVer + ']';
            }
        };
    }

    /**
     * @return Ready topology version.
     */
    public AffinityTopologyVersion readyTopologyVersion() {
        return ctx.cache().context().exchange().readyAffinityVersion();
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
     * Key for cached two-step query.
     */
    private static final class TwoStepCachedQueryKey {
        /** */
        private final String space;

        /** */
        private final String sql;

        /** */
        private final boolean grpByCollocated;

        /** */
        private final boolean distributedJoins;

        /** */
        private final boolean enforceJoinOrder;

        /** */
        private final boolean isLocal;

        /**
         * @param space Space.
         * @param sql Sql.
         * @param grpByCollocated Collocated GROUP BY.
         * @param distributedJoins Distributed joins enabled.
         * @param enforceJoinOrder Enforce join order of tables.
         * @param isLocal Query is local flag.
         */
        private TwoStepCachedQueryKey(String space,
            String sql,
            boolean grpByCollocated,
            boolean distributedJoins,
            boolean enforceJoinOrder,
            boolean isLocal) {
            this.space = space;
            this.sql = sql;
            this.grpByCollocated = grpByCollocated;
            this.distributedJoins = distributedJoins;
            this.enforceJoinOrder = enforceJoinOrder;
            this.isLocal = isLocal;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TwoStepCachedQueryKey that = (TwoStepCachedQueryKey)o;

            if (grpByCollocated != that.grpByCollocated)
                return false;

            if (distributedJoins != that.distributedJoins)
                return false;

            if (enforceJoinOrder != that.enforceJoinOrder)
                return false;

            if (space != null ? !space.equals(that.space) : that.space != null)
                return false;

            return isLocal == that.isLocal && sql.equals(that.sql);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = space != null ? space.hashCode() : 0;
            res = 31 * res + sql.hashCode();
            res = 31 * res + (grpByCollocated ? 1 : 0);
            res = res + (distributedJoins ? 2 : 0);
            res = res + (enforceJoinOrder ? 4 : 0);
            res = res + (isLocal ? 8 : 0);

            return res;
        }
    }

    /**
     * Cached two-step query.
     */
    private static final class TwoStepCachedQuery {
        /** */
        final List<GridQueryFieldMetadata> meta;

        /** */
        final GridCacheTwoStepQuery twoStepQry;

        /**
         * @param meta Fields metadata.
         * @param twoStepQry Query.
         */
        public TwoStepCachedQuery(List<GridQueryFieldMetadata> meta, GridCacheTwoStepQuery twoStepQry) {
            this.meta = meta;
            this.twoStepQry = twoStepQry;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TwoStepCachedQuery.class, this);
        }
    }

    /**
     * @param c1 First column.
     * @param c2 Second column.
     * @return {@code true} If they are the same.
     */
    private static boolean equal(IndexColumn c1, IndexColumn c2) {
        return c1.column.getColumnId() == c2.column.getColumnId();
    }

    /**
     * @param cols Columns list.
     * @param col Column to find.
     * @return {@code true} If found.
     */
    private static boolean containsColumn(List<IndexColumn> cols, IndexColumn col) {
        for (int i = cols.size() - 1; i >= 0; i--) {
            if (equal(cols.get(i), col))
                return true;
        }

        return false;
    }

    /**
     * Check whether columns list contains key or key alias column.
     *
     * @param desc Row descriptor.
     * @param cols Columns list.
     * @return Result.
     */
    private static boolean containsKeyColumn(GridH2RowDescriptor desc, List<IndexColumn> cols) {
        for (int i = cols.size() - 1; i >= 0; i--) {
            if (desc.isKeyColumn(cols.get(i).column.getColumnId()))
                return true;
        }

        return false;
    }

    /**
     * @param desc Row descriptor.
     * @param cols Columns list.
     * @param keyCol Primary key column.
     * @param affCol Affinity key column.
     * @return The same list back.
     */
    private static List<IndexColumn> treeIndexColumns(GridH2RowDescriptor desc, List<IndexColumn> cols, IndexColumn keyCol, IndexColumn affCol) {
        assert keyCol != null;

        if (!containsKeyColumn(desc, cols))
            cols.add(keyCol);

        if (affCol != null && !containsColumn(cols, affCol))
            cols.add(affCol);

        return cols;
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
        for (Connection conn : conns)
            U.close(conn, log);
    }

    /**
     * Wrapper to store connection and flag is schema set or not.
     */
    private static class ConnectionWrapper {
        /** */
        private Connection conn;

        /** */
        private volatile String schema;

        /**
         * @param conn Connection to use.
         */
        ConnectionWrapper(Connection conn) {
            this.conn = conn;
        }

        /**
         * @return Schema name if schema is set, null otherwise.
         */
        public String schema() {
            return schema;
        }

        /**
         * @param schema Schema name set on this connection.
         */
        public void schema(@Nullable String schema) {
            this.schema = schema;
        }

        /**
         * @return Connection.
         */
        public Connection connection() {
            return conn;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ConnectionWrapper.class, this);
        }
    }

    /**
     * Enum that helps to map java types to database types.
     */
    private enum DBTypeEnum {
        /** */
        INT("INT"),

        /** */
        BOOL("BOOL"),

        /** */
        TINYINT("TINYINT"),

        /** */
        SMALLINT("SMALLINT"),

        /** */
        BIGINT("BIGINT"),

        /** */
        DECIMAL("DECIMAL"),

        /** */
        DOUBLE("DOUBLE"),

        /** */
        REAL("REAL"),

        /** */
        TIME("TIME"),

        /** */
        TIMESTAMP("TIMESTAMP"),

        /** */
        DATE("DATE"),

        /** */
        VARCHAR("VARCHAR"),

        /** */
        CHAR("CHAR"),

        /** */
        BINARY("BINARY"),

        /** */
        UUID("UUID"),

        /** */
        ARRAY("ARRAY"),

        /** */
        GEOMETRY("GEOMETRY"),

        /** */
        OTHER("OTHER");

        /** Map of Class to enum. */
        private static final Map<Class<?>, DBTypeEnum> map = new HashMap<>();

        /**
         * Initialize map of DB types.
         */
        static {
            map.put(int.class, INT);
            map.put(Integer.class, INT);
            map.put(boolean.class, BOOL);
            map.put(Boolean.class, BOOL);
            map.put(byte.class, TINYINT);
            map.put(Byte.class, TINYINT);
            map.put(short.class, SMALLINT);
            map.put(Short.class, SMALLINT);
            map.put(long.class, BIGINT);
            map.put(Long.class, BIGINT);
            map.put(BigDecimal.class, DECIMAL);
            map.put(double.class, DOUBLE);
            map.put(Double.class, DOUBLE);
            map.put(float.class, REAL);
            map.put(Float.class, REAL);
            map.put(Time.class, TIME);
            map.put(Timestamp.class, TIMESTAMP);
            map.put(java.util.Date.class, TIMESTAMP);
            map.put(java.sql.Date.class, DATE);
            map.put(String.class, VARCHAR);
            map.put(UUID.class, UUID);
            map.put(byte[].class, BINARY);
        }

        /** */
        private final String dbType;

        /**
         * Constructs new instance.
         *
         * @param dbType DB type name.
         */
        DBTypeEnum(String dbType) {
            this.dbType = dbType;
        }

        /**
         * Resolves enum by class.
         *
         * @param cls Class.
         * @return Enum value.
         */
        public static DBTypeEnum fromClass(Class<?> cls) {
            DBTypeEnum res = map.get(cls);

            if (res != null)
                return res;

            if (DataType.isGeometryClass(cls))
                return GEOMETRY;

            return cls.isArray() && !cls.getComponentType().isPrimitive() ? ARRAY : OTHER;
        }

        /**
         * Gets DB type name.
         *
         * @return DB type name.
         */
        public String dBTypeAsString() {
            return dbType;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DBTypeEnum.class, this);
        }
    }

    /**
     * Information about table in database.
     */
    private class TableDescriptor implements GridH2SystemIndexFactory {
        /** */
        private final String fullTblName;

        /** */
        private final GridQueryTypeDescriptor type;

        /** */
        private final Schema schema;

        /** */
        private GridH2Table tbl;

        /** */
        private GridLuceneIndex luceneIdx;

        /** */
        private H2PkHashIndex pkHashIdx;

        /**
         * @param schema Schema.
         * @param type Type descriptor.
         */
        TableDescriptor(Schema schema, GridQueryTypeDescriptor type) {
            this.type = type;
            this.schema = schema;

            String tblName = escapeName(type.tableName(), schema.escapeAll());

            fullTblName = schema.schemaName + "." + tblName;
        }

        /**
         * @return Schema name.
         */
        public String schemaName() {
            return schema.schemaName;
        }

        /**
         * @return Database full table name.
         */
        String fullTableName() {
            return fullTblName;
        }

        /**
         * @return type name.
         */
        String typeName() {
            return type.name();
        }

        /**
         * @return Type.
         */
        GridQueryTypeDescriptor type() {
            return type;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TableDescriptor.class, this);
        }

        /**
         * Create H2 row factory.
         *
         * @param rowDesc Row descriptor.
         * @return H2 row factory.
         */
        H2RowFactory rowFactory(GridH2RowDescriptor rowDesc) {
            GridCacheContext cctx = schema.cacheContext();

            if (cctx.affinityNode() && cctx.offheapIndex())
                return new H2RowFactory(rowDesc, cctx);

            return null;
        }

        /** {@inheritDoc} */
        @Override public ArrayList<Index> createSystemIndexes(GridH2Table tbl) {
            ArrayList<Index> idxs = new ArrayList<>();

            IndexColumn keyCol = tbl.indexColumn(KEY_COL, SortOrder.ASCENDING);
            IndexColumn affCol = tbl.getAffinityKeyColumn();

            if (affCol != null && equal(affCol, keyCol))
                affCol = null;

            GridH2RowDescriptor desc = tbl.rowDescriptor();

            Index hashIdx = createHashIndex(
                schema,
                tbl,
                "_key_PK_hash",
                treeIndexColumns(desc, new ArrayList<IndexColumn>(2), keyCol, affCol)
            );

            if (hashIdx != null)
                idxs.add(hashIdx);

            // Add primary key index.
            Index pkIdx = createSortedIndex(
                schema,
                "_key_PK",
                tbl,
                true,
                treeIndexColumns(desc, new ArrayList<IndexColumn>(2), keyCol, affCol),
                -1
            );

            idxs.add(pkIdx);

            if (type().valueClass() == String.class) {
                try {
                    luceneIdx = new GridLuceneIndex(ctx, schema.offheap, schema.spaceName, type);
                }
                catch (IgniteCheckedException e1) {
                    throw new IgniteException(e1);
                }
            }

            boolean affIdxFound = false;

            GridQueryIndexDescriptor textIdx = type.textIndex();

            if (textIdx != null) {
                try {
                    luceneIdx = new GridLuceneIndex(ctx, schema.offheap, schema.spaceName, type);
                }
                catch (IgniteCheckedException e1) {
                    throw new IgniteException(e1);
                }
            }

            // Locate index where affinity column is first (if any).
            if (affCol != null) {
                for (GridQueryIndexDescriptor idxDesc : type.indexes().values()) {
                    if (idxDesc.type() != QueryIndexType.SORTED)
                        continue;

                    String firstField = idxDesc.fields().iterator().next();

                    String firstFieldName =
                        schema.escapeAll() ? firstField : escapeName(firstField, false).toUpperCase();

                    Column col = tbl.getColumn(firstFieldName);

                    IndexColumn idxCol = tbl.indexColumn(col.getColumnId(),
                        idxDesc.descending(firstField) ? SortOrder.DESCENDING : SortOrder.ASCENDING);

                    affIdxFound |= equal(idxCol, affCol);
                }
            }

            // Add explicit affinity key index if nothing alike was found.
            if (affCol != null && !affIdxFound) {
                idxs.add(createSortedIndex(schema, "AFFINITY_KEY", tbl, false,
                    treeIndexColumns(desc, new ArrayList<IndexColumn>(2), affCol, keyCol), -1));
            }

            return idxs;
        }

        /**
         * Get collection of user indexes.
         *
         * @return User indexes.
         */
        public Collection<GridH2IndexBase> createUserIndexes() {
            assert tbl != null;

            ArrayList<GridH2IndexBase> res = new ArrayList<>();

            for (GridQueryIndexDescriptor idxDesc : type.indexes().values()) {
                GridH2IndexBase idx = createUserIndex(idxDesc);

                res.add(idx);
            }

            return res;
        }

        /**
         * Create user index.
         *
         * @param idxDesc Index descriptor.
         * @return Index.
         */
        private GridH2IndexBase createUserIndex(GridQueryIndexDescriptor idxDesc) {
            String name = schema.escapeAll() ? idxDesc.name() : escapeName(idxDesc.name(), false).toUpperCase();

            IndexColumn keyCol = tbl.indexColumn(KEY_COL, SortOrder.ASCENDING);
            IndexColumn affCol = tbl.getAffinityKeyColumn();

            List<IndexColumn> cols = new ArrayList<>(idxDesc.fields().size() + 2);

            boolean escapeAll = schema.escapeAll();

            for (String field : idxDesc.fields()) {
                String fieldName = escapeAll ? field : escapeName(field, false).toUpperCase();

                Column col = tbl.getColumn(fieldName);

                cols.add(tbl.indexColumn(col.getColumnId(),
                    idxDesc.descending(field) ? SortOrder.DESCENDING : SortOrder.ASCENDING));
            }

            GridH2RowDescriptor desc = tbl.rowDescriptor();
            if (idxDesc.type() == QueryIndexType.SORTED) {
                cols = treeIndexColumns(desc, cols, keyCol, affCol);
                return createSortedIndex(schema, name, tbl, false, cols, idxDesc.inlineSize());
            }
            else if (idxDesc.type() == QueryIndexType.GEOSPATIAL) {
                return createSpatialIndex(tbl, name, cols.toArray(new IndexColumn[cols.size()]));
            }

            throw new IllegalStateException("Index type: " + idxDesc.type());
        }

        /**
         * Create hash index.
         *
         * @param schema Schema.
         * @param tbl Table.
         * @param idxName Index name.
         * @param cols Columns.
         * @return Index.
         */
        private Index createHashIndex(Schema schema, GridH2Table tbl, String idxName, List<IndexColumn> cols) {
            GridCacheContext cctx = schema.cacheContext();

            if (cctx.affinityNode() && cctx.offheapIndex()) {
                assert pkHashIdx == null : pkHashIdx;

                pkHashIdx = new H2PkHashIndex(cctx, tbl, idxName, cols);

                return pkHashIdx;
            }

            return null;
        }

        /**
         *
         */
        void onDrop() {
            dataTables.remove(tbl.identifier(), tbl);

            tbl.destroy();

            U.closeQuiet(luceneIdx);
        }
    }

    /**
     * Special field set iterator based on database result set.
     */
    public static class FieldsIterator extends GridH2ResultSetIterator<List<?>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param data Data.
         * @throws IgniteCheckedException If failed.
         */
        public FieldsIterator(ResultSet data) throws IgniteCheckedException {
            super(data, false, true);
        }

        /** {@inheritDoc} */
        @Override protected List<?> createRow() {
            ArrayList<Object> res = new ArrayList<>(row.length);

            Collections.addAll(res, row);

            return res;
        }
    }

    /**
     * Special key/value iterator based on database result set.
     */
    private static class KeyValIterator<K, V> extends GridH2ResultSetIterator<IgniteBiTuple<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param data Data array.
         * @throws IgniteCheckedException If failed.
         */
        protected KeyValIterator(ResultSet data) throws IgniteCheckedException {
            super(data, false, true);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override protected IgniteBiTuple<K, V> createRow() {
            K key = (K)row[0];
            V val = (V)row[1];

            return new IgniteBiTuple<>(key, val);
        }
    }

    /**
     * Closeable iterator.
     */
    private interface ClIter<X> extends AutoCloseable, Iterator<X> {
        // No-op.
    }

    /**
     * Field descriptor.
     */
    static class SqlFieldMetadata implements GridQueryFieldMetadata {
        /** */
        private static final long serialVersionUID = 0L;

        /** Schema name. */
        private String schemaName;

        /** Type name. */
        private String typeName;

        /** Name. */
        private String name;

        /** Type. */
        private String type;

        /**
         * Required by {@link Externalizable}.
         */
        public SqlFieldMetadata() {
            // No-op
        }

        /**
         * @param schemaName Schema name.
         * @param typeName Type name.
         * @param name Name.
         * @param type Type.
         */
        SqlFieldMetadata(@Nullable String schemaName, @Nullable String typeName, String name, String type) {
            assert name != null && type != null : schemaName + " | " + typeName + " | " + name + " | " + type;

            this.schemaName = schemaName;
            this.typeName = typeName;
            this.name = name;
            this.type = type;
        }

        /** {@inheritDoc} */
        @Override public String schemaName() {
            return schemaName;
        }

        /** {@inheritDoc} */
        @Override public String typeName() {
            return typeName;
        }

        /** {@inheritDoc} */
        @Override public String fieldName() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public String fieldTypeName() {
            return type;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, schemaName);
            U.writeString(out, typeName);
            U.writeString(out, name);
            U.writeString(out, type);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            schemaName = U.readString(in);
            typeName = U.readString(in);
            name = U.readString(in);
            type = U.readString(in);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SqlFieldMetadata.class, this);
        }
    }

    /**
     * Database schema object.
     */
    private class Schema {
        /** */
        private final String spaceName;

        /** */
        private final String schemaName;

        /** */
        private final GridUnsafeMemory offheap = null;

        /** */
        private final ConcurrentMap<String, TableDescriptor> tbls = new ConcurrentHashMap8<>();

        /** Cache for deserialized offheap rows. */
        private final CacheLongKeyLIRS<GridH2Row> rowCache;

        /** */
        private final GridCacheContext<?, ?> cctx;

        /** */
        private final CacheConfiguration<?, ?> ccfg;

        /**
         * @param spaceName Space name.
         * @param schemaName Schema name.
         * @param cctx Cache context.
         * @param ccfg Cache configuration.
         */
        private Schema(String spaceName, String schemaName, GridCacheContext<?, ?> cctx,
            CacheConfiguration<?, ?> ccfg) {
            this.spaceName = spaceName;
            this.cctx = cctx;
            this.schemaName = schemaName;
            this.ccfg = ccfg;

            rowCache = null;
        }

        /**
         * @return Cache context.
         */
        public GridCacheContext cacheContext() {
            return cctx;
        }

        /**
         * @param tbl Table descriptor.
         */
        public void add(TableDescriptor tbl) {
            if (tbls.putIfAbsent(tbl.typeName(), tbl) != null)
                throw new IllegalStateException("Table already registered: " + tbl.fullTableName());
        }

        /**
         * @return Escape all.
         */
        public boolean escapeAll() {
            return ccfg.isSqlEscapeAll();
        }

        /**
         * Called after the schema was dropped.
         */
        public void onDrop() {
            for (TableDescriptor tblDesc : tbls.values())
                tblDesc.onDrop();
        }
    }

    /**
     * Row descriptor.
     */
    private class RowDescriptor implements GridH2RowDescriptor {
        /** */
        private final GridQueryTypeDescriptor type;

        /** */
        private final String[] fields;

        /** */
        private final int[] fieldTypes;

        /** */
        private final int keyType;

        /** */
        private final int valType;

        /** */
        private final Schema schema;

        /** */
        private final GridUnsafeGuard guard;

        /** */
        private final boolean snapshotableIdx;

        /** */
        private final GridQueryProperty[] props;

        /** Id of user-defined key column */
        private final int keyAliasColumnId;

        /** Id of user-defined value column */
        private final int valueAliasColumnId;

        /**
         * @param type Type descriptor.
         * @param schema Schema.
         */
        RowDescriptor(GridQueryTypeDescriptor type, Schema schema) {
            assert type != null;
            assert schema != null;

            this.type = type;
            this.schema = schema;

            guard = schema.offheap == null ? null : new GridUnsafeGuard();

            Map<String, Class<?>> allFields = new LinkedHashMap<>();

            allFields.putAll(type.fields());

            fields = allFields.keySet().toArray(new String[allFields.size()]);

            fieldTypes = new int[fields.length];

            Class[] classes = allFields.values().toArray(new Class[fields.length]);

            for (int i = 0; i < fieldTypes.length; i++)
                fieldTypes[i] = DataType.getTypeFromClass(classes[i]);

            keyType = DataType.getTypeFromClass(type.keyClass());
            valType = DataType.getTypeFromClass(type.valueClass());

            props = new GridQueryProperty[fields.length];

            for (int i = 0; i < fields.length; i++) {
                GridQueryProperty p = type.property(fields[i]);

                assert p != null : fields[i];

                props[i] = p;
            }

            final List<String> fieldsList = Arrays.asList(fields);
            keyAliasColumnId = (type.keyFieldName() != null) ? DEFAULT_COLUMNS_COUNT + fieldsList.indexOf(type.keyFieldName()) : -1;
            valueAliasColumnId = (type.valueFieldName() != null) ? DEFAULT_COLUMNS_COUNT + fieldsList.indexOf(type.valueFieldName()) : -1;

            // Index is not snapshotable in db-x.
            snapshotableIdx = false;
        }

        /** {@inheritDoc} */
        @Override public IgniteH2Indexing indexing() {
            return IgniteH2Indexing.this;
        }

        /** {@inheritDoc} */
        @Override public GridQueryTypeDescriptor type() {
            return type;
        }

        /** {@inheritDoc} */
        @Override public GridCacheContext<?, ?> context() {
            return schema.cacheContext();
        }

        /** {@inheritDoc} */
        @Override public CacheConfiguration configuration() {
            return schema.ccfg;
        }

        /** {@inheritDoc} */
        @Override public GridUnsafeGuard guard() {
            return guard;
        }

        /** {@inheritDoc} */
        @Override public void cache(GridH2Row row) {
            long ptr = row.pointer();

            assert ptr > 0 : ptr;

            schema.rowCache.put(ptr, row);
        }

        /** {@inheritDoc} */
        @Override public void uncache(long ptr) {
            schema.rowCache.remove(ptr);
        }

        /** {@inheritDoc} */
        @Override public GridUnsafeMemory memory() {
            return schema.offheap;
        }

        /** {@inheritDoc} */
        @Override public Value wrap(Object obj, int type) throws IgniteCheckedException {
            assert obj != null;

            if (obj instanceof CacheObject) { // Handle cache object.
                CacheObject co = (CacheObject)obj;

                if (type == Value.JAVA_OBJECT)
                    return new GridH2ValueCacheObject(cacheContext(schema.spaceName), co);

                obj = co.value(objectContext(schema.spaceName), false);
            }

            switch (type) {
                case Value.BOOLEAN:
                    return ValueBoolean.get((Boolean)obj);
                case Value.BYTE:
                    return ValueByte.get((Byte)obj);
                case Value.SHORT:
                    return ValueShort.get((Short)obj);
                case Value.INT:
                    return ValueInt.get((Integer)obj);
                case Value.FLOAT:
                    return ValueFloat.get((Float)obj);
                case Value.LONG:
                    return ValueLong.get((Long)obj);
                case Value.DOUBLE:
                    return ValueDouble.get((Double)obj);
                case Value.UUID:
                    UUID uuid = (UUID)obj;
                    return ValueUuid.get(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
                case Value.DATE:
                    return ValueDate.get((Date)obj);
                case Value.TIME:
                    return ValueTime.get((Time)obj);
                case Value.TIMESTAMP:
                    if (obj instanceof java.util.Date && !(obj instanceof Timestamp))
                        obj = new Timestamp(((java.util.Date)obj).getTime());

                    return ValueTimestamp.get((Timestamp)obj);
                case Value.DECIMAL:
                    return ValueDecimal.get((BigDecimal)obj);
                case Value.STRING:
                    return ValueString.get(obj.toString());
                case Value.BYTES:
                    return ValueBytes.get((byte[])obj);
                case Value.JAVA_OBJECT:
                    return ValueJavaObject.getNoCopy(obj, null, null);
                case Value.ARRAY:
                    Object[] arr = (Object[])obj;

                    Value[] valArr = new Value[arr.length];

                    for (int i = 0; i < arr.length; i++) {
                        Object o = arr[i];

                        valArr[i] = o == null ? ValueNull.INSTANCE : wrap(o, DataType.getTypeFromClass(o.getClass()));
                    }

                    return ValueArray.get(valArr);

                case Value.GEOMETRY:
                    return ValueGeometry.getFromGeometry(obj);
            }

            throw new IgniteCheckedException("Failed to wrap value[type=" + type + ", value=" + obj + "]");
        }

        /** {@inheritDoc} */
        @Override public GridH2Row createRow(KeyCacheObject key, int partId, @Nullable CacheObject val,
            GridCacheVersion ver,
            long expirationTime) throws IgniteCheckedException {
            GridH2Row row;

            try {
                if (val == null) // Only can happen for remove operation, can create simple search row.
                    row = GridH2RowFactory.create(wrap(key, keyType));
                else
                    row = schema.offheap == null ?
                        new GridH2KeyValueRowOnheap(this, key, keyType, val, valType, ver, expirationTime) :
                        new GridH2KeyValueRowOffheap(this, key, keyType, val, valType, ver, expirationTime);
            }
            catch (ClassCastException e) {
                throw new IgniteCheckedException("Failed to convert key to SQL type. " +
                    "Please make sure that you always store each value type with the same key type " +
                    "or configure key type as common super class for all actual keys for this value type.", e);
            }

            GridCacheContext cctx = cacheContext(schema.spaceName);

            if (cctx.offheapIndex()) {
                row.ver = ver;

                row.key = key;
                row.val = val;
                row.partId = partId;
            }

            return row;
        }

        /** {@inheritDoc} */
        @Override public int valueType() {
            return valType;
        }

        /** {@inheritDoc} */
        @Override public int fieldsCount() {
            return fields.length;
        }

        /** {@inheritDoc} */
        @Override public int fieldType(int col) {
            return fieldTypes[col];
        }

        /** {@inheritDoc} */
        @Override public Object columnValue(Object key, Object val, int col) {
            try {
                return props[col].value(key, val);
            }
            catch (IgniteCheckedException e) {
                throw DbException.convert(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void setColumnValue(Object key, Object val, Object colVal, int col) {
            try {
                props[col].setValue(key, val, colVal);
            }
            catch (IgniteCheckedException e) {
                throw DbException.convert(e);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean isColumnKeyProperty(int col) {
            return props[col].key();
        }

        /** {@inheritDoc} */
        @Override public GridH2KeyValueRowOffheap createPointer(long ptr) {
            GridH2KeyValueRowOffheap row = (GridH2KeyValueRowOffheap)schema.rowCache.get(ptr);

            if (row != null) {
                assert row.pointer() == ptr : ptr + " " + row.pointer();

                return row;
            }

            return new GridH2KeyValueRowOffheap(this, ptr);
        }

        /** {@inheritDoc} */
        @Override public GridH2Row cachedRow(long link) {
            return schema.rowCache.get(link);
        }

        /** {@inheritDoc} */
        @Override public boolean snapshotableIndex() {
            return snapshotableIdx;
        }

        /** {@inheritDoc} */
        @Override public boolean isKeyColumn(int columnId) {
            assert columnId >= 0;
            return columnId == KEY_COL || columnId == keyAliasColumnId;
        }

        /** {@inheritDoc} */
        @Override public boolean isValueColumn(int columnId) {
            assert columnId >= 0;
            return columnId == VAL_COL || columnId == valueAliasColumnId;
        }

        /** {@inheritDoc} */
        @Override public boolean isKeyValueOrVersionColumn(int columnId) {
            assert columnId >= 0;
            if (columnId < DEFAULT_COLUMNS_COUNT)
                return true;
            if (columnId == keyAliasColumnId)
                return true;
            if (columnId == valueAliasColumnId)
                return true;
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean checkKeyIndexCondition(int masks[], int mask) {
            assert masks != null;
            assert masks.length > 0;

            if (keyAliasColumnId < 0)
                return (masks[KEY_COL] & mask) != 0;
            else
                return (masks[KEY_COL] & mask) != 0 || (masks[keyAliasColumnId] & mask) != 0;
        }

        /** {@inheritDoc} */
        @Override public void initValueCache(Value valCache[], Value key, Value value, Value version) {
            assert valCache != null;
            assert valCache.length > 0;

            valCache[KEY_COL] = key;
            valCache[VAL_COL] = value;
            valCache[VER_COL] = version;

            if (keyAliasColumnId > 0)
                valCache[keyAliasColumnId] = key;

            if (valueAliasColumnId > 0)
                valCache[valueAliasColumnId] = value;
        }

        /** {@inheritDoc} */
        @Override public SearchRow prepareProxyIndexRow(SearchRow row) {
            if (row == null)
                return null;

            Value[] data = new Value[row.getColumnCount()];
            for (int idx = 0; idx < data.length; idx++)
                data[idx] = row.getValue(idx);

            copyAliasColumnData(data, KEY_COL, keyAliasColumnId);
            copyAliasColumnData(data, VAL_COL, valueAliasColumnId);

            return new SimpleRow(data);
        }

        /**
         * Copies data between original and alias columns
         *
         * @param data Array of values.
         * @param colId Original column id.
         * @param aliasColId Alias column id.
         */
        private void copyAliasColumnData(Value[] data, int colId, int aliasColId) {
            if (aliasColId <= 0)
                return;

            if (data[aliasColId] == null && data[colId] != null)
                data[aliasColId] = data[colId];

            if (data[colId] == null && data[aliasColId] != null)
                data[colId] = data[aliasColId];
        }

        /** {@inheritDoc} */
        @Override public int getAlternativeColumnId(int colId) {
            if (keyAliasColumnId > 0) {
                if (colId == KEY_COL)
                    return keyAliasColumnId;
                else if (colId == keyAliasColumnId)
                    return KEY_COL;
            }
            if (valueAliasColumnId > 0) {
                if (colId == VAL_COL)
                    return valueAliasColumnId;
                else if (colId == valueAliasColumnId)
                    return VAL_COL;
            }

            return colId;
        }
    }

    /**
     * Statement cache.
     */
    private static class StatementCache extends LinkedHashMap<String, PreparedStatement> {
        /** */
        private int size;

        /** Last usage. */
        private volatile long lastUsage;

        /**
         * @param size Size.
         */
        private StatementCache(int size) {
            super(size, (float)0.75, true);

            this.size = size;
        }

        /** {@inheritDoc} */
        @Override protected boolean removeEldestEntry(Map.Entry<String, PreparedStatement> eldest) {
            boolean rmv = size() > size;

            if (rmv) {
                PreparedStatement stmt = eldest.getValue();

                U.closeQuiet(stmt);
            }

            return rmv;
        }

        /**
         * The timestamp of the last usage of the cache. Used by {@link #cleanupStatementCache()} to remove unused caches.
         * @return last usage timestamp
         */
        private long lastUsage() {
            return lastUsage;
        }

        /**
         * Updates the {@link #lastUsage} timestamp by current time.
         */
        private void updateLastUsage() {
            lastUsage = U.currentTimeMillis();
        }
    }

    /**
     * H2 Table engine.
     */
    public static class H2TableEngine implements TableEngine {
        /** */
        private static GridH2RowDescriptor rowDesc0;

        /** */
        private static H2RowFactory rowFactory0;

        /** */
        private static TableDescriptor tblDesc0;

        /** */
        private static GridH2Table resTbl0;

        /**
         * Creates table using given connection, DDL clause for given type descriptor and list of indexes.
         *
         * @param conn Connection.
         * @param sql DDL clause.
         * @param rowDesc Row descriptor.
         * @param rowFactory Row factory.
         * @param tblDesc Table descriptor.
         * @throws SQLException If failed.
         * @return Created table.
         */
        public static synchronized GridH2Table createTable(Connection conn, String sql,
            @Nullable GridH2RowDescriptor rowDesc, H2RowFactory rowFactory, TableDescriptor tblDesc)
            throws SQLException {
            rowDesc0 = rowDesc;
            rowFactory0 = rowFactory;
            tblDesc0 = tblDesc;

            try {
                try (Statement s = conn.createStatement()) {
                    s.execute(sql + " engine \"" + H2TableEngine.class.getName() + "\"");
                }

                tblDesc.tbl = resTbl0;

                return resTbl0;
            }
            finally {
                resTbl0 = null;
                tblDesc0 = null;
                rowFactory0 = null;
                rowDesc0 = null;
            }
        }

        /** {@inheritDoc} */
        @Override public TableBase createTable(CreateTableData createTblData) {
            resTbl0 = new GridH2Table(createTblData, rowDesc0, rowFactory0, tblDesc0, tblDesc0.schema.spaceName);

            return resTbl0;
        }
    }
}
