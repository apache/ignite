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
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheMemoryMode;
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
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMarshallable;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResult;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResultAdapter;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2DefaultTableEngine;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOffheap;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowFactory;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ValueCacheObject;
import org.apache.ignite.internal.processors.query.h2.opt.GridLuceneIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuerySplitter;
import org.apache.ignite.internal.processors.query.h2.twostep.GridMapQueryExecutor;
import org.apache.ignite.internal.processors.query.h2.twostep.GridReduceQueryExecutor;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.GridEmptyCloseableIterator;
import org.apache.ignite.internal.util.GridSpinBusyLock;
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
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.index.Index;
import org.h2.index.SpatialIndex;
import org.h2.jdbc.JdbcConnection;
import org.h2.jdbc.JdbcPreparedStatement;
import org.h2.jdbc.JdbcStatement;
import org.h2.message.DbException;
import org.h2.mvstore.cache.CacheLongKeyLIRS;
import org.h2.result.SortOrder;
import org.h2.server.web.WebServer;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
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
import static org.apache.ignite.internal.processors.query.GridQueryIndexType.FULLTEXT;
import static org.apache.ignite.internal.processors.query.GridQueryIndexType.GEO_SPATIAL;
import static org.apache.ignite.internal.processors.query.GridQueryIndexType.SORTED;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2AbstractKeyValueRow.KEY_COL;
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
    /** Default DB options. */
    private static final String DB_OPTIONS = ";LOCK_MODE=3;MULTI_THREADED=1;DB_CLOSE_ON_EXIT=FALSE" +
        ";DEFAULT_LOCK_TIMEOUT=10000;FUNCTIONS_IN_SCHEMA=true;OPTIMIZE_REUSE_RESULTS=0;QUERY_CACHE_SIZE=0" +
        ";RECOMPILE_ALWAYS=1;MAX_OPERATION_MEMORY=0;NESTED_JOINS=0;BATCH_JOINS=1" +
        ";ROW_FACTORY=\"" + GridH2RowFactory.class.getName() + "\"" +
        ";DEFAULT_TABLE_ENGINE=" + GridH2DefaultTableEngine.class.getName();

    /** */
    private static final int PREPARED_STMT_CACHE_SIZE = 256;

    /** */
    private static final int TWO_STEP_QRY_CACHE_SIZE = 1024;

    /** Field name for key. */
    public static final String KEY_FIELD_NAME = "_KEY";

    /** Field name for value. */
    public static final String VAL_FIELD_NAME = "_VAL";

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

    /**
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
    private volatile GridKernalContext ctx;

    /** */
    private final DmlStatementsProcessor dmlProc = new DmlStatementsProcessor(this);

    /** */
    private final ConcurrentMap<String, GridH2Table> dataTables = new ConcurrentHashMap8<>();

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
    public Connection connectionForSpace(@Nullable String space) {
        try {
            return connectionForThread(schema(space));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @return Logger.
     */
    IgniteLogger getLogger() {
        return log;
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

            if (stmt != null && !stmt.isClosed() && !((JdbcStatement)stmt).wasCancelled()) {
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
    @Override public PreparedStatement prepareNativeStatement(String schema, String sql) throws SQLException {
        return prepareStatement(connectionForSpace(schema), sql, false);
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
     * Removes entry with specified key from any tables (if exist).
     *
     * @param spaceName Space name.
     * @param key Key.
     * @param tblToUpdate Table to update.
     * @throws IgniteCheckedException In case of error.
     */
    private void removeKey(@Nullable String spaceName, CacheObject key, TableDescriptor tblToUpdate)
        throws IgniteCheckedException {
        try {
            Collection<TableDescriptor> tbls = tables(schema(spaceName));

            Class<?> keyCls = getClass(objectContext(spaceName), key);

            for (TableDescriptor tbl : tbls) {
                if (tbl != tblToUpdate && tbl.type().keyClass().isAssignableFrom(keyCls)) {
                    if (tbl.tbl.update(key, null, 0, true)) {
                        if (tbl.luceneIdx != null)
                            tbl.luceneIdx.remove(key);

                        return;
                    }
                }
            }
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to remove key: " + key, e);
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
    @Override public void store(@Nullable String spaceName, GridQueryTypeDescriptor type, CacheObject k, CacheObject v,
        byte[] ver, long expirationTime) throws IgniteCheckedException {
        TableDescriptor tbl = tableDescriptor(spaceName, type);

        removeKey(spaceName, k, tbl);

        if (tbl == null)
            return; // Type was rejected.

        if (expirationTime == 0)
            expirationTime = Long.MAX_VALUE;

        tbl.tbl.update(k, v, expirationTime, false);

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
     * @param coctx Cache object context.
     * @param o Object.
     * @return Object class.
     */
    private Class<?> getClass(CacheObjectContext coctx, CacheObject o) {
        return isBinary(o) ?
            Object.class :
            o.value(coctx, false).getClass();
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
    @Override public void remove(@Nullable String spaceName, CacheObject key, CacheObject val) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Removing key from cache query index [locId=" + nodeId + ", key=" + key + ", val=" + val + ']');

        CacheObjectContext coctx = objectContext(spaceName);

        Class<?> keyCls = getClass(coctx, key);
        Class<?> valCls = val == null ? null : getClass(coctx, val);

        for (TableDescriptor tbl : tables(schema(spaceName))) {
            if (tbl.type().keyClass().isAssignableFrom(keyCls)
                && (val == null || tbl.type().valueClass().isAssignableFrom(valCls))) {
                if (tbl.tbl.update(key, val, 0, true)) {
                    if (tbl.luceneIdx != null)
                        tbl.luceneIdx.remove(key);

                    return;
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onSwap(@Nullable String spaceName, CacheObject key) throws IgniteCheckedException {
        Schema schema = schemas.get(schema(spaceName));

        if (schema == null)
            return;

        Class<?> keyCls = getClass(objectContext(spaceName), key);

        for (TableDescriptor tbl : schema.tbls.values()) {
            if (tbl.type().keyClass().isAssignableFrom(keyCls)) {
                try {
                    if (tbl.tbl.onSwap(key))
                        return;
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteCheckedException(e);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onUnswap(@Nullable String spaceName, CacheObject key, CacheObject val)
        throws IgniteCheckedException {
        assert val != null;

        CacheObjectContext coctx = objectContext(spaceName);

        Class<?> keyCls = getClass(coctx, key);
        Class<?> valCls = getClass(coctx, val);

        for (TableDescriptor tbl : tables(schema(spaceName))) {
            if (tbl.type().keyClass().isAssignableFrom(keyCls)
                && tbl.type().valueClass().isAssignableFrom(valCls)) {
                try {
                    if (tbl.tbl.onUnswap(key, val))
                        return;
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteCheckedException(e);
                }
            }
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

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocalText(
        @Nullable String spaceName, String qry, GridQueryTypeDescriptor type,
        IndexingQueryFilter filters) throws IgniteCheckedException {
        TableDescriptor tbl = tableDescriptor(spaceName, type);

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
    @Override public void unregisterType(@Nullable String spaceName, GridQueryTypeDescriptor type)
        throws IgniteCheckedException {
        TableDescriptor tbl = tableDescriptor(spaceName, type);

        if (tbl != null)
            removeTable(tbl);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridQueryFieldsResult queryLocalSqlFields(@Nullable final String spaceName, final String qry,
        @Nullable final Collection<Object> params, final IndexingQueryFilter filters, boolean enforceJoinOrder,
        final int timeout, final GridQueryCancel cancel)
        throws IgniteCheckedException {
        final Connection conn = connectionForSpace(spaceName);

        setupConnection(conn, false, enforceJoinOrder);

        final PreparedStatement stmt = preparedStatementWithParams(conn, qry, params, true);

        Prepared p = GridSqlQueryParser.prepared((JdbcPreparedStatement)stmt);

        if (!p.isQuery()) {
            SqlFieldsQuery fldsQry = new SqlFieldsQuery(qry);

            if (params != null)
                fldsQry.setArgs(params.toArray());

            fldsQry.setEnforceJoinOrder(enforceJoinOrder);
            fldsQry.setTimeout(timeout, TimeUnit.MILLISECONDS);

            return dmlProc.updateLocalSqlFields(spaceName, stmt, fldsQry, filters, cancel);
        }

        List<GridQueryFieldMetadata> meta;

        try {
            meta = meta(stmt.getMetaData());
        }
        catch (SQLException e) {
            throw new IgniteCheckedException("Cannot prepare query metadata", e);
        }

        final GridH2QueryContext ctx = new GridH2QueryContext(nodeId, nodeId, 0, LOCAL)
            .filter(filters).distributedJoins(false);

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
    private static String schemaNameFromCacheConf(CacheConfiguration<?,?> ccfg) {
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

        if (timeoutMillis > 0)
            ((Session)((JdbcConnection)conn).getSession()).setQueryTimeout(timeoutMillis);

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
                ((Session)((JdbcConnection)conn).getSession()).setQueryTimeout(0);
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
    public void bindParameters(PreparedStatement stmt, @Nullable Collection<Object> params) throws IgniteCheckedException {
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
    public void setupConnection(Connection conn, boolean distributedJoins, boolean enforceJoinOrder) {
        Session s = session(conn);

        s.setForceJoinOrder(enforceJoinOrder);
        s.setJoinBatchEnabled(distributedJoins);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocalSql(@Nullable String spaceName,
        final String qry, String alias, @Nullable final Collection<Object> params, GridQueryTypeDescriptor type,
        final IndexingQueryFilter filter) throws IgniteCheckedException {
        final TableDescriptor tbl = tableDescriptor(spaceName, type);

        if (tbl == null)
            throw new IgniteSQLException("Failed to find SQL table for type: " + type.name(),
                IgniteQueryErrorCode.TABLE_NOT_FOUND);

        String sql = generateQuery(qry, alias, tbl);

        Connection conn = connectionForThread(tbl.schemaName());

        setupConnection(conn, false, false);

        GridH2QueryContext.set(new GridH2QueryContext(nodeId, nodeId, 0, LOCAL).filter(filter).distributedJoins(false));

        GridRunningQueryInfo run = new GridRunningQueryInfo(qryIdGen.incrementAndGet(), qry, SQL, spaceName,
            U.currentTimeMillis(), null, true);

        runs.put(run.id(), run);

        try {
            ResultSet rs = executeSqlQueryWithTimer(spaceName, conn, sql, params, true, 0, null);

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
     * @return Iterable result.
     */
    private Iterable<List<?>> runQueryTwoStep(final GridCacheContext<?,?> cctx, final GridCacheTwoStepQuery qry,
        final boolean keepCacheObj, final boolean enforceJoinOrder,
        final int timeoutMillis,
        final GridQueryCancel cancel) {
        return new Iterable<List<?>>() {
            @Override public Iterator<List<?>> iterator() {
                return rdcQryExec.query(cctx, qry, keepCacheObj, enforceJoinOrder, timeoutMillis, cancel);
            }
        };
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> QueryCursor<Cache.Entry<K,V>> queryTwoStep(GridCacheContext<?,?> cctx, SqlQuery qry) {
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

        if(qry.getTimeout() > 0)
            fqry.setTimeout(qry.getTimeout(), TimeUnit.MILLISECONDS);

        final QueryCursor<List<?>> res = queryTwoStep(cctx, fqry, null);

        final Iterable<Cache.Entry<K, V>> converted = new Iterable<Cache.Entry<K, V>>() {
            @Override public Iterator<Cache.Entry<K, V>> iterator() {
                final Iterator<List<?>> iter0 = res.iterator();

                return new Iterator<Cache.Entry<K,V>>() {
                    @Override public boolean hasNext() {
                        return iter0.hasNext();
                    }

                    @Override public Cache.Entry<K,V> next() {
                        List<?> l = iter0.next();

                        return new CacheEntryImpl<>((K)l.get(0),(V)l.get(1));
                    }

                    @Override public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };

        // No metadata for SQL queries.
        return new QueryCursorImpl<Cache.Entry<K,V>>(converted) {
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
    @Override public QueryCursor<List<?>> queryTwoStep(GridCacheContext<?, ?> cctx, SqlFieldsQuery qry,
        GridQueryCancel cancel) {
        final String space = cctx.name();
        final String sqlQry = qry.getSql();

        Connection c = connectionForSpace(space);

        final boolean enforceJoinOrder = qry.isEnforceJoinOrder();
        final boolean distributedJoins = qry.isDistributedJoins() && cctx.isPartitioned();
        final boolean grpByCollocated = qry.isCollocated();

        GridCacheTwoStepQuery twoStepQry;
        List<GridQueryFieldMetadata> meta;

        final TwoStepCachedQueryKey cachedQryKey = new TwoStepCachedQueryKey(space, sqlQry, grpByCollocated,
            distributedJoins, enforceJoinOrder);
        TwoStepCachedQuery cachedQry = twoStepCache.get(cachedQryKey);

        if (cachedQry != null) {
            twoStepQry = cachedQry.twoStepQry.copy(qry.getArgs());
            meta = cachedQry.meta;
        }
        else {
            final UUID locNodeId = ctx.localNodeId();

            setupConnection(c, distributedJoins, enforceJoinOrder);

            GridH2QueryContext.set(new GridH2QueryContext(locNodeId, locNodeId, 0, PREPARE)
                .distributedJoins(distributedJoins));

            PreparedStatement stmt;

            boolean cachesCreated = false;

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
                                ctx.cache().createMissingCaches();
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
            }
            finally {
                GridH2QueryContext.clearThreadLocal();
            }

            Prepared prepared = GridSqlQueryParser.prepared((JdbcPreparedStatement) stmt);

            if (qry instanceof JdbcSqlFieldsQuery && ((JdbcSqlFieldsQuery) qry).isQuery() != prepared.isQuery())
                throw new IgniteSQLException("Given statement type does not match that declared by JDBC driver",
                    IgniteQueryErrorCode.STMT_TYPE_MISMATCH);

            if (!prepared.isQuery()) {
                try {
                    return dmlProc.updateSqlFieldsTwoStep(cctx.namexx(), stmt, qry, cancel);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteSQLException("Failed to execute DML statement [qry=" + sqlQry + ", params=" +
                        Arrays.deepToString(qry.getArgs()) + "]", e);
                }
            }

            try {
                bindParameters(stmt, F.asList(qry.getArgs()));

                twoStepQry = GridSqlQuerySplitter.split((JdbcPreparedStatement)stmt, qry.getArgs(), grpByCollocated,
                    distributedJoins);

                List<Integer> caches;
                List<Integer> extraCaches = null;

                // Setup spaces from schemas.
                if (!twoStepQry.schemas().isEmpty()) {
                    Collection<String> spaces = new ArrayList<>(twoStepQry.schemas().size());
                    caches = new ArrayList<>(twoStepQry.schemas().size() + 1);
                    caches.add(cctx.cacheId());

                    for (String schema : twoStepQry.schemas()) {
                        String space0 = space(schema);

                        spaces.add(space0);

                        if (!F.eq(space0, space)) {
                            int cacheId = CU.cacheId(space0);

                            caches.add(cacheId);

                            if (extraCaches == null)
                                extraCaches = new ArrayList<>();

                            extraCaches.add(cacheId);
                        }
                    }

                    twoStepQry.spaces(spaces);
                }
                else {
                    caches = Collections.singletonList(cctx.cacheId());
                    extraCaches = null;
                }

                twoStepQry.caches(caches);
                twoStepQry.extraCaches(extraCaches);

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
            runQueryTwoStep(cctx, twoStepQry, cctx.keepBinary(), enforceJoinOrder, qry.getTimeout(), cancel), cancel);

        cursor.fieldsMeta(meta);

        if (cachedQry == null && !twoStepQry.explain()) {
            cachedQry = new TwoStepCachedQuery(meta, twoStepQry.copy(null));
            twoStepCache.putIfAbsent(cachedQryKey, cachedQry);
        }

        return cursor;
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
    @Override public boolean registerType(@Nullable String spaceName, GridQueryTypeDescriptor type)
        throws IgniteCheckedException {
        if (!validateTypeDescriptor(type))
            return false;

        String schemaName = schema(spaceName);

        Schema schema = schemas.get(schemaName);

        TableDescriptor tbl = new TableDescriptor(schema, type);

        try {
            Connection conn = connectionForThread(schemaName);

            createTable(schema, tbl, conn);

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
     * @return True if type is valid.
     * @throws IgniteCheckedException If validation failed.
     */
    private boolean validateTypeDescriptor(GridQueryTypeDescriptor type)
        throws IgniteCheckedException {
        assert type != null;

        Collection<String> names = new HashSet<>();

        names.addAll(type.fields().keySet());

        if (names.size() < type.fields().size())
            throw new IgniteCheckedException("Found duplicated properties with the same name [keyType=" +
                type.keyClass().getName() + ", valueType=" + type.valueClass().getName() + "]");

        String ptrn = "Name ''{0}'' is reserved and cannot be used as a field name [type=" + type.name() + "]";

        for (String name : names) {
            if (name.equalsIgnoreCase(KEY_FIELD_NAME) || name.equalsIgnoreCase(VAL_FIELD_NAME))
                throw new IgniteCheckedException(MessageFormat.format(ptrn, name));
        }

        return true;
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
     * @param schema Schema.
     * @param tbl Table descriptor.
     * @param conn Connection.
     * @throws SQLException If failed to create db table.
     */
    private void createTable(Schema schema, TableDescriptor tbl, Connection conn) throws SQLException {
        assert schema != null;
        assert tbl != null;

        boolean escapeAll = schema.escapeAll();

        String keyType = dbTypeFromClass(tbl.type().keyClass());
        String valTypeStr = dbTypeFromClass(tbl.type().valueClass());

        SB sql = new SB();

        sql.a("CREATE TABLE ").a(tbl.fullTableName()).a(" (")
            .a(KEY_FIELD_NAME).a(' ').a(keyType).a(" NOT NULL");

        sql.a(',').a(VAL_FIELD_NAME).a(' ').a(valTypeStr);

        for (Map.Entry<String, Class<?>> e: tbl.type().fields().entrySet())
            sql.a(',').a(escapeName(e.getKey(), escapeAll)).a(' ').a(dbTypeFromClass(e.getValue()));

        sql.a(')');

        if (log.isDebugEnabled())
            log.debug("Creating DB table with SQL: " + sql);

        GridH2RowDescriptor desc = new RowDescriptor(tbl.type(), schema);

        GridH2Table res = GridH2Table.Engine.createTable(conn, sql.toString(), desc, tbl, tbl.schema.spaceName);

        if (dataTables.putIfAbsent(res.identifier(), res) != null)
            throw new IllegalStateException("Table already exists: " + res.identifier());
    }

    /**
     * @param identifier Table identifier.
     * @return Data table.
     */
    public GridH2Table dataTable(String identifier) {
        return dataTables.get(identifier);
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
     * Gets table descriptor by value type.
     *
     * @param spaceName Space name.
     * @param type Value type descriptor.
     * @return Table descriptor or {@code null} if not found.
     */
    @Nullable private TableDescriptor tableDescriptor(@Nullable String spaceName, GridQueryTypeDescriptor type) {
        return tableDescriptor(type.name(), spaceName);
    }

    /**
     * Gets table descriptor by type and space names.
     *
     * @param type Type name.
     * @param space Space name.
     * @return Table descriptor.
     */
    @Nullable private TableDescriptor tableDescriptor(String type, @Nullable String space) {
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
    private String schema(@Nullable String space) {
        return emptyIfNull(space2schema.get(emptyIfNull(space)));
    }

    /**
     * Called periodically by {@link GridTimeoutProcessor} to clean up the {@link #stmtCache}.
     */
    private void cleanupStatementCache() {
        long cur = U.currentTimeMillis();

        for(Iterator<Map.Entry<Thread, StatementCache>> it = stmtCache.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Thread, StatementCache> entry = it.next();

            Thread t = entry.getKey();

            if (t.getState() == Thread.State.TERMINATED
                || cur - entry.getValue().lastUsage() > STATEMENT_CACHE_THREAD_USAGE_TIMEOUT)
                it.remove();
        }
    }

    /**
     * Gets space name from database schema.
     *
     * @param schemaName Schema name. Could not be null. Could be empty.
     * @return Space name. Could be null.
     */
    public String space(String schemaName) {
        assert schemaName != null;

        Schema schema = schemas.get(schemaName);

        // For the compatibility with conversion from """" to "" inside h2 lib
        if (schema == null) {
            assert schemaName.isEmpty() || schemaName.charAt(0) != ESC_CH;

            schema = schemas.get(escapeName(schemaName, true));
        }

        return schema.spaceName;
    }

    /** {@inheritDoc} */
    @Override public void rebuildIndexes(@Nullable String spaceName, GridQueryTypeDescriptor type) {
        TableDescriptor tbl = tableDescriptor(spaceName, type);

        if (tbl == null)
            return;

        if (tbl.schema.offheap != null)
            throw new UnsupportedOperationException("Index rebuilding is not supported when off-heap memory is used");

        tbl.tbl.rebuildIndexes();
    }

    /**
     * Gets size (for tests only).
     *
     * @param spaceName Space name.
     * @param type Type descriptor.
     * @return Size.
     * @throws IgniteCheckedException If failed or {@code -1} if the type is unknown.
     */
    long size(@Nullable String spaceName, GridQueryTypeDescriptor type) throws IgniteCheckedException {
        TableDescriptor tbl = tableDescriptor(spaceName, type);

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

        if (JdbcUtils.serializer != null)
            U.warn(log, "Custom H2 serialization is already configured, will override.");

        JdbcUtils.serializer = h2Serializer();

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
        }

        // TODO https://issues.apache.org/jira/browse/IGNITE-2139
        // registerMBean(gridName, this, GridH2IndexingSpiMBean.class);
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
                locNode = node;

                continue;
            }

            try {
                if (specialize != null) {
                    msg = specialize.apply(node, msg);

                    if (msg instanceof GridCacheQueryMarshallable)
                        ((GridCacheQueryMarshallable)msg).marshall(marshaller);
                }

                ctx.io().send(node, topic, topicOrd, msg, plc);
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
                            locNodeHnd.apply(finalLocNode, finalMsg);
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

        for (Schema schema : schemas.values())
            schema.onDrop();

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
    @Override public void registerCache(GridCacheContext<?, ?> cctx, CacheConfiguration<?,?> ccfg)
        throws IgniteCheckedException {
        String schema = schemaNameFromCacheConf(ccfg);

        if (schemas.putIfAbsent(schema, new Schema(ccfg.getName(), schema, cctx, ccfg)) != null)
            throw new IgniteCheckedException("Cache already registered: " + U.maskName(ccfg.getName()));

        space2schema.put(emptyIfNull(ccfg.getName()), schema);

        createSchema(schema);

        createSqlFunctions(schema, ccfg.getSqlFunctionClasses());
    }

    /** {@inheritDoc} */
    @Override public void unregisterCache(CacheConfiguration<?, ?> ccfg) {
        String schema = schema(ccfg.getName());
        Schema rmv = schemas.remove(schema);

        if (rmv != null) {
            space2schema.remove(emptyIfNull(rmv.spaceName));
            mapQryExec.onCacheStop(ccfg.getName());

            rmv.onDrop();

            try {
                dropSchema(schema);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to drop schema on cache stop (will ignore): " + U.maskName(ccfg.getName()), e);
            }

            for (Iterator<Map.Entry<TwoStepCachedQueryKey, TwoStepCachedQuery>> it = twoStepCache.entrySet().iterator();
                it.hasNext();) {
                Map.Entry<TwoStepCachedQueryKey, TwoStepCachedQuery> e = it.next();

                if (F.eq(e.getKey().space, ccfg.getName()))
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
                        return new IgniteBiPredicate<K,V>() {
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

                    return new IgniteBiPredicate<K,V>() {
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

        /**
         * @param space Space.
         * @param sql Sql.
         * @param grpByCollocated Collocated GROUP BY.
         * @param distributedJoins Distributed joins enabled.
         * @param enforceJoinOrder Enforce join order of tables.
         */
        private TwoStepCachedQueryKey(String space,
            String sql,
            boolean grpByCollocated,
            boolean distributedJoins,
            boolean enforceJoinOrder) {
            this.space = space;
            this.sql = sql;
            this.grpByCollocated = grpByCollocated;
            this.distributedJoins = distributedJoins;
            this.enforceJoinOrder = enforceJoinOrder;
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

            return sql.equals(that.sql);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = space != null ? space.hashCode() : 0;
            res = 31 * res + sql.hashCode();
            res = 31 * res + (grpByCollocated ? 1 : 0);
            res = 31 * res + (distributedJoins ? 1 : 0);
            res = 31 * res + (enforceJoinOrder ? 1 : 0);

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
     * @param cols Columns list.
     * @param keyCol Primary key column.
     * @param affCol Affinity key column.
     * @return The same list back.
     */
    private static List<IndexColumn> treeIndexColumns(List<IndexColumn> cols, IndexColumn keyCol, IndexColumn affCol) {
        assert keyCol != null;

        if (!containsColumn(cols, keyCol))
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
    private class TableDescriptor implements GridH2Table.IndexesFactory {
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

        /**
         * @param schema Schema.
         * @param type Type descriptor.
         */
        TableDescriptor(Schema schema, GridQueryTypeDescriptor type) {
            this.type = type;
            this.schema = schema;

            String tblName = escapeName(type.tableName() != null ? type.tableName() : type.name(), schema.escapeAll());

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

        /** {@inheritDoc} */
        @Override public ArrayList<Index> createIndexes(GridH2Table tbl) {
            this.tbl = tbl;

            ArrayList<Index> idxs = new ArrayList<>();

            IndexColumn keyCol = tbl.indexColumn(KEY_COL, SortOrder.ASCENDING);
            IndexColumn affCol = tbl.getAffinityKeyColumn();

            if (affCol != null && equal(affCol, keyCol))
                affCol = null;

            // Add primary key index.
            idxs.add(new GridH2TreeIndex("_key_PK", tbl, true,
                treeIndexColumns(new ArrayList<IndexColumn>(2), keyCol, affCol)));

            if (type().valueClass() == String.class) {
                try {
                    luceneIdx = new GridLuceneIndex(ctx, schema.offheap, schema.spaceName, type);
                }
                catch (IgniteCheckedException e1) {
                    throw new IgniteException(e1);
                }
            }

            boolean affIdxFound = false;

            for (Map.Entry<String, GridQueryIndexDescriptor> e : type.indexes().entrySet()) {
                String name = e.getKey();
                GridQueryIndexDescriptor idx = e.getValue();

                if (idx.type() == FULLTEXT) {
                    try {
                        luceneIdx = new GridLuceneIndex(ctx, schema.offheap, schema.spaceName, type);
                    }
                    catch (IgniteCheckedException e1) {
                        throw new IgniteException(e1);
                    }
                }
                else {
                    List<IndexColumn> cols = new ArrayList<>(idx.fields().size() + 2);

                    boolean escapeAll = schema.escapeAll();

                    for (String field : idx.fields()) {
                        String fieldName = escapeAll ? field : escapeName(field, false).toUpperCase();

                        Column col = tbl.getColumn(fieldName);

                        cols.add(tbl.indexColumn(col.getColumnId(),
                            idx.descending(field) ? SortOrder.DESCENDING : SortOrder.ASCENDING));
                    }

                    if (idx.type() == SORTED) {
                        // We don't care about number of fields in affinity index, just affinity key must be the first.
                        affIdxFound |= affCol != null && equal(cols.get(0), affCol);

                        cols = treeIndexColumns(cols, keyCol, affCol);

                        idxs.add(new GridH2TreeIndex(name, tbl, false, cols));
                    }
                    else if (idx.type() == GEO_SPATIAL)
                        idxs.add(createH2SpatialIndex(tbl, name, cols.toArray(new IndexColumn[cols.size()])));
                    else
                        throw new IllegalStateException("Index type: " + idx.type());
                }
            }

            // Add explicit affinity key index if nothing alike was found.
            if (affCol != null && !affIdxFound) {
                idxs.add(new GridH2TreeIndex("AFFINITY_KEY", tbl, false,
                    treeIndexColumns(new ArrayList<IndexColumn>(2), affCol, keyCol)));
            }

            return idxs;
        }

        /**
         *
         */
        void onDrop() {
            dataTables.remove(tbl.identifier(), tbl);

            tbl.destroy();

            U.closeQuiet(luceneIdx);
        }

        /**
         * @param tbl Table.
         * @param idxName Index name.
         * @param cols Columns.
         */
        private SpatialIndex createH2SpatialIndex(
            Table tbl,
            String idxName,
            IndexColumn[] cols
        ) {
            String className = "org.apache.ignite.internal.processors.query.h2.opt.GridH2SpatialIndex";

            try {
                Class<?> cls = Class.forName(className);

                Constructor<?> ctor = cls.getConstructor(
                    Table.class,
                    String.class,
                    IndexColumn[].class);

                if (!ctor.isAccessible())
                    ctor.setAccessible(true);

                return (SpatialIndex)ctor.newInstance(tbl, idxName, cols);
            }
            catch (Exception e) {
                throw new IgniteException("Failed to instantiate: " + className, e);
            }
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
        private final GridUnsafeMemory offheap;

        /** */
        private final ConcurrentMap<String, TableDescriptor> tbls = new ConcurrentHashMap8<>();

        /** Cache for deserialized offheap rows. */
        private final CacheLongKeyLIRS<GridH2KeyValueRowOffheap> rowCache;

        /** */
        private final GridCacheContext<?,?> cctx;

        /** */
        private final CacheConfiguration<?,?> ccfg;

        /**
         * @param spaceName Space name.
         * @param schemaName Schema name.
         * @param cctx Cache context.
         * @param ccfg Cache configuration.
         */
        private Schema(String spaceName, String schemaName, GridCacheContext<?,?> cctx, CacheConfiguration<?,?> ccfg) {
            this.spaceName = spaceName;
            this.cctx = cctx;
            this.schemaName = schemaName;
            this.ccfg = ccfg;

            offheap = ccfg.getOffHeapMaxMemory() >= 0 || ccfg.getMemoryMode() == CacheMemoryMode.OFFHEAP_TIERED ?
                new GridUnsafeMemory(0) : null;

            if (offheap != null) {
                CacheLongKeyLIRS.Config lirsCfg = new CacheLongKeyLIRS.Config();

                lirsCfg.maxMemory = ccfg.getSqlOnheapRowCacheSize();

                rowCache = new CacheLongKeyLIRS<>(lirsCfg);
            } else
                rowCache = null;
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
        private final boolean preferSwapVal;

        /** */
        private final boolean snapshotableIdx;

        /** */
        private final GridQueryProperty[] props;

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

            preferSwapVal = schema.ccfg.getMemoryMode() == CacheMemoryMode.OFFHEAP_TIERED;
            snapshotableIdx = schema.ccfg.isSnapshotableIndex() || schema.offheap != null;
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
        @Override public GridCacheContext<?,?> context() {
            return schema.cctx;
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
        @Override public void cache(GridH2KeyValueRowOffheap row) {
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
                        obj = new Timestamp(((java.util.Date) obj).getTime());

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
        @Override public GridH2Row createRow(CacheObject key, @Nullable CacheObject val, long expirationTime)
            throws IgniteCheckedException {
            try {
                if (val == null) // Only can happen for remove operation, can create simple search row.
                    return GridH2RowFactory.create(wrap(key, keyType));

                return schema.offheap == null ?
                    new GridH2KeyValueRowOnheap(this, key, keyType, val, valType, expirationTime) :
                    new GridH2KeyValueRowOffheap(this, key, keyType, val, valType, expirationTime);
            }
            catch (ClassCastException e) {
                throw new IgniteCheckedException("Failed to convert key to SQL type. " +
                    "Please make sure that you always store each value type with the same key type " +
                    "or configure key type as common super class for all actual keys for this value type.", e);
            }
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public Object readFromSwap(Object key) throws IgniteCheckedException {
            IgniteInternalCache<Object, ?> cache = ctx.cache().cache(schema.spaceName);

            GridCacheContext cctx = cache.context();

            if (cctx.isNear())
                cctx = cctx.near().dht().context();

            CacheObject v = cctx.swap().readValue(cctx.toCacheKeyObject(key), true, true);

            if (v == null)
                return null;

            return v;
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
            GridH2KeyValueRowOffheap row = schema.rowCache.get(ptr);

            if (row != null) {
                assert row.pointer() == ptr : ptr + " " + row.pointer();

                return row;
            }

            return new GridH2KeyValueRowOffheap(this, ptr);
        }

        /** {@inheritDoc} */
        @Override public boolean preferSwapValue() {
            return preferSwapVal;
        }

        /** {@inheritDoc} */
        @Override public boolean snapshotableIndex() {
            return snapshotableIdx;
        }

        /** {@inheritDoc} */
        @Override public boolean quoteAllIdentifiers() {
            return schema.escapeAll();
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
}
