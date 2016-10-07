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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMarshallable;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResult;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResultAdapter;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
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
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlConst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlDelete;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlElement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlInsert;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlMerge;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlParameter;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuery;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuerySplitter;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlSelect;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlTable;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlUpdate;
import org.apache.ignite.internal.processors.query.h2.twostep.GridMapQueryExecutor;
import org.apache.ignite.internal.processors.query.h2.twostep.GridReduceQueryExecutor;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.GridEmptyCloseableIterator;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridPlainClosure;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.lang.GridTriple;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.lang.IgniteSingletonIterator;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeGuard;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
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
import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_INDEXING_CACHE_CLEANUP_PERIOD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_H2_INDEXING_CACHE_THREAD_USAGE_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.getString;
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
    public static final String KEY_FIELD_NAME = "_key";

    /** Field name for value. */
    public static final String VAL_FIELD_NAME = "_val";

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

    /** */
    private static int DFLT_DML_RERUN_ATTEMPTS = 4;

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
    private GridSpinBusyLock busyLock;

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
    private final ConcurrentMap<String, GridH2Table> dataTables = new ConcurrentHashMap8<>();

    /** */
    private final ConcurrentMap<String, String> dataTablesToTypes = new ConcurrentHashMap8<>();

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

            if (stmt != null && !stmt.isClosed()) {
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
        JdbcPreparedStatement stmt = (JdbcPreparedStatement) prepareStatement(connectionForSpace(schema), sql, false);

        Prepared p = GridSqlQueryParser.prepared(stmt);
        p.prepare(); // To enforce types resolution.

        return stmt;
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
                ", table=" + tbl.fullTableName() + "]", e);
        }
        finally {
            U.close(stmt, log);
        }

        tbl.onDrop();

        tbl.schema.tbls.remove(tbl.name());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocalText(
        @Nullable String spaceName, String qry, GridQueryTypeDescriptor type,
        IndexingQueryFilter filters) throws IgniteCheckedException {
        TableDescriptor tbl = tableDescriptor(spaceName, type);

        if (tbl != null && tbl.luceneIdx != null)
            return tbl.luceneIdx.query(qry, filters);

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
                                                               @Nullable final Collection<Object> params, final IndexingQueryFilter filters, boolean enforceJoinOrder)
        throws IgniteCheckedException {
        Connection conn = connectionForSpace(spaceName);

        long start = U.currentTimeMillis();

        PreparedStatement stmt;

        try {
            stmt = prepareStatement(conn, qry, true);
        }
        catch (SQLException e) {
            throw new IgniteSQLException(e);
        }

        Prepared p = GridSqlQueryParser.prepared((JdbcPreparedStatement) stmt);

        initLocalQueryContext(conn, enforceJoinOrder, filters);

        if (!p.isQuery())
            return updateLocalSqlFields(cacheContext(spaceName), stmt, params != null ? params.toArray() : null,
                filters, enforceJoinOrder);

        try {
            ResultSet rs = executeSqlQueryStatementWithTimer(spaceName, conn, stmt, qry, params, start);

            List<GridQueryFieldMetadata> meta = null;

            if (rs != null) {
                try {
                    meta = meta(rs.getMetaData());
                }
                catch (SQLException e) {
                    throw new IgniteSQLException("Failed to get meta data.", e);
                }
            }

            return new GridQueryFieldsResultAdapter(meta, new FieldsIterator(rs));
        }
        finally {
            GridH2QueryContext.clearThreadLocal();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    private GridQueryFieldsResult updateLocalSqlFields(final GridCacheContext cctx, final PreparedStatement stmt,
        final Object[] params, final IndexingQueryFilter filters, boolean enforceJoinOrder)
        throws IgniteCheckedException {
        Object[] errKeys = null;

        int items = 0;

        for (int i = 0; i < DFLT_DML_RERUN_ATTEMPTS; i++) {
            IgniteBiTuple<Integer, Object[]> r = updateLocalSqlFields0(cctx, stmt, params, errKeys, filters,
                enforceJoinOrder);

            if (F.isEmpty(r.get2())) {
                return new GridQueryFieldsResultAdapter(Collections.<GridQueryFieldMetadata>emptyList(),
                    new IgniteSingletonIterator(Collections.singletonList(items + r.get1())));
            }
            else {
                items += r.get1();
                errKeys = r.get2();
            }
        }

        throw createSqlException("Failed to update or delete some keys: " + Arrays.deepToString(errKeys),
            ErrorCode.CONCURRENT_UPDATE_1);
    }

    /**
     * Actually perform SQL DML operation locally.
     *
     * @param cctx Cache context.
     * @param prepStmt Prepared statement for DML query.
     * @param params Query params.
     * @param failedKeys Keys to restrict UPDATE and DELETE operations with. Null or empty array means no restriction.
     * @param filters Filters.
     * @param enforceJoinOrder Enforce join order.
     * @return Pair [number of successfully processed items; keys that have failed to be processed]
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("ConstantConditions")
    private IgniteBiTuple<Integer, Object[]> updateLocalSqlFields0(final GridCacheContext cctx,
                                                                   final PreparedStatement prepStmt, Object[] params, final Object[] failedKeys, final IndexingQueryFilter filters,
                                                                   boolean enforceJoinOrder) throws IgniteCheckedException {
        Connection conn = connectionForSpace(cctx.name());

        initLocalQueryContext(conn, enforceJoinOrder, filters);

        try {
            Prepared p = GridSqlQueryParser.prepared((JdbcPreparedStatement) prepStmt);

            GridSqlStatement stmt = new GridSqlQueryParser().parse(p);

            if (stmt instanceof GridSqlMerge || stmt instanceof GridSqlInsert) {
                GridSqlQuery sel;

                if (stmt instanceof GridSqlInsert) {
                    GridSqlInsert ins = (GridSqlInsert) stmt;
                    sel = GridSqlQuerySplitter.selectForInsertOrMerge(ins.rows(), ins.query());
                }
                else {
                    GridSqlMerge merge = (GridSqlMerge) stmt;
                    sel = GridSqlQuerySplitter.selectForInsertOrMerge(merge.rows(), merge.query());
                }

                ResultSet rs = executeSqlQueryWithTimer(cctx.name(), conn, sel.getSQL(), F.asList(params), true);

                final Iterator<List<?>> rsIter = new FieldsIterator(rs);

                Iterable<List<?>> it = new Iterable<List<?>>() {
                    /** {@inheritDoc} */
                    @Override public Iterator<List<?>> iterator() {
                        return rsIter;
                    }
                };

                QueryCursorImpl<List<?>> cur = new QueryCursorImpl<>(it);

                int res;

                if (stmt instanceof GridSqlMerge)
                    res = doMerge(cctx, (GridSqlMerge) stmt, cur);
                else
                    res = doInsert(cctx, (GridSqlInsert) stmt, cur);

                return new IgniteBiTuple<>(res, X.EMPTY_OBJECT_ARRAY);
            }
            else {
                if (F.isEmpty(failedKeys)) {
                    GridTriple<GridSqlElement> singleUpdate;

                    if (stmt instanceof GridSqlUpdate)
                        singleUpdate = GridSqlQuerySplitter.getSingleItemFilter((GridSqlUpdate) stmt);
                    else if (stmt instanceof GridSqlDelete)
                        singleUpdate = GridSqlQuerySplitter.getSingleItemFilter((GridSqlDelete) stmt);
                    else
                        throw createSqlException("Unexpected DML operation [cls=" + stmt.getClass().getName() + ']',
                            ErrorCode.PARSE_ERROR_1);

                    if (singleUpdate != null)
                        return new IgniteBiTuple<>(doSingleUpdate(cctx, singleUpdate, params), X.EMPTY_OBJECT_ARRAY);
                }

                GridSqlSelect map;

                int paramsCnt = F.isEmpty(params) ? 0 : params.length;

                Integer keysParamIdx = !F.isEmpty(failedKeys) ? paramsCnt + 1 : null;

                if (stmt instanceof GridSqlUpdate)
                    map = GridSqlQuerySplitter.mapQueryForUpdate((GridSqlUpdate) stmt, keysParamIdx);
                else
                    map = GridSqlQuerySplitter.mapQueryForDelete((GridSqlDelete) stmt, keysParamIdx);

                if (keysParamIdx != null) {
                    params = Arrays.copyOf(U.firstNotNull(params, X.EMPTY_OBJECT_ARRAY), paramsCnt + 1);
                    params[paramsCnt] = failedKeys;
                }

                ResultSet rs = executeSqlQueryWithTimer(cctx.name(), conn, map.getSQL(), F.asList(params), true);

                final Iterator<List<?>> rsIter = new FieldsIterator(rs);

                Iterable<List<?>> it = new Iterable<List<?>>() {
                    /** {@inheritDoc} */
                    @Override public Iterator<List<?>> iterator() {
                        return rsIter;
                    }
                };

                QueryCursorImpl<List<?>> cur = new QueryCursorImpl<>(it);

                if (stmt instanceof GridSqlUpdate)
                    return doUpdate(cctx, (GridSqlUpdate) stmt, cur);
                else
                    return doDelete(cctx, cur);
            }
        }
        finally {
            GridH2QueryContext.clearThreadLocal();
        }
    }

    /**
     * Perform single cache operation based on given args.
     *
     * @param cctx Cache context.
     * @param singleUpdate Triple {@code [key; value; new value]} to perform remove or replace with.
     * @param params
     * @return 1 if an item was affected, 0 otherwise.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    private static int doSingleUpdate(GridCacheContext cctx, GridTriple<GridSqlElement> singleUpdate, Object[] params)
        throws IgniteCheckedException {
        int res;

        Object key = getElementValue(singleUpdate.get1(), params);
        Object val = getElementValue(singleUpdate.get2(), params);
        Object newVal = getElementValue(singleUpdate.get3(), params);

        if (newVal != null) { // Single item UPDATE
            if (val == null) // No _val bound in source query
                res = cctx.cache().replace(key, newVal) ? 1 : 0;
            else
                res = cctx.cache().replace(key, val, newVal) ? 1 : 0;
        }
        else { // Single item DELETE
            if (val == null) // No _val bound in source query
                res = cctx.cache().remove(key) ? 1 : 0;
            else
                res = cctx.cache().remove(key, val) ? 1 : 0;
        }

        return res;
    }

    /**
     * @param rsMeta Metadata.
     * @return List of fields metadata.
     * @throws SQLException If failed.
     */
    private static List<GridQueryFieldMetadata> meta(ResultSetMetaData rsMeta) throws SQLException {
        if (rsMeta == null)
            return Collections.emptyList();

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
     * Executes sql query.
     *
     * @param conn Connection,.
     * @param sql Sql query.
     * @param params Parameters.
     * @param useStmtCache If {@code true} uses statement cache.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    private ResultSet executeSqlQuery(Connection conn, String sql, Collection<Object> params, boolean useStmtCache)
        throws IgniteCheckedException {
        PreparedStatement stmt;

        try {
            stmt = prepareStatement(conn, sql, useStmtCache);
        }
        catch (SQLException e) {
            throw new IgniteSQLException("Failed to parse SQL query: " + sql, e);
        }

        return executeSqlQueryStatement((JdbcPreparedStatement) stmt, sql, params);
    }

    /**
     * @param stmt
     * @param sql
     * @param params
     * @return
     * @throws IgniteCheckedException
     */
    private ResultSet executeSqlQueryStatement(JdbcPreparedStatement stmt, String sql, Collection<Object> params)
        throws IgniteCheckedException {
        switch (commandType(stmt)) {
            case CommandInterface.SELECT:
            case CommandInterface.CALL:
            case CommandInterface.EXPLAIN:
            case CommandInterface.ANALYZE:
                break;
            default:
                throw new IgniteCheckedException("Failed to execute non-query SQL statement: " + sql);
        }

        bindParameters(stmt, params);

        try {
            return stmt.executeQuery();
        }
        catch (SQLException e) {
            onSqlException();

            throw new IgniteSQLException("Failed to execute SQL query.", e);
        }
    }

    /**
     * Executes sql query and prints warning if query is too slow.
     *
     * @param space Space name.
     * @param conn Connection,.
     * @param sql Sql query.
     * @param params Parameters.
     * @param useStmtCache If {@code true} uses statement cache.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    public ResultSet executeSqlQueryWithTimer(String space, Connection conn, String sql, @Nullable Collection<Object> params,
        boolean useStmtCache) throws IgniteCheckedException {
        long start = U.currentTimeMillis();
        PreparedStatement stmt;

        try {
            stmt = prepareStatement(conn, sql, useStmtCache);
        }
        catch (SQLException e) {
            throw new IgniteSQLException(e);
        }

        return executeSqlQueryStatementWithTimer(space, conn, stmt, sql, params, start);
    }

    /**
     * Executes sql query and prints warning if query is too slow.
     *
     * @param space Space name.
     * @param conn Connection,.
     * @param sql Sql query.
     * @param params Parameters.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    private ResultSet executeSqlQueryStatementWithTimer(String space, Connection conn, PreparedStatement stmt, String sql,
        @Nullable Collection<Object> params, long start) throws IgniteCheckedException {
        long start0 = start != 0 ? start : U.currentTimeMillis();

        try {
            ResultSet rs = executeSqlQueryStatement((JdbcPreparedStatement) stmt, sql, params);

            long time = U.currentTimeMillis() - start0;

            long longQryExecTimeout = schemas.get(schema(space)).ccfg.getLongQueryWarningTimeout();

            if (time > longQryExecTimeout) {
                String msg = "Query execution is too long (" + time + " ms): " + sql;

                ResultSet plan = executeSqlQuery(conn, "EXPLAIN " + sql, params, false);

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

            throw new IgniteSQLException(e);
        }
    }

    /**
     * @param cctx Cache context.
     * @param gridStmt Grid SQL statement.
     * @param cursor Cursor to take inserted data from.
     * @return Number of items affected.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    private int doMerge(GridCacheContext cctx, GridSqlMerge gridStmt, QueryCursorImpl<List<?>> cursor)
        throws IgniteCheckedException {
        // This check also protects us from attempts to update key or its fields directly -
        // when no key except cache key can be used, it will serve only for uniqueness checks,
        // not for updates, and hence will allow putting new pairs only.
        GridSqlColumn[] keys = gridStmt.keys();
        if (keys.length != 1 || !keys[0].columnName().equalsIgnoreCase(KEY_FIELD_NAME))
            throw new CacheException("SQL MERGE does not support arbitrary keys");

        GridSqlColumn[] cols = gridStmt.columns();

        int keyColIdx = -1;
        int valColIdx = -1;

        boolean hasKeyProps = false;
        boolean hasValProps = false;

        GridH2Table tbl = gridTableForElement(gridStmt.into());

        GridH2RowDescriptor desc = tbl.rowDescriptor();

        for (int i = 0; i < cols.length; i++) {
            if (cols[i].columnName().equalsIgnoreCase(KEY_FIELD_NAME)) {
                keyColIdx = i;
                continue;
            }

            if (cols[i].columnName().equalsIgnoreCase(VAL_FIELD_NAME)) {
                valColIdx = i;
                continue;
            }

            GridQueryProperty prop = desc.type().property(cols[i].columnName());

            X.ensureX(prop != null, "Property '" + cols[i].columnName() + "' not found.");

            if (prop.key())
                hasKeyProps = true;
            else
                hasValProps = true;
        }

        Supplier keySupplier = createSupplier(cctx, desc.type(), keyColIdx, hasKeyProps, true);
        Supplier valSupplier = createSupplier(cctx, desc.type(), valColIdx, hasValProps, false);

        Iterable<List<?>> src;

        boolean singleRow = false;

        if (gridStmt.query() != null)
            src = cursor;
        else {
            singleRow = gridStmt.rows().size() == 1;
            src = rowsCursorToRows(cursor);
        }

        // If we have just one item to put, just do so
        if (singleRow) {
            IgniteBiTuple t = rowToKeyValue(cctx, desc.type(), keySupplier, valSupplier, keyColIdx, valColIdx, cols,
                src.iterator().next().toArray());
            cctx.cache().put(t.getKey(), t.getValue());
            return 1;
        }
        else {
            Map<Object, Object> rows = new LinkedHashMap<>();
            for (List<?> row : src) {
                IgniteBiTuple t = rowToKeyValue(cctx, desc.type(), keySupplier, valSupplier, keyColIdx, valColIdx, cols,
                    row.toArray());
                rows.put(t.getKey(), t.getValue());
            }
            cctx.cache().putAll(rows);
            return rows.size();
        }
    }

    /**
     * @param cctx Cache context.
     * @param ins Insert statement.
     * @param cursor Cursor to take inserted data from.
     * @return Number of items affected.
     * @throws IgniteCheckedException if failed, particularly in case of duplicate keys.
     */
    @SuppressWarnings("unchecked")
    private int doInsert(GridCacheContext cctx, GridSqlInsert ins, QueryCursorImpl<List<?>> cursor)
        throws IgniteCheckedException {
        GridSqlColumn[] cols = ins.columns();

        int keyColIdx = -1;
        int valColIdx = -1;

        boolean hasKeyProps = false;
        boolean hasValProps = false;

        GridH2Table tbl = gridTableForElement(ins.into());

        GridH2RowDescriptor desc = tbl.rowDescriptor();

        for (int i = 0; i < cols.length; i++) {
            if (cols[i].columnName().equalsIgnoreCase(KEY_FIELD_NAME)) {
                keyColIdx = i;
                continue;
            }

            if (cols[i].columnName().equalsIgnoreCase(VAL_FIELD_NAME)) {
                valColIdx = i;
                continue;
            }

            GridQueryProperty prop = desc.type().property(cols[i].columnName());

            X.ensureX(prop != null, "Property '" + cols[i].columnName() + "' not found.");

            if (prop.key())
                hasKeyProps = true;
            else
                hasValProps = true;
        }

        Supplier keySupplier = createSupplier(cctx, desc.type(), keyColIdx, hasKeyProps, true);
        Supplier valSupplier = createSupplier(cctx, desc.type(), valColIdx, hasValProps, false);

        Iterable<List<?>> src;

        boolean singleRow = false;

        if (ins.query() != null)
            src = cursor;
        else {
            singleRow = ins.rows().size() == 1;
            src = rowsCursorToRows(cursor);
        }

        // If we have just one item to put, just do so
        if (singleRow) {
            IgniteBiTuple t = rowToKeyValue(cctx, desc.type(), keySupplier, valSupplier, keyColIdx, valColIdx, cols,
                src.iterator().next().toArray());

            if (cctx.cache().putIfAbsent(t.getKey(), t.getValue()))
                return 1;
            else
                throw createSqlException("Duplicate key during INSERT [key=" + t.getKey() + ']',
                    ErrorCode.DUPLICATE_KEY_1);
        }
        else {
            Map<Object, EntryProcessor<Object, Object, Boolean>> rows = new LinkedHashMap<>(ins.rows().size());

            for (List<?> row : src) {
                final IgniteBiTuple t = rowToKeyValue(cctx, desc.type(), keySupplier, valSupplier, keyColIdx, valColIdx,
                    cols, row.toArray());

                rows.put(t.getKey(), new InsertEntryProcessor(t.getValue()));
            }

            Map<Object, EntryProcessorResult<Boolean>> res = cctx.cache().invokeAll(rows);

            int duplicateKeys = 0;

            if (!F.isEmpty(res)) {
                SQLException resEx;

                IgniteBiTuple<Object[], SQLException> splitRes = splitErrors(res);

                // Everything left in errKeys is not erroneous keys, but duplicate keys
                if (!F.isEmpty(splitRes.get1())) {
                    duplicateKeys = splitRes.get1().length;

                    String msg = "Failed to INSERT some keys because they are already in cache " +
                        "[keys=" + Arrays.deepToString(splitRes.get1()) + ']';

                    resEx = new SQLException(msg, ErrorCode.getState(ErrorCode.DUPLICATE_KEY_1),
                        ErrorCode.DUPLICATE_KEY_1);

                    if (splitRes.get2() != null)
                        resEx.setNextException(splitRes.get2());
                }
                else
                    resEx = splitRes.get2();

                if (resEx != null)
                    throw new IgniteSQLException(resEx);
            }

            // Result will differ from rows.size() only in case of quiet mode with duplicate keys and no other exceptions
            return rows.size() - duplicateKeys;
        }
    }

    /**
     * @param cursor single "row" cursor that has arrays as columns, each array represents new inserted row.
     * @return List of lists, each representing new inserted row.
     */
    private static List<List<?>> rowsCursorToRows(QueryCursorEx<List<?>> cursor) {
        List<List<?>> newRowsRow = cursor.getAll();

        // We expect all new rows to be selected as single row with "columns" each of which is an array
        assert newRowsRow.size() == 1;

        List<?> newRowsList = newRowsRow.get(0);

        List<List<?>> newRows = new ArrayList<>(newRowsList.size());

        for (Object o : newRowsList) {
            assert o instanceof Object[];

            newRows.add(F.asList((Object[]) o));
        }

        return newRows;
    }

    /**
     * Convert bunch of {@link GridSqlElement}s into key-value pair to be inserted to cache.
     *
     * @param cctx Cache context.
     * @param desc Table descriptor.
     * @param keySupplier Key instantiation method.
     * @param valSupplier Key instantiation method.
     * @param keyColIdx Key column index, or {@code -1} if no key column is mentioned in {@code cols}.
     * @param valColIdx Value column index, or {@code -1} if no value column is mentioned in {@code cols}.
     * @param cols Query cols.
     * @param row Row to process.
     * @return Key-value pair.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private IgniteBiTuple<?, ?> rowToKeyValue(GridCacheContext cctx, GridQueryTypeDescriptor desc, Supplier keySupplier,
        Supplier valSupplier, int keyColIdx, int valColIdx, GridSqlColumn[] cols, Object[] row)
        throws IgniteCheckedException {

        Object key = keySupplier.apply(F.asList(row));
        Object val = valSupplier.apply(F.asList(row));

        if (key == null)
            throw createSqlException("Key for INSERT or MERGE must not be null", ErrorCode.NULL_NOT_ALLOWED);

        if (val == null)
            throw createSqlException("Value for INSERT or MERGE must not be null", ErrorCode.NULL_NOT_ALLOWED);

        for (int i = 0; i < cols.length; i++) {
            if (i == keyColIdx || i == valColIdx)
                continue;

            desc.setValue(cols[i].columnName(), key, val, row[i]);
        }

        if (cctx.binaryMarshaller()) {
            if (key instanceof BinaryObjectBuilder) {
                key = ((BinaryObjectBuilder) key).build();

                // TODO change/remove these statements when there's a way to generate hash codes for new binary objects
                // We have to deserialize for the object to have hash code,
                // but we do so only if we've constructed binary key ourselves.
                if (keyColIdx == -1)
                    key = ((BinaryObject) key).deserialize();
            }

            if (val instanceof BinaryObjectBuilder)
                val = ((BinaryObjectBuilder) val).build();
        }

        return new IgniteBiTuple<>(key, val);
    }

    /**
     * Detect appropriate method of instantiating key or value (take from param, create binary builder,
     * invoke default ctor, or allocate).
     *
     * @param cctx Cache context.
     * @param desc Table descriptor.
     * @param colIdx Column index if key or value is present in columns list, {@code -1} if it's not.
     * @param hasProps Whether column list affects individual properties of key or value.
     * @param key Whether supplier should be created for key or for value.
     * @return Closure returning key or value.
     * @throws IgniteCheckedException
     */
    @SuppressWarnings({"ConstantConditions", "unchecked"})
    private static Supplier createSupplier(final GridCacheContext<?, ?> cctx, GridQueryTypeDescriptor desc,
        final int colIdx, boolean hasProps, final boolean key) throws IgniteCheckedException {
        final String typeName = key ? desc.keyTypeName() : desc.valueTypeName();

        //Try to find class for the key locally.
        final Class<?> cls = key ? U.firstNotNull(U.classForName(desc.keyTypeName(), null), desc.keyClass())
            : desc.valueClass();

        boolean isSqlType = GridQueryProcessor.isSqlType(cls);

        // If we don't need to construct anything from scratch, just return value from array.
        if (isSqlType || !hasProps || !cctx.binaryMarshaller()) {
            if (colIdx != -1)
                return new Supplier() {
                    /** {@inheritDoc} */
                    @Override public Object apply(List<?> arg) throws IgniteCheckedException {
                        return arg.get(colIdx);
                    }
                };
            else if (isSqlType)
                // Non constructable keys and values (SQL types) must be present in the query explicitly.
                throw new IgniteCheckedException((key ? "Key" : "Value") + " is missing from query");
        }

        if (cctx.binaryMarshaller()) {
            // We can't (yet) generate a hash code for a class that is not present locally and
            // for which we don't have an explicitly set object in binary form.
            if (key && colIdx == -1 && (cls == Object.class || !U.overridesEqualsAndHashCode(cls)))
                throw new UnsupportedOperationException("Currently only local types with overridden equals/hashCode " +
                    "methods may be used in keys construction from fields in SQL DML operations w/binary marshalling.");

            if (colIdx != -1) {
                // If we have key or value explicitly present in query, create new builder upon them...
                return new Supplier() {
                    /** {@inheritDoc} */
                    @Override public Object apply(List<?> arg) throws IgniteCheckedException {
                        BinaryObject bin = cctx.grid().binary().toBinary(arg.get(colIdx));

                        return cctx.grid().binary().builder(bin);
                    }
                };
            }
            else {
                // ...and if we don't, just create a new builder.
                return new Supplier() {
                    /** {@inheritDoc} */
                    @Override public Object apply(List<?> arg) throws IgniteCheckedException {
                        return cctx.grid().binary().builder(typeName);
                    }
                };
            }
        }
        else {
            Constructor<?> ctor;

            try {
                ctor = cls.getDeclaredConstructor();
                ctor.setAccessible(true);
            }
            catch (NoSuchMethodException | SecurityException ignored) {
                ctor = null;
            }

            if (ctor != null) {
                final Constructor<?> ctor0 = ctor;

                // Use default ctor, if it's present...
                return new Supplier() {
                    /** {@inheritDoc} */
                    @Override public Object apply(List<?> arg) throws IgniteCheckedException {
                        try {
                            return ctor0.newInstance();
                        }
                        catch (Exception e) {
                            throw new IgniteCheckedException("Failed to invoke default ctor for " +
                                (key ? "key" : "value"), e);
                        }
                    }
                };
            }
            else {
                // ...or allocate new instance with unsafe, if it's not
                return new Supplier() {
                    /** {@inheritDoc} */
                    @Override public Object apply(List<?> arg) throws IgniteCheckedException {
                        try {
                            return GridUnsafe.allocateInstance(cls);
                        }
                        catch (InstantiationException e) {
                            throw new IgniteCheckedException("Failed to invoke default ctor for " +
                                (key ? "key" : "value"), e);
                        }
                    }
                };
            }
        }
    }

    /**
     * Gets value of this element, if it's a {@link GridSqlConst}, or gets a param by index if element
     * is a {@link GridSqlParameter}
     *
     * @param element SQL element.
     * @param params Params to use if {@code element} is a {@link GridSqlParameter}.
     * @return column value.
     */
    private static Object getElementValue(GridSqlElement element, Object[] params) throws IgniteCheckedException {
        if (element == null)
            return null;

        if (element instanceof GridSqlConst)
            return ((GridSqlConst)element).value().getObject();
        else if (element instanceof GridSqlParameter)
            return params[((GridSqlParameter)element).index()];
        else
            throw new IgniteCheckedException("Unexpected SQL expression type [cls=" + element.getClass().getName() + ']');
    }

    /**
     * Executes query.
     *
     * @param space Space.
     * @param qry Query.
     * @param params Query parameters.
     * @param tbl Target table of query to generate select.
     * @return Result set.
     * @throws IgniteCheckedException If failed.
     */
    private ResultSet executeQuery(String space, String qry, @Nullable Collection<Object> params, TableDescriptor tbl)
            throws IgniteCheckedException {
        Connection conn = connectionForThread(tbl.schemaName());

        String sql = generateQuery(qry, tbl);

        return executeSqlQueryWithTimer(space, conn, sql, params, true);
    }

    /**
     * Binds parameters to prepared statement.
     *
     * @param stmt Prepared statement.
     * @param params Parameters collection.
     * @throws IgniteCheckedException If failed.
     */
    private void bindParameters(PreparedStatement stmt, @Nullable Collection<Object> params) throws IgniteCheckedException {
        if (!F.isEmpty(params)) {
            int idx = 1;

            for (Object arg : params)
                bindObject(stmt, idx++, arg);
        }
    }

    /**
     * @param conn Connection.
     * @param enforceJoinOrder Enforce join order of tables.
     * @param filter Filter.
     */
    private void initLocalQueryContext(Connection conn, boolean enforceJoinOrder, IndexingQueryFilter filter) {
        setupConnection(conn, false, enforceJoinOrder);

        GridH2QueryContext.set(new GridH2QueryContext(nodeId, nodeId, 0, LOCAL).filter(filter).distributedJoins(false));
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
        final String qry, @Nullable final Collection<Object> params, GridQueryTypeDescriptor type,
        final IndexingQueryFilter filter) throws IgniteCheckedException {
        final TableDescriptor tbl = tableDescriptor(spaceName, type);

        if (tbl == null)
            throw createSqlException("Failed to find SQL table for type: " + type.name(), ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1);

        String sql = generateQuery(qry, tbl);

        Connection conn = connectionForThread(tbl.schemaName());

        initLocalQueryContext(conn, false, filter);

        try {
            ResultSet rs = executeSqlQueryWithTimer(spaceName, conn, sql, params, true);

            return new KeyValIterator(rs);
        }
        finally {
            GridH2QueryContext.clearThreadLocal();
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
        final boolean keepCacheObj, final boolean enforceJoinOrder) {
        return new Iterable<List<?>>() {
            @Override public Iterator<List<?>> iterator() {
                return rdcQryExec.query(cctx, qry, keepCacheObj, enforceJoinOrder);
            }
        };
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> QueryCursor<Cache.Entry<K,V>> queryTwoStep(GridCacheContext<?,?> cctx, SqlQuery qry)
        throws IgniteCheckedException {
        String type = qry.getType();
        String space = cctx.name();

        TableDescriptor tblDesc = tableDescriptor(type, space);

        if (tblDesc == null)
            throw createSqlException("Failed to find SQL table for type: " + type, ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1);

        String sql;

        try {
            sql = generateQuery(qry.getSql(), tblDesc);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        SqlFieldsQuery fqry = new SqlFieldsQuery(sql);

        fqry.setArgs(qry.getArgs());
        fqry.setPageSize(qry.getPageSize());
        fqry.setDistributedJoins(qry.isDistributedJoins());

        final QueryCursor<List<?>> res = queryTwoStep(cctx, fqry);

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
    @SuppressWarnings("unchecked")
    @Override public QueryCursor<List<?>> queryTwoStep(GridCacheContext<?,?> cctx, SqlFieldsQuery qry)
        throws IgniteCheckedException {
        Object[] errKeys = null;

        int items = 0;

        for (int i = 0; i < DFLT_DML_RERUN_ATTEMPTS; i++) {
            IgniteBiTuple<QueryCursorImpl<List<?>>, Object[]> r = queryTwoStep0(cctx, qry, errKeys);

            if (F.isEmpty(r.get2())) {
                if (items == 0)
                    return r.get1();
                else {
                    if (r.get1().isResultSet())
                        throw new IgniteSQLException("Unexpected result set in results of UPDATE or DELETE");

                    int cnt = (Integer) r.get1().getAll().get(0).get(0);
                    return QueryCursorImpl.forUpdateResult(items + cnt);
                }
            }
            else {
                if (r.get1().isResultSet())
                    throw new IgniteSQLException("Unexpected result set in results of UPDATE or DELETE");

                items += (Integer) r.get1().getAll().get(0).get(0);
                errKeys = r.get2();
            }
        }

        throw createSqlException("Failed to update or delete some keys: " + Arrays.deepToString(errKeys),
            ErrorCode.CONCURRENT_UPDATE_1);
    }

    /**
     * Actually perform two-step query.
     *
     * @param cctx Cache context.
     * @param qry Query.
     * @param failedKeys Keys to restrict UPDATE and DELETE operation with. Null or empty array means no restriction.
     * @return Pair [Query results cursor; keys that have failed to be processed (for UPDATE and DELETE only)]
     */
    private IgniteBiTuple<QueryCursorImpl<List<?>>, Object[]> queryTwoStep0(GridCacheContext<?,?> cctx, SqlFieldsQuery qry,
        Object[] failedKeys) throws IgniteCheckedException {
        final String space = cctx.name();
        final String sqlQry = qry.getSql();

        Connection c = connectionForSpace(space);

        final boolean enforceJoinOrder = qry.isEnforceJoinOrder();
        final boolean distributedJoins = qry.isDistributedJoins() && cctx.isPartitioned();
        final boolean grpByCollocated = qry.isCollocated();

        GridCacheTwoStepQuery twoStepQry;
        List<GridQueryFieldMetadata> meta;

        TwoStepCachedQuery cachedQry = null;
        TwoStepCachedQueryKey cachedQryKey = null;

        if (F.isEmpty(failedKeys)) {
            cachedQryKey = new TwoStepCachedQueryKey(space, sqlQry, grpByCollocated,
                distributedJoins, enforceJoinOrder);
            cachedQry = twoStepCache.get(cachedQryKey);
        }

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
                            catch (IgniteCheckedException e1) {
                                throw new CacheException("Failed to create missing caches.", e);
                            }

                            cachesCreated = true;
                        }
                        else
                            throw new IgniteSQLException("Failed to parse query: " + sqlQry, e.getSQLState(),
                                e.getErrorCode());
                    }
                }
            }
            finally {
                GridH2QueryContext.clearThreadLocal();
            }

            try {
                bindParameters(stmt, F.asList(qry.getArgs()));

                twoStepQry = GridSqlQuerySplitter.split((JdbcPreparedStatement)stmt, qry.getArgs(),
                    failedKeys, grpByCollocated, distributedJoins);

                //Let's verify (early) that the user is not trying to mess with the key or its fields directly
                if (twoStepQry.initialStatement() instanceof GridSqlUpdate)
                    verifyUpdateColumns((GridSqlUpdate) twoStepQry.initialStatement());

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

        if (twoStepQry.singleUpdate() != null) {
            int res = doSingleUpdate(cctx, twoStepQry.singleUpdate(), qry.getArgs());
            return new IgniteBiTuple<>(QueryCursorImpl.forUpdateResult(res), X.EMPTY_OBJECT_ARRAY);
        }

        if (log.isDebugEnabled())
            log.debug("Parsed query: `" + sqlQry + "` into two step query: " + twoStepQry);

        twoStepQry.pageSize(qry.getPageSize());

        Iterable<List<?>> qryRes = runQueryTwoStep(cctx, twoStepQry, cctx.keepBinary(), enforceJoinOrder);

        QueryCursorImpl<List<?>> cursor = new QueryCursorImpl<>(qryRes);

        cursor.fieldsMeta(meta);

        // Don't cache re-runs of DML operations - they have non-empty failedKeys
        if (cachedQry == null && !twoStepQry.explain() && cachedQryKey != null && F.isEmpty(failedKeys)) {
            cachedQry = new TwoStepCachedQuery(meta, twoStepQry.copy(null));
            twoStepCache.putIfAbsent(cachedQryKey, cachedQry);
        }

        X.ensureX(twoStepQry.initialStatement() != null, "Source statement undefined");

        IgniteBiTuple<Integer, Object[]> updateRes = null;

        if (twoStepQry.initialStatement() instanceof GridSqlDelete)
            updateRes = doDelete(cctx, cursor);

        if (twoStepQry.initialStatement() instanceof GridSqlUpdate)
            updateRes = doUpdate(cctx, (GridSqlUpdate) twoStepQry.initialStatement(), cursor);

        if (twoStepQry.initialStatement() instanceof GridSqlMerge)
            updateRes = new IgniteBiTuple<>(doMerge(cctx, (GridSqlMerge) twoStepQry.initialStatement(), cursor),
                X.EMPTY_OBJECT_ARRAY);

        if (twoStepQry.initialStatement() instanceof GridSqlInsert)
            updateRes = new IgniteBiTuple<>(doInsert(cctx, (GridSqlInsert) twoStepQry.initialStatement(), cursor
            ), X.EMPTY_OBJECT_ARRAY);

        if (updateRes != null)
            return new IgniteBiTuple<>(QueryCursorImpl.forUpdateResult(updateRes.get1() - updateRes.get2().length),
                updateRes.get2());

        return new IgniteBiTuple<>(cursor, null);
    }

    /**
     * Check that UPDATE statement affects no key columns.
     *
     * @param update Update statement.
     */
    private static void verifyUpdateColumns(GridSqlUpdate update) throws IgniteCheckedException {
        GridSqlElement updTarget = update.target();

        Set<GridSqlTable> tbls = new HashSet<>();

        GridSqlQuerySplitter.collectAllGridTablesInTarget(updTarget, tbls);

        if (tbls.size() != 1)
            throw createSqlException("Failed to determine target table for UPDATE", ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1);

        GridSqlTable tbl = tbls.iterator().next();

        GridH2Table gridTbl = tbl.dataTable();

        if (updateAffectsKeyColumns(gridTbl, update.set().keySet()))
            throw createSqlException("SQL UPDATE can't modify key or its fields directly", ErrorCode.COLUMN_NOT_FOUND_1);
    }

    /**
     * Check if given set of modified columns intersects with the set of SQL properties of the key.
     *
     * @param gridTbl Table.
     * @param affectedColNames Column names.
     * @return {@code true} if any of given columns corresponds to the key or any of its properties.
     */
    private static boolean updateAffectsKeyColumns(GridH2Table gridTbl, Set<String> affectedColNames) {
        GridH2RowDescriptor desc = gridTbl.rowDescriptor();

        Column[] cols = gridTbl.getColumns();

        // Check "_key" column itself - always has index of 0.
        if (affectedColNames.contains(cols[0].getName()))
            return true;

        // Start off from i = 2 to skip indices of 0 an 1 corresponding to key and value respectively.
        for (int i = 2; i < cols.length; i++)
           if (affectedColNames.contains(cols[i].getName()) && desc.isColumnKeyProperty(i - 2))
               return true;

        return false;
    }

    /**
     * Perform DELETE operation on top of results of SELECT.
     * @param cctx Cache context.
     * @param cursor SELECT results.
     * @return Results of DELETE (number of items affected AND keys that failed to be updated).
     */
    @SuppressWarnings("unchecked")
    private IgniteBiTuple<Integer, Object[]> doDelete(GridCacheContext cctx, QueryCursorImpl<List<?>> cursor)
        throws IgniteCheckedException {
        // With DELETE, we have only two columns - key and value.
        int res = 0;

        Iterator<List<?>> it = cursor.iterator();
        Map<Object, EntryProcessor<Object, Object, Boolean>> m = new HashMap<>();

        while (it.hasNext()) {
            List<?> e = it.next();
            if (e.size() != 2) {
                U.warn(log, "Invalid row size on DELETE - expected 2, got " + e.size());
                continue;
            }

            m.put(e.get(0), new ModifyingEntryProcessor(e.get(1), RMV));
            res++;
        }

        Map<Object, EntryProcessorResult<Boolean>> delRes = cctx.cache().invokeAll(m);

        if (F.isEmpty(delRes))
            return new IgniteBiTuple<>(res, X.EMPTY_OBJECT_ARRAY);

        SQLException resEx;

        IgniteBiTuple<Object[], SQLException> splitRes = splitErrors(delRes);

        if (splitRes.get2() == null)
            return new IgniteBiTuple<>(res, splitRes.get1());

        // Everything left in errKeys is not erroneous keys, but duplicate keys
        if (!F.isEmpty(splitRes.get1())) {
            resEx = new SQLException("Failed to UPDATE some keys because they were modified concurrently " +
                "[keys=" + Arrays.deepToString(splitRes.get1()) + ']',
                ErrorCode.getState(ErrorCode.CONCURRENT_UPDATE_1), ErrorCode.CONCURRENT_UPDATE_1);

            resEx.setNextException(splitRes.get2());
        }
        else
            resEx = splitRes.get2();

        throw new IgniteSQLException(resEx);
    }

    /**
     * Perform UPDATE operation on top of results of SELECT.
     * @param cctx Cache context.
     * @param update Update statement.
     * @param cursor SELECT results.
     * @return Cursor corresponding to results of UPDATE (contains number of items affected).
     */
    @SuppressWarnings("unchecked")
    private IgniteBiTuple<Integer, Object[]> doUpdate(GridCacheContext cctx, GridSqlUpdate update,
        QueryCursorImpl<List<?>> cursor) throws IgniteCheckedException {

        GridH2Table gridTbl = gridTableForElement(update.target());

        GridH2RowDescriptor desc = gridTbl.rowDescriptor();

        if (desc == null)
            throw createSqlException("Row descriptor undefined for table '" + gridTbl.getName() + "'",
                ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1);

        boolean bin = cctx.binaryMarshaller();

        Map<Object, EntryProcessor<Object, Object, Boolean>> m = new HashMap<>();

        Column[] cols = gridTbl.getColumns();

        List<GridSqlColumn> updatedCols = update.cols();

        int valColIdx = -1;

        for (int i = 0; i < updatedCols.size(); i++) {
            if (VAL_FIELD_NAME.equalsIgnoreCase(updatedCols.get(i).columnName())) {
                valColIdx = i;
                break;
            }
        }

        boolean hasNewVal = (valColIdx != -1);

        // Statement updates distinct properties if it does not have _val in updated columns list
        // or if its list of updated columns includes only _val, i.e. is single element.
        boolean hasProps = !hasNewVal || updatedCols.size() > 1;

        // Index of new _val in results of SELECT
        if (hasNewVal)
            valColIdx += 2;

        int newValColIdx;

        if (!hasProps) // No distinct properties, only whole new value - let's take it
            newValColIdx = valColIdx;
        else if (bin) // We update distinct columns in binary mode - let's choose correct index for the builder
            newValColIdx = (hasNewVal ? valColIdx : 1);
        else // Distinct properties, non binary mode - let's instantiate.
            newValColIdx = -1;

        // We want supplier to take present value only in case of binary mode as it will
        // otherwise we always want it to instantiate new object
        Supplier newValSupplier = createSupplier(cctx, desc.type(), newValColIdx, hasProps, false);

        int res = 0;

        for (List<?> e : cursor) {
            Object key = e.get(0);
            Object val = (hasNewVal ? e.get(valColIdx) : e.get(1));

            Object newVal;

            Map<String, Object> newColVals = new HashMap<>();

            for (int i = 0; i < updatedCols.size(); i++) {
                if (hasNewVal && i == valColIdx - 2)
                    continue;

                newColVals.put(updatedCols.get(i).columnName(), e.get(i + 2));
            }

            newVal = newValSupplier.apply(e);

            if (bin && !(val instanceof BinaryObject))
                val = cctx.grid().binary().toBinary(val);

            // Skip key and value - that's why we start off with 2nd column
            for (int i = 0; i < cols.length - 2; i++) {
                Column c = cols[i + 2];

                boolean hasNewColVal = newColVals.containsKey(c.getName());

                // Binary objects get old field values from the Builder, so we can skip what we're not updating
                if (bin && !hasNewColVal)
                    continue;

                Object colVal = hasNewColVal ? newColVals.get(c.getName()) : desc.columnValue(key, val, i);

                desc.setColumnValue(key, newVal, colVal, i);
            }

            if (bin && hasProps) {
                assert newVal instanceof BinaryObjectBuilder;

                newVal = ((BinaryObjectBuilder) newVal).build();
            }

            Object srcVal = e.get(1);

            if (bin && !(srcVal instanceof BinaryObject))
                srcVal = cctx.grid().binary().toBinary(srcVal);

            m.put(key, new ModifyingEntryProcessor(srcVal, new EntryValueUpdater(newVal)));

            res++;
        }

        CacheOperationContext opCtx = cctx.operationContextPerCall();

        if (bin) {
            CacheOperationContext newOpCtx = null;

            if (opCtx == null)
                // Mimics behavior of GridCacheAdapter#keepBinary and GridCacheProxyImpl#keepBinary
                newOpCtx = new CacheOperationContext(false, null, true, null, false, null);
            else if (!opCtx.isKeepBinary())
                newOpCtx = opCtx.keepBinary();

            if (newOpCtx != null)
                cctx.operationContextPerCall(newOpCtx);
        }

        try {
            Map<Object, EntryProcessorResult<Boolean>> updRes = cctx.cache().invokeAll(m);

            if (F.isEmpty(updRes))
                return new IgniteBiTuple<>(res, X.EMPTY_OBJECT_ARRAY);

            SQLException resEx;

            IgniteBiTuple<Object[], SQLException> splitRes = splitErrors(updRes);

            if (splitRes.get2() == null)
                return new IgniteBiTuple<>(res, splitRes.get1());

            // Everything left in errKeys is not erroneous keys, but duplicate keys
            if (!F.isEmpty(splitRes.get1())) {
                resEx = new SQLException("Failed to UPDATE some keys because they were modified concurrently " +
                    "[keys=" + Arrays.deepToString(splitRes.get1()) + ']',
                    ErrorCode.getState(ErrorCode.CONCURRENT_UPDATE_1), ErrorCode.CONCURRENT_UPDATE_1);

                resEx.setNextException(splitRes.get2());
            }
            else
                resEx = splitRes.get2();

            throw new IgniteSQLException(resEx);
        }
        finally {
            cctx.operationContextPerCall(opCtx);
        }
    }

    /**
     * Process errors of entry processor - split the keys into duplicated/concurrently modified and those whose
     * processing yielded an exception.
     *
     * @param res Result of {@link GridCacheAdapter#invokeAll)}
     * @return pair [array of duplicated/concurrently modified keys, SQL exception for erroneous keys] (exception is
     * null if all keys are duplicates/concurrently modified ones).
     */
    private static IgniteBiTuple<Object[], SQLException> splitErrors(Map<Object, EntryProcessorResult<Boolean>> res) {
        Set<Object> errKeys = new LinkedHashSet<>(res.keySet());

        SQLException currSqlEx = null;

        SQLException firstSqlEx = null;

        // Let's form a chain of SQL exceptions
        for (Map.Entry<Object, EntryProcessorResult<Boolean>> e : res.entrySet()) {
            try {
                e.getValue().get();
            }
            catch (EntryProcessorException ex) {
                SQLException next = new SQLException("Failed to INSERT key '" + e.getKey() + '\'', ex);

                if (currSqlEx != null)
                    currSqlEx.setNextException(next);
                else
                    firstSqlEx = next;

                currSqlEx = next;

                errKeys.remove(e.getKey());
            }


        }

        return new IgniteBiTuple<>(errKeys.toArray(), firstSqlEx);
    }

    /**
     * Prepares statement for query.
     *
     * @param qry Query string.
     * @param tbl Table to use.
     * @return Prepared statement.
     * @throws IgniteCheckedException In case of error.
     */
    private String generateQuery(String qry, TableDescriptor tbl) throws IgniteCheckedException {
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
            from = " FROM " + t +
                (upper.startsWith("WHERE") || upper.startsWith("ORDER") || upper.startsWith("LIMIT") ?
                " " : " WHERE ");

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
            if (name.equals(KEY_FIELD_NAME) || name.equals(VAL_FIELD_NAME))
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
    private static String escapeName(String name, boolean escapeAll) {
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

        if (dataTablesToTypes.putIfAbsent(res.identifier(), tbl.type().valueClass().getSimpleName()) != null)
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
     * @param target Expression to extract the table from.
     * @return Back end table for this element.
     */
    private GridH2Table gridTableForElement(GridSqlElement target) {
        Set<GridSqlTable> tbls = new HashSet<>();

        GridSqlQuerySplitter.collectAllGridTablesInTarget(target, tbls);

        if (tbls.size() != 1)
            throw createSqlException("Failed to determine target table", ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1);

        GridSqlTable tbl = tbls.iterator().next();

        return tbl.dataTable();
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

        ResultSet rs = executeSqlQuery(conn,
            "SELECT COUNT(*) FROM " + tbl.fullTableName(), null, false);

        try {
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

                WebServer webSrv = new WebServer();
                Server web = new Server(webSrv, "-webPort", "0");
                web.start();
                String url = webSrv.addSession(c);

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
            rdcQryExec = new GridReduceQueryExecutor(busyLock);

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
                    return marshaller.marshal(obj);
                }

                @Override public Object deserialize(byte[] bytes) throws Exception {
                    ClassLoader clsLdr = ctx != null ? U.resolveClassLoader(ctx.config()) : null;

                    return marshaller.unmarshal(bytes, clsLdr);
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
                        return aff.primary(locNode, k, topVer0);
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

            fullTblName = schema.schemaName + "." + escapeName(type.name(), schema.escapeAll());
        }

        /**
         * @return Schema name.
         */
        public String schemaName() {
            return schema.schemaName;
        }

        /**
         * @return Database table name.
         */
        String fullTableName() {
            return fullTblName;
        }

        /**
         * @return Database table name.
         */
        String name() {
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
    private static class SqlFieldMetadata implements GridQueryFieldMetadata {
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
            if (tbls.putIfAbsent(tbl.name(), tbl) != null)
                throw new IllegalStateException("Table already registered: " + tbl.name());
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

    /** */
    private final static class InsertEntryProcessor implements EntryProcessor<Object, Object, Boolean> {
        /** Value to set. */
        private final Object val;

        /** */
        private InsertEntryProcessor(Object val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Boolean process(MutableEntry<Object, Object> entry, Object... arguments) throws EntryProcessorException {
            if (entry.getValue() != null)
                return false;

            entry.setValue(val);
            return null; // To leave out only erroneous keys - nulls are skipped on results' processing.
        }
    }

    /**
     * Entry processor invoked by UPDATE and DELETE operations.
     */
    private final static class ModifyingEntryProcessor implements EntryProcessor<Object, Object, Boolean> {
        /** Value to expect. */
        private final Object val;

        /** Action to perform on entry. */
        private final IgniteInClosure<MutableEntry<Object, Object>> entryModifier;

        /** */
        private ModifyingEntryProcessor(Object val, IgniteInClosure<MutableEntry<Object, Object>> entryModifier) {
            this.val = val;
            this.entryModifier = entryModifier;
        }

        /** {@inheritDoc} */
        @Override public Boolean process(MutableEntry<Object, Object> entry, Object... arguments) throws EntryProcessorException {
            // Something happened to the cache while we were performing map-reduce.
            if (!F.eq(entry.getValue(), val))
                return false;

            entryModifier.apply(entry);
            return null; // To leave out only erroneous keys - nulls are skipped on results' processing.
        }
    }

    /** */
    private static IgniteInClosure<MutableEntry<Object, Object>> RMV = new IgniteInClosure<MutableEntry<Object, Object>>() {
        /** {@inheritDoc} */
        @Override public void apply(MutableEntry<Object, Object> e) {
            e.remove();
        }
    };

    /**
     *
     */
    private static final class EntryValueUpdater implements IgniteInClosure<MutableEntry<Object, Object>> {
        /** Value to set. */
        private final Object val;

        /** */
        private EntryValueUpdater(Object val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public void apply(MutableEntry<Object, Object> e) {
            e.setValue(val);
        }
    };

    /**
     * Method to construct new instances of keys and values on SQL MERGE and INSERT.
     */
    private interface Supplier extends GridPlainClosure<List<?>, Object> {
        // No-op.
    }

    /**
     * Create an {@link IgniteSQLException} bearing details meaningful to JDBC for more detailed
     * exceptions in the driver.
     *
     * @param msg Message.
     * @param code H2 status code.
     * @return {@link IgniteSQLException} with given details.
     * @see ErrorCode
     */
    private static IgniteSQLException createSqlException(String msg, int code) {
        return new IgniteSQLException(msg, ErrorCode.getState(code), code);
    }
}
