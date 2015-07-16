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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.processors.query.h2.opt.*;
import org.apache.ignite.internal.processors.query.h2.sql.*;
import org.apache.ignite.internal.processors.query.h2.twostep.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.offheap.unsafe.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.jdk.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.indexing.*;
import org.h2.api.*;
import org.h2.command.*;
import org.h2.constant.*;
import org.h2.index.*;
import org.h2.jdbc.*;
import org.h2.message.*;
import org.h2.mvstore.cache.*;
import org.h2.server.web.*;
import org.h2.table.*;
import org.h2.tools.*;
import org.h2.util.*;
import org.h2.value.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import javax.cache.Cache;
import javax.cache.*;
import java.io.*;
import java.lang.reflect.*;
import java.math.*;
import java.sql.*;
import java.sql.Date;
import java.text.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.internal.processors.query.GridQueryIndexType.*;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2AbstractKeyValueRow.*;
import static org.h2.result.SortOrder.*;

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
        ";DEFAULT_LOCK_TIMEOUT=10000;FUNCTIONS_IN_SCHEMA=true;OPTIMIZE_REUSE_RESULTS=0;QUERY_CACHE_SIZE=0;" +
        "RECOMPILE_ALWAYS=1;MAX_OPERATION_MEMORY=0";

    /** Field name for key. */
    public static final String KEY_FIELD_NAME = "_key";

    /** Field name for value. */
    public static final String VAL_FIELD_NAME = "_val";

    /** */
    private static final Field COMMAND_FIELD;

    /**
     * Command in H2 prepared statement.
     */
    static {
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
            }

            return c;
        }

        @Nullable @Override protected ConnectionWrapper initialValue() {
            Connection c;

            try {
                c = DriverManager.getConnection(dbUrl);
            }
            catch (SQLException e) {
                throw new IgniteException("Failed to initialize DB connection: " + dbUrl, e);
            }

            conns.add(c);

            return new ConnectionWrapper(c);
        }
    };

    /** */
    private volatile GridKernalContext ctx;

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

                stmt.executeUpdate("SET SCHEMA \"" + schema + '"');

                if (log.isDebugEnabled())
                    log.debug("Set schema: " + schema);

                c.schema(schema);
            }
            catch (SQLException e) {
                throw new IgniteCheckedException("Failed to set schema for DB connection for thread [schema=" +
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
        executeStatement("INFORMATION_SCHEMA", "CREATE SCHEMA IF NOT EXISTS \"" + schema + '"');

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
        executeStatement("INFORMATION_SCHEMA", "DROP SCHEMA IF EXISTS \"" + schema + '"');

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

            throw new IgniteCheckedException("Failed to execute statement: " + sql, e);
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
     * @return {@code true} If it is a portable object.
     */
    private boolean isPortable(CacheObject o) {
        if (ctx == null)
            return false;

        return ctx.cacheObjects().isPortableObject(o);
    }

    /**
     * @param coctx Cache object context.
     * @param o Object.
     * @return Object class.
     */
    private Class<?> getClass(CacheObjectContext coctx, CacheObject o) {
        return isPortable(o) ?
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

        Connection c = connectionForThread(tbl.schema());

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

            throw new IgniteCheckedException("Failed to drop database index table [type=" + tbl.type().name() +
                ", table=" + tbl.fullTableName() + "]", e);
        }
        finally {
            U.close(stmt, log);
        }

        tbl.tbl.close();

        if (tbl.luceneIdx != null)
            U.closeQuiet(tbl.luceneIdx);

        tbl.schema.tbls.remove(tbl.name());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryText(
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
    @Override public GridQueryFieldsResult queryFields(@Nullable final String spaceName, final String qry,
        @Nullable final Collection<Object> params, final IndexingQueryFilter filters)
        throws IgniteCheckedException {
        setFilters(filters);

        try {
            Connection conn = connectionForThread(schema(spaceName));

            ResultSet rs = executeSqlQueryWithTimer(spaceName, conn, qry, params);

            List<GridQueryFieldMetadata> meta = null;

            if (rs != null) {
                try {
                    meta = meta(rs.getMetaData());
                }
                catch (SQLException e) {
                    throw new IgniteCheckedException("Failed to get meta data.", e);
                }
            }

            return new GridQueryFieldsResultAdapter(meta, new FieldsIterator(rs));
        }
        finally {
            setFilters(null);
        }
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
     * Executes sql query.
     *
     * @param conn Connection,.
     * @param sql Sql query.
     * @param params Parameters.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    private ResultSet executeSqlQuery(Connection conn, String sql, Collection<Object> params)
        throws IgniteCheckedException {
        PreparedStatement stmt;

        try {
            stmt = conn.prepareStatement(sql);
        }
        catch (SQLException e) {
            throw new IgniteCheckedException("Failed to parse SQL query: " + sql, e);
        }

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
            throw new IgniteCheckedException("Failed to execute SQL query.", e);
        }
    }

    /**
     * Executes sql query and prints warning if query is too slow..
     *
     * @param conn Connection,.
     * @param sql Sql query.
     * @param params Parameters.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    public ResultSet executeSqlQueryWithTimer(String space, Connection conn, String sql,
        @Nullable Collection<Object> params) throws IgniteCheckedException {
        long start = U.currentTimeMillis();

        try {
            ResultSet rs = executeSqlQuery(conn, sql, params);

            long time = U.currentTimeMillis() - start;

            long longQryExecTimeout = schemas.get(schema(space)).ccfg.getLongQueryWarningTimeout();

            if (time > longQryExecTimeout) {
                String msg = "Query execution is too long (" + time + " ms): " + sql;

                ResultSet plan = executeSqlQuery(conn, "EXPLAIN " + sql, params);

                plan.next();

                // Add SQL explain result message into log.
                String longMsg = "Query execution is too long [time=" + time + " ms, sql='" + sql + '\'' +
                    ", plan=" + U.nl() + plan.getString(1) + U.nl() + ", parameters=" + params + "]";

                LT.warn(log, null, longMsg, msg);
            }

            return rs;
        }
        catch (SQLException e) {
            onSqlException();

            throw new IgniteCheckedException(e);
        }
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
    private ResultSet executeQuery(String space, String qry, @Nullable Collection<Object> params,
        TableDescriptor tbl) throws IgniteCheckedException {
        Connection conn = connectionForThread(tbl.schema());

        String sql = generateQuery(qry, tbl);

        return executeSqlQueryWithTimer(space, conn, sql, params);
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
     * Executes regular query.
     * Note that SQL query can not refer to table alias, so use full table name instead.
     *
     * @param spaceName Space name.
     * @param qry Query.
     * @param params Query parameters.
     * @param type Query return type.
     * @param filters Space name and key filters.
     * @return Queried rows.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    @Override public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> query(@Nullable String spaceName,
        final String qry, @Nullable final Collection<Object> params, GridQueryTypeDescriptor type,
        final IndexingQueryFilter filters) throws IgniteCheckedException {
        final TableDescriptor tbl = tableDescriptor(spaceName, type);

        if (tbl == null)
            throw new CacheException("Failed to find SQL table for type: " + type.name());

        setFilters(filters);

        try {
            ResultSet rs = executeQuery(spaceName, qry, params, tbl);

            return new KeyValIterator(rs);
        }
        finally {
            setFilters(null);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterable<List<?>> queryTwoStep(final GridCacheContext<?,?> cctx, final GridCacheTwoStepQuery qry,
        final boolean keepCacheObj) {
        return new Iterable<List<?>>() {
            @Override public Iterator<List<?>> iterator() {
                return rdcQryExec.query(cctx, qry, keepCacheObj);
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
            throw new CacheException("Failed to find SQL table for type: " + type);

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

    /** {@inheritDoc} */
    @Override public QueryCursor<List<?>> queryTwoStep(GridCacheContext<?,?> cctx, SqlFieldsQuery qry) {
        String space = cctx.name();
        String sqlQry = qry.getSql();

        Connection c = connectionForSpace(space);

        PreparedStatement stmt;

        try {
            stmt = c.prepareStatement(sqlQry);
        }
        catch (SQLException e) {
            throw new CacheException("Failed to parse query: " + sqlQry, e);
        }

        GridCacheTwoStepQuery twoStepQry;
        List<GridQueryFieldMetadata> meta;

        try {
            twoStepQry = GridSqlQuerySplitter.split((JdbcPreparedStatement)stmt, qry.getArgs(), qry.isCollocated());

            meta = meta(stmt.getMetaData());
        }
        catch (SQLException e) {
            throw new CacheException(e);
        }
        finally {
            U.close(stmt, log);
        }

        if (log.isDebugEnabled())
            log.debug("Parsed query: `" + sqlQry + "` into two step query: " + twoStepQry);

        twoStepQry.pageSize(qry.getPageSize());

        QueryCursorImpl<List<?>> cursor = new QueryCursorImpl<>(queryTwoStep(cctx, twoStepQry, cctx.keepPortable()));

        cursor.fieldsMeta(meta);

        return cursor;
    }

    /**
     * Sets filters for current thread. Must be set to not null value
     * before executeQuery and reset to null after in finally block since it signals
     * to table that it should return content without expired values.
     *
     * @param filters Filters.
     */
    public void setFilters(@Nullable IndexingQueryFilter filters) {
        GridH2IndexBase.setFiltersForThread(filters);
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

            if (!qry.startsWith("*"))
                throw new IgniteCheckedException("Only queries starting with 'SELECT *' are supported or " +
                    "use SqlFieldsQuery instead: " + qry0);

            qry = qry.substring(1).trim();
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

        String ptrn = "Name ''{0}'' is reserved and cannot be used as a field name [class=" + type + "]";

        for (String name : names) {
            if (name.equals(KEY_FIELD_NAME) || name.equals(VAL_FIELD_NAME))
                throw new IgniteCheckedException(MessageFormat.format(ptrn, name));
        }

        return true;
    }

    /**
     * Escapes name to be valid SQL identifier. Currently just replaces '.' and '$' sign with '_'.
     *
     * @param name Name.
     * @param escapeAll Escape flag.
     * @return Escaped name.
     */
    private static String escapeName(String name, boolean escapeAll) {
        if (escapeAll)
            return "\"" + name + "\"";

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

        GridH2Table.Engine.createTable(conn, sql.toString(), desc, tbl, tbl.schema.spaceName);
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
     * @param space Space name.
     * @return Schema name.
     */
    public static String schema(@Nullable String space) {
        if (space == null)
            return "";

        return space;
    }

    /**
     * @param schema Schema.
     * @return Space name.
     */
    public static String space(String schema) {
        assert schema != null;

        return "".equals(schema) ? null : schema;
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

    /** {@inheritDoc} */
    @Override public long size(@Nullable String spaceName, GridQueryTypeDescriptor type,
        IndexingQueryFilter filters) throws IgniteCheckedException {
        TableDescriptor tbl = tableDescriptor(spaceName, type);

        if (tbl == null)
            return -1;

        IgniteSpiCloseableIterator<List<?>> iter = queryFields(spaceName,
            "SELECT COUNT(*) FROM " + tbl.fullTableName(), null, null).iterator();

        return ((Number)iter.next().get(0)).longValue();
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

        System.setProperty("h2.serializeJavaObject", "false");
        System.setProperty("h2.objectCacheMaxPerElementSize", "0"); // Avoid ValueJavaObject caching.

        if (SysProperties.serializeJavaObject) {
            U.warn(log, "Serialization of Java objects in H2 was enabled.");

            SysProperties.serializeJavaObject = false;
        }

        if (Utils.serializer != null)
            U.warn(log, "Custom H2 serialization is already configured, will override.");

        Utils.serializer = h2Serializer();

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

        if (ctx == null) // This is allowed in some tests.
            marshaller = new JdkMarshaller();
        else {
            this.ctx = ctx;

            nodeId = ctx.localNodeId();
            marshaller = ctx.config().getMarshaller();

            mapQryExec = new GridMapQueryExecutor(busyLock);
            rdcQryExec = new GridReduceQueryExecutor(busyLock);

            mapQryExec.start(ctx, this);
            rdcQryExec.start(ctx, this);
        }

        // TODO https://issues.apache.org/jira/browse/IGNITE-751
        // registerMBean(gridName, this, GridH2IndexingSpiMBean.class);
    }

    /**
     * @return Serializer.
     */
    protected JavaObjectSerializer h2Serializer() {
        return new JavaObjectSerializer() {
                @Override public byte[] serialize(Object obj) throws Exception {
                    return marshaller.marshal(obj);
                }

                @Override public Object deserialize(byte[] bytes) throws Exception {
                    return marshaller.unmarshal(bytes, null);
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

//        unregisterMBean(); TODO

        for (Schema schema : schemas.values()) {
            for (TableDescriptor desc : schema.tbls.values()) {
                desc.tbl.close();

                if (desc.luceneIdx != null)
                    U.closeQuiet(desc.luceneIdx);
            }
        }

        executeStatement("INFORMATION_SCHEMA", "SHUTDOWN");

        for (Connection c : conns)
            U.close(c, log);

        conns.clear();
        schemas.clear();

        if (log.isDebugEnabled())
            log.debug("Cache query index stopped.");
    }

    /** {@inheritDoc} */
    @Override public void registerCache(CacheConfiguration<?,?> ccfg) throws IgniteCheckedException {
        String schema = schema(ccfg.getName());

        if (schemas.putIfAbsent(schema, new Schema(ccfg.getName(),
            ccfg.getOffHeapMaxMemory() >= 0 || ccfg.getMemoryMode() == CacheMemoryMode.OFFHEAP_TIERED ?
            new GridUnsafeMemory(0) : null, ccfg)) != null)
            throw new IgniteCheckedException("Cache already registered: " + U.maskName(ccfg.getName()));

        createSchema(schema);

        executeStatement(schema, "CREATE ALIAS " + GridSqlQuerySplitter.TABLE_FUNC_NAME +
            " NOBUFFER FOR \"" + GridReduceQueryExecutor.class.getName() + ".mergeTableFunction\"");

        createSqlFunctions(schema, ccfg.getSqlFunctionClasses());
    }

    /** {@inheritDoc} */
    @Override public void unregisterCache(CacheConfiguration<?, ?> ccfg) {
        String schema = schema(ccfg.getName());

        Schema rmv = schemas.remove(schema);

        if (rmv != null) {
            mapQryExec.onCacheStop(ccfg.getName());

            try {
                dropSchema(schema);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to drop schema on cache stop (will ignore): " + U.maskName(ccfg.getName()), e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public IndexingQueryFilter backupFilter(
        @Nullable final List<String> caches,
        @Nullable final AffinityTopologyVersion topVer,
        @Nullable final int[] parts
    ) {
        final AffinityTopologyVersion topVer0 = topVer != null ? topVer : AffinityTopologyVersion.NONE;

        return new IndexingQueryFilter() {
            @Nullable @Override public <K, V> IgniteBiPredicate<K, V> forSpace(String spaceName) {
                final GridCacheAdapter<Object, Object> cache = ctx.cache().internalCache(spaceName);

                if (cache.context().isReplicated() || (cache.configuration().getBackups() == 0 && parts == null))
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

    /** {@inheritDoc} */
    @Override public boolean isSqlType(Class<?> cls) {
        switch (DBTypeEnum.fromClass(cls)) {
            case OTHER:
            case ARRAY:
                return false;

            default:
                return true;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isGeometryClass(Class<?> cls) {
        return DataType.isGeometryClass(cls);
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
            map.put(char.class, CHAR);
            map.put(Character.class, CHAR);
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

            fullTblName = '\"' + IgniteH2Indexing.schema(schema.spaceName) + "\"." +
                escapeName(type.name(), schema.escapeAll());
        }

        /**
         * @return Schema name.
         */
        public String schema() {
            return IgniteH2Indexing.schema(schema.spaceName);
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

            idxs.add(new GridH2TreeIndex("_key_PK", tbl, true, KEY_COL, VAL_COL, tbl.indexColumn(0, ASCENDING)));

            if (type().valueClass() == String.class) {
                try {
                    luceneIdx = new GridLuceneIndex(ctx, schema.offheap, schema.spaceName, type);
                }
                catch (IgniteCheckedException e1) {
                    throw new IgniteException(e1);
                }
            }

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
                    IndexColumn[] cols = new IndexColumn[idx.fields().size()];

                    int i = 0;

                    boolean escapeAll = schema.escapeAll();

                    for (String field : idx.fields()) {
                        // H2 reserved keywords used as column name is case sensitive.
                        String fieldName = escapeAll ? field : escapeName(field, false).toUpperCase();

                        Column col = tbl.getColumn(fieldName);

                        cols[i++] = tbl.indexColumn(col.getColumnId(), idx.descending(field) ? DESCENDING : ASCENDING);
                    }

                    if (idx.type() == SORTED)
                        idxs.add(new GridH2TreeIndex(name, tbl, false, KEY_COL, VAL_COL, cols));
                    else if (idx.type() == GEO_SPATIAL)
                        idxs.add(createH2SpatialIndex(tbl, name, cols, KEY_COL, VAL_COL));
                    else
                        throw new IllegalStateException();
                }
            }

            return idxs;
        }

        /**
         * @param tbl Table.
         * @param idxName Index name.
         * @param cols Columns.
         * @param keyCol Key column.
         * @param valCol Value column.
         */
        private SpatialIndex createH2SpatialIndex(
            Table tbl,
            String idxName,
            IndexColumn[] cols,
            int keyCol,
            int valCol
        ) {
            String className = "org.apache.ignite.internal.processors.query.h2.opt.GridH2SpatialIndex";

            try {
                Class<?> cls = Class.forName(className);

                Constructor<?> ctor = cls.getConstructor(
                    Table.class,
                    String.class,
                    IndexColumn[].class,
                    int.class,
                    int.class);

                if (!ctor.isAccessible())
                    ctor.setAccessible(true);

                return (SpatialIndex)ctor.newInstance(tbl, idxName, cols, keyCol, valCol);
            }
            catch (Exception e) {
                throw new IgniteException("Failed to instantiate: " + className, e);
            }
        }
    }

    /**
     * Special field set iterator based on database result set.
     */
    private static class FieldsIterator extends GridH2ResultSetIterator<List<?>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param data Data.
         * @throws IgniteCheckedException If failed.
         */
        protected FieldsIterator(ResultSet data) throws IgniteCheckedException {
            super(data);
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
            super(data);
        }

        /** {@inheritDoc} */
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
            assert name != null;
            assert type != null;

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
    private static class Schema {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final String spaceName;

        /** */
        private final GridUnsafeMemory offheap;

        /** */
        private final ConcurrentMap<String, TableDescriptor> tbls = new ConcurrentHashMap8<>();

        /** Cache for deserialized offheap rows. */
        private final CacheLongKeyLIRS<GridH2KeyValueRowOffheap> rowCache;

        /** */
        private final CacheConfiguration<?,?> ccfg;

        /**
         * @param spaceName Space name.
         * @param offheap Offheap memory.
         * @param ccfg Cache configuration.
         */
        private Schema(@Nullable String spaceName, GridUnsafeMemory offheap, CacheConfiguration<?,?> ccfg) {
            this.spaceName = spaceName;
            this.offheap = offheap;
            this.ccfg = ccfg;

            if (offheap != null)
                rowCache = new CacheLongKeyLIRS<>(ccfg.getSqlOnheapRowCacheSize(), 1, 128, 256);
            else
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
        @Override public IgniteH2Indexing owner() {
            return IgniteH2Indexing.this;
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

                    return GridH2Utils.toValueTimestamp((Timestamp)obj);
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
                    return new GridH2Row(wrap(key, keyType), null);

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

            GridCacheSwapEntry e = cctx.swap().read(cctx.toCacheKeyObject(key), true, true);

            if (e == null)
                return null;

            CacheObject v = e.value();

            assert v != null : "swap must unmarshall it for us";

            return v.value(cctx.cacheObjectContext(), false);
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
                return type.value(fields[col], key, val);
            }
            catch (IgniteCheckedException e) {
                throw DbException.convert(e);
            }
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
    }
}
