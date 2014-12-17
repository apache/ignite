/* @java.file.header */

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.kernal.processors.query.h2;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.indexing.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.query.*;
import org.gridgain.grid.kernal.processors.query.h2.opt.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
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
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.lang.reflect.*;
import java.math.*;
import java.sql.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.gridgain.grid.kernal.processors.query.GridQueryIndexType.*;
import static org.gridgain.grid.kernal.processors.query.h2.opt.GridH2AbstractKeyValueRow.*;
import static org.h2.result.SortOrder.*;

/**
 * Indexing implementation based on H2 database engine. In this implementation main query language is SQL,
 * fulltext indexing can be performed using Lucene. For each registered space
 * the SPI will create respective schema, for default space (where space name is null) schema
 * with name {@code PUBLIC} will be used. To avoid name conflicts user should not explicitly name
 * a schema {@code PUBLIC}.
 * <p>
 * For each registered {@link GridQueryTypeDescriptor} this SPI will create respective SQL table with
 * {@code '_key'} and {@code '_val'} fields for key and value, and fields from
 * {@link GridQueryTypeDescriptor#keyFields()} and {@link GridQueryTypeDescriptor#valueFields()}.
 * For each table it will create indexes declared in {@link GridQueryTypeDescriptor#indexes()}.
 * <h1 class="header">Some important defaults.</h1>
 * <ul>
 *     <li>All the data will be kept in memory</li>
 *     <li>Primitive types will not be indexed (e.g. java types which can be directly converted to SQL types)</li>
 *     <li>
 *         Key types will be converted to SQL types, so it is impossible to store one value type with
 *         different key types
 *     </li>
 * </ul>
 * @see GridIndexingSpi
 */
@SuppressWarnings({"UnnecessaryFullyQualifiedName", "NonFinalStaticVariableUsedInClassInitialization"})
public class GridH2Indexing implements GridQueryIndexing {
    /** Default DB options. */
    private static final String DFLT_DB_OPTIONS = ";LOCK_MODE=3;MULTI_THREADED=1;DB_CLOSE_ON_EXIT=FALSE" +
        ";DEFAULT_LOCK_TIMEOUT=10000";

    /** Options for optimized mode to work properly. */
    private static final String OPTIMIZED_DB_OPTIONS = ";OPTIMIZE_REUSE_RESULTS=0;QUERY_CACHE_SIZE=0;" +
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

    /** */
    private static final ThreadLocal<GridH2Indexing> localSpi = new ThreadLocal<>();

    /** */
    private volatile String cachedSearchPathCmd;

    /** Cache for deserialized offheap rows. */
    private CacheLongKeyLIRS<GridH2KeyValueRowOffheap> rowCache = new CacheLongKeyLIRS<>(32 * 1024, 1, 128, 256);

    /** Logger. */
    @IgniteLoggerResource
    private IgniteLogger log;

    /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** */
    private GridUnsafeMemory offheap;

    /** */
    private final Collection<String> schemaNames = new GridConcurrentHashSet<>();

    /** Collection of schemaNames and registered tables. */
    private final ConcurrentMap<String, Schema> schemas = new ConcurrentHashMap8<>();

    /** */
    private String dbUrl = "jdbc:h2:mem:";

    /** */
    private final Collection<Connection> conns = Collections.synchronizedCollection(new ArrayList<Connection>());

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
            Connection c = null;

            try {
                c = DriverManager.getConnection(dbUrl);

                String[] searchPath = cfg.getSearchPath();

                if (!F.isEmpty(searchPath)) {
                    try (Statement s = c.createStatement()) {
                        String cmd = cachedSearchPathCmd;

                        if (cmd == null) {
                            SB b = new SB("SET SCHEMA_SEARCH_PATH ");

                            for (int i = 0; i < searchPath.length; i++) {
                                if (i != 0)
                                    b.a(',');

                                b.a('"').a(schema(searchPath[i])).a('"');
                            }

                            cachedSearchPathCmd = cmd = b.toString();
                        }

                        s.executeUpdate(cmd);
                    }
                }

                conns.add(c);

                return new ConnectionWrapper(c);
            }
            catch (SQLException e) {
                U.close(c, log);

                throw new IgniteException("Failed to initialize DB connection: " + dbUrl, e);
            }
        }
    };

    /** */
    private volatile GridQueryConfiguration cfg = new GridQueryConfiguration();

    /** */
    private volatile GridKernalContext ctx;

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
                    log.debug("Initialized H2 schema for queries on space: " + schema);

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
    private void createSchemaIfAbsent(String schema) throws IgniteCheckedException {
        executeStatement("CREATE SCHEMA IF NOT EXISTS \"" + schema + '"');

        if (log.isDebugEnabled())
            log.debug("Created H2 schema for index database: " + schema);
    }

    /**
     * @param sql SQL statement.
     * @throws IgniteCheckedException If failed.
     */
    private void executeStatement(String sql) throws IgniteCheckedException {
        Statement stmt = null;

        try {
            Connection c = connectionForThread(null);

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
    private void removeKey(@Nullable String spaceName, Object key, TableDescriptor tblToUpdate)
        throws IgniteCheckedException {
        try {
            Collection<TableDescriptor> tbls = tables(schema(spaceName));

            if (tbls.size() > 1) {
                boolean fixedTyping = isIndexFixedTyping(spaceName);

                for (TableDescriptor tbl : tbls) {
                    if (tbl != tblToUpdate && (tbl.type().keyClass().equals(key.getClass()) ||
                        !fixedTyping)) {
                        if (tbl.tbl.update(key, null, 0)) {
                            if (tbl.luceneIdx != null)
                                tbl.luceneIdx.remove(key);

                            return;
                        }
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
            throw new IgniteCheckedException("Failed to bind parameter [idx=" + idx + ", obj=" + obj + ']', e);
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
    @Override public void store(@Nullable String spaceName, GridQueryTypeDescriptor type, Object k, Object v, byte[] ver,
        long expirationTime) throws IgniteCheckedException {
        TableDescriptor tbl = tableDescriptor(spaceName, type);

        if (tbl == null)
            return; // Type was rejected.

        localSpi.set(this);

        try {
            removeKey(spaceName, k, tbl);

            if (expirationTime == 0)
                expirationTime = Long.MAX_VALUE;

            tbl.tbl.update(k, v, expirationTime);

            if (tbl.luceneIdx != null)
                tbl.luceneIdx.store(k, v, ver, expirationTime);
        }
        finally {
            localSpi.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable String spaceName, Object key) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Removing key from cache query index [locId=" + ignite.configuration().getNodeId() +
                ", key=" + key + ']');

        localSpi.set(this);

        try {
            for (TableDescriptor tbl : tables(schema(spaceName))) {
                if (tbl.type().keyClass().equals(key.getClass()) || !isIndexFixedTyping(spaceName)) {
                    if (tbl.tbl.update(key, null, 0)) {
                        if (tbl.luceneIdx != null)
                            tbl.luceneIdx.remove(key);

                        return;
                    }
                }
            }
        }
        finally {
            localSpi.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public void onSwap(@Nullable String spaceName, Object key) throws IgniteCheckedException {
        Schema schema = schemas.get(schema(spaceName));

        if (schema == null)
            return;

        localSpi.set(this);

        try {
            for (TableDescriptor tbl : schema.values()) {
                if (tbl.type().keyClass().equals(key.getClass()) || !isIndexFixedTyping(spaceName)) {
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
        finally {
            localSpi.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public void onUnswap(@Nullable String spaceName, Object key, Object val, byte[] valBytes)
        throws IgniteCheckedException {
        localSpi.set(this);

        try {
            for (TableDescriptor tbl : tables(schema(spaceName))) {
                if (tbl.type().keyClass().equals(key.getClass()) || !isIndexFixedTyping(spaceName)) {
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
        finally {
            localSpi.remove();
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

        Connection c = connectionForThread(null);

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

        ConcurrentMap<String, TableDescriptor> tbls = schemas.get(tbl.schema());

        if (!F.isEmpty(tbls))
            tbls.remove(tbl.name());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryText(
        @Nullable String spaceName, String qry, GridQueryTypeDescriptor type,
        GridIndexingQueryFilter filters) throws IgniteCheckedException {
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
    @Override public <K, V> GridQueryFieldsResult queryFields(@Nullable final String spaceName, final String qry,
        @Nullable final Collection<Object> params, final GridIndexingQueryFilter filters)
        throws IgniteCheckedException {
        localSpi.set(this);

        setFilters(filters);

        try {
            Connection conn = connectionForThread(schema(spaceName));

            ResultSet rs = executeSqlQueryWithTimer(conn, qry, params);

            List<GridQueryFieldMetadata> meta = null;

            if (rs != null) {
                try {
                    ResultSetMetaData rsMeta = rs.getMetaData();

                    meta = new ArrayList<>(rsMeta.getColumnCount());

                    for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
                        String schemaName = rsMeta.getSchemaName(i);
                        String typeName = rsMeta.getTableName(i);
                        String name = rsMeta.getColumnLabel(i);
                        String type = rsMeta.getColumnClassName(i);

                        meta.add(new SqlFieldMetadata(schemaName, typeName, name, type));
                    }
                }
                catch (SQLException e) {
                    throw new IgniteSpiException("Failed to get meta data.", e);
                }
            }

            return new GridQueryFieldsResultAdapter(meta, new FieldsIterator(rs));
        }
        finally {
            setFilters(null);

            localSpi.remove();
        }
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
     * @return Configuration.
     */
    public GridQueryConfiguration configuration() {
        return cfg;
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
    @Nullable private ResultSet executeSqlQuery(Connection conn, String sql,
        @Nullable Collection<Object> params) throws IgniteCheckedException {
        PreparedStatement stmt;

        try {
            stmt = conn.prepareStatement(sql);
        }
        catch (SQLException e) {
            if (e.getErrorCode() == ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1)
                return null;

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
    private ResultSet executeSqlQueryWithTimer(Connection conn, String sql,
        @Nullable Collection<Object> params) throws IgniteCheckedException {
        long start = U.currentTimeMillis();

        try {
            ResultSet rs = executeSqlQuery(conn, sql, params);

            long time = U.currentTimeMillis() - start;

            long longQryExecTimeout = cfg.getLongQueryExecutionTimeout();

            if (time > longQryExecTimeout) {
                String msg = "Query execution is too long (" + time + " ms): " + sql;

                String longMsg = msg;

                if (cfg.isLongQueryExplain()) {
                    ResultSet plan = executeSqlQuery(conn, "EXPLAIN " + sql, params);

                    if (plan == null)
                        longMsg = "Failed to explain plan because required table does not exist: " + sql;
                    else {
                        plan.next();

                        // Add SQL explain result message into log.
                        longMsg = "Query execution is too long [time=" + time + " ms, sql='" + sql + '\'' +
                            ", plan=" + U.nl() + plan.getString(1) + U.nl() + ", parameters=" + params + "]";
                    }
                }

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
     * @param qry Query.
     * @param params Query parameters.
     * @param tbl Target table of query to generate select.
     * @return Result set.
     * @throws IgniteCheckedException If failed.
     */
    private ResultSet executeQuery(String qry, @Nullable Collection<Object> params,
        @Nullable TableDescriptor tbl) throws IgniteCheckedException {
        Connection conn = connectionForThread(tbl != null ? tbl.schema() : "PUBLIC");

        String sql = generateQuery(qry, tbl);

        return executeSqlQueryWithTimer(conn, sql, params);
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
        final GridIndexingQueryFilter filters) throws IgniteCheckedException {
        final TableDescriptor tbl = tableDescriptor(spaceName, type);

        if (tbl == null)
            return new GridEmptyCloseableIterator<>();

        setFilters(filters);

        localSpi.set(this);

        try {
            ResultSet rs = executeQuery(qry, params, tbl);

            return new KeyValIterator(rs);
        }
        finally {
            setFilters(null);

            localSpi.remove();
        }
    }

    /**
     * Sets filters for current thread. Must be set to not null value
     * before executeQuery and reset to null after in finally block since it signals
     * to table that it should return content without expired values.
     *
     * @param filters Filters.
     */
    private void setFilters(@Nullable GridIndexingQueryFilter filters) {
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
    private String generateQuery(String qry, @Nullable TableDescriptor tbl) throws IgniteCheckedException {
        boolean needSelect = tbl != null;

        String str = qry.trim().toUpperCase();

        if (!str.startsWith("FROM")) {
            if (str.startsWith("SELECT")) {
                if (needSelect) {
                    StringTokenizer st = new StringTokenizer(str, " ");

                    String errMsg = "Wrong query format, query must start with 'select * from' " +
                        "or 'from' or without such keywords.";

                    if (st.countTokens() > 3) {
                        st.nextToken();
                        String wildcard = st.nextToken();
                        String from = st.nextToken();

                        if (!"*".equals(wildcard) || !"FROM".equals(from))
                            throw new IgniteCheckedException(errMsg);

                        needSelect = false;
                    }
                    else
                        throw new IgniteCheckedException(errMsg);
                }
            }
            else {
                boolean needWhere = !str.startsWith("ORDER") && !str.startsWith("LIMIT");

                qry = needWhere ? "FROM " + tbl.fullTableName() + " WHERE " + qry :
                    "FROM " + tbl.fullTableName() + ' ' + qry;
            }
        }

        GridStringBuilder ptrn = new SB("SELECT {0}.").a(KEY_FIELD_NAME);

        ptrn.a(", {0}.").a(VAL_FIELD_NAME);

        return needSelect ? MessageFormat.format(ptrn.toString(), tbl.fullTableName()) + ' ' + qry : qry;
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
        if (!validateTypeDescriptor(spaceName, type))
            return false;

        for (TableDescriptor table : tables(schema(spaceName)))
            // Need to compare class names rather than classes to define
            // whether a class was previously undeployed.
            if (table.type().valueClass().getClass().getName().equals(type.valueClass().getName()))
                throw new IgniteCheckedException("Failed to register type in query index because" +
                    " class is already registered (most likely that class with the same name" +
                    " was not properly undeployed): " + type);

        TableDescriptor tbl = new TableDescriptor(spaceName, type);

        try {
            Connection conn = connectionForThread(null);

            Schema schema = schemas.get(tbl.schema());

            if (schema == null) {
                schema = new Schema(spaceName);

                Schema existing = schemas.putIfAbsent(tbl.schema(), schema);

                if (existing != null)
                    schema = existing;
            }

            createTable(schema, tbl, conn);

            schema.put(tbl.name(), tbl);
        }
        catch (SQLException e) {
            onSqlException();

            throw new IgniteCheckedException("Failed to register query type: " + type, e);
        }

        return true;
    }

    /**
     * @param cls Class.
     * @return True if given class has primitive respective sql type.
     */
    private boolean isPrimitive(Class<?> cls) {
        DBTypeEnum valType = DBTypeEnum.fromClass(cls);

        return valType != DBTypeEnum.BINARY && valType != DBTypeEnum.OTHER &&
            valType != DBTypeEnum.ARRAY;
    }

    /**
     * Validates properties described by query types.
     *
     * @param spaceName Space name.
     * @param type Type descriptor.
     * @return True if type is valid.
     * @throws IgniteCheckedException If validation failed.
     */
    private boolean validateTypeDescriptor(@Nullable String spaceName, GridQueryTypeDescriptor type)
        throws IgniteCheckedException {
        assert type != null;

        boolean keyPrimitive = isPrimitive(type.keyClass());
        boolean valPrimitive = isPrimitive(type.valueClass());

        // Do not register if value is not primitive and
        // there are no indexes or fields defined.
        if (!type.valueTextIndex() && type.indexes().isEmpty() &&
            type.keyFields().isEmpty() && type.valueFields().isEmpty())
            return keyPrimitive && isIndexPrimitiveKey(spaceName) || valPrimitive && isIndexPrimitiveValue(spaceName);

        Collection<String> names = new HashSet<>();

        names.addAll(type.keyFields().keySet());
        names.addAll(type.valueFields().keySet());

        if (names.size() < type.keyFields().size() + type.valueFields().size())
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
        assert tbl != null;

        boolean keyAsObj = !isIndexFixedTyping(schema.spaceName);

        boolean escapeAll = isEscapeAll(schema.spaceName);

        String keyType = keyAsObj ? "OTHER" : dbTypeFromClass(tbl.type().keyClass());
        String valTypeStr = dbTypeFromClass(tbl.type().valueClass());

        SB sql = new SB();

        sql.a("CREATE TABLE ").a(tbl.fullTableName()).a(" (")
            .a(KEY_FIELD_NAME).a(' ').a(keyType).a(" NOT NULL");

        sql.a(',').a(VAL_FIELD_NAME).a(' ').a(valTypeStr);

        for (Map.Entry<String, Class<?>> e: tbl.type().keyFields().entrySet())
            sql.a(',').a(escapeName(e.getKey(), escapeAll)).a(' ').a(dbTypeFromClass(e.getValue()));

        for (Map.Entry<String, Class<?>> e: tbl.type().valueFields().entrySet())
            sql.a(',').a(escapeName(e.getKey(), escapeAll)).a(' ').a(dbTypeFromClass(e.getValue()));

        sql.a(')');

        if (log.isDebugEnabled())
            log.debug("Creating DB table with SQL: " + sql);

        GridH2RowDescriptor desc = new RowDescriptor(tbl.type(), schema, keyAsObj);

        GridH2Table.Engine.createTable(conn, sql.toString(), desc, tbl, tbl.spaceName);
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
        ConcurrentMap<String, TableDescriptor> tbls = schemas.get(schema(space));

        if (tbls == null)
            return null;

        return tbls.get(type);
    }

    /**
     * Gets collection of table for given schema name.
     *
     * @param schema Schema name.
     * @return Collection of table descriptors.
     */
    private Collection<TableDescriptor> tables(String schema) {
        ConcurrentMap<String, TableDescriptor> tbls = schemas.get(schema);

        if (tbls == null)
            return Collections.emptySet();

        return tbls.values();
    }

    /**
     * Gets database schema from space.
     *
     * @param space Space name.
     * @return Schema name.
     */
    private static String schema(@Nullable String space) {
        if (F.isEmpty(space))
            return "PUBLIC";

        return space;
    }

    /** {@inheritDoc} */
    @Override public void rebuildIndexes(@Nullable String spaceName, GridQueryTypeDescriptor type) {
        if (offheap != null)
            throw new UnsupportedOperationException("Index rebuilding is not supported when off-heap memory is used");

        TableDescriptor tbl = tableDescriptor(spaceName, type);

        if (tbl == null)
            return;

        tbl.tbl.rebuildIndexes();
    }

    /** {@inheritDoc} */
    @Override public long size(@Nullable String spaceName, GridQueryTypeDescriptor type,
        GridIndexingQueryFilter filters) throws IgniteCheckedException {
        TableDescriptor tbl = tableDescriptor(spaceName, type);

        if (tbl == null)
            return -1;

        IgniteSpiCloseableIterator<List<?>> iter = queryFields(spaceName,
            "SELECT COUNT(*) FROM " + tbl.fullTableName(), null, null).iterator();

        return ((Number)iter.next().get(0)).longValue();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NonThreadSafeLazyInitialization")
    @Override public void start(GridKernalContext ctx) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Starting cache query index...");

        if (ctx != null) { // This is allowed in some tests.
            this.ctx = ctx;

            GridQueryConfiguration cfg0 = ctx.config().getQueryConfiguration();

            if (cfg0 != null)
                cfg = cfg0;

            for (GridCacheConfiguration cacheCfg : ctx.config().getCacheConfiguration())
                registerSpace(cacheCfg.getName());
        }

        System.setProperty("h2.serializeJavaObject", "false");

        if (SysProperties.serializeJavaObject) {
            U.warn(log, "Serialization of Java objects in H2 was enabled.");

            SysProperties.serializeJavaObject = false;
        }

        if (cfg.isUseOptimizedSerializer())
            Utils.serializer = h2Serializer();

        long maxOffHeapMemory = cfg.getMaxOffHeapMemory();

        if (maxOffHeapMemory != -1) {
            assert maxOffHeapMemory >= 0 : maxOffHeapMemory;

            offheap = new GridUnsafeMemory(maxOffHeapMemory);
        }

        SB opt = new SB();

        opt.a(DFLT_DB_OPTIONS).a(OPTIMIZED_DB_OPTIONS);

        String dbName = UUID.randomUUID().toString();

        dbUrl = "jdbc:h2:mem:" + dbName + opt;

        try {
            Class.forName("org.h2.Driver");
        }
        catch (ClassNotFoundException e) {
            throw new IgniteCheckedException("Failed to find org.h2.Driver class", e);
        }

        for (String schema : schemaNames)
            createSchemaIfAbsent(schema);

        try {
            createSqlFunctions();
            runInitScript();

            if (getString(GG_H2_DEBUG_CONSOLE) != null) {
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

//        registerMBean(gridName, this, GridH2IndexingSpiMBean.class); TODO
    }

    /**
     * @return Serializer.
     */
    protected JavaObjectSerializer h2Serializer() {
        return new JavaObjectSerializer() {
            @Override public byte[] serialize(Object obj) throws Exception {
                return ignite.configuration().getMarshaller().marshal(obj);
            }

            @Override public Object deserialize(byte[] bytes) throws Exception {
                return ignite.configuration().getMarshaller().unmarshal(bytes, null);
            }
        };
    }

    /**
     * Runs initial script.
     *
     * @throws IgniteCheckedException If failed.
     * @throws SQLException If failed.
     */
    private void runInitScript() throws IgniteCheckedException, SQLException {
        String initScriptPath = cfg.getInitialScriptPath();

        if (initScriptPath == null)
            return;

        try (PreparedStatement p = connectionForThread(null).prepareStatement("RUNSCRIPT FROM ? CHARSET 'UTF-8'")) {
            p.setString(1, initScriptPath);

            p.execute();
        }
    }

    /**
     * Registers SQL functions.
     *
     * @throws SQLException If failed.
     * @throws IgniteCheckedException If failed.
     */
    private void createSqlFunctions() throws SQLException, IgniteCheckedException {
        Class<?>[] idxCustomFuncClss = cfg.getIndexCustomFunctionClasses();

        if (F.isEmpty(idxCustomFuncClss))
            return;

        for (Class<?> cls : idxCustomFuncClss) {
            for (Method m : cls.getDeclaredMethods()) {
                GridCacheQuerySqlFunction ann = m.getAnnotation(GridCacheQuerySqlFunction.class);

                if (ann != null) {
                    int modifiers = m.getModifiers();

                    if (!Modifier.isStatic(modifiers) || !Modifier.isPublic(modifiers))
                        throw new IgniteCheckedException("Method " + m.getName() + " must be public static.");

                    String alias = ann.alias().isEmpty() ? m.getName() : ann.alias();

                    String clause = "CREATE ALIAS " + alias + (ann.deterministic() ? " DETERMINISTIC FOR \"" :
                        " FOR \"") + cls.getName() + '.' + m.getName() + '"';

                    Collection<String> schemas = new ArrayList<>(schemaNames);

                    if (!schemaNames.contains(schema(null)))
                        schemas.add(schema(null));

                    for (String schema : schemas) {
                        Connection c = connectionForThread(schema);

                        Statement s = c.createStatement();

                        s.execute(clause);

                        s.close();
                    }
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Stopping cache query index...");

//        unregisterMBean(); TODO

        Connection conn = connectionForThread(null);

        for (ConcurrentMap<String, TableDescriptor> m : schemas.values()) {
            for (TableDescriptor desc : m.values()) {
                desc.tbl.close();

                if (desc.luceneIdx != null)
                    U.closeQuiet(desc.luceneIdx);
            }
        }

        if (conn != null) {
            Statement stmt = null;

            try {
                stmt = conn.createStatement();

                stmt.execute("DROP ALL OBJECTS DELETE FILES");
                stmt.execute("SHUTDOWN");
            }
            catch (SQLException e) {
                throw new IgniteCheckedException("Failed to shutdown database.", e);
            }
            finally {
                U.close(stmt, log);
            }
        }

        for (Connection c : conns)
            U.close(c, log);

        conns.clear();
        schemas.clear();
        rowCache.clear();

        if (log.isDebugEnabled())
            log.debug("Cache query index stopped.");
    }

    /**
     * @param spaceName Space name.
     * @return {@code true} If primitive keys must be indexed.
     */
    public boolean isIndexPrimitiveKey(@Nullable String spaceName) {
        GridCacheQueryConfiguration cfg = cacheQueryConfiguration(spaceName);

        return cfg != null && cfg.isIndexPrimitiveKey();
    }
    
    /**
     * @param spaceName Space name.
     * @return {@code true} If primitive values must be indexed.
     */
    public boolean isIndexPrimitiveValue(String spaceName) {
        GridCacheQueryConfiguration cfg = cacheQueryConfiguration(spaceName);

        return cfg != null && cfg.isIndexPrimitiveValue();
    }

    /** {@inheritDoc} */
    public boolean isIndexFixedTyping(String spaceName) {
        GridCacheQueryConfiguration cfg = cacheQueryConfiguration(spaceName);

        return cfg != null && cfg.isIndexFixedTyping();
    }

    /** {@inheritDoc} */
    public boolean isEscapeAll(String spaceName) {
        GridCacheQueryConfiguration cfg = cacheQueryConfiguration(spaceName);

        return cfg != null && cfg.isEscapeAll();
    }

    /**
     * @param spaceName Space name.
     * @return Cache query configuration.
     */
    @Nullable private GridCacheQueryConfiguration cacheQueryConfiguration(String spaceName) {
        return ctx == null ? null : ctx.cache().internalCache(spaceName).configuration().getQueryConfiguration();
    }

    /** {@inheritDoc} */
    public int getMaxOffheapRowsCacheSize() {
        return (int)rowCache.getMaxMemory();
    }

    /** {@inheritDoc} */
    public int getOffheapRowsCacheSize() {
        return (int)rowCache.getUsedMemory();
    }

    /** {@inheritDoc} */
    public long getAllocatedOffHeapMemory() {
        return offheap == null ? -1 : offheap.allocatedSize();
    }

    /**
     * @param spaceName Space name.
     */
    public void registerSpace(String spaceName) {
        schemaNames.add(schema(spaceName));
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
        private final String spaceName;

        /** */
        private final String schema;

        /** */
        private GridH2Table tbl;

        /** */
        private GridLuceneIndex luceneIdx;

        /**
         * @param spaceName Space name.
         * @param type Type descriptor.
         */
        TableDescriptor(@Nullable String spaceName, GridQueryTypeDescriptor type) {
            this.spaceName = spaceName;
            this.type = type;

            schema = GridH2Indexing.schema(spaceName);

            fullTblName = '\"' + schema + "\"." + escapeName(type.name(), isEscapeAll(spaceName));
        }

        /**
         * @return Schema name.
         */
        public String schema() {
            return schema;
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
                    luceneIdx = new GridLuceneIndex(ignite.configuration().getMarshaller(),
                        offheap, spaceName, type, true);
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
                        luceneIdx = new GridLuceneIndex(ignite.configuration().getMarshaller(),
                            offheap, spaceName, type, true);
                    }
                    catch (IgniteCheckedException e1) {
                        throw new IgniteException(e1);
                    }
                }
                else {
                    IndexColumn[] cols = new IndexColumn[idx.fields().size()];

                    int i = 0;

                    boolean escapeAll = isEscapeAll(spaceName);

                    for (String field : idx.fields()) {
                        // H2 reserved keywords used as column name is case sensitive.
                        String fieldName = escapeAll ? field : escapeName(field, escapeAll).toUpperCase();

                        Column col = tbl.getColumn(fieldName);

                        cols[i++] = tbl.indexColumn(col.getColumnId(), idx.descending(field) ? DESCENDING : ASCENDING);
                    }

                    if (idx.type() == SORTED)
                        idxs.add(new GridH2TreeIndex(name, tbl, false, KEY_COL, VAL_COL, cols));
                    else if (idx.type() == GEO_SPATIAL)
                        idxs.add(new GridH2SpatialIndex(tbl, name, cols, KEY_COL, VAL_COL));
                    else
                        throw new IllegalStateException();
                }
            }

            return idxs;
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
    private static class Schema extends ConcurrentHashMap8<String, TableDescriptor> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final String spaceName;

        /**
         * @param spaceName Space name.
         */
        private Schema(@Nullable String spaceName) {
            this.spaceName = spaceName;
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
        private final int keyCols;

        /** */
        private final GridUnsafeGuard guard = offheap == null ? null : new GridUnsafeGuard();

        /**
         * @param type Type descriptor.
         * @param schema Schema.
         * @param keyAsObj Store key as java object.
         */
        RowDescriptor(GridQueryTypeDescriptor type, Schema schema, boolean keyAsObj) {
            assert type != null;
            assert schema != null;

            this.type = type;
            this.schema = schema;

            keyCols = type.keyFields().size();

            Map<String, Class<?>> allFields = new LinkedHashMap<>();

            allFields.putAll(type.keyFields());
            allFields.putAll(type.valueFields());

            fields = allFields.keySet().toArray(new String[allFields.size()]);

            fieldTypes = new int[fields.length];

            Class[] classes = allFields.values().toArray(new Class[fields.length]);

            for (int i = 0; i < fieldTypes.length; i++)
                fieldTypes[i] = DataType.getTypeFromClass(classes[i]);

            keyType = keyAsObj ? Value.JAVA_OBJECT : DataType.getTypeFromClass(type.keyClass());
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

            rowCache.put(ptr, row);
        }

        /** {@inheritDoc} */
        @Override public void uncache(long ptr) {
            rowCache.remove(ptr);
        }

        /** {@inheritDoc} */
        @Override public GridUnsafeMemory memory() {
            return offheap;
        }

        /** {@inheritDoc} */
        @Override public GridH2Indexing owner() {
            return GridH2Indexing.this;
        }

        /** {@inheritDoc} */
        @Override public GridH2AbstractKeyValueRow createRow(Object key, @Nullable Object val, long expirationTime)
            throws IgniteCheckedException {
            try {
                return offheap == null ?
                    new GridH2KeyValueRowOnheap(this, key, keyType, val, valType, expirationTime) :
                    new GridH2KeyValueRowOffheap(this, key, keyType, val, valType, expirationTime);
            }
            catch (ClassCastException e) {
                throw new IgniteCheckedException("Failed to convert key to SQL type. " +
                    "Please make sure that you always store each value type with the same key type or disable " +
                    "'defaultIndexFixedTyping' property.", e);
            }
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public Object readFromSwap(Object key) throws IgniteCheckedException {
            GridCache<Object, ?> cache = ctx.cache().cache(schema.spaceName);

            GridCacheContext cctx = ((GridCacheProxyImpl)cache).context();

            if (cctx.isNear())
                cctx = cctx.near().dht().context();

            GridCacheSwapEntry e = cctx.swap().read(key);

            return e != null ? e.value() : null;
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
        @Override public Object columnValue(Object obj, int col) {
            try {
                return type.value(obj, fields[col]);
            }
            catch (IgniteCheckedException e) {
                throw DbException.convert(e);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean isKeyColumn(int col) {
            return keyCols > col;
        }

        /** {@inheritDoc} */
        @Override public boolean valueToString() {
            return type.valueTextIndex();
        }

        /** {@inheritDoc} */
        @Override public GridH2KeyValueRowOffheap createPointer(long ptr) {
            GridH2KeyValueRowOffheap row = rowCache.get(ptr);

            if (row != null) {
                assert row.pointer() == ptr : ptr + " " + row.pointer();

                return row;
            }

            return new GridH2KeyValueRowOffheap(this, ptr);
        }
    }
}
