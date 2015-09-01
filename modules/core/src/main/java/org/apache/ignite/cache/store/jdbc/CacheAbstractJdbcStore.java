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

package org.apache.ignite.cache.store.jdbc;

import java.nio.ByteBuffer;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.sql.DataSource;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheTypeFieldMetadata;
import org.apache.ignite.cache.CacheTypeMetadata;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.jdbc.dialect.BasicJdbcDialect;
import org.apache.ignite.cache.store.jdbc.dialect.DB2Dialect;
import org.apache.ignite.cache.store.jdbc.dialect.H2Dialect;
import org.apache.ignite.cache.store.jdbc.dialect.JdbcDialect;
import org.apache.ignite.cache.store.jdbc.dialect.MySQLDialect;
import org.apache.ignite.cache.store.jdbc.dialect.OracleDialect;
import org.apache.ignite.cache.store.jdbc.dialect.SQLServerDialect;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static java.sql.Statement.EXECUTE_FAILED;
import static java.sql.Statement.SUCCESS_NO_INFO;

/**
 * Implementation of {@link CacheStore} backed by JDBC.
 * <p>
 * Store works with database via SQL dialect. Ignite ships with dialects for most popular databases:
 * <ul>
 *     <li>{@link DB2Dialect} - dialect for IBM DB2 database.</li>
 *     <li>{@link OracleDialect} - dialect for Oracle database.</li>
 *     <li>{@link SQLServerDialect} - dialect for Microsoft SQL Server database.</li>
 *     <li>{@link MySQLDialect} - dialect for Oracle MySQL database.</li>
 *     <li>{@link H2Dialect} - dialect for H2 database.</li>
 *     <li>{@link BasicJdbcDialect} - dialect for any database via plain JDBC.</li>
 * </ul>
 * <p>
 * <h2 class="header">Configuration</h2>
 * <ul>
 *     <li>Data source (see {@link #setDataSource(DataSource)}</li>
 *     <li>Dialect (see {@link #setDialect(JdbcDialect)}</li>
 *     <li>Maximum batch size for writeAll and deleteAll operations. (see {@link #setBatchSize(int)})</li>
 *     <li>Max workers thread count. These threads are responsible for load cache. (see {@link #setMaximumPoolSize(int)})</li>
 *     <li>Parallel load cache minimum threshold. (see {@link #setParallelLoadCacheMinimumThreshold(int)})</li>
 * </ul>
 * <h2 class="header">Java Example</h2>
 * <pre name="code" class="java">
 *    ...
 *    CacheConfiguration ccfg = new CacheConfiguration&lt;&gt;();
 *
 *    // Configure cache store.
 *    ccfg.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(ConfigurationSnippet.store()));
 *    ccfg.setReadThrough(true);
 *    ccfg.setWriteThrough(true);
 *
 *    // Configure cache types metadata.
 *    ccfg.setTypeMetadata(ConfigurationSnippet.typeMetadata());
 *
 *    cfg.setCacheConfiguration(ccfg);
 *    ...
 * </pre>
 */
public abstract class CacheAbstractJdbcStore<K, V> implements CacheStore<K, V>, LifecycleAware {
    /** Max attempt write count. */
    protected static final int MAX_ATTEMPT_WRITE_COUNT = 2;

    /** Default batch size for put and remove operations. */
    protected static final int DFLT_BATCH_SIZE = 512;

    /** Default batch size for put and remove operations. */
    protected static final int DFLT_PARALLEL_LOAD_CACHE_MINIMUM_THRESHOLD = 512;

    /** Connection attribute property name. */
    protected static final String ATTR_CONN_PROP = "JDBC_STORE_CONNECTION";

    /** Empty column value. */
    protected static final Object[] EMPTY_COLUMN_VALUE = new Object[] { null };

    /** Auto-injected store session. */
    @CacheStoreSessionResource
    private CacheStoreSession ses;

    /** Auto injected ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Auto-injected logger instance. */
    @LoggerResource
    protected IgniteLogger log;

    /** Lock for metadata cache. */
    @GridToStringExclude
    private final Lock cacheMappingsLock = new ReentrantLock();

    /** Data source. */
    protected DataSource dataSrc;

    /** Cache with entry mapping description. (cache name, (key id, mapping description)). */
    protected volatile Map<String, Map<Object, EntryMapping>> cacheMappings = Collections.emptyMap();

    /** Database dialect. */
    protected JdbcDialect dialect;

    /** Max workers thread count. These threads are responsible for load cache. */
    private int maxPoolSz = Runtime.getRuntime().availableProcessors();

    /** Maximum batch size for writeAll and deleteAll operations. */
    private int batchSz = DFLT_BATCH_SIZE;

    /** Parallel load cache minimum threshold. If {@code 0} then load sequentially. */
    private int parallelLoadCacheMinThreshold = DFLT_PARALLEL_LOAD_CACHE_MINIMUM_THRESHOLD;

    /**
     * Get field value from object for use as query parameter.
     *
     * @param cacheName Cache name.
     * @param typeName Type name.
     * @param fieldName Field name.
     * @param obj Cache object.
     * @return Field value from object.
     * @throws CacheException in case of error.
     */
    @Nullable protected abstract Object extractParameter(@Nullable String cacheName, String typeName, String fieldName,
        Object obj) throws CacheException;

    /**
     * Construct object from query result.
     *
     * @param <R> Type of result object.
     * @param cacheName Cache name.
     * @param typeName Type name.
     * @param fields Fields descriptors.
     * @param loadColIdxs Select query columns index.
     * @param rs ResultSet.
     * @return Constructed object.
     * @throws CacheLoaderException If failed to construct cache object.
     */
    protected abstract <R> R buildObject(@Nullable String cacheName, String typeName,
        Collection<CacheTypeFieldMetadata> fields, Map<String, Integer> loadColIdxs, ResultSet rs)
        throws CacheLoaderException;

    /**
     * Extract key type id from key object.
     *
     * @param key Key object.
     * @return Key type id.
     * @throws CacheException If failed to get type key id from object.
     */
    protected abstract Object keyTypeId(Object key) throws CacheException;

    /**
     * Extract key type id from key class name.
     *
     * @param type String description of key type.
     * @return Key type id.
     * @throws CacheException If failed to get type key id from object.
     */
    protected abstract Object keyTypeId(String type) throws CacheException;

    /**
     * Prepare internal store specific builders for provided types metadata.
     *
     * @param cacheName Cache name to prepare builders for.
     * @param types Collection of types.
     * @throws CacheException If failed to prepare internal builders for types.
     */
    protected abstract void prepareBuilders(@Nullable String cacheName, Collection<CacheTypeMetadata> types)
        throws CacheException;

    /**
     * Perform dialect resolution.
     *
     * @return The resolved dialect.
     * @throws CacheException Indicates problems accessing the metadata.
     */
    protected JdbcDialect resolveDialect() throws CacheException {
        Connection conn = null;

        String dbProductName = null;

        try {
            conn = openConnection(false);

            dbProductName = conn.getMetaData().getDatabaseProductName();
        }
        catch (SQLException e) {
            throw new CacheException("Failed access to metadata for detect database dialect.", e);
        }
        finally {
            U.closeQuiet(conn);
        }

        if ("H2".equals(dbProductName))
            return new H2Dialect();

        if ("MySQL".equals(dbProductName))
            return new MySQLDialect();

        if (dbProductName.startsWith("Microsoft SQL Server"))
            return new SQLServerDialect();

        if ("Oracle".equals(dbProductName))
            return new OracleDialect();

        if (dbProductName.startsWith("DB2/"))
            return new DB2Dialect();

        U.warn(log, "Failed to resolve dialect (BasicJdbcDialect will be used): " + dbProductName);

        return new BasicJdbcDialect();
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        if (dataSrc == null)
            throw new IgniteException("Failed to initialize cache store (data source is not provided).");

        if (dialect == null) {
            dialect = resolveDialect();

            if (log.isDebugEnabled() && dialect.getClass() != BasicJdbcDialect.class)
                log.debug("Resolved database dialect: " + U.getSimpleName(dialect.getClass()));
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        // No-op.
    }

    /**
     * Gets connection from a pool.
     *
     * @param autocommit {@code true} If connection should use autocommit mode.
     * @return Pooled connection.
     * @throws SQLException In case of error.
     */
    protected Connection openConnection(boolean autocommit) throws SQLException {
        Connection conn = dataSrc.getConnection();

        conn.setAutoCommit(autocommit);

        return conn;
    }

    /**
     * @return Connection.
     * @throws SQLException In case of error.
     */
    protected Connection connection() throws SQLException {
        CacheStoreSession ses = session();

        if (ses.transaction() != null) {
            Map<String, Connection> prop = ses.properties();

            Connection conn = prop.get(ATTR_CONN_PROP);

            if (conn == null) {
                conn = openConnection(false);

                // Store connection in session to used it for other operations in the same session.
                prop.put(ATTR_CONN_PROP, conn);
            }

            return conn;
        }
        // Transaction can be null in case of simple load operation.
        else
            return openConnection(true);
    }

    /**
     * Closes connection.
     *
     * @param conn Connection to close.
     */
    protected void closeConnection(@Nullable Connection conn) {
        CacheStoreSession ses = session();

        // Close connection right away if there is no transaction.
        if (ses.transaction() == null)
            U.closeQuiet(conn);
    }

    /**
     * Closes allocated resources depending on transaction status.
     *
     * @param conn Allocated connection.
     * @param st Created statement,
     */
    protected void end(@Nullable Connection conn, @Nullable Statement st) {
        U.closeQuiet(st);

        closeConnection(conn);
    }

    /** {@inheritDoc} */
    @Override public void sessionEnd(boolean commit) throws CacheWriterException {
        CacheStoreSession ses = session();

        Transaction tx = ses.transaction();

        if (tx != null) {
            Connection conn = ses.<String, Connection>properties().remove(ATTR_CONN_PROP);

            assert conn != null;

            try {
                if (commit)
                    conn.commit();
                else
                    conn.rollback();
            }
            catch (SQLException e) {
                throw new CacheWriterException(
                    "Failed to end transaction [xid=" + tx.xid() + ", commit=" + commit + ']', e);
            }
            finally {
                U.closeQuiet(conn);
            }

            if (log.isDebugEnabled())
                log.debug("Transaction ended [xid=" + tx.xid() + ", commit=" + commit + ']');
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object and
     * will convert to the requested Java data type.
     *
     * @param rs Result set.
     * @param colIdx Column index in result set.
     * @param type Class representing the Java data type to convert the designated column to.
     * @return Value in column.
     * @throws SQLException If a database access error occurs or this method is called.
     */
    protected Object getColumnValue(ResultSet rs, int colIdx, Class<?> type) throws SQLException {
        Object val = rs.getObject(colIdx);

        if (val == null)
            return null;

        if (type == int.class)
            return rs.getInt(colIdx);

        if (type == long.class)
            return rs.getLong(colIdx);

        if (type == double.class)
            return rs.getDouble(colIdx);

        if (type == boolean.class || type == Boolean.class)
            return rs.getBoolean(colIdx);

        if (type == byte.class)
            return rs.getByte(colIdx);

        if (type == short.class)
            return rs.getShort(colIdx);

        if (type == float.class)
            return rs.getFloat(colIdx);

        if (type == Integer.class || type == Long.class || type == Double.class ||
            type == Byte.class || type == Short.class ||  type == Float.class) {
            Number num = (Number)val;

            if (type == Integer.class)
                return num.intValue();
            else if (type == Long.class)
                return num.longValue();
            else if (type == Double.class)
                return num.doubleValue();
            else if (type == Byte.class)
                return num.byteValue();
            else if (type == Short.class)
                return num.shortValue();
            else if (type == Float.class)
                return num.floatValue();
        }

        if (type == UUID.class) {
            if (val instanceof UUID)
                return val;

            if (val instanceof byte[]) {
                ByteBuffer bb = ByteBuffer.wrap((byte[])val);

                long most = bb.getLong();
                long least = bb.getLong();

                return new UUID(most, least);
            }

            if (val instanceof String)
                return UUID.fromString((String)val);
        }

        return val;
    }

    /**
     * Construct load cache from range.
     *
     * @param em Type mapping description.
     * @param clo Closure that will be applied to loaded values.
     * @param lowerBound Lower bound for range.
     * @param upperBound Upper bound for range.
     * @return Callable for pool submit.
     */
    private Callable<Void> loadCacheRange(final EntryMapping em, final IgniteBiInClosure<K, V> clo,
        @Nullable final Object[] lowerBound, @Nullable final Object[] upperBound) {
        return new Callable<Void>() {
            @Override public Void call() throws Exception {
                Connection conn = null;

                PreparedStatement stmt = null;

                try {
                    conn = openConnection(true);

                    stmt = conn.prepareStatement(lowerBound == null && upperBound == null
                        ? em.loadCacheQry
                        : em.loadCacheRangeQuery(lowerBound != null, upperBound != null));

                    int ix = 1;

                    if (lowerBound != null)
                        for (int i = lowerBound.length; i > 0; i--)
                            for (int j = 0; j < i; j++)
                                stmt.setObject(ix++, lowerBound[j]);

                    if (upperBound != null)
                        for (int i = upperBound.length; i > 0; i--)
                            for (int j = 0; j < i; j++)
                                stmt.setObject(ix++, upperBound[j]);

                    ResultSet rs = stmt.executeQuery();

                    while (rs.next()) {
                        K key = buildObject(em.cacheName, em.keyType(), em.keyColumns(), em.loadColIdxs, rs);
                        V val = buildObject(em.cacheName, em.valueType(), em.valueColumns(), em.loadColIdxs, rs);

                        clo.apply(key, val);
                    }
                }
                catch (SQLException e) {
                    throw new IgniteCheckedException("Failed to load cache", e);
                }
                finally {
                    U.closeQuiet(stmt);

                    U.closeQuiet(conn);
                }

                return null;
            }
        };
    }

    /**
     * Construct load cache in one select.
     *
     * @param m Type mapping description.
     * @param clo Closure for loaded values.
     * @return Callable for pool submit.
     */
    private Callable<Void> loadCacheFull(EntryMapping m, IgniteBiInClosure<K, V> clo) {
        return loadCacheRange(m, clo, null, null);
    }

    /**
     * Object is a simple type.
     *
     * @param cls Class.
     * @return {@code True} if object is a simple type.
     */
    protected static boolean simpleType(Class<?> cls) {
        return (Number.class.isAssignableFrom(cls) || String.class.isAssignableFrom(cls) ||
            java.util.Date.class.isAssignableFrom(cls) || Boolean.class.isAssignableFrom(cls) ||
            UUID.class.isAssignableFrom(cls));
    }

    /**
     * @param cacheName Cache name to check mapping for.
     * @param clsName Class name.
     * @param fields Fields descriptors.
     * @throws CacheException If failed to check type metadata.
     */
    private static void checkMapping(@Nullable String cacheName, String clsName,
        Collection<CacheTypeFieldMetadata> fields) throws CacheException {
        try {
            Class<?> cls = Class.forName(clsName);

            if (simpleType(cls)) {
                if (fields.size() != 1)
                    throw new CacheException("More than one field for simple type [cache name=" + cacheName
                        + ", type=" + clsName + " ]");

                CacheTypeFieldMetadata field = F.first(fields);

                if (field.getDatabaseName() == null)
                    throw new CacheException("Missing database name in mapping description [cache name=" + cacheName
                        + ", type=" + clsName + " ]");

                field.setJavaType(cls);
            }
            else
                for (CacheTypeFieldMetadata field : fields) {
                    if (field.getDatabaseName() == null)
                        throw new CacheException("Missing database name in mapping description [cache name=" + cacheName
                            + ", type=" + clsName + " ]");

                    if (field.getJavaName() == null)
                        throw new CacheException("Missing field name in mapping description [cache name=" + cacheName
                            + ", type=" + clsName + " ]");

                    if (field.getJavaType() == null)
                        throw new CacheException("Missing field type in mapping description [cache name=" + cacheName
                            + ", type=" + clsName + " ]");
                }
        }
        catch (ClassNotFoundException e) {
            throw new CacheException("Failed to find class: " + clsName, e);
        }
    }

    /**
     * @param cacheName Cache name to check mappings for.
     * @return Type mappings for specified cache name.
     * @throws CacheException If failed to initialize cache mappings.
     */
    private Map<Object, EntryMapping> cacheMappings(@Nullable String cacheName) throws CacheException {
        Map<Object, EntryMapping> entryMappings = cacheMappings.get(cacheName);

        if (entryMappings != null)
            return entryMappings;

        cacheMappingsLock.lock();

        try {
            entryMappings = cacheMappings.get(cacheName);

            if (entryMappings != null)
                return entryMappings;

            CacheConfiguration ccfg = ignite().cache(cacheName).getConfiguration(CacheConfiguration.class);

            Collection<CacheTypeMetadata> types = ccfg.getTypeMetadata();

            entryMappings = U.newHashMap(types.size());

            for (CacheTypeMetadata type : types) {
                Object keyTypeId = keyTypeId(type.getKeyType());

                if (entryMappings.containsKey(keyTypeId))
                    throw new CacheException("Key type must be unique in type metadata [cache name=" + cacheName +
                        ", key type=" + type.getKeyType() + "]");

                checkMapping(cacheName, type.getKeyType(), type.getKeyFields());
                checkMapping(cacheName, type.getValueType(), type.getValueFields());

                entryMappings.put(keyTypeId(type.getKeyType()), new EntryMapping(cacheName, dialect, type));
            }

            Map<String, Map<Object, EntryMapping>> mappings = new HashMap<>(cacheMappings);

            mappings.put(cacheName, entryMappings);

            prepareBuilders(cacheName, types);

            cacheMappings = mappings;

            return entryMappings;
        }
        finally {
            cacheMappingsLock.unlock();
        }
    }

    /**
     * @param cacheName Cache name.
     * @param keyTypeId Key type id.
     * @param key Key object.
     * @return Entry mapping.
     * @throws CacheException If mapping for key was not found.
     */
    private EntryMapping entryMapping(String cacheName, Object keyTypeId, Object key) throws CacheException {
        EntryMapping em = cacheMappings(cacheName).get(keyTypeId);

        if (em == null) {
            String maskedCacheName = U.maskName(cacheName);

            throw new CacheException("Failed to find mapping description [key=" + key +
                ", cache=" + maskedCacheName + "]. Please configure CacheTypeMetadata to associate '" + maskedCacheName + "' with JdbcPojoStore.");
        }

        return em;
    }

    /** {@inheritDoc} */
    @Override public void loadCache(final IgniteBiInClosure<K, V> clo, @Nullable Object... args)
        throws CacheLoaderException {
        ExecutorService pool = null;

        String cacheName = session().cacheName();

        try {
            pool = Executors.newFixedThreadPool(maxPoolSz);

            Collection<Future<?>> futs = new ArrayList<>();

            if (args != null && args.length > 0) {
                if (args.length % 2 != 0)
                    throw new CacheLoaderException("Expected even number of arguments, but found: " + args.length);

                if (log.isDebugEnabled())
                    log.debug("Start loading entries from db using user queries from arguments");

                for (int i = 0; i < args.length; i += 2) {
                    String keyType = args[i].toString();

                    String selQry = args[i + 1].toString();

                    EntryMapping em = entryMapping(cacheName, keyTypeId(keyType), keyType);

                    futs.add(pool.submit(new LoadCacheCustomQueryWorker<>(em, selQry, clo)));
                }
            }
            else {
                Collection<EntryMapping> entryMappings = cacheMappings(session().cacheName()).values();

                for (EntryMapping em : entryMappings) {
                    if (parallelLoadCacheMinThreshold > 0) {
                        log.debug("Multithread loading entries from db [cache name=" + cacheName +
                            ", key type=" + em.keyType() + " ]");

                        Connection conn = null;

                        try {
                            conn = connection();

                            PreparedStatement stmt = conn.prepareStatement(em.loadCacheSelRangeQry);

                            stmt.setInt(1, parallelLoadCacheMinThreshold);

                            ResultSet rs = stmt.executeQuery();

                            if (rs.next()) {
                                int keyCnt = em.keyCols.size();

                                Object[] upperBound = new Object[keyCnt];

                                for (int i = 0; i < keyCnt; i++)
                                    upperBound[i] = rs.getObject(i + 1);

                                futs.add(pool.submit(loadCacheRange(em, clo, null, upperBound)));

                                while (rs.next()) {
                                    Object[] lowerBound = upperBound;

                                    upperBound = new Object[keyCnt];

                                    for (int i = 0; i < keyCnt; i++)
                                        upperBound[i] = rs.getObject(i + 1);

                                    futs.add(pool.submit(loadCacheRange(em, clo, lowerBound, upperBound)));
                                }

                                futs.add(pool.submit(loadCacheRange(em, clo, upperBound, null)));
                            }
                            else
                                futs.add(pool.submit(loadCacheFull(em, clo)));
                        }
                        catch (SQLException ignored) {
                            futs.add(pool.submit(loadCacheFull(em, clo)));
                        }
                        finally {
                            U.closeQuiet(conn);
                        }
                    }
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Single thread loading entries from db [cache name=" + cacheName +
                                ", key type=" + em.keyType() + " ]");

                        futs.add(pool.submit(loadCacheFull(em, clo)));
                    }
                }
            }

            for (Future<?> fut : futs)
                U.get(fut);

            if (log.isDebugEnabled())
                log.debug("Cache loaded from db: " + cacheName);
        }
        catch (IgniteCheckedException e) {
            throw new CacheLoaderException("Failed to load cache: " + cacheName, e.getCause());
        }
        finally {
            U.shutdownNow(getClass(), pool, log);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V load(K key) throws CacheLoaderException {
        assert key != null;

        EntryMapping em = entryMapping(session().cacheName(), keyTypeId(key), key);

        if (log.isDebugEnabled())
            log.debug("Load value from db [table= " + em.fullTableName() + ", key=" + key + "]");

        Connection conn = null;

        PreparedStatement stmt = null;

        try {
            conn = connection();

            stmt = conn.prepareStatement(em.loadQrySingle);

            fillKeyParameters(stmt, em, key);

            ResultSet rs = stmt.executeQuery();

            if (rs.next())
                return buildObject(em.cacheName, em.valueType(), em.valueColumns(), em.loadColIdxs, rs);
        }
        catch (SQLException e) {
            throw new CacheLoaderException("Failed to load object [table=" + em.fullTableName() +
                ", key=" + key + "]", e);
        }
        finally {
            end(conn, stmt);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> loadAll(Iterable<? extends K> keys) throws CacheLoaderException {
        assert keys != null;

        Connection conn = null;

        try {
            conn = connection();

            String cacheName = session().cacheName();

            Map<Object, LoadWorker<K, V>> workers = U.newHashMap(cacheMappings(cacheName).size());

            Map<K, V> res = new HashMap<>();

            for (K key : keys) {
                Object keyTypeId = keyTypeId(key);

                EntryMapping em = entryMapping(cacheName, keyTypeId, key);

                LoadWorker<K, V> worker = workers.get(keyTypeId);

                if (worker == null)
                    workers.put(keyTypeId, worker = new LoadWorker<>(conn, em));

                worker.keys.add(key);

                if (worker.keys.size() == em.maxKeysPerStmt)
                    res.putAll(workers.remove(keyTypeId).call());
            }

            for (LoadWorker<K, V> worker : workers.values())
                res.putAll(worker.call());

            return res;
        }
        catch (Exception e) {
            throw new CacheWriterException("Failed to load entries from database", e);
        }
        finally {
            closeConnection(conn);
        }
    }

    /**
     * @param insStmt Insert statement.
     * @param updStmt Update statement.
     * @param em Entry mapping.
     * @param entry Cache entry.
     * @throws CacheWriterException If failed to update record in database.
     */
    private void writeUpsert(PreparedStatement insStmt, PreparedStatement updStmt,
        EntryMapping em, Cache.Entry<? extends K, ? extends V> entry) throws CacheWriterException {
        try {
            CacheWriterException we = null;

            for (int attempt = 0; attempt < MAX_ATTEMPT_WRITE_COUNT; attempt++) {
                int paramIdx = fillValueParameters(updStmt, 1, em, entry.getValue());

                fillKeyParameters(updStmt, paramIdx, em, entry.getKey());

                if (updStmt.executeUpdate() == 0) {
                    paramIdx = fillKeyParameters(insStmt, em, entry.getKey());

                    fillValueParameters(insStmt, paramIdx, em, entry.getValue());

                    try {
                        insStmt.executeUpdate();

                        if (attempt > 0)
                            U.warn(log, "Entry was inserted in database on second try [table=" + em.fullTableName() +
                                ", entry=" + entry + "]");
                    }
                    catch (SQLException e) {
                        String sqlState = e.getSQLState();

                        SQLException nested = e.getNextException();

                        while (sqlState == null && nested != null) {
                            sqlState = nested.getSQLState();

                            nested = nested.getNextException();
                        }

                        // The error with code 23505 or 23000 is thrown when trying to insert a row that
                        // would violate a unique index or primary key.
                        if ("23505".equals(sqlState) || "23000".equals(sqlState)) {
                            if (we == null)
                                we = new CacheWriterException("Failed insert entry in database, violate a unique" +
                                    " index or primary key [table=" + em.fullTableName() + ", entry=" + entry + "]");

                            we.addSuppressed(e);

                            U.warn(log, "Failed insert entry in database, violate a unique index or primary key" +
                                " [table=" + em.fullTableName() + ", entry=" + entry + "]");

                            continue;
                        }

                        throw new CacheWriterException("Failed insert entry in database [table=" + em.fullTableName() +
                            ", entry=" + entry, e);
                    }
                }

                if (attempt > 0)
                    U.warn(log, "Entry was updated in database on second try [table=" + em.fullTableName() +
                        ", entry=" + entry + "]");

                return;
            }

            throw we;
        }
        catch (SQLException e) {
            throw new CacheWriterException("Failed update entry in database [table=" + em.fullTableName() +
                ", entry=" + entry + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void write(Cache.Entry<? extends K, ? extends V> entry) throws CacheWriterException {
        assert entry != null;

        K key = entry.getKey();

        EntryMapping em = entryMapping(session().cacheName(), keyTypeId(key), key);

        if (log.isDebugEnabled())
            log.debug("Start write entry to database [table=" + em.fullTableName() + ", entry=" + entry + "]");

        Connection conn = null;

        try {
            conn = connection();

            if (dialect.hasMerge()) {
                PreparedStatement stmt = null;

                try {
                    stmt = conn.prepareStatement(em.mergeQry);

                    int i = fillKeyParameters(stmt, em, key);

                    fillValueParameters(stmt, i, em, entry.getValue());

                    int updCnt = stmt.executeUpdate();

                    if (updCnt != 1)
                        U.warn(log, "Unexpected number of updated entries [table=" + em.fullTableName() +
                            ", entry=" + entry + "expected=1, actual=" + updCnt + "]");
                }
                finally {
                    U.closeQuiet(stmt);
                }
            }
            else {
                PreparedStatement insStmt = null;

                PreparedStatement updStmt = null;

                try {
                    insStmt = conn.prepareStatement(em.insQry);

                    updStmt = conn.prepareStatement(em.updQry);

                    writeUpsert(insStmt, updStmt, em, entry);
                }
                finally {
                    U.closeQuiet(insStmt);

                    U.closeQuiet(updStmt);
                }
            }
        }
        catch (SQLException e) {
            throw new CacheWriterException("Failed to write entry to database [table=" + em.fullTableName() +
                ", entry=" + entry + "]", e);
        }
        finally {
            closeConnection(conn);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeAll(final Collection<Cache.Entry<? extends K, ? extends V>> entries)
        throws CacheWriterException {
        assert entries != null;

        Connection conn = null;

        try {
            conn = connection();

            String cacheName = session().cacheName();

            Object currKeyTypeId = null;

            if (dialect.hasMerge()) {
                PreparedStatement mergeStmt = null;

                try {
                    EntryMapping em = null;

                    LazyValue<Object[]> lazyEntries = new LazyValue<Object[]>() {
                        @Override public Object[] create() {
                            return entries.toArray();
                        }
                    };

                    int fromIdx = 0, prepared = 0;

                    for (Cache.Entry<? extends K, ? extends V> entry : entries) {
                        K key = entry.getKey();

                        Object keyTypeId = keyTypeId(key);

                        em = entryMapping(cacheName, keyTypeId, key);

                        if (currKeyTypeId == null || !currKeyTypeId.equals(keyTypeId)) {
                            if (mergeStmt != null) {
                                if (log.isDebugEnabled())
                                    log.debug("Write entries to db [cache name=" + cacheName +
                                        ", key type=" + em.keyType() + ", count=" + prepared + "]");

                                executeBatch(em, mergeStmt, "writeAll", fromIdx, prepared, lazyEntries);

                                U.closeQuiet(mergeStmt);
                            }

                            mergeStmt = conn.prepareStatement(em.mergeQry);

                            currKeyTypeId = keyTypeId;

                            fromIdx += prepared;

                            prepared = 0;
                        }

                        int i = fillKeyParameters(mergeStmt, em, key);

                        fillValueParameters(mergeStmt, i, em, entry.getValue());

                        mergeStmt.addBatch();

                        if (++prepared % batchSz == 0) {
                            if (log.isDebugEnabled())
                                log.debug("Write entries to db [cache name=" + cacheName +
                                    ", key type=" + em.keyType() + ", count=" + prepared + "]");

                            executeBatch(em, mergeStmt, "writeAll", fromIdx, prepared, lazyEntries);

                            fromIdx += prepared;

                            prepared = 0;
                        }
                    }

                    if (mergeStmt != null && prepared % batchSz != 0) {
                        if (log.isDebugEnabled())
                            log.debug("Write entries to db [cache name=" + cacheName +
                                ", key type=" + em.keyType() + ", count=" + prepared + "]");

                        executeBatch(em, mergeStmt, "writeAll", fromIdx, prepared, lazyEntries);

                    }
                }
                finally {
                    U.closeQuiet(mergeStmt);
                }
            }
            else {
                log.debug("Write entries to db one by one using update and insert statements [cache name=" +
                    cacheName + ", count=" + entries.size() + "]");

                PreparedStatement insStmt = null;

                PreparedStatement updStmt = null;

                try {
                    for (Cache.Entry<? extends K, ? extends V> entry : entries) {
                        K key = entry.getKey();

                        Object keyTypeId = keyTypeId(key);

                        EntryMapping em = entryMapping(cacheName, keyTypeId, key);

                        if (currKeyTypeId == null || !currKeyTypeId.equals(keyTypeId)) {
                            U.closeQuiet(insStmt);

                            insStmt = conn.prepareStatement(em.insQry);

                            U.closeQuiet(updStmt);

                            updStmt = conn.prepareStatement(em.updQry);

                            currKeyTypeId = keyTypeId;
                        }

                        writeUpsert(insStmt, updStmt, em, entry);
                    }
                }
                finally {
                    U.closeQuiet(insStmt);

                    U.closeQuiet(updStmt);
                }
            }
        }
        catch (SQLException e) {
            throw new CacheWriterException("Failed to write entries in database", e);
        }
        finally {
            closeConnection(conn);
        }
    }

    /** {@inheritDoc} */
    @Override public void delete(Object key) throws CacheWriterException {
        assert key != null;

        EntryMapping em = entryMapping(session().cacheName(), keyTypeId(key), key);

        if (log.isDebugEnabled())
            log.debug("Remove value from db [table=" + em.fullTableName() + ", key=" + key + "]");

        Connection conn = null;

        PreparedStatement stmt = null;

        try {
            conn = connection();

            stmt = conn.prepareStatement(em.remQry);

            fillKeyParameters(stmt, em, key);

            int delCnt = stmt.executeUpdate();

            if (delCnt != 1)
                U.warn(log, "Unexpected number of deleted entries [table=" + em.fullTableName() + ", key=" + key +
                    ", expected=1, actual=" + delCnt + "]");
        }
        catch (SQLException e) {
            throw new CacheWriterException("Failed to remove value from database [table=" + em.fullTableName() +
                ", key=" + key + "]", e);
        }
        finally {
            end(conn, stmt);
        }
    }

    /**
     * @param em Entry mapping.
     * @param stmt Statement.
     * @param desc Statement description for error message.
     * @param fromIdx Objects in batch start from index.
     * @param prepared Expected objects in batch.
     * @param lazyObjs All objects used in batch statement as array.
     * @throws SQLException If failed to execute batch statement.
     */
    private void executeBatch(EntryMapping em, Statement stmt, String desc, int fromIdx, int prepared,
        LazyValue<Object[]> lazyObjs) throws SQLException {
        try {
            int[] rowCounts = stmt.executeBatch();

            int numOfRowCnt = rowCounts.length;

            if (numOfRowCnt != prepared)
                U.warn(log, "Unexpected number of updated rows [table=" + em.fullTableName() + ", expected=" + prepared +
                    ", actual=" + numOfRowCnt + "]");

            for (int i = 0; i < numOfRowCnt; i++) {
                int cnt = rowCounts[i];

                if (cnt != 1 && cnt != SUCCESS_NO_INFO) {
                    Object[] objs = lazyObjs.value();

                    U.warn(log, "Batch " + desc + " returned unexpected updated row count [table=" + em.fullTableName() +
                        ", entry=" + objs[fromIdx + i] + ", expected=1, actual=" + cnt + "]");
                }
            }
        }
        catch (BatchUpdateException be) {
            int[] rowCounts = be.getUpdateCounts();

            for (int i = 0; i < rowCounts.length; i++) {
                if (rowCounts[i] == EXECUTE_FAILED) {
                    Object[] objs = lazyObjs.value();

                    U.warn(log, "Batch " + desc + " failed on execution [table=" + em.fullTableName() +
                        ", entry=" + objs[fromIdx + i] + "]");
                }
            }

            throw be;
        }
    }

    /** {@inheritDoc} */
    @Override public void deleteAll(final Collection<?> keys) throws CacheWriterException {
        assert keys != null;

        Connection conn = null;

        try {
            conn = connection();

            LazyValue<Object[]> lazyKeys = new LazyValue<Object[]>() {
                @Override public Object[] create() {
                    return keys.toArray();
                }
            };

            String cacheName = session().cacheName();

            Object currKeyTypeId = null;

            EntryMapping em = null;

            PreparedStatement delStmt = null;

            int fromIdx = 0, prepared = 0;

            for (Object key : keys) {
                Object keyTypeId = keyTypeId(key);

                em = entryMapping(cacheName, keyTypeId, key);

                if (delStmt == null) {
                    delStmt = conn.prepareStatement(em.remQry);

                    currKeyTypeId = keyTypeId;
                }

                if (!currKeyTypeId.equals(keyTypeId)) {
                    if (log.isDebugEnabled())
                        log.debug("Delete entries from db [cache name=" + cacheName +
                            ", key type=" + em.keyType() + ", count=" + prepared + "]");

                    executeBatch(em, delStmt, "deleteAll", fromIdx, prepared, lazyKeys);

                    fromIdx += prepared;

                    prepared = 0;

                    currKeyTypeId = keyTypeId;
                }

                fillKeyParameters(delStmt, em, key);

                delStmt.addBatch();

                if (++prepared % batchSz == 0) {
                    if (log.isDebugEnabled())
                        log.debug("Delete entries from db [cache name=" + cacheName +
                            ", key type=" + em.keyType() + ", count=" + prepared + "]");

                    executeBatch(em, delStmt, "deleteAll", fromIdx, prepared, lazyKeys);

                    fromIdx += prepared;

                    prepared = 0;
                }
            }

            if (delStmt != null && prepared % batchSz != 0) {
                if (log.isDebugEnabled())
                    log.debug("Delete entries from db [cache name=" + cacheName +
                        ", key type=" + em.keyType() + ", count=" + prepared + "]");

                executeBatch(em, delStmt, "deleteAll", fromIdx, prepared, lazyKeys);
            }
        }
        catch (SQLException e) {
            throw new CacheWriterException("Failed to remove values from database", e);
        }
        finally {
            closeConnection(conn);
        }
    }

    /**
     * Sets the value of the designated parameter using the given object.
     *
     * @param stmt Prepare statement.
     * @param i Index for parameters.
     * @param field Field descriptor.
     * @param fieldVal Field value.
     * @throws CacheException If failed to set statement parameter.
     */
    protected void fillParameter(PreparedStatement stmt, int i, CacheTypeFieldMetadata field, @Nullable Object fieldVal)
        throws CacheException {
        try {
            if (fieldVal != null) {
                if (field.getJavaType() == UUID.class) {
                    switch (field.getDatabaseType()) {
                        case Types.BINARY:
                            fieldVal = U.uuidToBytes((UUID)fieldVal);

                            break;
                        case Types.CHAR:
                        case Types.VARCHAR:
                            fieldVal = fieldVal.toString();

                            break;
                    }
                }

                stmt.setObject(i, fieldVal);
            }
            else
                stmt.setNull(i, field.getDatabaseType());
        }
        catch (SQLException e) {
            throw new CacheException("Failed to set statement parameter name: " + field.getDatabaseName(), e);
        }
    }

    /**
     * @param stmt Prepare statement.
     * @param idx Start index for parameters.
     * @param em Entry mapping.
     * @param key Key object.
     * @return Next index for parameters.
     * @throws CacheException If failed to set statement parameters.
     */
    protected int fillKeyParameters(PreparedStatement stmt, int idx, EntryMapping em,
        Object key) throws CacheException {
        for (CacheTypeFieldMetadata field : em.keyColumns()) {
            Object fieldVal = extractParameter(em.cacheName, em.keyType(), field.getJavaName(), key);

            fillParameter(stmt, idx++, field, fieldVal);
        }

        return idx;
    }

    /**
     * @param stmt Prepare statement.
     * @param m Type mapping description.
     * @param key Key object.
     * @return Next index for parameters.
     * @throws CacheException If failed to set statement parameters.
     */
    protected int fillKeyParameters(PreparedStatement stmt, EntryMapping m, Object key) throws CacheException {
        return fillKeyParameters(stmt, 1, m, key);
    }

    /**
     * @param stmt Prepare statement.
     * @param idx Start index for parameters.
     * @param em Type mapping description.
     * @param val Value object.
     * @return Next index for parameters.
     * @throws CacheException If failed to set statement parameters.
     */
    protected int fillValueParameters(PreparedStatement stmt, int idx, EntryMapping em, Object val)
        throws CacheWriterException {
        for (CacheTypeFieldMetadata field : em.uniqValFields) {
            Object fieldVal = extractParameter(em.cacheName, em.valueType(), field.getJavaName(), val);

            fillParameter(stmt, idx++, field, fieldVal);
        }

        return idx;
    }

    /**
     * @return Data source.
     */
    public DataSource getDataSource() {
        return dataSrc;
    }

    /**
     * @param dataSrc Data source.
     */
    public void setDataSource(DataSource dataSrc) {
        this.dataSrc = dataSrc;
    }

    /**
     * Get database dialect.
     *
     * @return Database dialect.
     */
    public JdbcDialect getDialect() {
        return dialect;
    }

    /**
     * Set database dialect.
     *
     * @param dialect Database dialect.
     */
    public void setDialect(JdbcDialect dialect) {
        this.dialect = dialect;
    }

    /**
     * Get Max workers thread count. These threads are responsible for execute query.
     *
     * @return Max workers thread count.
     */
    public int getMaximumPoolSize() {
        return maxPoolSz;
    }

    /**
     * Set Max workers thread count. These threads are responsible for execute query.
     *
     * @param maxPoolSz Max workers thread count.
     */
    public void setMaximumPoolSize(int maxPoolSz) {
        this.maxPoolSz = maxPoolSz;
    }

    /**
     * Get maximum batch size for delete and delete operations.
     *
     * @return Maximum batch size.
     */
    public int getBatchSize() {
        return batchSz;
    }

    /**
     * Set maximum batch size for write and delete operations.
     *
     * @param batchSz Maximum batch size.
     */
    public void setBatchSize(int batchSz) {
        this.batchSz = batchSz;
    }

    /**
     * Parallel load cache minimum row count threshold.
     *
     * @return If {@code 0} then load sequentially.
     */
    public int getParallelLoadCacheMinimumThreshold() {
        return parallelLoadCacheMinThreshold;
    }

    /**
     * Parallel load cache minimum row count threshold.
     *
     * @param parallelLoadCacheMinThreshold Minimum row count threshold. If {@code 0} then load sequentially.
     */
    public void setParallelLoadCacheMinimumThreshold(int parallelLoadCacheMinThreshold) {
        this.parallelLoadCacheMinThreshold = parallelLoadCacheMinThreshold;
    }

    /**
     * @return Ignite instance.
     */
    protected Ignite ignite() {
        return ignite;
    }

    /**
     * @return Store session.
     */
    protected CacheStoreSession session() {
        return ses;
    }

    /**
     * Entry mapping description.
     */
    protected static class EntryMapping {
        /** Cache name. */
        private final String cacheName;

        /** Database dialect. */
        private final JdbcDialect dialect;

        /** Select border for range queries. */
        private final String loadCacheSelRangeQry;

        /** Select all items query. */
        private final String loadCacheQry;

        /** Select item query. */
        private final String loadQrySingle;

        /** Select items query. */
        private final String loadQry;

        /** Merge item(s) query. */
        private final String mergeQry;

        /** Update item query. */
        private final String insQry;

        /** Update item query. */
        private final String updQry;

        /** Remove item(s) query. */
        private final String remQry;

        /** Max key count for load query per statement. */
        private final int maxKeysPerStmt;

        /** Database key columns. */
        private final Collection<String> keyCols;

        /** Database unique value columns. */
        private final Collection<String> cols;

        /** Select query columns index. */
        private final Map<String, Integer> loadColIdxs;

        /** Unique value fields. */
        private final Collection<CacheTypeFieldMetadata> uniqValFields;

        /** Type metadata. */
        private final CacheTypeMetadata typeMeta;

        /** Full table name. */
        private final String fullTblName;

        /**
         * @param cacheName Cache name.
         * @param dialect JDBC dialect.
         * @param typeMeta Type metadata.
         */
        public EntryMapping(@Nullable String cacheName, JdbcDialect dialect, CacheTypeMetadata typeMeta) {
            this.cacheName = cacheName;

            this.dialect = dialect;

            this.typeMeta = typeMeta;

            Collection<CacheTypeFieldMetadata> keyFields = typeMeta.getKeyFields();

            Collection<CacheTypeFieldMetadata> valFields = typeMeta.getValueFields();

            keyCols = databaseColumns(keyFields);

            uniqValFields = F.view(valFields, new IgnitePredicate<CacheTypeFieldMetadata>() {
                @Override public boolean apply(CacheTypeFieldMetadata col) {
                    return !keyCols.contains(col.getDatabaseName());
                }
            });

            String schema = typeMeta.getDatabaseSchema();

            String tblName = typeMeta.getDatabaseTable();

            fullTblName = F.isEmpty(schema) ? tblName : schema + "." + tblName;

            Collection<String> uniqValCols = databaseColumns(uniqValFields);

            cols = F.concat(false, keyCols, uniqValCols);

            loadColIdxs = U.newHashMap(cols.size());

            int idx = 1;

            for (String col : cols)
                loadColIdxs.put(col, idx++);

            loadCacheQry = dialect.loadCacheQuery(fullTblName, cols);

            loadCacheSelRangeQry = dialect.loadCacheSelectRangeQuery(fullTblName, keyCols);

            loadQrySingle = dialect.loadQuery(fullTblName, keyCols, cols, 1);

            maxKeysPerStmt = dialect.getMaxParameterCount() / keyCols.size();

            loadQry = dialect.loadQuery(fullTblName, keyCols, cols, maxKeysPerStmt);

            insQry = dialect.insertQuery(fullTblName, keyCols, uniqValCols);

            updQry = dialect.updateQuery(fullTblName, keyCols, uniqValCols);

            mergeQry = dialect.mergeQuery(fullTblName, keyCols, uniqValCols);

            remQry = dialect.removeQuery(fullTblName, keyCols);
        }

        /**
         * Extract database column names from {@link CacheTypeFieldMetadata}.
         *
         * @param dsc collection of {@link CacheTypeFieldMetadata}.
         * @return Collection with database column names.
         */
        private static Collection<String> databaseColumns(Collection<CacheTypeFieldMetadata> dsc) {
            return F.transform(dsc, new C1<CacheTypeFieldMetadata, String>() {
                /** {@inheritDoc} */
                @Override public String apply(CacheTypeFieldMetadata col) {
                    return col.getDatabaseName();
                }
            });
        }

        /**
         * Construct query for select values with key count less or equal {@code maxKeysPerStmt}
         *
         * @param keyCnt Key count.
         * @return Load query statement text.
         */
        protected String loadQuery(int keyCnt) {
            assert keyCnt <= maxKeysPerStmt;

            if (keyCnt == maxKeysPerStmt)
                return loadQry;

            if (keyCnt == 1)
                return loadQrySingle;

            return dialect.loadQuery(fullTblName, keyCols, cols, keyCnt);
        }

        /**
         * Construct query for select values in range.
         *
         * @param appendLowerBound Need add lower bound for range.
         * @param appendUpperBound Need add upper bound for range.
         * @return Query with range.
         */
        protected String loadCacheRangeQuery(boolean appendLowerBound, boolean appendUpperBound) {
            return dialect.loadCacheRangeQuery(fullTblName, keyCols, cols, appendLowerBound, appendUpperBound);
        }

        /**
         * @return Key type.
         */
        protected String keyType() {
            return typeMeta.getKeyType();
        }

        /**
         * @return Value type.
         */
        protected String valueType() {
            return typeMeta.getValueType();
        }

        /**
         * Gets key columns.
         *
         * @return Key columns.
         */
        protected Collection<CacheTypeFieldMetadata> keyColumns() {
            return typeMeta.getKeyFields();
        }

        /**
         * Gets value columns.
         *
         * @return Value columns.
         */
        protected Collection<CacheTypeFieldMetadata> valueColumns() {
            return typeMeta.getValueFields();
        }

        /**
         * Get full table name.
         *
         * @return &lt;schema&gt;.&lt;table name&gt
         */
        protected String fullTableName() {
            return fullTblName;
        }
    }

    /**
     * Worker for load cache using custom user query.
     *
     * @param <K1> Key type.
     * @param <V1> Value type.
     */
    private class LoadCacheCustomQueryWorker<K1, V1> implements Callable<Void> {
        /** Entry mapping description. */
        private final EntryMapping em;

        /** User query. */
        private final String qry;

        /** Closure for loaded values. */
        private final IgniteBiInClosure<K1, V1> clo;

        /**
         * @param em Entry mapping description.
         * @param qry User query.
         * @param clo Closure for loaded values.
         */
        private LoadCacheCustomQueryWorker(EntryMapping em, String qry, IgniteBiInClosure<K1, V1> clo) {
            this.em = em;
            this.qry = qry;
            this.clo = clo;
        }

        /** {@inheritDoc} */
        @Override public Void call() throws Exception {
            if (log.isDebugEnabled())
                log.debug("Load cache using custom query [cache name= " + em.cacheName +
                    ", key type=" + em.keyType() + ", query=" + qry + "]");

            Connection conn = null;

            PreparedStatement stmt = null;

            try {
                conn = openConnection(true);

                stmt = conn.prepareStatement(qry);

                ResultSet rs = stmt.executeQuery();

                ResultSetMetaData meta = rs.getMetaData();

                Map<String, Integer> colIdxs = U.newHashMap(meta.getColumnCount());

                for (int i = 1; i <= meta.getColumnCount(); i++)
                    colIdxs.put(meta.getColumnLabel(i), i);

                while (rs.next()) {
                    K1 key = buildObject(em.cacheName, em.keyType(), em.keyColumns(), colIdxs, rs);
                    V1 val = buildObject(em.cacheName, em.valueType(), em.valueColumns(), colIdxs, rs);

                    clo.apply(key, val);
                }

                return null;
            }
            catch (SQLException e) {
                throw new CacheLoaderException("Failed to execute custom query for load cache", e);
            }
            finally {
                U.closeQuiet(stmt);

                U.closeQuiet(conn);
            }
        }
    }

    /**
     * Lazy initialization of value.
     *
     * @param <T> Cached object type
     */
    private abstract static class LazyValue<T> {
        /** Cached value. */
        private T val;

        /**
         * @return Construct value.
         */
        protected abstract T create();

        /**
         * @return Value.
         */
        public T value() {
            if (val == null)
                val = create();

            return val;
        }
    }

    /**
     * Worker for load by keys.
     *
     * @param <K1> Key type.
     * @param <V1> Value type.
     */
    private class LoadWorker<K1, V1> implements Callable<Map<K1, V1>> {
        /** Connection. */
        private final Connection conn;

        /** Keys for load. */
        private final Collection<K1> keys;

        /** Entry mapping description. */
        private final EntryMapping em;

        /**
         * @param conn Connection.
         * @param em Entry mapping description.
         */
        private LoadWorker(Connection conn, EntryMapping em) {
            this.conn = conn;
            this.em = em;

            keys = new ArrayList<>(em.maxKeysPerStmt);
        }

        /** {@inheritDoc} */
        @Override public Map<K1, V1> call() throws Exception {
            if (log.isDebugEnabled())
                log.debug("Load values from db [table= " + em.fullTableName() +
                    ", key count=" + keys.size() + "]");

            PreparedStatement stmt = null;

            try {
                stmt = conn.prepareStatement(em.loadQuery(keys.size()));

                int idx = 1;

                for (Object key : keys)
                    for (CacheTypeFieldMetadata field : em.keyColumns()) {
                        Object fieldVal = extractParameter(em.cacheName, em.keyType(), field.getJavaName(), key);

                        fillParameter(stmt, idx++, field, fieldVal);
                    }

                ResultSet rs = stmt.executeQuery();

                Map<K1, V1> entries = U.newHashMap(keys.size());

                while (rs.next()) {
                    K1 key = buildObject(em.cacheName, em.keyType(), em.keyColumns(), em.loadColIdxs, rs);
                    V1 val = buildObject(em.cacheName, em.valueType(), em.valueColumns(), em.loadColIdxs, rs);

                    entries.put(key, val);
                }

                return entries;
            }
            finally {
                U.closeQuiet(stmt);
            }
        }
    }
}