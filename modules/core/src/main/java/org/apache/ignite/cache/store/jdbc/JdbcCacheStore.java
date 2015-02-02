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

import org.apache.ignite.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.cache.store.jdbc.dialect.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.interop.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.integration.*;
import javax.sql.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/**
 * Base {@link CacheStore} implementation backed by JDBC. This implementation stores objects in underlying database
 * using mapping description.
 * <p>
 * <h2 class="header">Configuration</h2>
 * Sections below describe mandatory and optional configuration settings as well
 * as providing example using Java and Spring XML.
 * <h3>Mandatory</h3>
 * There are no mandatory configuration parameters.
 * <h3>Optional</h3>
 * <ul>
 *     <li>Data source (see {@link #setDataSource(DataSource)}</li>
 *     <li>Maximum batch size for writeAll and deleteAll operations. (see {@link #setBatchSize(int)})</li>
 *     <li>Max workers thread count. These threads are responsible for load cache. (see {@link #setMaxPoolSize(int)})</li>
 *     <li>Parallel load cache minimum threshold. (see {@link #setParallelLoadCacheMinimumThreshold(int)})</li>
 * </ul>
 * <h2 class="header">Java Example</h2>
 * <pre name="code" class="java">
 *     ...
 *     JdbcPojoCacheStore store = new JdbcPojoCacheStore();
 *     ...
 *
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * <pre name="code" class="xml">
 *     ...
 *     &lt;bean id=&quot;cache.jdbc.store&quot;
 *         class=&quot;org.apache.ignite.cache.store.jdbc.JdbcPojoCacheStore&quot;&gt;
 *         &lt;property name=&quot;connectionUrl&quot; value=&quot;jdbc:h2:mem:&quot;/&gt;
 *     &lt;/bean&gt;
 *     ...
 * </pre>
 * <p>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 */
public abstract class JdbcCacheStore<K, V> extends CacheStore<K, V> implements GridInteropAware {
    /** Max attempt write count. */
    protected static final int MAX_ATTEMPT_WRITE_COUNT = 2;

    /** Default batch size for put and remove operations. */
    protected static final int DFLT_BATCH_SIZE = 512;

    /** Default batch size for put and remove operations. */
    protected static final int DFLT_PARALLEL_LOAD_CACHE_MINIMUM_THRESHOLD = 512;

    /** Connection attribute property name. */
    protected static final String ATTR_CONN_PROP = "JDBC_STORE_CONNECTION";

    /** Auto-injected logger instance. */
    @IgniteLoggerResource
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
     * Get field value from object.
     *
     * @param typeName Type name.
     * @param fieldName Field name.
     * @param obj Cache object.
     * @return Field value from object.
     */
    @Nullable protected abstract Object extractField(String typeName, String fieldName, Object obj)
        throws CacheException;

    /**
     * Construct object from query result.
     *
     * @param <R> Type of result object.
     * @param typeName Type name.
     * @param fields Fields descriptors.
     * @param rs ResultSet.
     * @return Constructed object.
     */
    protected abstract <R> R buildObject(String typeName, Collection<CacheQueryTableColumnMetadata> fields, ResultSet rs)
        throws CacheLoaderException;

    /**
     * Extract key type id from key object.
     *
     * @param key Key object.
     * @return Key type id.
     */
    protected abstract Object keyTypeId(Object key) throws CacheException;

    /**
     * Extract key type id from key class name.
     *
     * @param type String description of key type.
     * @return Key type id.
     */
    protected abstract Object keyTypeId(String type) throws CacheException;

    /**
     * Prepare internal store specific builders for provided types metadata.
     *
     * @param types Collection of types.
     * @throws CacheException If failed to prepare.
     */
    protected abstract void prepareBuilders(@Nullable String cacheName, Collection<CacheQueryTypeMetadata> types)
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

    /**
     *
     * @return Cache key id.
     */
    protected Integer cacheKeyId() {
        String cacheName = session().cacheName();

        return cacheName != null ? cacheName.hashCode() : 0;
    }

    /** {@inheritDoc} */
    @Override public void configure(Object... params) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void initialize(GridKernalContext ctx) throws IgniteCheckedException {
        if (dataSrc == null)
            throw new IgniteCheckedException("Failed to initialize cache store (data source is not provided).");

        if (dialect == null)
            dialect = resolveDialect();
    }

    /** {@inheritDoc} */
    @Override public void destroy(GridKernalContext ctx) throws IgniteCheckedException {
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
    @Override public void txEnd(boolean commit) throws CacheWriterException {
        CacheStoreSession ses = session();

        IgniteTx tx = ses.transaction();

        Connection conn = ses.<String, Connection>properties().remove(ATTR_CONN_PROP);

        if (conn != null) {
            assert tx != null;

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
        }

        if (tx != null && log.isDebugEnabled())
            log.debug("Transaction ended [xid=" + tx.xid() + ", commit=" + commit + ']');
    }

    /**
     * Construct load cache from range.
     *
     * @param m Type mapping description.
     * @param clo Closure that will be applied to loaded values.
     * @param lowerBound Lower bound for range.
     * @param upperBound Upper bound for range.
     * @return Callable for pool submit.
     */
    private Callable<Void> loadCacheRange(final EntryMapping m, final IgniteBiInClosure<K, V> clo,
        @Nullable final Object[] lowerBound, @Nullable final Object[] upperBound) {
        return new Callable<Void>() {
            @Override public Void call() throws Exception {
                Connection conn = null;

                PreparedStatement stmt = null;

                try {
                    conn = openConnection(true);

                    stmt = conn.prepareStatement(lowerBound == null && upperBound == null
                        ? m.loadCacheQry
                        : m.loadCacheRangeQuery(lowerBound != null, upperBound != null));

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
                        K key = buildObject(m.keyType(), m.keyColumns(), rs);
                        V val = buildObject(m.valueType(), m.valueColumns(), rs);

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
    private Callable<Void> loadCacheFull(final EntryMapping m, final IgniteBiInClosure<K, V> clo) {
        return loadCacheRange(m, clo, null, null);
    }

    /**
     * @return Type mappings for specified cache name.
     *
     * @throws CacheException If failed to initialize.
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

            Collection<CacheQueryTypeMetadata> typeMetadata =
                ignite().cache(session().cacheName()).configuration().getQueryConfiguration().getTypeMetadata();

            entryMappings = U.newHashMap(typeMetadata.size());

            for (CacheQueryTypeMetadata type : typeMetadata)
                entryMappings.put(keyTypeId(type.getKeyType()), new EntryMapping(dialect, type));

            Map<String, Map<Object, EntryMapping>> mappings = new HashMap<>(cacheMappings);

            mappings.put(cacheName, entryMappings);

            prepareBuilders(cacheName, typeMetadata);

            cacheMappings = mappings;

            return entryMappings;
        }
        finally {
            cacheMappingsLock.unlock();
        }
    }

    /**
     * @param keyTypeId Key type id.
     * @param key Key object.
     * @return Entry mapping.
     * @throws CacheException if mapping for key was not found.
     */
    private EntryMapping entryMapping(Object keyTypeId, Object key) throws CacheException {
        String cacheName = session().cacheName();

        EntryMapping em = cacheMappings(cacheName).get(keyTypeId);

        if (em == null)
            throw new CacheException("Failed to find mapping description for key: " + key +
                " in cache: " + (cacheName != null ? cacheName : "<default>"));

        return em;
    }

    /** {@inheritDoc} */
    @Override public void loadCache(final IgniteBiInClosure<K, V> clo, @Nullable Object... args)
        throws CacheLoaderException {
        try {
            ExecutorService pool = Executors.newFixedThreadPool(maxPoolSz);

            Collection<Future<?>> futs = new ArrayList<>();

            if (args != null && args.length > 0) {
                if (args.length % 2 != 0)
                    throw new CacheLoaderException("Expected even number of arguments, but found: " + args.length);

                if (log.isDebugEnabled())
                    log.debug("Start loading entries from db using user queries from arguments");

                for (int i = 0; i < args.length; i += 2) {
                    String keyType = args[i].toString();

                    String selQry = args[i + 1].toString();

                    EntryMapping em = entryMapping(keyTypeId(keyType), keyType);

                    futs.add(pool.submit(new LoadCacheCustomQueryWorker<>(em, selQry, clo)));
                }
            }
            else {
                Collection<EntryMapping> entryMappings = cacheMappings(session().cacheName()).values();

                if (log.isDebugEnabled())
                    log.debug("Start loading all cache types entries from db");

                for (EntryMapping em : entryMappings) {
                    if (parallelLoadCacheMinThreshold > 0) {
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
                    else
                        futs.add(pool.submit(loadCacheFull(em, clo)));
                }
            }

            for (Future<?> fut : futs)
                U.get(fut);
        }
        catch (IgniteCheckedException e) {
            throw new CacheLoaderException("Failed to load cache", e.getCause());
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V load(K key) throws CacheLoaderException {
        assert key != null;

        EntryMapping em = entryMapping(keyTypeId(key), key);

        if (log.isDebugEnabled())
            log.debug("Start load value from database by key: " + key);

        Connection conn = null;

        PreparedStatement stmt = null;

        try {
            conn = connection();

            stmt = conn.prepareStatement(em.loadQrySingle);

            fillKeyParameters(stmt, em, key);

            ResultSet rs = stmt.executeQuery();

            if (rs.next())
                return buildObject(em.valueType(), em.valueColumns(), rs);
        }
        catch (SQLException e) {
            throw new CacheLoaderException("Failed to load object by key: " + key, e);
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

            Map<Object, LoadWorker<K, V>> workers = U.newHashMap(cacheMappings(session().cacheName()).size());

            Map<K, V> res = new HashMap<>();

            for (K key : keys) {
                Object keyTypeId = keyTypeId(key);

                EntryMapping em = entryMapping(keyTypeId, key);

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

    /** {@inheritDoc} */
    @Override public void write(Cache.Entry<? extends K, ? extends V> entry) throws CacheWriterException {
        assert entry != null;

        K key = entry.getKey();

        EntryMapping em = entryMapping(keyTypeId(key), key);

        if (log.isDebugEnabled())
            log.debug("Start write entry to database: " + entry);

        Connection conn = null;

        PreparedStatement stmt = null;

        try {
            conn = connection();

            if (dialect.hasMerge()) {
                stmt = conn.prepareStatement(em.mergeQry);

                int i = fillKeyParameters(stmt, em, key);

                fillValueParameters(stmt, i, em, entry.getValue());

                stmt.executeUpdate();
            }
            else {
                V val = entry.getValue();

                for (int attempt = 0; attempt < MAX_ATTEMPT_WRITE_COUNT; attempt++) {
                    stmt = conn.prepareStatement(em.updQry);

                    int i = fillValueParameters(stmt, 1, em, val);

                    fillKeyParameters(stmt, i, em, key);

                    if (stmt.executeUpdate() == 0) {
                        U.closeQuiet(stmt);

                        stmt = conn.prepareStatement(em.insQry);

                        i = fillKeyParameters(stmt, em, key);

                        fillValueParameters(stmt, i, em, val);

                        try {
                            stmt.executeUpdate();
                        }
                        catch (SQLException e) {
                            // The error with code 23505 is thrown when trying to insert a row that
                            // would violate a unique index or primary key.
                            // TODO check with all RDBMS
                            if (e.getErrorCode() == 23505)
                                continue;

                            throw e;
                        }
                    }

                    return;
                }

                throw new CacheWriterException("Failed write entry to database: " + entry);
            }
        }
        catch (SQLException e) {
            throw new CacheWriterException("Failed to write entry to database: " + entry, e);
        }
        finally {
            end(conn, stmt);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> entries)
        throws CacheWriterException {
        assert entries != null;

        if (dialect.hasMerge()) {
            Connection conn = null;

            PreparedStatement mergeStmt = null;

            try {
                conn = connection();

                Object currKeyTypeId = null;

                int cnt = 0;

                for (Cache.Entry<? extends K, ? extends V> entry : entries) {
                    K key = entry.getKey();

                    Object keyTypeId = keyTypeId(key);

                    EntryMapping em = entryMapping(keyTypeId, key);

                    if (mergeStmt == null) {
                        mergeStmt = conn.prepareStatement(em.mergeQry);

                        currKeyTypeId = keyTypeId;
                    }

                    if (!currKeyTypeId.equals(keyTypeId)) {
                        executeBatch(mergeStmt, "writeAll", cnt);

                        currKeyTypeId = keyTypeId;

                        cnt = 0;
                    }

                    int i = fillKeyParameters(mergeStmt, em, key);

                    fillValueParameters(mergeStmt, i, em, entry.getValue());

                    mergeStmt.addBatch();

                    if (++cnt % batchSz == 0) {
                        executeBatch(mergeStmt, "writeAll", cnt);

                        cnt = 0;
                    }
                }

                if (mergeStmt != null && cnt % batchSz != 0)
                    executeBatch(mergeStmt, "writeAll", cnt);
            }
            catch (SQLException e) {
                throw new CacheWriterException("Failed to write entries in database", e);
            }
            finally {
                U.closeQuiet(mergeStmt);

                closeConnection(conn);
            }
        }
        else
            for (Cache.Entry<? extends K, ? extends V> e : entries)
                write(e);
    }

    /** {@inheritDoc} */
    @Override public void delete(Object key) throws CacheWriterException {
        assert key != null;

        EntryMapping em = entryMapping(keyTypeId(key), key);

        if (log.isDebugEnabled())
            log.debug("Start remove value from database by key: " + key);

        Connection conn = null;

        PreparedStatement stmt = null;

        try {
            conn = connection();

            stmt = conn.prepareStatement(em.remQry);

            fillKeyParameters(stmt, em, key);

            if (stmt.executeUpdate() == 0)
                log.warning("Nothing was deleted in database for key: " + key);
        }
        catch (SQLException e) {
            throw new CacheWriterException("Failed to remove value from database by key: " + key, e);
        }
        finally {
            end(conn, stmt);
        }
    }

    /**
     * @param stmt Statement.
     * @param stmtType Statement type for error message.
     * @param batchSz Expected batch size.
     */
    private void executeBatch(Statement stmt, String stmtType, int batchSz) throws SQLException {
        int[] rowCounts = stmt.executeBatch();

        int numOfRowCnt = rowCounts.length;

        if (numOfRowCnt != batchSz)
            log.warning("JDBC driver did not return the expected number of row counts," +
                " actual row count: " + numOfRowCnt + " expected: " + batchSz);

        for (int rowCount : rowCounts)
            if (rowCount != 1) {
                log.warning("Batch " + stmtType + " returned unexpected row count from " + stmtType + " statement");

                break;
            }
    }

    /** {@inheritDoc} */
    @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
        assert keys != null;

        Connection conn = null;

        try {
            conn = connection();

            Object currKeyTypeId  = null;

            PreparedStatement delStmt = null;

            int cnt = 0;

            for (Object key : keys) {
                Object keyTypeId = keyTypeId(key);

                EntryMapping em = entryMapping(keyTypeId, key);

                if (delStmt == null) {
                    delStmt = conn.prepareStatement(em.remQry);

                    currKeyTypeId = keyTypeId;
                }

                if (!currKeyTypeId.equals(keyTypeId)) {
                    executeBatch(delStmt, "deleteAll", cnt);

                    cnt = 0;

                    currKeyTypeId = keyTypeId;
                }

                fillKeyParameters(delStmt, em, key);

                delStmt.addBatch();

                if (++cnt % batchSz == 0) {
                    executeBatch(delStmt, "deleteAll", cnt);

                    cnt = 0;
                }
            }

            if (delStmt != null && cnt % batchSz != 0)
                executeBatch(delStmt, "deleteAll", cnt);
        }
        catch (SQLException e) {
            throw new CacheWriterException("Failed to remove values from database", e);
        }
        finally {
            closeConnection(conn);
        }
    }

    /**
     * @param stmt Prepare statement.
     * @param i Start index for parameters.
     * @param type Type description.
     * @param key Key object.
     * @return Next index for parameters.
     */
    protected int fillKeyParameters(PreparedStatement stmt, int i, EntryMapping type,
        Object key) throws CacheException {
        for (CacheQueryTableColumnMetadata field : type.keyColumns()) {
            Object fieldVal = extractField(type.keyType(), field.getJavaName(), key);

            try {
                if (fieldVal != null)
                    stmt.setObject(i++, fieldVal);
                else
                    stmt.setNull(i++, field.getDbType());
            }
            catch (SQLException e) {
                throw new CacheException("Failed to set statement parameter name: " + field.getDbName(), e);
            }
        }

        return i;
    }

    /**
     * @param stmt Prepare statement.
     * @param m Type mapping description.
     * @param key Key object.
     * @return Next index for parameters.
     */
    protected int fillKeyParameters(PreparedStatement stmt, EntryMapping m, Object key) throws CacheException {
        return fillKeyParameters(stmt, 1, m, key);
    }

    /**
     * @param stmt Prepare statement.
     * @param i Start index for parameters.
     * @param m Type mapping description.
     * @param val Value object.
     * @return Next index for parameters.
     */
    protected int fillValueParameters(PreparedStatement stmt, int i, EntryMapping m, Object val)
        throws CacheWriterException {
        for (CacheQueryTableColumnMetadata field : m.uniqValFields) {
            Object fieldVal = extractField(m.valueType(), field.getJavaName(), val);

            try {
                if (fieldVal != null)
                    stmt.setObject(i++, fieldVal);
                else
                    stmt.setNull(i++, field.getDbType());
            }
            catch (SQLException e) {
                throw new CacheWriterException("Failed to set statement parameter name: " + field.getDbName(), e);
            }
        }

        return i;
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
    public int getMaxPoolSize() {
        return maxPoolSz;
    }

    /**
     * Set Max workers thread count. These threads are responsible for execute query.
     *
     * @param maxPoolSz Max workers thread count.
     */
    public void setMaxPoolSize(int maxPoolSz) {
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
     * Entry mapping description.
     */
    protected static class EntryMapping {
        /** Database dialect. */
        private final JdbcDialect dialect;

        /** Select border for range queries. */
        protected final String loadCacheSelRangeQry;

        /** Select all items query. */
        protected final String loadCacheQry;

        /** Select item query. */
        protected final String loadQrySingle;

        /** Select items query. */
        private final String loadQry;

        /** Merge item(s) query. */
        protected final String mergeQry;

        /** Update item query. */
        protected final String insQry;

        /** Update item query. */
        protected final String updQry;

        /** Remove item(s) query. */
        protected final String remQry;

        /** Max key count for load query per statement. */
        protected final int maxKeysPerStmt;

        /** Database key columns. */
        private final Collection<String> keyCols;

        /** Database unique value columns. */
        private final Collection<String> cols;

        /** Unique value fields. */
        private final Collection<CacheQueryTableColumnMetadata> uniqValFields;

        /** Type metadata. */
        private final CacheQueryTypeMetadata typeMeta;

        /** Table metadata. */
        private final CacheQueryTableMetadata tblMeta;

        /**
         * @param typeMeta Type metadata.
         */
        public EntryMapping(JdbcDialect dialect, CacheQueryTypeMetadata typeMeta) {
            this.dialect = dialect;

            this.typeMeta = typeMeta;

            tblMeta = typeMeta.getTableMetadata();

            final Collection<CacheQueryTableColumnMetadata> keyFields = tblMeta.getKeyColumns();

            Collection<CacheQueryTableColumnMetadata> valFields = tblMeta.getValueColumns();

            uniqValFields = F.view(valFields, new IgnitePredicate<CacheQueryTableColumnMetadata>() {
                @Override public boolean apply(CacheQueryTableColumnMetadata col) {
                    return !keyFields.contains(col);
                }
            });

            String schema = tblMeta.getSchema();

            String tblName = tblMeta.getTable();

            keyCols = databaseColumns(keyFields);

            Collection<String> valCols = databaseColumns(valFields);

            Collection<String> uniqValCols = databaseColumns(uniqValFields);

            cols = F.concat(false, keyCols, uniqValCols);

            loadCacheQry = dialect.loadCacheQuery(schema, tblName, cols);

            loadCacheSelRangeQry = dialect.loadCacheSelectRangeQuery(schema, tblName, keyCols);

            loadQrySingle = dialect.loadQuery(schema, tblName, keyCols, valCols, 1);

            maxKeysPerStmt = dialect.getMaxParamsCnt() / keyCols.size();

            loadQry = dialect.loadQuery(schema, tblName, keyCols, uniqValCols, maxKeysPerStmt);

            insQry = dialect.insertQuery(schema, tblName, keyCols, uniqValCols);

            updQry = dialect.updateQuery(schema, tblName, keyCols, uniqValCols);

            mergeQry = dialect.mergeQuery(schema, tblName, keyCols, uniqValCols);

            remQry = dialect.removeQuery(schema, tblName, keyCols);
        }

        /**
         * Extract database column names from {@link CacheQueryTableColumnMetadata}.
         *
         * @param dsc collection of {@link CacheQueryTableColumnMetadata}.
         */
        private static Collection<String> databaseColumns(Collection<CacheQueryTableColumnMetadata> dsc) {
            return F.transform(dsc, new C1<CacheQueryTableColumnMetadata, String>() {
                /** {@inheritDoc} */
                @Override public String apply(CacheQueryTableColumnMetadata col) {
                    return col.getDbName();
                }
            });
        }

        /**
         * Construct query for select values with key count less or equal {@code maxKeysPerStmt}
         *
         * @param keyCnt Key count.
         */
        protected String loadQuery(int keyCnt) {
            assert keyCnt <= maxKeysPerStmt;

            if (keyCnt == maxKeysPerStmt)
                return loadQry;

            if (keyCnt == 1)
                return loadQrySingle;

            return dialect.loadQuery(tblMeta.getSchema(), tblMeta.getTable(), keyCols, cols, keyCnt);
        }
        /**
         * Construct query for select values in range.
         *
         * @param appendLowerBound Need add lower bound for range.
         * @param appendUpperBound Need add upper bound for range.
         * @return Query with range.
         */
        protected String loadCacheRangeQuery(boolean appendLowerBound, boolean appendUpperBound) {
            return dialect.loadCacheRangeQuery(tblMeta.getSchema(), tblMeta.getTable(), keyCols, cols,
                appendLowerBound, appendUpperBound);
        }

        /** Key type. */
        protected String keyType() {
            return typeMeta.getKeyType();
        }

        /** Value type. */
        protected String valueType() {
            return typeMeta.getType();
        }

        /**
         * Gets key columns.
         *
         * @return Key columns.
         */
        protected Collection<CacheQueryTableColumnMetadata> keyColumns() {
            return tblMeta.getKeyColumns();
        }

        /**
         * Gets value columns.
         *
         * @return Value columns.
         */
        protected Collection<CacheQueryTableColumnMetadata> valueColumns() {
            return tblMeta.getValueColumns();
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
        private final EntryMapping m;

        /** User query. */
        private final String qry;

        /** Closure for loaded values. */
        private final IgniteBiInClosure<K1, V1> clo;

        /**
         * @param m Entry mapping description.
         * @param qry User query.
         * @param clo Closure for loaded values.
         */
        private LoadCacheCustomQueryWorker(EntryMapping m, String qry, IgniteBiInClosure<K1, V1> clo) {
            this.m = m;
            this.qry = qry;
            this.clo = clo;
        }

        /** {@inheritDoc} */
        @Override public Void call() throws Exception {
            Connection conn = null;

            PreparedStatement stmt = null;

            try {
                conn = openConnection(true);

                stmt = conn.prepareStatement(qry);

                ResultSet rs = stmt.executeQuery();

                while (rs.next()) {
                    K1 key = buildObject(m.keyType(), m.keyColumns(), rs);
                    V1 val = buildObject(m.valueType(), m.valueColumns(), rs);

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
        private final EntryMapping m;

        /**
         * @param conn Connection.
         * @param m Entry mapping description.
         */
        private LoadWorker(Connection conn, EntryMapping m) {
            this.conn = conn;
            this.m = m;

            keys = new ArrayList<>(m.maxKeysPerStmt);
        }

        /** {@inheritDoc} */
        @Override public Map<K1, V1> call() throws Exception {
            PreparedStatement stmt = null;

            try {
                stmt = conn.prepareStatement(m.loadQuery(keys.size()));

                int i = 1;

                for (Object key : keys)
                    for (CacheQueryTableColumnMetadata field : m.keyColumns()) {
                        Object fieldVal = extractField(m.keyType(), field.getJavaName(), key);

                        if (fieldVal != null)
                            stmt.setObject(i++, fieldVal);
                        else
                            stmt.setNull(i++, field.getDbType());
                    }

                ResultSet rs = stmt.executeQuery();

                Map<K1, V1> entries = U.newHashMap(keys.size());

                while (rs.next()) {
                    K1 key = buildObject(m.keyType(), m.keyColumns(), rs);
                    V1 val = buildObject(m.valueType(), m.valueColumns(), rs);

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
