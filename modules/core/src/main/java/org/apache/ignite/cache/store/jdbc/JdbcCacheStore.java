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
import org.apache.ignite.cache.store.*;
import org.apache.ignite.cache.store.jdbc.dialect.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.integration.*;
import javax.sql.*;
import java.net.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Base {@link CacheStore} implementation backed by JDBC. This implementation stores objects in underlying database
 * using mapping description.
 */
public abstract class JdbcCacheStore<K, V> extends CacheStore<K, V> {
    /**
     * Query cache by type.
     */
    protected static class QueryCache {
        /** Database dialect. */
        protected final BasicJdbcDialect dialect;

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
        private final Collection<GridCacheQueryTypeDescriptor> uniqValFields;

        /** Type metadata. */
        private final GridCacheQueryTypeMetadata typeMetadata;

        /**
         * @param typeMetadata Type metadata.
         */
        public QueryCache(BasicJdbcDialect dialect, GridCacheQueryTypeMetadata typeMetadata) {
            this.dialect = dialect;

            this.typeMetadata = typeMetadata;

            final Collection<GridCacheQueryTypeDescriptor> keyFields = typeMetadata.getKeyDescriptors();

            Collection<GridCacheQueryTypeDescriptor> valFields = typeMetadata.getValueDescriptors();

            uniqValFields = F.view(typeMetadata.getValueDescriptors(),
                new IgnitePredicate<GridCacheQueryTypeDescriptor>() {
                    @Override public boolean apply(GridCacheQueryTypeDescriptor desc) {
                        return !keyFields.contains(desc);
                    }
                });

            String schema = typeMetadata.getSchema();

            String tblName = typeMetadata.getTableName();

            keyCols = databaseColumns(keyFields);

            Collection<String> valCols = databaseColumns(valFields);

            Collection<String> uniqValCols = databaseColumns(uniqValFields);

            loadCacheQry = dialect.loadCacheQuery(schema, tblName, F.concat(false, keyCols, uniqValCols));

            loadQrySingle = dialect.loadQuery(schema, tblName, keyCols, valCols, 1);

            maxKeysPerStmt = dialect.getMaxParamsCnt() / keyCols.size();

            loadQry = dialect.loadQuery(schema, tblName, keyCols, uniqValCols, maxKeysPerStmt);

            insQry = dialect.insertQuery(schema, tblName, keyCols, uniqValCols);

            updQry = dialect.updateQuery(schema, tblName, keyCols, uniqValCols);

            mergeQry = dialect.mergeQuery(schema, tblName, keyCols, uniqValCols);

            remQry = dialect.removeQuery(schema, tblName, keyCols);

            cols = F.concat(false, keyCols, valCols);
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

            return dialect.loadQuery(typeMetadata.getSchema(), typeMetadata.getTableName(), keyCols, cols, keyCnt);
        }

        /** Key type. */
        protected String keyType() {
            return typeMetadata.getKeyType();
        }

        /** Value type. */
        protected String valueType() {
            return typeMetadata.getType();
        }

        /**
         * Gets key fields type descriptors.
         *
         * @return Key fields type descriptors.
         */
        protected Collection<GridCacheQueryTypeDescriptor> keyDescriptors() {
            return typeMetadata.getKeyDescriptors();
        }

        /**
         * Gets value fields type descriptors.
         *
         * @return Key value type descriptors.
         */
        protected Collection<GridCacheQueryTypeDescriptor> valueDescriptors() {
            return typeMetadata.getValueDescriptors();
        }
    }

    /** Default batch size for put and remove operations. */
    protected static final int DFLT_BATCH_SIZE = 512;

    /** Connection attribute name. */
    protected static final String ATTR_CONN = "JDBC_STORE_CONNECTION";

    /** Auto-injected grid instance. */
    @IgniteInstanceResource
    protected Ignite ignite;

    /** Auto-injected logger instance. */
    @IgniteLoggerResource
    protected IgniteLogger log;

    /** Init guard. */
    @GridToStringExclude
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Init latch. */
    @GridToStringExclude
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /** Successful initialization flag. */
    private boolean initOk;

    /** Data source. */
    protected DataSource dataSrc;

    /** Connection URL. */
    protected String connUrl;

    /** User name for database access. */
    protected String user;

    /** Password for database access. */
    @GridToStringExclude
    protected String passwd;

    /** Execute. */
    protected ExecutorService exec;

    /** Paths to xml with type mapping description. */
    protected Collection<String> typeMetadataPaths;

    /** Type mapping description. */
    protected Collection<GridCacheQueryTypeMetadata> typeMetadata;

    /** Cache with query by type. */
    protected Map<Object, QueryCache> entryQtyCache;

    /** Database dialect. */
    protected BasicJdbcDialect dialect;

    /** Max workers thread count. These threads are responsible for execute query. */
    protected int maxPoolSz = Runtime.getRuntime().availableProcessors();

    /** Maximum batch size for put and remove operations. */
    protected int batchSz = DFLT_BATCH_SIZE;

    /**
     * Perform dialect resolution.
     *
     * @return The resolved dialect.
     * @throws IgniteCheckedException Indicates problems accessing the metadata.
     */
    protected BasicJdbcDialect resolveDialect() throws IgniteCheckedException {
        Connection conn = null;

        String dbProductName = null;

        try {
            conn = openConnection(false);

            dbProductName = conn.getMetaData().getDatabaseProductName();
        }
        catch (SQLException e) {
            throw new IgniteCheckedException("Failed access to metadata for detect database dialect.", e);
        }
        finally {
            closeConnection(conn);
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

        return new BasicJdbcDialect();
    }

    /**
     * Initializes store.
     *
     * @throws IgniteCheckedException If failed to initialize.
     */
    protected void init() throws IgniteCheckedException {
        if (initLatch.getCount() > 0) {
            if (initGuard.compareAndSet(false, true)) {
                if (log.isDebugEnabled())
                    log.debug("Initializing cache store.");

                if (dataSrc == null && F.isEmpty(connUrl))
                    throw new IgniteCheckedException("Failed to initialize cache store (connection is not provided).");

                if (dialect == null)
                    dialect = resolveDialect();

                try {
                    if (typeMetadata == null) {
                        if (typeMetadataPaths == null)
                            throw new IgniteCheckedException(
                                "Failed to initialize cache store (metadata paths is not provided).");

// TODO: IGNITE-32 Replace with reading from config.
//                        GridSpringProcessor spring = SPRING.create(false);

                        Collection<GridCacheQueryTypeMetadata> typeMeta = new ArrayList<>();

                        for (String path : typeMetadataPaths) {
                            URL url = U.resolveGridGainUrl(path);
// TODO: IGNITE-32 Replace with reading from config.
//                            if (url != null) {
//                                Map<String, Object> beans = spring.loadBeans(url, GridCacheQueryTypeMetadata.class).
//                                    get(GridCacheQueryTypeMetadata.class);
//
//                                if (beans != null)
//                                    for (Object bean : beans.values())
//                                        if (bean instanceof GridCacheQueryTypeMetadata)
//                                            typeMeta.add((GridCacheQueryTypeMetadata)bean);
//                            }
//                            else
                                log.warning("Failed to resolve metadata path: " + path);
                        }

                        setTypeMetadata(typeMeta);
                    }

                    exec = Executors.newFixedThreadPool(maxPoolSz);

                    buildTypeCache();

                    initOk = true;
                }
                finally {
                    initLatch.countDown();
                }
            }
            else
                U.await(initLatch);
        }

        if (!initOk)
            throw new IgniteCheckedException("Cache store was not properly initialized.");
    }

    /**
     * Closes allocated resources depending on transaction status.
     *
     * @param tx Active transaction, if any.
     * @param conn Allocated connection.
     * @param st Created statement,
     */
    protected void end(@Nullable IgniteTx tx, @Nullable Connection conn, @Nullable Statement st) {
        U.closeQuiet(st);

        if (tx == null)
            // Close connection right away if there is no transaction.
            closeConnection(conn);
    }

    /**
     * Gets connection from a pool.
     *
     * @param autocommit {@code true} If connection should use autocommit mode.
     * @return Pooled connection.
     * @throws SQLException In case of error.
     */
    protected Connection openConnection(boolean autocommit) throws SQLException {
        Connection conn = dataSrc != null ? dataSrc.getConnection() :
            DriverManager.getConnection(connUrl, user, passwd);

        conn.setAutoCommit(autocommit);

        return conn;
    }

    /**
     * Closes connection.
     *
     * @param conn Connection to close.
     */
    protected void closeConnection(@Nullable Connection conn) {
        U.closeQuiet(conn);
    }

    /**
     * @param tx Cache transaction.
     * @return Connection.
     * @throws SQLException In case of error.
     */
    protected Connection connection(@Nullable IgniteTx tx) throws SQLException {
        if (tx != null) {
            Connection conn = null;// TODO: IGNITE-32 FIXME tx.meta(ATTR_CONN);

            if (conn == null) {
                conn = openConnection(false);

                // Store connection in transaction metadata, so it can be accessed
                // for other operations on the same transaction.
                // TODO: IGNITE-32 FIXME tx.addMeta(ATTR_CONN, conn);
            }

            return conn;
        }
        // Transaction can be null in case of simple load operation.
        else
            return openConnection(true);
    }

    /** {@inheritDoc} */
    public void txEnd(IgniteTx tx, boolean commit) throws IgniteCheckedException {
        init();

        Connection conn = null; // TODO: IGNITE-32 FIXME tx.removeMeta(ATTR_CONN);

        if (conn != null) {
            try {
                if (commit)
                    conn.commit();
                else
                    conn.rollback();
            }
            catch (SQLException e) {
                throw new IgniteCheckedException(
                    "Failed to end transaction [xid=" + tx.xid() + ", commit=" + commit + ']', e);
            }
            finally {
                closeConnection(conn);
            }
        }

        if (log.isDebugEnabled())
            log.debug("Transaction ended [xid=" + tx.xid() + ", commit=" + commit + ']');
    }

    /**
     * Extract database column names from {@link GridCacheQueryTypeDescriptor}.
     *
     * @param dsc collection of {@link GridCacheQueryTypeDescriptor}.
     */
    protected static Collection<String> databaseColumns(Collection<GridCacheQueryTypeDescriptor> dsc) {
        return F.transform(dsc, new C1<GridCacheQueryTypeDescriptor, String>() {
            /** {@inheritDoc} */
            @Override public String apply(GridCacheQueryTypeDescriptor desc) {
                return desc.getDbName();
            }
        });
    }

    /**
     * Get field value from object.
     *
     * @param typeName Type name.
     * @param fieldName Field name.
     * @param obj Cache object.
     * @return Field value from object.
     */
    @Nullable protected abstract Object extractField(String typeName, String fieldName, Object obj)
        throws IgniteCheckedException;

    /**
     * Construct object from query result.
     *
     * @param <R> Type of result object.
     * @param typeName Type name.
     * @param fields Fields descriptors.
     * @param rs ResultSet.
     * @return Constructed object.
     */
    protected abstract <R> R buildObject(String typeName, Collection<GridCacheQueryTypeDescriptor> fields, ResultSet rs)
        throws IgniteCheckedException;

    /**
     * Extract type key from object.
     *
     * @param key Key object.
     * @return Type key.
     */
    protected abstract Object typeKey(K key);

    /**
     * Build cache for mapped types.
     *
     * @throws IgniteCheckedException If failed to initialize.
     */
    protected abstract void buildTypeCache() throws IgniteCheckedException;

    /** {@inheritDoc} */
    @Override public void loadCache(final IgniteBiInClosure<K, V> clo, @Nullable Object... args)
        throws CacheLoaderException {
        try {
            init();

            if (log.isDebugEnabled())
                log.debug("Loading all values from db");

            Collection<Future<?>> futs = new ArrayList<>();

            for (final QueryCache type : entryQtyCache.values())
                futs.add(exec.submit(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        Connection conn = null;

                        try {
                            PreparedStatement stmt = null;

                            try {
                                conn = connection(null);

                                stmt = conn.prepareStatement(type.loadCacheQry);

                                ResultSet rs = stmt.executeQuery();

                                while (rs.next()) {
                                    K key = buildObject(type.keyType(), type.keyDescriptors(), rs);
                                    V val = buildObject(type.valueType(), type.valueDescriptors(), rs);

                                    clo.apply(key, val);
                                }
                            }
                            catch (SQLException e) {
                                throw new IgniteCheckedException("Failed to load cache", e);
                            }
                            finally {
                                U.closeQuiet(stmt);
                            }
                        }
                        finally {
                            closeConnection(conn);
                        }

                        return null;
                    }
                }));

            for (Future<?> fut : futs)
                U.get(fut);
        }
        catch (Exception e) {
            throw new CacheException(e);
        }
    }

    /**
     * @param stmt Prepare statement.
     * @param i Start index for parameters.
     * @param type Type description.
     * @param key Key object.
     * @return Next index for parameters.
     */
    protected int fillKeyParameters(PreparedStatement stmt, int i, QueryCache type,
        K key) throws IgniteCheckedException {
        for (GridCacheQueryTypeDescriptor field : type.keyDescriptors()) {
            Object fieldVal = extractField(type.keyType(), field.getJavaName(), key);

            try {
                if (fieldVal != null)
                    stmt.setObject(i++, fieldVal);
                else
                    stmt.setNull(i++, field.getDbType());
            }
            catch (SQLException e) {
                throw new IgniteCheckedException("Failed to set statement parameter name: " + field.getDbName(), e);
            }
        }

        return i;
    }

    /**
     * @param stmt Prepare statement.
     * @param type Type description.
     * @param key Key object.
     * @return Next index for parameters.
     */
    protected int fillKeyParameters(PreparedStatement stmt, QueryCache type, K key) throws IgniteCheckedException {
        return fillKeyParameters(stmt, 1, type, key);
    }

    /**
     * @param stmt Prepare statement.
     * @param i Start index for parameters.
     * @param type Type description.
     * @param val Value object.
     * @return Next index for parameters.
     */
    protected int fillValueParameters(PreparedStatement stmt, int i, QueryCache type, V val)
        throws IgniteCheckedException {
        for (GridCacheQueryTypeDescriptor field : type.uniqValFields) {
            Object fieldVal = extractField(type.valueType(), field.getJavaName(), val);

            try {
                if (fieldVal != null)
                    stmt.setObject(i++, fieldVal);
                else
                    stmt.setNull(i++, field.getDbType());
            }
            catch (SQLException e) {
                throw new IgniteCheckedException("Failed to set statement parameter name: " + field.getDbName(), e);
            }
        }

        return i;
    }

    /** {@inheritDoc} */
    @Nullable public V load(@Nullable IgniteTx tx, K key) throws IgniteCheckedException {
        init();

        QueryCache type = entryQtyCache.get(typeKey(key));

        if (type == null)
            throw new IgniteCheckedException("Failed to find mapping description for type: " + key.getClass());

        if (log.isDebugEnabled())
            log.debug("Start load value from db by key: " + key);

        Connection conn = null;

        PreparedStatement stmt = null;

        try {
            conn = connection(tx);

            stmt = conn.prepareStatement(type.loadQrySingle);

            fillKeyParameters(stmt, type, key);

            ResultSet rs = stmt.executeQuery();

            if (rs.next())
                return buildObject(type.valueType(), type.valueDescriptors(), rs);
        }
        catch (SQLException e) {
            throw new IgniteCheckedException("Failed to load object by key: " + key, e);
        }
        finally {
            end(tx, conn, stmt);
        }

        return null;
    }

    /**
     * Loads all values for given keys with same type and passes every value to the provided closure.
     *
     * @param tx Cache transaction, if write-behind is not enabled, null otherwise.
     * @param qry Query cache for type.
     * @param keys Collection of keys to load.
     * @param c Closure to call for every loaded element.
     * @throws IgniteCheckedException If load failed.
     */
    protected void loadAll(@Nullable IgniteTx tx, QueryCache qry, Collection<? extends K> keys,
        IgniteBiInClosure<K, V> c) throws IgniteCheckedException {
        init();

        Connection conn = null;

        PreparedStatement stmt = null;

        try {
            conn = connection(tx);

            stmt = conn.prepareStatement(qry.loadQuery(keys.size()));

            int i = 1;

            for (K key : keys) {
                for (GridCacheQueryTypeDescriptor field : qry.keyDescriptors()) {
                    Object fieldVal = extractField(qry.keyType(), field.getJavaName(), key);

                    if (fieldVal != null)
                        stmt.setObject(i++, fieldVal);
                    else
                        stmt.setNull(i++, field.getDbType());
                }
            }

            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                K key = buildObject(qry.keyType(), qry.keyDescriptors(), rs);
                V val = buildObject(qry.valueType(), qry.valueDescriptors(), rs);

                c.apply(key, val);
            }
        }
        catch (SQLException e) {
            throw new IgniteCheckedException("Failed to load objects", e);
        }
        finally {
            end(tx, conn, stmt);
        }
    }

    /** {@inheritDoc} */
    public void loadAll(@Nullable final IgniteTx tx, Collection<? extends K> keys,
        final IgniteBiInClosure<K, V> c) throws IgniteCheckedException {
        assert keys != null;

        init();

        Map<QueryCache, Collection<K>> splittedKeys = U.newHashMap(entryQtyCache.size());

        final Collection<Future<?>> futs = new ArrayList<>();

        for (K key : keys) {
            final QueryCache qry = entryQtyCache.get(typeKey(key));

            Collection<K> batch = splittedKeys.get(qry);

            if (batch == null)
                splittedKeys.put(qry, batch = new ArrayList<>());

            batch.add(key);

            if (batch.size() == qry.maxKeysPerStmt) {
                final Collection<K> p = splittedKeys.remove(qry);

                futs.add(exec.submit(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        loadAll(tx, qry, p, c);

                        return null;
                    }
                }));
            }
        }

        for (final Map.Entry<QueryCache, Collection<K>> entry : splittedKeys.entrySet())
            futs.add(exec.submit(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    loadAll(tx, entry.getKey(), entry.getValue(), c);

                    return null;
                }
            }));

        for (Future<?> fut : futs)
            U.get(fut);
    }

    /** {@inheritDoc} */
    public void put(@Nullable IgniteTx tx, K key, V val) throws IgniteCheckedException {
        init();

        QueryCache type = entryQtyCache.get(typeKey(key));

        if (type == null)
            throw new IgniteCheckedException("Failed to find metadata for type: " + key.getClass());

        if (log.isDebugEnabled())
            log.debug("Start put value in db: (" + key + ", " + val);

        Connection conn = null;

        PreparedStatement stmt = null;

        try {
            conn = connection(tx);

            if (dialect.hasMerge()) {
                stmt = conn.prepareStatement(type.mergeQry);

                int i = fillKeyParameters(stmt, type, key);

                fillValueParameters(stmt, i, type, val);

                stmt.executeUpdate();
            }
            else {
                stmt = conn.prepareStatement(type.updQry);

                int i = fillValueParameters(stmt, 1, type, val);

                fillKeyParameters(stmt, i, type, key);

                if (stmt.executeUpdate() == 0) {
                    stmt.close();

                    stmt = conn.prepareStatement(type.insQry);

                    i = fillKeyParameters(stmt, type, key);

                    fillValueParameters(stmt, i, type, val);

                    stmt.executeUpdate();
                }
            }
        }
        catch (SQLException e) {
            throw new IgniteCheckedException("Failed to put object by key: " + key, e);
        }
        finally {
            end(tx, conn, stmt);
        }
    }

    /**
     * Stores given key value pairs in persistent storage.
     *
     * @param tx Cache transaction, if write-behind is not enabled, null otherwise.
     * @param qry Query cache for type.
     * @param map Values to store.
     * @throws IgniteCheckedException If store failed.
     */
    /** {@inheritDoc} */
    protected void putAll(@Nullable IgniteTx tx, QueryCache qry, Iterable<Map.Entry<? extends K, ? extends V>> map)
        throws IgniteCheckedException {
        assert map != null;

        init();

        Connection conn = null;

        PreparedStatement stmt = null;

        try {
            conn = connection(tx);

            stmt = conn.prepareStatement(qry.mergeQry);

            int cnt = 0;

            for (Map.Entry<? extends K, ? extends V> entry : map) {
                int i = fillKeyParameters(stmt, qry, entry.getKey());

                fillValueParameters(stmt, i, qry, entry.getValue());

                stmt.addBatch();

                if (cnt++ % batchSz == 0)
                    stmt.executeBatch();
            }

            if (cnt % batchSz != 0)
                stmt.executeBatch();
        }
        catch (SQLException e) {
            throw new IgniteCheckedException("Failed to put objects", e);
        }
        finally {
            end(tx, conn, stmt);
        }
    }

    /** {@inheritDoc} */
    public void putAll(@Nullable final IgniteTx tx, Map<? extends K, ? extends V> map)
        throws IgniteCheckedException {
        assert map != null;

        init();

        Map<Object, Collection<Map.Entry<? extends K, ? extends V>>> keyByType = U.newHashMap(entryQtyCache.size());

        if (dialect.hasMerge()) {
            for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
                Object typeKey = typeKey(entry.getKey());

                Collection<Map.Entry<? extends K, ? extends V>> batch = keyByType.get(typeKey);

                if (batch == null)
                    keyByType.put(typeKey, batch = new ArrayList<>());

                batch.add(entry);
            }

            final Collection<Future<?>> futs = new ArrayList<>();

            for (final Map.Entry<Object, Collection<Map.Entry<? extends K, ? extends V>>> e : keyByType.entrySet()) {
                final QueryCache qry = entryQtyCache.get(e.getKey());

                futs.add(exec.submit(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        putAll(tx, qry, e.getValue());

                        return null;
                    }
                }));
            }

            for (Future<?> fut : futs)
                U.get(fut);
        }
        else
            for (Map.Entry<? extends K, ? extends V> e : map.entrySet())
                put(tx, e.getKey(), e.getValue());
    }

    /** {@inheritDoc} */
    public void remove(@Nullable IgniteTx tx, K key) throws IgniteCheckedException {
        init();

        QueryCache type = entryQtyCache.get(typeKey(key));

        if (type == null)
            throw new IgniteCheckedException("Failed to find metadata for type: " + key.getClass());

        if (log.isDebugEnabled())
            log.debug("Start remove value from db by key: " + key);

        Connection conn = null;

        PreparedStatement stmt = null;

        try {
            conn = connection(tx);

            stmt = conn.prepareStatement(type.remQry);

            fillKeyParameters(stmt, type, key);

            stmt.executeUpdate();
        }
        catch (SQLException e) {
            throw new IgniteCheckedException("Failed to load object by key: " + key, e);
        }
        finally {
            end(tx, conn, stmt);
        }
    }

    /**
     * Removes all vales identified by given keys from persistent storage.
     *
     * @param tx Cache transaction, if write-behind is not enabled, null otherwise.
     * @param qry Query cache for type.
     * @param keys Collection of keys to remove.
     * @throws IgniteCheckedException If remove failed.
     */
    protected void removeAll(@Nullable IgniteTx tx, QueryCache qry, Collection<? extends K> keys)
        throws IgniteCheckedException {
        assert keys != null && !keys.isEmpty();

        init();

        if (log.isDebugEnabled())
            log.debug("Start remove values by keys: " + Arrays.toString(keys.toArray()));

        Connection conn = null;

        PreparedStatement stmt = null;

        try {
            conn = connection(tx);

            stmt = conn.prepareStatement(qry.remQry);

            int cnt = 0;

            for (K key : keys) {
                fillKeyParameters(stmt, qry, key);

                stmt.addBatch();

                if (cnt++ % batchSz == 0)
                    stmt.executeBatch();
            }

            if (cnt % batchSz != 0)
                stmt.executeBatch();
        }
        catch (SQLException e) {
            throw new IgniteCheckedException("Failed to remove values by keys.", e);
        }
        finally {
            end(tx, conn, stmt);
        }
    }

    /** {@inheritDoc} */
    public void removeAll(@Nullable IgniteTx tx, Collection<? extends K> keys) throws IgniteCheckedException {
        assert keys != null;

        Map<Object, Collection<K>> keyByType = U.newHashMap(entryQtyCache.size());

        for (K key : keys) {
            Object typeKey = typeKey(key);

            Collection<K> batch = keyByType.get(typeKey);

            if (batch == null)
                keyByType.put(typeKey, batch = new ArrayList<>());

            batch.add(key);
        }

        for (Map.Entry<Object, Collection<K>> entry : keyByType.entrySet()) {
            QueryCache qry = entryQtyCache.get(entry.getKey());

            removeAll(tx, qry, entry.getValue());
        }
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
     * @return Connection URL.
     */
    public String getConnUrl() {
        return connUrl;
    }

    /**
     * @param connUrl Connection URL.
     */
    public void setConnUrl(String connUrl) {
        this.connUrl = connUrl;
    }

    /**
     * @return Password for database access.
     */
    public String getPassword() {
        return passwd;
    }

    /**
     * @param passwd Password for database access.
     */
    public void setPassword(String passwd) {
        this.passwd = passwd;
    }

    /**
     * @return User name for database access.
     */
    public String getUser() {
        return user;
    }

    /**
     * @param user User name for database access.
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * @return Paths to xml with type mapping description.
     */
    public Collection<String> getTypeMetadataPaths() {
        return typeMetadataPaths;
    }

    /**
     * Set paths to xml with type mapping description.
     *
     * @param typeMetadataPaths Paths to xml.
     */
    public void setTypeMetadataPaths(Collection<String> typeMetadataPaths) {
        this.typeMetadataPaths = typeMetadataPaths;
    }

    /**
     * Set type mapping description.
     *
     * @param typeMetadata Type mapping description.
     */
    public void setTypeMetadata(Collection<GridCacheQueryTypeMetadata> typeMetadata) {
        this.typeMetadata = typeMetadata;
    }

    /**
     * Get database dialect.
     *
     * @return Database dialect.
     */
    public BasicJdbcDialect getDialect() {
        return dialect;
    }

    /**
     * Set database dialect.
     *
     * @param dialect Database dialect.
     */
    public void setDialect(BasicJdbcDialect dialect) {
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
     * Get maximum batch size for put and remove operations.
     *
     * @return Maximum batch size.
     */
    public int getBatchSize() {
        return batchSz;
    }

    /**
     * Set maximum batch size for put and remove operations.
     *
     * @param batchSz Maximum batch size.
     */
    public void setBatchSize(int batchSz) {
        this.batchSz = batchSz;
    }

    @Override public void txEnd(boolean commit) throws CacheWriterException {
        // TODO: SPRINT-32 CODE: implement.
    }

    @Override public V load(K k) throws CacheLoaderException {
        return null; // TODO: SPRINT-32 CODE: implement.
    }

    @Override public Map<K, V> loadAll(Iterable<? extends K> iterable) throws CacheLoaderException {
        return null; // TODO: SPRINT-32 CODE: implement.
    }

    @Override public void write(Cache.Entry<? extends K, ? extends V> entry) throws CacheWriterException {
        // TODO: SPRINT-32 CODE: implement.
    }

    @Override
    public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> collection) throws CacheWriterException {
        // TODO: SPRINT-32 CODE: implement.
    }

    @Override public void delete(Object o) throws CacheWriterException {
        // TODO: SPRINT-32 CODE: implement.
    }

    @Override public void deleteAll(Collection<?> collection) throws CacheWriterException {
        // TODO: SPRINT-32 CODE: implement.
    }
}
