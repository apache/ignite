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
import java.util.concurrent.atomic.*;

/**
 * Base {@link CacheStore} implementation backed by JDBC. This implementation stores objects in underlying database
 * using mapping description.
 */
public abstract class JdbcCacheStore<K, V> extends CacheStore<K, V> {
    /**
     * Entry mapping description.
     */
    protected static class EntryMapping {
        /** Database dialect. */
        protected final JdbcDialect dialect;

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
        private final Collection<CacheQueryTypeDescriptor> uniqValFields;

        /** Type metadata. */
        private final CacheQueryTypeMetadata typeMetadata;

        /**
         * @param typeMetadata Type metadata.
         */
        public EntryMapping(JdbcDialect dialect, CacheQueryTypeMetadata typeMetadata) {
            this.dialect = dialect;

            this.typeMetadata = typeMetadata;

            final Collection<CacheQueryTypeDescriptor> keyFields = typeMetadata.getKeyDescriptors();

            Collection<CacheQueryTypeDescriptor> valFields = typeMetadata.getValueDescriptors();

            uniqValFields = F.view(typeMetadata.getValueDescriptors(),
                new IgnitePredicate<CacheQueryTypeDescriptor>() {
                    @Override public boolean apply(CacheQueryTypeDescriptor desc) {
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
        protected Collection<CacheQueryTypeDescriptor> keyDescriptors() {
            return typeMetadata.getKeyDescriptors();
        }

        /**
         * Gets value fields type descriptors.
         *
         * @return Key value type descriptors.
         */
        protected Collection<CacheQueryTypeDescriptor> valueDescriptors() {
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

    /** Type mapping description. */
    protected Collection<CacheQueryTypeMetadata> typeMetadata;

    /** Cache with query by type. */
    protected Map<Object, EntryMapping> typeMeta;

    /** Database dialect. */
    protected JdbcDialect dialect;

    /** Max workers thread count. These threads are responsible for execute query. */
    protected int maxPoolSz = Runtime.getRuntime().availableProcessors();

    /** Maximum batch size for put and remove operations. */
    protected int batchSz = DFLT_BATCH_SIZE;

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
    protected abstract <R> R buildObject(String typeName, Collection<CacheQueryTypeDescriptor> fields, ResultSet rs)
        throws CacheLoaderException;

    /**
     * Extract type key from object.
     *
     * @param key Key object.
     * @return Type key.
     * @throws CacheException If failed to extract type key.
     */
    protected abstract Object typeId(Object key) throws CacheException;

    /**
     * Build cache for mapped types.
     *
     * @throws CacheException If failed to initialize.
     */
    protected abstract void buildTypeCache() throws CacheException;

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

        return new BasicJdbcDialect();
    }

    /**
     * Initializes store.
     *
     * @throws CacheException If failed to initialize.
     */
    protected void init() throws CacheException {
        if (initLatch.getCount() > 0) {
            if (initGuard.compareAndSet(false, true)) {
                if (log.isDebugEnabled())
                    log.debug("Initializing cache store.");

                if (dataSrc == null && F.isEmpty(connUrl))
                    throw new CacheException("Failed to initialize cache store (connection is not provided).");

                if (dialect == null)
                    dialect = resolveDialect();

                try {
                    if (typeMetadata == null)
                        throw new CacheException("Failed to initialize cache store (mappping description is not provided).");

                    exec = Executors.newFixedThreadPool(maxPoolSz);

                    buildTypeCache();

                    initOk = true;
                }
                finally {
                    initLatch.countDown();
                }
            }
            else
                try {
                    if (initLatch.getCount() > 0)
                        initLatch.await();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new CacheException(e);
                }
        }

        if (!initOk)
            throw new CacheException("Cache store was not properly initialized.");
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
     * @return Connection.
     * @throws SQLException In case of error.
     */
    protected Connection connection() throws SQLException {
        CacheStoreSession ses = session();

        if (ses.transaction() != null) {
            Map<String, Connection> prop = ses.properties();

            Connection conn = prop.get(ATTR_CONN);

            if (conn == null) {
                conn = openConnection(false);

                // Store connection in session, so it can be accessed
                // for other operations on the same session.
                prop.put(ATTR_CONN, conn);
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
    @Override public void loadCache(final IgniteBiInClosure<K, V> clo, @Nullable Object... args)
        throws CacheLoaderException {
        try {
            init();

            if (log.isDebugEnabled())
                log.debug("Loading all values from db");

            Collection<Future<?>> futs = new ArrayList<>();

            for (final EntryMapping type : typeMeta.values())
                futs.add(exec.submit(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                    Connection conn = null;

                    try {
                        PreparedStatement stmt = null;

                        try {
                            conn = connection();

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
                        U.closeQuiet(conn);
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

    /** {@inheritDoc} */
    @Override public void txEnd(boolean commit) throws CacheWriterException {
        CacheStoreSession ses = session();

        IgniteTx tx = ses.transaction();

        Connection conn = ses.<String, Connection>properties().remove(ATTR_CONN);


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

    /** {@inheritDoc} */
    @Nullable @Override public V load(K key) throws CacheLoaderException {
        assert key != null;

        init();

        EntryMapping type = typeMeta.get(typeId(key));

        if (type == null)
            throw new CacheLoaderException("Failed to find store mapping description for key: " + key);

        if (log.isDebugEnabled())
            log.debug("Start load value from database by key: " + key);

        Connection conn = null;

        PreparedStatement stmt = null;

        try {
            conn = connection();

            stmt = conn.prepareStatement(type.loadQrySingle);

            fillKeyParameters(stmt, type, key);

            ResultSet rs = stmt.executeQuery();

            if (rs.next())
                return buildObject(type.valueType(), type.valueDescriptors(), rs);
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

        init();

        Connection conn = null;
        try {
            conn = connection();

            Map<Object, LoadWorker<K, V>> workers = U.newHashMap(typeMeta.size());

            Collection<Future<Map<K, V>>> futs = new ArrayList<>();

            int cnt = 0;

            for (K key : keys) {
                Object typeId = typeId(key);

                final EntryMapping m = typeMeta.get(typeId);

                if (m == null)
                    throw new CacheWriterException("Failed to find store mapping description for key: " + key);

                LoadWorker<K, V> worker = workers.get(typeId);

                if (worker == null)
                    workers.put(typeId, worker = new LoadWorker<>(conn, m));

                worker.keys.add(key);

                if (worker.keys.size() == m.maxKeysPerStmt)
                    futs.add(exec.submit(workers.remove(typeId)));

                cnt ++;
            }

            for (LoadWorker<K, V> worker : workers.values())
                futs.add(exec.submit(worker));

            Map<K, V> res = U.newHashMap(cnt);

            for (Future<Map<K, V>> fut : futs)
                res.putAll(U.get(fut));

            return res;
        }
        catch (SQLException e) {
            throw new CacheWriterException("Failed to open connection", e);
        }
        catch (IgniteCheckedException e) {
            throw new CacheWriterException("Failed to load entries from database", e);
        }
        finally {
            closeConnection(conn);
        }
    }

    /** {@inheritDoc} */
    @Override public void write(Cache.Entry<? extends K, ? extends V> entry) throws CacheWriterException {
        assert entry != null;

        init();

        K key = entry.getKey();

        EntryMapping type = typeMeta.get(typeId(key));

        if (type == null)
            throw new CacheWriterException("Failed to find store mapping description for entry: " + entry);

        if (log.isDebugEnabled())
            log.debug("Start write entry to database: " + entry);

        Connection conn = null;

        PreparedStatement stmt = null;

        try {
            conn = connection();

            if (dialect.hasMerge()) {
                stmt = conn.prepareStatement(type.mergeQry);

                int i = fillKeyParameters(stmt, type, key);

                fillValueParameters(stmt, i, type, entry.getValue());

                stmt.executeUpdate();
            }
            else {
                V val = entry.getValue();

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

        init();

        Connection conn = null;

        try {
            conn = connection();

            if (dialect.hasMerge()) {
                Map<Object, WriteWorker> workers = U.newHashMap(typeMeta.size());

                Collection<Future<?>> futs = new ArrayList<>();

                for (Cache.Entry<? extends K, ? extends V> entry : entries) {
                    Object typeId = typeId(entry.getKey());

                    final EntryMapping m = typeMeta.get(typeId);

                    if (m == null)
                        throw new CacheWriterException("Failed to find store mapping description for key: " +
                            entry.getKey());

                    WriteWorker worker = workers.get(typeId);

                    if (worker == null)
                        workers.put(typeId, worker = new WriteWorker(conn, m));

                    worker.entries.add(entry);

                    if (worker.entries.size() == batchSz)
                        futs.add(exec.submit(workers.remove(typeId)));
                }

                for (WriteWorker worker : workers.values())
                    futs.add(exec.submit(worker));

                for (Future<?> fut : futs)
                    U.get(fut);
            }
            else {
                Map<Object, T2<PreparedStatement, PreparedStatement>> stmtByType = U.newHashMap(typeMeta.size());

                for (Cache.Entry<? extends K, ? extends V> entry : entries) {
                    Object typeId = typeId(entry.getKey());

                    final EntryMapping m = typeMeta.get(typeId);

                    if (m == null)
                        throw new CacheWriterException("Failed to find store mapping description for key: " +
                            entry.getKey());

                    T2<PreparedStatement, PreparedStatement> stmts = stmtByType.get(typeId);

                    if (stmts == null)
                        stmtByType.put(typeId,
                            stmts = new T2<>(conn.prepareStatement(m.updQry), conn.prepareStatement(m.insQry)));

                    PreparedStatement stmt = stmts.get1();

                    assert stmt != null;

                    int i = fillValueParameters(stmt, 1, m, entry.getValue());

                    fillKeyParameters(stmt, i, m, entry.getKey());

                    if (stmt.executeUpdate() == 0) {
                        stmt = stmts.get2();

                        assert stmt != null;

                        i = fillKeyParameters(stmt, m, entry.getKey());

                        fillValueParameters(stmt, i, m, entry.getValue());

                        stmt.executeUpdate();
                    }
                }

                for (T2<PreparedStatement, PreparedStatement> stmts :  stmtByType.values()) {
                    U.closeQuiet(stmts.get1());

                    U.closeQuiet(stmts.get2());
                }
            }
        }
        catch (SQLException e) {
            throw new CacheWriterException("Failed to open connection", e);
        }
        catch (IgniteCheckedException e) {
            throw new CacheWriterException("Failed to write values into database", e);
        }
        finally {
            closeConnection(conn);
        }
    }

    /** {@inheritDoc} */
    @Override public void delete(Object key) throws CacheWriterException {
        assert key != null;

        init();

        EntryMapping type = typeMeta.get(typeId(key));

        if (type == null)
            throw new CacheWriterException("Failed to find store mapping description for key: " + key);

        if (log.isDebugEnabled())
            log.debug("Start remove value from database by key: " + key);

        Connection conn = null;

        PreparedStatement stmt = null;

        try {
            conn = connection();

            stmt = conn.prepareStatement(type.remQry);

            fillKeyParameters(stmt, type, key);

            stmt.executeUpdate();
        }
        catch (SQLException e) {
            throw new CacheWriterException("Failed to remove value from database by key: " + key, e);
        }
        finally {
            end(conn, stmt);
        }
    }

    /** {@inheritDoc} */
    @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
        assert keys != null;

        Connection conn = null;

        try {
            conn = connection();

            Collection<Future<?>> futs = new ArrayList<>();

            Map<Object, DeleteWorker> workers = U.newHashMap(typeMeta.size());

            for (Object key : keys) {
                Object typeId = typeId(key);

                final EntryMapping m = typeMeta.get(typeId);

                if (m == null)
                    throw new CacheWriterException("Failed to find store mapping description for key: " + key);

                DeleteWorker worker = workers.get(typeId);

                if (worker == null)
                    workers.put(typeId, worker = new DeleteWorker(conn, m));

                worker.keys.add(key);

                if (worker.keys.size() == batchSz)
                    futs.add(exec.submit(workers.remove(typeId)));
            }

            for (DeleteWorker worker : workers.values())
                futs.add(exec.submit(worker));

            for (Future<?> fut : futs)
                U.get(fut);
        }
        catch (SQLException e) {
            throw new CacheWriterException("Failed to open connection", e);
        }
        catch (IgniteCheckedException e) {
            throw new CacheWriterException("Failed to remove values from database", e);
        }
        finally {
            closeConnection(conn);
        }
    }

    /**
     * Extract database column names from {@link CacheQueryTypeDescriptor}.
     *
     * @param dsc collection of {@link CacheQueryTypeDescriptor}.
     */
    protected static Collection<String> databaseColumns(Collection<CacheQueryTypeDescriptor> dsc) {
        return F.transform(dsc, new C1<CacheQueryTypeDescriptor, String>() {
            /** {@inheritDoc} */
            @Override public String apply(CacheQueryTypeDescriptor desc) {
                return desc.getDbName();
            }
        });
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
        for (CacheQueryTypeDescriptor field : type.keyDescriptors()) {
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
     * @param type Type description.
     * @param key Key object.
     * @return Next index for parameters.
     */
    protected int fillKeyParameters(PreparedStatement stmt, EntryMapping type, Object key) throws CacheException {
        return fillKeyParameters(stmt, 1, type, key);
    }

    /**
     * @param stmt Prepare statement.
     * @param i Start index for parameters.
     * @param type Type description.
     * @param val Value object.
     * @return Next index for parameters.
     */
    protected int fillValueParameters(PreparedStatement stmt, int i, EntryMapping type, Object val)
        throws CacheWriterException {
        for (CacheQueryTypeDescriptor field : type.uniqValFields) {
            Object fieldVal = extractField(type.valueType(), field.getJavaName(), val);

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
     * Set type mapping description.
     *
     * @param typeMetadata Type mapping description.
     */
    public void setTypeMetadata(Collection<CacheQueryTypeMetadata> typeMetadata) {
        this.typeMetadata = typeMetadata;
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

    private class LoadWorker<K1, V1> implements Callable<Map<K1, V1>> {
        private final Connection conn;

        private final Collection<K1> keys;

        private final EntryMapping m;

        private LoadWorker(Connection conn, EntryMapping m) {
            this.conn = conn;
            keys = new ArrayList<>(batchSz);
            this.m = m;
        }

        /** {@inheritDoc} */
        @Override public Map<K1, V1> call() throws Exception {
            PreparedStatement stmt = null;

            try {
                stmt = conn.prepareStatement(m.loadQuery(keys.size()));

                int i = 1;

                for (Object key : keys)
                    for (CacheQueryTypeDescriptor field : m.keyDescriptors()) {
                        Object fieldVal = extractField(m.keyType(), field.getJavaName(), key);

                        if (fieldVal != null)
                            stmt.setObject(i++, fieldVal);
                        else
                            stmt.setNull(i++, field.getDbType());
                    }

                ResultSet rs = stmt.executeQuery();

                Map<K1, V1> entries = U.newHashMap(keys.size());

                while (rs.next()) {
                    K1 key = buildObject(m.keyType(), m.keyDescriptors(), rs);
                    V1 val = buildObject(m.valueType(), m.valueDescriptors(), rs);

                    entries.put(key, val);
                }

                return entries;
            }
            finally {
                U.closeQuiet(stmt);
            }
        }
    }

    private class WriteWorker implements Callable<Void> {
        private final Connection conn;

        private final Collection<Cache.Entry<?, ?>> entries;

        private final EntryMapping m;

        private WriteWorker(Connection conn, EntryMapping m) {
            this.conn = conn;
            entries = new ArrayList<>(batchSz);
            this.m = m;
        }

        /** {@inheritDoc} */
        @Override public Void call() throws Exception {
            PreparedStatement stmt = null;

            try {
                stmt = conn.prepareStatement(m.mergeQry);

                for (Cache.Entry<?, ?> entry : entries) {
                    int i = fillKeyParameters(stmt, m, entry.getKey());

                    fillValueParameters(stmt, i, m, entry.getValue());

                    stmt.addBatch();
                }

                stmt.executeBatch();
            }
            finally {
                U.closeQuiet(stmt);
            }

            return null;
        }
    }

    private class DeleteWorker implements Callable<Void> {
        private final Connection conn;

        private final Collection<Object> keys;

        private final EntryMapping m;

        private DeleteWorker(Connection conn, EntryMapping m) {
            this.conn = conn;
            keys = new ArrayList<>(batchSz);
            this.m = m;
        }

        /** {@inheritDoc} */
        @Override public Void call() throws Exception {
            PreparedStatement stmt = null;

            try {
                stmt = conn.prepareStatement(m.remQry);

                for (Object key : keys) {
                    fillKeyParameters(stmt, m, key);

                    stmt.addBatch();
                }

                stmt.executeBatch();
            }
            finally {
                U.closeQuiet(stmt);
            }

            return null;
        }
    }
}
