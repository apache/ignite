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
import java.util.HashSet;
import java.util.List;
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
import org.apache.ignite.binary.BinaryObject;
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
import org.apache.ignite.internal.binary.BinaryMarshaller;
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
import static org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory.DFLT_BATCH_SIZE;
import static org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory.DFLT_PARALLEL_LOAD_CACHE_MINIMUM_THRESHOLD;
import static org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory.DFLT_WRITE_ATTEMPTS;
import static org.apache.ignite.cache.store.jdbc.JdbcTypesTransformer.NUMERIC_TYPES;

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
 *    // Create store factory.
 *    CacheJdbcPojoStoreFactory storeFactory = new CacheJdbcPojoStoreFactory();
 *    storeFactory.setDataSourceBean("your_data_source_name");
 *    storeFactory.setDialect(new H2Dialect());
 *    storeFactory.setTypes(array_with_your_types);
 *    ...
 *    ccfg.setCacheStoreFactory(storeFactory);
 *    ccfg.setReadThrough(true);
 *    ccfg.setWriteThrough(true);
 *
 *    cfg.setCacheConfiguration(ccfg);
 *    ...
 * </pre>
 */
public abstract class CacheAbstractJdbcStore<K, V> implements CacheStore<K, V>, LifecycleAware {
    /** Connection attribute property name. */
    protected static final String ATTR_CONN_PROP = "JDBC_STORE_CONNECTION";

    /** Built in Java types names. */
    protected static final Collection<String> BUILT_IN_TYPES = new HashSet<>();

    static {
        BUILT_IN_TYPES.add("java.math.BigDecimal");
        BUILT_IN_TYPES.add("java.lang.Boolean");
        BUILT_IN_TYPES.add("java.lang.Byte");
        BUILT_IN_TYPES.add("java.lang.Character");
        BUILT_IN_TYPES.add("java.lang.Double");
        BUILT_IN_TYPES.add("java.util.Date");
        BUILT_IN_TYPES.add("java.sql.Date");
        BUILT_IN_TYPES.add("java.lang.Float");
        BUILT_IN_TYPES.add("java.lang.Integer");
        BUILT_IN_TYPES.add("java.lang.Long");
        BUILT_IN_TYPES.add("java.lang.Short");
        BUILT_IN_TYPES.add("java.lang.String");
        BUILT_IN_TYPES.add("java.sql.Timestamp");
        BUILT_IN_TYPES.add("java.util.UUID");
    }

    /** Auto-injected store session. */
    @CacheStoreSessionResource
    private CacheStoreSession ses;

    /** Auto injected ignite instance. */
    @IgniteInstanceResource
    protected Ignite ignite;

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

    /** Maximum batch size for writeAll and deleteAll operations. */
    private int batchSize = DFLT_BATCH_SIZE;

    /** Database dialect. */
    protected JdbcDialect dialect;

    /** Maximum write attempts in case of database error. */
    private int maxWrtAttempts = DFLT_WRITE_ATTEMPTS;

    /** Max workers thread count. These threads are responsible for load cache. */
    private int maxPoolSize = Runtime.getRuntime().availableProcessors();

    /** Parallel load cache minimum threshold. If {@code 0} then load sequentially. */
    private int parallelLoadCacheMinThreshold = DFLT_PARALLEL_LOAD_CACHE_MINIMUM_THRESHOLD;

    /** Types that store could process. */
    private JdbcType[] types;

    /** Hash calculator.  */
    protected JdbcTypeHasher hasher = JdbcTypeDefaultHasher.INSTANCE;

    /** Types transformer. */
    protected JdbcTypesTransformer transformer = JdbcTypesDefaultTransformer.INSTANCE;

    /** Flag indicating that table and field names should be escaped in all SQL queries created by JDBC POJO store. */
    private boolean sqlEscapeAll;

    /**
     * Get field value from object for use as query parameter.
     *
     * @param cacheName Cache name.
     * @param typeName Type name.
     * @param typeKind Type kind.
     * @param fieldName Field name.
     * @param obj Cache object.
     * @return Field value from object.
     * @throws CacheException in case of error.
     */
    @Nullable protected abstract Object extractParameter(@Nullable String cacheName, String typeName, TypeKind typeKind,
        String fieldName, Object obj) throws CacheException;

    /**
     * Construct object from query result.
     *
     * @param <R> Type of result object.
     * @param cacheName Cache name.
     * @param typeName Type name.
     * @param typeKind Type kind.
     * @param flds Fields descriptors.
     * @param hashFlds Field names for hash code calculation.
     * @param loadColIdxs Select query columns index.
     * @param rs ResultSet.
     * @return Constructed object.
     * @throws CacheLoaderException If failed to construct cache object.
     */
    protected abstract <R> R buildObject(@Nullable String cacheName, String typeName, TypeKind typeKind,
        JdbcTypeField[] flds, Collection<String> hashFlds, Map<String, Integer> loadColIdxs, ResultSet rs)
        throws CacheLoaderException;

    /**
     * Calculate type ID for object.
     *
     * @param obj Object to calculate type ID for.
     * @return Type ID.
     * @throws CacheException If failed to calculate type ID for given object.
     */
    protected abstract Object typeIdForObject(Object obj) throws CacheException;

    /**
     * Calculate type ID for given type name.
     *
     * @param kind If {@code true} then calculate type ID for POJO otherwise for binary object .
     * @param typeName String description of type name.
     * @return Type ID.
     * @throws CacheException If failed to get type ID for given type name.
     */
    protected abstract Object typeIdForTypeName(TypeKind kind, String typeName) throws CacheException;

    /**
     * Prepare internal store specific builders for provided types metadata.
     *
     * @param cacheName Cache name to prepare builders for.
     * @param types Collection of types.
     * @throws CacheException If failed to prepare internal builders for types.
     */
    protected abstract void prepareBuilders(@Nullable String cacheName, Collection<JdbcType> types)
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
            Map<String, Connection> sesProps = ses.properties();

            Connection conn = sesProps.get(ATTR_CONN_PROP);

            if (conn != null) {
                sesProps.remove(ATTR_CONN_PROP);

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

            if (log.isDebugEnabled())
                log.debug("Transaction ended [xid=" + tx.xid() + ", commit=" + commit + ']');
        }
    }

    /**
     * Construct load cache from range.
     *
     * @param em Type mapping description.
     * @param clo Closure that will be applied to loaded values.
     * @param lowerBound Lower bound for range.
     * @param upperBound Upper bound for range.
     * @param fetchSize Number of rows to fetch from DB.
     * @return Callable for pool submit.
     */
    private Callable<Void> loadCacheRange(final EntryMapping em, final IgniteBiInClosure<K, V> clo,
        @Nullable final Object[] lowerBound, @Nullable final Object[] upperBound, final int fetchSize) {
        return new Callable<Void>() {
            @Override public Void call() throws Exception {
                Connection conn = null;

                PreparedStatement stmt = null;

                try {
                    conn = openConnection(true);

                    stmt = conn.prepareStatement(lowerBound == null && upperBound == null
                        ? em.loadCacheQry
                        : em.loadCacheRangeQuery(lowerBound != null, upperBound != null));

                    stmt.setFetchSize(fetchSize);

                    int idx = 1;

                    if (lowerBound != null)
                        for (int i = lowerBound.length; i > 0; i--)
                            for (int j = 0; j < i; j++)
                                stmt.setObject(idx++, lowerBound[j]);

                    if (upperBound != null)
                        for (int i = upperBound.length; i > 0; i--)
                            for (int j = 0; j < i; j++)
                                stmt.setObject(idx++, upperBound[j]);

                    ResultSet rs = stmt.executeQuery();

                    while (rs.next()) {
                        K key = buildObject(em.cacheName, em.keyType(), em.keyKind(), em.keyColumns(), em.keyCols, em.loadColIdxs, rs);
                        V val = buildObject(em.cacheName, em.valueType(), em.valueKind(), em.valueColumns(), null, em.loadColIdxs, rs);

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
        return loadCacheRange(m, clo, null, null, dialect.getFetchSize());
    }

    /**
     * Checks if type configured properly.
     *
     * @param cacheName Cache name to check mapping for.
     * @param typeName Type name.
     * @param flds Fields descriptors.
     * @throws CacheException If failed to check type configuration.
     */
    private void checkTypeConfiguration(@Nullable String cacheName, TypeKind kind, String typeName,
        JdbcTypeField[] flds) throws CacheException {
        try {
            if (kind == TypeKind.BUILT_IN) {
                if (flds.length != 1)
                    throw new CacheException("More than one field for built in type " +
                        "[cache=" + U.maskName(cacheName) + ", type=" + typeName + " ]");

                JdbcTypeField field = flds[0];

                if (field.getDatabaseFieldName() == null)
                    throw new CacheException("Missing database name in mapping description [cache=" +
                        U.maskName(cacheName) + ", type=" + typeName + " ]");

                field.setJavaFieldType(Class.forName(typeName));
            }
            else
                for (JdbcTypeField field : flds) {
                    if (field.getDatabaseFieldName() == null)
                        throw new CacheException("Missing database name in mapping description " +
                            "[cache=" + U.maskName(cacheName) + ", type=" + typeName + " ]");

                    if (field.getJavaFieldName() == null)
                        throw new CacheException("Missing field name in mapping description " +
                            "[cache=" + U.maskName(cacheName) + ", type=" + typeName + " ]");

                    if (field.getJavaFieldType() == null)
                        throw new CacheException("Missing field type in mapping description " +
                            "[cache=" + U.maskName(cacheName) + ", type=" + typeName + " ]");
                }
        }
        catch (ClassNotFoundException e) {
            throw new CacheException("Failed to find class: " + typeName, e);
        }
    }

    /**
     * For backward compatibility translate old field type descriptors to new format.
     *
     * @param oldFlds Fields in old format.
     * @return Fields in new format.
     */
    @Deprecated
    private JdbcTypeField[] translateFields(Collection<CacheTypeFieldMetadata> oldFlds) {
        JdbcTypeField[] newFlds = new JdbcTypeField[oldFlds.size()];

        int idx = 0;

        for (CacheTypeFieldMetadata oldField : oldFlds) {
            newFlds[idx] = new JdbcTypeField(oldField.getDatabaseType(), oldField.getDatabaseName(),
                oldField.getJavaType(), oldField.getJavaName());

            idx++;
        }

        return newFlds;
    }

    /**
     * @param type Type name to check.
     * @return {@code True} if class not found.
     */
    protected TypeKind kindForName(String type) {
        if (BUILT_IN_TYPES.contains(type))
            return TypeKind.BUILT_IN;

        try {
            Class.forName(type);

            return TypeKind.POJO;
        }
        catch(ClassNotFoundException ignored) {
            return TypeKind.BINARY;
        }
    }

    /**
     * @param cacheName Cache name to check mappings for.
     * @return Type mappings for specified cache name.
     * @throws CacheException If failed to initialize cache mappings.
     */
    private Map<Object, EntryMapping> getOrCreateCacheMappings(@Nullable String cacheName) throws CacheException {
        Map<Object, EntryMapping> entryMappings = cacheMappings.get(cacheName);

        if (entryMappings != null)
            return entryMappings;

        cacheMappingsLock.lock();
        try {
            entryMappings = cacheMappings.get(cacheName);

            if (entryMappings != null)
                return entryMappings;

            // If no types configured, check CacheTypeMetadata for backward compatibility.
            if (types == null) {
                CacheConfiguration ccfg = ignite.cache(cacheName).getConfiguration(CacheConfiguration.class);

                Collection<CacheTypeMetadata> oldTypes = ccfg.getTypeMetadata();

                types = new JdbcType[oldTypes.size()];

                int idx = 0;

                for (CacheTypeMetadata oldType : oldTypes) {
                    JdbcType newType = new JdbcType();

                    newType.setCacheName(cacheName);

                    newType.setDatabaseSchema(oldType.getDatabaseSchema());
                    newType.setDatabaseTable(oldType.getDatabaseTable());

                    newType.setKeyType(oldType.getKeyType());
                    newType.setKeyFields(translateFields(oldType.getKeyFields()));

                    newType.setValueType(oldType.getValueType());
                    newType.setValueFields(translateFields(oldType.getValueFields()));

                    types[idx] = newType;

                    idx++;
                }
            }

            List<JdbcType> cacheTypes = new ArrayList<>(types.length);

            for (JdbcType type : types)
                if ((cacheName != null && cacheName.equals(type.getCacheName())) ||
                    (cacheName == null && type.getCacheName() == null))
                    cacheTypes.add(type);

            entryMappings = U.newHashMap(cacheTypes.size());

            if (!cacheTypes.isEmpty()) {
                boolean binarySupported = ignite.configuration().getMarshaller() instanceof BinaryMarshaller;

                for (JdbcType type : cacheTypes) {
                    String keyType = type.getKeyType();
                    String valType = type.getValueType();

                    TypeKind keyKind = kindForName(keyType);

                    if (!binarySupported && keyKind == TypeKind.BINARY)
                        throw new CacheException("Key type has no class [cache=" + U.maskName(cacheName) +
                            ", type=" + keyType + "]");

                    checkTypeConfiguration(cacheName, keyKind, keyType, type.getKeyFields());

                    Object keyTypeId = typeIdForTypeName(keyKind, keyType);

                    if (entryMappings.containsKey(keyTypeId))
                        throw new CacheException("Key type must be unique in type metadata [cache=" +
                            U.maskName(cacheName) + ", type=" + keyType + "]");

                    TypeKind valKind = kindForName(valType);

                    checkTypeConfiguration(cacheName, valKind, valType, type.getValueFields());

                    entryMappings.put(keyTypeId, new EntryMapping(cacheName, dialect, type, keyKind, valKind, sqlEscapeAll));

                    // Add one more binding to binary typeId for POJOs,
                    // because object could be passed to store in binary format.
                    if (binarySupported && keyKind == TypeKind.POJO) {
                        keyTypeId = typeIdForTypeName(TypeKind.BINARY, keyType);

                        valKind = valKind == TypeKind.POJO ? TypeKind.BINARY : valKind;

                        entryMappings.put(keyTypeId, new EntryMapping(cacheName, dialect, type, TypeKind.BINARY, valKind, sqlEscapeAll));
                    }
                }

                Map<String, Map<Object, EntryMapping>> mappings = new HashMap<>(cacheMappings);

                mappings.put(cacheName, entryMappings);

                prepareBuilders(cacheName, cacheTypes);

                cacheMappings = mappings;
            }

            return entryMappings;
        }
        finally {
            cacheMappingsLock.unlock();
        }
    }

    /**
     * @param cacheName Cache name.
     * @param typeId Type id.
     * @return Entry mapping.
     * @throws CacheException If mapping for key was not found.
     */
    private EntryMapping entryMapping(String cacheName, Object typeId) throws CacheException {
        Map<Object, EntryMapping> mappings = getOrCreateCacheMappings(cacheName);

        EntryMapping em = mappings.get(typeId);

        if (em == null) {
            String maskedCacheName = U.maskName(cacheName);

            throw new CacheException("Failed to find mapping description [cache=" + maskedCacheName +
                ", typeId=" + typeId + "]. Please configure JdbcType to associate cache '" + maskedCacheName +
                "' with JdbcPojoStore.");
        }

        return em;
    }

    /**
     * Find column index by database name.
     *
     * @param loadColIdxs Select query columns indexes.
     * @param dbName Column name in database.
     * @return Column index.
     * @throws IllegalStateException if column not found.
     */
    protected Integer columnIndex(Map<String, Integer> loadColIdxs, String dbName) {
        Integer colIdx = loadColIdxs.get(dbName.toUpperCase());

        if (colIdx == null)
            throw new IllegalStateException("Failed to find column index for database field: " + dbName);

        return colIdx;
    }

    /** {@inheritDoc} */
    @Override public void loadCache(final IgniteBiInClosure<K, V> clo, @Nullable Object... args)
        throws CacheLoaderException {
        ExecutorService pool = null;

        String cacheName = session().cacheName();

        try {
            pool = Executors.newFixedThreadPool(maxPoolSize);

            Collection<Future<?>> futs = new ArrayList<>();

            Map<Object, EntryMapping> mappings = getOrCreateCacheMappings(cacheName);

            if (args != null && args.length > 0) {
                if (args.length % 2 != 0)
                    throw new CacheLoaderException("Expected even number of arguments, but found: " + args.length);

                if (log.isDebugEnabled())
                    log.debug("Start loading entries from db using user queries from arguments...");

                for (int i = 0; i < args.length; i += 2) {
                    final String keyType = args[i].toString();

                    if (!F.exist(mappings.values(), new IgnitePredicate<EntryMapping>() {
                        @Override public boolean apply(EntryMapping em) {
                            return em.keyType().equals(keyType);
                        }
                    }))
                        throw new CacheLoaderException("Provided key type is not found in store or cache configuration " +
                            "[cache=" + U.maskName(cacheName) + ", key=" + keyType + "]");

                    String qry = args[i + 1].toString();

                    EntryMapping em = entryMapping(cacheName, typeIdForTypeName(kindForName(keyType), keyType));

                    if (log.isInfoEnabled())
                        log.info("Started load cache using custom query [cache=" + U.maskName(cacheName) +
                            ", keyType=" + keyType + ", query=" + qry + "]");

                    futs.add(pool.submit(new LoadCacheCustomQueryWorker<>(em, qry, clo)));
                }
            }
            else {
                Collection<String> processedKeyTypes = new HashSet<>();

                for (EntryMapping em : mappings.values()) {
                    String keyType = em.keyType();

                    if (processedKeyTypes.contains(keyType))
                        continue;

                    processedKeyTypes.add(keyType);

                    if (log.isInfoEnabled())
                        log.info("Started load cache [cache=" + U.maskName(cacheName) + ", keyType=" + keyType + "]");

                    if (parallelLoadCacheMinThreshold > 0) {
                        Connection conn = null;

                        try {
                            conn = connection();

                            PreparedStatement stmt = conn.prepareStatement(em.loadCacheSelRangeQry);

                            stmt.setInt(1, parallelLoadCacheMinThreshold);

                            ResultSet rs = stmt.executeQuery();

                            if (rs.next()) {
                                if (log.isDebugEnabled())
                                    log.debug("Multithread loading entries from db [cache=" + U.maskName(cacheName) +
                                        ", keyType=" + keyType + "]");

                                int keyCnt = em.keyCols.size();

                                Object[] upperBound = new Object[keyCnt];

                                for (int i = 0; i < keyCnt; i++)
                                    upperBound[i] = rs.getObject(i + 1);

                                futs.add(pool.submit(loadCacheRange(em, clo, null, upperBound, 0)));

                                while (rs.next()) {
                                    Object[] lowerBound = upperBound;

                                    upperBound = new Object[keyCnt];

                                    for (int i = 0; i < keyCnt; i++)
                                        upperBound[i] = rs.getObject(i + 1);

                                    futs.add(pool.submit(loadCacheRange(em, clo, lowerBound, upperBound, 0)));
                                }

                                futs.add(pool.submit(loadCacheRange(em, clo, upperBound, null, 0)));

                                continue;
                            }
                        }
                        catch (SQLException e) {
                            log.warning("Failed to load entries from db in multithreaded mode, will try in single thread " +
                                "[cache=" + U.maskName(cacheName) + ", keyType=" + keyType + " ]", e);
                        }
                        finally {
                            U.closeQuiet(conn);
                        }
                    }

                    if (log.isDebugEnabled())
                        log.debug("Single thread loading entries from db [cache=" + U.maskName(cacheName) +
                            ", keyType=" + keyType + "]");

                    futs.add(pool.submit(loadCacheFull(em, clo)));
                }
            }

            for (Future<?> fut : futs)
                U.get(fut);

            if (log.isInfoEnabled())
                log.info("Finished load cache: " + U.maskName(cacheName));
        }
        catch (IgniteCheckedException e) {
            throw new CacheLoaderException("Failed to load cache: " + U.maskName(cacheName), e.getCause());
        }
        finally {
            U.shutdownNow(getClass(), pool, log);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V load(K key) throws CacheLoaderException {
        assert key != null;

        EntryMapping em = entryMapping(session().cacheName(), typeIdForObject(key));

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
                return buildObject(em.cacheName, em.valueType(), em.valueKind(), em.valueColumns(), null, em.loadColIdxs, rs);
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

            Map<Object, LoadWorker<K, V>> workers = U.newHashMap(getOrCreateCacheMappings(cacheName).size());

            Map<K, V> res = new HashMap<>();

            for (K key : keys) {
                Object keyTypeId = typeIdForObject(key);

                EntryMapping em = entryMapping(cacheName, keyTypeId);

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

            for (int attempt = 0; attempt < maxWrtAttempts; attempt++) {
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

        EntryMapping em = entryMapping(session().cacheName(), typeIdForObject(key));

        if (log.isDebugEnabled())
            log.debug("Start write entry to database [table=" + em.fullTableName() + ", entry=" + entry + "]");

        Connection conn = null;

        try {
            conn = connection();

            if (dialect.hasMerge()) {
                PreparedStatement stmt = null;

                try {
                    stmt = conn.prepareStatement(em.mergeQry);

                    int idx = fillKeyParameters(stmt, em, key);

                    fillValueParameters(stmt, idx, em, entry.getValue());

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

                        Object keyTypeId = typeIdForObject(key);

                        em = entryMapping(cacheName, keyTypeId);

                        if (currKeyTypeId == null || !currKeyTypeId.equals(keyTypeId)) {
                            if (mergeStmt != null) {
                                if (log.isDebugEnabled())
                                    log.debug("Write entries to db [cache=" + U.maskName(cacheName) +
                                        ", keyType=" + em.keyType() + ", cnt=" + prepared + "]");

                                executeBatch(em, mergeStmt, "writeAll", fromIdx, prepared, lazyEntries);

                                U.closeQuiet(mergeStmt);
                            }

                            mergeStmt = conn.prepareStatement(em.mergeQry);

                            currKeyTypeId = keyTypeId;

                            fromIdx += prepared;

                            prepared = 0;
                        }

                        int idx = fillKeyParameters(mergeStmt, em, key);

                        fillValueParameters(mergeStmt, idx, em, entry.getValue());

                        mergeStmt.addBatch();

                        if (++prepared % batchSize == 0) {
                            if (log.isDebugEnabled())
                                log.debug("Write entries to db [cache=" + U.maskName(cacheName) +
                                    ", keyType=" + em.keyType() + ", cnt=" + prepared + "]");

                            executeBatch(em, mergeStmt, "writeAll", fromIdx, prepared, lazyEntries);

                            fromIdx += prepared;

                            prepared = 0;
                        }
                    }

                    if (mergeStmt != null && prepared % batchSize != 0) {
                        if (log.isDebugEnabled())
                            log.debug("Write entries to db [cache=" + U.maskName(cacheName) +
                                ", keyType=" + em.keyType() + ", cnt=" + prepared + "]");

                        executeBatch(em, mergeStmt, "writeAll", fromIdx, prepared, lazyEntries);

                    }
                }
                finally {
                    U.closeQuiet(mergeStmt);
                }
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Write entries to db one by one using update and insert statements " +
                        "[cache=" + U.maskName(cacheName) + ", cnt=" + entries.size() + "]");

                PreparedStatement insStmt = null;

                PreparedStatement updStmt = null;

                try {
                    for (Cache.Entry<? extends K, ? extends V> entry : entries) {
                        K key = entry.getKey();

                        Object keyTypeId = typeIdForObject(key);

                        EntryMapping em = entryMapping(cacheName, keyTypeId);

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

        EntryMapping em = entryMapping(session().cacheName(), typeIdForObject(key));

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
                Object keyTypeId = typeIdForObject(key);

                em = entryMapping(cacheName, keyTypeId);

                if (delStmt == null) {
                    delStmt = conn.prepareStatement(em.remQry);

                    currKeyTypeId = keyTypeId;
                }

                if (!currKeyTypeId.equals(keyTypeId)) {
                    if (log.isDebugEnabled())
                        log.debug("Delete entries from db [cache=" + U.maskName(cacheName) +
                            ", keyType=" + em.keyType() + ", cnt=" + prepared + "]");

                    executeBatch(em, delStmt, "deleteAll", fromIdx, prepared, lazyKeys);

                    fromIdx += prepared;

                    prepared = 0;

                    currKeyTypeId = keyTypeId;
                }

                fillKeyParameters(delStmt, em, key);

                delStmt.addBatch();

                if (++prepared % batchSize == 0) {
                    if (log.isDebugEnabled())
                        log.debug("Delete entries from db [cache=" + U.maskName(cacheName) +
                            ", keyType=" + em.keyType() + ", cnt=" + prepared + "]");

                    executeBatch(em, delStmt, "deleteAll", fromIdx, prepared, lazyKeys);

                    fromIdx += prepared;

                    prepared = 0;
                }
            }

            if (delStmt != null && prepared % batchSize != 0) {
                if (log.isDebugEnabled())
                    log.debug("Delete entries from db [cache=" + U.maskName(cacheName) +
                        ", keyType=" + em.keyType() + ", cnt=" + prepared + "]");

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
     * @param idx Index for parameters.
     * @param field Field descriptor.
     * @param fieldVal Field value.
     * @throws CacheException If failed to set statement parameter.
     */
    protected void fillParameter(PreparedStatement stmt, int idx, JdbcTypeField field, @Nullable Object fieldVal)
        throws CacheException {
        try {
            if (fieldVal != null) {
                if (field.getJavaFieldType() == UUID.class) {
                    switch (field.getDatabaseFieldType()) {
                        case Types.BINARY:
                            fieldVal = U.uuidToBytes((UUID)fieldVal);

                            break;
                        case Types.CHAR:
                        case Types.VARCHAR:
                            fieldVal = fieldVal.toString();

                            break;
                        default:
                            // No-op.
                    }
                }
                else if (field.getJavaFieldType().isEnum() && fieldVal instanceof Enum) {
                    Enum val = (Enum)fieldVal;

                    fieldVal = NUMERIC_TYPES.contains(field.getDatabaseFieldType()) ? val.ordinal() : val.name();
                }

                stmt.setObject(idx, fieldVal);
            }
            else
                stmt.setNull(idx, field.getDatabaseFieldType());
        }
        catch (SQLException e) {
            throw new CacheException("Failed to set statement parameter name: " + field.getDatabaseFieldName(), e);
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
    protected int fillKeyParameters(PreparedStatement stmt, int idx, EntryMapping em, Object key) throws CacheException {
        for (JdbcTypeField field : em.keyColumns()) {
            Object fieldVal = extractParameter(em.cacheName, em.keyType(), em.keyKind(), field.getJavaFieldName(), key);

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
        TypeKind valKind = em.valueKind();

        // Object could be passed by cache in binary format in case of cache configured with setStoreKeepBinary(true).
        if (valKind == TypeKind.POJO && val instanceof BinaryObject)
            valKind = TypeKind.BINARY;

        for (JdbcTypeField field : em.uniqValFlds) {
            Object fieldVal = extractParameter(em.cacheName, em.valueType(), valKind, field.getJavaFieldName(), val);

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
        return maxPoolSize;
    }

    /**
     * Set Max workers thread count. These threads are responsible for execute query.
     *
     * @param maxPoolSize Max workers thread count.
     */
    public void setMaximumPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    /**
     * Gets maximum number of write attempts in case of database error.
     *
     * @return Maximum number of write attempts.
     */
    public int getMaximumWriteAttempts() {
        return maxWrtAttempts;
    }

    /**
     * Sets maximum number of write attempts in case of database error.
     *
     * @param maxWrtAttempts Number of write attempts.
     */
    public void setMaximumWriteAttempts(int maxWrtAttempts) {
        this.maxWrtAttempts = maxWrtAttempts;
    }

    /**
     * Gets types known by store.
     *
     * @return Types known by store.
     */
    public JdbcType[] getTypes() {
        return types;
    }

    /**
     * Sets store configurations.
     *
     * @param types Store should process.
     */
    public void setTypes(JdbcType... types) {
        this.types = types;
    }

    /**
     * Gets hash code calculator.
     *
     * @return Hash code calculator.
     */
    public JdbcTypeHasher getHasher() {
        return hasher;
    }

    /**
     * Sets hash code calculator.
     *
     * @param hasher Hash code calculator.
     */
    public void setHasher(JdbcTypeHasher hasher) {
        this.hasher = hasher;
    }

    /**
     * Gets types transformer.
     *
     * @return Types transformer.
     */
    public JdbcTypesTransformer getTransformer() {
        return transformer;
    }

    /**
     * Sets types transformer.
     *
     * @param transformer Types transformer.
     */
    public void setTransformer(JdbcTypesTransformer transformer) {
        this.transformer = transformer;
    }

    /**
     * Get maximum batch size for delete and delete operations.
     *
     * @return Maximum batch size.
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Set maximum batch size for write and delete operations.
     *
     * @param batchSize Maximum batch size.
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
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
     * If {@code true} all the SQL table and field names will be escaped with double quotes like
     * ({@code "tableName"."fieldsName"}). This enforces case sensitivity for field names and
     * also allows having special characters in table and field names.
     *
     * @return Flag value.
     */
    public boolean isSqlEscapeAll() {
        return sqlEscapeAll;
    }

    /**
     * If {@code true} all the SQL table and field names will be escaped with double quotes like
     * ({@code "tableName"."fieldsName"}). This enforces case sensitivity for field names and
     * also allows having special characters in table and field names.
     *
     * @param sqlEscapeAll Flag value.
     */
    public void setSqlEscapeAll(boolean sqlEscapeAll) {
        this.sqlEscapeAll = sqlEscapeAll;
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
     * Type kind.
     */
    protected enum TypeKind {
        /** Type is known as Java built in type, like {@link String} */
        BUILT_IN,
        /** Class for this type is available. */
        POJO,
        /** Class for this type is not available. */
        BINARY
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

        /** Database key columns prepared for building SQL queries.. */
        private final Collection<String> sqlKeyCols;

        /** Database unique value columns. */
        private final Collection<String> cols;

        /** Database unique value columns prepared for building SQL queries. */
        private final Collection<String> sqlCols;

        /** Select query columns index. */
        private final Map<String, Integer> loadColIdxs;

        /** Unique value fields. */
        private final Collection<JdbcTypeField> uniqValFlds;

        /** Type metadata. */
        private final JdbcType typeMeta;

        /** Key type kind. */
        private final TypeKind keyKind;

        /** Value type kind. */
        private final TypeKind valKind;

        /** Full table name. */
        private final String fullTblName;

        /** Full table name prepared for building SQL queries. */
        private final String sqlFullTblName;

        /**
         * Escape collection of column names.
         * @param dialect Database dialect.
         * @param cols Columns.
         * @return Collection of escaped names.
         */
        private static Collection<String> escape(JdbcDialect dialect, Collection<String> cols) {
            Collection<String> res = new ArrayList<>(cols.size());

            for (String col : cols)
                res.add(dialect.escape(col));

            return res;
        }

        /**
         * @param cacheName Cache name.
         * @param dialect JDBC dialect.
         * @param typeMeta Type metadata.
         * @param keyKind Type kind.
         * @param valKind Value kind.
         * @param escape Escape SQL identifiers flag.
         */
        public EntryMapping(@Nullable String cacheName, JdbcDialect dialect, JdbcType typeMeta,
            TypeKind keyKind, TypeKind valKind, boolean escape) {
            this.cacheName = cacheName;

            this.dialect = dialect;

            this.typeMeta = typeMeta;

            this.keyKind = keyKind;

            this.valKind = valKind;

            JdbcTypeField[] keyFields = typeMeta.getKeyFields();

            JdbcTypeField[] valFields = typeMeta.getValueFields();

            keyCols = databaseColumns(F.asList(keyFields));

            uniqValFlds = F.view(F.asList(valFields), new IgnitePredicate<JdbcTypeField>() {
                @Override public boolean apply(JdbcTypeField col) {
                    return !keyCols.contains(col.getDatabaseFieldName());
                }
            });

            String schema = typeMeta.getDatabaseSchema();

            String tblName = typeMeta.getDatabaseTable();

            Collection<String> uniqueValCols = databaseColumns(uniqValFlds);

            cols = F.concat(false, keyCols, uniqueValCols);

            loadColIdxs = U.newHashMap(cols.size());

            int idx = 1;

            for (String col : cols)
                loadColIdxs.put(col.toUpperCase(), idx++);

            fullTblName = F.isEmpty(schema) ? tblName : schema + "." + tblName;

            Collection<String> sqlUniqueValCols;

            if (escape) {
                sqlFullTblName = F.isEmpty(schema) ? dialect.escape(tblName) : dialect.escape(schema) + "." + dialect.escape(tblName);

                sqlCols = escape(dialect, cols);
                sqlKeyCols = escape(dialect, keyCols);
                sqlUniqueValCols = escape(dialect, uniqueValCols);
            }
            else {
                sqlFullTblName = fullTblName;
                sqlCols = cols;
                sqlKeyCols = keyCols;
                sqlUniqueValCols = uniqueValCols;
            }

            loadCacheQry = dialect.loadCacheQuery(sqlFullTblName, sqlCols);

            loadCacheSelRangeQry = dialect.loadCacheSelectRangeQuery(sqlFullTblName, sqlKeyCols);

            loadQrySingle = dialect.loadQuery(sqlFullTblName, sqlKeyCols, sqlCols, 1);

            maxKeysPerStmt = dialect.getMaxParameterCount() / sqlKeyCols.size();

            loadQry = dialect.loadQuery(sqlFullTblName, sqlKeyCols, sqlCols, maxKeysPerStmt);

            insQry = dialect.insertQuery(sqlFullTblName, sqlKeyCols, sqlUniqueValCols);

            updQry = dialect.updateQuery(sqlFullTblName, sqlKeyCols, sqlUniqueValCols);

            mergeQry = dialect.mergeQuery(sqlFullTblName, sqlKeyCols, sqlUniqueValCols);

            remQry = dialect.removeQuery(sqlFullTblName, sqlKeyCols);
        }

        /**
         * Extract database column names from {@link JdbcTypeField}.
         *
         * @param dsc collection of {@link JdbcTypeField}.
         * @return Collection with database column names.
         */
        private static Collection<String> databaseColumns(Collection<JdbcTypeField> dsc) {
            return F.transform(dsc, new C1<JdbcTypeField, String>() {
                /** {@inheritDoc} */
                @Override public String apply(JdbcTypeField col) {
                    return col.getDatabaseFieldName();
                }
            });
        }

        /**
         * @return Key type.
         */
        protected String keyType() {
            return typeMeta.getKeyType();
        }

        /**
         * @return Key type kind.
         */
        protected TypeKind keyKind() {
            return keyKind;
        }

        /**
         * @return Value type.
         */
        protected String valueType() {
            return typeMeta.getValueType();
        }

        /**
         * @return Value type kind.
         */
        protected TypeKind valueKind() {
            return valKind;
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

            return dialect.loadQuery(sqlFullTblName, sqlKeyCols, sqlCols, keyCnt);
        }

        /**
         * Construct query for select values in range.
         *
         * @param appendLowerBound Need add lower bound for range.
         * @param appendUpperBound Need add upper bound for range.
         * @return Query with range.
         */
        protected String loadCacheRangeQuery(boolean appendLowerBound, boolean appendUpperBound) {
            return dialect.loadCacheRangeQuery(sqlFullTblName, sqlKeyCols, sqlCols, appendLowerBound, appendUpperBound);
        }

        /**
         * Gets key columns.
         *
         * @return Key columns.
         */
        protected JdbcTypeField[] keyColumns() {
            return typeMeta.getKeyFields();
        }

        /**
         * Gets value columns.
         *
         * @return Value columns.
         */
        protected JdbcTypeField[] valueColumns() {
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
            Connection conn = null;

            PreparedStatement stmt = null;

            try {
                conn = openConnection(true);

                stmt = conn.prepareStatement(qry);

                stmt.setFetchSize(dialect.getFetchSize());

                ResultSet rs = stmt.executeQuery();

                ResultSetMetaData meta = rs.getMetaData();

                Map<String, Integer> colIdxs = U.newHashMap(meta.getColumnCount());

                for (int i = 1; i <= meta.getColumnCount(); i++)
                    colIdxs.put(meta.getColumnLabel(i).toUpperCase(), i);

                while (rs.next()) {
                    K1 key = buildObject(em.cacheName, em.keyType(), em.keyKind(), em.keyColumns(), em.keyCols, colIdxs, rs);
                    V1 val = buildObject(em.cacheName, em.valueType(), em.valueKind(), em.valueColumns(), null, colIdxs, rs);

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
                log.debug("Load values from db [table= " + em.fullTableName() + ", keysCnt=" + keys.size() + "]");

            PreparedStatement stmt = null;

            try {
                stmt = conn.prepareStatement(em.loadQuery(keys.size()));

                int idx = 1;

                for (Object key : keys) {
                    for (JdbcTypeField field : em.keyColumns()) {
                        Object fieldVal = extractParameter(em.cacheName, em.keyType(), em.keyKind(), field.getJavaFieldName(), key);

                        fillParameter(stmt, idx++, field, fieldVal);
                    }
                }

                ResultSet rs = stmt.executeQuery();

                Map<K1, V1> entries = U.newHashMap(keys.size());

                while (rs.next()) {
                    K1 key = buildObject(em.cacheName, em.keyType(), em.keyKind(), em.keyColumns(), em.keyCols, em.loadColIdxs, rs);
                    V1 val = buildObject(em.cacheName, em.valueType(), em.valueKind(), em.valueColumns(), null, em.loadColIdxs, rs);

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
