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

package org.apache.ignite.internal.processors.query.schema.management;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.jdbc2.JdbcUtils;
import org.apache.ignite.internal.managers.systemview.walker.SqlIndexViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.SqlSchemaViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.SqlTableColumnViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.SqlTableViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.SqlViewColumnViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.SqlViewViewWalker;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.processors.query.ColumnInformation;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.QuerySysIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.QueryUtils.KeyOrValProperty;
import org.apache.ignite.internal.processors.query.TableInformation;
import org.apache.ignite.internal.processors.query.schema.AbstractSchemaChangeListener;
import org.apache.ignite.internal.processors.query.schema.SchemaChangeListener;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.sql.SqlIndexView;
import org.apache.ignite.spi.systemview.view.sql.SqlSchemaView;
import org.apache.ignite.spi.systemview.view.sql.SqlTableColumnView;
import org.apache.ignite.spi.systemview.view.sql.SqlTableView;
import org.apache.ignite.spi.systemview.view.sql.SqlViewColumnView;
import org.apache.ignite.spi.systemview.view.sql.SqlViewView;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.VAL_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.matches;

/**
 * Local schema manager. Responsible for all manipulations on schema objects.
 */
public class SchemaManager {
    /** */
    public static final String SQL_SCHEMA_VIEW = "schemas";

    /** */
    public static final String SQL_SCHEMA_VIEW_DESC = "SQL schemas";

    /** */
    public static final String SQL_TBLS_VIEW = "tables";

    /** */
    public static final String SQL_TBLS_VIEW_DESC = "SQL tables";

    /** */
    public static final String SQL_VIEWS_VIEW = "views";

    /** */
    public static final String SQL_VIEWS_VIEW_DESC = "SQL views";

    /** */
    public static final String SQL_IDXS_VIEW = "indexes";

    /** */
    public static final String SQL_IDXS_VIEW_DESC = "SQL indexes";

    /** */
    public static final String SQL_TBL_COLS_VIEW = metricName("table", "columns");

    /** */
    public static final String SQL_TBL_COLS_VIEW_DESC = "SQL table columns";

    /** */
    public static final String SQL_VIEW_COLS_VIEW = metricName("view", "columns");

    /** */
    public static final String SQL_VIEW_COLS_VIEW_DESC = "SQL view columns";

    /** */
    private static final Map<QueryIndexType, IndexDescriptorFactory>
        IDX_DESC_FACTORY = new EnumMap<>(QueryIndexType.class);

    /** Index descriptor factory for current instance. */
    private final Map<QueryIndexType, IndexDescriptorFactory> idxDescFactory = new EnumMap<>(QueryIndexType.class);

    /** */
    private volatile SchemaChangeListener lsnr;

    /** Collection of schemaNames and registered tables. */
    private final ConcurrentMap<String, SchemaDescriptor> schemas = new ConcurrentHashMap<>();

    /** Cache name -> schema name. */
    private final Map<String, String> cacheName2schema = new ConcurrentHashMap<>();

    /** Map from table identifier to table. */
    private final ConcurrentMap<T2<String, String>, TableDescriptor> id2tbl = new ConcurrentHashMap<>();

    /** System VIEW collection. */
    private final Set<SystemView<?>> sysViews = new GridConcurrentHashSet<>();

    /** Lock to synchronize schema operations. */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public SchemaManager(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(SchemaManager.class);
    }

    /** Register index descriptor factory for custom index type. */
    public static void registerIndexDescriptorFactory(QueryIndexType type, IndexDescriptorFactory factory) {
        IDX_DESC_FACTORY.put(type, factory);
    }

    /** Unregister index descriptor factory for custom index type. */
    public static void unregisterIndexDescriptorFactory(QueryIndexType type) {
        IDX_DESC_FACTORY.remove(type);
    }

    /**
     * Handle node start.
     *
     * @param schemaNames Schema names.
     */
    public void start(String[] schemaNames) throws IgniteCheckedException {
        lsnr = schemaChangeListener(ctx);

        idxDescFactory.putAll(IDX_DESC_FACTORY);

        // Register default index descriptor factory for tree indexes if it wasn't overrided.
        if (!idxDescFactory.containsKey(QueryIndexType.SORTED))
            idxDescFactory.put(QueryIndexType.SORTED, new SortedIndexDescriptorFactory(log));

        ctx.systemView().registerView(SQL_SCHEMA_VIEW, SQL_SCHEMA_VIEW_DESC,
            new SqlSchemaViewWalker(),
            schemas.values(),
            SqlSchemaView::new);

        ctx.systemView().registerView(SQL_TBLS_VIEW, SQL_TBLS_VIEW_DESC,
            new SqlTableViewWalker(),
            id2tbl.values(),
            SqlTableView::new);

        ctx.systemView().registerInnerCollectionView(SQL_VIEWS_VIEW, SQL_VIEWS_VIEW_DESC,
            new SqlViewViewWalker(),
            schemas.values(),
            SchemaDescriptor::views,
            SqlViewView::new);

        ctx.systemView().registerInnerCollectionView(SQL_IDXS_VIEW, SQL_IDXS_VIEW_DESC,
            new SqlIndexViewWalker(),
            id2tbl.values(),
            t -> t.indexes().values(),
            SqlIndexView::new);

        ctx.systemView().registerInnerCollectionView(SQL_TBL_COLS_VIEW, SQL_TBL_COLS_VIEW_DESC,
            new SqlTableColumnViewWalker(),
            id2tbl.values(),
            this::tableColumns,
            SqlTableColumnView::new);

        ctx.systemView().registerInnerCollectionView(SQL_VIEW_COLS_VIEW, SQL_VIEW_COLS_VIEW_DESC,
            new SqlViewColumnViewWalker(),
            sysViews,
            v -> MetricUtils.systemViewAttributes(v).entrySet(),
            SqlViewColumnView::new);

        lock.writeLock().lock();

        try {
            // Register PUBLIC schema which is always present.
            createSchema(QueryUtils.DFLT_SCHEMA, true);

            // Create schemas listed in node's configuration.
            createPredefinedSchemas(schemaNames);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** */
    private Collection<GridQueryProperty> tableColumns(TableDescriptor tblDesc) {
        GridQueryTypeDescriptor typeDesc = tblDesc.type();
        Collection<GridQueryProperty> props = typeDesc.properties().values();

        if (!tblDesc.type().properties().containsKey(KEY_FIELD_NAME))
            props = F.concat(false, new KeyOrValProperty(true, KEY_FIELD_NAME, typeDesc.keyClass()), props);

        if (!tblDesc.type().properties().containsKey(VAL_FIELD_NAME))
            props = F.concat(false, new KeyOrValProperty(false, VAL_FIELD_NAME, typeDesc.valueClass()), props);

        return props;
    }

    /**
     * Handle node stop.
     */
    public void stop() {
        schemas.clear();
        cacheName2schema.clear();
    }

    /**
     * Registers new system view.
     *
     * @param schema Schema to create view in.
     * @param view System view.
     */
    public void createSystemView(String schema, SystemView<?> view) {
        boolean disabled = IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_SQL_DISABLE_SYSTEM_VIEWS);

        if (disabled) {
            if (log.isInfoEnabled()) {
                log.info("SQL system views will not be created because they are disabled (see " +
                    IgniteSystemProperties.IGNITE_SQL_DISABLE_SYSTEM_VIEWS + " system property)");
            }

            return;
        }

        lock.writeLock().lock();

        try {
            createSchema(schema, true);

            schema(schema).add(new ViewDescriptor(MetricUtils.toSqlName(view.name()), null, view.description()));

            sysViews.add(view);

            lsnr.onSystemViewCreated(schema, view);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Create predefined schemas.
     *
     * @param schemaNames Schema names.
     */
    private void createPredefinedSchemas(String[] schemaNames) {
        assert lock.isWriteLockedByCurrentThread();

        if (F.isEmpty(schemaNames))
            return;

        Collection<String> schemaNames0 = new LinkedHashSet<>();

        for (String schemaName : schemaNames) {
            if (F.isEmpty(schemaName))
                continue;

            schemaName = QueryUtils.normalizeSchemaName(null, schemaName);

            schemaNames0.add(schemaName);
        }

        for (String schemaName : schemaNames0)
            createSchema(schemaName, true);
    }

    /**
     * Invoked when cache is created.
     *
     * @param cacheName Cache name.
     * @param schemaName Schema name.
     * @param sqlFuncs Custom SQL functions.
     * @throws IgniteCheckedException If failed.
     */
    public void onCacheCreated(String cacheName, String schemaName, Class<?>[] sqlFuncs) throws IgniteCheckedException {
        lock.writeLock().lock();

        try {
            createSchema(schemaName, false);

            cacheName2schema.put(cacheName, schemaName);

            createSqlFunctions(schemaName, sqlFuncs);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Registers new class description.
     *
     * @param cacheInfo Cache info.
     * @param type Type descriptor.
     * @param isSql Whether SQL enabled.
     * @throws IgniteCheckedException If failed.
     */
    public void onCacheTypeCreated(
        GridCacheContextInfo<?, ?> cacheInfo,
        GridQueryTypeDescriptor type,
        boolean isSql
    ) throws IgniteCheckedException {
        validateTypeDescriptor(type);

        lock.writeLock().lock();

        try {
            String schemaName = schemaName(cacheInfo.name());

            SchemaDescriptor schema = schema(schemaName);

            TableDescriptor tbl = new TableDescriptor(cacheInfo, type, isSql);

            schema.add(tbl);

            T2<String, String> tableId = new T2<>(schemaName, type.tableName());

            if (id2tbl.putIfAbsent(tableId, tbl) != null)
                throw new IllegalStateException("Table already exists: " + schemaName + '.' + type.tableName());

            lsnr.onSqlTypeCreated(schemaName, type, cacheInfo);

            // Create system indexes.
            createPkIndex(tbl);
            createAffinityIndex(tbl);

            // Create initial user indexes.
            for (GridQueryIndexDescriptor idxDesc : tbl.type().indexes().values())
                createIndex0(idxDesc, tbl, null);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Validates properties described by query types.
     *
     * @param type Type descriptor.
     * @throws IgniteCheckedException If validation failed.
     */
    private static void validateTypeDescriptor(GridQueryTypeDescriptor type) throws IgniteCheckedException {
        assert type != null;

        Collection<String> names = new HashSet<>(type.fields().keySet());

        if (names.size() < type.fields().size())
            throw new IgniteCheckedException("Found duplicated properties with the same name [keyType=" +
                type.keyClass().getName() + ", valueType=" + type.valueClass().getName() + "]");

        String ptrn = "Name ''{0}'' is reserved and cannot be used as a field name [type=" + type.name() + "]";

        for (String name : names) {
            if (name.equalsIgnoreCase(KEY_FIELD_NAME) || name.equalsIgnoreCase(VAL_FIELD_NAME))
                throw new IgniteCheckedException(MessageFormat.format(ptrn, name));
        }
    }

    /**
     * Handle cache stop.
     *
     * @param cacheName Cache name.
     * @param destroy Destroy cache.
     * @param clearIdx Whether to clear the index.
     */
    public void onCacheStopped(String cacheName, boolean destroy, boolean clearIdx) {
        lock.writeLock().lock();

        try {
            String schemaName = schemaName(cacheName);

            SchemaDescriptor schema = schemas.get(schemaName);

            // Remove this mapping only after callback to DML proc - it needs that mapping internally.
            cacheName2schema.remove(cacheName);

            for (TableDescriptor tbl : schema.tables()) {
                if (F.eq(tbl.cacheInfo().name(), cacheName)) {
                    Collection<IndexDescriptor> idxs = new ArrayList<>(tbl.indexes().values());

                    for (IndexDescriptor idx : idxs) {
                        if (!idx.isProxy()) // Proxies will be deleted implicitly after deleting target index.
                            dropIndex(tbl, idx.name(), !clearIdx);
                    }

                    lsnr.onSqlTypeDropped(schemaName, tbl.type(), destroy);

                    schema.drop(tbl);

                    T2<String, String> tableId = new T2<>(tbl.type().schemaName(), tbl.type().tableName());
                    id2tbl.remove(tableId, tbl);
                }
            }

            if (schema.decrementUsageCount()) {
                schemas.remove(schemaName);

                if (destroy)
                    ctx.query().sqlViewManager().clearSchemaViews(schemaName);

                lsnr.onSchemaDropped(schemaName);
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Create and register schema if needed.
     *
     * @param schemaName Schema name.
     * @param predefined Whether this is predefined schema.
     */
    private void createSchema(String schemaName, boolean predefined) {
        assert lock.isWriteLockedByCurrentThread();

        if (!predefined)
            predefined = F.eq(QueryUtils.DFLT_SCHEMA, schemaName);

        SchemaDescriptor schema = new SchemaDescriptor(schemaName, predefined);

        SchemaDescriptor oldSchema = schemas.putIfAbsent(schemaName, schema);

        if (oldSchema == null)
            lsnr.onSchemaCreated(schemaName);
        else
            schema = oldSchema;

        schema.incrementUsageCount();
    }

    /**
     * Registers SQL functions.
     *
     * @param schema Schema.
     * @param clss Classes.
     * @throws IgniteCheckedException If failed.
     */
    private void createSqlFunctions(String schema, Class<?>[] clss) throws IgniteCheckedException {
        assert lock.isWriteLockedByCurrentThread();

        if (F.isEmpty(clss))
            return;

        for (Class<?> cls : clss) {
            for (Method m : cls.getDeclaredMethods()) {
                QuerySqlFunction ann = m.getAnnotation(QuerySqlFunction.class);

                if (ann != null) {
                    int modifiers = m.getModifiers();

                    if (!Modifier.isPublic(modifiers))
                        throw new IgniteCheckedException("Method " + m.getName() + " must be public.");

                    String alias = ann.alias().isEmpty() ? m.getName() : ann.alias();

                    lsnr.onFunctionCreated(schema, alias, ann.deterministic(), m, ann.tableColumnTypes(), ann.tableColumnNames());
                }
            }
        }
    }

    /**
     * Get schema name for cache.
     *
     * @param cacheName Cache name.
     * @return Schema name.
     */
    public String schemaName(String cacheName) {
        String res = cacheName2schema.get(cacheName);

        if (res == null)
            res = "";

        return res;
    }

    /**
     * Get schemas names.
     *
     * @return Schemas names.
     */
    public Set<String> schemaNames() {
        return new HashSet<>(schemas.keySet());
    }

    /**
     * Get schema by name.
     *
     * @param schemaName Schema name.
     * @return Schema.
     */
    private SchemaDescriptor schema(String schemaName) {
        return schemas.get(schemaName);
    }

    /** */
    private void createPkIndex(TableDescriptor tbl) throws IgniteCheckedException {
        assert lock.isWriteLockedByCurrentThread();

        GridQueryIndexDescriptor idxDesc = new QuerySysIndexDescriptorImpl(QueryUtils.PRIMARY_KEY_INDEX,
            Collections.emptyList(), tbl.type().primaryKeyInlineSize()); // _KEY field will be added implicitly.

        // Add primary key index.
        createIndex0(
            idxDesc,
            tbl,
            null
        );
    }

    /** */
    private void createAffinityIndex(TableDescriptor tbl) throws IgniteCheckedException {
        assert lock.isWriteLockedByCurrentThread();

        // Locate index where affinity column is first (if any).
        if (tbl.affinityKey() != null && !tbl.affinityKey().equals(KEY_FIELD_NAME)) {
            boolean affIdxFound = false;

            for (GridQueryIndexDescriptor idxDesc : tbl.type().indexes().values()) {
                if (idxDesc.type() != QueryIndexType.SORTED)
                    continue;

                affIdxFound |= F.eq(tbl.affinityKey(), F.first(idxDesc.fields()));
            }

            // Add explicit affinity key index if nothing alike was found.
            if (!affIdxFound) {
                GridQueryIndexDescriptor idxDesc = new QuerySysIndexDescriptorImpl(QueryUtils.AFFINITY_KEY_INDEX,
                    Collections.singleton(tbl.affinityKey()), tbl.type().affinityFieldInlineSize());

                createIndex0(
                    idxDesc,
                    tbl,
                    null
                );
            }
        }
    }

    /** */
    private void createIndex0(
        GridQueryIndexDescriptor idxDesc,
        TableDescriptor tbl,
        @Nullable SchemaIndexCacheVisitor cacheVisitor
    ) throws IgniteCheckedException {
        // If cacheVisitor is not null, index creation can be durable, schema lock should not be acquired in this case
        // to avoid blocking of other schema operations.
        assert cacheVisitor == null || !lock.isWriteLockedByCurrentThread();

        IndexDescriptorFactory factory = idxDescFactory.get(idxDesc.type());

        if (factory == null)
            throw new IllegalStateException("Not found factory for index type: " + idxDesc.type());

        IndexDescriptor desc = factory.create(ctx, idxDesc, tbl, cacheVisitor);

        addIndex(tbl, desc);
    }

    /** Create proxy index for real index if needed. */
    private void createProxyIndex(
        IndexDescriptor idxDesc,
        TableDescriptor tbl
    ) {
        assert lock.isWriteLockedByCurrentThread();

        GridQueryTypeDescriptor typeDesc = tbl.type();

        if (F.isEmpty(typeDesc.keyFieldName()) && F.isEmpty(typeDesc.valueFieldName()))
            return;

        String keyAlias = typeDesc.keyFieldAlias();
        String valAlias = typeDesc.valueFieldAlias();

        LinkedHashMap<String, IndexKeyDefinition> proxyKeyDefs = new LinkedHashMap<>(idxDesc.keyDefinitions().size());

        boolean modified = false;

        for (Map.Entry<String, IndexKeyDefinition> keyDef : idxDesc.keyDefinitions().entrySet()) {
            String oldFieldName = keyDef.getKey();
            String newFieldName = oldFieldName;

            // Replace _KEY/_VAL field with aliases and vice versa.
            if (F.eq(oldFieldName, QueryUtils.KEY_FIELD_NAME) && !F.isEmpty(keyAlias))
                newFieldName = keyAlias;
            else if (F.eq(oldFieldName, QueryUtils.VAL_FIELD_NAME) && !F.isEmpty(valAlias))
                newFieldName = valAlias;
            else if (F.eq(oldFieldName, keyAlias))
                newFieldName = QueryUtils.KEY_FIELD_NAME;
            else if (F.eq(oldFieldName, valAlias))
                newFieldName = QueryUtils.VAL_FIELD_NAME;

            modified |= !F.eq(oldFieldName, newFieldName);

            proxyKeyDefs.put(newFieldName, keyDef.getValue());
        }

        if (!modified)
            return;

        String proxyName = generateProxyIdxName(idxDesc.name());

        IndexDescriptor proxyDesc = new IndexDescriptor(proxyName, proxyKeyDefs, idxDesc);

        tbl.addIndex(proxyName, proxyDesc);

        lsnr.onIndexCreated(tbl.type().schemaName(), tbl.type().tableName(), proxyName, proxyDesc);
    }

    /** */
    public static String generateProxyIdxName(String idxName) {
        return idxName + "_proxy";
    }

    /**
     * Create index dynamically.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param idxDesc Index descriptor.
     * @param ifNotExists If-not-exists.
     * @param cacheVisitor Cache visitor.
     * @throws IgniteCheckedException If failed.
     */
    public void createIndex(
        String schemaName,
        String tblName,
        QueryIndexDescriptorImpl idxDesc,
        boolean ifNotExists,
        SchemaIndexCacheVisitor cacheVisitor
    ) throws IgniteCheckedException {
        TableDescriptor tbl;

        lock.readLock().lock();

        try {
            // Locate table.
            tbl = table(schemaName, tblName);

            if (tbl == null)
                throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND, tblName);

            if (tbl.indexes().containsKey(idxDesc.name())) {
                if (ifNotExists)
                    return;
                else
                    throw new SchemaOperationException(SchemaOperationException.CODE_INDEX_EXISTS, idxDesc.name());
            }
        }
        finally {
            lock.readLock().unlock();
        }

        createIndex0(idxDesc, tbl, cacheVisitor);
    }

    /**
     * Add index to the schema.
     *
     * @param tbl Table descriptor.
     * @param idxDesc Index descriptor.
     */
    public void addIndex(TableDescriptor tbl, IndexDescriptor idxDesc) throws IgniteCheckedException {
        lock.writeLock().lock();

        try {
            // Check under the lock if table is still exists.
            if (table(tbl.type().schemaName(), tbl.type().tableName()) == null) {
                ctx.indexProcessor().removeIndex(new IndexName(tbl.cacheInfo().name(), tbl.type().schemaName(),
                        tbl.type().tableName(), idxDesc.name()), false);

                throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND, tbl.type().tableName());
            }

            tbl.addIndex(idxDesc.name(), idxDesc);

            lsnr.onIndexCreated(tbl.type().schemaName(), tbl.type().tableName(), idxDesc.name(), idxDesc);

            createProxyIndex(idxDesc, tbl);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Drop index.
     *
     * @param schemaName Schema name.
     * @param idxName Index name.
     * @param ifExists If exists.
     */
    public void dropIndex(String schemaName, String idxName, boolean ifExists) throws IgniteCheckedException {
        lock.writeLock().lock();

        try {
            IndexDescriptor idxDesc = index(schemaName, idxName);

            if (idxDesc == null) {
                if (ifExists)
                    return;
                else
                    throw new SchemaOperationException(SchemaOperationException.CODE_INDEX_NOT_FOUND, idxName);
            }

            dropIndex(idxDesc.table(), idxName, false);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Drop index.
     *
     * @param tbl Table descriptor.
     * @param idxName Index name.
     */
    private void dropIndex(TableDescriptor tbl, String idxName, boolean softDelete) {
        assert lock.isWriteLockedByCurrentThread();

        String schemaName = tbl.type().schemaName();
        String cacheName = tbl.type().cacheName();
        String tableName = tbl.type().tableName();

        IndexDescriptor idxDesc = tbl.dropIndex(idxName);

        assert idxDesc != null;

        if (!idxDesc.isProxy()) {
            ctx.indexProcessor().removeIndex(
                new IndexName(cacheName, schemaName, tableName, idxName),
                softDelete
            );
        }

        lsnr.onIndexDropped(schemaName, tableName, idxName);

        // Drop proxy for target index.
        for (IndexDescriptor proxyDesc : tbl.indexes().values()) {
            if (proxyDesc.targetIdx() == idxDesc) {
                tbl.dropIndex(proxyDesc.name());

                lsnr.onIndexDropped(schemaName, tableName, proxyDesc.name());
            }
        }
    }

    /**
     * Add column.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param cols Columns.
     * @param ifTblExists If table exists.
     * @param ifColNotExists If column not exists.
     * @throws IgniteCheckedException If failed.
     */
    public void addColumn(
        String schemaName,
        String tblName,
        List<QueryField> cols,
        boolean ifTblExists,
        boolean ifColNotExists
    ) throws IgniteCheckedException {
        assert !ifColNotExists || cols.size() == 1;

        lock.writeLock().lock();

        try {
            // Locate table.
            TableDescriptor tbl = table(schemaName, tblName);

            if (tbl == null) {
                if (!ifTblExists) {
                    throw new IgniteCheckedException("Table not found in schema manager [schemaName=" + schemaName +
                        ", tblName=" + tblName + ']');
                }
                else
                    return;
            }

            lsnr.onColumnsAdded(schemaName, tbl.type(), tbl.cacheInfo(), cols);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Drop column.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param cols Columns.
     * @param ifTblExists If table exists.
     * @param ifColExists If column exists.
     * @throws IgniteCheckedException If failed.
     */
    public void dropColumn(
        String schemaName,
        String tblName,
        List<String> cols,
        boolean ifTblExists,
        boolean ifColExists
    ) throws IgniteCheckedException {
        assert !ifColExists || cols.size() == 1;

        lock.writeLock().lock();

        try {
            // Locate table.
            TableDescriptor tbl = table(schemaName, tblName);

            if (tbl == null) {
                if (!ifTblExists) {
                    throw new IgniteCheckedException("Table not found in schema manager [schemaName=" + schemaName +
                        ",tblName=" + tblName + ']');
                }
                else
                    return;
            }

            lsnr.onColumnsDropped(schemaName, tbl.type(), tbl.cacheInfo(), cols);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Create view.
     *
     * @param schemaName Schema name.
     * @param viewName View name.
     * @param viewSql View SQL.
     */
    public void createView(String schemaName, String viewName, String viewSql) {
        lock.writeLock().lock();

        try {
            SchemaDescriptor schema = schema(schemaName);

            if (schema == null) {
                log.warning("Schema for view not found in schema manager [schemaName=" + schemaName +
                    ", viewName=" + viewName + ']');

                return;
            }

            schema.add(new ViewDescriptor(viewName, viewSql, null));

            lsnr.onViewCreated(schemaName, viewName, viewSql);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Drop view.
     *
     * @param schemaName Schema name.
     * @param viewName View name.
     */
    public void dropView(String schemaName, String viewName) {
        lock.writeLock().lock();

        try {
            SchemaDescriptor schema = schema(schemaName);

            if (schema == null)
                return;

            ViewDescriptor viewDesc = schema.viewByName(viewName);

            if (viewDesc == null)
                return;

            schema(schemaName).drop(viewDesc);

            lsnr.onViewDropped(schemaName, viewName);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Initialize table's cache context created for not started cache.
     *
     * @param cctx Cache context.
     * @return {@code true} If context has been initialized.
     */
    public boolean initCacheContext(GridCacheContext<?, ?> cctx) {
        lock.writeLock().lock();

        try {
            GridCacheContextInfo<?, ?> cacheInfo = cacheInfo(cctx.name());

            if (cacheInfo != null) {
                assert !cacheInfo.isCacheContextInited() : cacheInfo.name();
                assert cacheInfo.name().equals(cctx.name()) : cacheInfo.name() + " != " + cctx.name();

                cacheInfo.initCacheContext((GridCacheContext)cctx);

                return true;
            }

            return false;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Clear cache context on call cache.close() on non-affinity node.
     *
     * @param cctx Cache context.
     * @return {@code true} If context has been cleared.
     */
    public boolean clearCacheContext(GridCacheContext<?, ?> cctx) {
        lock.writeLock().lock();

        try {
            GridCacheContextInfo<?, ?> cacheInfo = cacheInfo(cctx.name());

            if (cacheInfo != null && cacheInfo.isCacheContextInited()) {
                cacheInfo.clearCacheContext();

                return true;
            }

            return false;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Mark tables for index rebuild, so that their indexes are not used.
     *
     * @param cacheName Cache name.
     * @param mark Mark/unmark flag, {@code true} if index rebuild started, {@code false} if finished.
     */
    public void markIndexRebuild(String cacheName, boolean mark) {
        lock.writeLock().lock();

        try {
            for (TableDescriptor tbl : tablesForCache(cacheName)) {
                tbl.markIndexRebuildInProgress(mark);

                if (mark)
                    lsnr.onIndexRebuildStarted(tbl.type().schemaName(), tbl.type().tableName());
                else
                    lsnr.onIndexRebuildFinished(tbl.type().schemaName(), tbl.type().tableName());
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get table descriptor.
     *
     * @param schemaName Schema name.
     * @param cacheName Cache name.
     * @param type Type name.
     * @return Descriptor.
     */
    @Nullable public GridQueryTypeDescriptor typeDescriptorForType(String schemaName, String cacheName, String type) {
        lock.readLock().lock();

        try {
            SchemaDescriptor schema = schema(schemaName);

            if (schema == null)
                return null;

            TableDescriptor tbl = schema.tableByTypeName(cacheName, type);

            return tbl == null ? null : tbl.type();
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets collection of table for given schema name.
     *
     * @param cacheName Cache name.
     * @return Collection of table descriptors.
     */
    public Collection<TableDescriptor> tablesForCache(String cacheName) {
        lock.readLock().lock();

        try {
            SchemaDescriptor schema = schema(schemaName(cacheName));

            if (schema == null)
                return Collections.emptySet();

            List<TableDescriptor> tbls = new ArrayList<>();

            for (TableDescriptor tbl : schema.tables()) {
                if (F.eq(tbl.cacheInfo().name(), cacheName))
                    tbls.add(tbl);
            }

            return tbls;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Find registered cache info by it's name.
     */
    @Nullable public GridCacheContextInfo<?, ?> cacheInfo(String cacheName) {
        lock.readLock().lock();

        try {
            SchemaDescriptor schema = schema(schemaName(cacheName));

            if (schema == null)
                return null;

            for (TableDescriptor tbl : schema.tables()) {
                if (F.eq(tbl.cacheInfo().name(), cacheName))
                    return tbl.cacheInfo();
            }

            return null;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Find table by it's identifier.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @return Table descriptor or {@code null} if none found.
     */
    @Nullable public TableDescriptor table(String schemaName, String tblName) {
        lock.readLock().lock();

        try {
            return id2tbl.get(new T2<>(schemaName, tblName));
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Find index by it's identifier.
     *
     * @param schemaName Schema name.
     * @param idxName Index name.
     * @return Index or {@code null} if none found.
     */
    @Nullable public IndexDescriptor index(String schemaName, String idxName) {
        lock.readLock().lock();

        try {
            SchemaDescriptor schema = schema(schemaName);

            if (schema == null)
                return null;

            for (TableDescriptor tbl : schema.tables()) {
                IndexDescriptor idx = tbl.indexes().get(idxName);

                if (idx != null)
                    return idx;
            }

            return null;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @return Collection of all present index descriptors.
     */
    public Collection<IndexDescriptor> allIndexes() {
        return id2tbl.values().stream()
            .flatMap(t -> t.indexes().values().stream())
            .collect(Collectors.toList());
    }

    /**
     * Return table information filtered by given patterns.
     *
     * @param schemaNamePtrn Filter by schema name. Can be {@code null} to don't use the filter.
     * @param tblNamePtrn Filter by table name. Can be {@code null} to don't use the filter.
     * @param tblTypes Filter by table type. As Of now supported only 'TABLES' and 'VIEWS'.
     * Can be {@code null} or empty to don't use the filter.
     *
     * @return Table information filtered by given patterns.
     */
    public Collection<TableInformation> tablesInformation(
        String schemaNamePtrn,
        String tblNamePtrn,
        String... tblTypes
    ) {
        Set<String> types = F.isEmpty(tblTypes) ? Collections.emptySet() : new HashSet<>(Arrays.asList(tblTypes));

        Collection<TableInformation> infos = new ArrayList<>();

        boolean allTypes = F.isEmpty(tblTypes);

        if (allTypes || types.contains(JdbcUtils.TYPE_TABLE)) {
            id2tbl.values().stream()
                .filter(t -> matches(t.type().schemaName(), schemaNamePtrn))
                .filter(t -> matches(t.type().tableName(), tblNamePtrn))
                .map(t -> {
                    int cacheGrpId = t.cacheInfo().groupId();

                    CacheGroupDescriptor cacheGrpDesc = ctx.cache().cacheGroupDescriptors().get(cacheGrpId);

                    // We should skip table in case regarding cache group has been removed.
                    if (cacheGrpDesc == null)
                        return null;

                    GridQueryTypeDescriptor type = t.type();

                    return new TableInformation(t.type().schemaName(), t.type().tableName(),
                        JdbcUtils.TYPE_TABLE, cacheGrpId, cacheGrpDesc.cacheOrGroupName(), t.cacheInfo().cacheId(),
                        t.cacheInfo().name(), type.affinityKey(), type.keyFieldAlias(), type.valueFieldAlias(),
                        type.keyTypeName(), type.valueTypeName());
                })
                .filter(Objects::nonNull)
                .forEach(infos::add);
        }

        if ((allTypes || types.contains(JdbcUtils.TYPE_VIEW))) {
            schemas.values().stream()
                .filter(s -> matches(s.schemaName(), schemaNamePtrn))
                .forEach(s -> s.views().stream()
                    .filter(v -> matches(v.name(), tblNamePtrn))
                    .map(v -> new TableInformation(s.schemaName(), v.name(), JdbcUtils.TYPE_VIEW))
                    .forEach(infos::add));
        }

        return infos;
    }

    /**
     * Return column information filtered by given patterns.
     *
     * @param schemaNamePtrn Filter by schema name. Can be {@code null} to don't use the filter.
     * @param tblNamePtrn Filter by table name. Can be {@code null} to don't use the filter.
     * @param colNamePtrn Filter by column name. Can be {@code null} to don't use the filter.
     *
     * @return Column information filtered by given patterns.
     */
    public Collection<ColumnInformation> columnsInformation(
        String schemaNamePtrn,
        String tblNamePtrn,
        String colNamePtrn
    ) {
        Collection<ColumnInformation> infos = new ArrayList<>();

        // Gather information about tables.
        id2tbl.values().stream()
            .filter(t -> matches(t.type().schemaName(), schemaNamePtrn))
            .filter(t -> matches(t.type().tableName(), tblNamePtrn))
            .forEach(tbl -> {
                AtomicInteger cnt = new AtomicInteger(1); // Column ordinal position, start from 1.

                GridQueryTypeDescriptor d = tbl.type();

                // Add default columns if fields not specified explicitely.
                if (F.isEmpty(d.fields())) {
                    if (matches(KEY_FIELD_NAME, colNamePtrn)) {
                        infos.add(new ColumnInformation(cnt.getAndIncrement(), d.schemaName(), d.tableName(),
                            KEY_FIELD_NAME, d.keyClass(), false, null, -1, -1, false));
                    }

                    if (matches(VAL_FIELD_NAME, colNamePtrn)) {
                        infos.add(new ColumnInformation(cnt.getAndIncrement(), d.schemaName(), d.tableName(),
                            VAL_FIELD_NAME, d.valueClass(), false, null, -1, -1, false));
                    }
                }
                else {
                    d.fields().keySet().stream()
                        .filter(field -> matches(field, colNamePtrn))
                        .forEach(field -> {
                            GridQueryProperty prop = d.property(field);

                            infos.add(new ColumnInformation(
                                cnt.getAndIncrement(),
                                d.schemaName(),
                                d.tableName(),
                                field,
                                prop.type(),
                                !prop.notNull(),
                                prop.defaultValue(),
                                prop.precision(),
                                prop.scale(),
                                field.equals(d.affinityKey()))
                            );
                        });
                }
            });

        // Gather information about system views.
        if (matches(QueryUtils.SCHEMA_SYS, schemaNamePtrn)) {
            sysViews.stream()
                .filter(v -> matches(MetricUtils.toSqlName(v.name()), tblNamePtrn))
                .flatMap(
                    view -> {
                        AtomicInteger cnt = new AtomicInteger(1); // Column ordinal position, start from 1.

                        return MetricUtils.systemViewAttributes(view).entrySet().stream()
                            .filter(c -> matches(MetricUtils.toSqlName(c.getKey()), colNamePtrn))
                            .map(c -> new ColumnInformation(
                                cnt.getAndIncrement(),
                                QueryUtils.SCHEMA_SYS,
                                MetricUtils.toSqlName(view.name()),
                                MetricUtils.toSqlName(c.getKey()),
                                c.getValue(),
                                true,
                                null,
                                -1,
                                -1,
                                false)
                            );
                    }
                ).forEach(infos::add);
        }

        return infos;
    }

    /** */
    private SchemaChangeListener schemaChangeListener(GridKernalContext ctx) {
        List<SchemaChangeListener> subscribers = new ArrayList<>(ctx.internalSubscriptionProcessor().getSchemaChangeSubscribers());

        if (F.isEmpty(subscribers))
            return new NoOpSchemaChangeListener();

        return new CompoundSchemaChangeListener(ctx, subscribers);
    }

    /** */
    private static final class NoOpSchemaChangeListener extends AbstractSchemaChangeListener {
        // No-op.
    }

    /** */
    private static final class CompoundSchemaChangeListener implements SchemaChangeListener {
        /** */
        private final List<SchemaChangeListener> lsnrs;

        /** */
        private final IgniteLogger log;

        /**
         * @param ctx Kernal context.
         * @param lsnrs Lsnrs.
         */
        private CompoundSchemaChangeListener(GridKernalContext ctx, List<SchemaChangeListener> lsnrs) {
            this.lsnrs = lsnrs;
            log = ctx.log(CompoundSchemaChangeListener.class);
        }

        /** {@inheritDoc} */
        @Override public void onSchemaCreated(String schemaName) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onSchemaCreated(schemaName)));
        }

        /** {@inheritDoc} */
        @Override public void onSchemaDropped(String schemaName) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onSchemaDropped(schemaName)));
        }

        /** {@inheritDoc} */
        @Override public void onSqlTypeCreated(
            String schemaName,
            GridQueryTypeDescriptor typeDesc,
            GridCacheContextInfo<?, ?> cacheInfo
        ) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onSqlTypeCreated(schemaName, typeDesc, cacheInfo)));
        }

        /** {@inheritDoc} */
        @Override public void onColumnsAdded(
            String schemaName,
            GridQueryTypeDescriptor typeDesc,
            GridCacheContextInfo<?, ?> cacheInfo,
            List<QueryField> cols
        ) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onColumnsAdded(schemaName, typeDesc, cacheInfo, cols)));
        }

        /** {@inheritDoc} */
        @Override public void onColumnsDropped(
            String schemaName,
            GridQueryTypeDescriptor typeDesc,
            GridCacheContextInfo<?, ?> cacheInfo,
            List<String> cols
        ) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onColumnsDropped(schemaName, typeDesc, cacheInfo, cols)));
        }

        /** {@inheritDoc} */
        @Override public void onSqlTypeDropped(
            String schemaName,
            GridQueryTypeDescriptor typeDescriptor,
            boolean destroy
        ) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onSqlTypeDropped(schemaName, typeDescriptor, destroy)));
        }

        /** {@inheritDoc} */
        @Override public void onIndexCreated(
            String schemaName,
            String tblName,
            String idxName,
            IndexDescriptor idxDesc
        ) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onIndexCreated(schemaName, tblName, idxName, idxDesc)));
        }

        /** {@inheritDoc} */
        @Override public void onIndexDropped(String schemaName, String tblName, String idxName) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onIndexDropped(schemaName, tblName, idxName)));
        }

        /** {@inheritDoc} */
        @Override public void onIndexRebuildStarted(String schemaName, String tblName) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onIndexRebuildStarted(schemaName, tblName)));
        }

        /** {@inheritDoc} */
        @Override public void onIndexRebuildFinished(String schemaName, String tblName) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onIndexRebuildFinished(schemaName, tblName)));
        }

        /** {@inheritDoc} */
        @Override public void onFunctionCreated(String schemaName, String name, boolean deterministic, Method mtd,
            @Nullable Class<?>[] tblColTypes, @Nullable String[] tblColNames) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onFunctionCreated(schemaName, name, deterministic, mtd,
                tblColTypes, tblColNames)));
        }

        /** {@inheritDoc} */
        @Override public void onSystemViewCreated(String schemaName, SystemView<?> sysView) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onSystemViewCreated(schemaName, sysView)));
        }

        /** {@inheritDoc} */
        @Override public void onViewCreated(String schemaName, String viewName, String viewSql) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onViewCreated(schemaName, viewName, viewSql)));
        }

        /** {@inheritDoc} */
        @Override public void onViewDropped(String schemaName, String viewName) {
            lsnrs.forEach(lsnr -> executeSafe(() -> lsnr.onViewDropped(schemaName, viewName)));
        }

        /** */
        private void executeSafe(Runnable r) {
            try {
                r.run();
            }
            catch (Exception e) {
                log.warning("Failed to notify listener (will ignore): " + e.getMessage(), e);
            }
        }
    }
}
