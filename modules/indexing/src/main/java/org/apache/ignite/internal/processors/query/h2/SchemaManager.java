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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.systemview.walker.SqlIndexViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.SqlSchemaViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.SqlTableColumnViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.SqlTableViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.SqlViewColumnViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.SqlViewViewWalker;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sys.SqlSystemTableEngine;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemView;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemViewBaselineNodes;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemViewCacheGroupsIOStatistics;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemViewNodeAttributes;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemViewNodeMetrics;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.systemview.view.SqlIndexView;
import org.apache.ignite.spi.systemview.view.SqlSchemaView;
import org.apache.ignite.spi.systemview.view.SqlTableColumnView;
import org.apache.ignite.spi.systemview.view.SqlTableView;
import org.apache.ignite.spi.systemview.view.SqlViewColumnView;
import org.apache.ignite.spi.systemview.view.SqlViewView;
import org.h2.index.Index;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Schema manager. Responsible for all manipulations on schema objects.
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

    /** Connection manager. */
    private final ConnectionManager connMgr;

    /** Collection of schemaNames and registered tables. */
    private final ConcurrentMap<String, H2Schema> schemas = new ConcurrentHashMap<>();

    /** Cache name -> schema name */
    private final Map<String, String> cacheName2schema = new ConcurrentHashMap<>();

    /** Data tables. */
    private final ConcurrentMap<QueryTable, GridH2Table> dataTables = new ConcurrentHashMap<>();

    /** System VIEW collection. */
    private final Set<SqlSystemView> systemViews = new GridConcurrentHashSet<>();

    /** Mutex to synchronize schema operations. */
    private final Object schemaMux = new Object();

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param connMgr Connection manager.
     */
    public SchemaManager(GridKernalContext ctx, ConnectionManager connMgr) {
        this.ctx = ctx;
        this.connMgr = connMgr;

        log = ctx.log(SchemaManager.class);

        ctx.systemView().registerView(SQL_SCHEMA_VIEW, SQL_SCHEMA_VIEW_DESC,
            new SqlSchemaViewWalker(),
            schemas.values(),
            SqlSchemaView::new);

        ctx.systemView().registerView(SQL_TBLS_VIEW, SQL_TBLS_VIEW_DESC,
            new SqlTableViewWalker(),
            dataTables.values(),
            SqlTableView::new);

        ctx.systemView().registerView(SQL_VIEWS_VIEW, SQL_VIEWS_VIEW_DESC,
            new SqlViewViewWalker(),
            systemViews,
            SqlViewView::new);

        ctx.systemView().registerInnerCollectionView(SQL_IDXS_VIEW, SQL_IDXS_VIEW_DESC,
            new SqlIndexViewWalker(),
            dataTables.values(),
            GridH2Table::getIndexes,
            SqlIndexView::new);

        ctx.systemView().registerInnerArrayView(SQL_TBL_COLS_VIEW, SQL_TBL_COLS_VIEW_DESC,
            new SqlTableColumnViewWalker(),
            dataTables.values(),
            GridH2Table::getColumns,
            SqlTableColumnView::new);

        ctx.systemView().registerInnerArrayView(SQL_VIEW_COLS_VIEW, SQL_VIEW_COLS_VIEW_DESC,
            new SqlViewColumnViewWalker(),
            systemViews,
            SqlSystemView::getColumns,
            SqlViewColumnView::new);
    }

    /**
     * Handle node start.
     *
     * @param schemaNames Schema names.
     */
    public void start(String[] schemaNames) throws IgniteCheckedException {
        // Register PUBLIC schema which is always present.
        schemas.put(QueryUtils.DFLT_SCHEMA, new H2Schema(QueryUtils.DFLT_SCHEMA, true));

        // Create system views.
        createSystemViews();

        // Create schemas listed in node's configuration.
        createPredefinedSchemas(schemaNames);
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
    public void createSystemView(String schema, SqlSystemView view) {

        boolean disabled = IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_SQL_DISABLE_SYSTEM_VIEWS);

        if (disabled) {
            if (log.isInfoEnabled()) {
                log.info("SQL system views will not be created because they are disabled (see " +
                    IgniteSystemProperties.IGNITE_SQL_DISABLE_SYSTEM_VIEWS + " system property)");
            }

            return;
        }

        try {
            synchronized (schemaMux) {
                createSchema(schema, true);
            }

            try (H2PooledConnection c = connMgr.connection(schema)) {
                SqlSystemTableEngine.registerView(c.connection(), view);

                systemViews.add(view);
            }
        }
        catch (IgniteCheckedException | SQLException e) {
            throw new IgniteException("Failed to register system view.", e);
        }
    }

    /**
     * Create system views.
     */
    private void createSystemViews() throws IgniteCheckedException {
        for (SqlSystemView view : systemViews(ctx))
            createSystemView(QueryUtils.SCHEMA_SYS, view);
    }

    /**
     * @param ctx Context.
     * @return Predefined system views.
     */
    private Collection<SqlSystemView> systemViews(GridKernalContext ctx) {
        Collection<SqlSystemView> views = new ArrayList<>();

        views.add(new SqlSystemViewNodeAttributes(ctx));
        views.add(new SqlSystemViewBaselineNodes(ctx));
        views.add(new SqlSystemViewNodeMetrics(ctx));
        views.add(new SqlSystemViewCacheGroupsIOStatistics(ctx));

        return views;
    }

    /**
     * Create predefined schemas.
     *
     * @param schemaNames Schema names.
     */
    private void createPredefinedSchemas(String[] schemaNames) throws IgniteCheckedException {
        if (F.isEmpty(schemaNames))
            return;

        Collection<String> schemaNames0 = new LinkedHashSet<>();

        for (String schemaName : schemaNames) {
            if (F.isEmpty(schemaName))
                continue;

            schemaName = QueryUtils.normalizeSchemaName(null, schemaName);

            schemaNames0.add(schemaName);
        }

        synchronized (schemaMux) {
            for (String schemaName : schemaNames0)
                createSchema(schemaName, true);
        }
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
        synchronized (schemaMux) {
            createSchema(schemaName, false);
        }

        cacheName2schema.put(cacheName, schemaName);

        createSqlFunctions(schemaName, sqlFuncs);
    }

    /**
     * Registers new class description.
     *
     * @param cacheInfo Cache info.
     * @param idx Indexing.
     * @param type Type descriptor.
     * @param isSql Whether SQL enabled.
     * @throws IgniteCheckedException If failed.
     */
    public void onCacheTypeCreated(GridCacheContextInfo cacheInfo, IgniteH2Indexing idx,
        GridQueryTypeDescriptor type, boolean isSql) throws IgniteCheckedException {
        String schemaName = schemaName(cacheInfo.name());

        H2TableDescriptor tblDesc = new H2TableDescriptor(idx, schemaName, type, cacheInfo, isSql);

        H2Schema schema = schema(schemaName);

        try (H2PooledConnection conn = connMgr.connection(schema.schemaName())) {
            GridH2Table h2tbl = createTable(schema.schemaName(), schema, tblDesc, conn);

            schema.add(tblDesc);

            if (dataTables.putIfAbsent(h2tbl.identifier(), h2tbl) != null)
                throw new IllegalStateException("Table already exists: " + h2tbl.identifierString());
        }
        catch (SQLException e) {
            throw new IgniteCheckedException("Failed to register query type: " + tblDesc, e);
        }
    }

    /**
     * Handle cache destroy.
     *
     * @param cacheName Cache name.
     * @param rmvIdx Whether to remove indexes.
     */
    public void onCacheDestroyed(String cacheName, boolean rmvIdx) {
        String schemaName = schemaName(cacheName);

        H2Schema schema = schemas.get(schemaName);

        // Remove this mapping only after callback to DML proc - it needs that mapping internally
        cacheName2schema.remove(cacheName);

        // Drop tables.
        Collection<H2TableDescriptor> rmvTbls = new HashSet<>();

        for (H2TableDescriptor tbl : schema.tables()) {
            if (F.eq(tbl.cacheName(), cacheName)) {
                try {
                    tbl.table().setRemoveIndexOnDestroy(rmvIdx);

                    dropTable(tbl);
                }
                catch (Exception e) {
                    U.error(log, "Failed to drop table on cache stop (will ignore): " + tbl.fullTableName(), e);
                }

                schema.drop(tbl);

                rmvTbls.add(tbl);

                GridH2Table h2Tbl = tbl.table();

                dataTables.remove(h2Tbl.identifier(), h2Tbl);
            }
        }

        synchronized (schemaMux) {
            if (schema.decrementUsageCount()) {
                schemas.remove(schemaName);

                try {
                    dropSchema(schemaName);
                }
                catch (Exception e) {
                    U.error(log, "Failed to drop schema on cache stop (will ignore): " + cacheName, e);
                }
            }
        }

        for (H2TableDescriptor tbl : rmvTbls) {
            for (Index idx : tbl.table().getIndexes())
                idx.close(null);
        }
    }

    /**
     * Create and register schema if needed.
     *
     * @param schemaName Schema name.
     * @param predefined Whether this is predefined schema.
     */
    private void createSchema(String schemaName, boolean predefined) throws IgniteCheckedException {
        assert Thread.holdsLock(schemaMux);

        if (!predefined)
            predefined = isSchemaPredefined(schemaName);

        H2Schema schema = new H2Schema(schemaName, predefined);

        H2Schema oldSchema = schemas.putIfAbsent(schemaName, schema);

        if (oldSchema == null)
            createSchema0(schemaName);
        else
            schema = oldSchema;

        schema.incrementUsageCount();
    }

    /**
     * Creates DB schema if it has not been created yet.
     *
     * @param schema Schema name.
     */
    private void createSchema0(String schema) throws IgniteCheckedException {
        connMgr.executeSystemStatement("CREATE SCHEMA IF NOT EXISTS " + H2Utils.withQuotes(schema));

        if (log.isDebugEnabled())
            log.debug("Created H2 schema for index database: " + schema);
    }

    /**
     * Check if schema is predefined.
     *
     * @param schemaName Schema name.
     * @return {@code True} if predefined.
     */
    private boolean isSchemaPredefined(String schemaName) {
        if (F.eq(QueryUtils.DFLT_SCHEMA, schemaName))
            return true;

        for (H2Schema schema : schemas.values()) {
            if (F.eq(schema.schemaName(), schemaName) && schema.predefined())
                return true;
        }

        return false;
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

                    connMgr.executeStatement(schema, clause);
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
    private H2Schema schema(String schemaName) {
        return schemas.get(schemaName);
    }

    /**
     * Create db table by using given table descriptor.
     *
     * @param schemaName Schema name.
     * @param schema Schema.
     * @param tbl Table descriptor.
     * @param conn Connection.
     * @throws SQLException If failed to create db table.
     * @throws IgniteCheckedException If failed.
     */
    private GridH2Table createTable(String schemaName, H2Schema schema, H2TableDescriptor tbl, H2PooledConnection conn)
        throws SQLException, IgniteCheckedException {
        assert schema != null;
        assert tbl != null;

        String sql = H2Utils.tableCreateSql(tbl);

        if (log.isDebugEnabled())
            log.debug("Creating DB table with SQL: " + sql);

        GridH2RowDescriptor rowDesc = new GridH2RowDescriptor(tbl, tbl.type());

        GridH2Table h2Tbl = H2TableEngine.createTable(conn.connection(), sql, rowDesc, tbl);

        for (GridH2IndexBase usrIdx : tbl.createUserIndexes())
            createInitialUserIndex(schemaName, tbl, usrIdx);

        return h2Tbl;
    }

    /**
     * Drops table form h2 database and clear all related indexes (h2 text, lucene).
     *
     * @param tbl Table to unregister.
     */
    private void dropTable(H2TableDescriptor tbl) {
        assert tbl != null;

        if (log.isDebugEnabled())
            log.debug("Removing query index table: " + tbl.fullTableName());

        try (H2PooledConnection c = connMgr.connection(tbl.schemaName())) {
            Statement stmt = null;

            try {
                stmt = c.connection().createStatement();

                String sql = "DROP TABLE IF EXISTS " + tbl.fullTableName();

                if (log.isDebugEnabled())
                    log.debug("Dropping database index table with SQL: " + sql);

                stmt.executeUpdate(sql);
            }
            catch (SQLException e) {
                throw new IgniteSQLException("Failed to drop database index table [type=" + tbl.type().name() +
                    ", table=" + tbl.fullTableName() + "]", IgniteQueryErrorCode.TABLE_DROP_FAILED, e);
            }
            finally {
                U.close(stmt, log);
            }
        }
    }

    /**
     * Creates DB schema if it has not been created yet.
     *
     * @param schema Schema name.
     */
    private void dropSchema(String schema) throws IgniteCheckedException {
        connMgr.executeSystemStatement("DROP SCHEMA IF EXISTS " + H2Utils.withQuotes(schema));

        if (log.isDebugEnabled())
            log.debug("Dropped H2 schema for index database: " + schema);
    }

    /**
     * Add initial user index.
     *
     * @param schemaName Schema name.
     * @param desc Table descriptor.
     * @param h2Idx User index.
     * @throws IgniteCheckedException If failed.
     */
    private void createInitialUserIndex(String schemaName, H2TableDescriptor desc, GridH2IndexBase h2Idx)
        throws IgniteCheckedException {
        GridH2Table h2Tbl = desc.table();

        h2Tbl.proposeUserIndex(h2Idx);

        try {
            String sql = H2Utils.indexCreateSql(desc.fullTableName(), h2Idx, false);

            connMgr.executeStatement(schemaName, sql);
        }
        catch (Exception e) {
            // Rollback and re-throw.
            h2Tbl.rollbackUserIndex(h2Idx.getName());

            throw e;
        }
    }

    /**
     * Create index.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param idxDesc Index descriptor.
     * @param ifNotExists If-not-exists.
     * @param cacheVisitor Cache visitor.
     * @throws IgniteCheckedException If failed.
     */
    public void createIndex(String schemaName, String tblName, QueryIndexDescriptorImpl idxDesc, boolean ifNotExists,
        SchemaIndexCacheVisitor cacheVisitor) throws IgniteCheckedException {
        // Locate table.
        H2Schema schema = schema(schemaName);

        H2TableDescriptor desc = (schema != null ? schema.tableByName(tblName) : null);

        if (desc == null)
            throw new IgniteCheckedException("Table not found in internal H2 database [schemaName=" + schemaName +
                ", tblName=" + tblName + ']');

        GridH2Table h2Tbl = desc.table();

        // Create index.
        final GridH2IndexBase h2Idx = desc.createUserIndex(idxDesc);

        h2Tbl.proposeUserIndex(h2Idx);

        try {
            // Populate index with existing cache data.
            IndexRebuildPartialClosure idxBuild = new IndexRebuildPartialClosure(h2Tbl.cacheContext());

            idxBuild.addIndex(h2Tbl, h2Idx);

            cacheVisitor.visit(idxBuild);

            // At this point index is in consistent state, promote it through H2 SQL statement, so that cached
            // prepared statements are re-built.
            String sql = H2Utils.indexCreateSql(desc.fullTableName(), h2Idx, ifNotExists);

            connMgr.executeStatement(schemaName, sql);
        }
        catch (Exception e) {
            // Rollback and re-throw.
            h2Tbl.rollbackUserIndex(h2Idx.getName());

            throw e;
        }
    }

    /**
     * Drop index.
     *
     * @param schemaName Schema name.
     * @param idxName Index name.
     * @param ifExists If exists.
     * @throws IgniteCheckedException If failed.
     */
    public void dropIndex(final String schemaName, String idxName, boolean ifExists)
        throws IgniteCheckedException {
        String sql = H2Utils.indexDropSql(schemaName, idxName, ifExists);

        GridH2Table tbl = dataTableForIndex(schemaName, idxName);

        assert tbl != null;

        tbl.setRemoveIndexOnDestroy(true);

        connMgr.executeStatement(schemaName, sql);
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
    public void addColumn(String schemaName, String tblName, List<QueryField> cols,
        boolean ifTblExists, boolean ifColNotExists) throws IgniteCheckedException {
        // Locate table.
        H2Schema schema = schema(schemaName);

        H2TableDescriptor desc = (schema != null ? schema.tableByName(tblName) : null);

        if (desc == null) {
            if (!ifTblExists)
                throw new IgniteCheckedException("Table not found in internal H2 database [schemaName=" + schemaName +
                    ", tblName=" + tblName + ']');
            else
                return;
        }

        desc.table().addColumns(cols, ifColNotExists);
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
    public void dropColumn(String schemaName, String tblName, List<String> cols, boolean ifTblExists,
        boolean ifColExists) throws IgniteCheckedException {
        // Locate table.
        H2Schema schema = schema(schemaName);

        H2TableDescriptor desc = (schema != null ? schema.tableByName(tblName) : null);

        if (desc == null) {
            if (!ifTblExists)
                throw new IgniteCheckedException("Table not found in internal H2 database [schemaName=" + schemaName +
                    ",tblName=" + tblName + ']');
            else
                return;
        }

        desc.table().dropColumns(cols, ifColExists);
    }

    /**
     * Get table descriptor.
     *
     * @param schemaName Schema name.
     * @param cacheName Cache name.
     * @param type Type name.
     * @return Descriptor.
     */
    @Nullable public H2TableDescriptor tableForType(String schemaName, String cacheName, String type) {
        H2Schema schema = schema(schemaName);

        if (schema == null)
            return null;

        return schema.tableByTypeName(cacheName, type);
    }

    /**
     * Gets collection of table for given schema name.
     *
     * @param cacheName Cache name.
     * @return Collection of table descriptors.
     */
    public Collection<H2TableDescriptor> tablesForCache(String cacheName) {
        H2Schema schema = schema(schemaName(cacheName));

        if (schema == null)
            return Collections.emptySet();

        List<H2TableDescriptor> tbls = new ArrayList<>();

        for (H2TableDescriptor tbl : schema.tables()) {
            if (F.eq(tbl.cacheName(), cacheName))
                tbls.add(tbl);
        }

        return tbls;
    }

    /**
     * Find table by it's identifier.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @return Table or {@code null} if none found.
     */
    public GridH2Table dataTable(String schemaName, String tblName) {
        return dataTables.get(new QueryTable(schemaName, tblName));
    }

    /**
     * @return all known tables.
     */
    public Collection<GridH2Table> dataTables() {
        return dataTables.values();
    }

    /**
     * @return all known system views.
     */
    public Collection<SqlSystemView> systemViews() {
        return Collections.unmodifiableSet(systemViews);
    }

    /**
     * Find table for index.
     *
     * @param schemaName Schema name.
     * @param idxName Index name.
     * @return Table or {@code null} if index is not found.
     */
    public GridH2Table dataTableForIndex(String schemaName, String idxName) {
        for (Map.Entry<QueryTable, GridH2Table> dataTableEntry : dataTables.entrySet()) {
            if (F.eq(dataTableEntry.getKey().schema(), schemaName)) {
                GridH2Table h2Tbl = dataTableEntry.getValue();

                if (h2Tbl.userIndex(idxName) != null)
                    return h2Tbl;
            }
        }

        return null;
    }
}
