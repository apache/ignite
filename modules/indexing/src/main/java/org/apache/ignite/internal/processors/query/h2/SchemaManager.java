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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.database.H2RowFactory;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sys.SqlSystemTableEngine;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemView;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemViewBaselineNodes;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemViewCacheGroups;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemViewCaches;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemViewNodeAttributes;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemViewNodeMetrics;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemViewNodes;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.index.Index;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Schema manager. Responsible for all manipulations on schema objects.
 */
public class SchemaManager {
    /** Connection manager. */
    private final ConnectionManager connMgr;

    /** Collection of schemaNames and registered tables. */
    private final ConcurrentMap<String, H2Schema> schemas = new ConcurrentHashMap<>();

    /** Cache name -> schema name */
    private final Map<String, String> cacheName2schema = new ConcurrentHashMap<>();

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
     * Create system views.
     */
    private void createSystemViews() throws IgniteCheckedException {
        boolean disabled = IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_SQL_DISABLE_SYSTEM_VIEWS);

        if (disabled) {
            log.info("SQL system views will not be created because they are disabled (see " +
                    IgniteSystemProperties.IGNITE_SQL_DISABLE_SYSTEM_VIEWS + " system property)");

            return;
        }

        try {
            synchronized (schemaMux) {
                createSchema0(QueryUtils.SCHEMA_SYS);
            }

            try (Connection c = connMgr.connectionNoCache(QueryUtils.SCHEMA_SYS)) {
                for (SqlSystemView view : systemViews(ctx))
                    SqlSystemTableEngine.registerView(c, view);
            }
        }
        catch (SQLException e) {
            throw new IgniteCheckedException("Failed to register system view.", e);
        }
    }

    /**
     * @param ctx Context.
     * @return Predefined system views.
     */
    private Collection<SqlSystemView> systemViews(GridKernalContext ctx) {
        Collection<SqlSystemView> views = new ArrayList<>();

        views.add(new SqlSystemViewNodes(ctx));
        views.add(new SqlSystemViewNodeAttributes(ctx));
        views.add(new SqlSystemViewBaselineNodes(ctx));
        views.add(new SqlSystemViewNodeMetrics(ctx));
        views.add(new SqlSystemViewCaches(ctx));
        views.add(new SqlSystemViewCacheGroups(ctx));

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
     * This implementation doesn't support type reregistration.
     *
     * @param schema Schema.
     * @param tblDesc Table descriptor.
     * @throws IgniteCheckedException In case of error.
     */
    public GridH2Table onCacheTypeCreated(H2Schema schema, H2TableDescriptor tblDesc)
        throws IgniteCheckedException {
        try {
            Connection conn = connMgr.connectionForThread(schema.schemaName());

            GridH2Table tbl0 = createTable(schema.schemaName(), schema, tblDesc, conn);

            schema.add(tblDesc);

            return tbl0;
        }
        catch (SQLException e) {
            connMgr.onSqlException();

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
     * Get schema by name.
     *
     * @param schemaName Schema name.
     * @return Schema.
     */
    // TODO: Should be private!
    public H2Schema schema(String schemaName) {
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
    private GridH2Table createTable(String schemaName, H2Schema schema, H2TableDescriptor tbl, Connection conn)
        throws SQLException, IgniteCheckedException {
        assert schema != null;
        assert tbl != null;

        String sql = H2Utils.tableCreateSql(tbl);

        if (log.isDebugEnabled())
            log.debug("Creating DB table with SQL: " + sql);

        GridH2RowDescriptor rowDesc = new GridH2RowDescriptor(tbl, tbl.type());

        H2RowFactory rowFactory = tbl.rowFactory(rowDesc);

        GridH2Table h2Tbl = H2TableEngine.createTable(conn, sql, rowDesc, rowFactory, tbl);

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

        Connection c = connMgr.connectionForThread(tbl.schemaName());

        Statement stmt = null;

        try {
            stmt = c.createStatement();

            String sql = "DROP TABLE IF EXISTS " + tbl.fullTableName();

            if (log.isDebugEnabled())
                log.debug("Dropping database index table with SQL: " + sql);

            stmt.executeUpdate(sql);
        }
        catch (SQLException e) {
            connMgr.onSqlException();

            throw new IgniteSQLException("Failed to drop database index table [type=" + tbl.type().name() +
                ", table=" + tbl.fullTableName() + "]", IgniteQueryErrorCode.TABLE_DROP_FAILED, e);
        }
        finally {
            U.close(stmt, log);
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
            final GridH2RowDescriptor rowDesc = h2Tbl.rowDescriptor();

            cacheVisitor.visit(new IndexBuildClosure(rowDesc, h2Idx));

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
        throws IgniteCheckedException{
        String sql = H2Utils.indexDropSql(schemaName, idxName, ifExists);

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
}
