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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridQueryRowDescriptorImpl;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ProxyIndex;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sys.SqlSystemTableEngine;
import org.apache.ignite.internal.processors.query.h2.sys.view.FiltrableSystemViewLocal;
import org.apache.ignite.internal.processors.query.h2.sys.view.SystemViewLocal;
import org.apache.ignite.internal.processors.query.schema.SchemaChangeListener;
import org.apache.ignite.internal.processors.query.schema.management.IndexDescriptor;
import org.apache.ignite.internal.processors.query.schema.management.SchemaManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.systemview.view.FiltrableSystemView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.h2.index.Index;
import org.jetbrains.annotations.Nullable;

/**
 * H2 schema manager. Responsible for reflecting to H2 database all schema changes.
 */
public class H2SchemaManager implements SchemaChangeListener {
    /** Connection manager. */
    private final ConnectionManager connMgr;

    /** Data tables. */
    private final ConcurrentMap<QueryTable, GridH2Table> dataTables = new ConcurrentHashMap<>();

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Indexing. */
    private final IgniteH2Indexing idx;

    /** H2 index factory. */
    private final H2IndexFactory idxFactory;

    /** Logger. */
    private final IgniteLogger log;

    /** Underlying core schema manager. */
    private volatile SchemaManager schemaMgr;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param idx Indexing.
     * @param connMgr Connection manager.
     */
    public H2SchemaManager(GridKernalContext ctx, IgniteH2Indexing idx, ConnectionManager connMgr) {
        this.ctx = ctx;
        this.idx = idx;
        this.connMgr = connMgr;

        idxFactory = new H2IndexFactory(ctx);
        log = ctx.log(H2SchemaManager.class);
    }

    /**
     * Handle node start.
     */
    public void start() throws IgniteCheckedException {
        schemaMgr = ctx.query().schemaManager();

        ctx.internalSubscriptionProcessor().registerSchemaChangeListener(this);

        // Register predefined system functions.
        createSqlFunction(QueryUtils.DFLT_SCHEMA, "QUERY_ENGINE", true,
            H2Utils.class.getName() + ".queryEngine");
    }

    /** {@inheritDoc} */
    @Override public void onSchemaCreated(String schema) {
        try {
            connMgr.executeSystemStatement("CREATE SCHEMA IF NOT EXISTS " + H2Utils.withQuotes(schema));

            if (log.isDebugEnabled())
                log.debug("Created H2 schema for index database: " + schema);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to create database schema: " + schema, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onSchemaDropped(String schema) {
        try {
            connMgr.executeSystemStatement("DROP SCHEMA IF EXISTS " + H2Utils.withQuotes(schema));

            if (log.isDebugEnabled())
                log.debug("Dropped H2 schema for index database: " + schema);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to drop database schema: " + schema, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onSqlTypeCreated(
        String schemaName,
        GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?, ?> cacheInfo
    ) {
        H2TableDescriptor tblDesc = new H2TableDescriptor(idx, schemaName, typeDesc, cacheInfo);

        try (H2PooledConnection conn = connMgr.connection(schemaName)) {
            GridH2Table h2tbl = createTable(tblDesc, conn);

            if (dataTables.putIfAbsent(h2tbl.identifier(), h2tbl) != null)
                throw new IllegalStateException("Table already exists: " + h2tbl.identifierString());
        }
        catch (SQLException e) {
            throw new IgniteException("Failed to register query type: " + tblDesc, e);
        }
    }

    /**
     * Create db table by using given table descriptor.
     *
     * @param tbl Table descriptor.
     * @param conn Connection.
     * @throws SQLException If failed to create db table.
     */
    private GridH2Table createTable(H2TableDescriptor tbl, H2PooledConnection conn) throws SQLException {
        assert tbl != null;

        String sql = H2Utils.tableCreateSql(tbl);

        if (log.isDebugEnabled())
            log.debug("Creating DB table with SQL: " + sql);

        GridH2RowDescriptor rowDesc = new GridH2RowDescriptor(
            new GridQueryRowDescriptorImpl(tbl.cacheInfo(), tbl.type()));

        return H2TableEngine.createTable(conn.connection(), sql, rowDesc, tbl, ctx.indexProcessor());
    }

    /** {@inheritDoc} */
    @Override public void onSqlTypeDropped(String schemaName, GridQueryTypeDescriptor typeDesc, boolean destroy) {
        GridH2Table tbl = dataTable(schemaName, typeDesc.tableName());

        if (tbl == null) {
            throw new IgniteException("Failed to drop database table (table not found) [schemaName=" + schemaName +
                ", tblName=" + typeDesc.tableName() + ']');
        }

        dropTable(tbl.tableDescriptor());
        dataTables.remove(tbl.identifier(), tbl);
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
                throw new IgniteSQLException("Failed to drop database table [type=" + tbl.type().name() +
                    ", table=" + tbl.fullTableName() + "]", IgniteQueryErrorCode.TABLE_DROP_FAILED, e);
            }
            finally {
                U.close(stmt, log);
            }
        }

        tbl.onDrop();
    }

    /** {@inheritDoc} */
    @Override public void onFunctionCreated(String schema, String name, boolean deterministic, Method method) {
        if (!Modifier.isStatic(method.getModifiers())) {
            log.warning("Skip creating SQL function '" + name + "' in H2 engine because it is not static.");

            return;
        }

        try {
            createSqlFunction(schema, name, deterministic,
                method.getDeclaringClass().getName() + '.' + method.getName());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to create database function: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onTableFunctionCreated(
        String schemaName,
        String name,
        Method method,
        Class<?>[] colTypes,
        @Nullable String[] colNames
    ) {
        log.warning("Skipped creation of SQL table function '" + name + "' in H2 engine. Table functions arent't supported yet.");
    }

    /**
     * Registers SQL function.
     *
     * @param schema Schema.
     * @param alias Function alias.
     * @param deterministic Deterministic flag.
     * @param methodName Public static method name (including class full name).
     */
    private void createSqlFunction(String schema, String alias, boolean deterministic, String methodName)
        throws IgniteCheckedException {
        String clause = "CREATE ALIAS IF NOT EXISTS " + alias + (deterministic ?
            " DETERMINISTIC FOR \"" :
            " FOR \"") +
            methodName + '"';

        connMgr.executeStatement(schema, clause);
    }

    /** {@inheritDoc} */
    @Override public void onSystemViewCreated(String schema, SystemView<?> view) {
        SystemViewLocal<?> sysView = view instanceof FiltrableSystemView ?
            new FiltrableSystemViewLocal<>(ctx, view) : new SystemViewLocal<>(ctx, view);

        try {
            try (H2PooledConnection c = connMgr.connection(schema)) {
                SqlSystemTableEngine.registerView(c.connection(), sysView);
            }
        }
        catch (SQLException e) {
            throw new IgniteException("Failed to register system view: " + view.name(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onIndexCreated(String schemaName, String tblName, String idxName, IndexDescriptor idxDesc) {
        GridH2Table tbl = dataTable(schemaName, tblName);

        if (tbl == null)
            return;

        try {
            Index h2Idx = idxFactory.createIndex(tbl, idxDesc);

            // Do not register system indexes as DB objects.
            if (isSystemIndex(h2Idx)) {
                tbl.addSystemIndex(h2Idx);

                return;
            }

            tbl.proposeUserIndex(h2Idx);

            try {
                // At this point index is in consistent state, promote it through H2 SQL statement, so that cached
                // prepared statements are re-built.
                String sql = H2Utils.indexCreateSql(tbl.tableDescriptor().fullTableName(), h2Idx, true);

                connMgr.executeStatement(schemaName, sql);
            }
            catch (Exception e) {
                // Rollback and re-throw.
                tbl.rollbackUserIndex(h2Idx.getName());

                throw e;
            }
        }
        catch (Exception e) {
            throw new IgniteException("Failed to register index in internal H2 database: " + idxName, e);
        }
    }

    /** */
    private static boolean isSystemIndex(Index idx) {
        return QueryUtils.PRIMARY_KEY_INDEX.equals(idx.getName())
            || QueryUtils.AFFINITY_KEY_INDEX.equals(idx.getName())
            || (idx instanceof GridH2ProxyIndex && isSystemIndex(((GridH2ProxyIndex)idx).underlyingIndex()));
    }

    /** {@inheritDoc} */
    @Override public void onIndexDropped(String schemaName, String tblName, String idxName) {
        GridH2Table tbl = dataTable(schemaName, tblName);

        if (tbl == null)
            return;

        Index idx = F.find(tbl.getIndexes(), null, i -> idxName.equals(i.getName()));

        if (idx == null)
            return;

        // System indexes are not registred as DB objects.
        if (isSystemIndex(idx)) {
            tbl.removeIndex(idx);

            return;
        }

        String sql = H2Utils.indexDropSql(schemaName, idxName, true);

        try {
            connMgr.executeStatement(schemaName, sql);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to unregister index: " + idxName, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onColumnsAdded(
        String schemaName,
        GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?, ?> cacheInfo,
        List<QueryField> cols
    ) {
        GridH2Table tbl = dataTable(schemaName, typeDesc.tableName());

        if (tbl == null)
            return;

        tbl.addColumns(cols);
    }

    /** {@inheritDoc} */
    @Override public void onColumnsDropped(
        String schemaName,
        GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?, ?> cacheInfo,
        List<String> cols
    ) {
        GridH2Table tbl = dataTable(schemaName, typeDesc.tableName());

        if (tbl == null)
            return;

        tbl.dropColumns(cols);
    }

    /** {@inheritDoc} */
    @Override public void onIndexRebuildStarted(String schemaName, String tblName) {
        GridH2Table tbl = dataTable(schemaName, tblName);

        if (tbl != null)
            tbl.markRebuildFromHashInProgress(true);
    }

    /** {@inheritDoc} */
    @Override public void onIndexRebuildFinished(String schemaName, String tblName) {
        GridH2Table tbl = dataTable(schemaName, tblName);

        if (tbl != null)
            tbl.markRebuildFromHashInProgress(false);
    }

    /** {@inheritDoc} */
    @Override public void onViewCreated(String schemaName, String viewName, String viewSql) {
        try (H2PooledConnection conn = connMgr.connection(schemaName)) {
            try (Statement s = conn.connection().createStatement()) {
                s.execute("CREATE OR REPLACE VIEW \"" + viewName + "\" AS " + viewSql);
            }
        }
        catch (SQLException e) {
            throw new IgniteException("Failed to create view: " + viewName, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onViewDropped(String schemaName, String viewName) {
        try (H2PooledConnection conn = connMgr.connection(schemaName)) {
            try (Statement s = conn.connection().createStatement()) {
                s.execute("DROP VIEW IF EXISTS \"" + viewName + "\"");
            }
        }
        catch (SQLException e) {
            throw new IgniteException("Failed to drop view: " + viewName, e);
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
    @Nullable public H2TableDescriptor tableForType(String schemaName, String cacheName, String type) {
        GridQueryTypeDescriptor typeDesc = schemaMgr.typeDescriptorForType(schemaName, cacheName, type);

        if (typeDesc != null) {
            GridH2Table tbl = dataTable(schemaName, typeDesc.tableName());

            if (tbl != null)
                return tbl.tableDescriptor();
        }

        return null;
    }

    /**
     * Find H2 table by it's identifier.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @return Table or {@code null} if none found.
     */
    public GridH2Table dataTable(String schemaName, String tblName) {
        return dataTables.get(new QueryTable(schemaName, tblName));
    }
}
