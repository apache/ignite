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

package org.apache.ignite.internal.processors.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.mvcc.MvccQueryTracker;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.jetbrains.annotations.Nullable;

/**
 * Abstraction for internal indexing implementation.
 */
public interface GridQueryIndexing {
    /**
     * Starts indexing.
     *
     * @param ctx Context.
     * @param busyLock Busy lock.
     * @throws IgniteCheckedException If failed.
     */
    public void start(GridKernalContext ctx, GridSpinBusyLock busyLock) throws IgniteCheckedException;

    /**
     * Stops indexing.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void stop() throws IgniteCheckedException;

    /**
     * Performs necessary actions on disconnect of a stateful client (say, one associated with a transaction).
     *
     * @throws IgniteCheckedException If failed.
     */
    public void onClientDisconnect() throws IgniteCheckedException;

    /**
     * Parses SQL query into two step query and executes it.
     *
     * @param schemaName Schema name.
     * @param cacheName Cache name.
     * @param qry Query.
     * @param keepBinary Keep binary flag.
     * @throws IgniteCheckedException If failed.
     */
    public <K, V> QueryCursor<Cache.Entry<K, V>> queryDistributedSql(String schemaName, String cacheName, SqlQuery qry,
        boolean keepBinary) throws IgniteCheckedException;

    /**
     * Detect whether SQL query should be executed in distributed or local manner and execute it.
     * @param schemaName Schema name.
     * @param qry Query.
     * @param cliCtx Client context.
     * @param keepBinary Keep binary flag.
     * @param failOnMultipleStmts Whether an exception should be thrown for multiple statements query.
     * @param tracker Query tracker.
     * @return Cursor.
     */
    public List<FieldsQueryCursor<List<?>>> querySqlFields(String schemaName, SqlFieldsQuery qry,
        SqlClientContext cliCtx, boolean keepBinary, boolean failOnMultipleStmts, MvccQueryTracker tracker, GridQueryCancel cancel);

    /**
     * Execute an INSERT statement using data streamer as receiver.
     *
     * @param schemaName Schema name.
     * @param qry Query.
     * @param params Query parameters.
     * @param streamer Data streamer to feed data to.
     * @return Update counter.
     * @throws IgniteCheckedException If failed.
     */
    public long streamUpdateQuery(String schemaName, String qry, @Nullable Object[] params,
        IgniteDataStreamer<?, ?> streamer) throws IgniteCheckedException;

    /**
     * Execute a batched INSERT statement using data streamer as receiver.
     *
     * @param schemaName Schema name.
     * @param qry Query.
     * @param params Query parameters.
     * @param cliCtx Client connection context.
     * @return Update counters.
     * @throws IgniteCheckedException If failed.
     */
    public List<Long> streamBatchedUpdateQuery(String schemaName, String qry, List<Object[]> params,
        SqlClientContext cliCtx) throws IgniteCheckedException;

    /**
     * Executes regular query.
     *
     * @param schemaName Schema name.
     * @param cacheName Cache name.
     * @param qry Query.
     * @param filter Cache name and key filter.
     * @param keepBinary Keep binary flag.    @return Cursor.
     */
    public <K, V> QueryCursor<Cache.Entry<K,V>> queryLocalSql(String schemaName, String cacheName, SqlQuery qry,
        IndexingQueryFilter filter, boolean keepBinary) throws IgniteCheckedException;

    /**
     * Queries individual fields (generally used by JDBC drivers).
     *
     * @param schemaName Schema name.
     * @param qry Query.
     * @param keepBinary Keep binary flag.
     * @param filter Cache name and key filter.
     * @param cancel Query cancel.
     * @return Cursor.
     */
    public FieldsQueryCursor<List<?>> queryLocalSqlFields(String schemaName, SqlFieldsQuery qry,
        boolean keepBinary, IndexingQueryFilter filter, GridQueryCancel cancel) throws IgniteCheckedException;

    /**
     * Executes text query.
     *
     * @param schemaName Schema name.
     * @param cacheName Cache name.
     * @param qry Text query.
     * @param typeName Type name.
     * @param filter Cache name and key filter.    @return Queried rows.
     * @throws IgniteCheckedException If failed.
     */
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocalText(String schemaName, String cacheName,
        String qry, String typeName, IndexingQueryFilter filter) throws IgniteCheckedException;

    /**
     * Create new index locally.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param idxDesc Index descriptor.
     * @param ifNotExists Ignore operation if index exists (instead of throwing an error).
     * @param cacheVisitor Cache visitor
     * @throws IgniteCheckedException if failed.
     */
    public void dynamicIndexCreate(String schemaName, String tblName, QueryIndexDescriptorImpl idxDesc,
        boolean ifNotExists, SchemaIndexCacheVisitor cacheVisitor) throws IgniteCheckedException;

    /**
     * Remove index from the cache.
     *
     * @param schemaName Schema name.
     * @param idxName Index name.
     * @param ifExists Ignore operation if index does not exist (instead of throwing an error).
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    public void dynamicIndexDrop(String schemaName, String idxName, boolean ifExists) throws IgniteCheckedException;

    /**
     * Add columns to dynamic table.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param cols Columns to add.
     * @param ifTblExists Ignore operation if target table does not exist (instead of throwing an error).
     * @param ifColNotExists Ignore operation if column already exists (instead of throwing an error) - is honored only
     *     for single column case.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    public void dynamicAddColumn(String schemaName, String tblName, List<QueryField> cols, boolean ifTblExists,
        boolean ifColNotExists) throws IgniteCheckedException;

    /**
     * Drop columns from dynamic table.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param cols Columns to drop.
     * @param ifTblExists Ignore operation if target table does not exist (instead of throwing an error).
     * @param ifColExists Ignore operation if column does not exist (instead of throwing an error) - is honored only
     *     for single column case.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    public void dynamicDropColumn(String schemaName, String tblName, List<String> cols, boolean ifTblExists,
        boolean ifColExists) throws IgniteCheckedException;

    /**
     * Registers cache.
     *
     * @param cacheName Cache name.
     * @param schemaName Schema name.
     * @param cctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    public void registerCache(String cacheName, String schemaName, GridCacheContext<?,?> cctx)
        throws IgniteCheckedException;

    /**
     * Unregisters cache.
     *
     * @param cctx Cache context.
     * @param rmvIdx If {@code true}, will remove index.
     * @throws IgniteCheckedException If failed to drop cache schema.
     */
    public void unregisterCache(GridCacheContext cctx, boolean rmvIdx) throws IgniteCheckedException;

    /**
     *
     * @param cctx Cache context.
     * @param ids Involved cache ids.
     * @param parts Partitions.
     * @param schema Schema name.
     * @param qry Query string.
     * @param params Query parameters.
     * @param flags Flags.
     * @param pageSize Fetch page size.
     * @param timeout Timeout.
     * @param topVer Topology version.
     * @param mvccSnapshot MVCC snapshot.
     * @param cancel Query cancel object.
     * @return Cursor over entries which are going to be changed.
     * @throws IgniteCheckedException If failed.
     */
    public UpdateSourceIterator<?> prepareDistributedUpdate(GridCacheContext<?, ?> cctx, int[] ids, int[] parts,
        String schema, String qry, Object[] params, int flags,
        int pageSize, int timeout, AffinityTopologyVersion topVer,
        MvccSnapshot mvccSnapshot, GridQueryCancel cancel) throws IgniteCheckedException;

    /**
     * Registers type if it was not known before or updates it otherwise.
     *
     * @param cctx Cache context.
     * @param desc Type descriptor.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if type was registered, {@code false} if for some reason it was rejected.
     */
    public boolean registerType(GridCacheContext cctx, GridQueryTypeDescriptor desc) throws IgniteCheckedException;

    /**
     * Updates index. Note that key is unique for cache, so if cache contains multiple indexes
     * the key should be removed from indexes other than one being updated.
     *
     * @param cctx Cache context.
     * @param type Type descriptor.
     * @param row New row.
     * @param prevRow Previous row.
     * @param prevRowAvailable Whether previous row is available.
     * @throws IgniteCheckedException If failed.
     */
    public void store(GridCacheContext cctx,
        GridQueryTypeDescriptor type,
        CacheDataRow row,
        CacheDataRow prevRow,
        boolean prevRowAvailable) throws IgniteCheckedException;

    /**
     * Removes index entry by key.
     *
     * @param cctx Cache context.
     * @param type Type descriptor.
     * @param row Row.
     * @throws IgniteCheckedException If failed.
     */
    public void remove(GridCacheContext cctx, GridQueryTypeDescriptor type, CacheDataRow row)
        throws IgniteCheckedException;

    /**
     * Rebuilds all indexes of given type from hash index.
     *
     * @param cacheName Cache name.
     * @throws IgniteCheckedException If failed.
     */
    public void rebuildIndexesFromHash(String cacheName) throws IgniteCheckedException;

    /**
     * Marks all indexes of given type for rebuild from hash index, making them unusable until rebuild finishes.
     *
     * @param cacheName Cache name.
     */
    public void markForRebuildFromHash(String cacheName);

    /**
     * Returns backup filter.
     *
     * @param topVer Topology version.
     * @param parts Partitions.
     * @return Backup filter.
     */
    public IndexingQueryFilter backupFilter(AffinityTopologyVersion topVer, int[] parts);

    /**
     * Client disconnected callback.
     *
     * @param reconnectFut Reconnect future.
     */
    public void onDisconnected(IgniteFuture<?> reconnectFut);

    /**
     * Prepare native statement to retrieve JDBC metadata from.
     *
     * @param schemaName Schema name.
     * @param sql Query.
     * @return {@link PreparedStatement} from underlying engine to supply metadata to Prepared - most likely H2.
     */
    public PreparedStatement prepareNativeStatement(String schemaName, String sql) throws SQLException;

    /**
     * Collect queries that already running more than specified duration.
     *
     * @param duration Duration to check.
     * @return Collection of long running queries.
     */
    public Collection<GridRunningQueryInfo> runningQueries(long duration);

    /**
     * Cancel specified queries.
     *
     * @param queries Queries ID's to cancel.
     */
    public void cancelQueries(Collection<Long> queries);

    /**
     * Cancels all executing queries.
     */
    public void cancelAllQueries();

    /**
     * Gets database schema from cache name.
     *
     * @param cacheName Cache name. {@code null} would be converted to an empty string.
     * @return Schema name. Should not be null since we should not fail for an invalid cache name.
     */
    public String schema(String cacheName);

    /**
     * Check if passed statement is insert statement eligible for streaming, throw an {@link IgniteSQLException} if not.
     *
     * @param nativeStmt Native statement.
     */
    public void checkStatementStreamable(PreparedStatement nativeStmt);

    /**
     * Return row cache cleaner.
     *
     * @param cacheGroupId Cache group id.
     * @return Row cache cleaner.
     */
    public GridQueryRowCacheCleaner rowCacheCleaner(int cacheGroupId);
}
