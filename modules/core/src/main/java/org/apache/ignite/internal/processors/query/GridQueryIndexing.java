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

import java.sql.SQLException;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.IgniteMBeansManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcParameterMeta;
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
     * Generate SqlFieldsQuery from SqlQuery.
     *
     * @param cacheName Cache name.
     * @param qry Query.
     * @return Fields query.
     */
    public SqlFieldsQuery generateFieldsQuery(String cacheName, SqlQuery qry);

    /**
     * Detect whether SQL query should be executed in distributed or local manner and execute it.
     * @param schemaName Schema name.
     * @param qry Query.
     * @param cliCtx Client context.
     * @param keepBinary Keep binary flag.
     * @param failOnMultipleStmts Whether an exception should be thrown for multiple statements query.
     * @return Cursor.
     */
    public List<FieldsQueryCursor<List<?>>> querySqlFields(
        String schemaName,
        SqlFieldsQuery qry,
        SqlClientContext cliCtx,
        boolean keepBinary,
        boolean failOnMultipleStmts,
        GridQueryCancel cancel
    );

    /**
     * Execute an INSERT statement using data streamer as receiver.
     *
     * @param schemaName Schema name.
     * @param qry Query.
     * @param params Query parameters.
     * @param streamer Data streamer to feed data to.
     * @param qryInitiatorId Query initiator ID.
     * @return Update counter.
     * @throws IgniteCheckedException If failed.
     */
    public long streamUpdateQuery(String schemaName, String qry, @Nullable Object[] params,
        IgniteDataStreamer<?, ?> streamer, String qryInitiatorId) throws IgniteCheckedException;

    /**
     * Execute a batched INSERT statement using data streamer as receiver.
     *
     * @param schemaName Schema name.
     * @param qry Query.
     * @param params Query parameters.
     * @param cliCtx Client connection context.
     * @param qryInitiatorId Query initiator ID.
     * @return Update counters.
     * @throws IgniteCheckedException If failed.
     */
    public List<Long> streamBatchedUpdateQuery(String schemaName, String qry, List<Object[]> params,
        SqlClientContext cliCtx, String qryInitiatorId) throws IgniteCheckedException;

    /**
     * Executes text query.
     *
     * @param schemaName Schema name.
     * @param cacheName Cache name.
     * @param qry Text query.
     * @param typeName Type name.
     * @param filter Cache name and key filter.    @return Queried rows.
     * @param limit Limits response records count. If 0 or less, the limit considered to be Integer.MAX_VALUE, that is virtually no limit.
     * @throws IgniteCheckedException If failed.
     */
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocalText(String schemaName, String cacheName,
        String qry, String typeName, IndexingQueryFilter filter, int limit) throws IgniteCheckedException;

    /**
     * Registers cache.
     *
     * @param cacheName Cache name.
     * @param schemaName Schema name.
     * @param cacheInfo Cache context info.
     * @throws IgniteCheckedException If failed.
     */
    public void registerCache(String cacheName, String schemaName, GridCacheContextInfo<?, ?> cacheInfo)
        throws IgniteCheckedException;

    /**
     * Unregisters cache.
     *
     * @param cacheInfo Cache context info.
     */
    public void unregisterCache(GridCacheContextInfo<?, ?> cacheInfo);

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
    public UpdateSourceIterator<?> executeUpdateOnDataNodeTransactional(
        GridCacheContext<?, ?> cctx,
        int[] ids,
        int[] parts,
        String schema,
        String qry,
        Object[] params,
        int flags,
        int pageSize,
        int timeout,
        AffinityTopologyVersion topVer,
        MvccSnapshot mvccSnapshot,
        GridQueryCancel cancel
    ) throws IgniteCheckedException;

    /**
     * Jdbc parameters metadata of the specified query.
     *
     * @param schemaName the default schema name for query.
     * @param sql Sql query.
     * @return metadata describing all the parameters, even in case of multi-statement.
     * @throws SQLException if failed to get meta.
     */
    public List<JdbcParameterMeta> parameterMetaData(String schemaName, SqlFieldsQuery sql) throws IgniteSQLException;

    /**
     * Metadata of the result set that is returned if specified query gets executed.
     *
     * @param schemaName the default schema name for query.
     * @param sql Sql query.
     * @return metadata or {@code null} if provided query is multi-statement or id it's not a SELECT statement.
     * @throws SQLException if failed to get meta.
     */
    @Nullable public List<GridQueryFieldMetadata> resultMetaData(String schemaName, SqlFieldsQuery sql)
        throws IgniteSQLException;

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
     * @return Running query manager.
     */
    public RunningQueryManager runningQueryManager();

    /**
     * Cancels all executing queries.
     */
    public void onKernalStop();

    /**
     * Whether passed sql statement is single insert statement eligible for streaming.
     *
     * @param schemaName name of the schema.
     * @param sql sql statement.
     */
    public boolean isStreamableInsertStatement(String schemaName, SqlFieldsQuery sql) throws SQLException;

    /**
     * Register SQL JMX beans.
     *
     * @param mbMgr Ignite MXBean manager.
     * @throws IgniteCheckedException On bean registration error.
     */
    public void registerMxBeans(IgniteMBeansManager mbMgr) throws IgniteCheckedException;
}
