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
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
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
     * Parses SQL query into two step query and executes it.
     *
     * @param cctx Cache context.
     * @param qry Query.
     * @param cancel Query cancel.
     * @return Cursor.
     * @throws IgniteCheckedException If failed.
     */
    public QueryCursor<List<?>> queryTwoStep(GridCacheContext<?, ?> cctx, SqlFieldsQuery qry, GridQueryCancel cancel)
        throws IgniteCheckedException;

    /**
     * Parses SQL query into two step query and executes it.
     *
     * @param cctx Cache context.
     * @param qry Query.
     * @return Cursor.
     * @throws IgniteCheckedException If failed.
     */
    public <K,V> QueryCursor<Cache.Entry<K,V>> queryTwoStep(GridCacheContext<?,?> cctx, SqlQuery qry)
        throws IgniteCheckedException;

    /**
     * Queries individual fields (generally used by JDBC drivers).
     *
     * @param spaceName Space name.
     * @param qry Query.
     * @param params Query parameters.
     * @param filter Space name and key filter.
     * @param enforceJoinOrder Enforce join order of tables in the query.
     * @param timeout Query timeout in milliseconds.
     * @param cancel Query cancel.
     * @return Query result.
     * @throws IgniteCheckedException If failed.
     */
    public GridQueryFieldsResult queryLocalSqlFields(@Nullable String spaceName, String qry,
        Collection<Object> params, IndexingQueryFilter filter, boolean enforceJoinOrder, int timeout,
        GridQueryCancel cancel) throws IgniteCheckedException;

    /**
     * Executes regular query.
     *
     * @param spaceName Space name.
     * @param qry Query.
     * @param alias Table alias used in Query.
     * @param params Query parameters.
     * @param type Query return type.
     * @param filter Space name and key filter.
     * @return Queried rows.
     * @throws IgniteCheckedException If failed.
     */
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocalSql(@Nullable String spaceName, String qry,
        String alias, Collection<Object> params, GridQueryTypeDescriptor type, IndexingQueryFilter filter)
        throws IgniteCheckedException;

    /**
     * Executes text query.
     *
     * @param spaceName Space name.
     * @param qry Text query.
     * @param type Query return type.
     * @param filter Space name and key filter.
     * @return Queried rows.
     * @throws IgniteCheckedException If failed.
     */
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocalText(@Nullable String spaceName, String qry,
        GridQueryTypeDescriptor type, IndexingQueryFilter filter) throws IgniteCheckedException;

    /**
     * Registers cache.
     *
     * @param cctx Cache context.
     * @param ccfg Cache configuration.
     * @throws IgniteCheckedException If failed.
     */
    public void registerCache(GridCacheContext<?,?> cctx, CacheConfiguration<?,?> ccfg) throws IgniteCheckedException;

    /**
     * Unregisters cache.
     *
     * @param ccfg Cache configuration.
     * @throws IgniteCheckedException If failed to drop cache schema.
     */
    public void unregisterCache(CacheConfiguration<?, ?> ccfg) throws IgniteCheckedException;

    /**
     * Registers type if it was not known before or updates it otherwise.
     *
     * @param spaceName Space name.
     * @param desc Type descriptor.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if type was registered, {@code false} if for some reason it was rejected.
     */
    public boolean registerType(@Nullable String spaceName, GridQueryTypeDescriptor desc) throws IgniteCheckedException;

    /**
     * Unregisters type and removes all corresponding data.
     *
     * @param spaceName Space name.
     * @param type Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    public void unregisterType(@Nullable String spaceName, GridQueryTypeDescriptor type) throws IgniteCheckedException;

    /**
     * Updates index. Note that key is unique for space, so if space contains multiple indexes
     * the key should be removed from indexes other than one being updated.
     *
     * @param spaceName Space name.
     * @param type Value type.
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     * @param expirationTime Expiration time or 0 if never expires.
     * @throws IgniteCheckedException If failed.
     */
    public void store(@Nullable String spaceName, GridQueryTypeDescriptor type, CacheObject key, CacheObject val,
        byte[] ver, long expirationTime) throws IgniteCheckedException;

    /**
     * Removes index entry by key.
     *
     * @param spaceName Space name.
     * @param key Key.
     * @param val Value.
     * @throws IgniteCheckedException If failed.
     */
    public void remove(@Nullable String spaceName, CacheObject key, CacheObject val) throws IgniteCheckedException;

    /**
     * Will be called when entry with given key is swapped.
     *
     * @param spaceName Space name.
     * @param key Key.
     * @throws IgniteCheckedException If failed.
     */
    public void onSwap(@Nullable String spaceName, CacheObject key) throws IgniteCheckedException;

    /**
     * Will be called when entry with given key is unswapped.
     *
     * @param spaceName Space name.
     * @param key Key.
     * @param val Value.
     * @throws IgniteCheckedException If failed.
     */
    public void onUnswap(@Nullable String spaceName, CacheObject key, CacheObject val) throws IgniteCheckedException;

    /**
     * Rebuilds all indexes of given type.
     *
     * @param spaceName Space name.
     * @param type Type descriptor.
     */
    public void rebuildIndexes(@Nullable String spaceName, GridQueryTypeDescriptor type);

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
     * @param schema Schema.
     * @param sql Query.
     * @return {@link PreparedStatement} from underlying engine to supply metadata to Prepared - most likely H2.
     */
    public PreparedStatement prepareNativeStatement(String schema, String sql) throws SQLException;

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
}
