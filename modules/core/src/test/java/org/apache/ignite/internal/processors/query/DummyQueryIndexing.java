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
 * Empty indexing class used in tests to simulate failures.
 */
@SuppressWarnings({"deprecation", "RedundantThrows"})
public class DummyQueryIndexing implements GridQueryIndexing {
    /** {@inheritDoc} */
    @Override public void start(GridKernalContext ctx, GridSpinBusyLock busyLock) throws IgniteCheckedException {

    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteCheckedException {

    }

    /** {@inheritDoc} */
    @Override public void onClientDisconnect() throws IgniteCheckedException {

    }

    /** {@inheritDoc} */
    @Override public SqlFieldsQuery generateFieldsQuery(String cacheName, SqlQuery qry) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public List<FieldsQueryCursor<List<?>>> querySqlFields(
        String schemaName,
        SqlFieldsQuery qry,
        SqlClientContext cliCtx,
        boolean keepBinary,
        boolean failOnMultipleStmts,
        GridQueryCancel cancel
    ) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public long streamUpdateQuery(
        String schemaName,
        String qry,
        @Nullable Object[] params,
        IgniteDataStreamer<?, ?> streamer,
        String qryInitiatorId
    ) throws IgniteCheckedException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public List<Long> streamBatchedUpdateQuery(
        String schemaName,
        String qry,
        List<Object[]> params,
        SqlClientContext cliCtx,
        String qryInitiatorId
    ) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocalText(
        String schemaName,
        String cacheName,
        String qry,
        String typeName,
        IndexingQueryFilter filter,
        int limit
    ) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void registerCache(
        String cacheName,
        String schemaName,
        GridCacheContextInfo<?, ?> cacheInfo
    ) throws IgniteCheckedException {

    }

    /** {@inheritDoc} */
    @Override public void unregisterCache(GridCacheContextInfo<?, ?> cacheInfo) {

    }

    /** {@inheritDoc} */
    @Override public UpdateSourceIterator<?> executeUpdateOnDataNodeTransactional(
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
    ) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public List<JdbcParameterMeta> parameterMetaData(
        String schemaName,
        SqlFieldsQuery sql
    ) throws IgniteSQLException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @Nullable List<GridQueryFieldMetadata> resultMetaData(
        String schemaName,
        SqlFieldsQuery sql
    ) throws IgniteSQLException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void store(
        GridCacheContext cctx,
        GridQueryTypeDescriptor type,
        CacheDataRow row,
        CacheDataRow prevRow,
        boolean prevRowAvailable
    ) throws IgniteCheckedException {

    }

    /** {@inheritDoc} */
    @Override public void remove(GridCacheContext cctx, GridQueryTypeDescriptor type, CacheDataRow row) {

    }

    /** {@inheritDoc} */
    @Override public IndexingQueryFilter backupFilter(AffinityTopologyVersion topVer, int[] parts) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {

    }

    /** {@inheritDoc} */
    @Override public RunningQueryManager runningQueryManager() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop() {

    }

    /** {@inheritDoc} */
    @Override public boolean isStreamableInsertStatement(String schemaName, SqlFieldsQuery sql) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void registerMxBeans(IgniteMBeansManager mbMgr) {

    }

    /** {@inheritDoc} */
    @Override public boolean isConvertibleToColumnType(String schemaName, String tblName, String colName, Class<?> cls) {
        return false;
    }
}
