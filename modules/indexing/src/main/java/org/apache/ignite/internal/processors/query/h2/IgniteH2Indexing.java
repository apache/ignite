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

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheServerNotFoundException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxSelectForUpdateFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.TxTopologyVersionFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccQueryTracker;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.mvcc.StaticMvccQueryTracker;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMarshallable;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.query.RegisteredQueryCursor;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxAdapter;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.processors.query.CacheQueryObjectValueContext;
import org.apache.ignite.internal.processors.query.EnlistOperation;
import org.apache.ignite.internal.processors.query.GridQueryCacheObjectsIterator;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResult;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResultAdapter;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.RunningQueryManager;
import org.apache.ignite.internal.processors.query.SqlClientContext;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.internal.processors.query.h2.affinity.H2PartitionResolver;
import org.apache.ignite.internal.processors.query.h2.dml.DmlDistributedPlanInfo;
import org.apache.ignite.internal.processors.query.h2.dml.DmlUpdateResultsIterator;
import org.apache.ignite.internal.processors.query.h2.dml.DmlUpdateSingleEntryIterator;
import org.apache.ignite.internal.processors.query.h2.dml.UpdateMode;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlanBuilder;
import org.apache.ignite.internal.processors.query.h2.opt.QueryContext;
import org.apache.ignite.internal.processors.query.h2.opt.QueryContextRegistry;
import org.apache.ignite.internal.processors.query.h2.twostep.PartitionReservationManager;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionResult;
import org.apache.ignite.internal.processors.query.h2.affinity.PartitionExtractor;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeClientIndex;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2InnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2MvccInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2MvccLeafIO;
import org.apache.ignite.internal.processors.query.h2.dml.DmlUtils;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlan;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAlias;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlTable;
import org.apache.ignite.internal.processors.query.h2.twostep.GridMapQueryExecutor;
import org.apache.ignite.internal.processors.query.h2.twostep.GridReduceQueryExecutor;
import org.apache.ignite.internal.processors.query.h2.twostep.MapQueryLazyWorker;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorImpl;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.sql.command.SqlCommitTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlRollbackTransactionCommand;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.GridEmptyCloseableIterator;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.lang.IgniteSingletonIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.util.worker.GridWorkerFuture;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilterImpl;
import org.h2.api.ErrorCode;
import org.h2.api.JavaObjectSerializer;
import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.table.IndexColumn;
import org.h2.util.JdbcUtils;
import org.jetbrains.annotations.Nullable;

import static java.lang.Boolean.FALSE;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.checkActive;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.mvccEnabled;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.requestSnapshot;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.tx;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.txStart;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.TEXT;
import static org.apache.ignite.internal.processors.query.h2.PreparedStatementEx.MVCC_CACHE_ID;
import static org.apache.ignite.internal.processors.query.h2.PreparedStatementEx.MVCC_STATE;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest.isDataPageScanEnabled;

/**
 * Indexing implementation based on H2 database engine. In this implementation main query language is SQL,
 * fulltext indexing can be performed using Lucene.
 * <p>
 * For each registered {@link GridQueryTypeDescriptor} this SPI will create respective SQL table with
 * {@code '_key'} and {@code '_val'} fields for key and value, and fields from
 * {@link GridQueryTypeDescriptor#fields()}.
 * For each table it will create indexes declared in {@link GridQueryTypeDescriptor#indexes()}.
 */
public class IgniteH2Indexing implements GridQueryIndexing {
    /*
     * Register IO for indexes.
     */
    static {
        PageIO.registerH2(H2InnerIO.VERSIONS, H2LeafIO.VERSIONS, H2MvccInnerIO.VERSIONS, H2MvccLeafIO.VERSIONS);

        H2ExtrasInnerIO.register();
        H2ExtrasLeafIO.register();
    }

    /** Dummy metadata for update result. */
    public static final List<GridQueryFieldMetadata> UPDATE_RESULT_META =
        Collections.singletonList(new H2SqlFieldMetadata(null, null, "UPDATED", Long.class.getName(), -1, -1));

    /** Default number of attempts to re-run DELETE and UPDATE queries in case of concurrent modifications of values. */
    private static final int DFLT_UPDATE_RERUN_ATTEMPTS = 4;

    /** Default size for update plan cache. */
    private static final int UPDATE_PLAN_CACHE_SIZE = 1024;

    /** Cached value of {@code IgniteSystemProperties.IGNITE_ALLOW_DML_INSIDE_TRANSACTION}. */
    private final boolean updateInTxAllowed =
        Boolean.getBoolean(IgniteSystemProperties.IGNITE_ALLOW_DML_INSIDE_TRANSACTION);

    /** Update plans cache. */
    private volatile ConcurrentMap<H2CachedStatementKey, UpdatePlan> updatePlanCache =
        new GridBoundedConcurrentLinkedHashMap<>(UPDATE_PLAN_CACHE_SIZE);

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Node ID. */
    private UUID nodeId;

    /** */
    private Marshaller marshaller;

    /** */
    private GridMapQueryExecutor mapQryExec;

    /** */
    private GridReduceQueryExecutor rdcQryExec;

    /** */
    private GridSpinBusyLock busyLock;

    /** Row cache. */
    private final H2RowCacheRegistry rowCache = new H2RowCacheRegistry();

    /** */
    protected volatile GridKernalContext ctx;

    /** Cache object value context. */
    protected CacheQueryObjectValueContext valCtx;

    /** Query context registry. */
    private final QueryContextRegistry qryCtxRegistry = new QueryContextRegistry();

    /** Processor to execute commands which are neither SELECT, nor DML. */
    private CommandProcessor cmdProc;

    /** Partition reservation manager. */
    private PartitionReservationManager partReservationMgr;

    /** Partition extractor. */
    private PartitionExtractor partExtractor;

    /** Running query manager. */
    private RunningQueryManager runningQryMgr;

    /** Parser. */
    private QueryParser parser;

    /** */
    private final IgniteInClosure<? super IgniteInternalFuture<?>> logger = new IgniteInClosure<IgniteInternalFuture<?>>() {
        @Override public void apply(IgniteInternalFuture<?> fut) {
            try {
                fut.get();
            }
            catch (IgniteCheckedException e) {
                U.error(log, e.getMessage(), e);
            }
        }
    };

    /** Query executor. */
    private ConnectionManager connMgr;

    /** Schema manager. */
    private SchemaManager schemaMgr;

    /**
     * @return Kernal context.
     */
    public GridKernalContext kernalContext() {
        return ctx;
    }

    /**
     * @param c Connection.
     * @param sql SQL.
     * @return <b>Cached</b> prepared statement.
     */
    @SuppressWarnings("ConstantConditions")
    @Nullable private PreparedStatement cachedStatement(Connection c, String sql) {
        try {
            return connMgr.cachedPreparedStatement(c, sql);
        }
        catch (SQLException e) {
            // We actually don't except anything SQL related here as we're supposed to work with cache only.
            throw new AssertionError(e);
        }
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement prepareNativeStatement(String schemaName, String sql) {
        Connection conn = connMgr.connectionForThread().connection(schemaName);

        return prepareStatementAndCaches(conn, sql);
    }

    /** {@inheritDoc} */
    @Override public void store(GridCacheContext cctx,
        GridQueryTypeDescriptor type,
        CacheDataRow row,
        @Nullable CacheDataRow prevRow,
        boolean prevRowAvailable
    ) throws IgniteCheckedException {
        String cacheName = cctx.name();

        H2TableDescriptor tbl = schemaMgr.tableForType(schema(cacheName), cacheName, type.name());

        if (tbl == null)
            return; // Type was rejected.

        tbl.table().update(row, prevRow,  prevRowAvailable);

        if (tbl.luceneIndex() != null) {
            long expireTime = row.expireTime();

            if (expireTime == 0L)
                expireTime = Long.MAX_VALUE;

            tbl.luceneIndex().store(row.key(), row.value(), row.version(), expireTime);
        }
    }

    /** {@inheritDoc} */
    @Override public void remove(GridCacheContext cctx, GridQueryTypeDescriptor type, CacheDataRow row)
        throws IgniteCheckedException {
        if (log.isDebugEnabled()) {
            log.debug("Removing key from cache query index [locId=" + nodeId +
                ", key=" + row.key() +
                ", val=" + row.value() + ']');
        }

        String cacheName = cctx.name();

        H2TableDescriptor tbl = schemaMgr.tableForType(schema(cacheName), cacheName, type.name());

        if (tbl == null)
            return;

        if (tbl.table().remove(row)) {
            if (tbl.luceneIndex() != null)
                tbl.luceneIndex().remove(row.key());
        }
    }

    /** {@inheritDoc} */
    @Override public void dynamicIndexCreate(String schemaName, String tblName, QueryIndexDescriptorImpl idxDesc,
        boolean ifNotExists, SchemaIndexCacheVisitor cacheVisitor) throws IgniteCheckedException {
        schemaMgr.createIndex(schemaName, tblName, idxDesc, ifNotExists, cacheVisitor);
    }

    /** {@inheritDoc} */
    @Override public void dynamicIndexDrop(String schemaName, String idxName, boolean ifExists)
        throws IgniteCheckedException{
        schemaMgr.dropIndex(schemaName, idxName, ifExists);
    }

    /** {@inheritDoc} */
    @Override public void dynamicAddColumn(String schemaName, String tblName, List<QueryField> cols,
        boolean ifTblExists, boolean ifColNotExists) throws IgniteCheckedException {
        schemaMgr.addColumn(schemaName, tblName, cols, ifTblExists, ifColNotExists);

        clearPlanCache();
    }

    /** {@inheritDoc} */
    @Override public void dynamicDropColumn(String schemaName, String tblName, List<String> cols, boolean ifTblExists,
        boolean ifColExists) throws IgniteCheckedException {
        schemaMgr.dropColumn(schemaName, tblName, cols, ifTblExists, ifColExists);

        clearPlanCache();
    }

    /**
     * Create sorted index.
     *
     * @param name Index name,
     * @param tbl Table.
     * @param pk Primary key flag.
     * @param affinityKey Affinity key flag.
     * @param unwrappedCols Unwrapped index columns for complex types.
     * @param wrappedCols Index columns as is complex types.
     * @param inlineSize Index inline size.
     * @return Index.
     */
    @SuppressWarnings("ConstantConditions")
    GridH2IndexBase createSortedIndex(String name, GridH2Table tbl, boolean pk, boolean affinityKey,
        List<IndexColumn> unwrappedCols, List<IndexColumn> wrappedCols, int inlineSize) {
        try {
            GridCacheContextInfo cacheInfo = tbl.cacheInfo();

            if (log.isDebugEnabled())
                log.debug("Creating cache index [cacheId=" + cacheInfo.cacheId() + ", idxName=" + name + ']');

            if (cacheInfo.affinityNode()) {
                final int segments = tbl.rowDescriptor().context().config().getQueryParallelism();

                H2RowCache cache = rowCache.forGroup(cacheInfo.groupId());

                return new H2TreeIndex(
                    cacheInfo.cacheContext(),
                    cache,
                    tbl,
                    name,
                    pk,
                    affinityKey,
                    unwrappedCols,
                    wrappedCols,
                    inlineSize,
                    segments,
                    qryCtxRegistry
                );
            }
            else
                return new H2TreeClientIndex(tbl, name, pk, unwrappedCols);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocalText(String schemaName,
        String cacheName, String qry, String typeName, IndexingQueryFilter filters) throws IgniteCheckedException {
        H2TableDescriptor tbl = schemaMgr.tableForType(schemaName, cacheName, typeName);

        if (tbl != null && tbl.luceneIndex() != null) {
            Long qryId = runningQueryManager().register(qry, TEXT, schemaName, true, null);

            try {
                return tbl.luceneIndex().query(qry.toUpperCase(), filters);
            }
            finally {
                runningQueryManager().unregister(qryId, false);
            }
        }

        return new GridEmptyCloseableIterator<>();
    }

    /**
     * Queries individual fields (generally used by JDBC drivers).
     *
     * @param schemaName Schema name.
     * @param qry Query.
     * @param params Query parameters.
     * @param filter Cache name and key filter.
     * @param enforceJoinOrder Enforce join order of tables in the query.
     * @param startTx Start transaction flag.
     * @param qryTimeout Query timeout in milliseconds.
     * @param cancel Query cancel.
     * @param mvccTracker Query tracker.
     * @param dataPageScanEnabled If data page scan is enabled.
     * @return Query result.
     * @throws IgniteCheckedException If failed.
     */
    private GridQueryFieldsResult executeQueryLocal0(
        final String schemaName,
        String qry,
        @Nullable final Collection<Object> params,
        @Nullable List<GridQueryFieldMetadata> meta,
        final IndexingQueryFilter filter,
        boolean enforceJoinOrder,
        boolean startTx,
        int qryTimeout,
        final GridQueryCancel cancel,
        MvccQueryTracker mvccTracker,
        Boolean dataPageScanEnabled
    ) throws IgniteCheckedException {
        GridNearTxLocal tx = null;

        boolean mvccEnabled = mvccEnabled(kernalContext());

        assert mvccEnabled || mvccTracker == null;

        try {
            final Connection conn = connMgr.connectionForThread().connection(schemaName);

            H2Utils.setupConnection(conn, false, enforceJoinOrder);

            PreparedStatement stmt = preparedStatementWithParams(conn, qry, params, true);

            if (GridSqlQueryParser.checkMultipleStatements(stmt))
                throw new IgniteSQLException("Multiple statements queries are not supported for local queries");

            Prepared p = GridSqlQueryParser.prepared(stmt);

            if (GridSqlQueryParser.isDml(p)) {
                SqlFieldsQuery fldsQry = new SqlFieldsQuery(qry);

                if (params != null)
                    fldsQry.setArgs(params.toArray());

                fldsQry.setEnforceJoinOrder(enforceJoinOrder);
                fldsQry.setTimeout(qryTimeout, TimeUnit.MILLISECONDS);
                fldsQry.setDataPageScanEnabled(dataPageScanEnabled);

                UpdateResult updRes = executeUpdate(schemaName, conn, p, fldsQry, true, filter, cancel);

                List<?> updResRow = Collections.singletonList(updRes.counter());

                return new GridQueryFieldsResultAdapter(UPDATE_RESULT_META, new IgniteSingletonIterator<>(updResRow));
            }
            else if (CommandProcessor.isCommand(p)) {
                throw new IgniteSQLException("DDL statements are supported for the whole cluster only.",
                    IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
            }

            MvccSnapshot mvccSnapshot = null;

            boolean forUpdate = GridSqlQueryParser.isForUpdateQuery(p);

            if (forUpdate && !mvccEnabled)
                throw new IgniteSQLException("SELECT FOR UPDATE query requires transactional " +
                    "cache with MVCC enabled.", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

            if (ctx.security().enabled()) {
                GridSqlQueryParser parser = new GridSqlQueryParser(false);

                parser.parse(p);

                checkSecurity(parser.cacheIds());
            }

            GridNearTxSelectForUpdateFuture sfuFut = null;

            int opTimeout = qryTimeout;

            if (mvccEnabled) {
                if (mvccTracker == null)
                    mvccTracker = mvccTracker(stmt, startTx);

                if (mvccTracker != null) {
                    mvccSnapshot = mvccTracker.snapshot();

                    tx = checkActive(tx(ctx));

                    opTimeout = operationTimeout(opTimeout, tx);
                }

                if (forUpdate) {
                    if (mvccTracker == null)
                        throw new IgniteSQLException("SELECT FOR UPDATE query requires transactional " +
                            "cache with MVCC enabled.", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

                    GridSqlStatement stmt0 = new GridSqlQueryParser(false).parse(p);

                    qry = GridSqlQueryParser.rewriteQueryForUpdateIfNeeded(stmt0, forUpdate = tx != null);

                    stmt = preparedStatementWithParams(conn, qry, params, true);

                    if (forUpdate) {
                        GridCacheContext cctx = mvccTracker.context();

                        try {
                            if (tx.topologyVersionSnapshot() == null)
                                new TxTopologyVersionFuture(tx, cctx).get();
                        }
                        catch (Exception e) {
                            throw new IgniteSQLException("Failed to lock topology for SELECT FOR UPDATE query.", e);
                        }

                        sfuFut = new GridNearTxSelectForUpdateFuture(cctx, tx, opTimeout);

                        sfuFut.initLocal();
                    }
                }
            }

            GridNearTxLocal tx0 = tx;
            MvccQueryTracker mvccTracker0 = mvccTracker;
            GridNearTxSelectForUpdateFuture sfuFut0 = sfuFut;
            PreparedStatement stmt0 = stmt;
            String qry0 = qry;
            int timeout0 = opTimeout;

            final QueryContext qctx = new QueryContext(
                0,
                filter,
                null,
                mvccSnapshot,
                null
            );

            return new GridQueryFieldsResultAdapter(meta, null) {
                @Override public GridCloseableIterator<List<?>> iterator() throws IgniteCheckedException {
                    assert qryCtxRegistry.getThreadLocal() == null;

                    qryCtxRegistry.setThreadLocal(qctx);

                    ThreadLocalObjectPool<H2ConnectionWrapper>.Reusable detachedConn = connMgr.detachThreadConnection();

                    try {
                        ResultSet rs = executeSqlQueryWithTimer(stmt0, conn, qry0, params, timeout0, cancel, dataPageScanEnabled);

                        if (sfuFut0 != null) {
                            assert tx0.mvccSnapshot() != null;

                            ResultSetEnlistFuture enlistFut = ResultSetEnlistFuture.future(
                                ctx.localNodeId(),
                                tx0.nearXidVersion(),
                                tx0.mvccSnapshot(),
                                tx0.threadId(),
                                IgniteUuid.randomUuid(),
                                -1,
                                null,
                                tx0,
                                timeout0,
                                sfuFut0.cache(),
                                rs
                            );

                            enlistFut.listen(new IgniteInClosure<IgniteInternalFuture<Long>>() {
                                @Override public void apply(IgniteInternalFuture<Long> fut) {
                                    if (fut.error() != null) {
                                        sfuFut0.onResult(
                                            ctx.localNodeId(),
                                            0L,
                                            false,
                                            fut.error());
                                    }
                                    else {
                                        sfuFut0.onResult(
                                            ctx.localNodeId(),
                                            fut.result(),
                                            false,
                                            null);
                                    }
                                }
                            });

                            enlistFut.init();

                            try {
                                sfuFut0.get();

                                rs.beforeFirst();
                            }
                            catch (Exception e) {
                                U.closeQuiet(rs);

                                throw new IgniteSQLException("Failed to obtain locks on result of SELECT FOR UPDATE.",
                                    e);
                            }
                        }

                        return new H2FieldsIterator(rs, mvccTracker0, sfuFut0 != null, detachedConn);
                    }
                    catch (IgniteCheckedException | RuntimeException | Error e) {
                        detachedConn.recycle();

                        try {
                            if (mvccTracker0 != null)
                                mvccTracker0.onDone();
                        }
                        catch (Exception e0) {
                            e.addSuppressed(e0);
                        }

                        throw e;
                    }
                    finally {
                        qryCtxRegistry.clearThreadLocal();
                    }
                }
            };
        }
        catch (IgniteCheckedException | RuntimeException | Error e) {
            if (mvccEnabled && (tx != null || (tx = tx(ctx)) != null))
                tx.setRollbackOnly();

            throw e;
        }
    }

    /**
     * @param qryTimeout Query timeout in milliseconds.
     * @param tx Transaction.
     * @return Timeout for operation in milliseconds based on query and tx timeouts.
     */
    public static int operationTimeout(int qryTimeout, IgniteTxAdapter tx) {
        if (tx != null) {
            int remaining = (int)tx.remainingTime();

            return remaining > 0 && qryTimeout > 0 ? Math.min(remaining, qryTimeout) : Math.max(remaining, qryTimeout);
        }

        return qryTimeout;
    }

    /** {@inheritDoc} */
    @Override public long streamUpdateQuery(String schemaName, String qry,
        @Nullable Object[] params, IgniteDataStreamer<?, ?> streamer) throws IgniteCheckedException {
        final Connection conn = connMgr.connectionForThread().connection(schemaName);

        final PreparedStatement stmt;

        try {
            stmt = connMgr.prepareStatement(conn, qry);
        }
        catch (SQLException e) {
            throw new IgniteSQLException(e);
        }

        return streamQuery0(qry, schemaName, streamer, stmt, params);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override public List<Long> streamBatchedUpdateQuery(String schemaName, String qry, List<Object[]> params,
        SqlClientContext cliCtx) throws IgniteCheckedException {
        if (cliCtx == null || !cliCtx.isStream()) {
            U.warn(log, "Connection is not in streaming mode.");

            return zeroBatchedStreamedUpdateResult(params.size());
        }

        final Connection conn = connMgr.connectionForThread().connection(schemaName);

        final PreparedStatement stmt = prepareStatementAndCaches(conn, qry);

        if (GridSqlQueryParser.checkMultipleStatements(stmt))
            throw new IgniteSQLException("Multiple statements queries are not supported for streaming mode.",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        checkStatementStreamable(stmt);

        Prepared p = GridSqlQueryParser.prepared(stmt);

        UpdatePlan plan = updatePlan(schemaName, conn, p, null);

        IgniteDataStreamer<?, ?> streamer = cliCtx.streamerForCache(plan.cacheContext().name());

        assert streamer != null;

        List<Long> res = new ArrayList<>(params.size());

        for (int i = 0; i < params.size(); i++)
            res.add(streamQuery0(qry, schemaName, streamer, stmt, params.get(i)));

        return res;
    }

    /**
     * Perform given statement against given data streamer. Only rows based INSERT is supported.
     *
     * @param qry Query.
     * @param schemaName Schema name.
     * @param streamer Streamer to feed data to.
     * @param stmt Statement.
     * @param args Statement arguments.
     * @return Number of rows in given INSERT statement.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"unchecked", "Anonymous2MethodRef"})
    private long streamQuery0(String qry, String schemaName, IgniteDataStreamer streamer, PreparedStatement stmt,
        final Object[] args) throws IgniteCheckedException {
        Long qryId = runningQryMgr.register(qry, GridCacheQueryType.SQL_FIELDS, schemaName, true, null);

        boolean fail = false;

        try {
            checkStatementStreamable(stmt);

            Prepared p = GridSqlQueryParser.prepared(stmt);

            assert p != null;

            final UpdatePlan plan = updatePlan(schemaName, null, p, null);

            assert plan.isLocalSubquery();

            final GridCacheContext cctx = plan.cacheContext();

            QueryCursorImpl<List<?>> cur;

            final ArrayList<List<?>> data = new ArrayList<>(plan.rowCount());

            QueryCursorImpl<List<?>> stepCur = new QueryCursorImpl<>(new Iterable<List<?>>() {
                @Override public Iterator<List<?>> iterator() {
                    try {
                        assert F.isEmpty(plan.selectQuery());

                        Object[] params = args != null ? args : X.EMPTY_OBJECT_ARRAY;

                        Iterator<List<?>> it = plan.createRows(params).iterator();

                        return new GridQueryCacheObjectsIterator(it, objectContext(), cctx.keepBinary());
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
            }, null);

            data.addAll(stepCur.getAll());

            cur = new QueryCursorImpl<>(new Iterable<List<?>>() {
                @Override public Iterator<List<?>> iterator() {
                    return data.iterator();
                }
            }, null);

            if (plan.rowCount() == 1) {
                IgniteBiTuple t = plan.processRow(cur.iterator().next());

                streamer.addData(t.getKey(), t.getValue());

                return 1;
            }

            Map<Object, Object> rows = new LinkedHashMap<>(plan.rowCount());

            for (List<?> row : cur) {
                final IgniteBiTuple t = plan.processRow(row);

                rows.put(t.getKey(), t.getValue());
            }

            streamer.addData(rows);

            return rows.size();
        }
        catch (IgniteCheckedException e) {
            fail = true;

            throw e;
        }
        finally {
            runningQryMgr.unregister(qryId, fail);
        }
    }

    /**
     * @param size Result size.
     * @return List of given size filled with 0Ls.
     */
    private static List<Long> zeroBatchedStreamedUpdateResult(int size) {
        Long[] res = new Long[size];

        Arrays.fill(res, 0L);

        return Arrays.asList(res);
    }

    /**
     * Prepares sql statement.
     *
     * @param conn Connection.
     * @param sql Sql.
     * @param params Params.
     * @param useStmtCache If {@code true} use stmt cache.
     * @return Prepared statement with set parameters.
     * @throws IgniteCheckedException If failed.
     */
    private PreparedStatement preparedStatementWithParams(Connection conn, String sql, Collection<Object> params,
        boolean useStmtCache) throws IgniteCheckedException {
        final PreparedStatement stmt;

        try {
            stmt = useStmtCache ? connMgr.prepareStatement(conn, sql) : connMgr.prepareStatementNoCache(conn, sql);
        }
        catch (SQLException e) {
            throw new IgniteCheckedException("Failed to parse SQL query: " + sql, e);
        }

        H2Utils.bindParameters(stmt, params);

        return stmt;
    }

    /**
     * Executes sql query statement.
     *
     * @param conn Connection,.
     * @param stmt Statement.
     * @param timeoutMillis Query timeout.
     * @param cancel Query cancel.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    private ResultSet executeSqlQuery(final Connection conn, final PreparedStatement stmt,
        int timeoutMillis, @Nullable GridQueryCancel cancel) throws IgniteCheckedException {
        final MapQueryLazyWorker lazyWorker = MapQueryLazyWorker.currentWorker();

        if (cancel != null) {
            cancel.set(new Runnable() {
                @Override public void run() {
                    if (lazyWorker != null) {
                        lazyWorker.submit(new Runnable() {
                            @Override public void run() {
                                cancelStatement(stmt);
                            }
                        });
                    }
                    else
                        cancelStatement(stmt);
                }
            });
        }

        Session ses = H2Utils.session(conn);

        if (timeoutMillis > 0)
            ses.setQueryTimeout(timeoutMillis);

        if (lazyWorker != null)
            ses.setLazyQueryExecution(true);

        try {
            return stmt.executeQuery();
        }
        catch (SQLException e) {
            // Throw special exception.
            if (e.getErrorCode() == ErrorCode.STATEMENT_WAS_CANCELED)
                throw new QueryCancelledException();

            throw new IgniteCheckedException("Failed to execute SQL query. " + e.getMessage(), e);
        }
        finally {
            if (timeoutMillis > 0)
                ses.setQueryTimeout(0);

            if (lazyWorker != null)
                ses.setLazyQueryExecution(false);
        }
    }

    /**
     * Cancel prepared statement.
     *
     * @param stmt Statement.
     */
    private static void cancelStatement(PreparedStatement stmt) {
        try {
            stmt.cancel();
        }
        catch (SQLException ignored) {
            // No-op.
        }
    }

    /**
     * Executes sql query and prints warning if query is too slow..
     *
     * @param conn Connection,
     * @param sql Sql query.
     * @param params Parameters.
     * @param timeoutMillis Query timeout.
     * @param cancel Query cancel.
     * @param dataPageScanEnabled If data page scan is enabled.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    public ResultSet executeSqlQueryWithTimer(
        Connection conn,
        String sql,
        @Nullable Collection<Object> params,
        int timeoutMillis,
        @Nullable GridQueryCancel cancel,
        Boolean dataPageScanEnabled
    ) throws IgniteCheckedException {
        return executeSqlQueryWithTimer(preparedStatementWithParams(conn, sql, params, false),
            conn, sql, params, timeoutMillis, cancel, dataPageScanEnabled);
    }

    /**
     * @param dataPageScanEnabled If data page scan is enabled.
     */
    public void enableDataPageScan(Boolean dataPageScanEnabled) {
        // Data page scan is enabled by default for SQL.
        CacheDataTree.setDataPageScanEnabled(dataPageScanEnabled != FALSE);
    }

    /**
     * Executes sql query and prints warning if query is too slow.
     *
     * @param stmt Prepared statement for query.
     * @param conn Connection.
     * @param sql Sql query.
     * @param params Parameters.
     * @param timeoutMillis Query timeout.
     * @param cancel Query cancel.
     * @param dataPageScanEnabled If data page scan is enabled.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    public ResultSet executeSqlQueryWithTimer(
        PreparedStatement stmt,
        Connection conn,
        String sql,
        @Nullable Collection<Object> params,
        int timeoutMillis,
        @Nullable GridQueryCancel cancel,
        Boolean dataPageScanEnabled
    ) throws IgniteCheckedException {
        long start = U.currentTimeMillis();

        enableDataPageScan(dataPageScanEnabled);

        try {
            ResultSet rs = executeSqlQuery(conn, stmt, timeoutMillis, cancel);

            long time = U.currentTimeMillis() - start;

            long longQryExecTimeout = ctx.config().getLongQueryWarningTimeout();

            if (time > longQryExecTimeout) {
                ResultSet plan = executeSqlQuery(conn, preparedStatementWithParams(conn, "EXPLAIN " + sql,
                    params, false), 0, null);

                plan.next();

                // Add SQL explain result message into log.
                String msg = "Query execution is too long [time=" + time + " ms, sql='" + sql + '\'' +
                    ", plan=" + U.nl() + plan.getString(1) + U.nl() + ", parameters=" +
                    (params == null ? "[]" : Arrays.deepToString(params.toArray())) + "]";

                LT.warn(log, msg);
            }

            return rs;
        }
        catch (SQLException e) {
            connMgr.onSqlException(conn);

            throw new IgniteCheckedException(e);
        }
        finally {
            CacheDataTree.setDataPageScanEnabled(false);
        }
    }

    /**
     * Queries individual fields (generally used by JDBC drivers).
     *
     * @param schemaName Schema name.
     * @param qry Query.
     * @param keepBinary Keep binary flag.
     * @param filter Cache name and key filter.
     * @param cancel Query cancel.
     * @param qryId Running query id. {@code null} in case query is not registered.
     * @return Cursor.
     */
    private FieldsQueryCursor<List<?>> executeQueryLocal(
        String schemaName,
        SqlFieldsQuery qry,
        List<GridQueryFieldMetadata> meta,
        final boolean keepBinary,
        IndexingQueryFilter filter,
        GridQueryCancel cancel,
        Long qryId
    ) throws IgniteCheckedException {
        boolean startTx = autoStartTx(qry);

        final GridQueryFieldsResult res = executeQueryLocal0(
            schemaName,
            qry.getSql(),
            F.asList(qry.getArgs()),
            meta,
            filter,
            qry.isEnforceJoinOrder(),
            startTx,
            qry.getTimeout(),
            cancel,
            null,
            qry.isDataPageScanEnabled()
        );

        Iterable<List<?>> iter = () -> {
            try {
                return new GridQueryCacheObjectsIterator(res.iterator(), objectContext(), keepBinary);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        };

        QueryCursorImpl<List<?>> cursor = qryId != null
            ? new RegisteredQueryCursor<>(iter, cancel, runningQueryManager(), qryId)
            : new QueryCursorImpl<>(iter, cancel);

        cursor.fieldsMeta(res.metaData());

        return cursor;
    }

    /**
     * Initialises MVCC filter and returns MVCC query tracker if needed.
     * @param stmt Prepared statement.
     * @param startTx Start transaction flag.
     * @return MVCC query tracker or {@code null} if MVCC is disabled for involved caches.
     */
    private MvccQueryTracker mvccTracker(PreparedStatement stmt, boolean startTx) throws IgniteCheckedException {
        boolean mvccEnabled;

        GridCacheContext mvccCacheCtx = null;

        try {
            if (stmt.isWrapperFor(PreparedStatementEx.class)) {
                PreparedStatementEx stmtEx = stmt.unwrap(PreparedStatementEx.class);

                Boolean mvccState = stmtEx.meta(MVCC_STATE);

                mvccEnabled = mvccState != null ? mvccState : checkMvcc(stmt);

                if (mvccEnabled) {
                    Integer cacheId = stmtEx.meta(MVCC_CACHE_ID);

                    assert cacheId != null;

                    mvccCacheCtx = ctx.cache().context().cacheContext(cacheId);

                    assert mvccCacheCtx != null;
                }
            }
            else
                mvccEnabled = checkMvcc(stmt);
        }
        catch (SQLException e) {
            throw new IgniteSQLException(e);
        }

        assert !mvccEnabled || mvccCacheCtx != null;

        return mvccEnabled ? MvccUtils.mvccTracker(mvccCacheCtx, startTx) : null;
    }

    /**
     * Checks if statement uses MVCC caches. If it does, additional metadata is added to statement.
     *
     * @param stmt Statement to check.
     * @return {@code True} if there MVCC cache involved in statement.
     * @throws SQLException If parser failed.
     */
    private static Boolean checkMvcc(PreparedStatement stmt) throws SQLException {
        GridSqlQueryParser parser = new GridSqlQueryParser(false);

        parser.parse(GridSqlQueryParser.prepared(stmt));

        Boolean mvccEnabled = null;
        Integer mvccCacheId = null;
        GridCacheContext ctx0 = null;

        for (Object o : parser.objectsMap().values()) {
            if (o instanceof GridSqlAlias)
                o = GridSqlAlias.unwrap((GridSqlAst) o);
            if (o instanceof GridSqlTable && ((GridSqlTable) o).dataTable() != null) {
                GridCacheContext cctx = ((GridSqlTable)o).dataTable().cacheContext();

                assert cctx != null;

                if (mvccEnabled == null) {
                    mvccEnabled = cctx.mvccEnabled();
                    mvccCacheId = cctx.cacheId();
                    ctx0 = cctx;
                }
                else if (mvccEnabled != cctx.mvccEnabled())
                    MvccUtils.throwAtomicityModesMismatchException(ctx0, cctx);
            }
        }

        if (mvccEnabled == null)
            return false;

        // Remember mvccEnabled flag to avoid further additional parsing if statement obtained from the statement cache.
        if (stmt.isWrapperFor(PreparedStatementEx.class)) {
            PreparedStatementEx stmtEx = stmt.unwrap(PreparedStatementEx.class);

            if (mvccEnabled) {
                stmtEx.putMeta(MVCC_CACHE_ID, mvccCacheId);
                stmtEx.putMeta(MVCC_STATE, Boolean.TRUE);
            }
            else
                stmtEx.putMeta(MVCC_STATE, FALSE);
        }

        return mvccEnabled;
    }

    /**
     * @param schemaName Schema name.
     * @param qry Query.
     * @param keepCacheObj Flag to keep cache object.
     * @param enforceJoinOrder Enforce join order of tables.
     * @param startTx Start transaction flag.
     * @param qryTimeout Query timeout.
     * @param cancel Cancel object.
     * @param params Query parameters.
     * @param parts Partitions.
     * @param lazy Lazy query execution flag.
     * @param mvccTracker Query tracker.
     * @param dataPageScanEnabled If data page scan is enabled.
     * @param pageSize Page size.
     * @return Iterable result.
     */
    private Iterable<List<?>> runQueryTwoStep(
        final String schemaName,
        final GridCacheTwoStepQuery qry,
        final boolean keepCacheObj,
        final boolean enforceJoinOrder,
        boolean startTx,
        final int qryTimeout,
        final GridQueryCancel cancel,
        final Object[] params,
        final int[] parts,
        final boolean lazy,
        MvccQueryTracker mvccTracker,
        Boolean dataPageScanEnabled,
        int pageSize
    ) {
        assert !qry.mvccEnabled() || !F.isEmpty(qry.cacheIds());

        try {
            final MvccQueryTracker tracker = mvccTracker == null && qry.mvccEnabled() ?
                MvccUtils.mvccTracker(ctx.cache().context().cacheContext(qry.cacheIds().get(0)), startTx) : mvccTracker;

            GridNearTxLocal tx = tracker != null ? tx(ctx) : null;

            // Locking has no meaning if SELECT FOR UPDATE is not executed in explicit transaction.
            // So, we can can reset forUpdate flag if there is no explicit transaction.
            boolean forUpdate = qry.forUpdate() && checkActive(tx) != null;

            int opTimeout = operationTimeout(qryTimeout, tx);

            return new Iterable<List<?>>() {
                @SuppressWarnings("NullableProblems")
                @Override public Iterator<List<?>> iterator() {
                    try {
                        return rdcQryExec.query(
                            schemaName,
                            qry,
                            keepCacheObj,
                            enforceJoinOrder,
                            opTimeout,
                            cancel,
                            params,
                            parts,
                            lazy,
                            tracker,
                            dataPageScanEnabled,
                            forUpdate,
                            pageSize
                        );
                    }
                    catch (Throwable e) {
                        if (tracker != null)
                            tracker.onDone();

                        throw e;
                    }
                }
            };
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
    }

    /**
     * Run DML on remote nodes.
     *
     * @param schemaName Schema name.
     * @param fieldsQry Initial update query.
     * @param cacheIds Cache identifiers.
     * @param isReplicatedOnly Whether query uses only replicated caches.
     * @param cancel Cancel state.
     * @return Update result.
     */
    UpdateResult runDistributedUpdate(
        String schemaName,
        SqlFieldsQuery fieldsQry,
        List<Integer> cacheIds,
        boolean isReplicatedOnly,
        GridQueryCancel cancel) {
        return rdcQryExec.update(schemaName, cacheIds, fieldsQry.getSql(), fieldsQry.getArgs(),
            fieldsQry.isEnforceJoinOrder(), fieldsQry.getPageSize(), fieldsQry.getTimeout(),
            fieldsQry.getPartitions(), isReplicatedOnly, cancel);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public SqlFieldsQuery generateFieldsQuery(String cacheName, SqlQuery qry) {
        String schemaName = schema(cacheName);

        String type = qry.getType();

        H2TableDescriptor tblDesc = schemaMgr.tableForType(schemaName, cacheName, type);

        if (tblDesc == null)
            throw new IgniteSQLException("Failed to find SQL table for type: " + type,
                IgniteQueryErrorCode.TABLE_NOT_FOUND);

        String sql;

        try {
            sql = H2Utils.generateFieldsQueryString(qry.getSql(), qry.getAlias(), tblDesc);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        SqlFieldsQuery res = new SqlFieldsQuery(sql);

        res.setArgs(qry.getArgs());
        res.setDistributedJoins(qry.isDistributedJoins());
        res.setLocal(qry.isLocal());
        res.setPageSize(qry.getPageSize());
        res.setPartitions(qry.getPartitions());
        res.setReplicatedOnly(qry.isReplicatedOnly());
        res.setSchema(schemaName);
        res.setSql(sql);
        res.setDataPageScanEnabled(qry.isDataPageScanEnabled());

        if (qry.getTimeout() > 0)
            res.setTimeout(qry.getTimeout(), TimeUnit.MILLISECONDS);

        return res;
    }

    /**
     * Execute command.
     *
     * @param schemaName Schema name.
     * @param qry Query.
     * @param cliCtx CLient context.
     * @param cmd Command (native).
     * @return Result.
     */
    public FieldsQueryCursor<List<?>> executeCommand(
        String schemaName,
        SqlFieldsQuery qry,
        @Nullable SqlClientContext cliCtx,
        QueryParserResultCommand cmd
    ) {
        if (cmd.noOp())
            return H2Utils.zeroCursor();

        SqlCommand cmdNative = cmd.commandNative();
        GridSqlStatement cmdH2 = cmd.commandH2();

        if (qry.isLocal()) {
            throw new IgniteSQLException("DDL statements are not supported for LOCAL caches",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        Long qryId = registerRunningQuery(schemaName, null, qry.getSql(), qry.isLocal(), true);

        boolean fail = false;

        CommandResult res = null;

        try {
            res = cmdProc.runCommand(qry, cmdNative, cmdH2, cliCtx, qryId);

            return res.cursor();
        }
        catch (IgniteCheckedException e) {
            fail = true;

            throw new IgniteSQLException("Failed to execute DDL statement [stmt=" + qry.getSql() +
                ", err=" + e.getMessage() + ']', e);
        }
        finally {
            if (fail || (res != null && res.unregisterRunningQuery()))
                runningQryMgr.unregister(qryId, fail);
        }
    }

    /**
     * Check whether command could be executed with the given cluster state.
     *
     * @param parseRes Parsing result.
     */
    private void checkClusterState(QueryParserResult parseRes) {
        if (!ctx.state().publicApiActiveState(true)) {
            if (parseRes.isCommand()) {
                SqlCommand cmd = parseRes.command().commandNative();

                if (cmd instanceof SqlCommitTransactionCommand || cmd instanceof SqlRollbackTransactionCommand)
                    return;
            }

            throw new IgniteException("Can not perform the operation because the cluster is inactive. Note, " +
                "that the cluster is considered inactive by default if Ignite Persistent Store is used to " +
                "let all the nodes join the cluster. To activate the cluster call Ignite.active(true).");
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"StringEquality", "unchecked"})
    @Override public List<FieldsQueryCursor<List<?>>> querySqlFields(String schemaName, SqlFieldsQuery qry,
        @Nullable SqlClientContext cliCtx, boolean keepBinary, boolean failOnMultipleStmts, MvccQueryTracker tracker,
        GridQueryCancel cancel, boolean registerAsNewQry) {
        boolean mvccEnabled = mvccEnabled(ctx), startTx = autoStartTx(qry);

        try {
            List<FieldsQueryCursor<List<?>>> res = new ArrayList<>(1);

            SqlFieldsQuery remainingQry = qry;

            while (remainingQry != null) {
                QueryParserResult parseRes = parser.parse(schemaName, remainingQry);

                remainingQry = parseRes.remainingQuery();

                if (remainingQry != null && failOnMultipleStmts)
                    throw new IgniteSQLException("Multiple statements queries are not supported");

                SqlFieldsQuery newQry = parseRes.query();

                assert newQry.getSql() != null;

                checkClusterState(parseRes);

                if (parseRes.isCommand()) {
                    // Execute command.
                    FieldsQueryCursor<List<?>> cmdRes = executeCommand(
                        schemaName,
                        newQry,
                        cliCtx,
                        parseRes.command()
                    );

                    res.add(cmdRes);
                }
                else {
                    // Execute query or DML.
                    List<? extends FieldsQueryCursor<List<?>>> qryRes = executeSelectOrDml(
                        schemaName,
                        newQry,
                        parseRes.select(),
                        parseRes.dml(),
                        keepBinary,
                        startTx,
                        tracker,
                        cancel,
                        registerAsNewQry
                    );

                    res.addAll(qryRes);
                }
            }

            return res;
        }
        catch (RuntimeException | Error e) {
            GridNearTxLocal tx;

            if (mvccEnabled && (tx = tx(ctx)) != null &&
                (!(e instanceof IgniteSQLException) || /* Parsing errors should not rollback Tx. */
                    ((IgniteSQLException)e).sqlState() != SqlStateCode.PARSING_EXCEPTION)) {

                tx.setRollbackOnly();
            }

            throw e;
        }
    }

    /**
     * Execute an all-ready {@link SqlFieldsQuery}.
     * @param schemaName Schema name.
     * @param qry Fields query with flags.
     * @param select Select.
     * @param dml DML.
     * @param keepBinary Whether binary objects must not be deserialized automatically.
     * @param startTx Start transaction flag.
     * @param mvccTracker MVCC tracker.
     * @param cancel Query cancel state holder.
     * @param registerAsNewQry {@code true} In case it's new query which should be registered as running query,
     * @return Query result.
     */
    private List<? extends FieldsQueryCursor<List<?>>> executeSelectOrDml(
        String schemaName,
        SqlFieldsQuery qry,
        @Nullable QueryParserResultSelect select,
        @Nullable QueryParserResultDml dml,
        boolean keepBinary,
        boolean startTx,
        MvccQueryTracker mvccTracker,
        GridQueryCancel cancel,
        boolean registerAsNewQry
    ) {
        String sqlQry = qry.getSql();

        boolean loc = qry.isLocal();

        IndexingQueryFilter filter = (loc ? backupFilter(null, qry.getPartitions()) : null);

        if (dml != null) {
            Prepared prepared = dml.prepared();

            Long qryId = registerRunningQuery(schemaName, cancel, sqlQry, loc, registerAsNewQry);

            boolean fail = false;

            try {
                if (GridSqlQueryParser.isDml(prepared)) {
                    try {
                        Connection conn = connMgr.connectionForThread().connection(schemaName);

                        if (!loc)
                            return executeUpdateDistributed(schemaName, conn, prepared, qry, cancel);
                        else {
                            UpdateResult updRes = executeUpdate(schemaName, conn, prepared, qry, true, filter, cancel);

                            return Collections.singletonList(new QueryCursorImpl<>(new Iterable<List<?>>() {
                                @SuppressWarnings("NullableProblems")
                                @Override public Iterator<List<?>> iterator() {
                                    return new IgniteSingletonIterator<>(Collections.singletonList(updRes.counter()));
                                }
                            }, cancel));
                        }
                    }
                    catch (IgniteCheckedException e) {
                        fail = true;

                        throw new IgniteSQLException("Failed to execute DML statement [stmt=" + sqlQry +
                            ", params=" + Arrays.deepToString(qry.getArgs()) + "]", e);
                    }
                }

                fail = true;

                throw new IgniteSQLException("Unsupported DDL/DML operation: " + prepared.getClass().getName(),
                    IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
            }
            finally {
                runningQryMgr.unregister(qryId, fail);
            }
        }

        // Execute SQL.
        assert select != null;

        if (select.splitNeeded()) {
            // Distributed query.
            GridCacheTwoStepQuery twoStepQry = select.twoStepQuery();

            if (ctx.security().enabled())
                checkSecurity(twoStepQry.cacheIds());

            FieldsQueryCursor<List<?>> res = executeQueryWithSplit(
                schemaName,
                qry,
                twoStepQry,
                select.meta(),
                keepBinary,
                startTx,
                mvccTracker,
                cancel,
                registerAsNewQry
            );

            return Collections.singletonList(res);
        }
        else {
            // Local query.
            Long qryId = registerRunningQuery(schemaName, cancel, sqlQry, loc, registerAsNewQry);

            try {
                FieldsQueryCursor<List<?>> res = executeQueryLocal(
                    schemaName,
                    qry,
                    select.meta(),
                    keepBinary,
                    filter,
                    cancel,
                    qryId
                );

                return Collections.singletonList(res);
            }
            catch (IgniteCheckedException e) {
                runningQryMgr.unregister(qryId, true);

                throw new IgniteSQLException("Failed to execute local statement [stmt=" + sqlQry +
                    ", params=" + Arrays.deepToString(qry.getArgs()) + "]", e);
            }
        }
    }

    /**
     * @param schemaName Schema name.
     * @param cancel Query cancel state holder.
     * @param qry Query.
     * @param loc {@code true} for local query.
     * @param registerAsNewQry {@code true} In case it's new query which should be registered as running query,
     * @return Id of registered query or {@code null} if query wasn't registered.
     */
    private Long registerRunningQuery(String schemaName, GridQueryCancel cancel, String qry, boolean loc,
        boolean registerAsNewQry) {
        if (registerAsNewQry)
            return runningQryMgr.register(qry, GridCacheQueryType.SQL_FIELDS, schemaName, loc, cancel);

        return null;
    }

    /**
     * Check security access for caches.
     *
     * @param cacheIds Cache IDs.
     */
    private void checkSecurity(Collection<Integer> cacheIds) {
        if (F.isEmpty(cacheIds))
            return;

        for (Integer cacheId : cacheIds) {
            DynamicCacheDescriptor desc = ctx.cache().cacheDescriptor(cacheId);

            if (desc != null)
                ctx.security().authorize(desc.cacheName(), SecurityPermission.CACHE_READ, null);
        }
    }

    /**
     * @param qry Sql fields query.autoStartTx(qry)
     * @return {@code True} if need to start transaction.
     */
    @SuppressWarnings("SimplifiableIfStatement")
    public boolean autoStartTx(SqlFieldsQuery qry) {
        if (!mvccEnabled(ctx))
            return false;

        return qry instanceof SqlFieldsQueryEx && !((SqlFieldsQueryEx)qry).isAutoCommit() && tx(ctx) == null;
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
        SqlFieldsQuery fldsQry = new SqlFieldsQuery(qry);

        if (params != null)
            fldsQry.setArgs(params);

        fldsQry.setEnforceJoinOrder(U.isFlagSet(flags, GridH2QueryRequest.FLAG_ENFORCE_JOIN_ORDER));
        fldsQry.setTimeout(timeout, TimeUnit.MILLISECONDS);
        fldsQry.setPageSize(pageSize);
        fldsQry.setLocal(true);
        fldsQry.setDataPageScanEnabled(isDataPageScanEnabled(flags));

        boolean locSplit = false;

        final boolean replicated = U.isFlagSet(flags, GridH2QueryRequest.FLAG_REPLICATED);

        GridCacheContext<?, ?> cctx0;

        if (!replicated
            && !F.isEmpty(ids)
            && (cctx0 = CU.firstPartitioned(cctx.shared(), ids)) != null
            && cctx0.config().getQueryParallelism() > 1
        ) {
            locSplit = true;
        }

        Connection conn = connMgr.connectionForThread().connection(schema);

        H2Utils.setupConnection(conn, false, fldsQry.isEnforceJoinOrder());

        PreparedStatement stmt = preparedStatementWithParams(conn, fldsQry.getSql(),
            F.asList(fldsQry.getArgs()), true);

        IndexingQueryFilter filter = backupFilter(topVer, parts);

        Prepared prepared = GridSqlQueryParser.prepared(stmt);

        UpdatePlan plan = updatePlan(schema, conn, prepared, fldsQry);

        GridCacheContext planCctx = plan.cacheContext();

        // Force keepBinary for operation context to avoid binary deserialization inside entry processor
        DmlUtils.setKeepBinaryContext(planCctx);

        QueryCursorImpl<List<?>> cur;

        // Do a two-step query only if locality flag is not set AND if plan's SELECT corresponds to an actual
        // sub-query and not some dummy stuff like "select 1, 2, 3;"
        if (locSplit && !plan.isLocalSubquery()) {
            SqlFieldsQuery newFieldsQry = new SqlFieldsQuery(plan.selectQuery(), fldsQry.isCollocated())
                .setArgs(fldsQry.getArgs())
                .setDistributedJoins(fldsQry.isDistributedJoins())
                .setEnforceJoinOrder(fldsQry.isEnforceJoinOrder())
                .setLocal(fldsQry.isLocal())
                .setPageSize(fldsQry.getPageSize())
                .setTimeout(fldsQry.getTimeout(), TimeUnit.MILLISECONDS)
                .setDataPageScanEnabled(fldsQry.isDataPageScanEnabled());

            cur = (QueryCursorImpl<List<?>>)querySqlFields(
                schema,
                newFieldsQry,
                null,
                true,
                true,
                new StaticMvccQueryTracker(planCctx, mvccSnapshot),
                cancel,
                false
            ).get(0);
        }
        else {
            GridQueryFieldsResult res = executeQueryLocal0(
                schema,
                plan.selectQuery(),
                F.asList(fldsQry.getArgs()),
                null,
                filter,
                fldsQry.isEnforceJoinOrder(),
                false,
                fldsQry.getTimeout(),
                cancel,
                new StaticMvccQueryTracker(planCctx, mvccSnapshot),
                null
            );

            cur = new QueryCursorImpl<>(new Iterable<List<?>>() {
                @Override public Iterator<List<?>> iterator() {
                    try {
                        return res.iterator();
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
            }, cancel);
        }

        return plan.iteratorForTransaction(connMgr, cur);
    }

    /**
     * Run distributed query on detected set of partitions.
     * @param schemaName Schema name.
     * @param qry Original query.
     * @param twoStepQry Two-step query.
     * @param meta Metadata to set to cursor.
     * @param keepBinary Keep binary flag.
     * @param startTx Start transaction flag.
     * @param mvccTracker Query tracker.
     * @param cancel Cancel handler.
     * @param registerAsNewQry {@code true} In case it's new query which should be registered as running query,
     * @return Cursor representing distributed query result.
     */
    private FieldsQueryCursor<List<?>> executeQueryWithSplit(String schemaName, SqlFieldsQuery qry,
        GridCacheTwoStepQuery twoStepQry, List<GridQueryFieldMetadata> meta, boolean keepBinary,
        boolean startTx, MvccQueryTracker mvccTracker, GridQueryCancel cancel, boolean registerAsNewQry) {
        if (log.isDebugEnabled())
            log.debug("Parsed query: `" + qry.getSql() + "` into two step query: " + twoStepQry);

        if (cancel == null)
            cancel = new GridQueryCancel();

        Long qryId = registerRunningQuery(schemaName, cancel, qry.getSql(), qry.isLocal(), registerAsNewQry);

        boolean cursorCreated = false;
        boolean failed = true;

        try {
            // When explicit partitions are set, there must be an owning cache they should be applied to.
            int explicitParts[] = qry.getPartitions();
            PartitionResult derivedParts = twoStepQry.derivedPartitions();

            int parts[] = calculatePartitions(explicitParts, derivedParts, qry.getArgs());

            if (parts != null && parts.length == 0) {
                failed = false;

                return new QueryCursorImpl<>(new Iterable<List<?>>() {
                    @SuppressWarnings("NullableProblems")
                    @Override public Iterator<List<?>> iterator() {
                        return new Iterator<List<?>>() {
                            @Override public boolean hasNext() {
                                return false;
                            }

                            @SuppressWarnings("IteratorNextCanNotThrowNoSuchElementException")
                            @Override public List<?> next() {
                                return null;
                            }
                        };
                    }
                });
            }

            Iterable<List<?>> iter = runQueryTwoStep(
                schemaName,
                twoStepQry,
                keepBinary,
                qry.isEnforceJoinOrder(),
                startTx,
                qry.getTimeout(),
                cancel,
                qry.getArgs(),
                parts,
                qry.isLazy(),
                mvccTracker,
                qry.isDataPageScanEnabled(),
                qry.getPageSize()
            );

            QueryCursorImpl<List<?>> cursor = registerAsNewQry
                ? new RegisteredQueryCursor<>(iter, cancel, runningQueryManager(), qryId)
                : new QueryCursorImpl<>(iter, cancel);

            cursor.fieldsMeta(meta);

            cursorCreated = true;

            return cursor;
        }
        finally {
            if (!cursorCreated)
                runningQryMgr.unregister(qryId, failed);
        }
    }

    /**
     * Calculate partitions for the query.
     *
     * @param explicitParts Explicit partitions provided in SqlFieldsQuery.partitions property.
     * @param derivedParts Derived partitions found during partition pruning.
     * @param args Arguments.
     * @return Calculated partitions or {@code null} if failed to calculate and there should be a broadcast.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    private int[] calculatePartitions(int[] explicitParts, PartitionResult derivedParts, Object[] args) {
        if (!F.isEmpty(explicitParts))
            return explicitParts;
        else if (derivedParts != null) {
            try {
                Collection<Integer> realParts = derivedParts.tree().apply(null, args);

                if (realParts == null)
                    return null;
                else if (realParts.isEmpty())
                    return IgniteUtils.EMPTY_INTS;
                else {
                    int[] realParts0 = new int[realParts.size()];

                    int i = 0;

                    for (Integer realPart : realParts)
                        realParts0[i++] = realPart;

                    return realParts0;
                }
            }
            catch (IgniteCheckedException e) {
                throw new CacheException("Failed to calculate derived partitions for query.", e);
            }
        }

        return null;
    }

    /**
     * Do initial parsing of the statement and create query caches, if needed.
     * @param c Connection.
     * @param sqlQry Query.
     * @return H2 prepared statement.
     */
    private PreparedStatement prepareStatementAndCaches(Connection c, String sqlQry) {
        try {
            return connMgr.prepareStatement(c, sqlQry);
        }
        catch (SQLException e) {
            throw new IgniteSQLException("Failed to parse query. " + e.getMessage(),
                IgniteQueryErrorCode.PARSING, e);
        }
    }

    /**
     * Executes DML request on map node. Happens only for "skip reducer" mode.
     *
     * @param schemaName Schema name.
     * @param qry Query.
     * @param filter Filter.
     * @param cancel Cancel state.
     * @param locSplit Locality flag.
     * @return Update result.
     * @throws IgniteCheckedException if failed.
     */
    public UpdateResult executeUpdateOnDataNode(
        String schemaName,
        SqlFieldsQuery qry,
        IndexingQueryFilter filter,
        GridQueryCancel cancel,
        boolean locSplit
    ) throws IgniteCheckedException {
        Connection conn = connMgr.connectionForThread().connection(schemaName);

        H2Utils.setupConnection(conn, false, qry.isEnforceJoinOrder());

        PreparedStatement stmt = preparedStatementWithParams(conn, qry.getSql(),
            Arrays.asList(qry.getArgs()), true);

        Connection c;

        try {
            c = stmt.getConnection();
        }
        catch (SQLException e) {
            throw new IgniteCheckedException(e);
        }

        return executeUpdate(schemaName, c, GridSqlQueryParser.prepared(stmt), qry, locSplit, filter, cancel);
    }

    /**
     * Registers new class description.
     *
     * This implementation doesn't support type reregistration.
     *
     * @param cacheInfo Cache context info.
     * @param type Type description.
     * @param isSql {@code true} in case table has been created from SQL.
     * @throws IgniteCheckedException In case of error.
     */
    @Override public boolean registerType(GridCacheContextInfo cacheInfo, GridQueryTypeDescriptor type, boolean isSql)
        throws IgniteCheckedException {
        H2Utils.validateTypeDescriptor(type);
        schemaMgr.onCacheTypeCreated(cacheInfo, this, type, isSql);

        return true;
    }

    /** {@inheritDoc} */
    @Override public GridCacheContextInfo registeredCacheInfo(String cacheName) {
        for (H2TableDescriptor tbl : schemaMgr.tablesForCache(cacheName)) {
            if (F.eq(tbl.cacheName(), cacheName))
                return tbl.cacheInfo();
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public String schema(String cacheName) {
        return schemaMgr.schemaName(cacheName);
    }

    /** {@inheritDoc} */
    @Override public void checkStatementStreamable(PreparedStatement nativeStmt) {
        if (!GridSqlQueryParser.isStreamableInsertStatement(nativeStmt))
            throw new IgniteSQLException("Streaming mode supports only INSERT commands without subqueries.",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /** {@inheritDoc} */
    @Override public GridQueryRowCacheCleaner rowCacheCleaner(int grpId) {
        return rowCache.forGroup(grpId);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> rebuildIndexesFromHash(GridCacheContext cctx) {
        // No data in fresh in-memory cache.
        if (!cctx.group().persistenceEnabled())
            return null;

        IgnitePageStoreManager pageStore = cctx.shared().pageStore();

        assert pageStore != null;

        SchemaIndexCacheVisitorClosure clo;

        if (!pageStore.hasIndexStore(cctx.groupId())) {
            // If there are no index store, rebuild all indexes.
            clo = new IndexRebuildFullClosure(cctx.queries(), cctx.mvccEnabled());
        }
        else {
            // Otherwise iterate over tables looking for missing indexes.
            IndexRebuildPartialClosure clo0 = new IndexRebuildPartialClosure();

            for (H2TableDescriptor tblDesc : schemaMgr.tablesForCache(cctx.name())) {
                assert tblDesc.table() != null;

                tblDesc.table().collectIndexesForPartialRebuild(clo0);
            }

            if (clo0.hasIndexes())
                clo = clo0;
            else
                return null;
        }

        // Closure prepared, do rebuild.
        final GridWorkerFuture<?> fut = new GridWorkerFuture<>();

        markIndexRebuild(cctx.name(), true);

        GridWorker worker = new GridWorker(ctx.igniteInstanceName(), "index-rebuild-worker-" + cctx.name(), log) {
            @Override protected void body() {
                try {
                    rebuildIndexesFromHash0(cctx, clo);

                    markIndexRebuild(cctx.name(), false);

                    fut.onDone();
                }
                catch (Exception e) {
                    fut.onDone(e);
                }
                catch (Throwable e) {
                    U.error(log, "Failed to rebuild indexes for cache: " + cctx.name(), e);

                    fut.onDone(e);

                    throw e;
                }
            }
        };

        fut.setWorker(worker);

        ctx.getExecutorService().execute(worker);

        return fut;
    }

    /**
     * Do index rebuild.
     *
     * @param cctx Cache context.
     * @param clo Closure.
     * @throws IgniteCheckedException If failed.
     */
    protected void rebuildIndexesFromHash0(GridCacheContext cctx, SchemaIndexCacheVisitorClosure clo)
        throws IgniteCheckedException {
        SchemaIndexCacheVisitor visitor = new SchemaIndexCacheVisitorImpl(cctx);

        visitor.visit(clo);
    }

    /**
     * Mark tables for index rebuild, so that their indexes are not used.
     *
     * @param cacheName Cache name.
     * @param val Value.
     */
    private void markIndexRebuild(String cacheName, boolean val) {
        for (H2TableDescriptor tblDesc : schemaMgr.tablesForCache(cacheName)) {
            assert tblDesc.table() != null;

            tblDesc.table().markRebuildFromHashInProgress(val);
        }
    }

    /**
     * @return Busy lock.
     */
    public GridSpinBusyLock busyLock() {
        return busyLock;
    }

    /**
     * @return Map query executor.
     */
    public GridMapQueryExecutor mapQueryExecutor() {
        return mapQryExec;
    }

    /**
     * @return Reduce query executor.
     */
    public GridReduceQueryExecutor reduceQueryExecutor() {
        return rdcQryExec;
    }

    /**
     * Return Running query manager.
     *
     * @return Running query manager.
     */
    public RunningQueryManager runningQueryManager() {
        return runningQryMgr;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"deprecation"})
    @Override public void start(GridKernalContext ctx, GridSpinBusyLock busyLock) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Starting cache query index...");

        this.busyLock = busyLock;

        if (SysProperties.serializeJavaObject) {
            U.warn(log, "Serialization of Java objects in H2 was enabled.");

            SysProperties.serializeJavaObject = false;
        }

        this.ctx = ctx;

        partReservationMgr = new PartitionReservationManager(ctx);

        connMgr = new ConnectionManager(ctx);

        parser = new QueryParser(this, connMgr);

        schemaMgr = new SchemaManager(ctx, connMgr);
        schemaMgr.start(ctx.config().getSqlSchemas());

        valCtx = new CacheQueryObjectValueContext(ctx);

        nodeId = ctx.localNodeId();
        marshaller = ctx.config().getMarshaller();

        mapQryExec = new GridMapQueryExecutor(busyLock);
        rdcQryExec = new GridReduceQueryExecutor(busyLock);

        mapQryExec.start(ctx, this);
        rdcQryExec.start(ctx, this);

        runningQryMgr = new RunningQueryManager(ctx);
        partExtractor = new PartitionExtractor(new H2PartitionResolver(this));

        cmdProc = new CommandProcessor(ctx, schemaMgr, runningQryMgr);

        if (JdbcUtils.serializer != null)
            U.warn(log, "Custom H2 serialization is already configured, will override.");

        JdbcUtils.serializer = h2Serializer();
    }

    /**
     * @return Value object context.
     */
    public CacheObjectValueContext objectContext() {
        return ctx.query().objectContext();
    }

    /**
     * @param topic Topic.
     * @param topicOrd Topic ordinal for {@link GridTopic}.
     * @param nodes Nodes.
     * @param msg Message.
     * @param specialize Optional closure to specialize message for each node.
     * @param locNodeHnd Handler for local node.
     * @param plc Policy identifying the executor service which will process message.
     * @param runLocParallel Run local handler in parallel thread.
     * @return {@code true} If all messages sent successfully.
     */
    public boolean send(
        Object topic,
        int topicOrd,
        Collection<ClusterNode> nodes,
        Message msg,
        @Nullable IgniteBiClosure<ClusterNode, Message, Message> specialize,
        @Nullable final IgniteInClosure2X<ClusterNode, Message> locNodeHnd,
        byte plc,
        boolean runLocParallel
    ) {
        boolean ok = true;

        if (specialize == null && msg instanceof GridCacheQueryMarshallable)
            ((GridCacheQueryMarshallable)msg).marshall(marshaller);

        ClusterNode locNode = null;

        for (ClusterNode node : nodes) {
            if (node.isLocal()) {
                if (locNode != null)
                    throw new IllegalStateException();

                locNode = node;

                continue;
            }

            try {
                if (specialize != null) {
                    msg = specialize.apply(node, msg);

                    if (msg instanceof GridCacheQueryMarshallable)
                        ((GridCacheQueryMarshallable)msg).marshall(marshaller);
                }

                ctx.io().sendGeneric(node, topic, topicOrd, msg, plc);
            }
            catch (IgniteCheckedException e) {
                ok = false;

                U.warn(log, "Failed to send message [node=" + node + ", msg=" + msg +
                    ", errMsg=" + e.getMessage() + "]");
            }
        }

        // Local node goes the last to allow parallel execution.
        if (locNode != null) {
            assert locNodeHnd != null;

            if (specialize != null)
                msg = specialize.apply(locNode, msg);

            if (runLocParallel) {
                final ClusterNode finalLocNode = locNode;
                final Message finalMsg = msg;

                try {
                    // We prefer runLocal to runLocalSafe, because the latter can produce deadlock here.
                    ctx.closure().runLocal(new GridPlainRunnable() {
                        @Override public void run() {
                            if (!busyLock.enterBusy())
                                return;

                            try {
                                locNodeHnd.apply(finalLocNode, finalMsg);
                            }
                            finally {
                                busyLock.leaveBusy();
                            }
                        }
                    }, plc).listen(logger);
                }
                catch (IgniteCheckedException e) {
                    ok = false;

                    U.error(log, "Failed to execute query locally.", e);
                }
            }
            else
                locNodeHnd.apply(locNode, msg);
        }

        return ok;
    }

    /**
     * @return Serializer.
     */
    private JavaObjectSerializer h2Serializer() {
        return new JavaObjectSerializer() {
            @Override public byte[] serialize(Object obj) throws Exception {
                return U.marshal(marshaller, obj);
            }

            @Override public Object deserialize(byte[] bytes) throws Exception {
                ClassLoader clsLdr = ctx != null ? U.resolveClassLoader(ctx.config()) : null;

                return U.unmarshal(marshaller, bytes, clsLdr);
            }
        };
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        if (log.isDebugEnabled())
            log.debug("Stopping cache query index...");

        mapQryExec.cancelLazyWorkers();

        qryCtxRegistry.clearSharedOnLocalNodeStop();

        runningQryMgr.stop();
        schemaMgr.stop();
        connMgr.stop();

        if (log.isDebugEnabled())
            log.debug("Cache query index stopped.");
    }

    /** {@inheritDoc} */
    @Override public void onClientDisconnect() throws IgniteCheckedException {
        if (!mvccEnabled(ctx))
            return;

        GridNearTxLocal tx = tx(ctx);

        if (tx != null)
            cmdProc.doRollback(tx);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean initCacheContext(GridCacheContext cacheCtx) {
        GridCacheContextInfo cacheInfo = registeredCacheInfo(cacheCtx.name());

        if (cacheInfo != null) {
            assert !cacheInfo.isCacheContextInited() : cacheInfo.name();
            assert cacheInfo.name().equals(cacheCtx.name()) : cacheInfo.name() + " != " + cacheCtx.name();

            cacheInfo.initCacheContext(cacheCtx);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void registerCache(String cacheName, String schemaName, GridCacheContextInfo<?, ?> cacheInfo)
        throws IgniteCheckedException {
        rowCache.onCacheRegistered(cacheInfo);

        schemaMgr.onCacheCreated(cacheName, schemaName, cacheInfo.config().getSqlFunctionClasses());
    }

    /** {@inheritDoc} */
    @Override public void unregisterCache(GridCacheContextInfo cacheInfo, boolean rmvIdx) {
        rowCache.onCacheUnregistered(cacheInfo);

        String cacheName = cacheInfo.name();

        partReservationMgr.onCacheStop(cacheName);

        // Remove cached DML plans.
        Iterator<Map.Entry<H2CachedStatementKey, UpdatePlan>> iter = updatePlanCache.entrySet().iterator();

        while (iter.hasNext()) {
            UpdatePlan plan = iter.next().getValue();

            if (F.eq(cacheName, plan.cacheContext().name()))
                iter.remove();
        }

        // Drop schema (needs to be called after callback to DML processor because the latter depends on schema).
        schemaMgr.onCacheDestroyed(cacheName, rmvIdx);

        // Unregister connection.
        connMgr.onCacheDestroyed();

        // Clear query cache.
        clearPlanCache();
    }

    /**
     * Remove all cached queries from cached two-steps queries.
     */
    private void clearPlanCache() {
        parser.clearCache();

        updatePlanCache = new GridBoundedConcurrentLinkedHashMap<>(UPDATE_PLAN_CACHE_SIZE);
    }

    /** {@inheritDoc} */
    @Override public IndexingQueryFilter backupFilter(@Nullable final AffinityTopologyVersion topVer,
        @Nullable final int[] parts) {
        return new IndexingQueryFilterImpl(ctx, topVer, parts);
    }

    /**
     * @return Ready topology version.
     */
    public AffinityTopologyVersion readyTopologyVersion() {
        return ctx.cache().context().exchange().readyAffinityVersion();
    }

    /**
     * @param readyVer Ready topology version.
     *
     * @return {@code true} If pending distributed exchange exists because server topology is changed.
     */
    public boolean serverTopologyChanged(AffinityTopologyVersion readyVer) {
        GridDhtPartitionsExchangeFuture fut = ctx.cache().context().exchange().lastTopologyFuture();

        if (fut.isDone())
            return false;

        AffinityTopologyVersion initVer = fut.initialVersion();

        return initVer.compareTo(readyVer) > 0 && !fut.firstEvent().node().isClient();
    }

    /**
     * @param topVer Topology version.
     * @throws IgniteCheckedException If failed.
     */
    public void awaitForReadyTopologyVersion(AffinityTopologyVersion topVer) throws IgniteCheckedException {
        IgniteInternalFuture<?> fut = ctx.cache().context().exchange().affinityReadyFuture(topVer);

        if (fut != null)
            fut.get();
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {
        rdcQryExec.onDisconnected(reconnectFut);
    }

    /**
     * Return SQL running queries.
     *
     * @return SQL running queries.
     */
    public List<GridRunningQueryInfo> runningSqlQueries() {
        return runningQryMgr.runningSqlQueries();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRunningQueryInfo> runningQueries(long duration) {
        return runningQryMgr.longRunningQueries(duration);
    }

    /** {@inheritDoc} */
    @Override public void cancelQueries(Collection<Long> queries) {
        if (!F.isEmpty(queries)) {
            for (Long qryId : queries)
                runningQryMgr.cancel(qryId);
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop() {
        mapQryExec.cancelLazyWorkers();

        connMgr.onKernalStop();
    }

    /**
     * @return Query context registry.
     */
    public QueryContextRegistry queryContextRegistry() {
        return qryCtxRegistry;
    }

    /**
     * @return Connection manager.
     */
    public ConnectionManager connections() {
        return connMgr;
    }

    /**
     * @return Schema manager.
     */
    public SchemaManager schemaManager() {
        return schemaMgr;
    }

    /**
     * @return Partition extractor.
     */
    public PartitionExtractor partitionExtractor() {
        return partExtractor;
    }

    /**
     * @return Partition reservation manager.
     */
    public PartitionReservationManager partitionReservationManager() {
        return partReservationMgr;
    }

    /**
     * @param schemaName Schema.
     * @param conn Connection.
     * @param prepared Prepared statement.
     * @param fieldsQry Initial query
     * @param cancel Query cancel.
     * @return Update result wrapped into {@link GridQueryFieldsResult}
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    private List<QueryCursorImpl<List<?>>> executeUpdateDistributed(
        String schemaName,
        Connection conn,
        Prepared prepared,
        SqlFieldsQuery fieldsQry,
        GridQueryCancel cancel
    ) throws IgniteCheckedException {
        if (DmlUtils.isBatched(fieldsQry)) {
            SqlFieldsQueryEx fieldsQry0 = (SqlFieldsQueryEx)fieldsQry;

            Collection<UpdateResult> ress;

            List<Object[]> argss = fieldsQry0.batchedArguments();

            UpdatePlan plan = updatePlan(schemaName, conn, prepared, fieldsQry0);

            GridCacheContext<?, ?> cctx = plan.cacheContext();

            // For MVCC case, let's enlist batch elements one by one.
            if (plan.hasRows() && plan.mode() == UpdateMode.INSERT && !cctx.mvccEnabled()) {
                CacheOperationContext opCtx = DmlUtils.setKeepBinaryContext(cctx);

                try {
                    List<List<List<?>>> cur = plan.createRows(argss);

                    ress = DmlUtils.processSelectResultBatched(plan, cur, fieldsQry0.getPageSize());
                }
                finally {
                    DmlUtils.restoreKeepBinaryContext(cctx, opCtx);
                }
            }
            else {
                // Fallback to previous mode.
                ress = new ArrayList<>(argss.size());

                SQLException batchException = null;

                int[] cntPerRow = new int[argss.size()];

                int cntr = 0;

                for (Object[] args : argss) {
                    SqlFieldsQueryEx qry0 = (SqlFieldsQueryEx)fieldsQry0.copy();

                    qry0.clearBatchedArgs();
                    qry0.setArgs(args);

                    UpdateResult res;

                    try {
                        res = executeUpdate(schemaName, conn, prepared, qry0, false, null, cancel);

                        cntPerRow[cntr++] = (int)res.counter();

                        ress.add(res);
                    }
                    catch (Exception e ) {
                        SQLException sqlEx = QueryUtils.toSqlException(e);

                        batchException = DmlUtils.chainException(batchException, sqlEx);

                        cntPerRow[cntr++] = Statement.EXECUTE_FAILED;
                    }
                }

                if (batchException != null) {
                    BatchUpdateException e = new BatchUpdateException(batchException.getMessage(),
                        batchException.getSQLState(), batchException.getErrorCode(), cntPerRow, batchException);

                    throw new IgniteCheckedException(e);
                }
            }

            ArrayList<QueryCursorImpl<List<?>>> resCurs = new ArrayList<>(ress.size());

            for (UpdateResult res : ress) {
                res.throwIfError();

                QueryCursorImpl<List<?>> resCur = (QueryCursorImpl<List<?>>)new QueryCursorImpl(Collections.singletonList
                    (Collections.singletonList(res.counter())), cancel, false);

                resCur.fieldsMeta(UPDATE_RESULT_META);

                resCurs.add(resCur);
            }

            return resCurs;
        }
        else {
            UpdateResult res = executeUpdate(schemaName, conn, prepared, fieldsQry, false, null, cancel);

            res.throwIfError();

            QueryCursorImpl<List<?>> resCur = (QueryCursorImpl<List<?>>)new QueryCursorImpl(Collections.singletonList
                (Collections.singletonList(res.counter())), cancel, false);

            resCur.fieldsMeta(UPDATE_RESULT_META);

            return Collections.singletonList(resCur);
        }
    }

    /**
     * Execute DML statement, possibly with few re-attempts in case of concurrent data modifications.
     *
     * @param schemaName Schema.
     * @param conn Connection.
     * @param prepared Prepared statement.
     * @param fieldsQry Original query.
     * @param locSplit Whether local split is needed.
     * @param filters Cache name and key filter.
     * @param cancel Cancel.
     * @return Update result (modified items count and failed keys).
     * @throws IgniteCheckedException if failed.
     */
    private UpdateResult executeUpdate(String schemaName, Connection conn, Prepared prepared,
        SqlFieldsQuery fieldsQry, boolean locSplit, IndexingQueryFilter filters, GridQueryCancel cancel)
        throws IgniteCheckedException {
        Object[] errKeys = null;

        long items = 0;

        UpdatePlan plan = updatePlan(schemaName, conn, prepared, fieldsQry);

        GridCacheContext<?, ?> cctx = plan.cacheContext();

        for (int i = 0; i < DFLT_UPDATE_RERUN_ATTEMPTS; i++) {
            CacheOperationContext opCtx = DmlUtils.setKeepBinaryContext(cctx);

            UpdateResult r;

            try {
                r = executeUpdate0(schemaName, plan, fieldsQry, locSplit, filters, cancel);
            }
            finally {
                DmlUtils.restoreKeepBinaryContext(cctx, opCtx);
            }

            items += r.counter();
            errKeys = r.errorKeys();

            if (F.isEmpty(errKeys))
                break;
        }

        if (F.isEmpty(errKeys)) {
            if (items == 1L)
                return UpdateResult.ONE;
            else if (items == 0L)
                return UpdateResult.ZERO;
        }

        return new UpdateResult(items, errKeys);
    }

    /**
     * Actually perform SQL DML operation locally.
     *
     * @param schemaName Schema name.
     * @param plan Cache context.
     * @param fieldsQry Fields query.
     * @param locSplit Whether local split is needed.
     * @param filters Cache name and key filter.
     * @param cancel Query cancel state holder.
     * @return Pair [number of successfully processed items; keys that have failed to be processed]
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"ConstantConditions"})
    private UpdateResult executeUpdate0(
        String schemaName,
        final UpdatePlan plan,
        SqlFieldsQuery fieldsQry,
        boolean locSplit,
        IndexingQueryFilter filters,
        GridQueryCancel cancel
    ) throws IgniteCheckedException {
        GridCacheContext cctx = plan.cacheContext();

        if (cctx != null && cctx.mvccEnabled()) {
            assert cctx.transactional();

            DmlDistributedPlanInfo distributedPlan = plan.distributedPlan();

            GridNearTxLocal tx = tx(cctx.kernalContext());

            boolean implicit = (tx == null);

            boolean commit = implicit && (!(fieldsQry instanceof SqlFieldsQueryEx) ||
                ((SqlFieldsQueryEx)fieldsQry).isAutoCommit());

            if (implicit)
                tx = txStart(cctx, fieldsQry.getTimeout());

            requestSnapshot(cctx, tx);

            try (GridNearTxLocal toCommit = commit ? tx : null) {
                long timeout = implicit
                    ? tx.remainingTime()
                    : operationTimeout(fieldsQry.getTimeout(), tx);

                if (cctx.isReplicated() || distributedPlan == null || ((plan.mode() == UpdateMode.INSERT
                    || plan.mode() == UpdateMode.MERGE) && !plan.isLocalSubquery())) {

                    boolean sequential = true;

                    UpdateSourceIterator<?> it;

                    if (plan.fastResult()) {
                        IgniteBiTuple row = plan.getFastRow(fieldsQry.getArgs());

                        EnlistOperation op = UpdatePlan.enlistOperation(plan.mode());

                        it = new DmlUpdateSingleEntryIterator<>(op, op.isDeleteOrLock() ? row.getKey() : row);
                    }
                    else if (plan.hasRows())
                        it = new DmlUpdateResultsIterator(UpdatePlan.enlistOperation(plan.mode()), plan, plan.createRows(fieldsQry.getArgs()));
                    else {
                        // TODO IGNITE-8865 if there is no ORDER BY statement it's no use to retain entries order on locking (sequential = false).
                        SqlFieldsQuery newFieldsQry = new SqlFieldsQuery(plan.selectQuery(), fieldsQry.isCollocated())
                            .setArgs(fieldsQry.getArgs())
                            .setDistributedJoins(fieldsQry.isDistributedJoins())
                            .setEnforceJoinOrder(fieldsQry.isEnforceJoinOrder())
                            .setLocal(fieldsQry.isLocal())
                            .setPageSize(fieldsQry.getPageSize())
                            .setTimeout((int)timeout, TimeUnit.MILLISECONDS)
                            .setDataPageScanEnabled(fieldsQry.isDataPageScanEnabled());

                        FieldsQueryCursor<List<?>> cur = querySqlFields(schemaName, newFieldsQry, null,
                            true, true, MvccUtils.mvccTracker(cctx, tx), cancel, false).get(0);

                        it = plan.iteratorForTransaction(connMgr, cur);
                    }

                    IgniteInternalFuture<Long> fut = tx.updateAsync(cctx, it,
                        fieldsQry.getPageSize(), timeout, sequential);

                    UpdateResult res = new UpdateResult(fut.get(), X.EMPTY_OBJECT_ARRAY);

                    if (commit)
                        toCommit.commit();

                    return res;
                }

                int[] ids = U.toIntArray(distributedPlan.getCacheIds());

                int flags = 0;

                if (fieldsQry.isEnforceJoinOrder())
                    flags |= GridH2QueryRequest.FLAG_ENFORCE_JOIN_ORDER;

                if (distributedPlan.isReplicatedOnly())
                    flags |= GridH2QueryRequest.FLAG_REPLICATED;

                flags = GridH2QueryRequest.setDataPageScanEnabled(flags,
                    fieldsQry.isDataPageScanEnabled());

                int[] parts = fieldsQry.getPartitions();

                IgniteInternalFuture<Long> fut = tx.updateAsync(
                    cctx,
                    ids,
                    parts,
                    schemaName,
                    fieldsQry.getSql(),
                    fieldsQry.getArgs(),
                    flags,
                    fieldsQry.getPageSize(),
                    timeout);

                UpdateResult res = new UpdateResult(fut.get(), X.EMPTY_OBJECT_ARRAY);

                if (commit)
                    toCommit.commit();

                return res;
            }
            catch (ClusterTopologyServerNotFoundException e) {
                throw new CacheServerNotFoundException(e.getMessage(), e);
            }
            catch (IgniteCheckedException e) {
                IgniteSQLException sqlEx = X.cause(e, IgniteSQLException.class);

                if(sqlEx != null)
                    throw sqlEx;

                Exception ex = IgniteUtils.convertExceptionNoWrap(e);

                if (ex instanceof IgniteException)
                    throw (IgniteException)ex;

                U.error(log, "Error during update [localNodeId=" + cctx.localNodeId() + "]", ex);

                throw new IgniteSQLException("Failed to run update. " + ex.getMessage(), ex);
            }
            finally {
                if (commit)
                    cctx.tm().resetContext();
            }
        }

        UpdateResult fastUpdateRes = plan.processFast(fieldsQry.getArgs());

        if (fastUpdateRes != null)
            return fastUpdateRes;

        if (plan.distributedPlan() != null) {
            DmlDistributedPlanInfo distributedPlan = plan.distributedPlan();

            assert distributedPlan != null;

            if (cancel == null)
                cancel = new GridQueryCancel();

            UpdateResult result = runDistributedUpdate(schemaName, fieldsQry, distributedPlan.getCacheIds(),
                distributedPlan.isReplicatedOnly(), cancel);

            // null is returned in case not all nodes support distributed DML.
            if (result != null)
                return result;
        }

        Iterable<List<?>> cur;

        // Do a two-step query only if locality flag is not set AND if plan's SELECT corresponds to an actual
        // sub-query and not some dummy stuff like "select 1, 2, 3;"
        if (locSplit && !plan.isLocalSubquery()) {
            assert !F.isEmpty(plan.selectQuery());

            SqlFieldsQuery newFieldsQry = new SqlFieldsQuery(plan.selectQuery(), fieldsQry.isCollocated())
                .setArgs(fieldsQry.getArgs())
                .setDistributedJoins(fieldsQry.isDistributedJoins())
                .setEnforceJoinOrder(fieldsQry.isEnforceJoinOrder())
                .setLocal(fieldsQry.isLocal())
                .setPageSize(fieldsQry.getPageSize())
                .setTimeout(fieldsQry.getTimeout(), TimeUnit.MILLISECONDS)
                .setDataPageScanEnabled(fieldsQry.isDataPageScanEnabled());

            cur = querySqlFields(schemaName, newFieldsQry, null, true, true, null, cancel, false).get(0);
        }
        else if (plan.hasRows())
            cur = plan.createRows(fieldsQry.getArgs());
        else {
            final GridQueryFieldsResult res = executeQueryLocal0(
                schemaName,
                plan.selectQuery(),
                F.asList(fieldsQry.getArgs()),
                null,
                filters,
                fieldsQry.isEnforceJoinOrder(),
                false,
                fieldsQry.getTimeout(),
                cancel,
                null,
                null
            );

            cur = new QueryCursorImpl<>(new Iterable<List<?>>() {
                @Override public Iterator<List<?>> iterator() {
                    try {
                        return new GridQueryCacheObjectsIterator(res.iterator(), objectContext(), true);
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
            }, cancel);
        }

        return DmlUtils.processSelectResult(plan, cur, fieldsQry.getPageSize());
    }

    /**
     * Generate SELECT statements to retrieve data for modifications from and find fast UPDATE or DELETE args,
     * if available.
     *
     * @param schema Schema.
     * @param conn Connection.
     * @param p Prepared statement.
     * @param fieldsQry Original fields query.
     * @return Update plan.
     */
    @SuppressWarnings("IfMayBeConditional")
    private UpdatePlan updatePlan(
        String schema,
        Connection conn,
        Prepared p,
        SqlFieldsQuery fieldsQry
    ) throws IgniteCheckedException {
        if (F.eq(QueryUtils.SCHEMA_SYS, schema))
            throw new IgniteSQLException("DML statements are not supported on " + schema + " schema",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        H2CachedStatementKey planKey = new H2CachedStatementKey(schema, p.getSQL(), fieldsQry);

        UpdatePlan res = updatePlanCache.get(planKey);

        if (res != null)
            return res;

        res = UpdatePlanBuilder.planForStatement(p, this, conn, fieldsQry, updateInTxAllowed);

        // Don't cache re-runs
        UpdatePlan oldRes = updatePlanCache.putIfAbsent(planKey, res);

        return oldRes != null ? oldRes : res;
    }
}
