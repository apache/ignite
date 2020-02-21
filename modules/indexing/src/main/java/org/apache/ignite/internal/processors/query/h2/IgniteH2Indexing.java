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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
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
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.managers.IgniteMBeansManager;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.mxbean.SqlQueryMXBean;
import org.apache.ignite.internal.mxbean.SqlQueryMXBeanImpl;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteClusterReadOnlyException;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.mvcc.MvccQueryTracker;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.mvcc.StaticMvccQueryTracker;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMarshallable;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.query.RegisteredQueryCursor;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxAdapter;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcParameterMeta;
import org.apache.ignite.internal.processors.query.ColumnInformation;
import org.apache.ignite.internal.processors.query.EnlistOperation;
import org.apache.ignite.internal.processors.query.GridQueryCacheObjectsIterator;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResult;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResultAdapter;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.RunningQueryManager;
import org.apache.ignite.internal.processors.query.SqlClientContext;
import org.apache.ignite.internal.processors.query.TableInformation;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.internal.processors.query.h2.affinity.H2PartitionResolver;
import org.apache.ignite.internal.processors.query.h2.affinity.PartitionExtractor;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeClientIndex;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2InnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2MvccInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2MvccLeafIO;
import org.apache.ignite.internal.processors.query.h2.dml.DmlDistributedPlanInfo;
import org.apache.ignite.internal.processors.query.h2.dml.DmlUpdateResultsIterator;
import org.apache.ignite.internal.processors.query.h2.dml.DmlUpdateSingleEntryIterator;
import org.apache.ignite.internal.processors.query.h2.dml.DmlUtils;
import org.apache.ignite.internal.processors.query.h2.dml.UpdateMode;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlan;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.apache.ignite.internal.processors.query.h2.opt.QueryContext;
import org.apache.ignite.internal.processors.query.h2.opt.QueryContextRegistry;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.apache.ignite.internal.processors.query.h2.twostep.GridMapQueryExecutor;
import org.apache.ignite.internal.processors.query.h2.twostep.GridReduceQueryExecutor;
import org.apache.ignite.internal.processors.query.h2.twostep.PartitionReservationManager;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryCancelRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryFailResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2DmlRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2DmlResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorImpl;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.sql.command.SqlCommitTransactionCommand;
import org.apache.ignite.internal.sql.command.SqlRollbackTransactionCommand;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionResult;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridEmptyCloseableIterator;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.lang.IgniteSingletonIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilterImpl;
import org.h2.api.ErrorCode;
import org.h2.api.JavaObjectSerializer;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableType;
import org.h2.util.JdbcUtils;
import org.h2.value.DataType;
import org.jetbrains.annotations.Nullable;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.singletonList;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MVCC_TX_SIZE_CACHING_THRESHOLD;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccCachingManager.TX_SIZE_THRESHOLD;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.checkActive;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.mvccEnabled;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.requestSnapshot;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.tx;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.txStart;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.TEXT;
import static org.apache.ignite.internal.processors.query.QueryUtils.matches;
import static org.apache.ignite.internal.processors.query.h2.H2Utils.UPDATE_RESULT_META;
import static org.apache.ignite.internal.processors.query.h2.H2Utils.generateFieldsQueryString;
import static org.apache.ignite.internal.processors.query.h2.H2Utils.session;
import static org.apache.ignite.internal.processors.query.h2.H2Utils.validateTypeDescriptor;
import static org.apache.ignite.internal.processors.query.h2.H2Utils.zeroCursor;

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

    /** Default number of attempts to re-run DELETE and UPDATE queries in case of concurrent modifications of values. */
    private static final int DFLT_UPDATE_RERUN_ATTEMPTS = 4;

    /** Cached value of {@code IgniteSystemProperties.IGNITE_ALLOW_DML_INSIDE_TRANSACTION}. */
    private final boolean updateInTxAllowed =
        Boolean.getBoolean(IgniteSystemProperties.IGNITE_ALLOW_DML_INSIDE_TRANSACTION);

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

    /** H2 Connection manager. */
    private LongRunningQueryManager longRunningQryMgr;

    /** Discovery event listener. */
    private GridLocalEventListener discoLsnr;

    /** Query message listener. */
    private GridMessageListener qryLsnr;

    /**
     * @return Kernal context.
     */
    public GridKernalContext kernalContext() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Override public List<JdbcParameterMeta> parameterMetaData(String schemaName, SqlFieldsQuery qry)
        throws IgniteSQLException {
        assert qry != null;

        ArrayList<JdbcParameterMeta> metas = new ArrayList<>();

        SqlFieldsQuery curQry = qry;

        while (curQry != null) {
            QueryParserResult parsed = parser.parse(schemaName, curQry, true);

            metas.addAll(parsed.parametersMeta());

            curQry = parsed.remainingQuery();
        }

        return metas;
    }

    /** {@inheritDoc} */
    @Override public List<GridQueryFieldMetadata> resultMetaData(String schemaName, SqlFieldsQuery qry)
        throws IgniteSQLException{
        QueryParserResult parsed = parser.parse(schemaName, qry, true);

        if (parsed.remainingQuery() != null)
            return null;

        if (parsed.isSelect())
            return parsed.select().meta();

        return null;
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
                return new H2TreeClientIndex(tbl, name, pk, unwrappedCols, inlineSize);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocalText(String schemaName,
        String cacheName, String qry, String typeName, IndexingQueryFilter filters, int limit) throws IgniteCheckedException {
        H2TableDescriptor tbl = schemaMgr.tableForType(schemaName, cacheName, typeName);

        if (tbl != null && tbl.luceneIndex() != null) {
            Long qryId = runningQueryManager().register(qry, TEXT, schemaName, true, null);

            try {
                return tbl.luceneIndex().query(qry.toUpperCase(), filters, limit);
            }
            finally {
                runningQueryManager().unregister(qryId, null);
            }
        }

        return new GridEmptyCloseableIterator<>();
    }

    /**
     * Queries individual fields (generally used by JDBC drivers).
     *
     * @param qryDesc Query descriptor.
     * @param qryParams Query parameters.
     * @param select Select.
     * @param filter Cache name and key filter.
     * @param mvccTracker Query tracker.
     * @param cancel Query cancel.
     * @param inTx Flag whether the query is executed in transaction.
     * @param timeout Timeout.
     * @return Query result.
     * @throws IgniteCheckedException If failed.
     */
    private GridQueryFieldsResult executeSelectLocal(
        QueryDescriptor qryDesc,
        QueryParameters qryParams,
        QueryParserResultSelect select,
        final IndexingQueryFilter filter,
        MvccQueryTracker mvccTracker,
        GridQueryCancel cancel,
        boolean inTx,
        int timeout
    ) throws IgniteCheckedException {
        assert !select.mvccEnabled() || mvccTracker != null;

        String qry;

        if (select.forUpdate())
            qry = inTx ? select.forUpdateQueryTx() : select.forUpdateQueryOutTx();
        else
            qry = qryDesc.sql();

        boolean mvccEnabled = mvccTracker != null;

        try {
            assert select != null;

            if (ctx.security().enabled())
                checkSecurity(select.cacheIds());

            MvccSnapshot mvccSnapshot = null;

            if (mvccEnabled)
                mvccSnapshot = mvccTracker.snapshot();

            final QueryContext qctx = new QueryContext(
                0,
                filter,
                null,
                mvccSnapshot,
                null,
                true
            );

            return new GridQueryFieldsResultAdapter(select.meta(), null) {
                @Override public GridCloseableIterator<List<?>> iterator() throws IgniteCheckedException {
                    assert qryCtxRegistry.getThreadLocal() == null;

                    qryCtxRegistry.setThreadLocal(qctx);

                    ThreadLocalObjectPool<H2ConnectionWrapper>.Reusable conn = connMgr.detachThreadConnection();

                    try {
                        Connection conn0 = conn.object().connection(qryDesc.schemaName());

                        H2Utils.setupConnection(conn0,
                            qryDesc.distributedJoins(), qryDesc.enforceJoinOrder(), qryParams.lazy());

                        List<Object> args = F.asList(qryParams.arguments());

                        PreparedStatement stmt = preparedStatementWithParams(
                            conn0,
                            qry,
                            args,
                            true
                        );

                        ResultSet rs = executeSqlQueryWithTimer(
                            stmt,
                            conn0,
                            qry,
                            args,
                            timeout,
                            cancel,
                            qryParams.dataPageScanEnabled(),
                            new H2QueryInfo(H2QueryInfo.QueryType.LOCAL, stmt, qry)
                        );

                        return new H2FieldsIterator(rs, mvccTracker, conn);
                    }
                    catch (IgniteCheckedException | RuntimeException | Error e) {
                        conn.recycle();

                        try {
                            if (mvccTracker != null)
                                mvccTracker.onDone();
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
        catch (Exception e) {
            GridNearTxLocal tx = null;

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

            return remaining > 0 && qryTimeout > 0 ? min(remaining, qryTimeout) : max(remaining, qryTimeout);
        }

        return qryTimeout;
    }

    /** {@inheritDoc} */
    @Override public long streamUpdateQuery(
        String schemaName,
        String qry,
        @Nullable Object[] params,
        IgniteDataStreamer<?, ?> streamer
    ) throws IgniteCheckedException {
        QueryParserResultDml dml = streamerParse(schemaName, qry);

        return streamQuery0(qry, schemaName, streamer, dml, params);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ForLoopReplaceableByForEach", "ConstantConditions"})
    @Override public List<Long> streamBatchedUpdateQuery(
        String schemaName,
        String qry,
        List<Object[]> params,
        SqlClientContext cliCtx
    ) throws IgniteCheckedException {
        if (cliCtx == null || !cliCtx.isStream()) {
            U.warn(log, "Connection is not in streaming mode.");

            return zeroBatchedStreamedUpdateResult(params.size());
        }

        QueryParserResultDml dml = streamerParse(schemaName, qry);

        IgniteDataStreamer<?, ?> streamer = cliCtx.streamerForCache(dml.streamTable().cacheName());

        assert streamer != null;

        List<Long> ress = new ArrayList<>(params.size());

        for (int i = 0; i < params.size(); i++) {
            long res = streamQuery0(qry, schemaName, streamer, dml, params.get(i));

            ress.add(res);
        }

        return ress;
    }

    /**
     * Perform given statement against given data streamer. Only rows based INSERT is supported.
     *
     * @param qry Query.
     * @param schemaName Schema name.
     * @param streamer Streamer to feed data to.
     * @param dml DML statement.
     * @param args Statement arguments.
     * @return Number of rows in given INSERT statement.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"unchecked", "Anonymous2MethodRef"})
    private long streamQuery0(String qry, String schemaName, IgniteDataStreamer streamer, QueryParserResultDml dml,
        final Object[] args) throws IgniteCheckedException {
        Long qryId = runningQryMgr.register(qry, GridCacheQueryType.SQL_FIELDS, schemaName, true, null);

        Exception failReason = null;

        try {
            UpdatePlan plan = dml.plan();

            List<List<?>> planRows = plan.createRows(args != null ? args : X.EMPTY_OBJECT_ARRAY);

            Iterator<List<?>> iter = new GridQueryCacheObjectsIterator(
                planRows.iterator(),
                objectContext(),
                true
            );

            if (planRows.size() == 1) {
                IgniteBiTuple t = plan.processRow(iter.next());

                streamer.addData(t.getKey(), t.getValue());

                return 1;
            }
            else {
                Map<Object, Object> rows = new LinkedHashMap<>(plan.rowCount());

                while (iter.hasNext()) {
                    List<?> row = iter.next();

                    IgniteBiTuple t = plan.processRow(row);

                    rows.put(t.getKey(), t.getValue());
                }

                streamer.addData(rows);

                return rows.size();
            }
        }
        catch (IgniteException | IgniteCheckedException e) {
            failReason = e;

            throw e;
        }
        finally {
            runningQryMgr.unregister(qryId, failReason);
        }
    }

    /**
     * Parse statement for streamer.
     *
     * @param schemaName Schema name.
     * @param qry Query.
     * @return DML.
     */
    private QueryParserResultDml streamerParse(String schemaName, String qry) {
        QueryParserResult parseRes = parser.parse(schemaName, new SqlFieldsQuery(qry), false);

        QueryParserResultDml dml = parseRes.dml();

        if (dml == null || !dml.streamable()) {
            throw new IgniteSQLException("Streaming mode supports only INSERT commands without subqueries.",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        return dml;
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
    public PreparedStatement preparedStatementWithParams(Connection conn, String sql, Collection<Object> params,
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
        if (cancel != null)
            cancel.set(() -> cancelStatement(stmt));

        Session ses = session(conn);

        if (timeoutMillis > 0)
            ses.setQueryTimeout(timeoutMillis);
        else
            ses.setQueryTimeout(0);

        try {
            return stmt.executeQuery();
        }
        catch (SQLException e) {
            // Throw special exception.
            if (e.getErrorCode() == ErrorCode.STATEMENT_WAS_CANCELED)
                throw new QueryCancelledException();

            throw new IgniteCheckedException("Failed to execute SQL query. " + e.getMessage(), e);
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
        Boolean dataPageScanEnabled,
        final H2QueryInfo qryInfo
    ) throws IgniteCheckedException {
        return executeSqlQueryWithTimer(preparedStatementWithParams(conn, sql, params, false),
            conn, sql, params, timeoutMillis, cancel, dataPageScanEnabled, qryInfo);
    }

    /**
     * @param dataPageScanEnabled If data page scan is enabled.
     */
    public void enableDataPageScan(Boolean dataPageScanEnabled) {
        // Data page scan is enabled by default for SQL.
        // TODO https://issues.apache.org/jira/browse/IGNITE-11998
        CacheDataTree.setDataPageScanEnabled(false);
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
        Boolean dataPageScanEnabled,
        final H2QueryInfo qryInfo
    ) throws IgniteCheckedException {
        if (qryInfo != null)
            longRunningQryMgr.registerQuery(qryInfo);

        enableDataPageScan(dataPageScanEnabled);

        try {
            ResultSet rs = executeSqlQuery(conn, stmt, timeoutMillis, cancel);

            if (qryInfo != null && qryInfo.time() > longRunningQryMgr.getTimeout())
                qryInfo.printLogMessage(log, "Long running query is finished");

            return rs;
        }
        catch (Throwable e) {
            if (qryInfo != null && qryInfo.time() > longRunningQryMgr.getTimeout()) {
                qryInfo.printLogMessage(log, "Long running query is finished with error: "
                    + e.getMessage());
            }

            throw  e;
        }
        finally {
            CacheDataTree.setDataPageScanEnabled(false);

            if (qryInfo != null)
                longRunningQryMgr.unregisterQuery(qryInfo);
        }
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
            sql = generateFieldsQueryString(qry.getSql(), qry.getAlias(), tblDesc);
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

        if (qry.getTimeout() > 0)
            res.setTimeout(qry.getTimeout(), TimeUnit.MILLISECONDS);

        return res;
    }

    /**
     * Execute command.
     *
     * @param qryDesc Query descriptor.
     * @param qryParams Query parameters.
     * @param cliCtx CLient context.
     * @param cmd Command (native).
     * @return Result.
     */
    private FieldsQueryCursor<List<?>> executeCommand(
        QueryDescriptor qryDesc,
        QueryParameters qryParams,
        @Nullable SqlClientContext cliCtx,
        QueryParserResultCommand cmd
    ) {
        if (cmd.noOp())
            return zeroCursor();

        SqlCommand cmdNative = cmd.commandNative();
        GridSqlStatement cmdH2 = cmd.commandH2();

        if (qryDesc.local()) {
            throw new IgniteSQLException("DDL statements are not supported for LOCAL caches",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        Long qryId = registerRunningQuery(qryDesc, null);

        CommandResult res = null;

        Exception failReason = null;

        try {
            res = cmdProc.runCommand(qryDesc.sql(), cmdNative, cmdH2, qryParams, cliCtx, qryId);

            return res.cursor();
        }
        catch (IgniteException e) {
            failReason = e;

            throw e;
        }
        catch (IgniteCheckedException e) {
            failReason = e;

            throw new IgniteSQLException("Failed to execute DDL statement [stmt=" + qryDesc.sql() +
                ", err=" + e.getMessage() + ']', e);
        }
        finally {
            if (res == null || res.unregisterRunningQuery())
                runningQryMgr.unregister(qryId, failReason);
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
                QueryParserResultCommand cmd = parseRes.command();

                assert cmd != null;

                SqlCommand cmd0 = cmd.commandNative();

                if (cmd0 instanceof SqlCommitTransactionCommand || cmd0 instanceof SqlRollbackTransactionCommand)
                    return;
            }

            throw new IgniteException("Can not perform the operation because the cluster is inactive. Note, " +
                "that the cluster is considered inactive by default if Ignite Persistent Store is used to " +
                "let all the nodes join the cluster. To activate the cluster call Ignite.active(true).");
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"StringEquality", "unchecked"})
    @Override public List<FieldsQueryCursor<List<?>>> querySqlFields(
        String schemaName,
        SqlFieldsQuery qry,
        @Nullable SqlClientContext cliCtx,
        boolean keepBinary,
        boolean failOnMultipleStmts,
        GridQueryCancel cancel
    ) {
        try {
            List<FieldsQueryCursor<List<?>>> res = new ArrayList<>(1);

            SqlFieldsQuery remainingQry = qry;

            while (remainingQry != null) {
                // Parse.
                QueryParserResult parseRes = parser.parse(schemaName, remainingQry, !failOnMultipleStmts);

                remainingQry = parseRes.remainingQuery();

                // Get next command.
                QueryDescriptor newQryDesc = parseRes.queryDescriptor();
                QueryParameters newQryParams = parseRes.queryParameters();

                // Check if there is enough parameters. Batched statements are not checked at this point
                // since they pass parameters differently.
                if (!newQryDesc.batched()) {
                    int qryParamsCnt = F.isEmpty(newQryParams.arguments()) ? 0 : newQryParams.arguments().length;

                    if (qryParamsCnt < parseRes.parametersCount())
                        throw new IgniteSQLException("Invalid number of query parameters [expected=" +
                            parseRes.parametersCount() + ", actual=" + qryParamsCnt + ']');
                }

                // Check if cluster state is valid.
                checkClusterState(parseRes);

                // Execute.
                if (parseRes.isCommand()) {
                    QueryParserResultCommand cmd = parseRes.command();

                    assert cmd != null;

                    FieldsQueryCursor<List<?>> cmdRes = executeCommand(
                        newQryDesc,
                        newQryParams,
                        cliCtx,
                        cmd
                    );

                    res.add(cmdRes);
                }
                else if (parseRes.isDml()) {
                    QueryParserResultDml dml = parseRes.dml();

                    assert dml != null;

                    List<? extends FieldsQueryCursor<List<?>>> dmlRes = executeDml(
                        newQryDesc,
                        newQryParams,
                        dml,
                        cancel
                    );

                    res.addAll(dmlRes);
                }
                else {
                    assert parseRes.isSelect();

                    QueryParserResultSelect select = parseRes.select();

                    assert select != null;

                    List<? extends FieldsQueryCursor<List<?>>> qryRes = executeSelect(
                        newQryDesc,
                        newQryParams,
                        select,
                        keepBinary,
                        cancel
                    );

                    res.addAll(qryRes);
                }
            }

            return res;
        }
        catch (RuntimeException | Error e) {
            GridNearTxLocal tx = ctx.cache().context().tm().tx();

            if (tx != null && tx.mvccSnapshot() != null &&
                (!(e instanceof IgniteSQLException) || /* Parsing errors should not rollback Tx. */
                    ((IgniteSQLException)e).sqlState() != SqlStateCode.PARSING_EXCEPTION)) {

                tx.setRollbackOnly();
            }

            throw e;
        }
    }

    /**
     * Execute an all-ready {@link SqlFieldsQuery}.
     *
     * @param qryDesc Plan key.
     * @param qryParams Parameters.
     * @param dml DML.
     * @param cancel Query cancel state holder.
     * @return Query result.
     */
    private List<? extends FieldsQueryCursor<List<?>>> executeDml(
        QueryDescriptor qryDesc,
        QueryParameters qryParams,
        QueryParserResultDml dml,
        GridQueryCancel cancel
    ) {
        IndexingQueryFilter filter = (qryDesc.local() ? backupFilter(null, qryParams.partitions()) : null);

        Long qryId = registerRunningQuery(qryDesc, cancel);

        Exception failReason = null;

        try {
            if (!dml.mvccEnabled() && !updateInTxAllowed && ctx.cache().context().tm().inUserTx()) {
                throw new IgniteSQLException("DML statements are not allowed inside a transaction over " +
                    "cache(s) with TRANSACTIONAL atomicity mode (change atomicity mode to " +
                    "TRANSACTIONAL_SNAPSHOT or disable this error message with system property " +
                    "\"-DIGNITE_ALLOW_DML_INSIDE_TRANSACTION=true\")");
            }

            if (!qryDesc.local()) {
                return executeUpdateDistributed(
                    qryDesc,
                    qryParams,
                    dml,
                    cancel
                );
            }
            else {
                UpdateResult updRes = executeUpdate(
                    qryDesc,
                    qryParams,
                    dml,
                    true,
                    filter,
                    cancel
                );

                return singletonList(new QueryCursorImpl<>(new Iterable<List<?>>() {
                    @SuppressWarnings("NullableProblems")
                    @Override public Iterator<List<?>> iterator() {
                        return new IgniteSingletonIterator<>(singletonList(updRes.counter()));
                    }
                }, cancel));
            }
        }
        catch (IgniteException e) {
            failReason = e;

            throw e;
        }
        catch (IgniteCheckedException e) {
            failReason = e;

            IgniteClusterReadOnlyException roEx = X.cause(e, IgniteClusterReadOnlyException.class);

            if (roEx != null) {
                throw new IgniteSQLException(
                    "Failed to execute DML statement. Cluster in read-only mode [stmt=" + qryDesc.sql() +
                    ", params=" + Arrays.deepToString(qryParams.arguments()) + "]",
                    IgniteQueryErrorCode.CLUSTER_READ_ONLY_MODE_ENABLED,
                    e
                );
            }

            throw new IgniteSQLException("Failed to execute DML statement [stmt=" + qryDesc.sql() +
                ", params=" + Arrays.deepToString(qryParams.arguments()) + "]", e);
        }
        finally {
            runningQryMgr.unregister(qryId, failReason);
        }
    }

    /**
     * Execute an all-ready {@link SqlFieldsQuery}.
     *
     * @param qryDesc Plan key.
     * @param qryParams Parameters.
     * @param select Select.
     * @param keepBinary Whether binary objects must not be deserialized automatically.
     * @param cancel Query cancel state holder.
     * @return Query result.
     */
    private List<? extends FieldsQueryCursor<List<?>>> executeSelect(
        QueryDescriptor qryDesc,
        QueryParameters qryParams,
        QueryParserResultSelect select,
        boolean keepBinary,
        GridQueryCancel cancel
    ) {
        assert cancel != null;

        // Register query.
        Long qryId = registerRunningQuery(qryDesc, cancel);

        try {
            GridNearTxLocal tx = null;
            MvccQueryTracker tracker = null;
            GridCacheContext mvccCctx = null;

            boolean inTx = false;

            if (select.mvccEnabled()) {
                mvccCctx = ctx.cache().context().cacheContext(select.mvccCacheId());

                if (mvccCctx == null)
                    throw new IgniteCheckedException("Cache has been stopped concurrently [cacheId=" +
                        select.mvccCacheId() + ']');

                boolean autoStartTx = !qryParams.autoCommit() && tx(ctx) == null;

                // Start new user tx in case of autocommit == false.
                if (autoStartTx)
                    txStart(ctx, qryParams.timeout());

                tx = tx(ctx);

                checkActive(tx);

                inTx = tx != null;

                tracker = MvccUtils.mvccTracker(mvccCctx, tx);
            }

            int timeout = operationTimeout(qryParams.timeout(), tx);

            Iterable<List<?>> iter = executeSelect0(
                qryDesc,
                qryParams,
                select,
                keepBinary,
                tracker,
                cancel,
                inTx,
                timeout);

            // Execute SELECT FOR UPDATE if needed.
            if (select.forUpdate() && inTx)
                iter = lockSelectedRows(iter, mvccCctx, timeout, qryParams.pageSize());

            QueryCursorImpl<List<?>> cursor = qryId != null
                ? new RegisteredQueryCursor<>(iter, cancel, runningQueryManager(), qryId)
                : new QueryCursorImpl<>(iter, cancel);

            cursor.fieldsMeta(select.meta());

            cursor.partitionResult(select.twoStepQuery() != null ? select.twoStepQuery().derivedPartitions(): null);

            return singletonList(cursor);
        }
        catch (Exception e) {
            runningQryMgr.unregister(qryId, e);

            if (e instanceof IgniteCheckedException)
                throw U.convertException((IgniteCheckedException)e);

            if (e instanceof RuntimeException)
                throw (RuntimeException)e;

            throw new IgniteSQLException("Failed to execute SELECT statement: " + qryDesc.sql(), e);
        }
    }

    /**
     * Execute SELECT statement for DML.
     *
     * @param schema Schema.
     * @param selectQry Select query.
     * @param mvccTracker MVCC tracker.
     * @param cancel Cancel.
     * @param timeout Timeout.
     * @return Fields query.
     * @throws IgniteCheckedException On error.
     */
    private QueryCursorImpl<List<?>> executeSelectForDml(
        String schema,
        SqlFieldsQuery selectQry,
        MvccQueryTracker mvccTracker,
        GridQueryCancel cancel,
        int timeout
    ) throws IgniteCheckedException {
        QueryParserResult parseRes = parser.parse(schema, selectQry, false);

        QueryParserResultSelect select = parseRes.select();

        assert select != null;

        Iterable<List<?>> iter = executeSelect0(
            parseRes.queryDescriptor(),
            parseRes.queryParameters(),
            select,
            true,
            mvccTracker,
            cancel,
            false,
            timeout
        );

        QueryCursorImpl<List<?>> cursor = new QueryCursorImpl<>(iter, cancel);

        cursor.fieldsMeta(select.meta());

        cursor.partitionResult(select.twoStepQuery() != null ? select.twoStepQuery().derivedPartitions(): null);

        return cursor;
    }

    /**
     * Execute an all-ready {@link SqlFieldsQuery}.
     *
     * @param qryDesc Plan key.
     * @param qryParams Parameters.
     * @param select Select.
     * @param keepBinary Whether binary objects must not be deserialized automatically.
     * @param mvccTracker MVCC tracker.
     * @param cancel Query cancel state holder.
     * @param inTx Flag whether query is executed within transaction.
     * @param timeout Timeout.
     * @return Query result.
     * @throws IgniteCheckedException On error.
     */
    private Iterable<List<?>> executeSelect0(
        QueryDescriptor qryDesc,
        QueryParameters qryParams,
        QueryParserResultSelect select,
        boolean keepBinary,
        MvccQueryTracker mvccTracker,
        GridQueryCancel cancel,
        boolean inTx,
        int timeout
    ) throws IgniteCheckedException {
        assert !select.mvccEnabled() || mvccTracker != null;

        // Check security.
        if (ctx.security().enabled())
            checkSecurity(select.cacheIds());

        Iterable<List<?>> iter;

        if (select.splitNeeded()) {
            // Distributed query.
            GridCacheTwoStepQuery twoStepQry = select.forUpdate() && inTx ?
                select.forUpdateTwoStepQuery() : select.twoStepQuery();

            assert twoStepQry != null;

            iter = executeSelectDistributed(
                qryDesc,
                qryParams,
                twoStepQry,
                keepBinary,
                mvccTracker,
                cancel,
                timeout
            );
        }
        else {
            // Local query.
            IndexingQueryFilter filter = (qryDesc.local() ? backupFilter(null, qryParams.partitions()) : null);

            GridQueryFieldsResult res = executeSelectLocal(
                qryDesc,
                qryParams,
                select,
                filter,
                mvccTracker,
                cancel,
                inTx,
                timeout
            );

            iter = () -> {
                try {
                    return new GridQueryCacheObjectsIterator(res.iterator(), objectContext(), keepBinary);
                }
                catch (IgniteCheckedException | IgniteSQLException e) {
                    throw new CacheException(e);
                }
            };
        }

        return iter;
    }

    /**
     * Locks rows from query cursor and returns the select result.
     *
     * @param cur Query cursor.
     * @param cctx Cache context.
     * @param pageSize Page size.
     * @param timeout Timeout.
     * @return Query results cursor.
     */
    private Iterable<List<?>> lockSelectedRows(Iterable<List<?>> cur, GridCacheContext cctx, int pageSize, long timeout) {
        assert cctx != null && cctx.mvccEnabled();

        GridNearTxLocal tx = tx(ctx);

        if (tx == null)
            throw new IgniteSQLException("Failed to perform SELECT FOR UPDATE operation: transaction has already finished.");

        Collection<List<?>> rowsCache = new ArrayList<>();

        UpdateSourceIterator srcIt = new UpdateSourceIterator<KeyCacheObject>() {
            private Iterator<List<?>> it = cur.iterator();

            @Override public EnlistOperation operation() {
                return EnlistOperation.LOCK;
            }

            @Override public boolean hasNextX() throws IgniteCheckedException {
                return it.hasNext();
            }

            @Override public KeyCacheObject nextX() throws IgniteCheckedException {
                List<?> res = it.next();

                // nextX() can be called from the different threads.
                synchronized (rowsCache) {
                    rowsCache.add(res.subList(0, res.size() - 1));

                    if (rowsCache.size() > TX_SIZE_THRESHOLD) {
                        throw new IgniteCheckedException("Too many rows are locked by SELECT FOR UPDATE statement. " +
                            "Consider locking fewer keys or increase the limit by setting a " +
                            IGNITE_MVCC_TX_SIZE_CACHING_THRESHOLD + " system property. Current value is " +
                            TX_SIZE_THRESHOLD + " rows.");
                    }
                }

                // The last column is expected to be a _key.
                return cctx.toCacheKeyObject(res.get(res.size() - 1));
            }
        };

        IgniteInternalFuture<Long> fut = tx.updateAsync(cctx, srcIt, pageSize,
            timeout, true);

        try {
            fut.get();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }

        return rowsCache;
    }

    /**
     * Register running query.
     *
     * @param qryDesc Query descriptor.
     * @param cancel Query cancel state holder.
     * @return Id of registered query or {@code null} if query wasn't registered.
     */
    private Long registerRunningQuery(QueryDescriptor qryDesc, GridQueryCancel cancel) {
        return runningQryMgr.register(
            qryDesc.sql(),
            GridCacheQueryType.SQL_FIELDS,
            qryDesc.schemaName(),
            qryDesc.local(),
            cancel
        );
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
                ctx.security().authorize(desc.cacheName(), SecurityPermission.CACHE_READ);
        }
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

        boolean loc = true;

        final boolean replicated = U.isFlagSet(flags, GridH2QueryRequest.FLAG_REPLICATED);

        GridCacheContext<?, ?> cctx0;

        if (!replicated
            && !F.isEmpty(ids)
            && (cctx0 = CU.firstPartitioned(cctx.shared(), ids)) != null
            && cctx0.config().getQueryParallelism() > 1) {
            fldsQry.setDistributedJoins(true);

            loc = false;
        }

        QueryParserResult parseRes = parser.parse(schema, fldsQry, false);

        assert parseRes.remainingQuery() == null;

        QueryParserResultDml dml = parseRes.dml();

        assert dml != null;

        IndexingQueryFilter filter = backupFilter(topVer, parts);

        UpdatePlan plan = dml.plan();

        GridCacheContext planCctx = plan.cacheContext();

        // Force keepBinary for operation context to avoid binary deserialization inside entry processor
        DmlUtils.setKeepBinaryContext(planCctx);

        SqlFieldsQuery selectFieldsQry = new SqlFieldsQuery(plan.selectQuery(), fldsQry.isCollocated())
            .setArgs(fldsQry.getArgs())
            .setDistributedJoins(fldsQry.isDistributedJoins())
            .setEnforceJoinOrder(fldsQry.isEnforceJoinOrder())
            .setLocal(fldsQry.isLocal())
            .setPageSize(fldsQry.getPageSize())
            .setTimeout(fldsQry.getTimeout(), TimeUnit.MILLISECONDS);

        QueryCursorImpl<List<?>> cur;

        // Do a two-step query only if locality flag is not set AND if plan's SELECT corresponds to an actual
        // sub-query and not some dummy stuff like "select 1, 2, 3;"
        if (!loc && !plan.isLocalSubquery()) {
            cur = executeSelectForDml(
                schema,
                selectFieldsQry,
                new StaticMvccQueryTracker(planCctx, mvccSnapshot),
                cancel,
                timeout
            );
        }
        else {
            selectFieldsQry.setLocal(true);

            QueryParserResult selectParseRes = parser.parse(schema, selectFieldsQry, false);

            GridQueryFieldsResult res = executeSelectLocal(
                selectParseRes.queryDescriptor(),
                selectParseRes.queryParameters(),
                selectParseRes.select(),
                filter,
                new StaticMvccQueryTracker(planCctx, mvccSnapshot),
                cancel,
                true,
                timeout
            );

            cur = new QueryCursorImpl<>(new Iterable<List<?>>() {
                @SuppressWarnings("NullableProblems")
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
     *
     * @param qryDesc Query descriptor.
     * @param qryParams Query parameters.
     * @param twoStepQry Two-step query.
     * @param keepBinary Keep binary flag.
     * @param mvccTracker Query tracker.
     * @param cancel Cancel handler.
     * @param timeout Timeout.
     * @return Cursor representing distributed query result.
     */
    @SuppressWarnings("IfMayBeConditional")
    private Iterable<List<?>> executeSelectDistributed(
        final QueryDescriptor qryDesc,
        final QueryParameters qryParams,
        final GridCacheTwoStepQuery twoStepQry,
        final boolean keepBinary,
        MvccQueryTracker mvccTracker,
        final GridQueryCancel cancel,
        int timeout
    ) {
        // When explicit partitions are set, there must be an owning cache they should be applied to.
        PartitionResult derivedParts = twoStepQry.derivedPartitions();

        final int parts[] = PartitionResult.calculatePartitions(
            qryParams.partitions(),
            derivedParts,
            qryParams.arguments()
        );

        Iterable<List<?>> iter;

        if (parts != null && parts.length == 0) {
            iter = new Iterable<List<?>>() {
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
            };
        }
        else {
            assert !twoStepQry.mvccEnabled() || !F.isEmpty(twoStepQry.cacheIds());
            assert twoStepQry.mvccEnabled() == (mvccTracker != null);

            iter = new Iterable<List<?>>() {
                @SuppressWarnings("NullableProblems")
                @Override public Iterator<List<?>> iterator() {
                    try {
                        return IgniteH2Indexing.this.rdcQryExec.query(
                            qryDesc.schemaName(),
                            twoStepQry,
                            keepBinary,
                            qryDesc.enforceJoinOrder(),
                            timeout,
                            cancel,
                            qryParams.arguments(),
                            parts,
                            qryParams.lazy(),
                            mvccTracker,
                            qryParams.dataPageScanEnabled(),
                            qryParams.pageSize()
                        );
                    }
                    catch (Throwable e) {
                        if (mvccTracker != null)
                            mvccTracker.onDone();

                        throw e;
                    }
                }
            };
        }

        return iter;
    }

    /**
     * Executes DML request on map node. Happens only for "skip reducer" mode.
     *
     * @param schemaName Schema name.
     * @param qry Query.
     * @param filter Filter.
     * @param cancel Cancel state.
     * @param loc Locality flag.
     * @return Update result.
     * @throws IgniteCheckedException if failed.
     */
    public UpdateResult executeUpdateOnDataNode(
        String schemaName,
        SqlFieldsQuery qry,
        IndexingQueryFilter filter,
        GridQueryCancel cancel,
        boolean loc
    ) throws IgniteCheckedException {
        QueryParserResult parseRes = parser.parse(schemaName, qry, false);

        assert parseRes.remainingQuery() == null;

        QueryParserResultDml dml = parseRes.dml();

        assert dml != null;

        return executeUpdate(
            parseRes.queryDescriptor(),
            parseRes.queryParameters(),
            dml,
            loc,
            filter,
            cancel
        );
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
        validateTypeDescriptor(type);
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
    @Override public void closeCacheOnClient(String cacheName) {
        GridCacheContextInfo cacheInfo = registeredCacheInfo(cacheName);

        // Only for SQL caches.
        if (cacheInfo != null) {
            parser.clearCache();

            cacheInfo.clearCacheContext();
        }
    }

    /** {@inheritDoc} */
    @Override public String schema(String cacheName) {
        return schemaMgr.schemaName(cacheName);
    }

    /** {@inheritDoc} */
    @Override public Set<String> schemasNames(){
        return schemaMgr.schemaNames();
    }

    /** {@inheritDoc} */
    @Override public Collection<TableInformation> tablesInformation(String schemaNamePtrn, String tblNamePtrn,
        String... tblTypes) {
        Set<String> types = F.isEmpty(tblTypes) ? Collections.emptySet() : new HashSet<>(Arrays.asList(tblTypes));

        Collection<TableInformation> infos = new ArrayList<>();

        boolean allTypes = F.isEmpty(tblTypes);

        if (allTypes || types.contains(TableType.TABLE.name())) {
            schemaMgr.dataTables().stream()
                .filter(t -> matches(t.getSchema().getName(), schemaNamePtrn))
                .filter(t -> matches(t.getName(), tblNamePtrn))
                .map(t -> {
                    int cacheGrpId = t.cacheInfo().groupId();

                    CacheGroupDescriptor cacheGrpDesc = ctx.cache().cacheGroupDescriptors().get(cacheGrpId);

                    // We should skip table in case regarding cache group has been removed.
                    if (cacheGrpDesc == null)
                        return null;

                    GridQueryTypeDescriptor type = t.rowDescriptor().type();

                    IndexColumn affCol = t.getExplicitAffinityKeyColumn();

                    String affinityKeyCol = affCol != null ? affCol.columnName : null;

                    return new TableInformation(t.getSchema().getName(), t.getName(), TableType.TABLE.name(), cacheGrpId,
                        cacheGrpDesc.cacheOrGroupName(), t.cacheId(), t.cacheName(), affinityKeyCol,
                        type.keyFieldAlias(), type.valueFieldAlias(), type.keyTypeName(), type.valueTypeName());
                })
                .filter(Objects::nonNull)
                .forEach(infos::add);
        }

        if ((allTypes || types.contains(TableType.VIEW.name()))
            && matches(QueryUtils.SCHEMA_SYS, schemaNamePtrn)) {
            schemaMgr.systemViews().stream()
                .filter(t -> matches(t.getTableName(), tblNamePtrn))
                .map(v -> new TableInformation(QueryUtils.SCHEMA_SYS, v.getTableName(), TableType.VIEW.name()))
                .forEach(infos::add);
        }

        return infos;
    }

    /** {@inheritDoc} */
    @Override public Collection<ColumnInformation> columnsInformation(String schemaNamePtrn, String tblNamePtrn,
        String colNamePtrn) {
        Collection<ColumnInformation> infos = new ArrayList<>();

        // Gather information about tables.
        schemaMgr.dataTables().stream()
            .filter(t -> matches(t.getSchema().getName(), schemaNamePtrn))
            .filter(t -> matches(t.getName(), tblNamePtrn))
            .flatMap(
                tbl -> {
                    IndexColumn affCol = tbl.getAffinityKeyColumn();

                    return Stream.of(tbl.getColumns())
                        .filter(Column::getVisible)
                        .filter(c -> matches(c.getName(), colNamePtrn))
                        .map(c -> {
                            GridQueryProperty prop = tbl.rowDescriptor().type().property(c.getName());

                            boolean isAff = affCol != null && c.getColumnId() == affCol.column.getColumnId();

                            return new ColumnInformation(
                                c.getColumnId() - QueryUtils.DEFAULT_COLUMNS_COUNT + 1,
                                tbl.getSchema().getName(),
                                tbl.getName(),
                                c.getName(),
                                prop.type(),
                                c.isNullable(),
                                prop.defaultValue(),
                                prop.precision(),
                                prop.scale(),
                                isAff);
                        });
                }
            ).forEach(infos::add);

        // Gather information about system views.
        if (matches(QueryUtils.SCHEMA_SYS, schemaNamePtrn)) {
            schemaMgr.systemViews().stream()
                .filter(v -> matches(v.getTableName(), tblNamePtrn))
                .flatMap(
                    view ->
                        Stream.of(view.getColumns())
                            .filter(c -> matches(c.getName(), colNamePtrn))
                            .map(c -> new ColumnInformation(
                                c.getColumnId() + 1,
                                QueryUtils.SCHEMA_SYS,
                                view.getTableName(),
                                c.getName(),
                                IgniteUtils.classForName(DataType.getTypeClassName(c.getType()), Object.class),
                                c.isNullable(),
                                null,
                                (int)c.getPrecision(),
                                c.getScale(),
                                false))
                ).forEach(infos::add);
        }

        return infos;
    }

    /** {@inheritDoc} */
    @Override public boolean isStreamableInsertStatement(String schemaName, SqlFieldsQuery qry) throws SQLException{
        QueryParserResult parsed = parser.parse(schemaName, qry, true);

        return parsed.isDml() && parsed.dml().streamable() && parsed.remainingQuery() == null;
    }

    /** {@inheritDoc} */
    @Override public GridQueryRowCacheCleaner rowCacheCleaner(int grpId) {
        return rowCache.forGroup(grpId);
    }

    /** {@inheritDoc} */
    @Override public void markAsRebuildNeeded(GridCacheContext cctx) {
        assert cctx.group().persistenceEnabled(): cctx;

        markIndexRebuild(cctx.name(), true);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> rebuildIndexesFromHash(GridCacheContext cctx) {
        assert nonNull(cctx);

        // No data in fresh in-memory cache.
        if (!cctx.group().persistenceEnabled())
            return null;

        IgnitePageStoreManager pageStore = cctx.shared().pageStore();

        assert nonNull(pageStore);

        SchemaIndexCacheVisitorClosure clo;

        String cacheName = cctx.name();

        if (!pageStore.hasIndexStore(cctx.groupId())) {
            // If there are no index store, rebuild all indexes.
            clo = new IndexRebuildFullClosure(cctx.queries(), cctx.mvccEnabled());
        }
        else {
            // Otherwise iterate over tables looking for missing indexes.
            IndexRebuildPartialClosure clo0 = new IndexRebuildPartialClosure();

            for (H2TableDescriptor tblDesc : schemaMgr.tablesForCache(cacheName)) {
                GridH2Table tbl = tblDesc.table();

                assert nonNull(tbl);

                tbl.collectIndexesForPartialRebuild(clo0);
            }

            if (clo0.hasIndexes())
                clo = clo0;
            else
                return null;
        }

        // Closure prepared, do rebuild.
        markIndexRebuild(cacheName, true);

        GridFutureAdapter<Void> rebuildCacheIdxFut = new GridFutureAdapter<>();

        rebuildCacheIdxFut.listen(fut -> {
            Throwable err = fut.error();

            if (isNull(err)) {
                try {
                    markIndexRebuild(cacheName, false);
                }
                catch (Throwable t) {
                    err = t;

                    rebuildCacheIdxFut.onDone(t);
                }
            }

            if (nonNull(err))
                U.error(log, "Failed to rebuild indexes for cache: " + cacheName, err);
        });

        rebuildIndexesFromHash0(cctx, clo, rebuildCacheIdxFut);

        return rebuildCacheIdxFut;
    }

    /**
     * Do index rebuild.
     *
     * @param cctx Cache context.
     * @param clo Closure.
     * @param rebuildIdxFut Future for rebuild indexes.
     */
    protected void rebuildIndexesFromHash0(
        GridCacheContext cctx,
        SchemaIndexCacheVisitorClosure clo,
        GridFutureAdapter<Void> rebuildIdxFut
    ) {
        new SchemaIndexCacheVisitorImpl(cctx, null, null, rebuildIdxFut).visit(clo);
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

        longRunningQryMgr = new LongRunningQueryManager(ctx);

        parser = new QueryParser(this, connMgr);

        schemaMgr = new SchemaManager(ctx, connMgr);
        schemaMgr.start(ctx.config().getSqlSchemas());

        nodeId = ctx.localNodeId();
        marshaller = ctx.config().getMarshaller();

        mapQryExec = new GridMapQueryExecutor();
        rdcQryExec = new GridReduceQueryExecutor();

        mapQryExec.start(ctx, this);
        rdcQryExec.start(ctx, this);

        discoLsnr = evt -> {
            mapQryExec.onNodeLeft((DiscoveryEvent)evt);
            rdcQryExec.onNodeLeft((DiscoveryEvent)evt);
        };

        ctx.event().addLocalEventListener(discoLsnr, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);

        qryLsnr = (nodeId, msg, plc) -> onMessage(nodeId, msg);

        ctx.io().addMessageListener(GridTopic.TOPIC_QUERY, qryLsnr);

        runningQryMgr = new RunningQueryManager(ctx);
        partExtractor = new PartitionExtractor(new H2PartitionResolver(this), ctx);

        cmdProc = new CommandProcessor(ctx, schemaMgr, this);
        cmdProc.start();

        if (JdbcUtils.serializer != null)
            U.warn(log, "Custom H2 serialization is already configured, will override.");

        JdbcUtils.serializer = h2Serializer();
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    public void onMessage(UUID nodeId, Object msg) {
        assert msg != null;

        ClusterNode node = ctx.discovery().node(nodeId);

        if (node == null)
            return; // Node left, ignore.

        if (!busyLock.enterBusy())
            return;

        try {
            if (msg instanceof GridCacheQueryMarshallable)
                ((GridCacheQueryMarshallable)msg).unmarshall(ctx.config().getMarshaller(), ctx);

            try {
                boolean processed = true;

                if (msg instanceof GridQueryNextPageRequest)
                    mapQueryExecutor().onNextPageRequest(node, (GridQueryNextPageRequest)msg);
                else if (msg instanceof GridQueryNextPageResponse)
                    reduceQueryExecutor().onNextPage(node, (GridQueryNextPageResponse)msg);
                else if (msg instanceof GridH2QueryRequest)
                    mapQueryExecutor().onQueryRequest(node, (GridH2QueryRequest)msg);
                else if (msg instanceof GridH2DmlRequest)
                    mapQueryExecutor().onDmlRequest(node, (GridH2DmlRequest)msg);
                else if (msg instanceof GridH2DmlResponse)
                    reduceQueryExecutor().onDmlResponse(node, (GridH2DmlResponse)msg);
                else if (msg instanceof GridQueryFailResponse)
                    reduceQueryExecutor().onFail(node, (GridQueryFailResponse)msg);
                else if (msg instanceof GridQueryCancelRequest)
                    mapQueryExecutor().onCancel(node, (GridQueryCancelRequest)msg);
                else
                    processed = false;

                if (processed && log.isDebugEnabled())
                    log.debug("Processed message " + msg.getClass().getName() + ": " + nodeId + "->" + ctx.localNodeId() + " " + msg);
            }
            catch (Throwable th) {
                U.error(log, "Failed to process message: " + msg, th);
            }
        }
        finally {
            busyLock.leaveBusy();
        }
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
        return new H2JavaObjectSerializer(ctx);
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        if (log.isDebugEnabled())
            log.debug("Stopping cache query index...");

        mapQryExec.stop();

        qryCtxRegistry.clearSharedOnLocalNodeStop();

        runningQryMgr.stop();
        schemaMgr.stop();
        longRunningQryMgr.stop();
        connMgr.stop();

        cmdProc.stop();

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

        // Drop schema (needs to be called after callback to DML processor because the latter depends on schema).
        schemaMgr.onCacheDestroyed(cacheName, rmvIdx);

        // Unregister connection.
        connMgr.onCacheDestroyed();

        // Clear query cache.
        clearPlanCache();
    }

    /** {@inheritDoc} */
    @Override public void destroyOrphanIndex(
        RootPage page,
        String indexName,
        int grpId,
        PageMemory pageMemory,
        final GridAtomicLong removeId,
        final ReuseList reuseList,
        boolean mvccEnabled
    ) throws IgniteCheckedException {
        assert ctx.cache().context().database().checkpointLockIsHeldByThread();

        long metaPageId = page.pageId().pageId();

        int inlineSize = getInlineSize(page, grpId, pageMemory);

        String grpName = ctx.cache().cacheGroup(grpId).cacheOrGroupName();

        PageLockListener lockLsnr = ctx.cache().context().diagnostic()
            .pageLockTracker().createPageLockTracker(grpName + "IndexTree##" + indexName);

        BPlusTree<H2Row, H2Row> tree = new BPlusTree<H2Row, H2Row>(
            indexName,
            grpId,
            grpName,
            pageMemory,
            ctx.cache().context().wal(),
            removeId,
            metaPageId,
            reuseList,
            H2ExtrasInnerIO.getVersions(inlineSize, mvccEnabled),
            H2ExtrasLeafIO.getVersions(inlineSize, mvccEnabled),
            ctx.failure(),
            lockLsnr
        ) {
            @Override protected int compare(BPlusIO io, long pageAddr, int idx, H2Row row) {
                throw new AssertionError();
            }

            @Override public H2Row getRow(BPlusIO io, long pageAddr, int idx, Object x) {
                throw new AssertionError();
            }
        };

        tree.destroy();
    }

    /**
     * @param page Root page.
     * @param grpId Cache group id.
     * @param pageMemory Page memory.
     * @return Inline size.
     * @throws IgniteCheckedException If something went wrong.
     */
    private int getInlineSize(RootPage page, int grpId, PageMemory pageMemory) throws IgniteCheckedException {
        long metaPageId = page.pageId().pageId();

        final long metaPage = pageMemory.acquirePage(grpId, metaPageId);

        try {
            long pageAddr = pageMemory.readLock(grpId, metaPageId, metaPage); // Meta can't be removed.

            assert pageAddr != 0 : "Failed to read lock meta page [metaPageId=" +
                U.hexLong(metaPageId) + ']';

            try {
                BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(pageAddr);

                return io.getInlineSize(pageAddr);
            }
            finally {
                pageMemory.readUnlock(grpId, metaPageId, metaPage);
            }
        }
        finally {
            pageMemory.releasePage(grpId, metaPageId, metaPage);
        }
    }

    /**
     * Remove all cached queries from cached two-steps queries.
     */
    private void clearPlanCache() {
        parser.clearCache();
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
        ctx.cache().context().exchange().affinityReadyFuture(topVer).get();
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {
        rdcQryExec.onDisconnected(reconnectFut);

        cmdProc.onDisconnected();
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
        connMgr.onKernalStop();

        ctx.io().removeMessageListener(GridTopic.TOPIC_QUERY, qryLsnr);
        ctx.event().removeLocalEventListener(discoLsnr);
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
     * @return Parser.
     */
    public QueryParser parser() {
        return parser;
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
     * @param qryDesc Query descriptor.
     * @param qryParams Query parameters.
     * @param dml DML statement.
     * @param cancel Query cancel.
     * @return Update result wrapped into {@link GridQueryFieldsResult}
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    private List<QueryCursorImpl<List<?>>> executeUpdateDistributed(
        QueryDescriptor qryDesc,
        QueryParameters qryParams,
        QueryParserResultDml dml,
        GridQueryCancel cancel
    ) throws IgniteCheckedException {
        if (qryDesc.batched()) {
            Collection<UpdateResult> ress;

            List<Object[]> argss = qryParams.batchedArguments();

            UpdatePlan plan = dml.plan();

            GridCacheContext<?, ?> cctx = plan.cacheContext();

            // For MVCC case, let's enlist batch elements one by one.
            if (plan.hasRows() && plan.mode() == UpdateMode.INSERT && !cctx.mvccEnabled()) {
                CacheOperationContext opCtx = DmlUtils.setKeepBinaryContext(cctx);

                try {
                    List<List<List<?>>> cur = plan.createRows(argss);

                    //TODO: IGNITE-11176 - Need to support cancellation
                    ress = DmlUtils.processSelectResultBatched(plan, cur, qryParams.updateBatchSize());
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
                    UpdateResult res;

                    try {
                        res = executeUpdate(
                            qryDesc,
                            qryParams.toSingleBatchedArguments(args),
                            dml,
                            false,
                            null,
                            cancel
                        );

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

                QueryCursorImpl<List<?>> resCur = (QueryCursorImpl<List<?>>)new QueryCursorImpl(singletonList
                    (singletonList(res.counter())), cancel, false);

                resCur.fieldsMeta(UPDATE_RESULT_META);

                resCurs.add(resCur);
            }

            return resCurs;
        }
        else {
            UpdateResult res = executeUpdate(
                qryDesc,
                qryParams,
                dml,
                false,
                null,
                cancel
            );

            res.throwIfError();

            QueryCursorImpl<List<?>> resCur = (QueryCursorImpl<List<?>>)new QueryCursorImpl(singletonList
                (singletonList(res.counter())), cancel, false);

            resCur.fieldsMeta(UPDATE_RESULT_META);

            resCur.partitionResult(res.partitionResult());

            return singletonList(resCur);
        }
    }

    /**
     * Execute DML statement, possibly with few re-attempts in case of concurrent data modifications.
     *
     * @param qryDesc Query descriptor.
     * @param qryParams Query parameters.
     * @param dml DML command.
     * @param loc Query locality flag.
     * @param filters Cache name and key filter.
     * @param cancel Cancel.
     * @return Update result (modified items count and failed keys).
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("IfMayBeConditional")
    private UpdateResult executeUpdate(
        QueryDescriptor qryDesc,
        QueryParameters qryParams,
        QueryParserResultDml dml,
        boolean loc,
        IndexingQueryFilter filters,
        GridQueryCancel cancel
    ) throws IgniteCheckedException {
        Object[] errKeys = null;

        long items = 0;

        PartitionResult partRes = null;

        GridCacheContext<?, ?> cctx = dml.plan().cacheContext();

        boolean transactional = cctx != null && cctx.mvccEnabled();

        int maxRetryCnt = transactional ? 1 : DFLT_UPDATE_RERUN_ATTEMPTS;

        for (int i = 0; i < maxRetryCnt; i++) {
            CacheOperationContext opCtx = cctx != null ? DmlUtils.setKeepBinaryContext(cctx) : null;

            UpdateResult r;

            try {
                if (transactional)
                    r = executeUpdateTransactional(
                        qryDesc,
                        qryParams,
                        dml,
                        loc,
                        cancel
                    );
                else
                    r = executeUpdateNonTransactional(
                        qryDesc,
                        qryParams,
                        dml,
                        loc,
                        filters,
                        cancel
                    );
            }
            finally {
                if (opCtx != null)
                    DmlUtils.restoreKeepBinaryContext(cctx, opCtx);
            }

            items += r.counter();
            errKeys = r.errorKeys();
            partRes = r.partitionResult();

            if (F.isEmpty(errKeys))
                break;
        }

        if (F.isEmpty(errKeys) && partRes == null) {
            if (items == 1L)
                return UpdateResult.ONE;
            else if (items == 0L)
                return UpdateResult.ZERO;
        }

        return new UpdateResult(items, errKeys, partRes);
    }

    /**
     * Execute update in non-transactional mode.
     *
     * @param qryDesc Query descriptor.
     * @param qryParams Query parameters.
     * @param dml Plan.
     * @param loc Local flag.
     * @param filters Filters.
     * @param cancel Cancel hook.
     * @return Update result.
     * @throws IgniteCheckedException If failed.
     */
    private UpdateResult executeUpdateNonTransactional(
        QueryDescriptor qryDesc,
        QueryParameters qryParams,
        QueryParserResultDml dml,
        boolean loc,
        IndexingQueryFilter filters,
        GridQueryCancel cancel
    ) throws IgniteCheckedException {
        UpdatePlan plan = dml.plan();

        UpdateResult fastUpdateRes = plan.processFast(qryParams.arguments());

        if (fastUpdateRes != null)
            return fastUpdateRes;

        DmlDistributedPlanInfo distributedPlan = loc ? null : plan.distributedPlan();

        if (distributedPlan != null) {
            if (cancel == null)
                cancel = new GridQueryCancel();

            UpdateResult result = rdcQryExec.update(
                qryDesc.schemaName(),
                distributedPlan.getCacheIds(),
                qryDesc.sql(),
                qryParams.arguments(),
                qryDesc.enforceJoinOrder(),
                qryParams.pageSize(),
                qryParams.timeout(),
                qryParams.partitions(),
                distributedPlan.isReplicatedOnly(),
                cancel
            );

            // Null is returned in case not all nodes support distributed DML.
            if (result != null)
                return result;
        }

        SqlFieldsQuery selectFieldsQry = new SqlFieldsQuery(plan.selectQuery(), qryDesc.collocated())
            .setArgs(qryParams.arguments())
            .setDistributedJoins(qryDesc.distributedJoins())
            .setEnforceJoinOrder(qryDesc.enforceJoinOrder())
            .setLocal(qryDesc.local())
            .setPageSize(qryParams.pageSize())
            .setTimeout(qryParams.timeout(), TimeUnit.MILLISECONDS);

        Iterable<List<?>> cur;

        // Do a two-step query only if locality flag is not set AND if plan's SELECT corresponds to an actual
        // sub-query and not some dummy stuff like "select 1, 2, 3;"
        if (!loc && !plan.isLocalSubquery()) {
            assert !F.isEmpty(plan.selectQuery());

            cur = executeSelectForDml(
                qryDesc.schemaName(),
                selectFieldsQry,
                null,
                cancel,
                qryParams.timeout()
            );
        }
        else if (plan.hasRows())
            cur = plan.createRows(qryParams.arguments());
        else {
            selectFieldsQry.setLocal(true);

            QueryParserResult selectParseRes = parser.parse(qryDesc.schemaName(), selectFieldsQry, false);

            final GridQueryFieldsResult res = executeSelectLocal(
                selectParseRes.queryDescriptor(),
                selectParseRes.queryParameters(),
                selectParseRes.select(),
                filters,
                null,
                cancel,
                false,
                qryParams.timeout()
            );

            cur = new QueryCursorImpl<>(new Iterable<List<?>>() {
                @SuppressWarnings("NullableProblems")
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

        int pageSize = qryParams.updateBatchSize();

        //TODO: IGNITE-11176 - Need to support cancellation
        return DmlUtils.processSelectResult(plan, cur, pageSize);
    }

    /**
     * Execute update in transactional mode.
     *
     * @param qryDesc Query descriptor.
     * @param qryParams Query parameters.
     * @param dml Plan.
     * @param loc Local flag.
     * @param cancel Cancel hook.
     * @return Update result.
     * @throws IgniteCheckedException If failed.
     */
    private UpdateResult executeUpdateTransactional(
        QueryDescriptor qryDesc,
        QueryParameters qryParams,
        QueryParserResultDml dml,
        boolean loc,
        GridQueryCancel cancel
    ) throws IgniteCheckedException {
        UpdatePlan plan = dml.plan();

        GridCacheContext cctx = plan.cacheContext();

        assert cctx != null;
        assert cctx.transactional();

        GridNearTxLocal tx = tx(ctx);

        boolean implicit = (tx == null);

        boolean commit = implicit && qryParams.autoCommit();

        if (implicit)
            tx = txStart(cctx, qryParams.timeout());

        requestSnapshot(tx);

        try (GridNearTxLocal toCommit = commit ? tx : null) {
            DmlDistributedPlanInfo distributedPlan = loc ? null : plan.distributedPlan();

            long timeout = implicit
                ? tx.remainingTime()
                : operationTimeout(qryParams.timeout(), tx);

            if (cctx.isReplicated() || distributedPlan == null || ((plan.mode() == UpdateMode.INSERT
                || plan.mode() == UpdateMode.MERGE) && !plan.isLocalSubquery())) {

                boolean sequential = true;

                UpdateSourceIterator<?> it;

                if (plan.fastResult()) {
                    IgniteBiTuple row = plan.getFastRow(qryParams.arguments());

                    assert row != null;

                    EnlistOperation op = UpdatePlan.enlistOperation(plan.mode());

                    it = new DmlUpdateSingleEntryIterator<>(op, op.isDeleteOrLock() ? row.getKey() : row);
                }
                else if (plan.hasRows()) {
                    it = new DmlUpdateResultsIterator(
                        UpdatePlan.enlistOperation(plan.mode()),
                        plan,
                        plan.createRows(qryParams.arguments())
                    );
                }
                else {
                    SqlFieldsQuery selectFieldsQry = new SqlFieldsQuery(plan.selectQuery(), qryDesc.collocated())
                        .setArgs(qryParams.arguments())
                        .setDistributedJoins(qryDesc.distributedJoins())
                        .setEnforceJoinOrder(qryDesc.enforceJoinOrder())
                        .setLocal(qryDesc.local())
                        .setPageSize(qryParams.pageSize())
                        .setTimeout((int)timeout, TimeUnit.MILLISECONDS);

                    FieldsQueryCursor<List<?>> cur = executeSelectForDml(
                        qryDesc.schemaName(),
                        selectFieldsQry,
                        MvccUtils.mvccTracker(cctx, tx),
                        cancel,
                        (int)timeout
                    );

                    it = plan.iteratorForTransaction(connMgr, cur);
                }

                //TODO: IGNITE-11176 - Need to support cancellation
                IgniteInternalFuture<Long> fut = tx.updateAsync(
                    cctx,
                    it,
                    qryParams.pageSize(),
                    timeout,
                    sequential
                );

                UpdateResult res = new UpdateResult(fut.get(), X.EMPTY_OBJECT_ARRAY,
                    plan.distributedPlan() != null ? plan.distributedPlan().derivedPartitions() : null);

                if (commit)
                    toCommit.commit();

                return res;
            }

            int[] ids = U.toIntArray(distributedPlan.getCacheIds());

            int flags = 0;

            if (qryDesc.enforceJoinOrder())
                flags |= GridH2QueryRequest.FLAG_ENFORCE_JOIN_ORDER;

            if (distributedPlan.isReplicatedOnly())
                flags |= GridH2QueryRequest.FLAG_REPLICATED;

            flags = GridH2QueryRequest.setDataPageScanEnabled(flags,
                qryParams.dataPageScanEnabled());

            int[] parts = PartitionResult.calculatePartitions(
                qryParams.partitions(),
                distributedPlan.derivedPartitions(),
                qryParams.arguments()
            );

            if (parts != null && parts.length == 0)
                return new UpdateResult(0, X.EMPTY_OBJECT_ARRAY, distributedPlan.derivedPartitions());
            else {
                //TODO: IGNITE-11176 - Need to support cancellation
                IgniteInternalFuture<Long> fut = tx.updateAsync(
                    cctx,
                    ids,
                    parts,
                    qryDesc.schemaName(),
                    qryDesc.sql(),
                    qryParams.arguments(),
                    flags,
                    qryParams.pageSize(),
                    timeout
                );

                UpdateResult res = new UpdateResult(fut.get(), X.EMPTY_OBJECT_ARRAY,
                    distributedPlan.derivedPartitions());

                if (commit)
                    toCommit.commit();

                return res;
            }
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

            U.error(log, "Error during update [localNodeId=" + ctx.localNodeId() + "]", ex);

            throw new IgniteSQLException("Failed to run update. " + ex.getMessage(), ex);
        }
        finally {
            if (commit)
                cctx.tm().resetContext();
        }
    }

    /** {@inheritDoc} */
    @Override public void registerMxBeans(IgniteMBeansManager mbMgr) throws IgniteCheckedException {
        SqlQueryMXBean qryMXBean = new SqlQueryMXBeanImpl(ctx);

        mbMgr.registerMBean("SQL Query", qryMXBean.getClass().getSimpleName(), qryMXBean, SqlQueryMXBean.class);
    }

    /**
     * @return Long running queries manager.
     */
    public LongRunningQueryManager longRunningQueries() {
        return longRunningQryMgr;
    }
}
