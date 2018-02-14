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

package org.apache.ignite.internal.processors.cache.query;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.QueryMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.CacheQueryExecutedEvent;
import org.apache.ignite.events.CacheQueryReadEvent;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.internal.processors.cache.CacheMetricsImpl;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectUtils;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtUnreservedPartitionException;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.apache.ignite.internal.processors.datastructures.GridSetQueryPredicate;
import org.apache.ignite.internal.processors.datastructures.SetItemKey;
import org.apache.ignite.internal.processors.platform.cache.PlatformCacheEntryFilter;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.GridBoundedPriorityQueue;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.GridEmptyCloseableIterator;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.GridSpiCloseableIteratorWrapper;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.IgniteSpiCloseableIterator;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilterImpl;
import org.apache.ignite.spi.indexing.IndexingSpi;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_OBJECT_READ;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridClosureCallMode.BROADCAST;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SCAN;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SPI;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL_FIELDS;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.TEXT;

/**
 * Query and index manager.
 */
@SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
public abstract class GridCacheQueryManager<K, V> extends GridCacheManagerAdapter<K, V> {
    /** Maximum number of query detail metrics to evict at once. */
    private static final int QRY_DETAIL_METRICS_EVICTION_LIMIT = 10_000;

    /** Support 'not null' field constraint since v 2.3.0. */
    private static final IgniteProductVersion NOT_NULLS_SUPPORT_VER = IgniteProductVersion.fromString("2.3.0");

    /** Comparator for priority queue with query detail metrics with priority to new metrics. */
    private static final Comparator<GridCacheQueryDetailMetricsAdapter> QRY_DETAIL_METRICS_PRIORITY_NEW_CMP =
        new Comparator<GridCacheQueryDetailMetricsAdapter>() {
            @Override public int compare(GridCacheQueryDetailMetricsAdapter m1, GridCacheQueryDetailMetricsAdapter m2) {
                return Long.compare(m1.lastStartTime(), m2.lastStartTime());
            }
        };

    /** Comparator for priority queue with query detail metrics with priority to old metrics. */
    private static final Comparator<GridCacheQueryDetailMetricsAdapter> QRY_DETAIL_METRICS_PRIORITY_OLD_CMP =
        new Comparator<GridCacheQueryDetailMetricsAdapter>() {
            @Override public int compare(GridCacheQueryDetailMetricsAdapter m1, GridCacheQueryDetailMetricsAdapter m2) {
                return Long.compare(m2.lastStartTime(), m1.lastStartTime());
            }
        };

    /** Function to merge query detail metrics. */
    private static final BiFunction<
        GridCacheQueryDetailMetricsAdapter,
        GridCacheQueryDetailMetricsAdapter,
        GridCacheQueryDetailMetricsAdapter>
        QRY_DETAIL_METRICS_MERGE_FX = GridCacheQueryDetailMetricsAdapter::aggregate;

    /** Default is @{code true} */
    private final boolean isIndexingSpiAllowsBinary = !IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_UNWRAP_BINARY_FOR_INDEXING_SPI);

    /** */
    private GridQueryProcessor qryProc;

    /** */
    private String cacheName;

    /** */
    private int maxIterCnt;

    /** */
    private volatile GridCacheQueryMetricsAdapter metrics = new GridCacheQueryMetricsAdapter();

    /** */
    private int detailMetricsSz;

    /** */
    private ConcurrentHashMap<GridCacheQueryDetailMetricsKey, GridCacheQueryDetailMetricsAdapter> detailMetrics;

    /** */
    private final ConcurrentMap<UUID, RequestFutureMap> qryIters = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentMap<UUID, Map<Long, GridFutureAdapter<FieldsResult>>> fieldsQryRes =
        new ConcurrentHashMap<>();

    /** */
    private volatile ConcurrentMap<Object, CachedResult<?>> qryResCache = new ConcurrentHashMap<>();

    /** */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Event listener. */
    private GridLocalEventListener lsnr;

    /** */
    private boolean enabled;

    /** */
    private boolean qryProcEnabled;

    /** */
    private AffinityTopologyVersion qryTopVer;

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        CacheConfiguration ccfg = cctx.config();

        qryProcEnabled = QueryUtils.isEnabled(ccfg);

        qryProc = cctx.kernalContext().query();

        cacheName = cctx.name();

        enabled = qryProcEnabled || (isIndexingSpiEnabled() && !CU.isSystemCache(cacheName));

        maxIterCnt = ccfg.getMaxQueryIteratorsCount();

        detailMetricsSz = ccfg.getQueryDetailMetricsSize();

        if (detailMetricsSz > 0)
            detailMetrics = new ConcurrentHashMap<>(detailMetricsSz);

        lsnr = new GridLocalEventListener() {
            @Override public void onEvent(Event evt) {
                UUID nodeId = ((DiscoveryEvent)evt).eventNode().id();

                Map<Long, GridFutureAdapter<QueryResult<K, V>>> futs = qryIters.remove(nodeId);

                if (futs != null) {
                    for (Map.Entry<Long, GridFutureAdapter<QueryResult<K, V>>> entry : futs.entrySet()) {
                        final Object rcpt = recipient(nodeId, entry.getKey());

                        entry.getValue().listen(new CIX1<IgniteInternalFuture<QueryResult<K, V>>>() {
                            @Override public void applyx(IgniteInternalFuture<QueryResult<K, V>> f)
                                throws IgniteCheckedException {
                                f.get().closeIfNotShared(rcpt);
                            }
                        });
                    }
                }

                Map<Long, GridFutureAdapter<FieldsResult>> fieldsFuts = fieldsQryRes.remove(nodeId);

                if (fieldsFuts != null) {
                    for (Map.Entry<Long, GridFutureAdapter<FieldsResult>> entry : fieldsFuts.entrySet()) {
                        final Object rcpt = recipient(nodeId, entry.getKey());

                        entry.getValue().listen(new CIX1<IgniteInternalFuture<FieldsResult>>() {
                            @Override public void applyx(IgniteInternalFuture<FieldsResult> f)
                                throws IgniteCheckedException {
                                f.get().closeIfNotShared(rcpt);
                            }
                        });
                    }
                }
            }
        };

        cctx.events().addListener(lsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);

        qryTopVer = cctx.startTopologyVersion();

        assert qryTopVer != null : cctx.name();
    }

    /**
     * @return {@code True} if indexing is enabled for cache.
     */
    public boolean enabled() {
        return enabled;
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        busyLock.block();

        cctx.events().removeListener(lsnr);

        if (cancel)
            onCancelAtStop();
        else
            onWaitAtStop();
    }

    /**
     * @return {@code True} if entered busy state.
     */
    private boolean enterBusy() {
        return busyLock.enterBusy();
    }

    /**
     * Leaves busy state.
     */
    private void leaveBusy() {
        busyLock.leaveBusy();
    }

    /**
     * Stops query manager.
     *
     * @param cancel Cancel queries.
     * @param destroy Cache destroy flag..
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    @Override public final void stop0(boolean cancel, boolean destroy) {
        if (log.isDebugEnabled())
            log.debug("Stopped cache query manager.");
    }

    /**
     * Marks this request as canceled.
     *
     * @param reqId Request id.
     */
    void onQueryFutureCanceled(long reqId) {
        // No-op.
    }

    /**
     * Cancel flag handler at stop.
     */
    void onCancelAtStop() {
        // No-op.
    }

    /**
     * Wait flag handler at stop.
     */
    void onWaitAtStop() {
        // No-op.
    }

    /**
     * Processes cache query request.
     *
     * @param sndId Sender node id.
     * @param req Query request.
     */
    void processQueryRequest(UUID sndId, GridCacheQueryRequest req) {
        // No-op.
    }

    /**
     * Checks if IndexinSPI is enabled.
     *
     * @return IndexingSPI enabled flag.
     */
    private boolean isIndexingSpiEnabled() {
        return cctx.kernalContext().indexing().enabled();
    }

    /**
     *
     */
    private void invalidateResultCache() {
        if (!qryResCache.isEmpty())
            qryResCache = new ConcurrentHashMap<>();
    }

    /**
     * @param newRow New row.
     * @param prevRow Previous row.
     * @param prevRowAvailable Whether previous row is available.
     * @throws IgniteCheckedException In case of error.
     */
    public void store(CacheDataRow newRow, @Nullable CacheDataRow prevRow, boolean prevRowAvailable)
        throws IgniteCheckedException {
        assert enabled();
        assert newRow != null && newRow.value() != null && newRow.link() != 0 : newRow;

        if (!enterBusy())
            throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

        try {
            if (isIndexingSpiEnabled()) {
                CacheObjectContext coctx = cctx.cacheObjectContext();

                Object key0 = unwrapIfNeeded(newRow.key(), coctx);

                Object val0 = unwrapIfNeeded(newRow.value(), coctx);

                cctx.kernalContext().indexing().store(cacheName, key0, val0, newRow.expireTime());
            }

            if (qryProcEnabled)
                qryProc.store(cctx, newRow, prevRow, prevRowAvailable);
        }
        finally {
            invalidateResultCache();

            leaveBusy();
        }
    }

    /**
     * @param key Key.
     * @param prevRow Previous row.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void remove(KeyCacheObject key, @Nullable CacheDataRow prevRow) throws IgniteCheckedException {
        if (!QueryUtils.isEnabled(cctx.config()))
            return; // No-op.

        if (!enterBusy())
            return; // Ignore index update when node is stopping.

        try {
            if (isIndexingSpiEnabled()) {
                Object key0 = unwrapIfNeeded(key, cctx.cacheObjectContext());

                cctx.kernalContext().indexing().remove(cacheName, key0);
            }

            // val may be null if we have no previous value. We should not call processor in this case.
            if (qryProcEnabled && prevRow != null)
                qryProc.remove(cctx, prevRow);
        }
        finally {
            invalidateResultCache();

            leaveBusy();
        }
    }

    /**
     * Executes local query.
     *
     * @param qry Query.
     * @return Query future.
     */
    @SuppressWarnings("unchecked")
    CacheQueryFuture<?> queryLocal(GridCacheQueryBean qry) {
        assert qry.query().type() != GridCacheQueryType.SCAN : qry;

        if (log.isDebugEnabled())
            log.debug("Executing query on local node: " + qry);

        GridCacheLocalQueryFuture fut = new GridCacheLocalQueryFuture<>(cctx, qry);

        try {
            qry.query().validate();

            fut.execute();
        }
        catch (IgniteCheckedException e) {
            if (fut != null)
                fut.onDone(e);
        }

        return fut;
    }

    /**
     * Executes distributed query.
     *
     * @param qry Query.
     * @param nodes Nodes.
     * @return Query future.
     */
    public abstract CacheQueryFuture<?> queryDistributed(GridCacheQueryBean qry, Collection<ClusterNode> nodes);

    /**
     * Executes distributed SCAN query.
     *
     * @param qry Query.
     * @param nodes Nodes.
     * @return Iterator.
     * @throws IgniteCheckedException If failed.
     */
    public abstract GridCloseableIterator scanQueryDistributed(GridCacheQueryAdapter qry,
        Collection<ClusterNode> nodes) throws IgniteCheckedException;

    /**
     * Loads page.
     *
     * @param id Query ID.
     * @param qry Query.
     * @param nodes Nodes.
     * @param all Whether to load all pages.
     */
    public abstract void loadPage(long id, GridCacheQueryAdapter<?> qry, Collection<ClusterNode> nodes, boolean all);

    /**
     * Executes distributed fields query.
     *
     * @param qry Query.
     * @return Query future.
     */
    public abstract CacheQueryFuture<?> queryFieldsLocal(GridCacheQueryBean qry);

    /**
     * Executes distributed fields query.
     *
     * @param qry Query.
     * @param nodes Nodes.
     * @return Query future.
     */
    public abstract CacheQueryFuture<?> queryFieldsDistributed(GridCacheQueryBean qry, Collection<ClusterNode> nodes);

    /**
     * Unwrap CacheObject if needed.
     */
    private Object unwrapIfNeeded(CacheObject obj, CacheObjectContext coctx) {
        return isIndexingSpiAllowsBinary && cctx.cacheObjects().isBinaryObject(obj) ? obj : obj.value(coctx, false);
    }

    /**
     * Performs query.
     *
     * @param qry Query.
     * @param args Arguments.
     * @param loc Local query or not.
     * @param subjId Security subject ID.
     * @param taskName Task name.
     * @param rcpt ID of the recipient.
     * @return Collection of found keys.
     * @throws IgniteCheckedException In case of error.
     */
    @SuppressWarnings("unchecked")
    private QueryResult<K, V> executeQuery(GridCacheQueryAdapter<?> qry,
        @Nullable Object[] args, boolean loc, @Nullable UUID subjId, @Nullable String taskName, Object rcpt)
        throws IgniteCheckedException {
        if (qry.type() == null) {
            assert !loc;

            throw new IgniteCheckedException("Received next page request after iterator was removed. " +
                "Consider increasing maximum number of stored iterators (see " +
                "CacheConfiguration.getMaxQueryIteratorsCount() configuration property).");
        }

        QueryResult<K, V> res;

        T3<String, String, List<Object>> resKey = null;

        if (qry.type() == SQL) {
            resKey = new T3<>(qry.queryClassName(), qry.clause(), F.asList(args));

            res = (QueryResult<K, V>)qryResCache.get(resKey);

            if (res != null && res.addRecipient(rcpt))
                return res;

            res = new QueryResult<>(qry.type(), rcpt);

            if (qryResCache.putIfAbsent(resKey, res) != null)
                resKey = null;
        }
        else
            res = new QueryResult<>(qry.type(), rcpt);

        GridCloseableIterator<IgniteBiTuple<K, V>> iter;

        try {
            switch (qry.type()) {
                case SQL:
                    throw new IllegalStateException("Should never be called.");

                case SCAN:
                    if (cctx.events().isRecordable(EVT_CACHE_QUERY_EXECUTED)) {
                        cctx.gridEvents().record(new CacheQueryExecutedEvent<>(
                            cctx.localNode(),
                            "Scan query executed.",
                            EVT_CACHE_QUERY_EXECUTED,
                            CacheQueryType.SCAN.name(),
                            cctx.name(),
                            null,
                            null,
                            qry.scanFilter(),
                            null,
                            null,
                            subjId,
                            taskName));
                    }

                    iter = scanIterator(qry, false);

                    break;

                case TEXT:
                    if (cctx.events().isRecordable(EVT_CACHE_QUERY_EXECUTED)) {
                        cctx.gridEvents().record(new CacheQueryExecutedEvent<>(
                            cctx.localNode(),
                            "Full text query executed.",
                            EVT_CACHE_QUERY_EXECUTED,
                            CacheQueryType.FULL_TEXT.name(),
                            cctx.name(),
                            qry.queryClassName(),
                            qry.clause(),
                            null,
                            null,
                            null,
                            subjId,
                            taskName));
                    }

                    iter = qryProc.queryText(cacheName, qry.clause(), qry.queryClassName(), filter(qry));

                    break;

                case SET:
                    iter = setIterator(qry);

                    break;

                case SQL_FIELDS:
                    assert false : "SQL fields query is incorrectly processed.";

                default:
                    throw new IgniteCheckedException("Unknown query type: " + qry.type());
            }

            res.onDone(iter);
        }
        catch (Exception e) {
            res.onDone(e);
        }
        finally {
            if (resKey != null)
                qryResCache.remove(resKey, res);
        }

        return res;
    }

    /**
     * Performs fields query.
     *
     * @param qry Query.
     * @param args Arguments.
     * @param loc Local query or not.
     * @param subjId Security subject ID.
     * @param taskName Task name.
     * @param rcpt ID of the recipient.
     * @return Collection of found keys.
     * @throws IgniteCheckedException In case of error.
     */
    private FieldsResult executeFieldsQuery(GridCacheQueryAdapter<?> qry, @Nullable Object[] args,
        boolean loc, @Nullable UUID subjId, @Nullable String taskName, Object rcpt) throws IgniteCheckedException {
        assert qry != null;

        FieldsResult res;

        T2<String, List<Object>> resKey = null;

        if (qry.clause() == null && qry.type() != SPI) {
            assert !loc;

            throw new IgniteCheckedException("Received next page request after iterator was removed. " +
                "Consider increasing maximum number of stored iterators (see " +
                "CacheConfiguration.getMaxQueryIteratorsCount() configuration property).");
        }

        if (qry.type() == SQL_FIELDS) {
            if (cctx.events().isRecordable(EVT_CACHE_QUERY_EXECUTED)) {
                cctx.gridEvents().record(new CacheQueryExecutedEvent<>(
                    cctx.localNode(),
                    "SQL fields query executed.",
                    EVT_CACHE_QUERY_EXECUTED,
                    CacheQueryType.SQL_FIELDS.name(),
                    cctx.name(),
                    null,
                    qry.clause(),
                    null,
                    null,
                    args,
                    subjId,
                    taskName));
            }

            // Attempt to get result from cache.
            resKey = new T2<>(qry.clause(), F.asList(args));

            res = (FieldsResult)qryResCache.get(resKey);

            if (res != null && res.addRecipient(rcpt))
                return res; // Cached result found.

            res = new FieldsResult(rcpt);

            if (qryResCache.putIfAbsent(resKey, res) != null)
                resKey = null; // Failed to cache result.
        }
        else {
            assert qry.type() == SPI : "Unexpected query type: " + qry.type();

            if (cctx.events().isRecordable(EVT_CACHE_QUERY_EXECUTED)) {
                cctx.gridEvents().record(new CacheQueryExecutedEvent<>(
                    cctx.localNode(),
                    "SPI query executed.",
                    EVT_CACHE_QUERY_EXECUTED,
                    CacheQueryType.SPI.name(),
                    cctx.name(),
                    null,
                    null,
                    null,
                    null,
                    args,
                    subjId,
                    taskName));
            }

            res = new FieldsResult(rcpt);
        }

        try {
            if (qry.type() == SPI) {
                IgniteSpiCloseableIterator<?> iter = cctx.kernalContext().indexing().query(cacheName, F.asList(args),
                    filter(qry));

                res.onDone(iter);
            }
            else {
                assert qry.type() == SQL_FIELDS;

                throw new IllegalStateException("Should never be called.");
            }
        }
        catch (Exception e) {
            res.onDone(e);
        }
        finally {
            if (resKey != null)
                qryResCache.remove(resKey, res);
        }

        return res;
    }

    /**
     * @param qry Query.
     * @return Cache set items iterator.
     */
    private GridCloseableIterator<IgniteBiTuple<K, V>> setIterator(GridCacheQueryAdapter<?> qry) {
        final GridSetQueryPredicate filter = (GridSetQueryPredicate)qry.scanFilter();

        filter.init(cctx);

        IgniteUuid id = filter.setId();

        Collection<SetItemKey> data = cctx.dataStructures().setData(id);

        if (data == null)
            data = Collections.emptyList();

        final GridIterator<IgniteBiTuple<K, V>> it = F.iterator(
            data,
            new C1<SetItemKey, IgniteBiTuple<K, V>>() {
                @Override public IgniteBiTuple<K, V> apply(SetItemKey e) {
                    return new IgniteBiTuple<>((K)e.item(), (V)Boolean.TRUE);
                }
            },
            true,
            new P1<SetItemKey>() {
                @Override public boolean apply(SetItemKey e) {
                    return filter.apply(e, null);
                }
            });

        return new GridCloseableIteratorAdapter<IgniteBiTuple<K, V>>() {
            @Override protected boolean onHasNext() {
                return it.hasNext();
            }

            @Override protected IgniteBiTuple<K, V> onNext() {
                return it.next();
            }

            @Override protected void onRemove() {
                it.remove();
            }

            @Override protected void onClose() {
                // No-op.
            }
        };
    }

    /**
     * @param qry Query.
     * @param locNode Local node.
     * @return Full-scan row iterator.
     * @throws IgniteCheckedException If failed to get iterator.
     */
    @SuppressWarnings({"unchecked"})
    private GridCloseableIterator scanIterator(final GridCacheQueryAdapter<?> qry, boolean locNode)
        throws IgniteCheckedException {
        final IgniteBiPredicate<K, V> keyValFilter = qry.scanFilter();

        try {
            injectResources(keyValFilter);

            Integer part = cctx.isLocal() ? null : qry.partition();

            if (part != null && (part < 0 || part >= cctx.affinity().partitions()))
                return new GridEmptyCloseableIterator() {
                    @Override public void close() throws IgniteCheckedException {
                        closeScanFilter(keyValFilter);
                        super.close();
                    }
                };

            AffinityTopologyVersion topVer = GridQueryProcessor.getRequestAffinityTopologyVersion();

            if (topVer == null)
                topVer = cctx.affinity().affinityTopologyVersion();

            final boolean backups = qry.includeBackups() || cctx.isReplicated();

            final GridDhtLocalPartition locPart;

            final GridIterator<CacheDataRow> it;

            if (part != null) {
                final GridDhtCacheAdapter dht = cctx.isNear() ? cctx.near().dht() : cctx.dht();

                GridDhtLocalPartition locPart0 = dht.topology().localPartition(part, topVer, false);

                if (locPart0 == null || locPart0.state() != OWNING || !locPart0.reserve())
                    throw new GridDhtUnreservedPartitionException(part, cctx.affinity().affinityTopologyVersion(),
                        "Partition can not be reserved");

                locPart = locPart0;

                it = cctx.offheap().cachePartitionIterator(cctx.cacheId(), part);
            }
            else {
                locPart = null;

                // TODO shouldn't we reserve all involved partitions?
                it = cctx.offheap().cacheIterator(cctx.cacheId(), true, backups, topVer);
            }

            return new ScanQueryIterator(it, qry, topVer, locPart, keyValFilter, locNode, cctx, log);
        }
        catch (IgniteCheckedException | RuntimeException e) {
            closeScanFilter(keyValFilter);

            throw e;
        }
    }

    /**
     * Closes a filter if it is closeable.
     *
     * @param f Filter.
     */
    private static void closeScanFilter(Object f) {
        if (f instanceof PlatformCacheEntryFilter)
            ((PlatformCacheEntryFilter)f).onClose();
    }

    /**
     * @param o Object to inject resources to.
     * @throws IgniteCheckedException If failure occurred while injecting resources.
     */
    private void injectResources(@Nullable Object o) throws IgniteCheckedException {
        if (o != null) {
            GridKernalContext ctx = cctx.kernalContext();

            ClassLoader ldr = o.getClass().getClassLoader();

            if (ctx.deploy().isGlobalLoader(ldr))
                ctx.resource().inject(ctx.deploy().getDeployment(ctx.deploy().getClassLoaderId(ldr)), o.getClass(), o);
            else
                ctx.resource().inject(ctx.deploy().getDeployment(o.getClass().getName()), o.getClass(), o);
        }
    }

    /**
     * Processes fields query request.
     *
     * @param qryInfo Query info.
     */
    protected void runFieldsQuery(GridCacheQueryInfo qryInfo) {
        assert qryInfo != null;

        if (!enterBusy()) {
            if (cctx.localNodeId().equals(qryInfo.senderId()))
                throw new IllegalStateException("Failed to process query request (grid is stopping).");

            return; // Ignore remote requests when when node is stopping.
        }

        try {
            if (log.isDebugEnabled())
                log.debug("Running query: " + qryInfo);

            boolean rmvRes = true;

            FieldsResult res = null;

            final boolean statsEnabled = cctx.statisticsEnabled();

            final boolean readEvt = cctx.events().isRecordable(EVT_CACHE_QUERY_OBJECT_READ);

            try {
                // Preparing query closures.
                IgniteReducer<Object, Object> rdc = (IgniteReducer<Object, Object>)qryInfo.reducer();

                injectResources(rdc);

                GridCacheQueryAdapter<?> qry = qryInfo.query();

                int pageSize = qry.pageSize();

                Collection<Object> data = null;
                Collection<Object> entities = null;

                if (qryInfo.local() || rdc != null || cctx.isLocalNode(qryInfo.senderId()))
                    data = new ArrayList<>(pageSize);
                else
                    entities = new ArrayList<>(pageSize);

                String taskName = cctx.kernalContext().task().resolveTaskName(qry.taskHash());

                res = qryInfo.local() ?
                    executeFieldsQuery(qry, qryInfo.arguments(), qryInfo.local(), qry.subjectId(), taskName,
                        recipient(qryInfo.senderId(), qryInfo.requestId())) :
                    fieldsQueryResult(qryInfo, taskName);

                // If metadata needs to be returned to user and cleaned from internal fields - copy it.
                List<GridQueryFieldMetadata> meta = qryInfo.includeMetaData() ?
                    (res.metaData() != null ? new ArrayList<>(res.metaData()) : null) :
                    res.metaData();

                if (!qryInfo.includeMetaData())
                    meta = null;

                GridCloseableIterator<?> it = new GridSpiCloseableIteratorWrapper<Object>(
                    res.iterator(recipient(qryInfo.senderId(), qryInfo.requestId())));

                if (log.isDebugEnabled())
                    log.debug("Received fields iterator [iterHasNext=" + it.hasNext() + ']');

                if (!it.hasNext()) {
                    if (rdc != null)
                        data = Collections.singletonList(rdc.reduce());

                    onFieldsPageReady(qryInfo.local(), qryInfo, meta, entities, data, true, null);

                    return;
                }

                int cnt = 0;
                boolean metaSent = false;

                while (!Thread.currentThread().isInterrupted() && it.hasNext()) {
                    long start = statsEnabled ? System.nanoTime() : 0L;

                    Object row = it.next();

                    // Query is cancelled.
                    if (row == null) {
                        onPageReady(qryInfo.local(), qryInfo, null, true, null);

                        break;
                    }

                    if (statsEnabled) {
                        CacheMetricsImpl metrics = cctx.cache().metrics0();

                        metrics.onRead(true);

                        metrics.addGetTimeNanos(System.nanoTime() - start);
                    }

                    if (readEvt && cctx.gridEvents().hasListener(EVT_CACHE_QUERY_OBJECT_READ)) {
                        cctx.gridEvents().record(new CacheQueryReadEvent<K, V>(
                            cctx.localNode(),
                            "SQL fields query result set row read.",
                            EVT_CACHE_QUERY_OBJECT_READ,
                            CacheQueryType.SQL_FIELDS.name(),
                            cctx.name(),
                            null,
                            qry.clause(),
                            null,
                            null,
                            qryInfo.arguments(),
                            qry.subjectId(),
                            taskName,
                            null,
                            null,
                            null,
                            row));
                    }

                    if ((qryInfo.local() || rdc != null || cctx.isLocalNode(qryInfo.senderId()))) {
                        // Reduce.
                        if (rdc != null) {
                            if (!rdc.collect(row))
                                break;
                        }
                        else
                            data.add(row);
                    }
                    else
                        entities.add(row);

                    if (rdc == null && ((!qryInfo.allPages() && ++cnt == pageSize) || !it.hasNext())) {
                        onFieldsPageReady(qryInfo.local(), qryInfo, !metaSent ? meta : null,
                            entities, data, !it.hasNext(), null);

                        if (it.hasNext())
                            rmvRes = false;

                        if (!qryInfo.allPages())
                            return;
                    }
                }

                if (rdc != null) {
                    onFieldsPageReady(qryInfo.local(), qryInfo, meta, null,
                        Collections.singletonList(rdc.reduce()), true, null);
                }
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled() || !e.hasCause(SQLException.class))
                    U.error(log, "Failed to run fields query [qry=" + qryInfo + ", node=" + cctx.nodeId() + ']', e);
                else {
                    if (e.hasCause(SQLException.class))
                        U.error(log, "Failed to run fields query [node=" + cctx.nodeId() +
                            ", msg=" + e.getCause(SQLException.class).getMessage() + ']');
                    else
                        U.error(log, "Failed to run fields query [node=" + cctx.nodeId() +
                            ", msg=" + e.getMessage() + ']');
                }

                onFieldsPageReady(qryInfo.local(), qryInfo, null, null, null, true, e);
            }
            catch (Throwable e) {
                U.error(log, "Failed to run fields query [qry=" + qryInfo + ", node=" + cctx.nodeId() + "]", e);

                onFieldsPageReady(qryInfo.local(), qryInfo, null, null, null, true, e);

                if (e instanceof Error)
                    throw (Error)e;
            }
            finally {
                if (qryInfo.local()) {
                    // Don't we need to always remove local iterators?
                    if (rmvRes && res != null) {
                        try {
                            res.closeIfNotShared(recipient(qryInfo.senderId(), qryInfo.requestId()));
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to close local iterator [qry=" + qryInfo + ", node=" +
                                cctx.nodeId() + "]", e);
                        }
                    }
                }
                else if (rmvRes)
                    removeFieldsQueryResult(qryInfo.senderId(), qryInfo.requestId());
            }
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * Processes cache query request.
     *
     * @param qryInfo Query info.
     */
    @SuppressWarnings("unchecked")
    protected void runQuery(GridCacheQueryInfo qryInfo) {
        assert qryInfo != null;
        assert qryInfo.query().type() != SCAN || !qryInfo.local() : qryInfo;

        if (!enterBusy()) {
            if (cctx.localNodeId().equals(qryInfo.senderId()))
                throw new IllegalStateException("Failed to process query request (grid is stopping).");

            return; // Ignore remote requests when when node is stopping.
        }

        try {
            boolean loc = qryInfo.local();

            QueryResult<K, V> res = null;

            if (log.isDebugEnabled())
                log.debug("Running query: " + qryInfo);

            boolean rmvIter = true;

            GridCacheQueryAdapter<?> qry = qryInfo.query();

            try {
                // Preparing query closures.
                IgniteClosure<Cache.Entry<K, V>, Object> trans =
                    (IgniteClosure<Cache.Entry<K, V>, Object>)qryInfo.transformer();

                IgniteReducer<Cache.Entry<K, V>, Object> rdc = (IgniteReducer<Cache.Entry<K, V>, Object>)qryInfo.reducer();

                injectResources(trans);
                injectResources(rdc);

                int pageSize = qry.pageSize();

                boolean incBackups = qry.includeBackups();

                String taskName = cctx.kernalContext().task().resolveTaskName(qry.taskHash());

                IgniteSpiCloseableIterator<IgniteBiTuple<K, V>> iter;
                GridCacheQueryType type;

                res = loc ?
                    executeQuery(qry, qryInfo.arguments(), loc, qry.subjectId(), taskName,
                        recipient(qryInfo.senderId(), qryInfo.requestId())) :
                    queryResult(qryInfo, taskName);

                if (res == null)
                    return;

                iter = res.iterator(recipient(qryInfo.senderId(), qryInfo.requestId()));
                type = res.type();

                final GridCacheAdapter<K, V> cache = cctx.cache();

                if (log.isDebugEnabled())
                    log.debug("Received index iterator [iterHasNext=" + iter.hasNext() +
                        ", cacheSize=" + cache.size() + ']');

                int cnt = 0;

                boolean stop = false;
                boolean pageSent = false;

                Collection<Object> data = new ArrayList<>(pageSize);

                AffinityTopologyVersion topVer = cctx.affinity().affinityTopologyVersion();

                final boolean statsEnabled = cctx.statisticsEnabled();

                final boolean readEvt = cctx.events().isRecordable(EVT_CACHE_QUERY_OBJECT_READ);

                CacheObjectContext objCtx = cctx.cacheObjectContext();

                while (!Thread.currentThread().isInterrupted()) {
                    long start = statsEnabled ? System.nanoTime() : 0L;

                    // Need to call it after gathering start time because
                    // actual row extracting may happen inside this method.
                    if(!iter.hasNext())
                        break;

                    IgniteBiTuple<K, V> row = iter.next();

                    // Query is cancelled.
                    if (row == null) {
                        onPageReady(loc, qryInfo, null, true, null);

                        break;
                    }

                    final K key = row.getKey();

                    // Filter backups for SCAN queries, if it isn't partition scan.
                    // Other types are filtered in indexing manager.
                    if (!cctx.isReplicated() && qry.type() == SCAN && qry.partition() == null &&
                        cctx.config().getCacheMode() != LOCAL && !incBackups &&
                        !cctx.affinity().primaryByKey(cctx.localNode(), key, topVer)) {
                        if (log.isDebugEnabled())
                            log.debug("Ignoring backup element [row=" + row +
                                ", cacheMode=" + cctx.config().getCacheMode() + ", incBackups=" + incBackups +
                                ", primary=" + cctx.affinity().primaryByKey(cctx.localNode(), key, topVer) + ']');

                        continue;
                    }

                    V val = row.getValue();

                    if (log.isDebugEnabled()) {
                        ClusterNode primaryNode = cctx.affinity().primaryByKey(key,
                            cctx.affinity().affinityTopologyVersion());

                        log.debug(S.toString("Record",
                            "key", key, true,
                            "val", val, true,
                            "incBackups", incBackups, false,
                            "priNode", primaryNode != null ? U.id8(primaryNode.id()) : null, false,
                            "node", U.id8(cctx.localNode().id()), false));
                    }

                    if (val == null) {
                        if (log.isDebugEnabled())
                            log.debug(S.toString("Unsuitable record value", "val", val, true));

                        continue;
                    }

                    if (statsEnabled) {
                        CacheMetricsImpl metrics = cctx.cache().metrics0();

                        metrics.onRead(true);

                        metrics.addGetTimeNanos(System.nanoTime() - start);
                    }

                    K key0 = null;
                    V val0 = null;

                    if (readEvt && cctx.gridEvents().hasListener(EVT_CACHE_QUERY_OBJECT_READ)) {
                        key0 = (K)CacheObjectUtils.unwrapBinaryIfNeeded(objCtx, key, qry.keepBinary(), false);
                        val0 = (V)CacheObjectUtils.unwrapBinaryIfNeeded(objCtx, val, qry.keepBinary(), false);

                        switch (type) {
                            case SQL:
                                cctx.gridEvents().record(new CacheQueryReadEvent<>(
                                    cctx.localNode(),
                                    "SQL query entry read.",
                                    EVT_CACHE_QUERY_OBJECT_READ,
                                    CacheQueryType.SQL.name(),
                                    cctx.name(),
                                    qry.queryClassName(),
                                    qry.clause(),
                                    null,
                                    null,
                                    qryInfo.arguments(),
                                    qry.subjectId(),
                                    taskName,
                                    key0,
                                    val0,
                                    null,
                                    null));

                                break;

                            case TEXT:
                                cctx.gridEvents().record(new CacheQueryReadEvent<>(
                                    cctx.localNode(),
                                    "Full text query entry read.",
                                    EVT_CACHE_QUERY_OBJECT_READ,
                                    CacheQueryType.FULL_TEXT.name(),
                                    cctx.name(),
                                    qry.queryClassName(),
                                    qry.clause(),
                                    null,
                                    null,
                                    null,
                                    qry.subjectId(),
                                    taskName,
                                    key0,
                                    val0,
                                    null,
                                    null));

                                break;

                            case SCAN:
                                cctx.gridEvents().record(new CacheQueryReadEvent<>(
                                    cctx.localNode(),
                                    "Scan query entry read.",
                                    EVT_CACHE_QUERY_OBJECT_READ,
                                    CacheQueryType.SCAN.name(),
                                    cctx.name(),
                                    null,
                                    null,
                                    qry.scanFilter(),
                                    null,
                                    null,
                                    qry.subjectId(),
                                    taskName,
                                    key0,
                                    val0,
                                    null,
                                    null));

                                break;
                        }
                    }

                    if (rdc != null || trans != null) {
                        if (key0 == null)
                            key0 = (K)CacheObjectUtils.unwrapBinaryIfNeeded(objCtx, key, qry.keepBinary(), false);
                        if (val0 == null)
                            val0 = (V)CacheObjectUtils.unwrapBinaryIfNeeded(objCtx, val, qry.keepBinary(), false);

                        Cache.Entry<K, V> entry = new CacheEntryImpl(key0, val0);

                        // Reduce.
                        if (rdc != null) {
                            if (!rdc.collect(entry) || !iter.hasNext()) {
                                onPageReady(loc, qryInfo, Collections.singletonList(rdc.reduce()), true, null);

                                pageSent = true;

                                break;
                            }
                            else
                                continue;
                        }

                        data.add(trans != null ? trans.apply(entry) :
                            !loc ? new GridCacheQueryResponseEntry<>(key, val) : F.t(key, val));
                    }
                    else
                        data.add(!loc ? new GridCacheQueryResponseEntry<>(key, val) : F.t(key, val));

                    if (!loc) {
                        if (++cnt == pageSize || !iter.hasNext()) {
                            boolean finished = !iter.hasNext();

                            onPageReady(loc, qryInfo, data, finished, null);

                            pageSent = true;

                            if (!finished)
                                rmvIter = false;

                            if (!qryInfo.allPages())
                                return;

                            data = new ArrayList<>(pageSize);

                            if (stop)
                                break; // while
                        }
                    }
                }

                if (!pageSent) {
                    if (rdc == null)
                        onPageReady(loc, qryInfo, data, true, null);
                    else
                        onPageReady(loc, qryInfo, Collections.singletonList(rdc.reduce()), true, null);
                }
            }
            catch (Throwable e) {
                if (X.hasCause(e, ClassNotFoundException.class) && !qry.keepBinary() && cctx.binaryMarshaller() &&
                    !cctx.localNode().isClient() && !log.isQuiet()) {
                    LT.warn(log, "Suggestion for the cause of ClassNotFoundException");
                    LT.warn(log, "To disable, set -D" + IGNITE_QUIET + "=true");
                    LT.warn(log, "  ^-- Ignite configured to use BinaryMarshaller but keepBinary is false for " +
                        "request");
                    LT.warn(log, "  ^-- Server node need to load definition of data classes. " +
                        "It can be reason of ClassNotFoundException(consider IgniteCache.withKeepBinary to fix)");
                    LT.warn(log, "Refer this page for detailed information: " +
                        "https://apacheignite.readme.io/docs/binary-marshaller");
                }

                if (!X.hasCause(e, GridDhtUnreservedPartitionException.class))
                    U.error(log, "Failed to run query [qry=" + qryInfo + ", node=" + cctx.nodeId() + "]", e);

                onPageReady(loc, qryInfo, null, true, e);

                if (e instanceof Error)
                    throw (Error)e;
            }
            finally {
                if (loc) {
                    // Local iterators are always removed.
                    if (res != null) {
                        try {
                            res.closeIfNotShared(recipient(qryInfo.senderId(), qryInfo.requestId()));
                        }
                        catch (IgniteCheckedException e) {
                            if (!X.hasCause(e, GridDhtUnreservedPartitionException.class))
                                U.error(log, "Failed to close local iterator [qry=" + qryInfo + ", node=" +
                                    cctx.nodeId() + "]", e);
                        }
                    }
                }
                else if (rmvIter)
                    removeQueryResult(qryInfo.senderId(), qryInfo.requestId());
            }
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * Process local scan query.
     *
     * @param qry Query.
     * @param updateStatistics Update statistics flag.
     */
    @SuppressWarnings({"unchecked", "serial"})
    protected GridCloseableIterator scanQueryLocal(final GridCacheQueryAdapter qry,
        boolean updateStatistics) throws IgniteCheckedException {
        if (!enterBusy())
            throw new IllegalStateException("Failed to process query request (grid is stopping).");

        final boolean statsEnabled = cctx.statisticsEnabled();

        updateStatistics &= statsEnabled;

        long startTime = U.currentTimeMillis();

        final String namex = cctx.name();

        final IgniteBiPredicate<K, V> scanFilter = qry.scanFilter();

        try {
            assert qry.type() == SCAN;

            if (log.isDebugEnabled())
                log.debug("Running local SCAN query: " + qry);

            final String taskName = cctx.kernalContext().task().resolveTaskName(qry.taskHash());
            final ClusterNode locNode = cctx.localNode();
            final UUID subjId = qry.subjectId();

            if (cctx.events().isRecordable(EVT_CACHE_QUERY_EXECUTED)) {
                cctx.gridEvents().record(new CacheQueryExecutedEvent<>(
                    locNode,
                    "Scan query executed.",
                    EVT_CACHE_QUERY_EXECUTED,
                    CacheQueryType.SCAN.name(),
                    namex,
                    null,
                    null,
                    scanFilter,
                    null,
                    null,
                    subjId,
                    taskName));
            }

            GridCloseableIterator it = scanIterator(qry, true);

            updateStatistics = false;

            return it;
        }
        catch (Exception e) {
            closeScanFilter(scanFilter);

            if (updateStatistics)
                cctx.queries().collectMetrics(GridCacheQueryType.SCAN, namex, startTime,
                    U.currentTimeMillis() - startTime, true);

            throw e;
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param qryInfo Info.
     * @param taskName Task name.
     * @return Iterator.
     * @throws IgniteCheckedException In case of error.
     */
    @Nullable private QueryResult<K, V> queryResult(final GridCacheQueryInfo qryInfo,
        String taskName) throws IgniteCheckedException {
        assert qryInfo != null;

        final UUID sndId = qryInfo.senderId();

        assert sndId != null;

        RequestFutureMap futs = qryIters.get(sndId);

        if (futs == null) {
            futs = new RequestFutureMap() {
                @Override protected boolean removeEldestEntry(Map.Entry<Long, GridFutureAdapter<QueryResult<K, V>>> e) {
                    boolean rmv = size() > maxIterCnt;

                    if (rmv) {
                        try {
                            e.getValue().get().closeIfNotShared(recipient(sndId, e.getKey()));
                        }
                        catch (IgniteCheckedException ex) {
                            U.error(log, "Failed to close query iterator.", ex);
                        }
                    }

                    return rmv;
                }
            };

            RequestFutureMap old = qryIters.putIfAbsent(sndId, futs);

            if (old != null)
                futs = old;
        }

        assert futs != null;

        GridFutureAdapter<QueryResult<K, V>> fut;

        boolean exec = false;

        synchronized (futs) {
            if (futs.isCanceled(qryInfo.requestId()))
                return null;

            fut = futs.get(qryInfo.requestId());

            if (fut == null) {
                futs.put(qryInfo.requestId(), fut = new GridFutureAdapter<>());

                exec = true;
            }
        }

        if (exec) {
            try {
                fut.onDone(executeQuery(qryInfo.query(), qryInfo.arguments(), false,
                    qryInfo.query().subjectId(), taskName, recipient(qryInfo.senderId(), qryInfo.requestId())));
            }
            catch (Throwable e) {
                fut.onDone(e);

                if (e instanceof Error)
                    throw (Error)e;
            }
        }

        return fut.get();
    }

    /**
     * @param sndId Sender node ID.
     * @param reqId Request ID.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    protected void removeQueryResult(@Nullable UUID sndId, long reqId) {
        if (sndId == null)
            return;

        RequestFutureMap futs = qryIters.get(sndId);

        if (futs != null) {
            IgniteInternalFuture<QueryResult<K, V>> fut;

            synchronized (futs) {
                fut = futs.remove(reqId);
            }

            if (fut != null) {
                try {
                    fut.get().closeIfNotShared(recipient(sndId, reqId));
                }
                catch (IgniteCheckedException e) {
                    if (!X.hasCause(e, GridDhtUnreservedPartitionException.class))
                        U.error(log, "Failed to close iterator.", e);
                }
            }
        }
    }

    /**
     * @param sndId Sender node ID.
     * @param reqId Request ID.
     * @return Recipient ID.
     */
    private static Object recipient(UUID sndId, long reqId) {
        assert sndId != null;

        return new IgniteBiTuple<>(sndId, reqId);
    }

    /**
     * @param qryInfo Info.
     * @return Iterator.
     * @throws IgniteCheckedException In case of error.
     */
    private FieldsResult fieldsQueryResult(GridCacheQueryInfo qryInfo, String taskName)
        throws IgniteCheckedException {
        final UUID sndId = qryInfo.senderId();

        assert sndId != null;

        Map<Long, GridFutureAdapter<FieldsResult>> iters = fieldsQryRes.get(sndId);

        if (iters == null) {
            iters = new LinkedHashMap<Long, GridFutureAdapter<FieldsResult>>(16, 0.75f, true) {
                @Override protected boolean removeEldestEntry(Map.Entry<Long,
                    GridFutureAdapter<FieldsResult>> e) {
                    boolean rmv = size() > maxIterCnt;

                    if (rmv) {
                        try {
                            e.getValue().get().closeIfNotShared(recipient(sndId, e.getKey()));
                        }
                        catch (IgniteCheckedException ex) {
                            U.error(log, "Failed to close fields query iterator.", ex);
                        }
                    }

                    return rmv;
                }

                @Override public boolean equals(Object o) {
                    return o == this;
                }
            };

            Map<Long, GridFutureAdapter<FieldsResult>> old = fieldsQryRes.putIfAbsent(sndId, iters);

            if (old != null)
                iters = old;
        }

        return fieldsQueryResult(iters, qryInfo, taskName);
    }

    /**
     * @param resMap Results map.
     * @param qryInfo Info.
     * @return Fields query result.
     * @throws IgniteCheckedException In case of error.
     */
    @SuppressWarnings({
        "SynchronizationOnLocalVariableOrMethodParameter",
        "NonPrivateFieldAccessedInSynchronizedContext"})
    private FieldsResult fieldsQueryResult(Map<Long, GridFutureAdapter<FieldsResult>> resMap,
        GridCacheQueryInfo qryInfo, String taskName) throws IgniteCheckedException {
        assert resMap != null;
        assert qryInfo != null;

        GridFutureAdapter<FieldsResult> fut;

        boolean exec = false;

        synchronized (resMap) {
            fut = resMap.get(qryInfo.requestId());

            if (fut == null) {
                resMap.put(qryInfo.requestId(), fut =
                    new GridFutureAdapter<>());

                exec = true;
            }
        }

        if (exec) {
            try {
                fut.onDone(executeFieldsQuery(qryInfo.query(), qryInfo.arguments(), false,
                    qryInfo.query().subjectId(), taskName, recipient(qryInfo.senderId(), qryInfo.requestId())));
            }
            catch (IgniteCheckedException e) {
                fut.onDone(e);
            }
        }

        return fut.get();
    }

    /**
     * @param sndId Sender node ID.
     * @param reqId Request ID.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    protected void removeFieldsQueryResult(@Nullable UUID sndId, long reqId) {
        if (sndId == null)
            return;

        Map<Long, GridFutureAdapter<FieldsResult>> futs = fieldsQryRes.get(sndId);

        if (futs != null) {
            IgniteInternalFuture<FieldsResult> fut;

            synchronized (futs) {
                fut = futs.remove(reqId);
            }

            if (fut != null) {
                assert fut.isDone();

                try {
                    fut.get().closeIfNotShared(recipient(sndId, reqId));
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to close iterator.", e);
                }
            }
        }
    }

    /**
     * Called when data for page is ready.
     *
     * @param loc Local query or not.
     * @param qryInfo Query info.
     * @param data Result data.
     * @param finished Last page or not.
     * @param e Exception in case of error.
     * @return {@code true} if page was processed right.
     */
    protected abstract boolean onPageReady(boolean loc, GridCacheQueryInfo qryInfo,
        @Nullable Collection<?> data, boolean finished, @Nullable Throwable e);

    /**
     * @param loc Local query or not.
     * @param qryInfo Query info.
     * @param metaData Meta data.
     * @param entities Indexing entities.
     * @param data Data.
     * @param finished Last page or not.
     * @param e Exception in case of error.
     * @return {@code true} if page was processed right.
     */
    protected abstract boolean onFieldsPageReady(boolean loc, GridCacheQueryInfo qryInfo,
        @Nullable List<GridQueryFieldMetadata> metaData,
        @Nullable Collection<?> entities,
        @Nullable Collection<?> data,
        boolean finished, @Nullable Throwable e);

    /**
     * Gets cache queries metrics.
     *
     * @return Cache queries metrics.
     */
    public QueryMetrics metrics() {
        return metrics.copy();
    }

    /**
     * Gets cache queries detailed metrics. Detail metrics could be enabled by setting non-zero value via {@link
     * CacheConfiguration#setQueryDetailMetricsSize(int)}
     *
     * @return Cache queries metrics aggregated by query type and query text.
     */
    public Collection<GridCacheQueryDetailMetricsAdapter> detailMetrics() {
        if (detailMetricsSz > 0) {
            // Return no more than latest detailMetricsSz items.
            if (detailMetrics.size() > detailMetricsSz) {
                GridBoundedPriorityQueue<GridCacheQueryDetailMetricsAdapter> latestMetrics =
                    new GridBoundedPriorityQueue<>(detailMetricsSz, QRY_DETAIL_METRICS_PRIORITY_NEW_CMP);

                latestMetrics.addAll(detailMetrics.values());

                return latestMetrics;
            }

            return new ArrayList<>(detailMetrics.values());
        }

        return Collections.emptyList();
    }

    /**
     * Evict detail metrics.
     */
    public void evictDetailMetrics() {
        if (detailMetricsSz > 0) {
            int sz = detailMetrics.size();

            if (sz > detailMetricsSz) {
                // Limit number of metrics to evict in order make eviction time predictable.
                int evictCnt = Math.min(QRY_DETAIL_METRICS_EVICTION_LIMIT, sz - detailMetricsSz);

                Queue<GridCacheQueryDetailMetricsAdapter> metricsToEvict =
                    new GridBoundedPriorityQueue<>(evictCnt, QRY_DETAIL_METRICS_PRIORITY_OLD_CMP);

                metricsToEvict.addAll(detailMetrics.values());

                for (GridCacheQueryDetailMetricsAdapter m : metricsToEvict)
                    detailMetrics.remove(m.key());
            }
        }
    }

    /**
     * Resets metrics.
     */
    public void resetMetrics() {
        metrics = new GridCacheQueryMetricsAdapter();
    }

    /**
     * Resets detail metrics.
     */
    public void resetDetailMetrics() {
        if (detailMetrics != null)
            detailMetrics.clear();
    }

    /**
     * @param qryType Query type.
     * @param qry Query description.
     * @param startTime Query start size.
     * @param duration Execution duration.
     * @param failed {@code True} if query execution failed.
     */
    public void collectMetrics(GridCacheQueryType qryType, String qry, long startTime, long duration, boolean failed) {
        metrics.update(duration, failed);

        if (detailMetricsSz > 0) {
            // Do not collect metrics for EXPLAIN queries.
            if (qryType == SQL_FIELDS && !F.isEmpty(qry)) {
                int off = 0;
                int len = qry.length();

                while (off < len && Character.isWhitespace(qry.charAt(off)))
                    off++;

                if (qry.regionMatches(true, off, "EXPLAIN", 0, 7))
                    return;
            }

            GridCacheQueryDetailMetricsAdapter m = new GridCacheQueryDetailMetricsAdapter(qryType, qry,
                cctx.name(), startTime, duration, failed);

            GridCacheQueryDetailMetricsKey key = m.key();

            detailMetrics.merge(key, m, QRY_DETAIL_METRICS_MERGE_FX);
        }
    }

    /**
     * Gets SQL metadata.
     *
     * @return SQL metadata.
     * @throws IgniteCheckedException In case of error.
     */
    public Collection<GridCacheSqlMetadata> sqlMetadata() throws IgniteCheckedException {
        if (!enterBusy())
            throw new IllegalStateException("Failed to get metadata (grid is stopping).");

        try {
            Callable<Collection<CacheSqlMetadata>> job = new MetadataJob();

            // Remote nodes that have current cache.
            Collection<ClusterNode> nodes = CU.affinityNodes(cctx, AffinityTopologyVersion.NONE);

            Collection<Collection<CacheSqlMetadata>> res = new ArrayList<>(nodes.size() + 1);

            IgniteInternalFuture<Collection<Collection<CacheSqlMetadata>>> rmtFut = null;

            // Get metadata from remote nodes.
            if (!nodes.isEmpty())
                rmtFut = cctx.closures().callAsyncNoFailover(BROADCAST, Collections.singleton(job), nodes, true, 0);

            // Get local metadata.
            IgniteInternalFuture<Collection<CacheSqlMetadata>> locFut = cctx.closures().callLocalSafe(job, true);

            if (rmtFut != null)
                res.addAll(rmtFut.get());

            res.add(locFut.get());

            Map<String, Collection<CacheSqlMetadata>> map = new HashMap<>();

            for (Collection<CacheSqlMetadata> col : res) {
                for (CacheSqlMetadata meta : col) {
                    String name = meta.cacheName();

                    Collection<CacheSqlMetadata> cacheMetas = map.get(name);

                    if (cacheMetas == null)
                        map.put(name, cacheMetas = new LinkedList<>());

                    cacheMetas.add(meta);
                }
            }

            Collection<GridCacheSqlMetadata> col = new ArrayList<>(map.size());

            // Metadata for current cache must be first in list.
            col.add(new CacheSqlMetadata(map.remove(cacheName)));

            for (Collection<CacheSqlMetadata> metas : map.values())
                col.add(new CacheSqlMetadata(metas));

            return col;
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * Gets SQL metadata with not nulls fields.
     *
     * @return SQL metadata.
     * @throws IgniteCheckedException In case of error.
     */
    public Collection<GridCacheSqlMetadata> sqlMetadataV2() throws IgniteCheckedException {
        if (!enterBusy())
            throw new IllegalStateException("Failed to get metadata (grid is stopping).");

        try {
            Callable<Collection<CacheSqlMetadata>> job = new GridCacheQuerySqlMetadataJobV2();

            // Remote nodes that have current cache.
            Collection<ClusterNode> nodes = CU.affinityNodes(cctx, AffinityTopologyVersion.NONE);

            Collection<Collection<CacheSqlMetadata>> res = new ArrayList<>(nodes.size() + 1);

            IgniteInternalFuture<Collection<Collection<CacheSqlMetadata>>> rmtFut = null;

            // Get metadata from remote nodes.
            if (!nodes.isEmpty()) {
                boolean allNodesNew = true;

                for (ClusterNode n : nodes) {
                    if (n.version().compareTo(NOT_NULLS_SUPPORT_VER) < 0)
                        allNodesNew = false;
                }

                if (!allNodesNew)
                    return sqlMetadata();

                rmtFut = cctx.closures().callAsyncNoFailover(BROADCAST, Collections.singleton(job), nodes, true, 0);
            }

            // Get local metadata.
            IgniteInternalFuture<Collection<CacheSqlMetadata>> locFut = cctx.closures().callLocalSafe(job, true);

            if (rmtFut != null)
                res.addAll(rmtFut.get());

            res.add(locFut.get());

            Map<String, Collection<CacheSqlMetadata>> map = new HashMap<>();

            for (Collection<CacheSqlMetadata> col : res) {
                for (CacheSqlMetadata meta : col) {
                    String name = meta.cacheName();

                    Collection<CacheSqlMetadata> cacheMetas = map.get(name);

                    if (cacheMetas == null)
                        map.put(name, cacheMetas = new LinkedList<>());

                    cacheMetas.add(meta);
                }
            }

            Collection<GridCacheSqlMetadata> col = new ArrayList<>(map.size());

            // Metadata for current cache must be first in list.
            col.add(new GridCacheQuerySqlMetadataV2(map.remove(cacheName)));

            for (Collection<CacheSqlMetadata> metas : map.values())
                col.add(new GridCacheQuerySqlMetadataV2(metas));

            return col;
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @return Topology version for query requests.
     */
    public AffinityTopologyVersion queryTopologyVersion() {
        return qryTopVer;
    }

    /**
     * @param qry Query.
     * @return Filter.
     */
    private IndexingQueryFilter filter(GridCacheQueryAdapter<?> qry) {
        if (qry.includeBackups())
            return null;

        return new IndexingQueryFilterImpl(cctx.kernalContext(), AffinityTopologyVersion.NONE, null);
    }

    /**
     * Prints memory statistics for debugging purposes.
     */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> Query manager memory stats [igniteInstanceName=" + cctx.igniteInstanceName() + ", cache=" + cctx.name() + ']');
    }

    /**
     * FOR TESTING ONLY
     *
     * @return Cache name for this query manager.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * Metadata job.
     */
    @GridInternal
    private static class MetadataJob implements IgniteCallable<Collection<CacheSqlMetadata>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Number of fields to report when no fields defined. Includes _key and _val columns.
         */
        private static final int NO_FIELDS_COLUMNS_COUNT = 2;

        /** Grid */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public Collection<CacheSqlMetadata> call() {
            final GridKernalContext ctx = ((IgniteKernal)ignite).context();

            Collection<String> cacheNames = F.viewReadOnly(ctx.cache().caches(),
                new C1<IgniteInternalCache<?, ?>, String>() {
                    @Override public String apply(IgniteInternalCache<?, ?> c) {
                        return c.name();
                    }
                },
                new P1<IgniteInternalCache<?, ?>>() {
                    @Override public boolean apply(IgniteInternalCache<?, ?> c) {
                        return !CU.isSystemCache(c.name()) && !DataStructuresProcessor.isDataStructureCache(c.name());
                    }
                }
            );

            return F.transform(cacheNames, new C1<String, CacheSqlMetadata>() {
                @Override public CacheSqlMetadata apply(String cacheName) {
                    Collection<GridQueryTypeDescriptor> types = ctx.query().types(cacheName);

                    Collection<String> names = U.newHashSet(types.size());
                    Map<String, String> keyClasses = U.newHashMap(types.size());
                    Map<String, String> valClasses = U.newHashMap(types.size());
                    Map<String, Map<String, String>> fields = U.newHashMap(types.size());
                    Map<String, Collection<GridCacheSqlIndexMetadata>> indexes = U.newHashMap(types.size());

                    for (GridQueryTypeDescriptor type : types) {
                        // Filter internal types (e.g., data structures).
                        if (type.name().startsWith("GridCache"))
                            continue;

                        names.add(type.name());

                        keyClasses.put(type.name(), type.keyClass().getName());
                        valClasses.put(type.name(), type.valueClass().getName());

                        int size = type.fields().isEmpty() ? NO_FIELDS_COLUMNS_COUNT : type.fields().size();

                        Map<String, String> fieldsMap = U.newLinkedHashMap(size);

                        // _KEY and _VAL are not included in GridIndexingTypeDescriptor.valueFields
                        if (type.fields().isEmpty()) {
                            fieldsMap.put("_KEY", type.keyClass().getName());
                            fieldsMap.put("_VAL", type.valueClass().getName());
                        }

                        for (Map.Entry<String, Class<?>> e : type.fields().entrySet())
                            fieldsMap.put(e.getKey().toUpperCase(), e.getValue().getName());

                        fields.put(type.name(), fieldsMap);

                        Map<String, GridQueryIndexDescriptor> idxs = type.indexes();

                        Collection<GridCacheSqlIndexMetadata> indexesCol = new ArrayList<>(idxs.size());

                        for (Map.Entry<String, GridQueryIndexDescriptor> e : idxs.entrySet()) {
                            GridQueryIndexDescriptor desc = e.getValue();

                            // Add only SQL indexes.
                            if (desc.type() == QueryIndexType.SORTED) {
                                Collection<String> idxFields = new LinkedList<>();
                                Collection<String> descendings = new LinkedList<>();

                                for (String idxField : e.getValue().fields()) {
                                    String idxFieldUpper = idxField.toUpperCase();

                                    idxFields.add(idxFieldUpper);

                                    if (desc.descending(idxField))
                                        descendings.add(idxFieldUpper);
                                }

                                indexesCol.add(new CacheSqlIndexMetadata(e.getKey().toUpperCase(),
                                    idxFields, descendings, false));
                            }
                        }

                        indexes.put(type.name(), indexesCol);
                    }

                    return new CacheSqlMetadata(cacheName, names, keyClasses, valClasses, fields, indexes);
                }
            });
        }
    }

    /**
     * Cache metadata.
     */
    public static class CacheSqlMetadata implements GridCacheSqlMetadata {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private String cacheName;

        /** */
        private Collection<String> types;

        /** */
        private Map<String, String> keyClasses;

        /** */
        private Map<String, String> valClasses;

        /** */
        private Map<String, Map<String, String>> fields;

        /** */
        private Map<String, Collection<GridCacheSqlIndexMetadata>> indexes;

        /**
         * Required by {@link Externalizable}.
         */
        public CacheSqlMetadata() {
            // No-op.
        }

        /**
         * @param cacheName Cache name.
         * @param types Types.
         * @param keyClasses Key classes map.
         * @param valClasses Value classes map.
         * @param fields Fields maps.
         * @param indexes Indexes.
         */
        CacheSqlMetadata(@Nullable String cacheName, Collection<String> types, Map<String, String> keyClasses,
            Map<String, String> valClasses, Map<String, Map<String, String>> fields,
            Map<String, Collection<GridCacheSqlIndexMetadata>> indexes) {
            assert types != null;
            assert keyClasses != null;
            assert valClasses != null;
            assert fields != null;
            assert indexes != null;

            this.cacheName = cacheName;
            this.types = types;
            this.keyClasses = keyClasses;
            this.valClasses = valClasses;
            this.fields = fields;
            this.indexes = indexes;
        }

        /**
         * @param metas Meta data instances from different nodes.
         */
        CacheSqlMetadata(Iterable<CacheSqlMetadata> metas) {
            types = new HashSet<>();
            keyClasses = new HashMap<>();
            valClasses = new HashMap<>();
            fields = new HashMap<>();
            indexes = new HashMap<>();

            for (CacheSqlMetadata meta : metas) {
                if (cacheName == null)
                    cacheName = meta.cacheName;
                else
                    assert F.eq(cacheName, meta.cacheName);

                types.addAll(meta.types);
                keyClasses.putAll(meta.keyClasses);
                valClasses.putAll(meta.valClasses);
                fields.putAll(meta.fields);
                indexes.putAll(meta.indexes);
            }
        }

        /** {@inheritDoc} */
        @Override public String cacheName() {
            return cacheName;
        }

        /** {@inheritDoc} */
        @Override public Collection<String> types() {
            return types;
        }

        /** {@inheritDoc} */
        @Override public String keyClass(String type) {
            return keyClasses.get(type);
        }

        /** {@inheritDoc} */
        @Override public String valueClass(String type) {
            return valClasses.get(type);
        }

        /** {@inheritDoc} */
        @Override public Map<String, String> fields(String type) {
            return fields.get(type);
        }

        /** {@inheritDoc} */
        @Override public Collection<String> notNullFields(String type) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Map<String, String> keyClasses() {
            return keyClasses;
        }

        /** {@inheritDoc} */
        @Override public Map<String, String> valClasses() {
            return valClasses;
        }

        /** {@inheritDoc} */
        @Override public Map<String, Map<String, String>> fields() {
            return fields;
        }

        /** {@inheritDoc} */
        @Override public Map<String, Collection<GridCacheSqlIndexMetadata>> indexes() {
            return indexes;
        }

        /** {@inheritDoc} */
        @Override public Collection<GridCacheSqlIndexMetadata> indexes(String type) {
            return indexes.get(type);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, cacheName);
            U.writeCollection(out, types);
            U.writeMap(out, keyClasses);
            U.writeMap(out, valClasses);
            U.writeMap(out, fields);
            U.writeMap(out, indexes);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            cacheName = U.readString(in);
            types = U.readCollection(in);
            keyClasses = U.readMap(in);
            valClasses = U.readMap(in);
            fields = U.readMap(in);
            indexes = U.readMap(in);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CacheSqlMetadata.class, this);
        }
    }

    /**
     * Cache metadata index.
     */
    public static class CacheSqlIndexMetadata implements GridCacheSqlIndexMetadata {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private String name;

        /** */
        private Collection<String> fields;

        /** */
        private Collection<String> descendings;

        /** */
        private boolean unique;

        /**
         * Required by {@link Externalizable}.
         */
        public CacheSqlIndexMetadata() {
            // No-op.
        }

        /**
         * @param name Index name.
         * @param fields Fields.
         * @param descendings Descendings.
         * @param unique Unique flag.
         */
        CacheSqlIndexMetadata(String name, Collection<String> fields, Collection<String> descendings,
            boolean unique) {
            assert name != null;
            assert fields != null;
            assert descendings != null;

            this.name = name;
            this.fields = fields;
            this.descendings = descendings;
            this.unique = unique;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public Collection<String> fields() {
            return fields;
        }

        /** {@inheritDoc} */
        @Override public boolean descending(String field) {
            return descendings.contains(field);
        }

        /** {@inheritDoc} */
        @Override public Collection<String> descendings() {
            return descendings;
        }

        /** {@inheritDoc} */
        @Override public boolean unique() {
            return unique;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, name);
            U.writeCollection(out, fields);
            U.writeCollection(out, descendings);
            out.writeBoolean(unique);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            name = U.readString(in);
            fields = U.readCollection(in);
            descendings = U.readCollection(in);
            unique = in.readBoolean();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CacheSqlIndexMetadata.class, this);
        }
    }

    /**
     *
     */
    private static class QueryResult<K, V> extends CachedResult<IgniteBiTuple<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final GridCacheQueryType type;

        /**
         * @param type Query type.
         * @param rcpt ID of the recipient.
         */
        private QueryResult(GridCacheQueryType type, Object rcpt) {
            super(rcpt);

            this.type = type;
        }

        /**
         * @return Type.
         */
        public GridCacheQueryType type() {
            return type;
        }
    }

    /**
     *
     */
    private static class FieldsResult<Q> extends CachedResult<Q> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private List<GridQueryFieldMetadata> meta;

        /**
         * @param rcpt ID of the recipient.
         */
        FieldsResult(Object rcpt) {
            super(rcpt);
        }

        /**
         * @return Metadata.
         * @throws IgniteCheckedException On error.
         */
        public List<GridQueryFieldMetadata> metaData() throws IgniteCheckedException {
            get(); // Ensure that result is ready.

            return meta;
        }

        /**
         * @param meta Metadata.
         */
        public void metaData(List<GridQueryFieldMetadata> meta) {
            this.meta = meta;
        }
    }

    /**
     * Cached result.
     */
    private abstract static class CachedResult<R> extends GridFutureAdapter<IgniteSpiCloseableIterator<R>> {
        /** Absolute position of each recipient. */
        private final Map<Object, QueueIterator> recipients = new GridLeanMap<>(1);

        /** */
        private CircularQueue<R> queue;

        /** */
        private int pruned;

        /**
         * @param rcpt ID of the recipient.
         */
        protected CachedResult(Object rcpt) {
            boolean res = addRecipient(rcpt);

            assert res;
        }

        /**
         * Close if this result does not have any other recipients.
         *
         * @param rcpt ID of the recipient.
         * @throws IgniteCheckedException If failed.
         */
        public void closeIfNotShared(Object rcpt) throws IgniteCheckedException {
            assert isDone();

            synchronized (recipients) {
                if (recipients.isEmpty())
                    return;

                recipients.remove(rcpt);

                if (recipients.isEmpty())
                    get().close();
            }
        }

        /**
         * @param rcpt ID of the recipient.
         * @return {@code true} If the recipient successfully added.
         */
        public boolean addRecipient(Object rcpt) {
            synchronized (recipients) {
                if (isDone())
                    return false;

                assert !recipients.containsKey(rcpt) : rcpt + " -> " + recipients;

                recipients.put(rcpt, new QueueIterator(rcpt));
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable IgniteSpiCloseableIterator<R> res, @Nullable Throwable err) {
            assert !isDone();

            synchronized (recipients) {
                if (recipients.size() > 1) {
                    queue = new CircularQueue<>(128);

                    for (QueueIterator it : recipients.values())
                        it.init();
                }

                return super.onDone(res, err);
            }
        }

        /**
         *
         */
        private void pruneQueue() {
            assert !recipients.isEmpty();
            assert Thread.holdsLock(recipients);

            int minPos = Collections.min(recipients.values()).pos;

            if (minPos > pruned) {
                queue.remove(minPos - pruned);

                pruned = minPos;
            }
        }

        /**
         * @param rcpt ID of the recipient.
         * @throws IgniteCheckedException If failed.
         */
        public IgniteSpiCloseableIterator<R> iterator(Object rcpt) throws IgniteCheckedException {
            assert rcpt != null;

            IgniteSpiCloseableIterator<R> it = get();

            assert it != null;

            synchronized (recipients) {
                return queue == null ? it : recipients.get(rcpt);
            }
        }

        /**
         *
         */
        @SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
        private class QueueIterator implements IgniteSpiCloseableIterator<R>, Comparable<QueueIterator> {
            /** */
            private static final long serialVersionUID = 0L;

            /** */
            private static final int NEXT_SIZE = 64;

            /** */
            private final Object rcpt;

            /** */
            private int pos;

            /** */
            private Queue<R> next;

            /**
             * @param rcpt ID of the recipient.
             */
            private QueueIterator(Object rcpt) {
                this.rcpt = rcpt;
            }

            /**
             */
            public void init() {
                assert next == null;

                next = new ArrayDeque<>(NEXT_SIZE);
            }

            /** {@inheritDoc} */
            @Override public void close() throws IgniteCheckedException {
                closeIfNotShared(rcpt);
            }

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return !next.isEmpty() || fillNext();
            }

            /** {@inheritDoc} */
            @SuppressWarnings("IteratorNextCanNotThrowNoSuchElementException") // It can actually.
            @Override public R next() {
                return next.remove();
            }

            /**
             * @return {@code true} If elements were fetched into local queue of the iterator.
             */
            private boolean fillNext() {
                assert next.isEmpty();

                IgniteSpiCloseableIterator<R> it;

                try {
                    it = get();
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }

                synchronized (recipients) {
                    for (int i = 0; i < NEXT_SIZE; i++) {
                        R res;

                        int off = pos - pruned; // Offset of current iterator relative to queue begin.

                        if (off == queue.size()) { // We are leading the race.
                            if (!it.hasNext())
                                break; // Happy end.

                            res = it.next();

                            queue.add(res);
                        }
                        else // Someone fetched result into queue before us.
                            res = queue.get(off);

                        assert res != null;

                        pos++;
                        next.add(res);
                    }

                    pruneQueue();
                }

                return !next.isEmpty();
            }

            /** {@inheritDoc} */
            @Override public void remove() {
                throw new UnsupportedOperationException();
            }

            /** {@inheritDoc} */
            @Override public int compareTo(QueueIterator o) {
                return Integer.compare(pos, o.pos);
            }
        }
    }

    /**
     * Queue.
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    static class CircularQueue<R> {
        /** */
        private int off;

        /** */
        private int size;

        /** */
        private R[] arr;

        /**
         * @param cap Initial capacity.
         */
        CircularQueue(int cap) {
            assert U.isPow2(cap);

            arr = (R[])new Object[cap];
        }

        /**
         * @param o Object to add.
         */
        public void add(R o) {
            if (size == arr.length) { // Resize.
                Object[] newArr = new Object[arr.length << 1];

                int tailSize = arr.length - off;

                System.arraycopy(arr, off, newArr, 0, tailSize);

                if (off != 0) {
                    System.arraycopy(arr, 0, newArr, tailSize, off);

                    off = 0;
                }

                arr = (R[])newArr;
            }

            int idx = (off + size) & (arr.length - 1);

            assert arr[idx] == null;

            arr[idx] = o;

            size++;
        }

        /**
         * @param n Number of elements to remove.
         */
        public void remove(int n) {
            assert n > 0 : n;
            assert n <= size : n + " " + size;

            int mask = arr.length - 1;

            for (int i = 0; i < n; i++) {
                int idx = (off + i) & mask;

                assert arr[idx] != null;

                arr[idx] = null;
            }

            size -= n;
            off += n;

            if (off >= arr.length)
                off -= arr.length;
        }

        /**
         * @param idx Index in queue.
         * @return Element at the given index.
         */
        public R get(int idx) {
            assert idx >= 0 : idx;
            assert idx < size : idx + " " + size;

            R res = arr[(idx + off) & (arr.length - 1)];

            assert res != null;

            return res;
        }

        /**
         * @return Size.
         */
        public int size() {
            return size;
        }
    }

    /**
     * Query for {@link IndexingSpi}.
     *
     * @param keepBinary Keep binary flag.
     * @return Query.
     */
    public <R> CacheQuery<R> createSpiQuery(boolean keepBinary) {
        return new GridCacheQueryAdapter<>(cctx,
            SPI,
            null,
            null,
            null,
            null,
            false,
            keepBinary);
    }

    /**
     * Creates user's predicate based scan query.
     *
     * @param filter Scan filter.
     * @param part Partition.
     * @param keepBinary Keep binary flag.
     * @return Created query.
     */
    public <R> CacheQuery<R> createScanQuery(@Nullable IgniteBiPredicate<K, V> filter,
        @Nullable Integer part, boolean keepBinary) {
        return createScanQuery(filter, null, part, keepBinary);
    }

    /**
     * Creates user's predicate based scan query.
     *
     * @param filter Scan filter.
     * @param part Partition.
     * @param keepBinary Keep binary flag.
     * @return Created query.
     */
    public <T, R> CacheQuery<R> createScanQuery(@Nullable IgniteBiPredicate<K, V> filter,
        @Nullable IgniteClosure<T, R> trans,
        @Nullable Integer part, boolean keepBinary) {

        return new GridCacheQueryAdapter(cctx,
            SCAN,
            filter,
            trans,
            part,
            keepBinary);
    }

    /**
     * Creates user's full text query, queried class, and query clause. For more information refer to {@link CacheQuery}
     * documentation.
     *
     * @param clsName Query class name.
     * @param search Search clause.
     * @param keepBinary Keep binary flag.
     * @return Created query.
     */
    public CacheQuery<Map.Entry<K, V>> createFullTextQuery(String clsName,
        String search, boolean keepBinary) {
        A.notNull("clsName", clsName);
        A.notNull("search", search);

        return new GridCacheQueryAdapter<>(cctx,
            TEXT,
            clsName,
            search,
            null,
            null,
            false,
            keepBinary);
    }

    /**
     * Creates user's SQL fields query for given clause. For more information refer to {@link CacheQuery}
     * documentation.
     *
     * @param qry Query.
     * @param keepBinary Keep binary flag.
     * @return Created query.
     */
    public CacheQuery<List<?>> createSqlFieldsQuery(String qry, boolean keepBinary) {
        A.notNull(qry, "qry");

        return new GridCacheQueryAdapter<>(cctx,
            SQL_FIELDS,
            null,
            qry,
            null,
            null,
            false,
            keepBinary);
    }

    /**
     * The map prevents put to the map in case the specified request has been removed previously.
     */
    private class RequestFutureMap extends LinkedHashMap<Long, GridFutureAdapter<QueryResult<K, V>>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Count of canceled keys */
        private static final int CANCELED_COUNT = 128;

        /**
         * The ID of the canceled request is stored to the set in case remove(reqId) is called before put(reqId,
         * future).
         */
        private Set<Long> canceled;

        /** {@inheritDoc} */
        @Override public GridFutureAdapter<QueryResult<K, V>> remove(Object key) {
            if (containsKey(key))
                return super.remove(key);
            else {
                if (canceled == null) {
                    canceled = Collections.newSetFromMap(
                        new LinkedHashMap<Long, Boolean>() {
                            @Override protected boolean removeEldestEntry(Map.Entry<Long, Boolean> eldest) {
                                return size() > CANCELED_COUNT;
                            }
                        });
                }

                canceled.add((Long)key);

                return null;
            }
        }

        /**
         * @return true if the key is canceled
         */
        boolean isCanceled(Long key) {
            return canceled != null && canceled.contains(key);
        }
    }

    /** */
    private static final class ScanQueryIterator<K, V> extends GridCloseableIteratorAdapter<Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final GridDhtCacheAdapter dht;

        /** */
        private final GridDhtLocalPartition locPart;

        /** */
        private final IgniteBiPredicate<K, V> scanFilter;

        /** */
        private final boolean statsEnabled;

        /** */
        private final GridIterator<CacheDataRow> it;

        /** */
        private final GridCacheAdapter cache;

        /** */
        private final AffinityTopologyVersion topVer;

        /** */
        private final boolean keepBinary;

        /** */
        private final boolean readEvt;

        /** */
        private final String cacheName;

        /** */
        private final UUID subjId;

        /** */
        private final String taskName;

        /** */
        private final IgniteClosure transform;

        /** */
        private final CacheObjectContext objCtx;

        /** */
        private final GridCacheContext cctx;

        /** */
        private final IgniteLogger log;

        /** */
        private Object next;

        /** */
        private boolean needAdvance;

        /** */
        private IgniteCacheExpiryPolicy expiryPlc;

        /**
         * @param it Iterator.
         * @param qry Query.
         * @param topVer Topology version.
         * @param locPart Local partition.
         * @param scanFilter Scan filter.
         * @param locNode Local node flag.
         * @param cctx Cache context.
         * @param log Logger.
         */
        ScanQueryIterator(
            GridIterator<CacheDataRow> it,
            GridCacheQueryAdapter qry,
            AffinityTopologyVersion topVer,
            GridDhtLocalPartition locPart,
            IgniteBiPredicate<K, V> scanFilter,
            boolean locNode,
            GridCacheContext cctx,
            IgniteLogger log) {
            this.it = it;
            this.topVer = topVer;
            this.locPart = locPart;
            this.scanFilter = scanFilter;
            this.cctx = cctx;
            this.log = log;

            statsEnabled = locNode && cctx.statisticsEnabled();

            readEvt = locNode && cctx.events().isRecordable(EVT_CACHE_QUERY_OBJECT_READ);

            if(readEvt){
                taskName = cctx.kernalContext().task().resolveTaskName(qry.taskHash());
                subjId = qry.subjectId();
            }
            else {
                taskName = null;
                subjId = null;
            }

            // keep binary for remote scans if possible
            keepBinary = (!locNode && scanFilter == null) || qry.keepBinary();
            transform = qry.transform();
            dht = cctx.isLocal() ? null : (cctx.isNear() ? cctx.near().dht() : cctx.dht());
            cache = dht != null ? dht : cctx.cache();
            objCtx = cctx.cacheObjectContext();
            cacheName = cctx.name();

            needAdvance = true;
            expiryPlc = this.cctx.cache().expiryPolicy(null);
        }

        /** {@inheritDoc} */
        @Override protected Object onNext() {
            if (needAdvance)
                advance();
            else
                needAdvance = true;

            if (next == null)
                throw new NoSuchElementException();

            return next;
        }

        /** {@inheritDoc} */
        @Override protected boolean onHasNext() {
            if (needAdvance) {
                advance();

                needAdvance = false;
            }

            return next != null;
        }

        /** {@inheritDoc} */
        @Override protected void onClose() {
            if (expiryPlc != null && dht != null) {
                dht.sendTtlUpdateRequest(expiryPlc);

                expiryPlc = null;
            }

            if (locPart != null)
                locPart.release();

            closeScanFilter(scanFilter);
        }

        /**
         * Moves the iterator to the next cache entry.
         */
        private void advance() {
            long start = statsEnabled ? System.nanoTime() : 0L;

            Object next = null;

            while (it.hasNext()) {
                CacheDataRow row = it.next();

                KeyCacheObject key = row.key();
                CacheObject val;

                if (expiryPlc != null) {
                    try {
                        CacheDataRow tmp = row;

                        while (true) {
                            try {
                                GridCacheEntryEx entry = cache.entryEx(key);

                                entry.unswap(tmp);

                                val = entry.peek(true, true, topVer, expiryPlc);

                                cctx.evicts().touch(entry, topVer);

                                break;
                            }
                            catch (GridCacheEntryRemovedException ignore) {
                                tmp = null;
                            }
                        }
                    }
                    catch (IgniteCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to peek value: " + e);

                        val = null;
                    }

                    if (dht != null && expiryPlc.readyToFlush(100))
                        dht.sendTtlUpdateRequest(expiryPlc);
                }
                else
                    val = row.value();

                if (val != null) {
                    K key0 = (K)CacheObjectUtils.unwrapBinaryIfNeeded(objCtx, key, keepBinary, false);
                    V val0 = (V)CacheObjectUtils.unwrapBinaryIfNeeded(objCtx, val, keepBinary, false);

                    if (statsEnabled) {
                        CacheMetricsImpl metrics = cctx.cache().metrics0();

                        metrics.onRead(true);

                        metrics.addGetTimeNanos(System.nanoTime() - start);
                    }

                    if (scanFilter == null || scanFilter.apply(key0, val0)) {
                        if (readEvt && cctx.gridEvents().hasListener(EVT_CACHE_QUERY_OBJECT_READ)) {
                            cctx.gridEvents().record(new CacheQueryReadEvent<>(
                                cctx.localNode(),
                                "Scan query entry read.",
                                EVT_CACHE_QUERY_OBJECT_READ,
                                CacheQueryType.SCAN.name(),
                                cacheName,
                                null,
                                null,
                                scanFilter,
                                null,
                                null,
                                subjId,
                                taskName,
                                key0,
                                val0,
                                null,
                                null));
                        }

                        next = transform == null ? new CacheQueryEntry<>(key0, val0)
                            : transform.apply(new CacheQueryEntry<>(key0, val0));

                        break;
                    }
                }
            }

            if ((this.next = next) == null && expiryPlc != null && dht != null) {
                dht.sendTtlUpdateRequest(expiryPlc);

                expiryPlc = null;
            }
        }
    }
}
