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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.QueryMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheQueryExecutedEvent;
import org.apache.ignite.events.CacheQueryReadEvent;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheMetricsImpl;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheInternal;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheOffheapSwapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheSwapEntryImpl;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtUnreservedPartitionException;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.datastructures.GridSetQueryPredicate;
import org.apache.ignite.internal.processors.datastructures.SetItemKey;
import org.apache.ignite.internal.processors.platform.cache.PlatformCacheEntryFilter;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResult;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryIndexType;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.GridEmptyCloseableIterator;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.GridSpiCloseableIteratorWrapper;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.lang.GridIteratorAdapter;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.CX2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.IgniteSpiCloseableIterator;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingSpi;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_OBJECT_READ;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridClosureCallMode.BROADCAST;
import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;
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
    /** */
    public static int MAX_ITERATORS = 1000;

    /** */
    protected GridQueryProcessor qryProc;

    /** */
    private String space;

    /** */
    private int maxIterCnt;

    /** */
    private volatile GridCacheQueryMetricsAdapter metrics = new GridCacheQueryMetricsAdapter();

    /** */
    private final ConcurrentMap<UUID, Map<Long, GridFutureAdapter<QueryResult<K, V>>>> qryIters =
        new ConcurrentHashMap8<>();

    /** */
    private final ConcurrentMap<UUID, Map<Long, GridFutureAdapter<FieldsResult>>> fieldsQryRes =
        new ConcurrentHashMap8<>();

    /** */
    private volatile ConcurrentMap<Object, CachedResult<?>> qryResCache = new ConcurrentHashMap8<>();

    /** */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Event listener. */
    private GridLocalEventListener lsnr;

    /** */
    private boolean enabled;

    /** */
    private AffinityTopologyVersion qryTopVer;

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        qryProc = cctx.kernalContext().query();
        space = cctx.name();
        maxIterCnt = MAX_ITERATORS;

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

        enabled = GridQueryProcessor.isEnabled(cctx.config());

        qryTopVer = cctx.startTopologyVersion();

        if (qryTopVer == null)
            qryTopVer = new AffinityTopologyVersion(cctx.localNode().order(), 0);
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
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    @Override public final void stop0(boolean cancel) {
        if (log.isDebugEnabled())
            log.debug("Stopped cache query manager.");
    }

    /**
     * Gets number of objects of given type in index.
     *
     * @param valType Value type.
     * @return Number of objects or -1 if type was not indexed at all.
     * @throws IgniteCheckedException If failed.
     */
    public long size(Class<?> valType) throws IgniteCheckedException {
        if (!enterBusy())
            throw new IllegalStateException("Failed to get size (grid is stopping).");

        try {
            return qryProc.size(space, valType);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * Rebuilds all search indexes of given value type.
     *
     * @param typeName Value type name.
     * @return Future that will be completed when rebuilding of all indexes is finished.
     */
    public IgniteInternalFuture<?> rebuildIndexes(String typeName) {
        if (!enterBusy())
            throw new IllegalStateException("Failed to rebuild indexes (grid is stopping).");

        try {
            return qryProc.rebuildIndexes(space, typeName);
        }
        finally {
            leaveBusy();
        }
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
     * Entry for given key unswapped.
     *
     * @param key Key.
     * @throws IgniteCheckedException If failed.
     */
    public void onSwap(CacheObject key) throws IgniteCheckedException {
        if (!enterBusy())
            return; // Ignore index update when node is stopping.

        try {
            qryProc.onSwap(space, key);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * Entry for given key unswapped.
     *
     * @param key Key.
     * @param val Value
     * @throws IgniteCheckedException If failed.
     */
    public void onUnswap(CacheObject key, CacheObject val) throws IgniteCheckedException {
        if (!enterBusy())
            return; // Ignore index update when node is stopping.

        try {
            qryProc.onUnswap(space, key, val);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     *
     */
    private void invalidateResultCache() {
        if (!qryResCache.isEmpty())
            qryResCache = new ConcurrentHashMap8<>();
    }

    /**
     * Writes key-value pair to index.
     *
     * @param key Key.
     * @param val Value.
     * @param ver Cache entry version.
     * @param expirationTime Expiration time or 0 if never expires.
     * @throws IgniteCheckedException In case of error.
     */
    public void store(CacheObject key, CacheObject val, GridCacheVersion ver, long expirationTime)
        throws IgniteCheckedException {
        assert key != null;
        assert val != null;
        assert enabled();

        if (key instanceof GridCacheInternal)
            return; // No-op.

        if (!enterBusy())
            return; // Ignore index update when node is stopping.

        try {
            qryProc.store(space, key, val, CU.versionToBytes(ver), expirationTime);
        }
        finally {
            invalidateResultCache();

            leaveBusy();
        }
    }

    /**
     * @param key Key.
     * @param val Value.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    @SuppressWarnings("SimplifiableIfStatement")
    public void remove(CacheObject key, CacheObject val) throws IgniteCheckedException {
        assert key != null;

        if (!GridQueryProcessor.isEnabled(cctx.config()) && !(key instanceof GridCacheInternal))
            return; // No-op.

        if (!enterBusy())
            return; // Ignore index update when node is stopping.

        try {
            qryProc.remove(space, key, val);
        }
        finally {
            invalidateResultCache();

            leaveBusy();
        }
    }

    /**
     * Undeploys given class loader.
     *
     * @param ldr Class loader to undeploy.
     */
    public void onUndeploy(ClassLoader ldr) {
        if (!enterBusy())
            return; // Ignore index update when node is stopping.

        try {
            qryProc.onUndeploy(space, ldr);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
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
    public abstract CacheQueryFuture<?> queryLocal(GridCacheQueryBean qry);

    /**
     * Executes distributed query.
     *
     * @param qry Query.
     * @param nodes Nodes.
     * @return Query future.
     */
    public abstract CacheQueryFuture<?> queryDistributed(GridCacheQueryBean qry, Collection<ClusterNode> nodes);

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
                "GridCacheConfiguration.getMaximumQueryIteratorCount() configuration property).");
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
                    if (cctx.gridEvents().isRecordable(EVT_CACHE_QUERY_EXECUTED)) {
                        cctx.gridEvents().record(new CacheQueryExecutedEvent<>(
                            cctx.localNode(),
                            "SQL query executed.",
                            EVT_CACHE_QUERY_EXECUTED,
                            CacheQueryType.SQL.name(),
                            cctx.namex(),
                            qry.queryClassName(),
                            qry.clause(),
                            null,
                            null,
                            args,
                            subjId,
                            taskName));
                    }

                    iter = qryProc.query(space, qry.clause(), F.asList(args),
                        qry.queryClassName(), filter(qry));

                    break;

                case SCAN:
                    if (cctx.gridEvents().isRecordable(EVT_CACHE_QUERY_EXECUTED)) {
                        cctx.gridEvents().record(new CacheQueryExecutedEvent<>(
                            cctx.localNode(),
                            "Scan query executed.",
                            EVT_CACHE_QUERY_EXECUTED,
                            CacheQueryType.SCAN.name(),
                            cctx.namex(),
                            null,
                            null,
                            qry.scanFilter(),
                            null,
                            null,
                            subjId,
                            taskName));
                    }

                    iter = scanIterator(qry);

                    break;

                case TEXT:
                    if (cctx.gridEvents().isRecordable(EVT_CACHE_QUERY_EXECUTED)) {
                        cctx.gridEvents().record(new CacheQueryExecutedEvent<>(
                            cctx.localNode(),
                            "Full text query executed.",
                            EVT_CACHE_QUERY_EXECUTED,
                            CacheQueryType.FULL_TEXT.name(),
                            cctx.namex(),
                            qry.queryClassName(),
                            qry.clause(),
                            null,
                            null,
                            null,
                            subjId,
                            taskName));
                    }

                    iter = qryProc.queryText(space, qry.clause(), qry.queryClassName(), filter(qry));

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

        if (qry.clause() == null) {
            assert !loc;

            throw new IgniteCheckedException("Received next page request after iterator was removed. " +
                "Consider increasing maximum number of stored iterators (see " +
                "GridCacheConfiguration.getMaximumQueryIteratorCount() configuration property).");
        }

        if (qry.type() == SQL_FIELDS) {
            if (cctx.gridEvents().isRecordable(EVT_CACHE_QUERY_EXECUTED)) {
                cctx.gridEvents().record(new CacheQueryExecutedEvent<>(
                    cctx.localNode(),
                    "SQL fields query executed.",
                    EVT_CACHE_QUERY_EXECUTED,
                    CacheQueryType.SQL_FIELDS.name(),
                    cctx.namex(),
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

            if (cctx.gridEvents().isRecordable(EVT_CACHE_QUERY_EXECUTED)) {
                cctx.gridEvents().record(new CacheQueryExecutedEvent<>(
                    cctx.localNode(),
                    "SPI query executed.",
                    EVT_CACHE_QUERY_EXECUTED,
                    CacheQueryType.SPI.name(),
                    cctx.namex(),
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
                IgniteSpiCloseableIterator<?> iter = cctx.kernalContext().indexing().query(space, F.asList(args),
                    filter(qry));

                res.onDone(iter);
            }
            else {
                assert qry.type() == SQL_FIELDS;

                GridQueryFieldsResult qryRes = qryProc.queryFields(space, qry.clause(), F.asList(args), filter(qry));

                res.metaData(qryRes.metaData());

                res.onDone(qryRes.iterator());
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
     * @return Full-scan row iterator.
     * @throws IgniteCheckedException If failed to get iterator.
     */
    @SuppressWarnings({"unchecked"})
    private GridCloseableIterator<IgniteBiTuple<K, V>> scanIterator(final GridCacheQueryAdapter<?> qry)
        throws IgniteCheckedException {
        IgniteInternalCache<K, V> prj0 = cctx.cache();

        prj0 = prj0.keepBinary();

        final IgniteInternalCache<K, V> prj = prj0;

        final IgniteBiPredicate<K, V> keyValFilter = qry.scanFilter();

        try {
            injectResources(keyValFilter);

            final GridDhtCacheAdapter dht = cctx.isLocal() ? null : (cctx.isNear() ? cctx.near().dht() : cctx.dht());

            final GridCacheAdapter cache = dht != null ? dht : cctx.cache();

            final ExpiryPolicy plc = cctx.expiry();

            final AffinityTopologyVersion topVer = cctx.affinity().affinityTopologyVersion();

            final boolean backups = qry.includeBackups() || cctx.isReplicated();

            final GridCloseableIteratorAdapter<IgniteBiTuple<K, V>> heapIt =
                new GridCloseableIteratorAdapter<IgniteBiTuple<K, V>>() {
                    private IgniteBiTuple<K, V> next;

                    private IgniteCacheExpiryPolicy expiryPlc = cctx.cache().expiryPolicy(plc);

                    private Iterator<K> iter;

                    private GridDhtLocalPartition locPart;

                    {
                        Integer part = qry.partition();

                        if (part == null || dht == null)
                            iter = backups ? prj.keySetx().iterator() : prj.primaryKeySet().iterator();
                        else if (part < 0 || part >= cctx.affinity().partitions())
                            iter = F.emptyIterator();
                        else {
                            locPart = dht.topology().localPartition(part, topVer, false);

                            // double check for owning state
                            if (locPart == null || locPart.state() != OWNING || !locPart.reserve() ||
                                locPart.state() != OWNING)
                                throw new GridDhtUnreservedPartitionException(part,
                                    cctx.affinity().affinityTopologyVersion(), "Partition can not be reserved");

                            iter = new Iterator<K>() {
                                private Iterator<KeyCacheObject> iter0 = locPart.keySet().iterator();

                                @Override public boolean hasNext() {
                                    return iter0.hasNext();
                                }

                                @Override public K next() {
                                    KeyCacheObject key = iter0.next();

                                    return (K)cctx.unwrapBinaryIfNeeded(key, true);
                                }

                                @Override public void remove() {
                                    iter0.remove();
                                }
                            };
                        }

                        advance();
                    }

                    @Override public boolean onHasNext() {
                        return next != null;
                    }

                    @Override public IgniteBiTuple<K, V> onNext() {
                        if (next == null)
                            throw new NoSuchElementException();

                        IgniteBiTuple<K, V> next0 = next;

                        advance();

                        return next0;
                    }

                    private void advance() {
                        IgniteBiTuple<K, V> next0 = null;

                        while (iter.hasNext()) {
                            next0 = null;

                            K key = iter.next();

                            V val;

                            try {
                                GridCacheEntryEx entry = cache.peekEx(key);

                                CacheObject cacheVal =
                                    entry != null ? entry.peek(true, false, false, topVer, expiryPlc) : null;

                                val = (V)cctx.cacheObjectContext().unwrapBinaryIfNeeded(cacheVal, true);
                            }
                            catch (GridCacheEntryRemovedException e) {
                                val = null;
                            }
                            catch (IgniteCheckedException e) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to peek value: " + e);

                                val = null;
                            }

                            if (dht != null && expiryPlc != null && expiryPlc.readyToFlush(100)) {
                                dht.sendTtlUpdateRequest(expiryPlc);

                                expiryPlc = cctx.cache().expiryPolicy(plc);
                            }

                            if (val != null) {
                                next0 = F.t(key, val);

                                if (checkPredicate(next0))
                                    break;
                                else
                                    next0 = null;
                            }
                        }

                        next = next0 != null ?
                            new IgniteBiTuple<>(next0.getKey(), next0.getValue()) :
                            null;

                        if (next == null)
                            sendTtlUpdate();
                    }

                    @Override protected void onClose() {
                        sendTtlUpdate();

                        if (locPart != null)
                            locPart.release();
                    }

                    private void sendTtlUpdate() {
                        if (dht != null && expiryPlc != null) {
                            dht.sendTtlUpdateRequest(expiryPlc);

                            expiryPlc = null;
                        }
                    }

                    private boolean checkPredicate(Map.Entry<K, V> e) {
                        if (keyValFilter != null) {
                            Map.Entry<K, V> e0 = (Map.Entry<K, V>)cctx.unwrapBinaryIfNeeded(e, qry.keepBinary());

                            return keyValFilter.apply(e0.getKey(), e0.getValue());
                        }

                        return true;
                    }
                };

            final GridIterator<IgniteBiTuple<K, V>> it;

            if (cctx.isSwapOrOffheapEnabled()) {
                List<GridIterator<IgniteBiTuple<K, V>>> iters = new ArrayList<>(3);

                iters.add(heapIt);

                if (cctx.isOffHeapEnabled())
                    iters.add(offheapIterator(qry, backups));

                if (cctx.swap().swapEnabled())
                    iters.add(swapIterator(qry, backups));

                it = new CompoundIterator<>(iters);
            }
            else
                it = heapIt;

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

                @Override protected void onClose() throws IgniteCheckedException {
                    try {
                        heapIt.close();
                    }
                    finally {
                        closeScanFilter(keyValFilter);
                    }
                }
            };
        }
        catch (IgniteCheckedException | RuntimeException e)
        {
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
     * @param qry Query.
     * @param backups Include backups.
     * @return Swap iterator.
     * @throws IgniteCheckedException If failed.
     */
    private GridIterator<IgniteBiTuple<K, V>> swapIterator(GridCacheQueryAdapter<?> qry, boolean backups)
        throws IgniteCheckedException {
        IgniteBiPredicate<K, V> filter = qry.scanFilter();

        Integer part = qry.partition();

        Iterator<Map.Entry<byte[], byte[]>> it = part == null ? cctx.swap().rawSwapIterator(true, backups) :
            cctx.swap().rawSwapIterator(part);

        return scanIterator(it, filter, qry.keepBinary());
    }

    /**
     * @param qry Query.
     * @param backups Include backups.
     * @return Offheap iterator.
     */
    private GridIterator<IgniteBiTuple<K, V>> offheapIterator(GridCacheQueryAdapter<?> qry, boolean backups) {
        IgniteBiPredicate<K, V> filter = qry.scanFilter();

        if (cctx.offheapTiered() && filter != null) {
            OffheapIteratorClosure c = new OffheapIteratorClosure(filter, qry.keepBinary());

            return cctx.swap().rawOffHeapIterator(c, qry.partition(), true, backups);
        }
        else {
            Iterator<Map.Entry<byte[], byte[]>> it = cctx.swap().rawOffHeapIterator(qry.partition(), true, backups);

            return scanIterator(it, filter, qry.keepBinary());
        }
    }

    /**
     * @param it Lazy swap or offheap iterator.
     * @param filter Scan filter.
     * @param keepBinary Keep binary flag.
     * @return Iterator.
     */
    private GridIteratorAdapter<IgniteBiTuple<K, V>> scanIterator(
        @Nullable final Iterator<Map.Entry<byte[], byte[]>> it,
        @Nullable final IgniteBiPredicate<K, V> filter,
        final boolean keepBinary) {
        if (it == null)
            return new GridEmptyCloseableIterator<>();

        return new GridIteratorAdapter<IgniteBiTuple<K, V>>() {
            private IgniteBiTuple<K, V> next;

            {
                advance();
            }

            @Override public boolean hasNextX() {
                return next != null;
            }

            @Override public IgniteBiTuple<K, V> nextX() {
                if (next == null)
                    throw new NoSuchElementException();

                IgniteBiTuple<K, V> next0 = next;

                advance();

                return next0;
            }

            @Override public void removeX() {
                throw new UnsupportedOperationException();
            }

            private void advance() {
                next = null;

                while (it.hasNext()) {
                    final LazySwapEntry e = new LazySwapEntry(it.next(), keepBinary);

                    if (filter != null) {
                        K key = (K)cctx.unwrapBinaryIfNeeded(e.key(), keepBinary);
                        V val = (V)cctx.unwrapBinaryIfNeeded(e.value(), keepBinary);

                        if (!filter.apply(key, val))
                            continue;
                    }

                    next = new IgniteBiTuple<>(e.key(), e.value());

                    break;
                }
            }
        };
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

            final boolean statsEnabled = cctx.config().isStatisticsEnabled();

            final boolean readEvt = cctx.gridEvents().isRecordable(EVT_CACHE_QUERY_OBJECT_READ);

            try {
                // Preparing query closures.
                IgniteClosure<Object, Object> trans = (IgniteClosure<Object, Object>)qryInfo.transformer();
                IgniteReducer<Object, Object> rdc = (IgniteReducer<Object, Object>)qryInfo.reducer();

                injectResources(trans);
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

                    if (readEvt) {
                        cctx.gridEvents().record(new CacheQueryReadEvent<K, V>(
                            cctx.localNode(),
                            "SQL fields query result set row read.",
                            EVT_CACHE_QUERY_OBJECT_READ,
                            CacheQueryType.SQL_FIELDS.name(),
                            cctx.namex(),
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

            try {
                // Preparing query closures.
                IgniteClosure<Map.Entry<K, V>, Object> trans =
                    (IgniteClosure<Map.Entry<K, V>, Object>)qryInfo.transformer();

                IgniteReducer<Map.Entry<K, V>, Object> rdc = (IgniteReducer<Map.Entry<K, V>, Object>)qryInfo.reducer();

                injectResources(trans);
                injectResources(rdc);

                GridCacheQueryAdapter<?> qry = qryInfo.query();

                int pageSize = qry.pageSize();

                boolean incBackups = qry.includeBackups();

                String taskName = cctx.kernalContext().task().resolveTaskName(qry.taskHash());

                IgniteSpiCloseableIterator<IgniteBiTuple<K, V>> iter;
                GridCacheQueryType type;

                res = loc ?
                    executeQuery(qry, qryInfo.arguments(), loc, qry.subjectId(), taskName,
                    recipient(qryInfo.senderId(), qryInfo.requestId())) :
                    queryResult(qryInfo, taskName);

                iter = res.iterator(recipient(qryInfo.senderId(), qryInfo.requestId()));
                type = res.type();

                GridCacheAdapter<K, V> cache = cctx.cache();

                if (log.isDebugEnabled())
                    log.debug("Received index iterator [iterHasNext=" + iter.hasNext() +
                        ", cacheSize=" + cache.size() + ']');

                int cnt = 0;

                boolean stop = false;
                boolean pageSent = false;

                Collection<Object> data = new ArrayList<>(pageSize);

                AffinityTopologyVersion topVer = cctx.affinity().affinityTopologyVersion();

                final boolean statsEnabled = cctx.config().isStatisticsEnabled();

                final boolean readEvt = cctx.gridEvents().isRecordable(EVT_CACHE_QUERY_OBJECT_READ);

                while (!Thread.currentThread().isInterrupted() && iter.hasNext()) {
                    long start = statsEnabled ? System.nanoTime() : 0L;

                    IgniteBiTuple<K, V> row = iter.next();

                    // Query is cancelled.
                    if (row == null) {
                        onPageReady(loc, qryInfo, null, true, null);

                        break;
                    }

                    K key = row.getKey();

                    // Filter backups for SCAN queries, if it isn't partition scan.
                    // Other types are filtered in indexing manager.
                    if (!cctx.isReplicated() && qry.type() == SCAN && qry.partition() == null &&
                        cctx.config().getCacheMode() != LOCAL && !incBackups &&
                        !cctx.affinity().primary(cctx.localNode(), key, topVer)) {
                        if (log.isDebugEnabled())
                            log.debug("Ignoring backup element [row=" + row +
                                ", cacheMode=" + cctx.config().getCacheMode() + ", incBackups=" + incBackups +
                                ", primary=" + cctx.affinity().primary(cctx.localNode(), key, topVer) + ']');

                        continue;
                    }

                    V val = row.getValue();

                    if (log.isDebugEnabled()) {
                        ClusterNode primaryNode = CU.primaryNode(cctx, key);

                        log.debug("Record [key=" + key +
                            ", val=" + val +
                            ", incBackups=" + incBackups +
                            ", priNode=" + (primaryNode != null ? U.id8(primaryNode.id()) : null) +
                            ", node=" + U.id8(cctx.localNode().id()) + ']');
                    }

                    if (val == null) {
                        if (log.isDebugEnabled())
                            log.debug("Unsuitable record value: " + val);

                        continue;
                    }

                    if (statsEnabled) {
                        CacheMetricsImpl metrics = cctx.cache().metrics0();

                        metrics.onRead(true);

                        metrics.addGetTimeNanos(System.nanoTime() - start);
                    }

                    if (readEvt) {
                        switch (type) {
                            case SQL:
                                cctx.gridEvents().record(new CacheQueryReadEvent<>(
                                    cctx.localNode(),
                                    "SQL query entry read.",
                                    EVT_CACHE_QUERY_OBJECT_READ,
                                    CacheQueryType.SQL.name(),
                                    cctx.namex(),
                                    qry.queryClassName(),
                                    qry.clause(),
                                    null,
                                    null,
                                    qryInfo.arguments(),
                                    qry.subjectId(),
                                    taskName,
                                    key,
                                    val,
                                    null,
                                    null));

                                break;

                            case TEXT:
                                cctx.gridEvents().record(new CacheQueryReadEvent<>(
                                    cctx.localNode(),
                                    "Full text query entry read.",
                                    EVT_CACHE_QUERY_OBJECT_READ,
                                    CacheQueryType.FULL_TEXT.name(),
                                    cctx.namex(),
                                    qry.queryClassName(),
                                    qry.clause(),
                                    null,
                                    null,
                                    null,
                                    qry.subjectId(),
                                    taskName,
                                    key,
                                    val,
                                    null,
                                    null));

                                break;

                            case SCAN:
                                cctx.gridEvents().record(new CacheQueryReadEvent<>(
                                    cctx.localNode(),
                                    "Scan query entry read.",
                                    EVT_CACHE_QUERY_OBJECT_READ,
                                    CacheQueryType.SCAN.name(),
                                    cctx.namex(),
                                    null,
                                    null,
                                    qry.scanFilter(),
                                    null,
                                    null,
                                    qry.subjectId(),
                                    taskName,
                                    key,
                                    val,
                                    null,
                                    null));

                                break;
                        }
                    }

                    Map.Entry<K, V> entry = F.t(key, val);

                    // Unwrap entry for reducer or transformer only.
                    if (rdc != null || trans != null)
                        entry = (Map.Entry<K, V>)cctx.unwrapBinaryIfNeeded(entry, qry.keepBinary());

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
     * @param qryInfo Info.
     * @param taskName Task name.
     * @return Iterator.
     * @throws IgniteCheckedException In case of error.
     */
    private QueryResult<K, V> queryResult(GridCacheQueryInfo qryInfo, String taskName) throws IgniteCheckedException {
        final UUID sndId = qryInfo.senderId();

        assert sndId != null;

        Map<Long, GridFutureAdapter<QueryResult<K, V>>> futs = qryIters.get(sndId);

        if (futs == null) {
            futs = new LinkedHashMap<Long, GridFutureAdapter<QueryResult<K, V>>>(
                16, 0.75f, true) {
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

            Map<Long, GridFutureAdapter<QueryResult<K, V>>> old = qryIters.putIfAbsent(sndId, futs);

            if (old != null)
                futs = old;
        }

        return queryResult(futs, qryInfo, taskName);
    }

    /**
     * @param futs Futures map.
     * @param qryInfo Info.
     * @return Iterator.
     * @throws IgniteCheckedException In case of error.
     */
    @SuppressWarnings({
        "SynchronizationOnLocalVariableOrMethodParameter",
        "NonPrivateFieldAccessedInSynchronizedContext"})
    private QueryResult<K, V> queryResult(Map<Long, GridFutureAdapter<QueryResult<K, V>>> futs,
        GridCacheQueryInfo qryInfo, String taskName) throws IgniteCheckedException {
        assert futs != null;
        assert qryInfo != null;

        GridFutureAdapter<QueryResult<K, V>> fut;

        boolean exec = false;

        synchronized (futs) {
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

        Map<Long, GridFutureAdapter<QueryResult<K, V>>> futs = qryIters.get(sndId);

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
     * Resets metrics.
     */
    public void resetMetrics() {
        metrics = new GridCacheQueryMetricsAdapter();
    }

    /**
     * @param fail {@code true} if execution failed.
     */
    public void onExecuted(boolean fail) {
        metrics.onQueryExecute(fail);
    }

    /**
     * @param duration Execution duration.
     * @param fail {@code true} if execution failed.
     */
    public void onCompleted(long duration, boolean fail) {
        metrics.onQueryCompleted(duration, fail);
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
            Collection<ClusterNode> nodes = F.view(cctx.discovery().remoteNodes(), new P1<ClusterNode>() {
                @Override public boolean apply(ClusterNode n) {
                    return cctx.kernalContext().discovery().cacheAffinityNode(n, space);
                }
            });

            Collection<Collection<CacheSqlMetadata>> res = new ArrayList<>(nodes.size() + 1);

            IgniteInternalFuture<Collection<Collection<CacheSqlMetadata>>> rmtFut = null;

            // Get metadata from remote nodes.
            if (!nodes.isEmpty())
                rmtFut = cctx.closures().callAsyncNoFailover(BROADCAST, F.asSet(job), nodes, true);

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
            col.add(new CacheSqlMetadata(map.remove(space)));

            for (Collection<CacheSqlMetadata> metas : map.values())
                col.add(new CacheSqlMetadata(metas));

            return col;
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param <K> Key type.
     * @param <V> Value type.
     * @param includeBackups Include backups.
     * @return Predicate.
     */
    @SuppressWarnings("unchecked")
    @Nullable public <K, V> IndexingQueryFilter backupsFilter(boolean includeBackups) {
        if (includeBackups)
            return null;

        return new IndexingQueryFilter() {
            @Nullable @Override public IgniteBiPredicate<K, V> forSpace(final String spaceName) {
                final GridKernalContext ctx = cctx.kernalContext();

                final GridCacheAdapter<Object, Object> cache = ctx.cache().internalCache(spaceName);

                if (cache.context().isReplicated() || cache.configuration().getBackups() == 0)
                    return null;

                return new IgniteBiPredicate<K, V>() {
                    @Override public boolean apply(K k, V v) {
                        return cache.context().affinity().primary(ctx.discovery().localNode(), k, NONE);
                    }
                };
            }
        };
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
        return backupsFilter(qry.includeBackups());
    }

    /**
     * Prints memory statistics for debugging purposes.
     */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> Query manager memory stats [grid=" + cctx.gridName() + ", cache=" + cctx.name() + ']');
    }

    /**
     * FOR TESTING ONLY
     *
     * @return Indexing space for this query manager.
     */
    public String space() {
        return space;
    }

    /**
     * Metadata job.
     */
    @GridInternal
    private static class MetadataJob implements IgniteCallable<Collection<CacheSqlMetadata>> {
        /** */
        private static final long serialVersionUID = 0L;

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
                        return !CU.MARSH_CACHE_NAME.equals(c.name()) && !CU.UTILITY_CACHE_NAME.equals(c.name()) &&
                            !CU.ATOMICS_CACHE_NAME.equals(c.name());
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

                        int size = 2 + type.fields().size();

                        Map<String, String> fieldsMap = U.newLinkedHashMap(size);

                        // _KEY and _VAL are not included in GridIndexingTypeDescriptor.valueFields
                        fieldsMap.put("_KEY", type.keyClass().getName());
                        fieldsMap.put("_VAL", type.valueClass().getName());

                        for (Map.Entry<String, Class<?>> e : type.fields().entrySet())
                            fieldsMap.put(e.getKey().toUpperCase(), e.getValue().getName());

                        fields.put(type.name(), fieldsMap);

                        Collection<GridCacheSqlIndexMetadata> indexesCol =
                            new ArrayList<>(type.indexes().size());

                        for (Map.Entry<String, GridQueryIndexDescriptor> e : type.indexes().entrySet()) {
                            GridQueryIndexDescriptor desc = e.getValue();

                            // Add only SQL indexes.
                            if (desc.type() == GridQueryIndexType.SORTED) {
                                Collection<String> idxFields = e.getValue().fields();
                                Collection<String> descendings = new LinkedList<>();

                                for (String idxField : idxFields)
                                    if (desc.descending(idxField))
                                        descendings.add(idxField);

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
    private static class CacheSqlMetadata implements GridCacheSqlMetadata {
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
    private static class CacheSqlIndexMetadata implements GridCacheSqlIndexMetadata {
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
     *
     */
    private abstract class AbstractLazySwapEntry {
        /** */
        private K key;

        /** */
        private V val;

        /**
         * @return Key bytes.
         */
        protected abstract byte[] keyBytes();

        /**
         * @return Value.
         * @throws IgniteCheckedException If failed.
         */
        protected abstract V unmarshalValue() throws IgniteCheckedException;

        /**
         * @return Key.
         */
        K key() {
            try {
                if (key != null)
                    return key;

                key = cctx.toCacheKeyObject(keyBytes()).value(cctx.cacheObjectContext(), false);

                return key;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /**
         * @return Value.
         */
        V value() {
            try {
                if (val != null)
                    return val;

                val = unmarshalValue();

                return val;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /**
         * @return TTL.
         */
        abstract long timeToLive();

        /**
         * @return Expire time.
         */
        abstract long expireTime();

        /**
         * @return Version.
         */
        abstract GridCacheVersion version();
    }

    /**
     *
     */
    private class LazySwapEntry extends AbstractLazySwapEntry {
        /** */
        private final Map.Entry<byte[], byte[]> e;

        /** */
        private boolean keepBinary;

        /**
         * @param e Entry with
         */
        LazySwapEntry(Map.Entry<byte[], byte[]> e, boolean keepBinary) {
            this.e = e;
            this.keepBinary = keepBinary;
        }

        /** {@inheritDoc} */
        @Override protected byte[] keyBytes() {
            return e.getKey();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("IfMayBeConditional")
        @Override protected V unmarshalValue() throws IgniteCheckedException {
            IgniteBiTuple<byte[], Byte> t = GridCacheSwapEntryImpl.getValue(e.getValue());

            CacheObject obj = cctx.cacheObjects().toCacheObject(cctx.cacheObjectContext(), t.get2(), t.get1());

            return (V)cctx.cacheObjectContext().unwrapBinaryIfNeeded(obj, keepBinary);
        }

        /** {@inheritDoc} */
        @Override long timeToLive() {
            return GridCacheSwapEntryImpl.timeToLive(e.getValue());
        }

        /** {@inheritDoc} */
        @Override long expireTime() {
            return GridCacheSwapEntryImpl.expireTime(e.getValue());
        }

        /** {@inheritDoc} */
        @Override GridCacheVersion version() {
            return GridCacheSwapEntryImpl.version(e.getValue());
        }
    }

    /**
     *
     */
    private class LazyOffheapEntry extends AbstractLazySwapEntry {
        /** */
        private final T2<Long, Integer> keyPtr;

        /** */
        private final T2<Long, Integer> valPtr;

        /**
         * @param keyPtr Key address.
         * @param valPtr Value address.
         */
        private LazyOffheapEntry(T2<Long, Integer> keyPtr, T2<Long, Integer> valPtr) {
            assert keyPtr != null;
            assert valPtr != null;

            this.keyPtr = keyPtr;
            this.valPtr = valPtr;
        }

        /** {@inheritDoc} */
        @Override protected byte[] keyBytes() {
            return U.copyMemory(keyPtr.get1(), keyPtr.get2());
        }

        /** {@inheritDoc} */
        @Override protected V unmarshalValue() throws IgniteCheckedException {
            long ptr = GridCacheOffheapSwapEntry.valueAddress(valPtr.get1(), valPtr.get2());

            CacheObject obj = cctx.fromOffheap(ptr, false);

            V val = CU.value(obj, cctx, false);

            assert val != null;

            return val;
        }

        /** {@inheritDoc} */
        @Override long timeToLive() {
            return GridCacheOffheapSwapEntry.timeToLive(valPtr.get1());
        }

        /** {@inheritDoc} */
        @Override long expireTime() {
            return GridCacheOffheapSwapEntry.expireTime(valPtr.get1());
        }

        /** {@inheritDoc} */
        @Override GridCacheVersion version() {
            return GridCacheOffheapSwapEntry.version(valPtr.get1());
        }
    }

    /**
     *
     */
    private class OffheapIteratorClosure
        extends CX2<T2<Long, Integer>, T2<Long, Integer>, IgniteBiTuple<K, V>> {
        /** */
        private static final long serialVersionUID = 7410163202728985912L;

        /** */
        private IgniteBiPredicate<K, V> filter;

        /** */
        private boolean keepBinary;

        /**
         * @param filter Filter.
         * @param keepBinary Keep binary flag.
         */
        private OffheapIteratorClosure(
            @Nullable IgniteBiPredicate<K, V> filter,
            boolean keepBinary) {
            assert filter != null;

            this.filter = filter;
            this.keepBinary = keepBinary;
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteBiTuple<K, V> applyx(T2<Long, Integer> keyPtr,
            T2<Long, Integer> valPtr)
            throws IgniteCheckedException {
            LazyOffheapEntry e = new LazyOffheapEntry(keyPtr, valPtr);

            K key = (K)cctx.unwrapBinaryIfNeeded(e.key(), keepBinary);
            V val = (V)cctx.unwrapBinaryIfNeeded(e.value(), keepBinary);

            if (!filter.apply(key, val))
                return null;

            return new IgniteBiTuple<>(e.key(), (V)cctx.unwrapTemporary(e.value()));
        }
    }

    /**
     *
     */
    private static class CompoundIterator<T> extends GridIteratorAdapter<T> {
        /** */
        private static final long serialVersionUID = 4585888051556166304L;

        /** */
        private final List<GridIterator<T>> iters;

        /** */
        private int idx;

        /** */
        private GridIterator<T> iter;

        /**
         * @param iters Iterators.
         */
        private CompoundIterator(List<GridIterator<T>> iters) {
            if (iters.isEmpty())
                throw new IllegalArgumentException();

            this.iters = iters;

            iter = F.first(iters);
        }

        /** {@inheritDoc} */
        @Override public boolean hasNextX() throws IgniteCheckedException {
            if (iter.hasNextX())
                return true;

            idx++;

            while (idx < iters.size()) {
                iter = iters.get(idx);

                if (iter.hasNextX())
                    return true;

                idx++;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public T nextX() throws IgniteCheckedException {
            if (!hasNextX())
                throw new NoSuchElementException();

            return iter.nextX();
        }

        /** {@inheritDoc} */
        @Override public void removeX() throws IgniteCheckedException {
            throw new UnsupportedOperationException();
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
    public CacheQuery<Map.Entry<K, V>> createScanQuery(@Nullable IgniteBiPredicate<K, V> filter,
        @Nullable Integer part, boolean keepBinary) {

        return new GridCacheQueryAdapter<>(cctx,
            SCAN,
            null,
            null,
            (IgniteBiPredicate<Object, Object>)filter,
            part,
            false,
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
}
