// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.indexing.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.indexing.*;
import org.gridgain.grid.spi.indexing.h2.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;
import org.springframework.util.*;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.kernal.GridClosureCallMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePeekMode.*;
import static org.gridgain.grid.cache.query.GridCacheQueryType.*;
import static org.gridgain.grid.events.GridEventType.*;

/**
 * Query and index manager.
 *
 * @author @java.author
 * @version @java.version
 */
@SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
public abstract class GridCacheQueryManager<K, V> extends GridCacheManagerAdapter<K, V> {
    /** Number of entries to keep in annotation cache. */
    private static final int DFLT_CLS_CACHE_SIZE = 1000;

    /** */
    private static final List<GridCachePeekMode> SWAP_GLOBAL = F.asList(SWAP, GLOBAL);

    /** */
    private static final GridCachePeekMode[] TX = new GridCachePeekMode[] {GridCachePeekMode.TX};

    /** */
    private static final Collection<String> IGNORED_FIELDS = F.asList(
        "_GG_VAL_STR__",
        "_GG_VER__",
        "_GG_EXPIRES__"
    );

    /** */
    private GridIndexingManager idxMgr;

    /** Indexing SPI name. */
    private String spi;

    /** */
    private String space;

    /** */
    private int maxIterCnt;

    /** Queries metrics bounded cache. */
    private final ConcurrentMap<GridCacheQueryMetricsKey, GridCacheQueryMetricsAdapter> metrics =
        new GridBoundedConcurrentLinkedHashMap<>(
            DFLT_CLS_CACHE_SIZE);

    /** */
    private final ConcurrentMap<UUID, Map<Long, GridFutureAdapter<GridCloseableIterator<
        GridIndexingKeyValueRow<K, V>>>>> qryIters = new ConcurrentHashMap8<>();

    /** */
    private final ConcurrentMap<UUID, Map<Long, GridFutureAdapter<GridIndexingFieldsResult>>> fieldsQryRes =
        new ConcurrentHashMap8<>();

    /** {@inheritDoc} */
    @Override public void start0() throws GridException {
        idxMgr = cctx.kernalContext().indexing();
        spi = cctx.config().getIndexingSpiName();
        space = cctx.name();
        maxIterCnt = cctx.config().getMaximumQueryIteratorCount();

        cctx.events().addListener(new GridLocalEventListener() {
            @Override public void onEvent(GridEvent evt) {
                UUID nodeId = ((GridDiscoveryEvent)evt).eventNodeId();

                Map<Long, GridFutureAdapter<GridCloseableIterator<GridIndexingKeyValueRow<K, V>>>> futs =
                    qryIters.remove(nodeId);

                if (futs != null) {
                    for (GridFutureAdapter<GridCloseableIterator<GridIndexingKeyValueRow<K, V>>> fut : futs.values()) {
                        fut.listenAsync(new CIX1<GridFuture<GridCloseableIterator<GridIndexingKeyValueRow<K, V>>>>() {
                            @Override public void applyx(
                                GridFuture<GridCloseableIterator<GridIndexingKeyValueRow<K, V>>> f)
                                throws GridException {
                                f.get().close();
                            }
                        });
                    }
                }

                Map<Long, GridFutureAdapter<GridIndexingFieldsResult>> fieldsFuts = fieldsQryRes.remove(nodeId);

                if (fieldsFuts != null) {
                    for (GridFutureAdapter<GridIndexingFieldsResult> fut : fieldsFuts.values()) {
                        fut.listenAsync(new CIX1<GridFuture<GridIndexingFieldsResult>>() {
                            @Override public void applyx(GridFuture<GridIndexingFieldsResult> f)
                                throws GridException {
                                f.get().iterator().close();
                            }
                        });
                    }
                }
            }
        }, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        if (cancel)
            onCancelAtStop();
        else
            onWaitAtStop();
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
     * @throws GridException If failed.
     */
    public long size(Class<?> valType) throws GridException {
        return idxMgr.size(spi, space, valType);
    }

    /**
     * Rebuilds all search indexes of given value type.
     *
     * @param valType Value type.
     * @return Future that will be completed when rebuilding of all indexes is finished.
     */
    public GridFuture<?> rebuildIndexes(Class<?> valType) {
        return idxMgr.rebuildIndexes(spi, space, valType);
    }

    /**
     * Rebuilds all search indexes of all types.
     *
     * @return Future that will be completed when rebuilding of all indexes is finished.
     */
    public GridFuture<?> rebuildAllIndexes() {
        return idxMgr.rebuildAllIndexes(spi);
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
    void processQueryRequest(UUID sndId, GridCacheQueryRequest<K, V> req) {
        // No-op.
    }

    /**
     * Entry for given key unswapped.
     *
     * @param swapSpaceName Swap space name.
     * @param key Key.
     * @throws GridSpiException If failed.
     */
    public void onSwap(String swapSpaceName, K key) throws GridSpiException {
        idxMgr.onSwap(spi, space, swapSpaceName, key);
    }

    /**
     * Entry for given key unswapped.
     *
     * @param key Key.
     * @param val Value
     * @param valBytes Value bytes.
     * @throws GridSpiException If failed.
     */
    public void onUnswap(K key, V val, byte[] valBytes) throws GridSpiException {
        idxMgr.onUnswap(spi, space, key, val, valBytes);
    }

    /**
     * Writes key-value pair to index.
     *
     * @param key Key.
     * @param keyBytes Byte array with key data.
     * @param val Value.
     * @param valBytes Value bytes.
     * @param ver Cache entry version.
     * @param expirationTime Expiration time or 0 if never expires.
     * @throws GridException In case of error.
     */
    public void store(K key, @Nullable byte[] keyBytes, @Nullable V val, @Nullable byte[] valBytes,
        GridCacheVersion ver, long expirationTime)
        throws GridException {
        assert key != null;
        assert val != null || valBytes != null;

        if (!cctx.config().isQueryIndexEnabled() && !(key instanceof GridCacheInternal))
            return; // No-op.

        if (val == null)
            val = cctx.marshaller().unmarshal(valBytes, cctx.deploy().globalLoader());

        idxMgr.store(spi, space, key, keyBytes, val, valBytes, CU.versionToBytes(ver), expirationTime);
    }

    /**
     * @param key Key.
     * @param keyBytes Byte array with key value.
     * @return {@code true} if key was found and removed, otherwise {@code false}.
     * @throws GridException Thrown in case of any errors.
     */
    @SuppressWarnings("SimplifiableIfStatement")
    public boolean remove(K key, @Nullable byte[] keyBytes) throws GridException {
        assert key != null;

        if (!cctx.config().isQueryIndexEnabled() && !(key instanceof GridCacheInternal))
            return false; // No-op.

        return idxMgr.remove(spi, space, key, keyBytes);
    }

    /**
     * Undeploys given class loader.
     *
     * @param ldr Class loader to undeploy.
     */
    public void onUndeploy(ClassLoader ldr) {
        try {
            idxMgr.onUndeploy(space, ldr);
        }
        catch (GridException e) {
            throw new GridRuntimeException(e);
        }
    }

    /**
     * Executes distributed query.
     *
     * @param qry Query.
     * @param single {@code true} if single result requested, {@code false} if multiple.
     * @param rmtRdcOnly {@code true} for reduce query when using remote reducer only,
     *      otherwise it is always {@code false}.
     * @param pageLsnr Page listener.
     * @param vis Visitor Predicate.
     * @return Iterator over query results. Note that results become available as they come.
     */
    public abstract <R> GridCacheQueryFuture<R> queryLocal(GridCacheQueryBaseAdapter<K, V, GridCacheQueryBase> qry,
        boolean single, boolean rmtRdcOnly, @Nullable GridBiInClosure<UUID, Collection<R>> pageLsnr,
        @Nullable GridPredicate<?> vis);

    /**
     * Executes distributed query.
     *
     * @param qry Query.
     * @param nodes Nodes.
     * @param single {@code true} if single result requested, {@code false} if multiple.
     * @param rmtOnly {@code true} for reduce query when using remote reducer only,
     *      otherwise it is always {@code false}.
     * @param pageLsnr Page listener.
     * @param vis Visitor predicate.
     * @return Iterator over query results. Note that results become available as they come.
     */
    public abstract <R> GridCacheQueryFuture<R> queryDistributed(GridCacheQueryBaseAdapter<K, V, GridCacheQueryBase> qry,
        Collection<GridNode> nodes, boolean single, boolean rmtOnly,
        @Nullable GridBiInClosure<UUID, Collection<R>> pageLsnr, @Nullable GridPredicate<?> vis);

    /**
     * Loads page.
     *
     * @param id Query ID.
     * @param qry Query.
     * @param nodes Nodes.
     * @param all Whether to load all pages.
     */
    public abstract void loadPage(long id, GridCacheQueryBaseAdapter<K, V, GridCacheQueryBase> qry,
        Collection<GridNode> nodes, boolean all);

    /**
     * Executes distributed fields query.
     *
     * @param qry Query.
     * @param single {@code true} if single result requested, {@code false} if multiple.
     * @param rmtOnly {@code true} for reduce query when using remote reducer only,
     *      otherwise it is always {@code false}.
     * @param pageLsnr Page listener.
     * @param vis Visitor predicate.
     * @return Iterator over query results. Note that results become available as they come.
     */
    public abstract GridCacheFieldsQueryFuture queryFieldsLocal(GridCacheFieldsQueryBase qry, boolean single,
        boolean rmtOnly, @Nullable GridBiInClosure<UUID, Collection<List<Object>>> pageLsnr,
        @Nullable GridPredicate<?> vis);

    /**
     * Executes distributed fields query.
     *
     * @param qry Query.
     * @param nodes Nodes.
     * @param single {@code true} if single result requested, {@code false} if multiple.
     * @param rmtOnly {@code true} for reduce query when using remote reducer only,
     *      otherwise it is always {@code false}.
     * @param pageLsnr Page listener.
     * @param vis Visitor predicate.
     * @return Iterator over query results. Note that results become available as they come.
     */
    public abstract GridCacheFieldsQueryFuture queryFieldsDistributed(GridCacheFieldsQueryBase qry,
        Collection<GridNode> nodes, boolean single, boolean rmtOnly,
        @Nullable GridBiInClosure<UUID, Collection<List<Object>>> pageLsnr, @Nullable GridPredicate<?> vis);

    /**
     * Performs query.
     *
     * @param qry Query.
     * @param loc Local query or not.
     * @return Collection of found keys.
     * @throws GridException In case of error.
     */
    private GridCloseableIterator<GridIndexingKeyValueRow<K, V>> executeQuery(GridCacheQueryBaseAdapter qry,
        boolean loc) throws GridException {
        Class<? extends V> resType = resultType(qry, loc);

        if (qry.type() == null) {
            assert !loc;

            throw new GridException("Received next page request after iterator was removed. " +
                "Consider increasing maximum number of stored iterators (see " +
                "GridCacheConfiguration.getMaximumQueryIteratorCount() configuration property).");
        }

        try {
            executeBeforeCallback(qry);

            switch (qry.type()) {
                case SQL:
                    return idxMgr.query(spi, space, qry.clause(), F.asList(qry.arguments()), resType,
                        qry.includeBackups(), projectionFilter(qry), keyValueFilter(qry));

                case SCAN:
                    return scanIterator(qry, resType);

                case TEXT:
                    return idxMgr.queryText(spi, space, qry.clause(), resType, qry.includeBackups(),
                        projectionFilter(qry), keyValueFilter(qry));

                default:
                    throw new GridException("Unknown query type: " + qry.type());
            }
        }
        finally {
            executeAfterCallback(qry);
        }
    }

    /**
     * Performs fields query.
     *
     * @param qry Query
     * @param loc Local query or not.
     * @return Fields query result.
     * @throws GridException In case of error.
     */
    private GridIndexingFieldsResult executeFieldsQuery(GridCacheFieldsQueryBase qry, boolean loc)
        throws GridException {
        assert qry != null;

        if (qry.clause() == null) {
            assert !loc;

            throw new GridException("Received next page request after iterator was removed. " +
                "Consider increasing maximum number of stored iterators (see " +
                "GridCacheConfiguration.getMaximumQueryIteratorCount() configuration property).");
        }

        try {
            executeBeforeCallback(qry);

            return idxMgr.queryFields(spi, space, qry.clause(), F.asList(qry.arguments()), qry.includeBackups(),
                projectionFilter(qry), keyValueFilter(qry));
        }
        finally {
            executeAfterCallback(qry);
        }
    }

    /**
     * @param qry Query.
     * @param loc Local.
     * @return Result type for query.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    private Class<? extends V> resultType(GridCacheQueryBaseAdapter qry, boolean loc) throws GridException {
        ClassLoader ldr = loc ? cctx.deploy().localLoader() : cctx.deploy().globalLoader();

        try {
            return qry.queryClass(ldr);
        }
        catch (ClassNotFoundException e) {
            throw new GridException("Failed to load class: " + qry.className(), e);
        }
    }

    /**
     * @param qry Query.
     * @param qryCls Query result type.
     * @return Full-scan row iterator.
     * @throws GridException If failed to get iterator.
     */
    @SuppressWarnings({"unchecked"})
    private GridCloseableIterator<GridIndexingKeyValueRow<K, V>> scanIterator(
        GridCacheQueryBaseAdapter qry, final Class<?> qryCls)
        throws GridException {

        P1<GridCacheEntry<K, V>> clsPred = new P1<GridCacheEntry<K, V>>() {
            @Override public boolean apply(GridCacheEntry<K, V> e) {
                V val = e.peek();

                return val != null && (qryCls == null || qryCls.isAssignableFrom(val.getClass()));
            }
        };

        GridPredicate<GridCacheEntry<K, V>> filter =
            qry.projectionFilter() != null ? F0.and(qry.projectionFilter(), clsPred) : clsPred;

        Map<K, V> resMap = new HashMap();

        GridCacheProjection<K, V> prj = cctx.cache().projection(filter);

        for (K key : prj.keySet()) {
            V val = prj.peek(key);

            if (val != null)
                resMap.put(key, val);
        }

        Set<Map.Entry<K, V>> entries = resMap.entrySet();

        final GridPredicate<K> keyFilter = keyFilter(qry);
        final GridPredicate<V> valFilter = valueFilter(qry);

        final GridIterator<GridIndexingKeyValueRow<K, V>> it = F.iterator(
            entries,
            new C1<Map.Entry<K, V>, GridIndexingKeyValueRow<K, V>>() {
                @Override public GridIndexingKeyValueRow<K, V> apply(Map.Entry<K, V> e) {
                    return new GridIndexingKeyValueRowAdapter<>(e.getKey(), e.getValue());
                }
            },
            true,
            new P1<Map.Entry<K, V>>() {
                @Override public boolean apply(Map.Entry<K, V> e) {
                    return (keyFilter == null || keyFilter.apply(e.getKey())) &&
                        (valFilter == null || valFilter.apply(e.getValue()));
                }
            });

        return new GridCloseableIteratorAdapter<GridIndexingKeyValueRow<K, V>>() {
            @Override protected boolean onHasNext() {
                return it.hasNext();
            }

            @Override protected GridIndexingKeyValueRow<K, V> onNext() {
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
     * @param o Object to inject resources to.
     * @throws GridException If failure occurred while injecting resources.
     */
    private void injectResources(@Nullable Object o) throws GridException {
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
    protected void runFieldsQuery(GridCacheQueryInfo<K, V> qryInfo) {
        assert qryInfo != null;

        int qryId = qryInfo.query().id();

        if (log.isDebugEnabled())
            log.debug("Running query [qryId=" + qryId + ", qryInfo=" + qryInfo + ']');

        boolean rmvRes = true;

        try {
            GridPredicate<GridCacheEntry<K, V>>[] prjFilter = cctx.vararg(qryInfo.projectionPredicate());
            GridReducer<List<Object>, Object> rdc = qryInfo.fieldsReducer();
            GridPredicate<Object> vis = (GridPredicate<Object>)qryInfo.visitor();

            for (GridPredicate<GridCacheEntry<K, V>> pred : prjFilter)
                injectResources(pred);

            injectResources(rdc);
            injectResources(vis);

            int pageSize = qryInfo.pageSize();

            Collection<Object> data = null;
            Collection<List<GridIndexingEntity<?>>> entities = null;

            if (qryInfo.local() || rdc != null || vis != null || cctx.isLocalNode(qryInfo.senderId()))
                data = new ArrayList<>(pageSize);
            else
                entities = new ArrayList<>(pageSize);

            GridIndexingFieldsResult res = qryInfo.local() ?
                executeFieldsQuery((GridCacheFieldsQueryBase)qryInfo.query(), qryInfo.local()) :
                fieldsQueryResult(qryInfo);

            // If metadata needs to be returned to user and cleaned from internal fields - copy it.
            List<GridCacheSqlFieldMetadata> meta = qryInfo.includeMetaData() ?
                (res.metaData() != null ? new ArrayList<>(res.metaData()) : null) :
                res.metaData();

            BitSet ignored = new BitSet();

            // Filter internal fields.
            if (meta != null) {
                Iterator<GridCacheSqlFieldMetadata> metaIt = meta.iterator();

                int i = 0;

                while (metaIt.hasNext()) {
                    if (IGNORED_FIELDS.contains(metaIt.next().fieldName())) {
                        ignored.set(i);

                        if (qryInfo.includeMetaData())
                            metaIt.remove();
                    }

                    i++;
                }
            }

            if (!qryInfo.includeMetaData())
                meta = null;

            GridCloseableIterator<List<GridIndexingEntity<?>>> it =
                new GridSpiCloseableIteratorWrapper<>(res.iterator());

            if (log.isDebugEnabled())
                log.debug("Received fields iterator [qryId=" + qryId + ", iterHasNext=" + it.hasNext() + ']');

            if (!it.hasNext()) {
                if (rdc != null)
                    data = Collections.singletonList(rdc.reduce());

                onFieldsPageReady(qryInfo.local(), qryInfo, meta, entities, data, true, null);

                return;
            }

            int cnt = 0;
            boolean metaSent = false;

            while (!Thread.currentThread().isInterrupted() && it.hasNext()) {
                List<GridIndexingEntity<?>> row = it.next();

                // Query is cancelled.
                if (row == null) {
                    onPageReady(qryInfo.local(), qryInfo, null, true, null);

                    break;
                }

                // Filter internal fields.
                Iterator<GridIndexingEntity<?>> rowIt = row.iterator();

                int i = 0;

                while (rowIt.hasNext()) {
                    rowIt.next();

                    if (ignored.get(i++))
                        rowIt.remove();
                }

                if ((qryInfo.local() || rdc != null || vis != null || cctx.isLocalNode(qryInfo.senderId()))) {
                    List<Object> fields = new ArrayList<>(row.size());

                    for (GridIndexingEntity<?> ent : row)
                        fields.add(ent.value());

                    // Visit.
                    if (vis != null) {
                        if (!vis.apply(fields) || !it.hasNext()) {
                            onFieldsPageReady(qryInfo.local(), qryInfo, null, null, null, true, null);

                            return;
                        }
                        else
                            continue;
                    }

                    // Reduce.
                    if (rdc != null) {
                        if (!rdc.collect(fields))
                            break;
                    }
                    else
                        data.add(fields);
                }
                else
                    entities.add(row);

                if (rdc == null && qryInfo.single()) {
                    onFieldsPageReady(qryInfo.local(), qryInfo, meta, entities, data, true, null);

                    break;
                }

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
        catch (GridException e) {
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
        }
        finally {
            if (rmvRes)
                removeFieldsQueryResult(qryInfo.senderId(), qryInfo.requestId());
        }
    }

    /**
     * Processes cache query request.
     *
     * @param qryInfo Query info.
     */
    @SuppressWarnings("unchecked")
    protected <R> void runQuery(GridCacheQueryInfo<K, V> qryInfo) {
        assert qryInfo != null;

        boolean loc = qryInfo.local();

        int qryId = qryInfo.query().id();

        if (log.isDebugEnabled())
            log.debug("Running query [qryId=" + qryId + ", qryInfo=" + qryInfo + ']');

        Collection<?> data = null;

        boolean rmvIter = true;

        try {
            // Preparing query closures.
            GridPredicate<GridCacheEntry<K, V>>[] prjFilter = cctx.vararg(qryInfo.projectionPredicate());
            GridClosure<Object, Object> trans = (GridClosure<Object, Object>)qryInfo.transformer();
            GridReducer<Map.Entry<K, V>, Object> rdc = qryInfo.reducer();
            GridPredicate<Map.Entry<K, V>> vis = (GridPredicate<Map.Entry<K, V>>)qryInfo.visitor();

            // Injecting resources into query closures.
            for (GridPredicate<GridCacheEntry<K, V>> pred : prjFilter)
                injectResources(pred);

            injectResources(trans);
            injectResources(rdc);
            injectResources(vis);

            GridCacheQueryBaseAdapter<?, ?, GridCacheQueryBase> qry = qryInfo.query();

            boolean single = qryInfo.single();

            int pageSize = qryInfo.pageSize();

            boolean incBackups = qryInfo.includeBackups();

            Map<K, Object> map = new LinkedHashMap<>(pageSize);

            GridCloseableIterator<GridIndexingKeyValueRow<K, V>> iter =
                loc ? executeQuery(qry, loc) : queryIterator(qryInfo);

            GridCacheAdapter<K, V> cache = cctx.cache();

            if (log.isDebugEnabled())
                log.debug("Received index iterator [qryId=" + qryId + ", iterHasNext=" + iter.hasNext() +
                    ", cacheSize=" + cache.size() + ']');

            int cnt = 0;

            boolean stop = false;
            boolean pageSent = false;

            while (!Thread.currentThread().isInterrupted() && iter.hasNext()) {
                GridIndexingKeyValueRow<K, V> row = iter.next();

                // Query is cancelled.
                if (row == null) {
                    onPageReady(loc, qryInfo, null, true, null);

                    break;
                }

                K key = row.key().value();

                // Filter backups for SCAN queries. Other types are filtered in indexing manager.
                if (cctx.config().getCacheMode() != LOCAL && qry.type() == SCAN &&
                    !incBackups && !cctx.affinity().primary(cctx.localNode(), key)) {
                    if (log.isDebugEnabled())
                        log.debug("Ignoring backup element [qryId=" + qryId + ", row=" + row +
                            ", cacheMode=" + cctx.config().getCacheMode() + ", incBackups=" + incBackups +
                            ", primary=" + cctx.affinity().primary(cctx.localNode(), key) + ']');

                    continue;
                }

                V val = null;

                GridIndexingEntity<V> v = row.value();

                if (v == null || !v.hasValue()) {
                    assert v == null || v.bytes() != null;

                    GridCacheEntryEx<K, V> entry = cache.entryEx(key);

                    boolean unmarshal;

                    try {
                        GridCachePeekMode[] oldExcl = cctx.excludePeekModes(TX);

                        try {
                            val = entry.peek(SWAP_GLOBAL, prjFilter);
                        }
                        finally {
                            cctx.excludePeekModes(oldExcl);
                        }

                        if (qry.cloneValues())
                            val = cctx.cloneValue(val);

                        GridCacheVersion ver = entry.version();

                        unmarshal = !Arrays.equals(row.version(), CU.versionToBytes(ver));
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        // If entry has been removed concurrently we have to unmarshal from bytes.
                        unmarshal = true;
                    }

                    if (unmarshal && v != null)
                        val = v.value();
                }
                else
                    val = v.value();

                if (log.isDebugEnabled())
                    log.debug("Record [qryId=" + qryId + ", key=" + key + ", val=" + val + ", incBackups=" +
                        incBackups + "priNode=" + U.id8(CU.primaryNode(cctx, key).id()) +
                        ", node=" + U.id8(cctx.grid().localNode().id()) + ']');

                if (val == null) {
                    if (log.isDebugEnabled())
                        log.debug("Unsuitable record value [qryId=" + qryId + ", val=" + val + ']');

                    continue;
                }

                // Visit.
                if (vis != null) {
                    if (!vis.apply(F.t(key, val)) || !iter.hasNext()) {
                        onPageReady(loc, qryInfo, null, true, null);

                        return;
                    }
                    else
                        continue;
                }

                // Reduce.
                if (rdc != null) {
                    if (!rdc.collect(F.t(key, val)) || !iter.hasNext()) {
                        onPageReady(loc, qryInfo, Collections.singletonList(rdc.reduce()), true, null);

                        pageSent = true;

                        break;
                    }
                    else
                        continue;
                }

                map.put(key, trans == null ? val : trans.apply(val));

                if (single)
                    break;

                if (!loc) {
                    if (++cnt == pageSize || !iter.hasNext()) {
                        boolean finished = !iter.hasNext();

                        // Put GridCacheQueryResponseEntry as map value to avoid using any new container.
                        for (Map.Entry entry : map.entrySet())
                            entry.setValue(new GridCacheQueryResponseEntry(entry.getKey(), entry.getValue()));

                        onPageReady(loc, qryInfo, map.values(), finished, null);

                        pageSent = true;

                        if (!finished)
                            rmvIter = false;

                        if (!qryInfo.allPages())
                            return;

                        map = new LinkedHashMap<>(pageSize);

                        if (stop)
                            break; // while
                    }
                }
            }

            if (!pageSent) {
                if (rdc == null) {
                    if (!loc) {
                        for (Map.Entry entry : map.entrySet())
                            entry.setValue(new GridCacheQueryResponseEntry(entry.getKey(), entry.getValue()));

                        data = map.values();
                    }
                    else
                        data = map.entrySet();

                    onPageReady(loc, qryInfo, data, true, null);
                }
                else
                    onPageReady(loc, qryInfo, Collections.singletonList(rdc.reduce()), true, null);
            }
        }
        catch (Throwable e) {
            U.error(log, "Failed to run query [qry=" + qryInfo + ", node=" + cctx.nodeId() + "]", e);

            onPageReady(loc, qryInfo, null, true, e);
        }
        finally {
            if (rmvIter)
                removeQueryIterator(qryInfo.senderId(), qryInfo.requestId());
        }

        if (log.isDebugEnabled())
            log.debug("End of running query [qryId=" + qryId + ", res=" + data + ']');
    }

    /**
     * @param qryInfo Info.
     * @return Iterator.
     * @throws GridException In case of error.
     */
    private GridCloseableIterator<GridIndexingKeyValueRow<K, V>> queryIterator(GridCacheQueryInfo<?, ?> qryInfo)
        throws GridException {
        UUID sndId = qryInfo.senderId();

        assert sndId != null;

        Map<Long, GridFutureAdapter<GridCloseableIterator<GridIndexingKeyValueRow<K, V>>>> futs = qryIters.get(sndId);

        if (futs == null) {
            futs = new LinkedHashMap<Long, GridFutureAdapter<GridCloseableIterator<GridIndexingKeyValueRow<K, V>>>>(
                16, 0.75f, true) {
                @Override protected boolean removeEldestEntry(Map.Entry<Long,
                    GridFutureAdapter<GridCloseableIterator<GridIndexingKeyValueRow<K, V>>>> e) {
                    boolean rmv = size() > maxIterCnt;

                    if (rmv) {
                        try {
                            GridCloseableIterator<GridIndexingKeyValueRow<K, V>> it = e.getValue().get();

                            it.close();
                        }
                        catch (GridException ex) {
                            U.error(log, "Failed to close query iterator.", ex);
                        }
                    }

                    return rmv;
                }
            };

            Map<Long, GridFutureAdapter<GridCloseableIterator<GridIndexingKeyValueRow<K, V>>>> old =
                qryIters.putIfAbsent(sndId, futs);

            if (old != null)
                futs = old;
        }

        return queryIterator(futs, qryInfo);
    }

    /**
     * @param futs Futures map.
     * @param qryInfo Info.
     * @return Iterator.
     * @throws GridException In case of error.
     */
    @SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter",
        "NonPrivateFieldAccessedInSynchronizedContext"})
    private GridCloseableIterator<GridIndexingKeyValueRow<K, V>> queryIterator(
        Map<Long, GridFutureAdapter<GridCloseableIterator<GridIndexingKeyValueRow<K, V>>>> futs,
        GridCacheQueryInfo<?, ?> qryInfo) throws GridException {
        assert futs != null;
        assert qryInfo != null;

        GridFutureAdapter<GridCloseableIterator<GridIndexingKeyValueRow<K, V>>> fut;

        boolean exec = false;

        synchronized (futs) {
            fut = futs.get(qryInfo.requestId());

            if (fut == null) {
                futs.put(qryInfo.requestId(), fut = new GridFutureAdapter<>(cctx.kernalContext()));

                exec = true;
            }
        }

        if (exec) {
            try {
                fut.onDone(executeQuery(qryInfo.query(), false));
            }
            catch (GridException e) {
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
    protected void removeQueryIterator(@Nullable UUID sndId, long reqId) {
        if (sndId == null)
            return;

        Map<Long, GridFutureAdapter<GridCloseableIterator<GridIndexingKeyValueRow<K, V>>>> futs = qryIters.get(sndId);

        if (futs != null) {
            GridFuture<GridCloseableIterator<GridIndexingKeyValueRow<K, V>>> fut;

            synchronized (futs) {
                fut = futs.remove(reqId);
            }

            if (fut != null) {
                try {
                    fut.get().close();
                }
                catch (GridException e) {
                    U.error(log, "Failed to close iterator.", e);
                }
            }
        }
    }

    /**
     * @param qryInfo Info.
     * @return Iterator.
     * @throws GridException In case of error.
     */
    private GridIndexingFieldsResult fieldsQueryResult(GridCacheQueryInfo<?, ?> qryInfo)
        throws GridException {
        UUID sndId = qryInfo.senderId();

        assert sndId != null;

        Map<Long, GridFutureAdapter<GridIndexingFieldsResult>> iters = fieldsQryRes.get(sndId);

        if (iters == null) {
            iters = new LinkedHashMap<Long, GridFutureAdapter<GridIndexingFieldsResult>>(16, 0.75f, true) {
                @Override protected boolean removeEldestEntry(Map.Entry<Long,
                    GridFutureAdapter<GridIndexingFieldsResult>> e) {
                    boolean rmv = size() > maxIterCnt;

                    if (rmv) {
                        try {
                            GridCloseableIterator<List<GridIndexingEntity<?>>> it =
                                new GridSpiCloseableIteratorWrapper<>(e.getValue().get().iterator());

                            it.close();
                        }
                        catch (GridException ex) {
                            U.error(log, "Failed to close fields query iterator.", ex);
                        }
                    }

                    return rmv;
                }

                @Override public boolean equals(Object o) {
                    return o == this;
                }
            };

            Map<Long, GridFutureAdapter<GridIndexingFieldsResult>> old = fieldsQryRes.putIfAbsent(sndId, iters);

            if (old != null)
                iters = old;
        }

        return fieldsQueryResult(iters, qryInfo);
    }

    /**
     * @param resMap Results map.
     * @param qryInfo Info.
     * @return Fields query result.
     * @throws GridException In case of error.
     */
    @SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter",
        "NonPrivateFieldAccessedInSynchronizedContext"})
    private GridIndexingFieldsResult fieldsQueryResult(Map<Long, GridFutureAdapter<GridIndexingFieldsResult>> resMap,
        GridCacheQueryInfo<?, ?> qryInfo) throws GridException {
        assert resMap != null;
        assert qryInfo != null;

        GridFutureAdapter<GridIndexingFieldsResult> fut;

        boolean exec = false;

        synchronized (resMap) {
            fut = resMap.get(qryInfo.requestId());

            if (fut == null) {
                resMap.put(qryInfo.requestId(), fut =
                    new GridFutureAdapter<>(cctx.kernalContext()));

                exec = true;
            }
        }

        if (exec) {
            try {
                fut.onDone(executeFieldsQuery((GridCacheFieldsQueryBase)qryInfo.query(), false));
            }
            catch (GridException e) {
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

        Map<Long, GridFutureAdapter<GridIndexingFieldsResult>> futs = fieldsQryRes.get(sndId);

        if (futs != null) {
            GridFuture<GridIndexingFieldsResult> fut;

            synchronized (futs) {
                fut = futs.remove(reqId);
            }

            if (fut != null) {
                try {
                    fut.get().iterator().close();
                }
                catch (GridException e) {
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
    protected abstract boolean onPageReady(boolean loc, GridCacheQueryInfo<K, V> qryInfo,
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
    protected abstract boolean onFieldsPageReady(boolean loc, GridCacheQueryInfo<K, V> qryInfo,
        @Nullable List<GridCacheSqlFieldMetadata> metaData,
        @Nullable Collection<List<GridIndexingEntity<?>>> entities,
        @Nullable Collection<?> data,
        boolean finished, @Nullable Throwable e);

    /**
     *
     * @param qry Query to validate.
     * @throws GridException In case of validation error.
     */
    public void validateQuery(GridCacheQueryBase<?, ?, GridCacheQueryBase> qry) throws GridException {
        if (log.isDebugEnabled())
            log.debug("Validating query: " + qry);

        if (!(qry instanceof GridCacheFieldsQueryBase) && qry.type() == null)
            throw new GridException("Type must be set for query.");

        if (qry.type() == SQL || qry.type() == TEXT)
            if (F.isEmpty(qry.className()))
                throw new GridException("Class must be set for " + qry.type().name() + " query.");

        if (qry.type() == SQL || qry.type() == TEXT || qry instanceof GridCacheFieldsQueryBase) {
            if (F.isEmpty(qry.clause()))
                throw new GridException("Clause must be set for " + qry.type().name() + " query.");

            if (!cctx.config().isQueryIndexEnabled())
                throw new GridException("Indexing is disabled for cache: " + cctx.cache().name());
        }
    }

    /**
     * Creates user's query.
     *
     * @param filter Projection filter.
     * @param flags Projection flags
     * @return Created query.
     */
    public GridCacheQuery<K, V> createQuery(@Nullable GridPredicate<GridCacheEntry<K, V>> filter,
        Set<GridCacheFlag> flags) {
        return new GridCacheQueryAdapter<>(cctx, null, null, null, null, filter, flags);
    }

    /**
     * Creates user's query.
     *
     * @param type Query type (may be {@code null} .
     * @param filter Projection filter (may be {@code null} .
     * @param flags Projection flags
     * @return Created query.
     */
    public GridCacheQuery<K, V> createQuery(@Nullable GridCacheQueryType type,
        @Nullable GridPredicate<GridCacheEntry<K, V>> filter, Set<GridCacheFlag> flags) {
        return new GridCacheQueryAdapter<>(cctx, type, null, null, null, filter, flags);
    }

    /**
     * Creates user's query.
     *
     * @param type Query type (may be {@code null} .
     * @param clsName Query class name (may be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @param clause Query clause (should be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @param filter Projection filter (may be {@code null} .
     * @param flags Projection flags
     * @return Created query.
     */
    public GridCacheQuery<K, V> createQuery(@Nullable GridCacheQueryType type, @Nullable String clsName,
        @Nullable String clause, @Nullable GridPredicate<GridCacheEntry<K, V>> filter, Set<GridCacheFlag> flags) {
        return new GridCacheQueryAdapter<>(cctx, type, clause, null, clsName, filter, flags);
    }

    /**
     * Creates fields query.
     *
     * @param clause Clause.
     * @param filter Projection filter.
     * @param flags Cache flags.
     * @return Query.
     */
    public GridCacheFieldsQuery<K, V> createFieldsQuery(String clause,
        @Nullable GridPredicate<GridCacheEntry<K, V>> filter, Set<GridCacheFlag> flags) {
        return new GridCacheFieldsQueryAdapter<>(cctx, clause, filter, flags);
    }

    /**
     * Creates reduce fields query.
     *
     * @param clause Clause.
     * @param filter Projection filter.
     * @param flags Cache flags.
     * @return Query.
     */
    public <R1, R2> GridCacheReduceFieldsQuery<K, V, R1, R2> createReduceFieldsQuery(String clause,
        @Nullable GridPredicate<GridCacheEntry<K, V>> filter, Set<GridCacheFlag> flags) {
        return new GridCacheReduceFieldsQueryAdapter<>(cctx, clause, filter, flags);
    }

    /**
     * Creates user's query.
     *
     * @param type Query type (may be {@code null} .
     * @param cls Query class (may be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @param clause Query clause (should be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @param filter Projection filter (may be {@code null} .
     * @param flags Projection flags
     * @return Created query.
     */
    public GridCacheQuery<K, V> createQuery(@Nullable GridCacheQueryType type, @Nullable Class<?> cls,
        @Nullable String clause, @Nullable GridPredicate<GridCacheEntry<K, V>> filter, Set<GridCacheFlag> flags) {
        checkPrimitiveIndexEnabled(cls);

        return new GridCacheQueryAdapter<>(cctx, type, clause, cls, null, filter, flags);
    }

    /**
     * Checks if a given query class is a Java primitive or wrapper
     * and throws {@link IllegalStateException} if there is configured {@link GridH2IndexingSpi}
     * with disabled {@link GridH2IndexingSpi#isDefaultIndexPrimitiveKey()}.
     *
     * @param cls Query class. May be {@code null}.
     * @throws IllegalStateException If checking failed.
     */
    private void checkPrimitiveIndexEnabled(@Nullable Class<?> cls) {
        if (cls == null)
            return;

        if (ClassUtils.isPrimitiveOrWrapper(cls)) {
            for (GridIndexingSpi indexingSpi : cctx.gridConfig().getIndexingSpi()) {
                if (indexingSpi instanceof GridH2IndexingSpi)
                    if (!((GridH2IndexingSpiMBean)indexingSpi).isDefaultIndexPrimitiveKey())
                        throw new IllegalStateException("Invalid use of primitive class type in queries when " +
                            "GridH2IndexingSpi.isDefaultIndexPrimitiveKey() is disabled " +
                            "(consider enabling indexing for primitive types).");
            }
        }
    }

    /**
     * Creates user's transform query.
     *
     * @param filter Projection filter.
     * @param flags Projection flags
     * @return Created query.
     */
    public <T> GridCacheTransformQuery<K, V, T> createTransformQuery(
        @Nullable GridPredicate<GridCacheEntry<K, V>> filter, Set<GridCacheFlag> flags) {
        return new GridCacheTransformQueryAdapter<>(cctx, null, null, null, null, filter, flags);
    }

    /**
     * Creates user's transform query.
     *
     * @param type Query type.
     * @param filter Projection filter.
     * @param flags Projection flags
     * @return Created query.
     */
    public <T> GridCacheTransformQuery<K, V, T> createTransformQuery(GridCacheQueryType type,
        @Nullable GridPredicate<GridCacheEntry<K, V>> filter, Set<GridCacheFlag> flags) {
        return new GridCacheTransformQueryAdapter<>(cctx, type, null, null, null, filter, flags);
    }

    /**
     * Creates user's transform query.
     *
     * @param type Query type.
     * @param clsName Query class name (may be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @param clause Query clause (should be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @param filter Projection filter.
     * @param flags Projection flags
     * @return Created query.
     */
    public <T> GridCacheTransformQuery<K, V, T> createTransformQuery(GridCacheQueryType type, String clsName,
        String clause, @Nullable GridPredicate<GridCacheEntry<K, V>> filter, Set<GridCacheFlag> flags) {
        return new GridCacheTransformQueryAdapter<>(cctx, type, clause, null, clsName, filter, flags);
    }

    /**
     * Creates user's transform query.
     *
     * @param type Query type.
     * @param cls Query class (may be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @param clause Query clause (should be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @param filter Projection filter.
     * @param flags Projection flags
     * @return Created query.
     */
    public <T> GridCacheTransformQuery<K, V, T> createTransformQuery(GridCacheQueryType type, Class<?> cls,
        String clause, @Nullable GridPredicate<GridCacheEntry<K, V>> filter, Set<GridCacheFlag> flags) {
        checkPrimitiveIndexEnabled(cls);

        return new GridCacheTransformQueryAdapter<>(cctx, type, clause, cls, null, filter, flags);
    }

    /**
     * Creates user's reduce query.
     *
     * @param filter Projection filter.
     * @param flags Projection flags
     * @return Created query.
     */
    public <R1, R2> GridCacheReduceQuery<K, V, R1, R2> createReduceQuery(
        @Nullable GridPredicate<GridCacheEntry<K, V>> filter, Set<GridCacheFlag> flags) {
        return new GridCacheReduceQueryAdapter<>(cctx, null, null, null, null, filter, flags);
    }

    /**
     * Creates user's reduce query.
     *
     * @param type Query type.
     * @param filter Projection filter.
     * @param flags Projection flags
     * @return Created query.
     */
    public <R1, R2> GridCacheReduceQuery<K, V, R1, R2> createReduceQuery(GridCacheQueryType type,
        @Nullable GridPredicate<GridCacheEntry<K, V>> filter, Set<GridCacheFlag> flags) {
        return new GridCacheReduceQueryAdapter<>(cctx, type, null, null, null, filter, flags);
    }

    /**
     * Creates user's reduce query.
     *
     * @param type Query type.
     * @param clsName Query class name (may be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @param clause Query clause (should be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @param filter Projection filter.
     * @param flags Projection flags
     * @return Created query.
     */
    public <R1, R2> GridCacheReduceQuery<K, V, R1, R2> createReduceQuery(GridCacheQueryType type, String clsName,
        String clause, @Nullable GridPredicate<GridCacheEntry<K, V>> filter, Set<GridCacheFlag> flags) {
        return new GridCacheReduceQueryAdapter<>(cctx, type, clause, null, clsName, filter, flags);
    }

    /**
     * Creates user's reduce query.
     *
     * @param type Query type.
     * @param cls Query class (may be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @param clause Query clause (should be {@code null} for {@link GridCacheQueryType#SCAN} queries).
     * @param filter Projection filter.
     * @param flags Projection flags
     * @return Created query.
     */
    public <R1, R2> GridCacheReduceQuery<K, V, R1, R2> createReduceQuery(GridCacheQueryType type,
        Class<?> cls, String clause, @Nullable GridPredicate<GridCacheEntry<K, V>> filter,
        Set<GridCacheFlag> flags) {
        checkPrimitiveIndexEnabled(cls);

        return new GridCacheReduceQueryAdapter<>(cctx, type, clause, cls, null, filter, flags);
    }

    /**
     * Gets cache queries metrics.
     *
     * @return Cache queries metrics.
     */
    public Collection<GridCacheQueryMetrics> metrics() {
        return new GridBoundedLinkedHashSet<GridCacheQueryMetrics>(metrics.values(), DFLT_CLS_CACHE_SIZE);
    }

    /**
     * @param m Updated metrics.
     * @param startTime Execution start time.
     * @param duration Execution duration.
     * @param fail {@code true} if execution failed.
     */
    public void onMetricsUpdate(GridCacheQueryMetricsAdapter m, long startTime, long duration, boolean fail) {
        assert m != null;

        GridCacheQueryMetricsAdapter prev = metrics.get(m.key());

        if (prev == null)
            prev = metrics.putIfAbsent(m.key(), m.copy());

        if (prev != null)
            prev.onQueryExecute(startTime, duration, fail);
    }

    /**
     * Gets SQL metadata.
     *
     * @return SQL metadata.
     * @throws GridException In case of error.
     */
    public Collection<GridCacheSqlMetadata> sqlMetadata() throws GridException {
        Callable<Collection<CacheSqlMetadata>> job = new MetadataJob(spi);

        // Remote nodes that have current cache.
        Collection<GridNode> nodes = F.view(cctx.discovery().remoteNodes(), new P1<GridNode>() {
            @Override public boolean apply(GridNode n) {
                return U.hasCache(n, space);
            }
        });

        Collection<Collection<CacheSqlMetadata>> res = new ArrayList<>(nodes.size() + 1);

        GridFuture<Collection<Collection<CacheSqlMetadata>>> rmtFut = null;

        // Get metadata from remote nodes.
        if (!nodes.isEmpty())
            rmtFut = cctx.closures().callAsyncNoFailover(BROADCAST, F.asSet(job), nodes, true);

        // Get local metadata.
        GridFuture<Collection<CacheSqlMetadata>> locFut = cctx.closures().callLocalSafe(job, true);

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

    /**
     * Gets key/value filter for query.
     *
     * @param qry Query.
     * @return Filter.
     * @throws GridException In case of error.
     */
    private GridIndexingQueryFilter<K, V> keyValueFilter(GridCacheQueryBaseAdapter qry) throws GridException {
        assert qry != null;

        final GridPredicate<K> keyFilter = keyFilter(qry);
        final GridPredicate<V> valFilter = valueFilter(qry);

        return new GridIndexingQueryFilter<K, V>() {
            @Override public boolean apply(String s, K k, V v) {
                return !F.eq(space, s) || (keyFilter == null || keyFilter.apply(k)) &&
                    (valFilter == null || valFilter.apply(v));
            }
        };
    }

    /**
     * Gets projection filter for query.
     *
     * @param qry Query.
     * @return Filter.
     */
    @SuppressWarnings("unchecked")
    private GridIndexingQueryFilter<K, V> projectionFilter(GridCacheQueryBaseAdapter qry) {
        assert qry != null;

        final GridPredicate<GridCacheEntry<K, V>> prjFilter = qry.projectionFilter();

        return new GridIndexingQueryFilter<K, V>() {
            @Override public boolean apply(String s, K k, V v) {
                if (prjFilter == null || F.isAlwaysTrue(prjFilter) || !F.eq(space, s))
                    return true;

                try {
                    GridCacheEntry<K, V> entry = context().cache().entry(k);

                    return entry != null && prjFilter.apply(entry);
                }
                catch (GridDhtInvalidPartitionException ignore) {
                    return false;
                }
            }
        };
    }

    /**
     * Gets key filter for query.
     *
     * @param qry Query.
     * @return Filter.
     * @throws GridException In case of error.
     */
    @SuppressWarnings("unchecked")
    private GridPredicate<K> keyFilter(GridCacheQueryBaseAdapter qry) throws GridException {
        assert qry != null;

        GridPredicate<K> filter = qry.remoteKeyFilter();

        injectResources(filter);

        return filter;
    }

    /**
     * Gets value filter for query.
     *
     * @param qry Query.
     * @return Filter.
     * @throws GridException In case of error.
     */
    @SuppressWarnings("unchecked")
    private GridPredicate<V> valueFilter(GridCacheQueryBaseAdapter qry) throws GridException {
        assert qry != null;

        GridPredicate<V> filter = qry.remoteValueFilter();

        injectResources(filter);

        return filter;
    }

    /**
     * Gets before callback for query.
     *
     * @param qry Query.
     * @throws GridException In case of error.
     */
    @SuppressWarnings("unchecked")
    private void executeBeforeCallback(GridCacheQueryBaseAdapter qry) throws GridException {
        assert qry != null;

        Runnable cb = qry.beforeCallback();

        if (cb != null) {
            injectResources(cb);

            cb.run();
        }
    }

    /**
     * Gets after callback for query.
     *
     * @param qry Query.
     * @throws GridException In case of error.
     */
    @SuppressWarnings("unchecked")
    private void executeAfterCallback(GridCacheQueryBaseAdapter qry) throws GridException {
        assert qry != null;

        Runnable cb = qry.afterCallback();

        if (cb != null) {
            injectResources(cb);

            cb.run();
        }
    }

    /**
     * Prints memory statistics for debugging purposes.
     */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> Query manager memory stats [grid=" + cctx.gridName() + ", cache=" + cctx.name() + ']');
        X.println(">>>   Metrics: " + metrics.size());
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
    private static class MetadataJob extends CO<Collection<CacheSqlMetadata>> {
        /** Grid */
        @GridInstanceResource
        private Grid grid;

        /** Indexing SPI name. */
        private final String spiName;

        /**
         * @param spiName Indexing SPI name.
         */
        private MetadataJob(@Nullable String spiName) {
            this.spiName = spiName;
        }

        /** {@inheritDoc} */
        @Override public Collection<CacheSqlMetadata> apply() {
            final GridKernalContext ctx = ((GridKernal)grid).context();

            Collection<String> cacheNames = F.viewReadOnly(ctx.cache().caches(),
                new C1<GridCache<?, ?>, String>() {
                    @Override public String apply(GridCache<?, ?> c) {
                        return c.name();
                    }
                },
                new P1<GridCache<?, ?>>() {
                    @Override public boolean apply(GridCache<?, ?> c) {
                        return F.eq(spiName, c.configuration().getIndexingSpiName());
                    }
                }
            );

            return F.transform(cacheNames, new C1<String, CacheSqlMetadata>() {
                @Override public CacheSqlMetadata apply(String cacheName) {
                    Collection<GridIndexingTypeDescriptor> types = ctx.indexing().types(cacheName);

                    Collection<String> names = new HashSet<>(types.size());
                    Map<String, String> keyClasses = new HashMap<>(types.size());
                    Map<String, String> valClasses = new HashMap<>(types.size());
                    Map<String, Map<String, String>> fields = new HashMap<>(types.size());
                    Map<String, Collection<GridCacheSqlIndexMetadata>> indexes =
                        new HashMap<>(types.size());

                    for (GridIndexingTypeDescriptor type : types) {
                        // Filter internal types (e.g., data structures).
                        if (type.name().startsWith("GridCache"))
                            continue;

                        names.add(type.name());

                        keyClasses.put(type.name(), type.keyClass().getName());
                        valClasses.put(type.name(), type.valueClass().getName());

                        int size = 2 + type.keyFields().size() + type.valueFields().size();

                        Map<String, String> fieldsMap = new LinkedHashMap<>(size);

                        // _KEY and _VAL are not included in GridIndexingTypeDescriptor.valueFields
                        fieldsMap.put("_KEY", type.keyClass().getName());
                        fieldsMap.put("_VAL", type.valueClass().getName());

                        for (Map.Entry<String, Class<?>> e : type.keyFields().entrySet())
                            fieldsMap.put(e.getKey().toUpperCase(), e.getValue().getName());

                        for (Map.Entry<String, Class<?>> e : type.valueFields().entrySet())
                            fieldsMap.put(e.getKey().toUpperCase(), e.getValue().getName());

                        fields.put(type.name(), fieldsMap);

                        Collection<GridCacheSqlIndexMetadata> indexesCol =
                            new ArrayList<>(type.indexes().size());

                        for (Map.Entry<String, GridIndexDescriptor> e : type.indexes().entrySet()) {
                            GridIndexDescriptor desc = e.getValue();

                            // Add only SQL indexes.
                            if (!desc.text()) {
                                Collection<String> idxFields = e.getValue().fields();
                                Collection<String> descendings = new LinkedList<>();

                                for (String idxField : idxFields)
                                    if (desc.descending(idxField))
                                        descendings.add(idxField);

                                indexesCol.add(new CacheSqlIndexMetadata(e.getKey().toUpperCase(),
                                    idxFields, descendings, desc.unique()));
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
}
