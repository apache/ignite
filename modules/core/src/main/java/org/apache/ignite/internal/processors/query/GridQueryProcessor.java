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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.cache.CacheTypeMetadata;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.CacheQueryExecutedEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.query.CacheQueryFuture;
import org.apache.ignite.internal.processors.cache.query.CacheQueryType;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.query.ddl.AbstractIndexOperation;
import org.apache.ignite.internal.processors.query.ddl.CreateIndexOperation;
import org.apache.ignite.internal.processors.query.ddl.IndexAcceptDiscoveryMessage;
import org.apache.ignite.internal.processors.query.ddl.IndexFinishDiscoveryMessage;
import org.apache.ignite.internal.processors.query.ddl.IndexProposeDiscoveryMessage;
import org.apache.ignite.internal.processors.query.ddl.task.IndexingAcceptTask;
import org.apache.ignite.internal.processors.query.ddl.task.IndexingCacheStartTask;
import org.apache.ignite.internal.processors.query.ddl.task.IndexingCacheStopTask;
import org.apache.ignite.internal.processors.query.ddl.task.IndexingFinishTask;
import org.apache.ignite.internal.processors.query.ddl.task.IndexingTask;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.IgniteOutClosureX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;
import static org.apache.ignite.internal.IgniteComponentType.INDEXING;

/**
 * Indexing processor.
 */
public class GridQueryProcessor extends GridProcessorAdapter {
    /** Queries detail metrics eviction frequency. */
    private static final int QRY_DETAIL_METRICS_EVICTION_FREQ = 3_000;

    /** For tests. */
    public static Class<? extends GridQueryIndexing> idxCls;

    /** */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Type descriptors. */
    private final Map<QueryTypeIdKey, QueryTypeDescriptorImpl> types = new ConcurrentHashMap<>();

    /** Type descriptors. */
    private final ConcurrentMap<QueryTypeNameKey, QueryTypeDescriptorImpl> typesByName = new ConcurrentHashMap<>();

    /** */
    private final GridQueryIndexing idx;

    /** RW lock for dynamic index create. */
    private final ReadWriteLock idxLock = new ReentrantReadWriteLock();

    /** All indexes. */
    private final ConcurrentMap<QueryIndexKey, QueryIndexDescriptorImpl> idxs = new ConcurrentHashMap<>();

    /** Index create/drop client futures. */
    private final ConcurrentMap<UUID, QueryIndexClientFuture> idxCliFuts = new ConcurrentHashMap<>();

    /** Index worker. */
    private final DynamicIndexManagerWorker idxWorker;

    /** */
    private GridTimeoutProcessor.CancelableTask qryDetailMetricsEvictTask;

    /** */
    private static final ThreadLocal<AffinityTopologyVersion> requestTopVer = new ThreadLocal<>();

    /**
     * @param ctx Kernal context.
     */
    public GridQueryProcessor(GridKernalContext ctx) throws IgniteCheckedException {
        super(ctx);

        if (idxCls != null) {
            idx = U.newInstance(idxCls);

            idxCls = null;
        }
        else
            idx = INDEXING.inClassPath() ? U.<GridQueryIndexing>newInstance(INDEXING.className()) : null;

        idxWorker = new DynamicIndexManagerWorker(ctx.igniteInstanceName(), log);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        if (idx != null) {
            ctx.resource().injectGeneric(idx);

            idx.start(ctx, busyLock);
        }

        new IgniteThread(idxWorker).start();

        // Schedule queries detail metrics eviction.
        qryDetailMetricsEvictTask = ctx.timeout().schedule(new Runnable() {
            @Override public void run() {
                for (IgniteCacheProxy cache : ctx.cache().jcaches())
                    cache.context().queries().evictDetailMetrics();
            }
        }, QRY_DETAIL_METRICS_EVICTION_FREQ, QRY_DETAIL_METRICS_EVICTION_FREQ);
    }

    /**
     * @return {@code true} If indexing module is in classpath and successfully initialized.
     */
    public boolean moduleEnabled() {
        return idx != null;
    }

    /**
     * @return Indexing.
     * @throws IgniteException If module is not enabled.
     */
    public GridQueryIndexing getIndexing() throws IgniteException {
        checkxEnabled();

        return idx;
    }

    /**
     * @param cctx Cache context.
     * @param initIdxStates Index states.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("deprecation")
    private void initializeCache(GridCacheContext<?, ?> cctx, @Nullable QueryIndexStates initIdxStates)
        throws IgniteCheckedException {
        String space = cctx.name();

        CacheConfiguration<?,?> ccfg = cctx.config();

        // Prepare candidates.
        List<Class<?>> mustDeserializeClss = new ArrayList<>();

        Collection<QueryTypeCandidate> cands = new ArrayList<>();

        if (!F.isEmpty(ccfg.getQueryEntities())) {
            for (QueryEntity qryEntity : ccfg.getQueryEntities()) {
                QueryTypeCandidate cand = QueryUtils.typeForQueryEntity(space, cctx, qryEntity, mustDeserializeClss);

                cands.add(cand);
            }
        }

        if (!F.isEmpty(ccfg.getTypeMetadata())) {
            for (CacheTypeMetadata meta : ccfg.getTypeMetadata()) {
                QueryTypeCandidate cand = QueryUtils.typeForCacheMetadata(space, cctx, meta, mustDeserializeClss);

                if (cand != null)
                    cands.add(cand);
            }
        }

        // Apply dynamic changes to candidates.
        if (initIdxStates != null) {
            Map<String, QueryIndexState> readyIdxStates = initIdxStates.readyOperations();

            for (QueryTypeCandidate cand : cands) {
                QueryTypeDescriptorImpl desc = cand.descriptor();

                for (Map.Entry<String, QueryIndexState> entry : readyIdxStates.entrySet()) {
                    String idxName = entry.getKey();
                    QueryIndexState idxState = entry.getValue();

                    if (F.eq(desc.tableName(), idxState.tableName()))
                        QueryUtils.processDynamicIndexChange(idxName, idxState.index(), desc);
                }
            }
        }

        // Ready to register at this point.
        registerCache0(space, cctx, cands);

        // Active operations will be applied from worker thread.
        if (initIdxStates != null)
            idxWorker.onCacheStart(space, initIdxStates);

        // Warn about possible implicit deserialization.
        if (!mustDeserializeClss.isEmpty()) {
            U.warn(log, "Some classes in query configuration cannot be written in binary format " +
                "because they either implement Externalizable interface or have writeObject/readObject methods. " +
                "Instances of these classes will be deserialized in order to build indexes. Please ensure that " +
                "all nodes have these classes in classpath. To enable binary serialization either implement " +
                Binarylizable.class.getSimpleName() + " interface or set explicit serializer using " +
                "BinaryTypeConfiguration.setSerializer() method: " + mustDeserializeClss);
        }
    }

    /**
     * Find current coordinator.
     *
     * @return {@code True} if node is coordinator.
     */
    private ClusterNode findCoordinator() {
        ClusterNode res = null;

        for (ClusterNode node : ctx.discovery().aliveServerNodes()) {
            if (res == null || res.order() > node.order())
                res = node;
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        super.onKernalStop(cancel);

        if (cancel && idx != null) {
            try {
                while (!busyLock.tryBlock(500))
                    idx.cancelAllQueries();

                return;
            } catch (InterruptedException ignored) {
                U.warn(log, "Interrupted while waiting for active queries cancellation.");

                Thread.currentThread().interrupt();
            }
        }

        idxWorker.cancel();

        try {
            idxWorker.join();
        }
        catch (InterruptedException e) {
            U.warn(log, "Failed to wait for index worker stop due to interrupt.", e);
        }

        busyLock.block();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        super.stop(cancel);

        if (idx != null)
            idx.stop();

        U.closeQuiet(qryDetailMetricsEvictTask);
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        if (idx != null)
            idx.onDisconnected(reconnectFut);

        // TODO: Complete index client futures, clear pending index state.
    }

    /**
     * Handle cache start. Invoked either from GridCacheProcessor.onKernalStart() method or from exchange worker.
     * When called for the first time, we initialize topology thus understanding whether current node is coordinator
     * or not.
     *
     * @param cctx Cache context.
     * @param idxStates Index states.
     * @throws IgniteCheckedException If failed.
     */
    public void onCacheStart(GridCacheContext cctx, @Nullable QueryIndexStates idxStates)
        throws IgniteCheckedException {
        if (idx == null)
            return;

        if (!busyLock.enterBusy())
            return;

        try {
            initializeCache(cctx, idxStates);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param cctx Cache context.
     */
    public void onCacheStop(GridCacheContext cctx) {
        if (idx == null)
            return;

        if (!busyLock.enterBusy())
            return;

        try {
            unregisterCache0(cctx.name());
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Handle index accept message.
     *
     * @param msg Message.
     */
    public void onIndexAcceptMessage(IndexAcceptDiscoveryMessage msg) {
        idxWorker.onAccept(msg);

        idxLock.writeLock().lock();

        // TODO

        try {
//            // Validate.
//            if (op instanceof CreateIndexOperation) {
//                CreateIndexOperation op0 = (CreateIndexOperation)op;
//
//                QueryIndex idx = op0.index();
//
//                // Check conflict with other indexes.
//                String idxName = op0.index().getName();
//
//                QueryIndexKey idxKey = new QueryIndexKey(space, idxName);
//
//                QueryIndexDescriptorImpl oldIdx = idxs.get(idxKey);
//
//                if (oldIdx != null) {
//                    if (!op0.ifNotExists())
//                        msg.onError(ctx.localNodeId(), "Index already exists [space=" + space + ", index=" + idxName);
//
//                    return;
//                }
//
//                // Make sure table exists.
//                String tblName = op0.tableName();
//
//                QueryTypeDescriptorImpl typeDesc = null;
//
//                for (QueryTypeDescriptorImpl type : types.values()) {
//                    if (F.eq(tblName, type.tableName())) {
//                        typeDesc = type;
//
//                        break;
//                    }
//                }
//
//                if (typeDesc == null) {
//                    msg.onError(ctx.localNodeId(), "Table doesn't exist: " + tblName);
//
//                    return;
//                }
//
//                // Make sure that index can be applied to the given table.
//                for (String idxField : idx.getFieldNames()) {
//                    if (!typeDesc.fields().containsKey(idxField)) {
//                        msg.onError(ctx.localNodeId(), "Field doesn't exist: " + idxField);
//
//                        return;
//                    }
//                }
//            }
//            else if (op instanceof DropIndexOperation) {
//                DropIndexOperation op0 = (DropIndexOperation)op;
//
//                String idxName = op0.indexName();
//
//                QueryIndexKey idxKey = new QueryIndexKey(space, idxName);
//
//                QueryIndexDescriptorImpl oldIdx = idxs.get(idxKey);
//
//                if (oldIdx == null) {
//                    if (!op0.ifExists())
//                        msg.onError(ctx.localNodeId(), "Index doesn't exist: " + idxName);
//                }
//            }
//            else
//                msg.onError(ctx.localNodeId(), "Unsupported operation: " + op);
        }
        finally {
            idxLock.writeLock().unlock();
        }
    }

    /**
     * Handle index accept message.
     *
     * @param msg Message.
     */
    public void onIndexFinishMessage(IndexFinishDiscoveryMessage msg) {
        idxWorker.onFinish(msg);
    }

    /**
     * Handle node leave.
     *
     * @param node Node.
     */
    public void onNodeLeave(ClusterNode node) {
        // TODO.
    }

    /**
     * Register cache in indexing SPI.
     *
     * @param space Space.
     * @param cctx Cache context.
     * @param cands Candidates.
     * @throws IgniteCheckedException If failed.
     */
    private void registerCache0(String space, GridCacheContext<?, ?> cctx, Collection<QueryTypeCandidate> cands)
        throws IgniteCheckedException {
        idxLock.writeLock().lock();

        try {
            idx.registerCache(space, cctx, cctx.config());

            try {
                for (QueryTypeCandidate cand : cands) {
                    QueryTypeIdKey typeId = cand.typeId();
                    QueryTypeIdKey altTypeId = cand.alternativeTypeId();
                    QueryTypeDescriptorImpl desc = cand.descriptor();

                    if (typesByName.putIfAbsent(new QueryTypeNameKey(space, desc.name()), desc) != null)
                        throw new IgniteCheckedException("Type with name '" + desc.name() + "' already indexed " +
                            "in cache '" + space + "'.");

                    types.put(typeId, desc);

                    if (altTypeId != null)
                        types.put(altTypeId, desc);

                    for (QueryIndexDescriptorImpl idx : desc.indexes0()) {
                        QueryIndexKey idxKey = new QueryIndexKey(space, idx.name());

                        QueryIndexDescriptorImpl oldIdx = idxs.putIfAbsent(idxKey, idx);

                        if (oldIdx != null) {
                            throw new IgniteException("Duplicate index name [space=" + space + ", idxName=" + idx.name() +
                                ", existingTable=" + oldIdx.typeDescriptor().tableName() +
                                ", table=" + desc.tableName() + ']');
                        }
                    }

                    boolean registered = idx.registerType(space, desc);

                    desc.registered(registered);
                }
            }
            catch (IgniteCheckedException | RuntimeException e) {
                unregisterCache0(space);

                throw e;
            }
        }
        finally {
            idxLock.writeLock().unlock();
        }
    }

    /**
     * Unregister cache.
     *
     * @param space Space.
     */
    private void unregisterCache0(String space) {
        assert idx != null;

        idxLock.writeLock().lock();

        try {
            // Clear types.
            Iterator<Map.Entry<QueryTypeIdKey, QueryTypeDescriptorImpl>> it = types.entrySet().iterator();

            while (it.hasNext()) {
                Map.Entry<QueryTypeIdKey, QueryTypeDescriptorImpl> entry = it.next();

                if (F.eq(space, entry.getKey().space())) {
                    it.remove();

                    typesByName.remove(new QueryTypeNameKey(space, entry.getValue().name()));
                }
            }

            // Clear indexes.
            idxWorker.onCacheStop(space);

            // Notify indexing.
            try {
                idx.unregisterCache(space);
            }
            catch (Exception e) {
                U.error(log, "Failed to clear indexing on cache unregister (will ignore): " + space, e);
            }
        }
        finally {
            idxLock.writeLock().unlock();
        }
    }

    /**
     * Remove indexes during complete space unregister.
     *
     * @param space Space.
     */
    private void removeIndexesOnSpaceUnregister(String space) {
        Iterator<Map.Entry<QueryIndexKey, QueryIndexDescriptorImpl>> idxIt = idxs.entrySet().iterator();

        while (idxIt.hasNext()) {
            Map.Entry<QueryIndexKey, QueryIndexDescriptorImpl> idxEntry = idxIt.next();

            QueryIndexKey idxKey = idxEntry.getKey();

            if (F.eq(space, idxKey.space()))
                idxIt.remove();
        }
    }

    /**
     * Complete index client futures in case of cache stop or type unregistration.
     *
     * @param space Space.
     */
    private void completeIndexClientFuturesOnSpaceUnregister(String space) {
        Iterator<Map.Entry<UUID, QueryIndexClientFuture>> idxCliFutIt = idxCliFuts.entrySet().iterator();

        while (idxCliFutIt.hasNext()) {
            Map.Entry<UUID, QueryIndexClientFuture> idxCliFutEntry = idxCliFutIt.next();

            QueryIndexClientFuture idxCliFut = idxCliFutEntry.getValue();

            if (F.eq(space, idxCliFut.key().space())) {
                idxCliFut.onCacheStopped();

                idxCliFutIt.remove();
            }
        }
    }

    /**
     * @param space Space name.
     * @return Cache object context.
     */
    private CacheObjectContext cacheObjectContext(String space) {
        return ctx.cache().internalCache(space).context().cacheObjectContext();
    }

    /**
     * Writes key-value pair to index.
     *
     * @param space Space.
     * @param key Key.
     * @param val Value.
     * @param ver Cache entry version.
     * @param expirationTime Expiration time or 0 if never expires.
     * @throws IgniteCheckedException In case of error.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    public void store(final String space, final CacheObject key, final CacheObject val,
        byte[] ver, long expirationTime) throws IgniteCheckedException {
        assert key != null;
        assert val != null;

        if (log.isDebugEnabled())
            log.debug("Store [space=" + space + ", key=" + key + ", val=" + val + "]");

        if (idx == null)
            return;

        if (!busyLock.enterBusy())
            return;

        try {
            CacheObjectContext coctx = cacheObjectContext(space);

            Class<?> valCls = null;

            QueryTypeIdKey id;

            boolean binaryVal = ctx.cacheObjects().isBinaryObject(val);

            if (binaryVal) {
                int typeId = ctx.cacheObjects().typeId(val);

                id = new QueryTypeIdKey(space, typeId);
            }
            else {
                valCls = val.value(coctx, false).getClass();

                id = new QueryTypeIdKey(space, valCls);
            }

            QueryTypeDescriptorImpl desc = types.get(id);

            if (desc == null || !desc.registered())
                return;

            if (!binaryVal && !desc.valueClass().isAssignableFrom(valCls))
                throw new IgniteCheckedException("Failed to update index due to class name conflict" +
                    "(multiple classes with same simple name are stored in the same cache) " +
                    "[expCls=" + desc.valueClass().getName() + ", actualCls=" + valCls.getName() + ']');

            if (!ctx.cacheObjects().isBinaryObject(key)) {
                Class<?> keyCls = key.value(coctx, false).getClass();

                if (!desc.keyClass().isAssignableFrom(keyCls))
                    throw new IgniteCheckedException("Failed to update index, incorrect key class [expCls=" +
                        desc.keyClass().getName() + ", actualCls=" + keyCls.getName() + "]");
            }

            idx.store(space, desc.name(), key, val, ver, expirationTime);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void checkEnabled() throws IgniteCheckedException {
        if (idx == null)
            throw new IgniteCheckedException("Indexing is disabled.");
    }

    /**
     * @throws IgniteException If indexing is disabled.
     */
    private void checkxEnabled() throws IgniteException {
        if (idx == null)
            throw new IgniteException("Failed to execute query because indexing is disabled (consider adding module " +
                INDEXING.module() + " to classpath or moving it from 'optional' to 'libs' folder).");
    }

    /**
     * @param cctx Cache context.
     * @param qry Query.
     * @return Cursor.
     */
    public QueryCursor<List<?>> queryTwoStep(final GridCacheContext<?,?> cctx, final SqlFieldsQuery qry) {
        checkxEnabled();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            return executeQuery(GridCacheQueryType.SQL_FIELDS, qry.getSql(), cctx, new IgniteOutClosureX<QueryCursor<List<?>>>() {
                @Override public QueryCursor<List<?>> applyx() throws IgniteCheckedException {
                    return idx.queryTwoStep(cctx, qry, null);
                }
            }, true);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param spaceName Cache name.
     * @param streamer Data streamer.
     * @param qry Query.
     * @return Iterator.
     */
    public long streamUpdateQuery(@Nullable final String spaceName,
        final IgniteDataStreamer<?, ?> streamer, final String qry, final Object[] args) {
        assert streamer != null;

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            GridCacheContext cctx = ctx.cache().cache(spaceName).context();

            return executeQuery(GridCacheQueryType.SQL_FIELDS, qry, cctx, new IgniteOutClosureX<Long>() {
                @Override public Long applyx() throws IgniteCheckedException {
                    return idx.streamUpdateQuery(spaceName, qry, args, streamer);
                }
            }, true);
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param cctx Cache context.
     * @param qry Query.
     * @return Cursor.
     */
    public <K,V> QueryCursor<Cache.Entry<K,V>> queryTwoStep(final GridCacheContext<?,?> cctx, final SqlQuery qry) {
        checkxEnabled();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            return executeQuery(GridCacheQueryType.SQL, qry.getSql(), cctx,
                new IgniteOutClosureX<QueryCursor<Cache.Entry<K, V>>>() {
                    @Override public QueryCursor<Cache.Entry<K, V>> applyx() throws IgniteCheckedException {
                        return idx.queryTwoStep(cctx, qry);
                    }
                }, true);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param cctx Cache context.
     * @param qry Query.
     * @param keepBinary Keep binary flag.
     * @return Cursor.
     */
    public <K, V> QueryCursor<Cache.Entry<K, V>> queryLocal(
        final GridCacheContext<?, ?> cctx,
        final SqlQuery qry,
        final boolean keepBinary
    ) {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            return executeQuery(GridCacheQueryType.SQL, qry.getSql(), cctx,
                new IgniteOutClosureX<QueryCursor<Cache.Entry<K, V>>>() {
                    @Override public QueryCursor<Cache.Entry<K, V>> applyx() throws IgniteCheckedException {
                        String type = qry.getType();

                        String typeName = type(cctx.name(), type);

                        qry.setType(typeName);

                        sendQueryExecutedEvent(
                            qry.getSql(),
                            qry.getArgs());

                        return idx.queryLocalSql(cctx, qry, idx.backupFilter(requestTopVer.get(), null), keepBinary);
                    }
                }, true);
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Collect queries that already running more than specified duration.
     *
     * @param duration Duration to check.
     * @return Collection of long running queries.
     */
    public Collection<GridRunningQueryInfo> runningQueries(long duration) {
        if (moduleEnabled())
            return idx.runningQueries(duration);

        return Collections.emptyList();
    }

    /**
     * Cancel specified queries.
     *
     * @param queries Queries ID's to cancel.
     */
    public void cancelQueries(Collection<Long> queries) {
        if (moduleEnabled())
            idx.cancelQueries(queries);
    }

    /**
     * Create index.
     *
     * @param space Space name.
     * @param tblName Table name.
     * @param idx Index.
     * @param ifNotExists When set to {@code true} operation will fail if index already exists.
     * @return Future completed when index is created.
     */
    public IgniteInternalFuture<?> createIndex(String space, String tblName, QueryIndex idx, boolean ifNotExists) {
        String idxName = idx.getName() != null ? idx.getName() : QueryEntity.defaultIndexName(idx);

        QueryIndexKey idxKey = new QueryIndexKey(space, idxName);

        idxLock.readLock().lock();

        try {
            QueryIndexDescriptorImpl oldIdxDesc = idxs.get(idxKey);

            if (oldIdxDesc != null) {
                // Make sure that index is bound to the same table.
                String oldTblName = oldIdxDesc.typeDescriptor().tableName();

                if (!F.eq(oldTblName, tblName)) {
                    return new GridFinishedFuture<>(new IgniteException("Index already exists and is bound to " +
                        "another table [space=" + space + ", idxName=" + idxName + ", expTblName=" + oldTblName +
                        ", actualTblName=" + tblName + ']'));
                }

                if (ifNotExists)
                    return new GridFinishedFuture<>();
                else
                    return new GridFinishedFuture<>(new IgniteException("Index already exists [space=" + space +
                        ", idxName=" + idxName + ']'));
            }

            UUID opId = UUID.randomUUID();
            QueryIndexClientFuture fut = new QueryIndexClientFuture(opId, idxKey);

            CreateIndexOperation op = new CreateIndexOperation(ctx.localNodeId(), opId, space, tblName, idx,
                ifNotExists);

            try {
                ctx.discovery().sendCustomEvent(new IndexProposeDiscoveryMessage(op));
            }
            catch (IgniteCheckedException e) {
                return new GridFinishedFuture<>(new IgniteException("Failed to start index create opeartion due to " +
                    "unexpected exception [space=" + space + ", idxName=" + idxName + ']'));
            }

            QueryIndexClientFuture oldFut = idxCliFuts.put(opId, fut);

            assert oldFut == null;

            return fut;
        }
        finally {
            idxLock.readLock().unlock();
        }
    }

    /**
     * @param sqlQry Sql query.
     * @param params Params.
     */
    private void sendQueryExecutedEvent(String sqlQry, Object[] params) {
        if (ctx.event().isRecordable(EVT_CACHE_QUERY_EXECUTED)) {
            ctx.event().record(new CacheQueryExecutedEvent<>(
                ctx.discovery().localNode(),
                "SQL query executed.",
                EVT_CACHE_QUERY_EXECUTED,
                CacheQueryType.SQL.name(),
                null,
                null,
                sqlQry,
                null,
                null,
                params,
                null,
                null));
        }
    }

    /**
     *
     * @param space Space name.
     * @param sql Query.
     * @return {@link PreparedStatement} from underlying engine to supply metadata to Prepared - most likely H2.
     */
    public PreparedStatement prepareNativeStatement(String space, String sql) throws SQLException {
        checkxEnabled();

        return idx.prepareNativeStatement(space, sql);
    }

    /**
     * @param schema Schema name.
     * @return space (cache) name from schema name.
     */
    public String space(String schema) throws SQLException {
        checkxEnabled();

        return idx.space(schema);
    }

    /**
     * @param spaceName Space name.
     * @param nativeStmt Native statement.
     * @param autoFlushFreq Automatic data flushing frequency, disabled if {@code 0}.
     * @param nodeBufSize Per node buffer size - see {@link IgniteDataStreamer#perNodeBufferSize(int)}
     * @param nodeParOps Per node parallel ops count - see {@link IgniteDataStreamer#perNodeParallelOperations(int)}
     * @param allowOverwrite Overwrite existing cache values on key duplication.
     * @see IgniteDataStreamer#allowOverwrite
     * @return {@link IgniteDataStreamer} tailored to specific needs of given native statement based on its metadata.
     */
    public IgniteDataStreamer<?, ?> createStreamer(String spaceName, PreparedStatement nativeStmt, long autoFlushFreq,
        int nodeBufSize, int nodeParOps, boolean allowOverwrite) {
        return idx.createStreamer(spaceName, nativeStmt, autoFlushFreq, nodeBufSize, nodeParOps, allowOverwrite);
    }

    /**
     * @param cctx Cache context.
     * @param qry Query.
     * @return Iterator.
     */
    @SuppressWarnings("unchecked")
    public QueryCursor<List<?>> queryLocalFields(final GridCacheContext<?, ?> cctx, final SqlFieldsQuery qry) {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            return executeQuery(GridCacheQueryType.SQL_FIELDS, qry.getSql(), cctx, new IgniteOutClosureX<QueryCursor<List<?>>>() {
                @Override public QueryCursor<List<?>> applyx() throws IgniteCheckedException {
                    GridQueryCancel cancel = new GridQueryCancel();

                    final QueryCursor<List<?>> cursor = idx.queryLocalSqlFields(cctx, qry,
                        idx.backupFilter(requestTopVer.get(), null), cancel);

                    return new QueryCursorImpl<List<?>>(new Iterable<List<?>>() {
                        @Override public Iterator<List<?>> iterator() {
                            sendQueryExecutedEvent(qry.getSql(), qry.getArgs());

                            return cursor.iterator();
                        }
                    }, cancel) {
                        @Override public List<GridQueryFieldMetadata> fieldsMeta() {
                            if (cursor instanceof QueryCursorImpl)
                                return ((QueryCursorImpl)cursor).fieldsMeta();

                            return super.fieldsMeta();
                        }
                    };
                }
            }, true);
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param space Space.
     * @param key Key.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void remove(String space, CacheObject key, CacheObject val) throws IgniteCheckedException {
        assert key != null;

        if (log.isDebugEnabled())
            log.debug("Remove [space=" + space + ", key=" + key + ", val=" + val + "]");

        if (idx == null)
            return;

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to remove from index (grid is stopping).");

        try {
            idx.remove(space, key, val);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param space Space.
     * @param clause Clause.
     * @param resType Result type.
     * @param filters Key and value filters.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Key/value rows.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryText(final String space, final String clause,
        final String resType, final IndexingQueryFilter filters) throws IgniteCheckedException {
        checkEnabled();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            final GridCacheContext<?, ?> cctx = ctx.cache().internalCache(space).context();

            return executeQuery(GridCacheQueryType.TEXT, clause, cctx,
                new IgniteOutClosureX<GridCloseableIterator<IgniteBiTuple<K, V>>>() {
                    @Override public GridCloseableIterator<IgniteBiTuple<K, V>> applyx() throws IgniteCheckedException {
                        String typeName = type(space, resType);

                        return idx.queryLocalText(space, clause, typeName, filters);
                    }
                }, true);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Will be called when entry for key will be swapped.
     *
     * @param spaceName Space name.
     * @param key key.
     * @throws IgniteCheckedException If failed.
     */
    public void onSwap(String spaceName, CacheObject key) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Swap [space=" + spaceName + ", key=" + key + "]");

        if (idx == null)
            return;

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to process swap event (grid is stopping).");

        try {
            idx.onSwap(
                spaceName,
                key);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Will be called when entry for key will be unswapped.
     *
     * @param spaceName Space name.
     * @param key Key.
     * @param val Value.
     * @throws IgniteCheckedException If failed.
     */
    public void onUnswap(String spaceName, CacheObject key, CacheObject val)
        throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Unswap [space=" + spaceName + ", key=" + key + ", val=" + val + "]");

        if (idx == null)
            return;

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to process swap event (grid is stopping).");

        try {
            idx.onUnswap(spaceName, key, val);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Gets types for space.
     *
     * @param space Space name.
     * @return Descriptors.
     */
    public Collection<GridQueryTypeDescriptor> types(@Nullable String space) {
        Collection<GridQueryTypeDescriptor> spaceTypes = new ArrayList<>();

        for (Map.Entry<QueryTypeIdKey, QueryTypeDescriptorImpl> e : types.entrySet()) {
            QueryTypeDescriptorImpl desc = e.getValue();

            if (desc.registered() && F.eq(e.getKey().space(), space))
                spaceTypes.add(desc);
        }

        return spaceTypes;
    }

    /**
     * Gets type name for provided space and type name if type is still valid.
     *
     * @param space Space name.
     * @param typeName Type name.
     * @return Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    private String type(@Nullable String space, String typeName) throws IgniteCheckedException {
        QueryTypeDescriptorImpl type = typesByName.get(new QueryTypeNameKey(space, typeName));

        if (type == null || !type.registered())
            throw new IgniteCheckedException("Failed to find SQL table for type: " + typeName);

        return type.name();
    }

    /**
     * @param qryType Query type.
     * @param qry Query description.
     * @param cctx Cache context.
     * @param clo Closure.
     * @param complete Complete.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public <R> R executeQuery(GridCacheQueryType qryType, String qry, GridCacheContext<?, ?> cctx,
        IgniteOutClosureX<R> clo, boolean complete) throws IgniteCheckedException {
        final long startTime = U.currentTimeMillis();

        Throwable err = null;

        R res = null;

        try {
            res = clo.apply();

            if (res instanceof CacheQueryFuture) {
                CacheQueryFuture fut = (CacheQueryFuture)res;

                err = fut.error();
            }

            return res;
        }
        catch (GridClosureException e) {
            err = e.unwrap();

            throw (IgniteCheckedException)err;
        }
        catch (CacheException | IgniteException e) {
            err = e;

            throw e;
        }
        catch (Exception e) {
            err = e;

            throw new IgniteCheckedException(e);
        }
        finally {
            boolean failed = err != null;

            long duration = U.currentTimeMillis() - startTime;

            if (complete || failed) {
                cctx.queries().collectMetrics(qryType, qry, startTime, duration, failed);

                if (log.isTraceEnabled())
                    log.trace("Query execution [startTime=" + startTime + ", duration=" + duration +
                        ", fail=" + failed + ", res=" + res + ']');
            }
        }
    }

    /**
     * @param ver Version.
     */
    public static void setRequestAffinityTopologyVersion(AffinityTopologyVersion ver) {
        requestTopVer.set(ver);
    }

    /**
     * @return Affinity topology version of the current request.
     */
    public static AffinityTopologyVersion getRequestAffinityTopologyVersion() {
        return requestTopVer.get();
    }

    /**
     * Worker which manages overall dynamic index creation process.
     */
    private class DynamicIndexManagerWorker extends GridWorker {
        /** Tasks queue. */
        private final LinkedBlockingQueue<IndexingTask> tasks = new LinkedBlockingQueue<>();

        /** Alive nodes. */
        private Collection<ClusterNode> aliveNodes;

        /** Local node ID. */
        private UUID locNodeId;

        /** Coordinator node. */
        private ClusterNode crdNode;

        /**
         * Constructor.
         *
         * @param igniteInstanceName Ignite instance name.
         * @param log Logger.
         */
        public DynamicIndexManagerWorker(String igniteInstanceName, IgniteLogger log) {
            super(igniteInstanceName, "dynamic-index-manager-worker", log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            // TODO Find coordinator.

            // TODO: Start processing tasks

            while (!isCancelled()) {
                IndexingTask task = tasks.take();

                if (task != null) {
                    if (task instanceof IndexingCacheStartTask) {
                        IndexingCacheStartTask task0 = (IndexingCacheStartTask)task;

                        handleCacheStart(task0.space(), task0.initialIndexStates());
                    }
                    else if (task instanceof IndexingCacheStopTask)
                        handleCacheStop((IndexingCacheStopTask)task);
                    else if (task instanceof IndexingAcceptTask)
                        handleAccept(((IndexingAcceptTask)task).message());
                    else if (task instanceof IndexingFinishTask)
                        handleFinish(((IndexingFinishTask)task).message());
                    else
                        U.warn(log, "Unsupported task: " + task);
                }
            }
        }

        /**
         * Submit new task.
         *
         * @param task Task.
         */
        public void submit(IndexingTask task) {
            tasks.add(task);
        }

        /**
         * Cache started callback.
         *
         * @param space Space.
         * @param initIdxStates Initial index states.
         */
        public void onCacheStart(String space, QueryIndexStates initIdxStates) {
            submit(new IndexingCacheStartTask(space, initIdxStates));
        }

        /**
         * Handle cache start task.
         *
         * @param space Space.
         * @param initIdxStates Initial index states.
         */
        private void handleCacheStart(String space, QueryIndexStates initIdxStates) {
            // TODO: Start active operations.
        }

        /**
         * Cache stopped callback.
         *
         * @param space Space.
         */
        public void onCacheStop(String space) {
            submit(new IndexingCacheStopTask(space));
        }

        /**
         * Handle cache stop task.
         *
         * @param task Task.
         */
        private void handleCacheStop(IndexingCacheStopTask task) {
            // TODO: Correct implementation.
            removeIndexesOnSpaceUnregister(task.space());

            completeIndexClientFuturesOnSpaceUnregister(task.space());
        }

        /**
         * Index accept callback.
         *
         * @param msg Message.
         */
        public void onAccept(IndexAcceptDiscoveryMessage msg) {
            submit(new IndexingAcceptTask(msg));
        }

        /**
         * Handle index accept.
         *
         * @param msg Message.
         */
        private void handleAccept(IndexAcceptDiscoveryMessage msg) {
            // TODO
        }

        /**
         * Index finish callback.
         *
         * @param msg Message.
         */
        public void onFinish(IndexFinishDiscoveryMessage msg) {
            submit(new IndexingFinishTask(msg));
        }

        /**
         * Handle index finish.
         *
         * @param msg Message.
         */
        private void handleFinish(IndexFinishDiscoveryMessage msg) {
            // TODO
        }

        /**
         * Update topology in response to node leave event.
         */
        private void updateTopology() {
            boolean crdChanged = true;
            Collection<ClusterNode> leftNodes = new HashSet<>();

            if (aliveNodes == null) {
                // First call.
                aliveNodes = new HashSet<>(ctx.discovery().aliveServerNodes());

                for (ClusterNode aliveNode : aliveNodes) {
                    if (crdNode == null || crdNode.order() > aliveNode.order()) {
                        crdNode = aliveNode;

                        crdChanged = true;
                    }
                }
            }
            else {
                Collection<ClusterNode> aliveNodes0 = ctx.discovery().aliveServerNodes();

                for (ClusterNode aliveNode : aliveNodes0) {
                    if (!aliveNodes.contains(aliveNode))
                        leftNodes.add(aliveNode);
                }

                aliveNodes = aliveNodes0;

                if (leftNodes.contains(crdNode)) {
                    crdNode = null;

                    for (ClusterNode aliveNode : aliveNodes) {
                        if (crdNode == null || crdNode.order() > aliveNode.order()) {
                            crdNode = aliveNode;

                            crdChanged = true;
                        }
                    }
                }
            }

            for (ClusterNode leftNode : leftNodes) {
                // TODO: Process left nodes
            }

            if (crdChanged) {
                // TODO: Process new coordinator.
            }
        }
    }

    /**
     * Node leave task.
     */
    private static class NodeLeaveTask implements IndexingTask {
        /** Node ID. */
        private final UUID nodeId;

        /**
         * Constructor.
         *
         * @param nodeId Node ID.
         */
        public NodeLeaveTask(UUID nodeId) {
            this.nodeId = nodeId;
        }

        /**
         * @return Node ID.
         */
        public UUID nodeId() {
            return nodeId;
        }
    }

    /**
     * Operation status message received.
     */
    private static class OperationStatusTask implements IndexingTask {
        /** Node ID. */
        private final UUID nodeId;

        /** Operation ID. */
        private final UUID opId;

        /** Error message. */
        private final String errMsg;

        /**
         * Constructor.
         *
         * @param nodeId Node ID.
         * @param opId Operation ID.
         * @param errMsg Error message.
         */
        public OperationStatusTask(UUID nodeId, UUID opId, String errMsg) {
            this.nodeId = nodeId;
            this.opId = opId;
            this.errMsg = errMsg;
        }

        /**
         * @return Node ID.
         */
        public UUID nodeId() {
            return nodeId;
        }

        /**
         * @return Operation ID.
         */
        public UUID operationId() {
            return opId;
        }

        /**
         * @return Error message (if any).
         */
        @Nullable public String errorMessage() {
            return errMsg;
        }
    }
}
