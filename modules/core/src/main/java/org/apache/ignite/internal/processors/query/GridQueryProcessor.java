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
import java.util.concurrent.TimeUnit;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.cache.CacheTypeMetadata;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.CacheQueryExecutedEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDefaultAffinityKeyMapper;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.query.CacheQueryFuture;
import org.apache.ignite.internal.processors.cache.query.CacheQueryType;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.query.property.QueryBinaryProperty;
import org.apache.ignite.internal.processors.query.property.QueryClassProperty;
import org.apache.ignite.internal.processors.query.property.QueryFieldAccessor;
import org.apache.ignite.internal.processors.query.property.QueryMethodsAccessor;
import org.apache.ignite.internal.processors.query.property.QueryPropertyAccessor;
import org.apache.ignite.internal.processors.query.property.QueryReadOnlyMethodsAccessor;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.IgniteOutClosureX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.util.worker.GridWorkerFuture;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;
import static org.apache.ignite.internal.IgniteComponentType.INDEXING;

/**
 * Indexing processor.
 */
public class GridQueryProcessor extends GridProcessorAdapter {
    /** */
    public static final String _VAL = "_val";

    /** */
    private static final Class<?> GEOMETRY_CLASS = U.classForName("com.vividsolutions.jts.geom.Geometry", null);

    /** Queries detail metrics eviction frequency. */
    private static final int QRY_DETAIL_METRICS_EVICTION_FREQ = 3_000;

    /** */
    private static final Set<Class<?>> SQL_TYPES = new HashSet<>(F.<Class<?>>asList(
        Integer.class,
        Boolean.class,
        Byte.class,
        Short.class,
        Long.class,
        BigDecimal.class,
        Double.class,
        Float.class,
        Time.class,
        Timestamp.class,
        java.util.Date.class,
        java.sql.Date.class,
        String.class,
        UUID.class,
        byte[].class
    ));

    /** For tests. */
    public static Class<? extends GridQueryIndexing> idxCls;

    /** */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Type descriptors. */
    private final Map<QueryTypeIdKey, QueryTypeDescriptorImpl> types = new ConcurrentHashMap8<>();

    /** Type descriptors. */
    private final ConcurrentMap<QueryTypeNameKey, QueryTypeDescriptorImpl> typesByName = new ConcurrentHashMap8<>();

    /** */
    private ExecutorService execSvc;

    /** */
    private final GridQueryIndexing idx;

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
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        if (idx != null) {
            ctx.resource().injectGeneric(idx);

            execSvc = ctx.getExecutorService();

            idx.start(ctx, busyLock);
        }

        // Schedule queries detail metrics eviction.
        qryDetailMetricsEvictTask = ctx.timeout().schedule(new Runnable() {
            @Override public void run() {
                for (IgniteCacheProxy cache : ctx.cache().jcaches())
                    cache.context().queries().evictDetailMetrics();
            }
        }, QRY_DETAIL_METRICS_EVICTION_FREQ, QRY_DETAIL_METRICS_EVICTION_FREQ);
    }

    /**
     * @param ccfg Cache configuration.
     * @return {@code true} If query index must be enabled for this cache.
     */
    public static boolean isEnabled(CacheConfiguration<?,?> ccfg) {
        return !F.isEmpty(ccfg.getIndexedTypes()) ||
            !F.isEmpty(ccfg.getTypeMetadata()) ||
            !F.isEmpty(ccfg.getQueryEntities());
    }

    /**
     * @return {@code true} If indexing module is in classpath and successfully initialized.
     */
    public boolean moduleEnabled() {
        return idx != null;
    }

    /**
     * @param cctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    private void initializeCache(GridCacheContext<?, ?> cctx) throws IgniteCheckedException {
        CacheConfiguration<?,?> ccfg = cctx.config();

        idx.registerCache(cctx, cctx.config());

        try {
            List<Class<?>> mustDeserializeClss = null;

            boolean binaryEnabled = ctx.cacheObjects().isBinaryEnabled(ccfg);

            CacheObjectContext coCtx = binaryEnabled ? ctx.cacheObjects().contextForCache(ccfg) : null;

            if (!F.isEmpty(ccfg.getQueryEntities())) {
                for (QueryEntity qryEntity : ccfg.getQueryEntities()) {
                    if (F.isEmpty(qryEntity.getValueType()))
                        throw new IgniteCheckedException("Value type is not set: " + qryEntity);

                    QueryTypeDescriptorImpl desc = new QueryTypeDescriptorImpl();

                    // Key and value classes still can be available if they are primitive or JDK part.
                    // We need that to set correct types for _key and _val columns.
                    Class<?> keyCls = U.classForName(qryEntity.getKeyType(), null);
                    Class<?> valCls = U.classForName(qryEntity.getValueType(), null);

                    // If local node has the classes and they are externalizable, we must use reflection properties.
                    boolean keyMustDeserialize = mustDeserializeBinary(keyCls);
                    boolean valMustDeserialize = mustDeserializeBinary(valCls);

                    boolean keyOrValMustDeserialize = keyMustDeserialize || valMustDeserialize;

                    if (keyCls == null)
                        keyCls = Object.class;

                    String simpleValType = ((valCls == null) ? typeName(qryEntity.getValueType()) : typeName(valCls));

                    desc.name(simpleValType);

                    desc.tableName(qryEntity.getTableName());

                    if (binaryEnabled && !keyOrValMustDeserialize) {
                        // Safe to check null.
                        if (SQL_TYPES.contains(valCls))
                            desc.valueClass(valCls);
                        else
                            desc.valueClass(Object.class);

                        if (SQL_TYPES.contains(keyCls))
                            desc.keyClass(keyCls);
                        else
                            desc.keyClass(Object.class);
                    }
                    else {
                        if (keyCls == null)
                            throw new IgniteCheckedException("Failed to find key class in the node classpath " +
                                "(use default marshaller to enable binary objects): " + qryEntity.getKeyType());

                        if (valCls == null)
                            throw new IgniteCheckedException("Failed to find value class in the node classpath " +
                                "(use default marshaller to enable binary objects) : " + qryEntity.getValueType());

                        desc.valueClass(valCls);
                        desc.keyClass(keyCls);
                    }

                    desc.keyTypeName(qryEntity.getKeyType());
                    desc.valueTypeName(qryEntity.getValueType());

                    if (binaryEnabled && keyOrValMustDeserialize) {
                        if (mustDeserializeClss == null)
                            mustDeserializeClss = new ArrayList<>();

                        if (keyMustDeserialize)
                            mustDeserializeClss.add(keyCls);

                        if (valMustDeserialize)
                            mustDeserializeClss.add(valCls);
                    }

                    QueryTypeIdKey typeId;
                    QueryTypeIdKey altTypeId = null;

                    if (valCls == null || (binaryEnabled && !keyOrValMustDeserialize)) {
                        processBinaryMeta(qryEntity, desc);

                        typeId = new QueryTypeIdKey(ccfg.getName(), ctx.cacheObjects().typeId(qryEntity.getValueType()));

                        if (valCls != null)
                            altTypeId = new QueryTypeIdKey(ccfg.getName(), valCls);

                        if (!cctx.customAffinityMapper() && qryEntity.getKeyType() != null) {
                            // Need to setup affinity key for distributed joins.
                            String affField = ctx.cacheObjects().affinityField(qryEntity.getKeyType());

                            if (affField != null)
                                desc.affinityKey(affField);
                        }
                    }
                    else {
                        processClassMeta(qryEntity, desc, coCtx);

                        AffinityKeyMapper keyMapper = cctx.config().getAffinityMapper();

                        if (keyMapper instanceof GridCacheDefaultAffinityKeyMapper) {
                            String affField =
                                ((GridCacheDefaultAffinityKeyMapper)keyMapper).affinityKeyPropertyName(desc.keyClass());

                            if (affField != null)
                                desc.affinityKey(affField);
                        }

                        typeId = new QueryTypeIdKey(ccfg.getName(), valCls);
                        altTypeId = new QueryTypeIdKey(ccfg.getName(), ctx.cacheObjects().typeId(qryEntity.getValueType()));
                    }

                    addTypeByName(ccfg, desc);
                    types.put(typeId, desc);

                    if (altTypeId != null)
                        types.put(altTypeId, desc);

                    desc.registered(idx.registerType(ccfg.getName(), desc));
                }
            }

            if (!F.isEmpty(ccfg.getTypeMetadata())) {
                for (CacheTypeMetadata meta : ccfg.getTypeMetadata()) {
                    if (F.isEmpty(meta.getValueType()))
                        throw new IgniteCheckedException("Value type is not set: " + meta);

                    if (meta.getQueryFields().isEmpty() && meta.getAscendingFields().isEmpty() &&
                        meta.getDescendingFields().isEmpty() && meta.getGroups().isEmpty())
                        continue;

                    QueryTypeDescriptorImpl desc = new QueryTypeDescriptorImpl();

                    // Key and value classes still can be available if they are primitive or JDK part.
                    // We need that to set correct types for _key and _val columns.
                    Class<?> keyCls = U.classForName(meta.getKeyType(), null);
                    Class<?> valCls = U.classForName(meta.getValueType(), null);

                    // If local node has the classes and they are externalizable, we must use reflection properties.
                    boolean keyMustDeserialize = mustDeserializeBinary(keyCls);
                    boolean valMustDeserialize = mustDeserializeBinary(valCls);

                    boolean keyOrValMustDeserialize = keyMustDeserialize || valMustDeserialize;

                    if (keyCls == null)
                        keyCls = Object.class;

                    String simpleValType = meta.getSimpleValueType();

                    if (simpleValType == null)
                        simpleValType = typeName(meta.getValueType());

                    desc.name(simpleValType);

                    if (binaryEnabled && !keyOrValMustDeserialize) {
                        // Safe to check null.
                        if (SQL_TYPES.contains(valCls))
                            desc.valueClass(valCls);
                        else
                            desc.valueClass(Object.class);

                        if (SQL_TYPES.contains(keyCls))
                            desc.keyClass(keyCls);
                        else
                            desc.keyClass(Object.class);
                    }
                    else {
                        desc.valueClass(valCls);
                        desc.keyClass(keyCls);
                    }

                    desc.keyTypeName(meta.getKeyType());
                    desc.valueTypeName(meta.getValueType());

                    if (binaryEnabled && keyOrValMustDeserialize) {
                        if (mustDeserializeClss == null)
                            mustDeserializeClss = new ArrayList<>();

                        if (keyMustDeserialize)
                            mustDeserializeClss.add(keyCls);

                        if (valMustDeserialize)
                            mustDeserializeClss.add(valCls);
                    }

                    QueryTypeIdKey typeId;
                    QueryTypeIdKey altTypeId = null;

                    if (valCls == null || (binaryEnabled && !keyOrValMustDeserialize)) {
                        processBinaryMeta(meta, desc);

                        typeId = new QueryTypeIdKey(ccfg.getName(), ctx.cacheObjects().typeId(meta.getValueType()));

                        if (valCls != null)
                            altTypeId = new QueryTypeIdKey(ccfg.getName(), valCls);
                    }
                    else {
                        processClassMeta(meta, desc, coCtx);

                        typeId = new QueryTypeIdKey(ccfg.getName(), valCls);
                        altTypeId = new QueryTypeIdKey(ccfg.getName(), ctx.cacheObjects().typeId(meta.getValueType()));
                    }

                    addTypeByName(ccfg, desc);
                    types.put(typeId, desc);

                    if (altTypeId != null)
                        types.put(altTypeId, desc);

                    desc.registered(idx.registerType(ccfg.getName(), desc));
                }
            }

            // Indexed types must be translated to CacheTypeMetadata in CacheConfiguration.

            if (mustDeserializeClss != null) {
                U.warn(log, "Some classes in query configuration cannot be written in binary format " +
                    "because they either implement Externalizable interface or have writeObject/readObject methods. " +
                    "Instances of these classes will be deserialized in order to build indexes. Please ensure that " +
                    "all nodes have these classes in classpath. To enable binary serialization either implement " +
                    Binarylizable.class.getSimpleName() + " interface or set explicit serializer using " +
                    "BinaryTypeConfiguration.setSerializer() method: " + mustDeserializeClss);
            }
        }
        catch (IgniteCheckedException | RuntimeException e) {
            idx.unregisterCache(ccfg);

            throw e;
        }
    }

    /**
     * Check whether type still must be deserialized when binary marshaller is set.
     *
     * @param cls Class.
     * @return {@code True} if will be deserialized.
     */
    private boolean mustDeserializeBinary(Class cls) {
        if (cls != null && ctx.config().getMarshaller() instanceof BinaryMarshaller) {
            CacheObjectBinaryProcessorImpl proc0 = (CacheObjectBinaryProcessorImpl)ctx.cacheObjects();

            return proc0.binaryContext().mustDeserialize(cls);
        }
        else
            return false;
    }

    /**
     * @param ccfg Cache configuration.
     * @param desc Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    private void addTypeByName(CacheConfiguration<?, ?> ccfg, QueryTypeDescriptorImpl desc) throws IgniteCheckedException {
        if (typesByName.putIfAbsent(new QueryTypeNameKey(ccfg.getName(), desc.name()), desc) != null)
            throw new IgniteCheckedException("Type with name '" + desc.name() + "' already indexed " +
                "in cache '" + ccfg.getName() + "'.");
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        super.onKernalStop(cancel);

        if (cancel && idx != null)
            try {
                while (!busyLock.tryBlock(500))
                    idx.cancelAllQueries();

                return;
            }
            catch (InterruptedException ignored) {
                U.warn(log, "Interrupted while waiting for active queries cancellation.");

                Thread.currentThread().interrupt();
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
    }

    /**
     * @param cctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    public void onCacheStart(GridCacheContext cctx) throws IgniteCheckedException {
        if (idx == null)
            return;

        if (!busyLock.enterBusy())
            return;

        try {
            initializeCache(cctx);
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
            idx.unregisterCache(cctx.config());

            Iterator<Map.Entry<QueryTypeIdKey, QueryTypeDescriptorImpl>> it = types.entrySet().iterator();

            while (it.hasNext()) {
                Map.Entry<QueryTypeIdKey, QueryTypeDescriptorImpl> entry = it.next();

                if (F.eq(cctx.name(), entry.getKey().space())) {
                    it.remove();

                    typesByName.remove(new QueryTypeNameKey(cctx.name(), entry.getValue().name()));
                }
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to clear indexing on cache stop (will ignore): " + cctx.name(), e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Rebuilds all search indexes of given value type for given space of spi.
     *
     * @param space Space.
     * @param valTypeName Value type name.
     * @return Future that will be completed when rebuilding of all indexes is finished.
     */
    public IgniteInternalFuture<?> rebuildIndexes(@Nullable final String space, String valTypeName) {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to rebuild indexes (grid is stopping).");

        try {
            return rebuildIndexes(
                space,
                typesByName.get(
                    new QueryTypeNameKey(
                        space,
                        valTypeName)));
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param space Space.
     * @param desc Type descriptor.
     * @return Future that will be completed when rebuilding of all indexes is finished.
     */
    private IgniteInternalFuture<?> rebuildIndexes(@Nullable final String space, @Nullable final QueryTypeDescriptorImpl desc) {
        if (idx == null)
            return new GridFinishedFuture<>(new IgniteCheckedException("Indexing is disabled."));

        if (desc == null || !desc.registered())
            return new GridFinishedFuture<Void>();

        final GridWorkerFuture<?> fut = new GridWorkerFuture<Void>();

        GridWorker w = new GridWorker(ctx.gridName(), "index-rebuild-worker", log) {
            @Override protected void body() {
                try {
                    idx.rebuildIndexes(space, desc);

                    fut.onDone();
                }
                catch (Exception e) {
                    fut.onDone(e);
                }
                catch (Throwable e) {
                    log.error("Failed to rebuild indexes for type: " + desc.name(), e);

                    fut.onDone(e);

                    if (e instanceof Error)
                        throw e;
                }
            }
        };

        fut.setWorker(w);

        execSvc.execute(w);

        return fut;
    }

    /**
     * Rebuilds all search indexes for given spi.
     *
     * @return Future that will be completed when rebuilding of all indexes is finished.
     */
    @SuppressWarnings("unchecked")
    public IgniteInternalFuture<?> rebuildAllIndexes() {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to get space size (grid is stopping).");

        try {
            GridCompoundFuture<?, ?> fut = new GridCompoundFuture<Object, Object>();

            for (Map.Entry<QueryTypeIdKey, QueryTypeDescriptorImpl> e : types.entrySet())
                fut.add((IgniteInternalFuture)rebuildIndexes(e.getKey().space(), e.getValue()));

            fut.markInitialized();

            return fut;
        }
        finally {
            busyLock.leaveBusy();
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
    @SuppressWarnings("unchecked")
    public void store(final String space, final CacheObject key, final CacheObject val,
        byte[] ver, long expirationTime) throws IgniteCheckedException {
        assert key != null;
        assert val != null;

        if (log.isDebugEnabled())
            log.debug("Store [space=" + space + ", key=" + key + ", val=" + val + "]");

        CacheObjectContext coctx = null;

        if (idx == null)
            return;

        if (!busyLock.enterBusy())
            return;

        try {
            if (coctx == null)
                coctx = cacheObjectContext(space);

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

            idx.store(space, desc, key, val, ver, expirationTime);
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
     * @param space Space.
     * @param clause Clause.
     * @param params Parameters collection.
     * @param resType Result type.
     * @param filters Filters.
     * @return Key/value rows.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> query(final String space, final String clause,
        final Collection<Object> params, final String resType, final IndexingQueryFilter filters)
        throws IgniteCheckedException {
        checkEnabled();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            final GridCacheContext<?, ?> cctx = ctx.cache().internalCache(space).context();

            return executeQuery(GridCacheQueryType.SQL_FIELDS, clause, cctx, new IgniteOutClosureX<GridCloseableIterator<IgniteBiTuple<K, V>>>() {
                @Override public GridCloseableIterator<IgniteBiTuple<K, V>> applyx() throws IgniteCheckedException {
                    QueryTypeDescriptorImpl type = typesByName.get(new QueryTypeNameKey(space, resType));

                    if (type == null || !type.registered())
                        throw new CacheException("Failed to find SQL table for type: " + resType);

                    return idx.queryLocalSql(space, clause, null, params, type, filters);
                }
            }, false);
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
     * @param cctx Cache context.
     * @param qry Query.
     * @return Cursor.
     */
    public <K,V> QueryCursor<Cache.Entry<K,V>> queryTwoStep(final GridCacheContext<?,?> cctx, final SqlQuery qry) {
        checkxEnabled();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            return executeQuery(GridCacheQueryType.SQL, qry.getSql(), cctx, new IgniteOutClosureX<QueryCursor<Cache.Entry<K, V>>>() {
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
    public <K, V> Iterator<Cache.Entry<K, V>> queryLocal(
        final GridCacheContext<?, ?> cctx,
        final SqlQuery qry,
        final boolean keepBinary
    ) {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            return executeQuery(GridCacheQueryType.SQL, qry.getSql(), cctx,
                new IgniteOutClosureX<Iterator<Cache.Entry<K, V>>>() {
                    @Override public Iterator<Cache.Entry<K, V>> applyx() throws IgniteCheckedException {
                        String space = cctx.name();
                        String type = qry.getType();
                        String sqlQry = qry.getSql();
                        Object[] params = qry.getArgs();

                        QueryTypeDescriptorImpl typeDesc = typesByName.get(
                            new QueryTypeNameKey(
                                space,
                                type));

                        if (typeDesc == null || !typeDesc.registered())
                            throw new CacheException("Failed to find SQL table for type: " + type);

                        final GridCloseableIterator<IgniteBiTuple<K, V>> i = idx.queryLocalSql(
                            space,
                            qry.getSql(),
                            qry.getAlias(),
                            F.asList(params),
                            typeDesc,
                            idx.backupFilter(requestTopVer.get(), null));

                        sendQueryExecutedEvent(
                            sqlQry,
                            params);

                        return new ClIter<Cache.Entry<K, V>>() {
                            @Override public void close() throws Exception {
                                i.close();
                            }

                            @Override public boolean hasNext() {
                                return i.hasNext();
                            }

                            @Override public Cache.Entry<K, V> next() {
                                IgniteBiTuple<K, V> t = i.next();

                                return new CacheEntryImpl<>(
                                    (K)cctx.unwrapBinaryIfNeeded(t.getKey(), keepBinary, false),
                                    (V)cctx.unwrapBinaryIfNeeded(t.getValue(), keepBinary, false));
                            }

                            @Override public void remove() {
                                throw new UnsupportedOperationException();
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
     * @param schema Schema.
     * @param sql Query.
     * @return {@link PreparedStatement} from underlying engine to supply metadata to Prepared - most likely H2.
     */
    public PreparedStatement prepareNativeStatement(String schema, String sql) throws SQLException {
        checkxEnabled();

        return idx.prepareNativeStatement(schema, sql);
    }

    /**
     * @param timeout Timeout.
     * @param timeUnit Time unit.
     * @return Converted time.
     */
    public static int validateTimeout(int timeout, TimeUnit timeUnit) {
        A.ensure(timeUnit != TimeUnit.MICROSECONDS && timeUnit != TimeUnit.NANOSECONDS,
            "timeUnit minimal resolution is millisecond.");

        A.ensure(timeout >= 0, "timeout value should be non-negative.");

        long tmp = TimeUnit.MILLISECONDS.convert(timeout, timeUnit);

        A.ensure(timeout <= Integer.MAX_VALUE, "timeout value too large.");

        return (int) tmp;
    }

    /**
     * Closeable iterator.
     */
    private interface ClIter<X> extends AutoCloseable, Iterator<X> {
        // No-op.
    }

    /**
     * @param cctx Cache context.
     * @param qry Query.
     * @return Iterator.
     */
    public QueryCursor<List<?>> queryLocalFields(final GridCacheContext<?, ?> cctx, final SqlFieldsQuery qry) {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            final boolean keepBinary = cctx.keepBinary();

            return executeQuery(GridCacheQueryType.SQL_FIELDS, qry.getSql(), cctx, new IgniteOutClosureX<QueryCursor<List<?>>>() {
                @Override public QueryCursor<List<?>> applyx() throws IgniteCheckedException {
                    final String space = cctx.name();
                    final String sql = qry.getSql();
                    final Object[] args = qry.getArgs();
                    final GridQueryCancel cancel = new GridQueryCancel();

                    final GridQueryFieldsResult res = idx.queryLocalSqlFields(space, sql, F.asList(args),
                        idx.backupFilter(requestTopVer.get(), null), qry.isEnforceJoinOrder(), qry.getTimeout(), cancel);

                    QueryCursorImpl<List<?>> cursor = new QueryCursorImpl<>(new Iterable<List<?>>() {
                        @Override public Iterator<List<?>> iterator() {
                            try {
                                sendQueryExecutedEvent(sql, args);

                                return new GridQueryCacheObjectsIterator(res.iterator(), cctx, keepBinary);
                            }
                            catch (IgniteCheckedException e) {
                                throw new IgniteException(e);
                            }
                        }
                    }, cancel);

                    cursor.fieldsMeta(res.metaData());

                    return cursor;
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
     * Checks if the given class can be mapped to a simple SQL type.
     *
     * @param cls Class.
     * @return {@code true} If can.
     */
    public static boolean isSqlType(Class<?> cls) {
        cls = U.box(cls);

        return SQL_TYPES.contains(cls) || isGeometryClass(cls);
    }

    /**
     * Checks if the given class is GEOMETRY.
     *
     * @param cls Class.
     * @return {@code true} If this is geometry.
     */
    public static boolean isGeometryClass(Class<?> cls) {
        return GEOMETRY_CLASS != null && GEOMETRY_CLASS.isAssignableFrom(cls);
    }

    /**
     * Gets type name by class.
     *
     * @param cls Class.
     * @return Type name.
     */
    public static String typeName(Class<?> cls) {
        String typeName = cls.getSimpleName();

        // To protect from failure on anonymous classes.
        if (F.isEmpty(typeName)) {
            String pkg = cls.getPackage().getName();

            typeName = cls.getName().substring(pkg.length() + (pkg.isEmpty() ? 0 : 1));
        }

        if (cls.isArray()) {
            assert typeName.endsWith("[]");

            typeName = typeName.substring(0, typeName.length() - 2) + "_array";
        }

        return typeName;
    }

    /**
     * Gets type name by class.
     *
     * @param clsName Class name.
     * @return Type name.
     */
    public static String typeName(String clsName) {
        int pkgEnd = clsName.lastIndexOf('.');

        if (pkgEnd >= 0 && pkgEnd < clsName.length() - 1)
            clsName = clsName.substring(pkgEnd + 1);

        if (clsName.endsWith("[]"))
            clsName = clsName.substring(0, clsName.length() - 2) + "_array";

        int parentEnd = clsName.lastIndexOf('$');

        if (parentEnd >= 0)
            clsName = clsName.substring(parentEnd + 1);

        return clsName;
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
                        QueryTypeDescriptorImpl type = typesByName.get(new QueryTypeNameKey(space, resType));

                        if (type == null || !type.registered())
                            throw new CacheException("Failed to find SQL table for type: " + resType);

                        return idx.queryLocalText(
                            space,
                            clause,
                            type,
                            filters);
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
     * Removes index tables for all classes belonging to given class loader.
     *
     * @param space Space name.
     * @param ldr Class loader to undeploy.
     * @throws IgniteCheckedException If undeploy failed.
     */
    public void onUndeploy(@Nullable String space, ClassLoader ldr) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Undeploy [space=" + space + "]");

        if (idx == null)
            return;

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to process undeploy event (grid is stopping).");

        try {
            Iterator<Map.Entry<QueryTypeIdKey, QueryTypeDescriptorImpl>> it = types.entrySet().iterator();

            while (it.hasNext()) {
                Map.Entry<QueryTypeIdKey, QueryTypeDescriptorImpl> e = it.next();

                if (!F.eq(e.getKey().space(), space))
                    continue;

                QueryTypeDescriptorImpl desc = e.getValue();

                if (ldr.equals(U.detectClassLoader(desc.valueClass())) ||
                    ldr.equals(U.detectClassLoader(desc.keyClass()))) {
                    idx.unregisterType(e.getKey().space(), desc);

                    it.remove();
                }
            }
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Processes declarative metadata for class.
     *
     * @param meta Type metadata.
     * @param d Type descriptor.
     * @param coCtx Cache object context.
     * @throws IgniteCheckedException If failed.
     */
    private void processClassMeta(CacheTypeMetadata meta, QueryTypeDescriptorImpl d, CacheObjectContext coCtx)
        throws IgniteCheckedException {
        Map<String,String> aliases = meta.getAliases();

        if (aliases == null)
            aliases = Collections.emptyMap();

        Class<?> keyCls = d.keyClass();
        Class<?> valCls = d.valueClass();

        assert keyCls != null;
        assert valCls != null;

        for (Map.Entry<String, Class<?>> entry : meta.getAscendingFields().entrySet())
            addToIndex(d, keyCls, valCls, entry.getKey(), entry.getValue(), 0, IndexType.ASC, null, aliases, coCtx);

        for (Map.Entry<String, Class<?>> entry : meta.getDescendingFields().entrySet())
            addToIndex(d, keyCls, valCls, entry.getKey(), entry.getValue(), 0, IndexType.DESC, null, aliases, coCtx);

        for (String txtField : meta.getTextFields())
            addToIndex(d, keyCls, valCls, txtField, String.class, 0, IndexType.TEXT, null, aliases, coCtx);

        Map<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> grps = meta.getGroups();

        if (grps != null) {
            for (Map.Entry<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> entry : grps.entrySet()) {
                String idxName = entry.getKey();

                LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>> idxFields = entry.getValue();

                int order = 0;

                for (Map.Entry<String, IgniteBiTuple<Class<?>, Boolean>> idxField : idxFields.entrySet()) {
                    Boolean descending = idxField.getValue().get2();

                    if (descending == null)
                        descending = false;

                    addToIndex(d, keyCls, valCls, idxField.getKey(), idxField.getValue().get1(), order,
                        descending ? IndexType.DESC : IndexType.ASC, idxName, aliases, coCtx);

                    order++;
                }
            }
        }

        for (Map.Entry<String, Class<?>> entry : meta.getQueryFields().entrySet()) {
            QueryClassProperty prop = buildClassProperty(
                keyCls,
                valCls,
                entry.getKey(),
                entry.getValue(),
                aliases,
                coCtx);

            d.addProperty(prop, false);
        }
    }

    /**
     * @param d Type descriptor.
     * @param keyCls Key class.
     * @param valCls Value class.
     * @param pathStr Path string.
     * @param resType Result type.
     * @param idxOrder Order number in index or {@code -1} if no need to index.
     * @param idxType Index type.
     * @param idxName Index name.
     * @param aliases Aliases.
     * @throws IgniteCheckedException If failed.
     */
    private void addToIndex(
        QueryTypeDescriptorImpl d,
        Class<?> keyCls,
        Class<?> valCls,
        String pathStr,
        Class<?> resType,
        int idxOrder,
        IndexType idxType,
        String idxName,
        Map<String,String> aliases,
        CacheObjectContext coCtx
    ) throws IgniteCheckedException {
        String propName;
        Class<?> propCls;

        if (_VAL.equals(pathStr)) {
            propName = _VAL;
            propCls = valCls;
        }
        else {
            QueryClassProperty prop = buildClassProperty(
                keyCls,
                valCls,
                pathStr,
                resType,
                aliases,
                coCtx);

            d.addProperty(prop, false);

            propName = prop.name();
            propCls = prop.type();
        }

        if (idxType != null) {
            if (idxName == null)
                idxName = propName + "_idx";

            if (idxOrder == 0) // Add index only on the first field.
                d.addIndex(idxName, isGeometryClass(propCls) ? QueryIndexType.GEOSPATIAL : QueryIndexType.SORTED);

            if (idxType == IndexType.TEXT)
                d.addFieldToTextIndex(propName);
            else
                d.addFieldToIndex(idxName, propName, idxOrder, idxType == IndexType.DESC);
        }
    }

    /**
     * Processes declarative metadata for binary object.
     *
     * @param meta Declared metadata.
     * @param d Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    private void processBinaryMeta(CacheTypeMetadata meta, QueryTypeDescriptorImpl d)
        throws IgniteCheckedException {
        Map<String,String> aliases = meta.getAliases();

        if (aliases == null)
            aliases = Collections.emptyMap();

        for (Map.Entry<String, Class<?>> entry : meta.getAscendingFields().entrySet()) {
            QueryBinaryProperty prop = buildBinaryProperty(entry.getKey(), entry.getValue(), aliases, null);

            d.addProperty(prop, false);

            String idxName = prop.name() + "_idx";

            d.addIndex(idxName, isGeometryClass(prop.type()) ? QueryIndexType.GEOSPATIAL : QueryIndexType.SORTED);

            d.addFieldToIndex(idxName, prop.name(), 0, false);
        }

        for (Map.Entry<String, Class<?>> entry : meta.getDescendingFields().entrySet()) {
            QueryBinaryProperty prop = buildBinaryProperty(entry.getKey(), entry.getValue(), aliases, null);

            d.addProperty(prop, false);

            String idxName = prop.name() + "_idx";

            d.addIndex(idxName, isGeometryClass(prop.type()) ? QueryIndexType.GEOSPATIAL : QueryIndexType.SORTED);

            d.addFieldToIndex(idxName, prop.name(), 0, true);
        }

        for (String txtIdx : meta.getTextFields()) {
            QueryBinaryProperty prop = buildBinaryProperty(txtIdx, String.class, aliases, null);

            d.addProperty(prop, false);

            d.addFieldToTextIndex(prop.name());
        }

        Map<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> grps = meta.getGroups();

        if (grps != null) {
            for (Map.Entry<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> entry : grps.entrySet()) {
                String idxName = entry.getKey();

                LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>> idxFields = entry.getValue();

                int order = 0;

                for (Map.Entry<String, IgniteBiTuple<Class<?>, Boolean>> idxField : idxFields.entrySet()) {
                    QueryBinaryProperty prop = buildBinaryProperty(idxField.getKey(), idxField.getValue().get1(), aliases,
                        null);

                    d.addProperty(prop, false);

                    Boolean descending = idxField.getValue().get2();

                    d.addFieldToIndex(idxName, prop.name(), order, descending != null && descending);

                    order++;
                }
            }
        }

        for (Map.Entry<String, Class<?>> entry : meta.getQueryFields().entrySet()) {
            QueryBinaryProperty prop = buildBinaryProperty(entry.getKey(), entry.getValue(), aliases, null);

            if (!d.properties().containsKey(prop.name()))
                d.addProperty(prop, false);
        }
    }

    /**
     * Processes declarative metadata for binary object.
     *
     * @param qryEntity Declared metadata.
     * @param d Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    private void processBinaryMeta(QueryEntity qryEntity, QueryTypeDescriptorImpl d) throws IgniteCheckedException {
        Map<String,String> aliases = qryEntity.getAliases();

        if (aliases == null)
            aliases = Collections.emptyMap();

        Set<String> keyFields = qryEntity.getKeyFields();

        // We have to distinguish between empty and null keyFields when the key is not of SQL type -
        // when a key is not of SQL type, absence of a field in nonnull keyFields tell us that this field
        // is a value field, and null keyFields tells us that current configuration
        // does not tell us anything about this field's ownership.
        boolean hasKeyFields = (keyFields != null);

        boolean isKeyClsSqlType = isSqlType(d.keyClass());

        if (hasKeyFields && !isKeyClsSqlType) {
            //ensure that 'keyFields' is case sensitive subset of 'fields'
            for (String keyField : keyFields) {
                if (!qryEntity.getFields().containsKey(keyField))
                    throw new IgniteCheckedException("QueryEntity 'keyFields' property must be a subset of keys " +
                        "from 'fields' property (case sensitive): " + keyField);
            }
        }

        for (Map.Entry<String, String> entry : qryEntity.getFields().entrySet()) {
            Boolean isKeyField;

            if (isKeyClsSqlType) // We don't care about keyFields in this case - it might be null, or empty, or anything
                isKeyField = false;
            else
                isKeyField = (hasKeyFields ? keyFields.contains(entry.getKey()) : null);

            QueryBinaryProperty prop = buildBinaryProperty(entry.getKey(),
                U.classForName(entry.getValue(), Object.class, true), aliases, isKeyField);

            d.addProperty(prop, false);
        }

        processIndexes(qryEntity, d);
    }

    /**
     * Processes declarative metadata for binary object.
     *
     * @param qryEntity Declared metadata.
     * @param d Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    private void processClassMeta(
        QueryEntity qryEntity,
        QueryTypeDescriptorImpl d,
        CacheObjectContext coCtx
    ) throws IgniteCheckedException {
        Map<String,String> aliases = qryEntity.getAliases();

        if (aliases == null)
            aliases = Collections.emptyMap();

        for (Map.Entry<String, String> entry : qryEntity.getFields().entrySet()) {
            QueryClassProperty prop = buildClassProperty(
                d.keyClass(),
                d.valueClass(),
                entry.getKey(),
                U.classForName(entry.getValue(), Object.class),
                aliases,
                coCtx);

            d.addProperty(prop, false);
        }

        processIndexes(qryEntity, d);
    }

    /**
     * Processes indexes based on query entity.
     *
     * @param qryEntity Query entity to process.
     * @param d Type descriptor to populate.
     * @throws IgniteCheckedException If failed to build index information.
     */
    private void processIndexes(QueryEntity qryEntity, QueryTypeDescriptorImpl d) throws IgniteCheckedException {
        if (!F.isEmpty(qryEntity.getIndexes())) {
            Map<String, String> aliases = qryEntity.getAliases();

            if (aliases == null)
                aliases = Collections.emptyMap();

            for (QueryIndex idx : qryEntity.getIndexes()) {
                String idxName = idx.getName();

                if (idxName == null)
                    idxName = QueryEntity.defaultIndexName(idx);

                QueryIndexType idxTyp = idx.getIndexType();

                if (idxTyp == QueryIndexType.SORTED || idxTyp == QueryIndexType.GEOSPATIAL) {
                    d.addIndex(idxName, idxTyp);

                    int i = 0;

                    for (Map.Entry<String, Boolean> entry : idx.getFields().entrySet()) {
                        String field = entry.getKey();
                        boolean asc = entry.getValue();

                        String alias = aliases.get(field);

                        if (alias != null)
                            field = alias;

                        d.addFieldToIndex(idxName, field, i++, !asc);
                    }
                }
                else if (idxTyp == QueryIndexType.FULLTEXT){
                    for (String field : idx.getFields().keySet()) {
                        String alias = aliases.get(field);

                        if (alias != null)
                            field = alias;

                        d.addFieldToTextIndex(field);
                    }
                }
                else if (idxTyp != null)
                    throw new IllegalArgumentException("Unsupported index type [idx=" + idx.getName() +
                        ", typ=" + idxTyp + ']');
                else
                    throw new IllegalArgumentException("Index type is not set: " + idx.getName());
            }
        }
    }

    /**
     * Builds binary object property.
     *
     * @param pathStr String representing path to the property. May contains dots '.' to identify
     *      nested fields.
     * @param resType Result type.
     * @param aliases Aliases.
     * @param isKeyField Key ownership flag, as defined in {@link QueryEntity#keyFields}: {@code true} if field belongs
     *      to key, {@code false} if it belongs to value, {@code null} if QueryEntity#keyFields is null.
     * @return Binary property.
     */
    private QueryBinaryProperty buildBinaryProperty(String pathStr, Class<?> resType, Map<String, String> aliases,
        @Nullable Boolean isKeyField) throws IgniteCheckedException {
        String[] path = pathStr.split("\\.");

        QueryBinaryProperty res = null;

        StringBuilder fullName = new StringBuilder();

        for (String prop : path) {
            if (fullName.length() != 0)
                fullName.append('.');

            fullName.append(prop);

            String alias = aliases.get(fullName.toString());

            // The key flag that we've found out is valid for the whole path.
            res = new QueryBinaryProperty(ctx, log, prop, res, resType, isKeyField, alias);
        }

        return res;
    }

    /**
     * @param keyCls Key class.
     * @param valCls Value class.
     * @param pathStr Path string.
     * @param resType Result type.
     * @param aliases Aliases.
     * @return Class property.
     * @throws IgniteCheckedException If failed.
     */
    private static QueryClassProperty buildClassProperty(Class<?> keyCls, Class<?> valCls, String pathStr, Class<?> resType,
        Map<String,String> aliases, CacheObjectContext coCtx) throws IgniteCheckedException {
        QueryClassProperty res = buildClassProperty(
            true,
            keyCls,
            pathStr,
            resType,
            aliases,
            coCtx);

        if (res == null) // We check key before value consistently with BinaryProperty.
            res = buildClassProperty(false, valCls, pathStr, resType, aliases, coCtx);

        if (res == null)
            throw new IgniteCheckedException("Failed to initialize property '" + pathStr + "' of type '" +
                resType.getName() + "' for key class '" + keyCls + "' and value class '" + valCls + "'. " +
                "Make sure that one of these classes contains respective getter method or field.");

        return res;
    }

    /**
     * @param key If this is a key property.
     * @param cls Source type class.
     * @param pathStr String representing path to the property. May contains dots '.' to identify nested fields.
     * @param resType Expected result type.
     * @param aliases Aliases.
     * @return Property instance corresponding to the given path.
     */
    private static QueryClassProperty buildClassProperty(boolean key, Class<?> cls, String pathStr, Class<?> resType,
        Map<String,String> aliases, CacheObjectContext coCtx) {
        String[] path = pathStr.split("\\.");

        QueryClassProperty res = null;

        StringBuilder fullName = new StringBuilder();

        for (String prop : path) {
            if (fullName.length() != 0)
                fullName.append('.');

            fullName.append(prop);

            String alias = aliases.get(fullName.toString());

            QueryPropertyAccessor accessor = findProperty(prop, cls);

            if (accessor == null)
                return null;

            QueryClassProperty tmp = new QueryClassProperty(accessor, key, alias, coCtx);

            tmp.parent(res);

            cls = tmp.type();

            res = tmp;
        }

        if (!U.box(resType).isAssignableFrom(U.box(res.type())))
            return null;

        return res;
    }

    /**
     * Gets types for space.
     *
     * @param space Space name.
     * @return Descriptors.
     */
    public Collection<GridQueryTypeDescriptor> types(@Nullable String space) {
        Collection<GridQueryTypeDescriptor> spaceTypes = new ArrayList<>(
            Math.min(10, types.size()));

        for (Map.Entry<QueryTypeIdKey, QueryTypeDescriptorImpl> e : types.entrySet()) {
            QueryTypeDescriptorImpl desc = e.getValue();

            if (desc.registered() && F.eq(e.getKey().space(), space))
                spaceTypes.add(desc);
        }

        return spaceTypes;
    }

    /**
     * Gets type descriptor for space and type name.
     *
     * @param space Space name.
     * @param typeName Type name.
     * @return Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    public GridQueryTypeDescriptor type(@Nullable String space, String typeName) throws IgniteCheckedException {
        QueryTypeDescriptorImpl type = typesByName.get(new QueryTypeNameKey(space, typeName));

        if (type == null || !type.registered())
            throw new IgniteCheckedException("Failed to find type descriptor for type name: " + typeName);

        return type;
    }

    /**
     * @param qryType Query type.
     * @param qry Query description.
     * @param cctx Cache context.
     * @param clo Closure.
     * @param complete Complete.
     */
    public <R> R executeQuery(GridCacheQueryType qryType, String qry, GridCacheContext<?, ?> cctx, IgniteOutClosureX<R> clo, boolean complete)
        throws IgniteCheckedException {
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
        catch (CacheException e) {
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
     * Find a member (either a getter method or a field) with given name of given class.
     * @param prop Property name.
     * @param cls Class to search for a member in.
     * @return Member for given name.
     */
    @Nullable private static QueryPropertyAccessor findProperty(String prop, Class<?> cls) {
        StringBuilder getBldr = new StringBuilder("get");
        getBldr.append(prop);
        getBldr.setCharAt(3, Character.toUpperCase(getBldr.charAt(3)));

        StringBuilder setBldr = new StringBuilder("set");
        setBldr.append(prop);
        setBldr.setCharAt(3, Character.toUpperCase(setBldr.charAt(3)));

        try {
            Method getter = cls.getMethod(getBldr.toString());

            Method setter;

            try {
                // Setter has to have the same name like 'setXxx' and single param of the same type
                // as the return type of the getter.
                setter = cls.getMethod(setBldr.toString(), getter.getReturnType());
            }
            catch (NoSuchMethodException ignore) {
                // Have getter, but no setter - return read-only accessor.
                return new QueryReadOnlyMethodsAccessor(getter, prop);
            }

            return new QueryMethodsAccessor(getter, setter, prop);
        }
        catch (NoSuchMethodException ignore) {
            // No-op.
        }

        getBldr = new StringBuilder("is");
        getBldr.append(prop);
        getBldr.setCharAt(2, Character.toUpperCase(getBldr.charAt(2)));

        // We do nothing about setBldr here as it corresponds to setProperty name which is what we need
        // for boolean property setter as well
        try {
            Method getter = cls.getMethod(getBldr.toString());

            Method setter;

            try {
                // Setter has to have the same name like 'setXxx' and single param of the same type
                // as the return type of the getter.
                setter = cls.getMethod(setBldr.toString(), getter.getReturnType());
            }
            catch (NoSuchMethodException ignore) {
                // Have getter, but no setter - return read-only accessor.
                return new QueryReadOnlyMethodsAccessor(getter, prop);
            }

            return new QueryMethodsAccessor(getter, setter, prop);
        }
        catch (NoSuchMethodException ignore) {
            // No-op.
        }

        Class cls0 = cls;

        while (cls0 != null)
            try {
                return new QueryFieldAccessor(cls0.getDeclaredField(prop));
            }
            catch (NoSuchFieldException ignored) {
                cls0 = cls0.getSuperclass();
            }

        try {
            Method getter = cls.getMethod(prop);

            Method setter;

            try {
                // Setter has to have the same name and single param of the same type
                // as the return type of the getter.
                setter = cls.getMethod(prop, getter.getReturnType());
            }
            catch (NoSuchMethodException ignore) {
                // Have getter, but no setter - return read-only accessor.
                return new QueryReadOnlyMethodsAccessor(getter, prop);
            }

            return new QueryMethodsAccessor(getter, setter, prop);
        }
        catch (NoSuchMethodException ignored) {
            // No-op.
        }

        // No luck.
        return null;
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
     * The way to index.
     */
    private enum IndexType {
        /** Ascending index. */
        ASC,

        /** Descending index. */
        DESC,

        /** Text index. */
        TEXT
    }
}
