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

package org.apache.ignite.internal.processors.cache.binary;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryMetadataHandler;
import org.apache.ignite.internal.binary.BinaryObjectEx;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.BinaryObjectOffheapImpl;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.builder.BinaryObjectBuilderImpl;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOffheapInputStream;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicateAdapter;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.query.CacheQuery;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessorImpl;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridMapEntry;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T1;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;

/**
 * Binary processor implementation.
 */
public class CacheObjectBinaryProcessorImpl extends IgniteCacheObjectProcessorImpl implements
    CacheObjectBinaryProcessor {
    /** */
    public static final IgniteProductVersion BINARY_CFG_CHECK_SINCE = IgniteProductVersion.fromString("1.5.7");

    /** */
    private final CountDownLatch startLatch = new CountDownLatch(1);

    /** */
    private final boolean clientNode;

    /** */
    private volatile IgniteCacheProxy<BinaryMetadataKey, BinaryMetadata> metaDataCache;

    /** */
    private final ConcurrentHashMap8<Integer, BinaryTypeImpl> clientMetaDataCache;

    /** Predicate to filter binary meta data in utility cache. */
    private final CacheEntryPredicate metaPred = new CacheEntryPredicateAdapter() {
        private static final long serialVersionUID = 0L;

        @Override public boolean apply(GridCacheEntryEx e) {
            return e.key().value(e.context().cacheObjectContext(), false) instanceof BinaryMetadataKey;
        }
    };

    /** */
    private BinaryContext binaryCtx;

    /** */
    private Marshaller marsh;

    /** */
    private GridBinaryMarshaller binaryMarsh;

    /** */
    @GridToStringExclude
    private IgniteBinary binaries;

    /** Listener removes all registred binary schemas after the local client reconnected. */
    private final GridLocalEventListener clientDisconLsnr = new GridLocalEventListener() {
        @Override public void onEvent(Event evt) {
            binaryContext().unregisterBinarySchemas();
        }
    };

    /** Metadata updates collected before metadata cache is initialized. */
    private final Map<Integer, BinaryMetadata> metaBuf = new ConcurrentHashMap<>();

    /** Cached affinity key field names. */
    private final ConcurrentHashMap<Integer, T1<BinaryField>> affKeyFields = new ConcurrentHashMap<>();

    /**
     * @param ctx Kernal context.
     */
    public CacheObjectBinaryProcessorImpl(GridKernalContext ctx) {
        super(ctx);

        marsh = ctx.grid().configuration().getMarshaller();

        clientNode = this.ctx.clientNode();

        clientMetaDataCache = clientNode ? new ConcurrentHashMap8<Integer, BinaryTypeImpl>() : null;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (marsh instanceof BinaryMarshaller) {
            if (ctx.clientNode())
                ctx.event().addLocalEventListener(clientDisconLsnr, EVT_CLIENT_NODE_DISCONNECTED);

            BinaryMetadataHandler metaHnd = new BinaryMetadataHandler() {
                @Override public void addMeta(int typeId, BinaryType newMeta) throws BinaryObjectException {
                    assert newMeta != null;
                    assert newMeta instanceof BinaryTypeImpl;

                    BinaryMetadata newMeta0 = ((BinaryTypeImpl)newMeta).metadata();

                    if (metaDataCache == null) {
                        BinaryMetadata oldMeta = metaBuf.get(typeId);
                        BinaryMetadata mergedMeta = BinaryUtils.mergeMetadata(oldMeta, newMeta0);

                        if (oldMeta != mergedMeta) {
                            synchronized (this) {
                                mergedMeta = BinaryUtils.mergeMetadata(oldMeta, newMeta0);

                                if (oldMeta != mergedMeta)
                                    metaBuf.put(typeId, mergedMeta);
                                else
                                    return;
                            }

                            if (metaDataCache == null)
                                return;
                            else
                                metaBuf.remove(typeId);
                        }
                        else
                            return;
                    }

                    assert metaDataCache != null;

                    CacheObjectBinaryProcessorImpl.this.addMeta(typeId, newMeta0.wrap(binaryCtx));
                }

                @Override public BinaryType metadata(int typeId) throws BinaryObjectException {
                    if (metaDataCache == null)
                        U.awaitQuiet(startLatch);

                    return CacheObjectBinaryProcessorImpl.this.metadata(typeId);
                }
            };

            BinaryMarshaller bMarsh0 = (BinaryMarshaller)marsh;

            binaryCtx = new BinaryContext(metaHnd, ctx.config(), ctx.log(BinaryContext.class));

            IgniteUtils.invoke(BinaryMarshaller.class, bMarsh0, "setBinaryContext", binaryCtx, ctx.config());

            binaryMarsh = new GridBinaryMarshaller(binaryCtx);

            binaries = new IgniteBinaryImpl(ctx, this);

            if (!getBoolean(IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK)) {
                BinaryConfiguration bCfg = ctx.config().getBinaryConfiguration();

                if (bCfg != null) {
                    Map<String, Object> map = new HashMap<>();

                    map.put("globIdMapper", bCfg.getIdMapper() != null ? bCfg.getIdMapper().getClass().getName() : null);
                    map.put("globSerializer", bCfg.getSerializer() != null ? bCfg.getSerializer().getClass() : null);
                    map.put("compactFooter", bCfg.isCompactFooter());

                    if (bCfg.getTypeConfigurations() != null) {
                        Map<Object, Object> typeCfgsMap = new HashMap<>();

                        for (BinaryTypeConfiguration c : bCfg.getTypeConfigurations()) {
                            typeCfgsMap.put(
                                c.getTypeName() != null,
                                Arrays.asList(
                                    c.getIdMapper() != null ? c.getIdMapper().getClass() : null,
                                    c.getSerializer() != null ? c.getSerializer().getClass() : null,
                                    c.isEnum()
                                )
                            );
                        }

                        map.put("typeCfgs", typeCfgsMap);
                    }

                    ctx.addNodeAttribute(IgniteNodeAttributes.ATTR_BINARY_CONFIGURATION, map);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        if (ctx.clientNode())
            ctx.event().removeLocalEventListener(clientDisconLsnr);
    }

    /** {@inheritDoc} */
    @Override public void onContinuousProcessorStarted(GridKernalContext ctx) throws IgniteCheckedException {
        if (clientNode && !ctx.isDaemon()) {
            ctx.continuous().registerStaticRoutine(
                CU.UTILITY_CACHE_NAME,
                new MetaDataEntryListener(),
                new MetaDataEntryFilter(),
                null);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onUtilityCacheStarted() throws IgniteCheckedException {
        IgniteCacheProxy<Object, Object> proxy = ctx.cache().jcache(CU.UTILITY_CACHE_NAME);

        boolean old = proxy.context().deploy().ignoreOwnership(true);

        try {
            metaDataCache = (IgniteCacheProxy)proxy.withNoRetries();
        }
        finally {
            proxy.context().deploy().ignoreOwnership(old);
        }

        if (clientNode) {
            assert !metaDataCache.context().affinityNode();

            while (true) {
                ClusterNode oldestSrvNode = ctx.discovery().oldestAliveCacheServerNode(AffinityTopologyVersion.NONE);

                if (oldestSrvNode == null)
                    break;

                GridCacheQueryManager qryMgr = metaDataCache.context().queries();

                CacheQuery<Map.Entry<BinaryMetadataKey, BinaryMetadata>> qry =
                    qryMgr.createScanQuery(new MetaDataPredicate(), null, false);

                qry.keepAll(false);

                qry.projection(ctx.cluster().get().forNode(oldestSrvNode));

                try (GridCloseableIterator<Map.Entry<BinaryMetadataKey, BinaryMetadata>> entries = qry.executeScanQuery()) {
                    for (Map.Entry<BinaryMetadataKey, BinaryMetadata> e : entries) {
                        assert e.getKey() != null : e;
                        assert e.getValue() != null : e;

                        addClientCacheMetaData(e.getKey(), e.getValue());
                    }
                }
                catch (IgniteCheckedException e) {
                    if (!ctx.discovery().alive(oldestSrvNode) || !ctx.discovery().pingNode(oldestSrvNode.id()))
                        continue;
                    else
                        throw e;
                }
                catch (CacheException e) {
                    if (X.hasCause(e, ClusterTopologyCheckedException.class, ClusterTopologyException.class))
                        continue;
                    else
                        throw e;
                }

                break;
            }
        }

        for (Map.Entry<Integer, BinaryMetadata> e : metaBuf.entrySet())
            addMeta(e.getKey(), e.getValue().wrap(binaryCtx));

        metaBuf.clear();

        startLatch.countDown();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        super.onKernalStart();

        if (!getBoolean(IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK) && marsh instanceof BinaryMarshaller) {
            BinaryConfiguration bcfg = ctx.config().getBinaryConfiguration();

            for (ClusterNode rmtNode : ctx.discovery().remoteNodes()) {
                if (rmtNode.version().compareTo(BINARY_CFG_CHECK_SINCE) < 0) {
                    if (bcfg == null || bcfg.getNameMapper() == null) {
                        throw new IgniteCheckedException("When BinaryMarshaller is used and topology contains old " +
                            "nodes, then " + BinaryBasicNameMapper.class.getName() + " mapper have to be set " +
                            "explicitely into binary configuration and 'simpleName' property of the mapper " +
                            "have to be set to 'true'.");
                    }

                    if (!(bcfg.getNameMapper() instanceof BinaryBasicNameMapper)
                        || !((BinaryBasicNameMapper)bcfg.getNameMapper()).isSimpleName()) {
                        U.quietAndWarn(log, "When BinaryMarshaller is used and topology contains old" +
                            " nodes, it's strongly recommended, to set " + BinaryBasicNameMapper.class.getName() +
                            " mapper into binary configuration explicitely " +
                            " and 'simpleName' property of the mapper set to 'true' (fix configuration or set " +
                            "-D" + IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK + "=true system property).");
                    }

                    break;
                }
            }
        }
    }

    /**
     * @param key Metadata key.
     * @param newMeta Metadata.
     */
    private void addClientCacheMetaData(BinaryMetadataKey key, final BinaryMetadata newMeta) {
        int key0 = key.typeId();

        clientMetaDataCache.compute(key0, new ConcurrentHashMap8.BiFun<Integer, BinaryTypeImpl, BinaryTypeImpl>() {
            @Override public BinaryTypeImpl apply(Integer key, BinaryTypeImpl oldMeta) {
                BinaryMetadata res;

                BinaryMetadata oldMeta0 = oldMeta != null ? oldMeta.metadata() : null;

                try {
                    res = BinaryUtils.mergeMetadata(oldMeta0, newMeta);
                }
                catch (BinaryObjectException ignored) {
                    res = oldMeta0;
                }

                return res != null ? res.wrap(binaryCtx) : null;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public int typeId(String typeName) {
        if (binaryCtx == null)
            return super.typeId(typeName);

        return binaryCtx.typeId(typeName);
    }

    /**
     * @param obj Object.
     * @return Bytes.
     * @throws org.apache.ignite.binary.BinaryObjectException If failed.
     */
    public byte[] marshal(@Nullable Object obj) throws BinaryObjectException {
        byte[] arr = binaryMarsh.marshal(obj);

        assert arr.length > 0;

        return arr;
    }

    /**
     * @param ptr Off-heap pointer.
     * @param forceHeap If {@code true} creates heap-based object.
     * @return Object.
     * @throws org.apache.ignite.binary.BinaryObjectException If failed.
     */
    public Object unmarshal(long ptr, boolean forceHeap) throws BinaryObjectException {
        assert ptr > 0 : ptr;

        int size = GridUnsafe.getInt(ptr);

        ptr += 4;

        byte type = GridUnsafe.getByte(ptr++);

        if (type != CacheObject.TYPE_BYTE_ARR) {
            assert size > 0 : size;

            BinaryInputStream in = new BinaryOffheapInputStream(ptr, size, forceHeap);

            return binaryMarsh.unmarshal(in);
        }
        else
            return U.copyMemory(ptr, size);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Object marshalToBinary(@Nullable Object obj) throws BinaryObjectException {
        if (obj == null)
            return null;

        if (BinaryUtils.isBinaryType(obj.getClass()))
            return obj;

        if (obj instanceof Object[]) {
            Object[] arr = (Object[])obj;

            Object[] pArr = new Object[arr.length];

            for (int i = 0; i < arr.length; i++)
                pArr[i] = marshalToBinary(arr[i]);

            return pArr;
        }

        if (obj instanceof IgniteBiTuple) {
            IgniteBiTuple tup = (IgniteBiTuple)obj;

            if (obj instanceof T2)
                return new T2<>(marshalToBinary(tup.get1()), marshalToBinary(tup.get2()));

            return new IgniteBiTuple<>(marshalToBinary(tup.get1()), marshalToBinary(tup.get2()));
        }

        {
            Collection<Object> pCol = BinaryUtils.newKnownCollection(obj);

            if (pCol != null) {
                Collection<?> col = (Collection<?>)obj;

                for (Object item : col)
                    pCol.add(marshalToBinary(item));

                return pCol;
            }
        }

        {
            Map<Object, Object> pMap = BinaryUtils.newKnownMap(obj);

            if (pMap != null) {
                Map<?, ?> map = (Map<?, ?>)obj;

                for (Map.Entry<?, ?> e : map.entrySet())
                    pMap.put(marshalToBinary(e.getKey()), marshalToBinary(e.getValue()));

                return pMap;
            }
        }

        if (obj instanceof Map.Entry) {
            Map.Entry<?, ?> e = (Map.Entry<?, ?>)obj;

            return new GridMapEntry<>(marshalToBinary(e.getKey()), marshalToBinary(e.getValue()));
        }

        if (binaryMarsh.mustDeserialize(obj))
            return obj; // No need to go through marshal-unmarshal because result will be the same as initial object.

        byte[] arr = binaryMarsh.marshal(obj);

        assert arr.length > 0;

        Object obj0 = binaryMarsh.unmarshal(arr, null);

        // Possible if a class has writeObject method.
        if (obj0 instanceof BinaryObjectImpl)
            ((BinaryObjectImpl)obj0).detachAllowed(true);

        return obj0;
    }

    /**
     * @return Marshaller.
     */
    public GridBinaryMarshaller marshaller() {
        return binaryMarsh;
    }

    /** {@inheritDoc} */
    @Override public String affinityField(String keyType) {
        if (binaryCtx == null)
            return null;

        return binaryCtx.affinityKeyFieldName(typeId(keyType));
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder builder(String clsName) {
        return new BinaryObjectBuilderImpl(binaryCtx, clsName);
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder builder(BinaryObject binaryObj) {
        return BinaryObjectBuilderImpl.wrap(binaryObj);
    }

    /** {@inheritDoc} */
    @Override public void updateMetadata(int typeId, String typeName, @Nullable String affKeyFieldName,
        Map<String, Integer> fieldTypeIds, boolean isEnum) throws BinaryObjectException {
        BinaryMetadata meta = new BinaryMetadata(typeId, typeName, fieldTypeIds, affKeyFieldName, null, isEnum);

        binaryCtx.updateMetadata(typeId, meta);
    }

    /** {@inheritDoc} */
    @Override public void addMeta(final int typeId, final BinaryType newMeta) throws BinaryObjectException {
        assert newMeta != null;
        assert newMeta instanceof BinaryTypeImpl;

        BinaryMetadata newMeta0 = ((BinaryTypeImpl)newMeta).metadata();

        final BinaryMetadataKey key = new BinaryMetadataKey(typeId);

        try {
            BinaryMetadata oldMeta = metaDataCache.localPeek(key);
            BinaryMetadata mergedMeta = BinaryUtils.mergeMetadata(oldMeta, newMeta0);

            AffinityTopologyVersion topVer = ctx.cache().context().lockedTopologyVersion(null);

            if (topVer == null)
                topVer = ctx.cache().context().exchange().readyAffinityVersion();

            BinaryObjectException err = metaDataCache.invoke(topVer, key, new MetadataProcessor(mergedMeta));

            if (err != null)
                throw err;
        }
        catch (CacheException e) {
            throw new BinaryObjectException("Failed to update meta data for type: " + newMeta.typeName(), e);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public BinaryType metadata(final int typeId) throws BinaryObjectException {
        try {
            if (clientNode) {
                BinaryType typeMeta = clientMetaDataCache.get(typeId);

                if (typeMeta != null)
                    return typeMeta;

                BinaryMetadata meta = metaDataCache.getTopologySafe(new BinaryMetadataKey(typeId));

                return meta != null ? meta.wrap(binaryCtx) : null;
            }
            else {
                BinaryMetadataKey key = new BinaryMetadataKey(typeId);

                BinaryMetadata meta = metaDataCache.localPeek(key);

                if (meta == null && !metaDataCache.context().preloader().syncFuture().isDone())
                    meta = metaDataCache.getTopologySafe(key);

                return meta != null ? meta.wrap(binaryCtx) : null;
            }
        }
        catch (CacheException e) {
            throw new BinaryObjectException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, BinaryType> metadata(Collection<Integer> typeIds)
        throws BinaryObjectException {
        try {
            Collection<BinaryMetadataKey> keys = new ArrayList<>(typeIds.size());

            for (Integer typeId : typeIds)
                keys.add(new BinaryMetadataKey(typeId));

            Map<BinaryMetadataKey, BinaryMetadata> meta = metaDataCache.getAll(keys);

            Map<Integer, BinaryType> res = U.newHashMap(meta.size());

            for (Map.Entry<BinaryMetadataKey, BinaryMetadata> e : meta.entrySet())
                res.put(e.getKey().typeId(), e.getValue().wrap(binaryCtx));

            return res;
        }
        catch (CacheException e) {
            throw new BinaryObjectException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Collection<BinaryType> metadata() throws BinaryObjectException {
        if (clientNode)
            return F.viewReadOnly(clientMetaDataCache.values(), new IgniteClosure<BinaryTypeImpl, BinaryType>() {
                @Override public BinaryType apply(BinaryTypeImpl meta) {
                    return meta;
                }
            });
        else {
            return F.viewReadOnly(metaDataCache.entrySetx(metaPred),
                new C1<Cache.Entry<BinaryMetadataKey, BinaryMetadata>, BinaryType>() {
                    private static final long serialVersionUID = 0L;

                    @Override public BinaryType apply(Cache.Entry<BinaryMetadataKey, BinaryMetadata> e) {
                        return e.getValue().wrap(binaryCtx);
                    }
                });
        }
    }

    /** {@inheritDoc} */
    @Override public BinaryObject buildEnum(String typeName, int ord) throws IgniteException {
        int typeId = binaryCtx.typeId(typeName);

        typeName = binaryCtx.userTypeName(typeName);

        updateMetadata(typeId, typeName, null, null, true);

        return new BinaryEnumObjectImpl(binaryCtx, typeId, null, ord);
    }

    /** {@inheritDoc} */
    @Override public IgniteBinary binary() throws IgniteException {
        return binaries;
    }

    /** {@inheritDoc} */
    @Override public boolean isBinaryObject(Object obj) {
        return obj instanceof BinaryObject;
    }

    /** {@inheritDoc} */
    @Override public boolean isBinaryEnabled(CacheConfiguration<?, ?> ccfg) {
        return marsh instanceof BinaryMarshaller;
    }

    /**
     * @param po Binary object.
     * @return Affinity key.
     */
    public Object affinityKey(BinaryObject po) {
        // Fast path for already cached field.
        if (po instanceof BinaryObjectEx) {
            int typeId = ((BinaryObjectEx)po).typeId();

            T1<BinaryField> fieldHolder = affKeyFields.get(typeId);

            if (fieldHolder != null) {
                BinaryField field = fieldHolder.get();

                return field != null ? field.value(po) : po;
            }
        }

        // Slow path if affinity field is not cached yet.
        try {
            BinaryType meta = po instanceof BinaryObjectEx ? ((BinaryObjectEx)po).rawType() : po.type();

            if (meta != null) {
                String name = meta.affinityKeyFieldName();

                if (name != null) {
                    BinaryField field = meta.field(name);

                    affKeyFields.putIfAbsent(meta.typeId(), new T1<>(field));

                    return field.value(po);
                }
                else
                    affKeyFields.putIfAbsent(meta.typeId(), new T1<BinaryField>(null));
            }
            else if (po instanceof BinaryObjectEx) {
                int typeId = ((BinaryObjectEx)po).typeId();

                String name = binaryCtx.affinityKeyFieldName(typeId);

                if (name != null)
                    return po.field(name);
            }
        }
        catch (BinaryObjectException e) {
            U.error(log, "Failed to get affinity field from binary object: " + po, e);
        }

        return po;
    }

    /** {@inheritDoc} */
    @Override public int typeId(Object obj) {
        if (obj == null)
            return 0;

        return isBinaryObject(obj) ? ((BinaryObjectEx)obj).typeId() : typeId(obj.getClass().getSimpleName());
    }

    /** {@inheritDoc} */
    @Override public Object field(Object obj, String fieldName) {
        if (obj == null)
            return null;

        return isBinaryObject(obj) ? ((BinaryObject)obj).field(fieldName) : super.field(obj, fieldName);
    }

    /** {@inheritDoc} */
    @Override public boolean hasField(Object obj, String fieldName) {
        return obj != null && ((BinaryObject)obj).hasField(fieldName);
    }

    /**
     * @return Binary context.
     */
    public BinaryContext binaryContext() {
        return binaryCtx;
    }

    /** {@inheritDoc} */
    @Override public CacheObjectContext contextForCache(CacheConfiguration cfg) throws IgniteCheckedException {
        assert cfg != null;

        boolean binaryEnabled = marsh instanceof BinaryMarshaller && !GridCacheUtils.isSystemCache(cfg.getName()) &&
            !GridCacheUtils.isIgfsCache(ctx.config(), cfg.getName());

        CacheObjectContext ctx0 = super.contextForCache(cfg);

        CacheObjectContext res = new CacheObjectBinaryContext(ctx,
            cfg.getName(),
            ctx0.copyOnGet(),
            ctx0.storeValue(),
            binaryEnabled,
            ctx0.addDeploymentInfo());

        ctx.resource().injectGeneric(res.defaultAffMapper());

        return res;
    }

    /** {@inheritDoc} */
    @Override public byte[] marshal(CacheObjectContext ctx, Object val) throws IgniteCheckedException {
        if (!((CacheObjectBinaryContext)ctx).binaryEnabled() || binaryMarsh == null)
            return super.marshal(ctx, val);

        byte[] arr = binaryMarsh.marshal(val);

        assert arr.length > 0;

        return arr;
    }

    /** {@inheritDoc} */
    @Override public Object unmarshal(CacheObjectContext ctx, byte[] bytes, ClassLoader clsLdr)
        throws IgniteCheckedException {
        if (!((CacheObjectBinaryContext)ctx).binaryEnabled() || binaryMarsh == null)
            return super.unmarshal(ctx, bytes, clsLdr);

        return binaryMarsh.unmarshal(bytes, clsLdr);
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject toCacheKeyObject(
        CacheObjectContext ctx,
        @Nullable GridCacheContext cctx,
        Object obj,
        boolean userObj
    ) {
        if (!((CacheObjectBinaryContext)ctx).binaryEnabled())
            return super.toCacheKeyObject(ctx, cctx, obj, userObj);

        if (obj instanceof KeyCacheObject) {
            KeyCacheObject key = (KeyCacheObject)obj;

            if (key instanceof BinaryObjectImpl) {
                // Need to create a copy because the key can be reused at the application layer after that (IGNITE-3505).
                key = key.copy(partition(ctx, cctx, key));
            }
            else if (key.partition() == -1)
                // Assume others KeyCacheObjects can not be reused for another cache.
                key.partition(partition(ctx, cctx, key));

            return key;
        }

        obj = toBinary(obj);

        if (obj instanceof BinaryObjectImpl) {
            ((BinaryObjectImpl)obj).partition(partition(ctx, cctx, obj));

            return (KeyCacheObject)obj;
        }

        return toCacheKeyObject0(ctx, cctx, obj, userObj);
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject toCacheObject(CacheObjectContext ctx, @Nullable Object obj,
        boolean userObj) {
        if (!((CacheObjectBinaryContext)ctx).binaryEnabled())
            return super.toCacheObject(ctx, obj, userObj);

        if (obj == null || obj instanceof CacheObject)
            return (CacheObject)obj;

        obj = toBinary(obj);

        if (obj instanceof CacheObject)
            return (CacheObject)obj;

        return toCacheObject0(obj, userObj);
    }

    /** {@inheritDoc} */
    @Override public CacheObject toCacheObject(CacheObjectContext ctx, byte type, byte[] bytes) {
        if (type == BinaryObjectImpl.TYPE_BINARY)
            return new BinaryObjectImpl(binaryContext(), bytes, 0);
        else if (type == BinaryObjectImpl.TYPE_BINARY_ENUM)
            return new BinaryEnumObjectImpl(binaryContext(), bytes);

        return super.toCacheObject(ctx, type, bytes);
    }

    /** {@inheritDoc} */
    @Override public CacheObject toCacheObject(GridCacheContext ctx, long valPtr, boolean tmp)
        throws IgniteCheckedException {
        if (!((CacheObjectBinaryContext)ctx.cacheObjectContext()).binaryEnabled())
            return super.toCacheObject(ctx, valPtr, tmp);

        Object val = unmarshal(valPtr, !tmp);

        if (val instanceof CacheObject)
            return (CacheObject)val;

        return toCacheObject(ctx.cacheObjectContext(), val, false);
    }

    /** {@inheritDoc} */
    @Override public Object unwrapTemporary(GridCacheContext ctx, Object obj) throws BinaryObjectException {
        if (!((CacheObjectBinaryContext)ctx.cacheObjectContext()).binaryEnabled())
            return obj;

        if (obj instanceof BinaryObjectOffheapImpl)
            return ((BinaryObjectOffheapImpl)obj).heapCopy();

        return obj;
    }

    /**
     * @param obj Object.
     * @return Binary object.
     * @throws IgniteException In case of error.
     */
    @Nullable public Object toBinary(@Nullable Object obj) throws IgniteException {
        if (obj == null)
            return null;

        if (isBinaryObject(obj))
            return obj;

        return marshalToBinary(obj);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode rmtNode) {
        IgniteNodeValidationResult res = super.validateNode(rmtNode);

        if (res != null)
            return res;

        if (getBoolean(IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK) || !(marsh instanceof BinaryMarshaller))
            return null;

        Object rmtBinaryCfg = rmtNode.attribute(IgniteNodeAttributes.ATTR_BINARY_CONFIGURATION);

        if (rmtNode.version().compareTo(BINARY_CFG_CHECK_SINCE) < 0)
            return null;

        ClusterNode locNode = ctx.discovery().localNode();

        Object locBinaryCfg = locNode.attribute(IgniteNodeAttributes.ATTR_BINARY_CONFIGURATION);

        if (!F.eq(locBinaryCfg, rmtBinaryCfg)) {
            String msg = "Local node's binary configuration is not equal to remote node's binary configuration " +
                "[locNodeId=%s, rmtNodeId=%s, locBinaryCfg=%s, rmtBinaryCfg=%s]";

            return new IgniteNodeValidationResult(rmtNode.id(),
                String.format(msg, locNode.id(), rmtNode.id(), locBinaryCfg, rmtBinaryCfg),
                String.format(msg, rmtNode.id(), locNode.id(), rmtBinaryCfg, locBinaryCfg));
        }

        return null;
    }

    /**
     * Processor responsible for metadata update.
     */
    private static class MetadataProcessor
        implements EntryProcessor<BinaryMetadataKey, BinaryMetadata, BinaryObjectException>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private BinaryMetadata newMeta;

        /**
         * For {@link Externalizable}.
         */
        public MetadataProcessor() {
            // No-op.
        }

        /**
         * @param newMeta New metadata.
         */
        private MetadataProcessor(BinaryMetadata newMeta) {
            assert newMeta != null;

            this.newMeta = newMeta;
        }

        /** {@inheritDoc} */
        @Override public BinaryObjectException process(MutableEntry<BinaryMetadataKey, BinaryMetadata> entry,
            Object... args) {
            try {
                BinaryMetadata oldMeta = entry.getValue();

                BinaryMetadata mergedMeta = BinaryUtils.mergeMetadata(oldMeta, newMeta);

                if (mergedMeta != oldMeta)
                    entry.setValue(mergedMeta);

                return null;
            }
            catch (BinaryObjectException e) {
                return e;
            }
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(newMeta);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            newMeta = (BinaryMetadata)in.readObject();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MetadataProcessor.class, this);
        }
    }

    /**
     *
     */
    class MetaDataEntryListener implements CacheEntryUpdatedListener<BinaryMetadataKey, BinaryMetadata> {
        /** {@inheritDoc} */
        @Override public void onUpdated(
            Iterable<CacheEntryEvent<? extends BinaryMetadataKey, ? extends BinaryMetadata>> evts)
            throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends BinaryMetadataKey, ? extends BinaryMetadata> evt : evts) {
                assert evt.getEventType() == EventType.CREATED || evt.getEventType() == EventType.UPDATED : evt;

                BinaryMetadataKey key = evt.getKey();

                final BinaryMetadata newMeta = evt.getValue();

                assert newMeta != null : evt;

                addClientCacheMetaData(key, newMeta);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MetaDataEntryListener.class, this);
        }
    }

    /**
     *
     */
    static class MetaDataEntryFilter implements CacheEntryEventSerializableFilter<Object, Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<?, ?> evt) throws CacheEntryListenerException {
            return evt.getKey() instanceof BinaryMetadataKey;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MetaDataEntryFilter.class, this);
        }
    }

    /**
     *
     */
    static class MetaDataPredicate implements IgniteBiPredicate<Object, Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean apply(Object key, Object val) {
            return key instanceof BinaryMetadataKey;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MetaDataPredicate.class, this);
        }
    }
}
