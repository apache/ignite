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

package org.apache.ignite.internal.processors.cache.portable;

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.portable.BinaryMetadata;
import org.apache.ignite.internal.portable.BinaryObjectImpl;
import org.apache.ignite.internal.portable.BinaryObjectOffheapImpl;
import org.apache.ignite.internal.portable.BinaryTypeImpl;
import org.apache.ignite.internal.portable.GridPortableMarshaller;
import org.apache.ignite.internal.portable.PortableContext;
import org.apache.ignite.internal.portable.BinaryMetadataHandler;
import org.apache.ignite.internal.portable.PortableSchema;
import org.apache.ignite.internal.portable.PortableUtils;
import org.apache.ignite.internal.portable.builder.BinaryObjectBuilderImpl;
import org.apache.ignite.internal.portable.streams.PortableInputStream;
import org.apache.ignite.internal.portable.streams.PortableOffheapInputStream;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicateAdapter;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.query.CacheQuery;
import org.apache.ignite.internal.processors.cache.query.CacheQueryFuture;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessorImpl;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.lang.GridMapEntry;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;
import sun.misc.Unsafe;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * Portable processor implementation.
 */
public class CacheObjectBinaryProcessorImpl extends IgniteCacheObjectProcessorImpl implements
    CacheObjectBinaryProcessor {
    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private final CountDownLatch startLatch = new CountDownLatch(1);

    /** */
    private final boolean clientNode;

    /** */
    private volatile IgniteCacheProxy<PortableMetaDataKey, BinaryMetadata> metaDataCache;

    /** */
    private final ConcurrentHashMap8<Integer, BinaryTypeImpl> clientMetaDataCache;

    /** Predicate to filter portable meta data in utility cache. */
    private final CacheEntryPredicate metaPred = new CacheEntryPredicateAdapter() {
        private static final long serialVersionUID = 0L;

        @Override public boolean apply(GridCacheEntryEx e) {
            return e.key().value(e.context().cacheObjectContext(), false) instanceof PortableMetaDataKey;
        }
    };

    /** */
    private PortableContext portableCtx;

    /** */
    private Marshaller marsh;

    /** */
    private GridPortableMarshaller portableMarsh;

    /** */
    @GridToStringExclude
    private IgniteBinary portables;

    /** Metadata updates collected before metadata cache is initialized. */
    private final Map<Integer, BinaryMetadata> metaBuf = new ConcurrentHashMap<>();

    /** */
    private UUID metaCacheQryId;

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
        if (marsh instanceof PortableMarshaller) {
            BinaryMetadataHandler metaHnd = new BinaryMetadataHandler() {
                @Override public void addMeta(int typeId, BinaryType newMeta) throws BinaryObjectException {
                    assert newMeta != null;
                    assert newMeta instanceof BinaryTypeImpl;

                    BinaryMetadata newMeta0 = ((BinaryTypeImpl)newMeta).metadata();

                    if (metaDataCache == null) {
                        BinaryMetadata oldMeta = metaBuf.get(typeId);
                        BinaryMetadata mergedMeta = mergeMetadata(oldMeta, newMeta0);

                        if (oldMeta != mergedMeta) {
                            synchronized (this) {
                                mergedMeta = mergeMetadata(oldMeta, newMeta0);

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
                    }

                    CacheObjectBinaryProcessorImpl.this.addMeta(typeId, newMeta0.wrap(portableCtx));
                }

                @Override public BinaryType metadata(int typeId) throws BinaryObjectException {
                    if (metaDataCache == null)
                        U.awaitQuiet(startLatch);

                    return CacheObjectBinaryProcessorImpl.this.metadata(typeId);
                }
            };

            PortableMarshaller pMarh0 = (PortableMarshaller)marsh;

            portableCtx = new PortableContext(metaHnd, ctx.config());

            IgniteUtils.invoke(PortableMarshaller.class, pMarh0, "setPortableContext", portableCtx);

            portableMarsh = new GridPortableMarshaller(portableCtx);

            portables = new IgniteBinaryImpl(ctx, this);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onUtilityCacheStarted() throws IgniteCheckedException {
        metaDataCache = ctx.cache().jcache(CU.UTILITY_CACHE_NAME);

        if (clientNode) {
            assert !metaDataCache.context().affinityNode();

            metaCacheQryId = metaDataCache.context().continuousQueries().executeInternalQuery(
                new MetaDataEntryListener(),
                new MetaDataEntryFilter(),
                false,
                true);

            while (true) {
                ClusterNode oldestSrvNode =
                    CU.oldestAliveCacheServerNode(ctx.cache().context(), AffinityTopologyVersion.NONE);

                if (oldestSrvNode == null)
                    break;

                GridCacheQueryManager qryMgr = metaDataCache.context().queries();

                CacheQuery<Map.Entry<PortableMetaDataKey, BinaryMetadata>> qry =
                    qryMgr.createScanQuery(new MetaDataPredicate(), null, false);

                qry.keepAll(false);

                qry.projection(ctx.cluster().get().forNode(oldestSrvNode));

                try {
                    CacheQueryFuture<Map.Entry<PortableMetaDataKey, BinaryMetadata>> fut = qry.execute();

                    Map.Entry<PortableMetaDataKey, BinaryMetadata> next;

                    while ((next = fut.next()) != null) {
                        assert next.getKey() != null : next;
                        assert next.getValue() != null : next;

                        addClientCacheMetaData(next.getKey(), next.getValue());
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

        startLatch.countDown();

        for (Map.Entry<Integer, BinaryMetadata> e : metaBuf.entrySet())
            addMeta(e.getKey(), e.getValue().wrap(portableCtx));

        metaBuf.clear();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        super.onKernalStop(cancel);

        if (metaCacheQryId != null)
            metaDataCache.context().continuousQueries().cancelInternalQuery(metaCacheQryId);
    }

    /**
     * @param key Metadata key.
     * @param newMeta Metadata.
     */
    private void addClientCacheMetaData(PortableMetaDataKey key, final BinaryMetadata newMeta) {
        int key0 = key.typeId();

        clientMetaDataCache.compute(key0, new ConcurrentHashMap8.BiFun<Integer, BinaryTypeImpl, BinaryTypeImpl>() {
            @Override public BinaryTypeImpl apply(Integer key, BinaryTypeImpl oldMeta) {
                BinaryMetadata res;

                BinaryMetadata oldMeta0 = oldMeta != null ? oldMeta.metadata() : null;

                try {
                    res = mergeMetadata(oldMeta0, newMeta);
                }
                catch (BinaryObjectException e) {
                    res = oldMeta0;
                }

                return res != null ? res.wrap(portableCtx) : null;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public int typeId(String typeName) {
        if (portableCtx == null)
            return super.typeId(typeName);

        return portableCtx.typeId(typeName);
    }

    /**
     * @param obj Object.
     * @return Bytes.
     * @throws org.apache.ignite.binary.BinaryObjectException If failed.
     */
    public byte[] marshal(@Nullable Object obj) throws BinaryObjectException {
        byte[] arr = portableMarsh.marshal(obj);

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

        int size = UNSAFE.getInt(ptr);

        ptr += 4;

        byte type = UNSAFE.getByte(ptr++);

        if (type != CacheObject.TYPE_BYTE_ARR) {
            assert size > 0 : size;

            PortableInputStream in = new PortableOffheapInputStream(ptr, size, forceHeap);

            return portableMarsh.unmarshal(in);
        }
        else
            return U.copyMemory(ptr, size);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Object marshalToPortable(@Nullable Object obj) throws BinaryObjectException {
        if (obj == null)
            return null;

        if (PortableUtils.isPortableType(obj.getClass()))
            return obj;

        if (obj instanceof Object[]) {
            Object[] arr = (Object[])obj;

            Object[] pArr = new Object[arr.length];

            for (int i = 0; i < arr.length; i++)
                pArr[i] = marshalToPortable(arr[i]);

            return pArr;
        }

        if (obj instanceof Collection) {
            Collection<Object> col = (Collection<Object>)obj;

            Collection<Object> pCol;

            if (col instanceof Set)
                pCol = (Collection<Object>)PortableUtils.newSet((Set<?>)col);
            else
                pCol = new ArrayList<>(col.size());

            for (Object item : col)
                pCol.add(marshalToPortable(item));

            return pCol;
        }

        if (obj instanceof Map) {
            Map<?, ?> map = (Map<?, ?>)obj;

            Map<Object, Object> pMap = PortableUtils.newMap((Map<Object, Object>)obj);

            for (Map.Entry<?, ?> e : map.entrySet())
                pMap.put(marshalToPortable(e.getKey()), marshalToPortable(e.getValue()));

            return pMap;
        }

        if (obj instanceof Map.Entry) {
            Map.Entry<?, ?> e = (Map.Entry<?, ?>)obj;

            return new GridMapEntry<>(marshalToPortable(e.getKey()), marshalToPortable(e.getValue()));
        }

        byte[] arr = portableMarsh.marshal(obj);

        assert arr.length > 0;

        Object obj0 = portableMarsh.unmarshal(arr, null);

        assert obj0 instanceof BinaryObject;

        ((BinaryObjectImpl)obj0).detachAllowed(true);

        return obj0;
    }

    /**
     * @return Marshaller.
     */
    public GridPortableMarshaller marshaller() {
        return portableMarsh;
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder builder(String clsName) {
        return new BinaryObjectBuilderImpl(portableCtx, clsName);
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder builder(BinaryObject portableObj) {
        return BinaryObjectBuilderImpl.wrap(portableObj);
    }

    /** {@inheritDoc} */
    @Override public void updateMetaData(int typeId, String typeName, @Nullable String affKeyFieldName,
        Map<String, Integer> fieldTypeIds) throws BinaryObjectException {
        portableCtx.updateMetadata(typeId, new BinaryMetadata(typeId, typeName, fieldTypeIds, affKeyFieldName, null));
    }

    /** {@inheritDoc} */
    @Override public void addMeta(final int typeId, final BinaryType newMeta) throws BinaryObjectException {
        assert newMeta != null;
        assert newMeta instanceof BinaryTypeImpl;

        BinaryMetadata newMeta0 = ((BinaryTypeImpl)newMeta).metadata();

        final PortableMetaDataKey key = new PortableMetaDataKey(typeId);

        try {
            BinaryMetadata oldMeta = metaDataCache.localPeek(key);
            BinaryMetadata mergedMeta = mergeMetadata(oldMeta, newMeta0);

            BinaryObjectException err = metaDataCache.invoke(key, new MetadataProcessor(mergedMeta));

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
            if (clientNode)
                return clientMetaDataCache.get(typeId);
            else {
                BinaryMetadata meta = metaDataCache.localPeek(new PortableMetaDataKey(typeId));

                return meta != null ? meta.wrap(portableCtx) : null;
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
            Collection<PortableMetaDataKey> keys = new ArrayList<>(typeIds.size());

            for (Integer typeId : typeIds)
                keys.add(new PortableMetaDataKey(typeId));

            Map<PortableMetaDataKey, BinaryMetadata> meta = metaDataCache.getAll(keys);

            Map<Integer, BinaryType> res = U.newHashMap(meta.size());

            for (Map.Entry<PortableMetaDataKey, BinaryMetadata> e : meta.entrySet())
                res.put(e.getKey().typeId(), e.getValue().wrap(portableCtx));

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
                new C1<Cache.Entry<PortableMetaDataKey, BinaryMetadata>, BinaryType>() {
                    private static final long serialVersionUID = 0L;

                    @Override public BinaryType apply(Cache.Entry<PortableMetaDataKey, BinaryMetadata> e) {
                        return e.getValue().wrap(portableCtx);
                    }
                });
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteBinary binary() throws IgniteException {
        return portables;
    }

    /** {@inheritDoc} */
    @Override public boolean isPortableObject(Object obj) {
        return obj instanceof BinaryObject;
    }

    /** {@inheritDoc} */
    @Override public boolean isPortableEnabled(CacheConfiguration<?, ?> ccfg) {
        return marsh instanceof PortableMarshaller;
    }

    /**
     * @param po Portable object.
     * @return Affinity key.
     */
    public Object affinityKey(BinaryObject po) {
        try {
            BinaryType meta = po.type();

            if (meta != null) {
                String affKeyFieldName = meta.affinityKeyFieldName();

                if (affKeyFieldName != null)
                    return po.field(affKeyFieldName);
            }
        }
        catch (BinaryObjectException e) {
            U.error(log, "Failed to get affinity field from portable object: " + po, e);
        }

        return po;
    }

    /** {@inheritDoc} */
    @Override public int typeId(Object obj) {
        if (obj == null)
            return 0;

        return isPortableObject(obj) ? ((BinaryObject)obj).typeId() : typeId(obj.getClass().getSimpleName());
    }

    /** {@inheritDoc} */
    @Override public Object field(Object obj, String fieldName) {
        if (obj == null)
            return null;

        return isPortableObject(obj) ? ((BinaryObject)obj).field(fieldName) : super.field(obj, fieldName);
    }

    /** {@inheritDoc} */
    @Override public boolean hasField(Object obj, String fieldName) {
        return obj != null && ((BinaryObject)obj).hasField(fieldName);
    }

    /**
     * @return Portable context.
     */
    public PortableContext portableContext() {
        return portableCtx;
    }

    /** {@inheritDoc} */
    @Override public CacheObjectContext contextForCache(CacheConfiguration cfg) throws IgniteCheckedException {
        assert cfg != null;

        boolean portableEnabled = marsh instanceof PortableMarshaller && !GridCacheUtils.isSystemCache(cfg.getName()) &&
            !GridCacheUtils.isIgfsCache(ctx.config(), cfg.getName());

        CacheObjectContext ctx0 = super.contextForCache(cfg);

        CacheObjectContext res = new CacheObjectPortableContext(ctx,
            ctx0.copyOnGet(),
            ctx0.storeValue(),
            portableEnabled,
            ctx0.addDeploymentInfo());

        ctx.resource().injectGeneric(res.defaultAffMapper());

        return res;
    }

    /** {@inheritDoc} */
    @Override public byte[] marshal(CacheObjectContext ctx, Object val) throws IgniteCheckedException {
        if (!((CacheObjectPortableContext)ctx).portableEnabled() || portableMarsh == null)
            return super.marshal(ctx, val);

        byte[] arr = portableMarsh.marshal(val);

        assert arr.length > 0;

        return arr;
    }

    /** {@inheritDoc} */
    @Override public Object unmarshal(CacheObjectContext ctx, byte[] bytes, ClassLoader clsLdr)
        throws IgniteCheckedException {
        if (!((CacheObjectPortableContext)ctx).portableEnabled() || portableMarsh == null)
            return super.unmarshal(ctx, bytes, clsLdr);

        return portableMarsh.unmarshal(bytes, clsLdr);
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject toCacheKeyObject(CacheObjectContext ctx, Object obj, boolean userObj) {
        if (!((CacheObjectPortableContext)ctx).portableEnabled())
            return super.toCacheKeyObject(ctx, obj, userObj);

        if (obj instanceof KeyCacheObject)
            return (KeyCacheObject)obj;

        if (((CacheObjectPortableContext)ctx).portableEnabled()) {
            obj = toPortable(obj);

            if (obj instanceof BinaryObject)
                return (BinaryObjectImpl)obj;
        }

        return toCacheKeyObject0(obj, userObj);
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject toCacheObject(CacheObjectContext ctx, @Nullable Object obj,
        boolean userObj) {
        if (!((CacheObjectPortableContext)ctx).portableEnabled())
            return super.toCacheObject(ctx, obj, userObj);

        if (obj == null || obj instanceof CacheObject)
            return (CacheObject)obj;

        obj = toPortable(obj);

        if (obj instanceof BinaryObject)
            return (BinaryObjectImpl)obj;

        return toCacheObject0(obj, userObj);
    }

    /** {@inheritDoc} */
    @Override public CacheObject toCacheObject(CacheObjectContext ctx, byte type, byte[] bytes) {
        if (type == BinaryObjectImpl.TYPE_BINARY)
            return new BinaryObjectImpl(portableContext(), bytes, 0);

        return super.toCacheObject(ctx, type, bytes);
    }

    /** {@inheritDoc} */
    @Override public CacheObject toCacheObject(GridCacheContext ctx, long valPtr, boolean tmp)
        throws IgniteCheckedException {
        if (!((CacheObjectPortableContext)ctx.cacheObjectContext()).portableEnabled())
            return super.toCacheObject(ctx, valPtr, tmp);

        Object val = unmarshal(valPtr, !tmp);

        if (val instanceof BinaryObjectOffheapImpl)
            return (BinaryObjectOffheapImpl)val;

        return new CacheObjectImpl(val, null);
    }

    /** {@inheritDoc} */
    @Override public Object unwrapTemporary(GridCacheContext ctx, Object obj) throws BinaryObjectException {
        if (!((CacheObjectPortableContext)ctx.cacheObjectContext()).portableEnabled())
            return obj;

        if (obj instanceof BinaryObjectOffheapImpl)
            return ((BinaryObjectOffheapImpl)obj).heapCopy();

        return obj;
    }

    /**
     * @param obj Object.
     * @return Portable object.
     * @throws IgniteException In case of error.
     */
    @Nullable public Object toPortable(@Nullable Object obj) throws IgniteException {
        if (obj == null)
            return null;

        if (isPortableObject(obj))
            return obj;

        return marshalToPortable(obj);
    }

    /**
     * Merge old and new metas.
     *
     * @param oldMeta Old meta.
     * @param newMeta New meta.
     * @return New meta if old meta was null, old meta if no changes detected, merged meta otherwise.
     * @throws BinaryObjectException If merge failed due to metadata conflict.
     */
    private static BinaryMetadata mergeMetadata(@Nullable BinaryMetadata oldMeta, BinaryMetadata newMeta) {
        assert newMeta != null;

        if (oldMeta == null)
            return newMeta;
        else {
            assert oldMeta.typeId() == newMeta.typeId();

            // Check type name.
            if (!F.eq(oldMeta.typeName(), newMeta.typeName())) {
                throw new BinaryObjectException(
                    "Two portable types have duplicate type ID [" + "typeId=" + oldMeta.typeId() +
                        ", typeName1=" + oldMeta.typeName() + ", typeName2=" + newMeta.typeName() + ']'
                );
            }

            // Check affinity field names.
            if (!F.eq(oldMeta.affinityKeyFieldName(), newMeta.affinityKeyFieldName())) {
                throw new BinaryObjectException(
                    "Binary type has different affinity key fields [" + "typeName=" + newMeta.typeName() +
                        ", affKeyFieldName1=" + oldMeta.affinityKeyFieldName() +
                        ", affKeyFieldName2=" + newMeta.affinityKeyFieldName() + ']'
                );
            }

            // Check and merge fields.
            boolean changed = false;

            Map<String, Integer> mergedFields = new HashMap<>(oldMeta.fieldsMap());
            Map<String, Integer> newFields = newMeta.fieldsMap();

            for (Map.Entry<String, Integer> newField : newFields.entrySet()) {
                Integer oldFieldType = mergedFields.put(newField.getKey(), newField.getValue());

                if (oldFieldType == null)
                    changed = true;
                else if (!F.eq(oldFieldType, newField.getValue())) {
                    throw new BinaryObjectException(
                        "Binary type has different field types [" + "typeName=" + oldMeta.typeName() +
                            ", fieldName=" + newField.getKey() +
                            ", fieldTypeName1=" + PortableUtils.fieldTypeName(oldFieldType) +
                            ", fieldTypeName2=" + PortableUtils.fieldTypeName(newField.getValue()) + ']'
                    );
                }
            }

            // Check and merge schemas.
            Collection<PortableSchema> mergedSchemas = new HashSet<>(oldMeta.schemas());

            for (PortableSchema newSchema : newMeta.schemas()) {
                if (mergedSchemas.add(newSchema))
                    changed = true;
            }

            // Return either old meta if no changes detected, or new merged meta.
            return changed ? new BinaryMetadata(oldMeta.typeId(), oldMeta.typeName(), mergedFields,
                oldMeta.affinityKeyFieldName(), mergedSchemas) : oldMeta;
        }
    }

    /**
     * Processor responsible for metadata update.
     */
    private static class MetadataProcessor
        implements EntryProcessor<PortableMetaDataKey, BinaryMetadata, BinaryObjectException>, Externalizable {
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
        @Override public BinaryObjectException process(MutableEntry<PortableMetaDataKey, BinaryMetadata> entry,
            Object... args) {
            try {
                BinaryMetadata oldMeta = entry.getValue();

                BinaryMetadata mergedMeta = mergeMetadata(oldMeta, newMeta);

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
    class MetaDataEntryListener implements CacheEntryUpdatedListener<PortableMetaDataKey, BinaryMetadata> {
        /** {@inheritDoc} */
        @Override public void onUpdated(
            Iterable<CacheEntryEvent<? extends PortableMetaDataKey, ? extends BinaryMetadata>> evts)
            throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends PortableMetaDataKey, ? extends BinaryMetadata> evt : evts) {
                assert evt.getEventType() == EventType.CREATED || evt.getEventType() == EventType.UPDATED : evt;

                PortableMetaDataKey key = evt.getKey();

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
            return evt.getKey() instanceof PortableMetaDataKey;
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
            return key instanceof PortableMetaDataKey;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MetaDataPredicate.class, this);
        }
    }
}
