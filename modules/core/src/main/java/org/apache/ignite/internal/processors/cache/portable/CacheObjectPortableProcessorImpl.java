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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgnitePortables;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.portable.GridPortableMarshaller;
import org.apache.ignite.internal.portable.PortableContext;
import org.apache.ignite.internal.portable.PortableMetaDataHandler;
import org.apache.ignite.internal.portable.PortableMetaDataImpl;
import org.apache.ignite.internal.portable.PortableObjectImpl;
import org.apache.ignite.internal.portable.PortableObjectOffheapImpl;
import org.apache.ignite.internal.portable.PortableUtils;
import org.apache.ignite.internal.portable.builder.PortableBuilderImpl;
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
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
import org.apache.ignite.portable.PortableBuilder;
import org.apache.ignite.portable.PortableException;
import org.apache.ignite.portable.PortableMetadata;
import org.apache.ignite.portable.PortableObject;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;
import sun.misc.Unsafe;

import static org.apache.ignite.internal.portable.GridPortableMarshaller.BOOLEAN;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.BOOLEAN_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.BYTE;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.BYTE_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.CHAR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.CHAR_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.COL;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DATE;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DATE_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DECIMAL;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DECIMAL_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DOUBLE;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DOUBLE_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.ENUM;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.ENUM_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.FLOAT;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.FLOAT_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.INT;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.INT_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.LONG;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.LONG_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.MAP;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.OBJ;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.OBJ_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.PORTABLE_OBJ;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.SHORT;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.SHORT_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.STRING;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.STRING_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.UUID;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.UUID_ARR;

/**
 * Portable processor implementation.
 */
public class CacheObjectPortableProcessorImpl extends IgniteCacheObjectProcessorImpl implements
    CacheObjectPortableProcessor {
    /** */
    public static final String[] FIELD_TYPE_NAMES;

    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private final CountDownLatch startLatch = new CountDownLatch(1);

    /** */
    private final boolean clientNode;

    /** */
    private volatile IgniteCacheProxy<PortableMetaDataKey, PortableMetadata> metaDataCache;

    /** */
    private final ConcurrentHashMap8<PortableMetaDataKey, PortableMetadata> clientMetaDataCache;

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
    private IgnitePortables portables;

    /** Metadata updates collected before metadata cache is initialized. */
    private final Map<Integer, PortableMetadata> metaBuf = new ConcurrentHashMap<>();

    /** */
    private UUID metaCacheQryId;

    /**
     *
     */
    static {
        FIELD_TYPE_NAMES = new String[104];

        FIELD_TYPE_NAMES[BYTE] = "byte";
        FIELD_TYPE_NAMES[SHORT] = "short";
        FIELD_TYPE_NAMES[INT] = "int";
        FIELD_TYPE_NAMES[LONG] = "long";
        FIELD_TYPE_NAMES[BOOLEAN] = "boolean";
        FIELD_TYPE_NAMES[FLOAT] = "float";
        FIELD_TYPE_NAMES[DOUBLE] = "double";
        FIELD_TYPE_NAMES[CHAR] = "char";
        FIELD_TYPE_NAMES[UUID] = "UUID";
        FIELD_TYPE_NAMES[DECIMAL] = "decimal";
        FIELD_TYPE_NAMES[STRING] = "String";
        FIELD_TYPE_NAMES[DATE] = "Date";
        FIELD_TYPE_NAMES[ENUM] = "Enum";
        FIELD_TYPE_NAMES[OBJ] = "Object";
        FIELD_TYPE_NAMES[PORTABLE_OBJ] = "Object";
        FIELD_TYPE_NAMES[COL] = "Collection";
        FIELD_TYPE_NAMES[MAP] = "Map";
        FIELD_TYPE_NAMES[BYTE_ARR] = "byte[]";
        FIELD_TYPE_NAMES[SHORT_ARR] = "short[]";
        FIELD_TYPE_NAMES[INT_ARR] = "int[]";
        FIELD_TYPE_NAMES[LONG_ARR] = "long[]";
        FIELD_TYPE_NAMES[BOOLEAN_ARR] = "boolean[]";
        FIELD_TYPE_NAMES[FLOAT_ARR] = "float[]";
        FIELD_TYPE_NAMES[DOUBLE_ARR] = "double[]";
        FIELD_TYPE_NAMES[CHAR_ARR] = "char[]";
        FIELD_TYPE_NAMES[UUID_ARR] = "UUID[]";
        FIELD_TYPE_NAMES[DECIMAL_ARR] = "decimal[]";
        FIELD_TYPE_NAMES[STRING_ARR] = "String[]";
        FIELD_TYPE_NAMES[DATE_ARR] = "Date[]";
        FIELD_TYPE_NAMES[OBJ_ARR] = "Object[]";
        FIELD_TYPE_NAMES[ENUM_ARR] = "Enum[]";
    }

    /**
     * @param typeName Field type name.
     * @return Field type ID;
     */
    @SuppressWarnings("StringEquality")
    public static int fieldTypeId(String typeName) {
        for (int i = 0; i < FIELD_TYPE_NAMES.length; i++) {
            String typeName0 = FIELD_TYPE_NAMES[i];

            if (typeName.equals(typeName0))
                return i;
        }

        throw new IllegalArgumentException("Invalid metadata type name: " + typeName);
    }

    /**
     * @param typeId Field type ID.
     * @return Field type name.
     */
    public static String fieldTypeName(int typeId) {
        assert typeId >= 0 && typeId < FIELD_TYPE_NAMES.length : typeId;

        String typeName = FIELD_TYPE_NAMES[typeId];

        assert typeName != null : typeId;

        return typeName;
    }

    /**
     * @param typeIds Field type IDs.
     * @return Field type names.
     */
    public static Map<String, String> fieldTypeNames(Map<String, Integer> typeIds) {
        Map<String, String> names = U.newHashMap(typeIds.size());

        for (Map.Entry<String, Integer> e : typeIds.entrySet())
            names.put(e.getKey(), fieldTypeName(e.getValue()));

        return names;
    }

    /**
     * @param ctx Kernal context.
     */
    public CacheObjectPortableProcessorImpl(GridKernalContext ctx) {
        super(ctx);

        marsh = ctx.grid().configuration().getMarshaller();

        clientNode = this.ctx.clientNode();

        clientMetaDataCache = clientNode ? new ConcurrentHashMap8<PortableMetaDataKey, PortableMetadata>() : null;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (marsh instanceof PortableMarshaller) {
            PortableMetaDataHandler metaHnd = new PortableMetaDataHandler() {
                @Override public void addMeta(int typeId, PortableMetadata newMeta)
                    throws PortableException {
                    if (metaDataCache == null) {
                        PortableMetadata oldMeta = metaBuf.get(typeId);

                        if (oldMeta == null || checkMeta(typeId, oldMeta, newMeta, null)) {
                            synchronized (this) {
                                Map<String, String> fields = new HashMap<>();

                                if (checkMeta(typeId, oldMeta, newMeta, fields)) {
                                    newMeta = new PortableMetaDataImpl(newMeta.typeName(),
                                        fields,
                                        newMeta.affinityKeyFieldName());

                                    metaBuf.put(typeId, newMeta);
                                }
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

                    CacheObjectPortableProcessorImpl.this.addMeta(typeId, newMeta);
                }

                @Override public PortableMetadata metadata(int typeId) throws PortableException {
                    if (metaDataCache == null)
                        U.awaitQuiet(startLatch);

                    return CacheObjectPortableProcessorImpl.this.metadata(typeId);
                }
            };

            PortableMarshaller pMarh0 = (PortableMarshaller)marsh;

            portableCtx = new PortableContext(metaHnd, ctx.gridName());

            IgniteUtils.invoke(PortableMarshaller.class, pMarh0, "setPortableContext", portableCtx);

            portableMarsh = new GridPortableMarshaller(portableCtx);

            portables = new IgnitePortablesImpl(ctx, this);
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

                CacheQuery<Map.Entry<PortableMetaDataKey, PortableMetadata>> qry =
                    qryMgr.createScanQuery(new MetaDataPredicate(), null, false);

                qry.keepAll(false);

                qry.projection(ctx.cluster().get().forNode(oldestSrvNode));

                try {
                    CacheQueryFuture<Map.Entry<PortableMetaDataKey, PortableMetadata>> fut = qry.execute();

                    Map.Entry<PortableMetaDataKey, PortableMetadata> next;

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

                break;
            }
        }

        startLatch.countDown();

        for (Map.Entry<Integer, PortableMetadata> e : metaBuf.entrySet())
            addMeta(e.getKey(), e.getValue());

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
    private void addClientCacheMetaData(PortableMetaDataKey key, final PortableMetadata newMeta) {
        clientMetaDataCache.compute(key,
            new ConcurrentHashMap8.BiFun<PortableMetaDataKey, PortableMetadata, PortableMetadata>() {
                @Override public PortableMetadata apply(PortableMetaDataKey key, PortableMetadata oldMeta) {
                    PortableMetadata res;

                    try {
                        res = checkMeta(key.typeId(), oldMeta, newMeta, null) ? newMeta : oldMeta;
                    }
                    catch (PortableException e) {
                        res = oldMeta;
                    }

                    return res;
                }
            }
        );
    }

    /** {@inheritDoc} */
    @Override public int typeId(String typeName) {
        return portableCtx.typeId(typeName);
    }

    /**
     * @param obj Object.
     * @return Bytes.
     * @throws PortableException If failed.
     */
    public byte[] marshal(@Nullable Object obj) throws PortableException {
        byte[] arr = portableMarsh.marshal(obj, 0);

        assert arr.length > 0;

        return arr;
    }

    /**
     * @param ptr Off-heap pointer.
     * @param forceHeap If {@code true} creates heap-based object.
     * @return Object.
     * @throws PortableException If failed.
     */
    public Object unmarshal(long ptr, boolean forceHeap) throws PortableException {
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
    @Override public Object marshalToPortable(@Nullable Object obj) throws PortableException {
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

        byte[] arr = portableMarsh.marshal(obj, 0);

        assert arr.length > 0;

        Object obj0 = portableMarsh.unmarshal(arr, null);

        assert obj0 instanceof PortableObject;

        ((PortableObjectImpl)obj0).detachAllowed(true);

        return obj0;
    }

    /**
     * @return Marshaller.
     */
    public GridPortableMarshaller marshaller() {
        return portableMarsh;
    }

    /** {@inheritDoc} */
    @Override public PortableBuilder builder(int typeId) {
        return new PortableBuilderImpl(portableCtx, typeId);
    }

    /** {@inheritDoc} */
    @Override public PortableBuilder builder(String clsName) {
        return new PortableBuilderImpl(portableCtx, clsName);
    }

    /** {@inheritDoc} */
    @Override public PortableBuilder builder(PortableObject portableObj) {
        return PortableBuilderImpl.wrap(portableObj);
    }

    /** {@inheritDoc} */
    @Override public void updateMetaData(int typeId, String typeName, @Nullable String affKeyFieldName,
        Map<String, Integer> fieldTypeIds) throws PortableException {
        portableCtx.updateMetaData(typeId,
            new PortableMetaDataImpl(typeName, fieldTypeNames(fieldTypeIds), affKeyFieldName));
    }

    /** {@inheritDoc} */
    @Override public void addMeta(final int typeId, final PortableMetadata newMeta) throws PortableException {
        assert newMeta != null;

        final PortableMetaDataKey key = new PortableMetaDataKey(typeId);

        try {
            PortableMetadata oldMeta = metaDataCache.localPeek(key);

            if (oldMeta == null || checkMeta(typeId, oldMeta, newMeta, null)) {
                PortableException err = metaDataCache.invoke(key, new MetaDataProcessor(typeId, newMeta));

                if (err != null)
                    throw err;
            }
        }
        catch (CacheException e) {
            throw new PortableException("Failed to update meta data for type: " + newMeta.typeName(), e);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public PortableMetadata metadata(final int typeId) throws PortableException {
        try {
            if (clientNode)
                return clientMetaDataCache.get(new PortableMetaDataKey(typeId));

            return metaDataCache.localPeek(new PortableMetaDataKey(typeId));
        }
        catch (CacheException e) {
            throw new PortableException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, PortableMetadata> metadata(Collection<Integer> typeIds)
        throws PortableException {
        try {
            Collection<PortableMetaDataKey> keys = new ArrayList<>(typeIds.size());

            for (Integer typeId : typeIds)
                keys.add(new PortableMetaDataKey(typeId));

            Map<PortableMetaDataKey, PortableMetadata> meta = metaDataCache.getAll(keys);

            Map<Integer, PortableMetadata> res = U.newHashMap(meta.size());

            for (Map.Entry<PortableMetaDataKey, PortableMetadata> e : meta.entrySet())
                res.put(e.getKey().typeId(), e.getValue());

            return res;
        }
        catch (CacheException e) {
            throw new PortableException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Collection<PortableMetadata> metadata() throws PortableException {
        if (clientNode)
            return new ArrayList<>(clientMetaDataCache.values());

        return F.viewReadOnly(metaDataCache.entrySetx(metaPred),
            new C1<Cache.Entry<PortableMetaDataKey, PortableMetadata>, PortableMetadata>() {
                private static final long serialVersionUID = 0L;

                @Override public PortableMetadata apply(
                    Cache.Entry<PortableMetaDataKey, PortableMetadata> e) {
                    return e.getValue();
                }
            });
    }

    /** {@inheritDoc} */
    @Override public IgnitePortables portables() throws IgniteException {
        return portables;
    }

    /** {@inheritDoc} */
    @Override public boolean isPortableObject(Object obj) {
        return obj instanceof PortableObject;
    }

    /** {@inheritDoc} */
    @Override public boolean isPortableEnabled(CacheConfiguration<?, ?> ccfg) {
        return marsh instanceof PortableMarshaller;
    }

    /**
     * @param po Portable object.
     * @return Affinity key.
     */
    public Object affinityKey(PortableObject po) {
        try {
            PortableMetadata meta = po.metaData();

            if (meta != null) {
                String affKeyFieldName = meta.affinityKeyFieldName();

                if (affKeyFieldName != null)
                    return po.field(affKeyFieldName);
            }
        }
        catch (PortableException e) {
            U.error(log, "Failed to get affinity field from portable object: " + po, e);
        }

        return po;
    }

    /** {@inheritDoc} */
    @Override public int typeId(Object obj) {
        if (obj == null)
            return 0;

        return isPortableObject(obj) ? ((PortableObject)obj).typeId() : typeId(obj.getClass().getSimpleName());
    }

    /** {@inheritDoc} */
    @Override public Object field(Object obj, String fieldName) {
        if (obj == null)
            return null;

        return isPortableObject(obj) ? ((PortableObject)obj).field(fieldName) : super.field(obj, fieldName);
    }

    /** {@inheritDoc} */
    @Override public boolean hasField(Object obj, String fieldName) {
        return obj != null && ((PortableObject)obj).hasField(fieldName);
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
            portableEnabled);

        ctx.resource().injectGeneric(res.defaultAffMapper());

        return res;
    }

    /** {@inheritDoc} */
    @Override public byte[] marshal(CacheObjectContext ctx, Object val) throws IgniteCheckedException {
        if (!((CacheObjectPortableContext)ctx).portableEnabled() || portableMarsh == null)
            return super.marshal(ctx, val);

        byte[] arr = portableMarsh.marshal(val, 0);

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

            if (obj instanceof PortableObject)
                return (PortableObjectImpl)obj;
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

        if (obj instanceof PortableObject)
            return (PortableObjectImpl)obj;

        return toCacheObject0(obj, userObj);
    }

    /** {@inheritDoc} */
    @Override public CacheObject toCacheObject(CacheObjectContext ctx, byte type, byte[] bytes) {
        if (type == PortableObjectImpl.TYPE_PORTABLE)
            return new PortableObjectImpl(portableContext(), bytes, 0);

        return super.toCacheObject(ctx, type, bytes);
    }

    /** {@inheritDoc} */
    @Override public CacheObject toCacheObject(GridCacheContext ctx, long valPtr, boolean tmp)
        throws IgniteCheckedException {
        if (!((CacheObjectPortableContext)ctx.cacheObjectContext()).portableEnabled())
            return super.toCacheObject(ctx, valPtr, tmp);

        Object val = unmarshal(valPtr, !tmp);

        if (val instanceof PortableObjectOffheapImpl)
            return (PortableObjectOffheapImpl)val;

        return new CacheObjectImpl(val, null);
    }

    /** {@inheritDoc} */
    @Override public Object unwrapTemporary(GridCacheContext ctx, Object obj) throws PortableException {
        if (!((CacheObjectPortableContext)ctx.cacheObjectContext()).portableEnabled())
            return obj;

        if (obj instanceof PortableObjectOffheapImpl)
            return ((PortableObjectOffheapImpl)obj).heapCopy();

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
     * @param typeId Type ID.
     * @param oldMeta Old meta.
     * @param newMeta New meta.
     * @param fields Fields map.
     * @return Whether meta is changed.
     * @throws PortableException In case of error.
     */
    private static boolean checkMeta(int typeId, @Nullable PortableMetadata oldMeta,
        PortableMetadata newMeta, @Nullable Map<String, String> fields) throws PortableException {
        assert newMeta != null;

        Map<String, String> oldFields = oldMeta != null ? ((PortableMetaDataImpl)oldMeta).fieldsMeta() : null;
        Map<String, String> newFields = ((PortableMetaDataImpl)newMeta).fieldsMeta();

        boolean changed = false;

        if (oldMeta != null) {
            if (!oldMeta.typeName().equals(newMeta.typeName())) {
                throw new PortableException(
                    "Two portable types have duplicate type ID [" +
                        "typeId=" + typeId +
                        ", typeName1=" + oldMeta.typeName() +
                        ", typeName2=" + newMeta.typeName() +
                        ']'
                );
            }

            if (!F.eq(oldMeta.affinityKeyFieldName(), newMeta.affinityKeyFieldName())) {
                throw new PortableException(
                    "Portable type has different affinity key fields on different clients [" +
                        "typeName=" + newMeta.typeName() +
                        ", affKeyFieldName1=" + oldMeta.affinityKeyFieldName() +
                        ", affKeyFieldName2=" + newMeta.affinityKeyFieldName() +
                        ']'
                );
            }

            if (fields != null)
                fields.putAll(oldFields);
        }
        else
            changed = true;

        for (Map.Entry<String, String> e : newFields.entrySet()) {
            String typeName = oldFields != null ? oldFields.get(e.getKey()) : null;

            if (typeName != null) {
                if (!typeName.equals(e.getValue())) {
                    throw new PortableException(
                        "Portable field has different types on different clients [" +
                            "typeName=" + newMeta.typeName() +
                            ", fieldName=" + e.getKey() +
                            ", fieldTypeName1=" + typeName +
                            ", fieldTypeName2=" + e.getValue() +
                            ']'
                    );
                }
            }
            else {
                if (fields != null)
                    fields.put(e.getKey(), e.getValue());

                changed = true;
            }
        }

        return changed;
    }

    /**
     */
    private static class MetaDataProcessor implements
        EntryProcessor<PortableMetaDataKey, PortableMetadata, PortableException>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private int typeId;

        /** */
        private PortableMetadata newMeta;

        /**
         * For {@link Externalizable}.
         */
        public MetaDataProcessor() {
            // No-op.
        }

        /**
         * @param typeId Type ID.
         * @param newMeta New metadata.
         */
        private MetaDataProcessor(int typeId, PortableMetadata newMeta) {
            assert newMeta != null;

            this.typeId = typeId;
            this.newMeta = newMeta;
        }

        /** {@inheritDoc} */
        @Override public PortableException process(
            MutableEntry<PortableMetaDataKey, PortableMetadata> entry,
            Object... args) {
            try {
                PortableMetadata oldMeta = entry.getValue();

                Map<String, String> fields = new HashMap<>();

                if (checkMeta(typeId, oldMeta, newMeta, fields)) {
                    PortableMetadata res = new PortableMetaDataImpl(newMeta.typeName(),
                        fields,
                        newMeta.affinityKeyFieldName());

                    entry.setValue(res);

                    return null;
                }
                else
                    return null;
            }
            catch (PortableException e) {
                return e;
            }
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(typeId);
            out.writeObject(newMeta);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            typeId = in.readInt();
            newMeta = (PortableMetadata)in.readObject();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MetaDataProcessor.class, this);
        }
    }

    /**
     *
     */
    class MetaDataEntryListener implements CacheEntryUpdatedListener<PortableMetaDataKey, PortableMetadata> {
        /** {@inheritDoc} */
        @Override public void onUpdated(
            Iterable<CacheEntryEvent<? extends PortableMetaDataKey, ? extends PortableMetadata>> evts)
            throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends PortableMetaDataKey, ? extends PortableMetadata> evt : evts) {
                assert evt.getEventType() == EventType.CREATED || evt.getEventType() == EventType.UPDATED : evt;

                PortableMetaDataKey key = evt.getKey();

                final PortableMetadata newMeta = evt.getValue();

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