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

package org.apache.ignite.internal.processors.cacheobject;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.optimized.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import java.math.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheMemoryMode.*;

/**
 *
 */
@SuppressWarnings("UnnecessaryFullyQualifiedName")
public class IgniteCacheObjectProcessorImpl extends GridProcessorAdapter implements IgniteCacheObjectProcessor {
    /** */
    private static final sun.misc.Unsafe UNSAFE = GridUnsafe.unsafe();

    /** Immutable classes. */
    private static final Collection<Class<?>> IMMUTABLE_CLS = new HashSet<>();

    /** */
    private static final OptimizedObjectMetadata EMPTY_META = new OptimizedObjectMetadata();

    /** */
    private volatile IgniteCacheProxy<OptimizedObjectMetadataKey, OptimizedObjectMetadata> metaDataCache;

    /** Metadata updates collected before metadata cache is initialized. */
    private final ConcurrentMap<Integer, OptimizedObjectMetadata> metaBuf = new ConcurrentHashMap<>();

    /** */
    private final CountDownLatch startLatch = new CountDownLatch(1);

    /** */
    private OptimizedMarshallerIndexingHandler idxHnd;

    /** */
    private OptimizedMarshaller optMarsh;

    /** Class presence cache map. */
    private ConcurrentMap<Integer, Boolean> clsPresenceMap = new ConcurrentHashMap8<>();

    /** Affinity fields map by type ID. */
    private ConcurrentMap<Integer, String> affFields = new ConcurrentHashMap8<>();

    /**
     *
     */
    static {
        IMMUTABLE_CLS.add(String.class);
        IMMUTABLE_CLS.add(Boolean.class);
        IMMUTABLE_CLS.add(Byte.class);
        IMMUTABLE_CLS.add(Short.class);
        IMMUTABLE_CLS.add(Character.class);
        IMMUTABLE_CLS.add(Integer.class);
        IMMUTABLE_CLS.add(Long.class);
        IMMUTABLE_CLS.add(Float.class);
        IMMUTABLE_CLS.add(Double.class);
        IMMUTABLE_CLS.add(UUID.class);
        IMMUTABLE_CLS.add(IgniteUuid.class);
        IMMUTABLE_CLS.add(BigDecimal.class);
    }

    /**
     * @param ctx Context.
     */
    public IgniteCacheObjectProcessorImpl(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        Marshaller marsh = ctx.config().getMarshaller();

        if (marsh instanceof OptimizedMarshaller) {
            optMarsh = (OptimizedMarshaller)marsh;

            idxHnd = new OptimizedMarshallerIndexingHandler();

            OptimizedMarshallerMetaHandler metaHandler = new OptimizedMarshallerMetaHandler() {
                @Override public void addMeta(int typeId, OptimizedObjectMetadata meta) {
                    if (metaBuf.containsKey(typeId))
                        return;

                    metaBuf.put(typeId, meta);

                    if (metaDataCache != null)
                        metaDataCache.putIfAbsent(new OptimizedObjectMetadataKey(typeId), meta);
                }

                @Override public OptimizedObjectMetadata metadata(int typeId) {
                    if (metaDataCache == null)
                        U.awaitQuiet(startLatch);

                    OptimizedObjectMetadata meta = metaBuf.get(typeId);

                    if (meta != null)
                        return meta == EMPTY_META ? null : meta;

                    meta = metaDataCache.localPeek(new OptimizedObjectMetadataKey(typeId));

                    if (meta == null)
                        meta = EMPTY_META;

                    metaBuf.put(typeId, meta);

                    return meta == EMPTY_META ? null : meta;
                }
            };

            idxHnd.setMetaHandler(metaHandler);
            optMarsh.setIndexingHandler(idxHnd);
        }
    }

    /** {@inheritDoc} */
    @Override public void onUtilityCacheStarted() throws IgniteCheckedException {
        metaDataCache = ctx.cache().jcache(CU.UTILITY_CACHE_NAME);

        startLatch.countDown();

        for (Map.Entry<Integer, OptimizedObjectMetadata> e : metaBuf.entrySet())
            metaDataCache.putIfAbsent(new OptimizedObjectMetadataKey(e.getKey()), e.getValue());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onCacheStart(GridCacheContext cctx) {
        if (!cctx.userCache())
            return;

        Collection<CacheTypeMetadata> typeMetadata = cctx.config().getTypeMetadata();

        for (CacheTypeMetadata typeMeta : typeMetadata) {
            int typeId = typeId(typeMeta.getKeyType());

            if (typeId != 0 && typeMeta.getAffinityKeyFieldName() != null)
                affFields.put(typeId, typeMeta.getAffinityKeyFieldName());
        }
    }

    /** {@inheritDoc} */
    @Override public void onCacheStop(GridCacheContext cctx) {

    }

    /**
     * @param io Indexed object.
     * @return Affinity key.
     */
    @Override public Object affinityKey(CacheIndexedObject io) {
        try {
            String affField = affFields.get(io.typeId());

            if (affField != null)
                return io.field(affField);
        }
        catch (IgniteException e) {
            U.error(log, "Failed to get affinity field from Ignite object: " + io, e);
        }

        return io;
    }

    /** {@inheritDoc} */
    @Override public boolean hasClass(int typeId) {
        Boolean res = clsPresenceMap.get(typeId);

        if (res == null) {
            try {
                res = ctx.marshallerContext().getClass(typeId, null) != null;
            }
            catch (ClassNotFoundException | IgniteCheckedException ignore) {
                res = false;
            }

            clsPresenceMap.put(typeId, res);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean isIndexedObjectOrCollectionType(Class<?> cls) {
        return CacheIndexedObject.class.isAssignableFrom(cls) ||
            cls == Object[].class ||
            Collection.class.isAssignableFrom(cls) ||
            Map.class.isAssignableFrom(cls) ||
            Map.Entry.class.isAssignableFrom(cls);
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject prepareForCache(@Nullable CacheObject obj, GridCacheContext cctx) {
        if (obj == null)
            return null;

        return obj.prepareForCache(cctx.cacheObjectContext());
    }

    /** {@inheritDoc} */
    @Override public byte[] marshal(CacheObjectContext ctx, Object val) throws IgniteCheckedException {
        return CU.marshal(ctx.kernalContext().cache().context(), val);
    }

    /** {@inheritDoc} */
    @Override public Object unmarshal(CacheObjectContext ctx, byte[] bytes, ClassLoader clsLdr)
        throws IgniteCheckedException
    {
        return ctx.kernalContext().cache().context().marshaller().unmarshal(bytes, clsLdr);
    }

    /** {@inheritDoc} */
    @Override public Object unmarshal(CacheObjectContext ctx, byte[] bytes, int off, int len,
        ClassLoader clsLdr) throws IgniteCheckedException {
        if (optMarsh != null)
            return optMarsh.unmarshal(bytes, off, len, clsLdr);

        else if (off > 0 || len != bytes.length) {
            byte[] arr = new byte[len];

            U.arrayCopy(bytes, off, arr, 0, len);

            bytes = arr;
        }

        return unmarshal(ctx, bytes, clsLdr);
    }

    /** {@inheritDoc} */
    @Override @Nullable public KeyCacheObject toCacheKeyObject(CacheObjectContext ctx, Object obj, boolean userObj) {
        if (obj instanceof KeyCacheObject)
            return (KeyCacheObject)obj;

        return toCacheKeyObject0(ctx, obj, userObj);
    }

    /**
     * @param obj Object.
     * @param userObj If {@code true} then given object is object provided by user and should be copied
     *        before stored in cache.
     * @return Key cache object.
     */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    protected KeyCacheObject toCacheKeyObject0(CacheObjectContext ctx, Object obj, boolean userObj) {
        if (!userObj)
            return isFieldsIndexingSupported(obj.getClass()) ? new KeyCacheIndexedObjectImpl(ctx, obj, null) :
                new KeyCacheObjectImpl(obj, null);

        return isFieldsIndexingSupported(obj.getClass()) ? new UserKeyCacheIndexedObjectImpl(ctx, obj) :
            new UserKeyCacheObjectImpl(obj);
    }

    /** {@inheritDoc} */
    @Override public CacheObject toCacheObject(GridCacheContext ctx, long valPtr, boolean tmp)
        throws IgniteCheckedException
    {
        assert valPtr != 0;

        int size = UNSAFE.getInt(valPtr);

        byte type = UNSAFE.getByte(valPtr + 4);

        byte[] bytes = U.copyMemory(valPtr + 5, size);

        if (ctx.kernalContext().config().isPeerClassLoadingEnabled() &&
            ctx.offheapTiered() &&
            type != CacheObject.TYPE_BYTE_ARR) {
            IgniteUuid valClsLdrId = U.readGridUuid(valPtr + 5 + size);

            ClassLoader ldr =
                valClsLdrId != null ? ctx.deploy().getClassLoader(valClsLdrId) : ctx.deploy().localLoader();

            return toCacheObject(ctx.cacheObjectContext(), unmarshal(ctx.cacheObjectContext(), bytes, ldr), false);
        }
        else
            return toCacheObject(ctx.cacheObjectContext(), type, bytes);
    }

    /** {@inheritDoc} */
    @Override public CacheObject toCacheObject(CacheObjectContext ctx, byte type, byte[] bytes) {
        switch (type) {
            case CacheObject.TYPE_BYTE_ARR:
                return new CacheObjectByteArrayImpl(bytes);

            case CacheObject.TYPE_REGULAR:
                return new CacheObjectImpl(null, bytes);

            case CacheObject.TYPE_OPTIMIZED:
                return new CacheIndexedObjectImpl(ctx, bytes, 0, bytes.length);
        }

        throw new IllegalArgumentException("Invalid object type: " + type);
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject toCacheObject(CacheObjectContext ctx, @Nullable Object obj, boolean userObj){
        if (obj == null || obj instanceof CacheObject)
            return (CacheObject)obj;

        return toCacheObject0(ctx, obj, userObj);
    }

    /**
     * @param obj Object.
     * @param userObj If {@code true} then given object is object provided by user and should be copied
     *        before stored in cache.
     * @return Cache object.
     */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    protected CacheObject toCacheObject0(CacheObjectContext ctx, @Nullable Object obj, boolean userObj) {
        assert obj != null;

        if (obj instanceof byte[]) {
            if (!userObj)
                return new CacheObjectByteArrayImpl((byte[])obj);

            return new UserCacheObjectByteArrayImpl((byte[])obj);
        }

        if (!userObj)
            return isFieldsIndexingSupported(obj.getClass()) ? new CacheIndexedObjectImpl(ctx, obj) :
                new CacheObjectImpl(obj, null);

        return isFieldsIndexingSupported(obj.getClass()) ? new UserCacheIndexedObjectImpl(ctx, obj, null) :
            new UserCacheObjectImpl(obj, null);
    }

    /** {@inheritDoc} */
    @Override public CacheObjectContext contextForCache(CacheConfiguration ccfg) throws IgniteCheckedException {
        assert ccfg != null;

        CacheMemoryMode memMode = ccfg.getMemoryMode();

        boolean storeVal = ctx.config().isPeerClassLoadingEnabled() ||
            GridQueryProcessor.isEnabled(ccfg) ||
            !ccfg.isCopyOnRead();

        boolean idxObj = ctx.cacheObjects().isFieldsIndexingEnabled() && !GridCacheUtils.isSystemCache(ccfg.getName())
            && !GridCacheUtils.isIgfsCache(ctx.config(), ccfg.getName());

        AffinityKeyMapper affMapper = ccfg.getAffinityMapper();

        if (affMapper == null)
            affMapper = new GridCacheDefaultAffinityKeyMapper();

        if (idxObj)
            affMapper = new CacheIndexedObjectDefaultAffinityMapper();

        ctx.resource().injectGeneric(affMapper);

        return new CacheObjectContext(
            ctx,
            ccfg.getName(),
            affMapper,
            ccfg.isCopyOnRead() && memMode != OFFHEAP_VALUES,
            storeVal
        );
    }

    /** {@inheritDoc} */
    @Override public boolean immutable(Object obj) {
        assert obj != null;

        return IMMUTABLE_CLS.contains(obj.getClass());
    }

    /** {@inheritDoc} */
    @Override public int typeId(String typeName) {
        return idxHnd != null ? OptimizedMarshallerUtils.resolveTypeId(typeName, idxHnd.idMapper()) : 0;
    }

    /** {@inheritDoc} */
    @Override public int typeId(Object obj) {
        if (obj instanceof CacheIndexedObjectImpl)
            return ((CacheIndexedObject)obj).typeId();

        return 0;
    }

    /** {@inheritDoc} */
    @Override public Object unwrapTemporary(GridCacheContext ctx, Object obj) throws IgniteException {
        return obj;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object unwrapIndexedObject(Object obj) throws IgniteException {
        return ((CacheIndexedObject)obj).deserialize();
    }

    /** {@inheritDoc} */
    @Override public boolean isIndexedObject(Object obj) {
        return obj instanceof CacheIndexedObject;
    }

    /** {@inheritDoc} */
    @Override public boolean isFieldsIndexingEnabled() {
        return idxHnd != null && idxHnd.isFieldsIndexingSupported();
    }

    /** {@inheritDoc} */
    @Override public boolean isFieldsIndexingSupported(Class<?> cls) {
        return idxHnd != null && idxHnd.isFieldsIndexingSupported() &&
            idxHnd.enableFieldsIndexingForClass(cls);
    }

    /**
     * Wraps key provided by user, must be serialized before stored in cache.
     */
    private static class UserKeyCacheObjectImpl extends KeyCacheObjectImpl {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         *
         */
        public UserKeyCacheObjectImpl() {
            //No-op.
        }

        /**
         * @param key Key.
         */
        UserKeyCacheObjectImpl(Object key) {
            super(key, null);
        }

        /** {@inheritDoc} */
        @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
            try {
                if (!ctx.processor().immutable(val)) {
                    if (valBytes == null)
                        valBytes = ctx.processor().marshal(ctx, val);

                    ClassLoader ldr = ctx.p2pEnabled() ?
                        IgniteUtils.detectClassLoader(IgniteUtils.detectClass(val)) : U.gridClassLoader();

                     Object val = ctx.processor().unmarshal(ctx, valBytes, ldr);

                    return new KeyCacheObjectImpl(val, valBytes);
                }

                return new KeyCacheObjectImpl(val, valBytes);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to marshal object: " + val, e);
            }
        }
    }

    /**
     * Wraps value provided by user, must be serialized before stored in cache.
     */
    private static class UserCacheObjectImpl extends CacheObjectImpl {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         *
         */
        public UserCacheObjectImpl() {
            //No-op.
        }

        /**
         * @param val Value.
         * @param valBytes Value bytes.
         */
        private UserCacheObjectImpl(Object val, byte[] valBytes) {
            super(val, valBytes);
        }

        /** {@inheritDoc} */
        @Nullable @Override public <T> T value(CacheObjectContext ctx, boolean cpy) {
            return super.value(ctx, false); // Do not need copy since user value is not in cache.
        }

        /** {@inheritDoc} */
        @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
            try {
                if (valBytes == null)
                    valBytes = ctx.processor().marshal(ctx, val);

                if (ctx.storeValue()) {
                    ClassLoader ldr = ctx.p2pEnabled() ?
                        IgniteUtils.detectClass(val).getClassLoader() : val.getClass().getClassLoader();

                    Object val = this.val != null && ctx.processor().immutable(this.val) ? this.val :
                        ctx.processor().unmarshal(ctx, valBytes, ldr);

                    return new CacheObjectImpl(val, valBytes);
                }

                return new CacheObjectImpl(null, valBytes);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to marshal object: " + val, e);
            }
        }
    }

    /**
     * Wraps value provided by user, must be serialized before stored in cache.
     * Used by classes that support fields indexing. Refer to {@link #isFieldsIndexingSupported(Class)}.
     */
    private static class UserCacheIndexedObjectImpl extends CacheIndexedObjectImpl {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         *
         */
        public UserCacheIndexedObjectImpl() {
            //No-op.
        }

        /**
         * @param val Value.
         * @param valBytes Value bytes.
         */
        private UserCacheIndexedObjectImpl(CacheObjectContext ctx, Object val, byte[] valBytes) {
            super(ctx, val, valBytes);
        }

        /** {@inheritDoc} */
        @Nullable @Override public <T> T value(CacheObjectContext ctx, boolean cpy) {
            return super.value(ctx, false); // Do not need copy since user value is not in cache.
        }

        /** {@inheritDoc} */
        @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
            try {
                toMarshaledFormIfNeeded();

                if (keepDeserialized(ctx, true)) {
                    ClassLoader ldr = ctx.p2pEnabled() ?
                        IgniteUtils.detectClass(val).getClassLoader() : val.getClass().getClassLoader();

                    Object val = this.val != null && ctx.processor().immutable(this.val) ? this.val :
                        ctx.processor().unmarshal(ctx, valBytes, start, len, ldr);

                    return new CacheIndexedObjectImpl(ctx, val, valBytes, start, len);
                }

                return new CacheIndexedObjectImpl(ctx, null, valBytes, start, len);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to marshal object: " + val, e);
            }
        }
    }

    /**
     * Wraps key provided by user, must be serialized before stored in cache.
     * Used by classes that support fields indexing. Refer to {@link #isFieldsIndexingSupported(Class)}.
     */
    private static class UserKeyCacheIndexedObjectImpl extends KeyCacheIndexedObjectImpl {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         *
         */
        public UserKeyCacheIndexedObjectImpl() {
            //No-op.
        }

        /**
         * @param key Key.
         */
        UserKeyCacheIndexedObjectImpl(CacheObjectContext ctx, Object key) {
            super(ctx, key, null);
        }

        /** {@inheritDoc} */
        @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
            try {
                if (!ctx.processor().immutable(val)) {
                    toMarshaledFormIfNeeded();

                    ClassLoader ldr = ctx.p2pEnabled() ?
                        IgniteUtils.detectClassLoader(IgniteUtils.detectClass(val)) : U.gridClassLoader();

                    Object val = ctx.processor().unmarshal(ctx, valBytes, start, len, ldr);

                    return new KeyCacheIndexedObjectImpl(ctx, val, valBytes, start, len);
                }

                return new KeyCacheIndexedObjectImpl(ctx, val, valBytes, start, len);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to marshal object: " + val, e);
            }
        }
    }

    /**
     * Wraps value provided by user, must be copied before stored in cache.
     */
    private static class UserCacheObjectByteArrayImpl extends CacheObjectByteArrayImpl {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         *
         */
        public UserCacheObjectByteArrayImpl() {
            // No-op.
        }

        /**
         * @param val Value.
         */
        private UserCacheObjectByteArrayImpl(byte[] val) {
            super(val);
        }

        /** {@inheritDoc} */
        @Nullable @Override public <T> T value(CacheObjectContext ctx, boolean cpy) {
            return super.value(ctx, false); // Do not need copy since user value is not in cache.
        }

        /** {@inheritDoc} */
        @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
            byte[] valCpy = Arrays.copyOf(val, val.length);

            return new CacheObjectByteArrayImpl(valCpy);
        }
    }
}
