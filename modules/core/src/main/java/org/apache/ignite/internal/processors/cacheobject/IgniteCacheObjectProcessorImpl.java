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

import java.math.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheMemoryMode.*;

/**
 *
 */
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
    private final ConcurrentHashMap<Integer, OptimizedObjectMetadata> metaBuf = new ConcurrentHashMap<>();

    /** */
    private final CountDownLatch startLatch = new CountDownLatch(1);

    /** */
    private OptimizedMarshallerExt optMarshExt;

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

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        Marshaller marsh = ctx.config().getMarshaller();

        if (marsh instanceof OptimizedMarshallerExt) {
            optMarshExt = (OptimizedMarshallerExt)marsh;

            OptimizedMarshallerMetaHandler metaHandler = new OptimizedMarshallerMetaHandler() {
                @Override public void addMeta(int typeId, OptimizedObjectMetadata meta) {
                    if (metaBuf.contains(typeId))
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

            optMarshExt.setMetadataHandler(metaHandler);
        }
    }

    /** {@inheritDoc} */
    @Override public void onUtilityCacheStarted() throws IgniteCheckedException {
        metaDataCache = ctx.cache().jcache(CU.UTILITY_CACHE_NAME);

        startLatch.countDown();

        for (Map.Entry<Integer, OptimizedObjectMetadata> e : metaBuf.entrySet())
            metaDataCache.putIfAbsent(new OptimizedObjectMetadataKey(e.getKey()), e.getValue());
    }

    /**
     * @param ctx Context.
     */
    public IgniteCacheObjectProcessorImpl(GridKernalContext ctx) {
        super(ctx);
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
        if (optMarshExt != null)
            return optMarshExt.unmarshal(bytes, off, len, clsLdr);

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

        return toCacheKeyObject0(obj, userObj);
    }

    /**
     * @param obj Object.
     * @param userObj If {@code true} then given object is object provided by user and should be copied
     *        before stored in cache.
     * @return Key cache object.
     */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    protected KeyCacheObject toCacheKeyObject0(Object obj, boolean userObj) {
        if (!userObj)
            return isFieldsIndexingEnabled(obj.getClass()) ? new KeyCacheIndexedObjectImpl(obj, null) :
                new KeyCacheObjectImpl(obj, null);

        return isFieldsIndexingEnabled(obj.getClass()) ? new UserKeyCacheIndexedObjectImpl(obj) :
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
                return new CacheIndexedObjectImpl(bytes, 0, bytes.length);
        }

        throw new IllegalArgumentException("Invalid object type: " + type);
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject toCacheObject(CacheObjectContext ctx, @Nullable Object obj, boolean userObj){
        if (obj == null || obj instanceof CacheObject)
            return (CacheObject)obj;

        return toCacheObject0(obj, userObj);
    }

    /**
     * @param obj Object.
     * @param userObj If {@code true} then given object is object provided by user and should be copied
     *        before stored in cache.
     * @return Cache object.
     */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    protected CacheObject toCacheObject0(@Nullable Object obj, boolean userObj) {
        assert obj != null;

        if (obj instanceof byte[]) {
            if (!userObj)
                return new CacheObjectByteArrayImpl((byte[])obj);

            return new UserCacheObjectByteArrayImpl((byte[])obj);
        }

        if (!userObj)
            return isFieldsIndexingEnabled(obj.getClass()) ? new CacheIndexedObjectImpl(obj) :
                new CacheObjectImpl(obj, null);

        return isFieldsIndexingEnabled(obj.getClass()) ? new UserCacheIndexedObjectImpl(obj, null) :
            new UserCacheObjectImpl(obj, null);
    }

    /** {@inheritDoc} */
    @Override public CacheObjectContext contextForCache(CacheConfiguration ccfg) throws IgniteCheckedException {
        assert ccfg != null;

        CacheMemoryMode memMode = ccfg.getMemoryMode();

        boolean storeVal = ctx.config().isPeerClassLoadingEnabled() ||
            GridQueryProcessor.isEnabled(ccfg) ||
            !ccfg.isCopyOnRead();

        CacheObjectContext res = new CacheObjectContext(ctx,
            ccfg.getAffinityMapper() != null ? ccfg.getAffinityMapper() : new GridCacheDefaultAffinityKeyMapper(),
            ccfg.isCopyOnRead() && memMode != OFFHEAP_VALUES,
            storeVal,
            GridCacheUtils.isSystemCache(ccfg.getName()));

        ctx.resource().injectGeneric(res.defaultAffMapper());

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean immutable(Object obj) {
        assert obj != null;

        return IMMUTABLE_CLS.contains(obj.getClass());
    }

    /** {@inheritDoc} */
    @Override public int typeId(String typeName) {
        return optMarshExt != null ? OptimizedMarshallerUtils.resolveTypeId(typeName, optMarshExt.idMapper()) : 0;
    }

    /** {@inheritDoc} */
    @Override public int typeId(Object obj) {
        if (obj instanceof CacheIndexedObjectImpl)
            return ((CacheIndexedObjectImpl)obj).typeId();

        return 0;
    }

    /** {@inheritDoc} */
    @Override public Object unwrapTemporary(GridCacheContext ctx, Object obj) throws IgniteException {
        return obj;
    }

    /** {@inheritDoc} */
    @Override public boolean isPortableObject(Object obj) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isPortableEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Object field(Object obj, String fieldName) throws IgniteFieldNotFoundException {
        assert optMarshExt != null;

        try {
            return ((CacheIndexedObjectImpl)obj).field(fieldName, optMarshExt);
        }
        catch (IgniteFieldNotFoundException e) {
            throw e;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        catch (ClassCastException e) {
            throw new IgniteFieldNotFoundException("Object doesn't have field [obj=" + obj + ", field=" + fieldName
                + "]");
        }
    }

    /** {@inheritDoc} */
    @Override public boolean hasField(Object obj, String fieldName) {
        if (obj instanceof CacheIndexedObjectImpl) {
            assert optMarshExt != null;

            try {
                return ((CacheIndexedObjectImpl)obj).hasField(fieldName, optMarshExt);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isFieldsIndexingEnabled() {
        return optMarshExt != null;
    }

    /** {@inheritDoc} */
    @Override public boolean isFieldsIndexingEnabled(Class<?> cls) {
        return optMarshExt != null && optMarshExt.fieldsIndexingEnabled(cls);
    }

    /** {@inheritDoc} */
    @Override public boolean enableFieldsIndexing(Class<?> cls) throws IgniteCheckedException {
        return optMarshExt != null && optMarshExt.enableFieldsIndexing(cls);
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
                        IgniteUtils.detectClassLoader(IgniteUtils.detectClass(this.val)) : U.gridClassLoader();

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
        public UserCacheObjectImpl(Object val, byte[] valBytes) {
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
                        IgniteUtils.detectClass(this.val).getClassLoader() : val.getClass().getClassLoader();

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
     * Used by classes that support fields indexing. Refer to {@link #isFieldsIndexingEnabled(Class)}.
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
        public UserCacheIndexedObjectImpl(Object val, byte[] valBytes) {
            super(val, valBytes);
        }

        /** {@inheritDoc} */
        @Nullable @Override public <T> T value(CacheObjectContext ctx, boolean cpy) {
            return super.value(ctx, false); // Do not need copy since user value is not in cache.
        }

        /** {@inheritDoc} */
        @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
            try {
                toMarshaledFormIfNeeded(ctx);

                if (ctx.storeValue()) {
                    ClassLoader ldr = ctx.p2pEnabled() ?
                        IgniteUtils.detectClass(this.val).getClassLoader() : val.getClass().getClassLoader();

                    Object val = this.val != null && ctx.processor().immutable(this.val) ? this.val :
                        ctx.processor().unmarshal(ctx, valBytes, start, len, ldr);

                    return new CacheIndexedObjectImpl(val, valBytes, start, len);
                }

                return new CacheIndexedObjectImpl(null, valBytes, start, len);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to marshal object: " + val, e);
            }
        }
    }

    /**
     * Wraps key provided by user, must be serialized before stored in cache.
     * Used by classes that support fields indexing. Refer to {@link #isFieldsIndexingEnabled(Class)}.
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
        UserKeyCacheIndexedObjectImpl(Object key) {
            super(key, null);
        }

        /** {@inheritDoc} */
        @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
            try {
                if (!ctx.processor().immutable(val)) {
                    toMarshaledFormIfNeeded(ctx);

                    ClassLoader ldr = ctx.p2pEnabled() ?
                        IgniteUtils.detectClassLoader(IgniteUtils.detectClass(this.val)) : U.gridClassLoader();

                    Object val = ctx.processor().unmarshal(ctx, valBytes, start, len, ldr);

                    return new KeyCacheIndexedObjectImpl(val, valBytes, start, len);
                }

                return new KeyCacheIndexedObjectImpl(val, valBytes, start, len);
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
        public UserCacheObjectByteArrayImpl(byte[] val) {
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
