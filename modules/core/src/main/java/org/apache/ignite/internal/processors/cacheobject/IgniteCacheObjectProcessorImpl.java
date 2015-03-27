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
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.math.*;
import java.util.*;

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
    private final GridBoundedConcurrentLinkedHashMap<Class<?>, Boolean> reflectionCache =
            new GridBoundedConcurrentLinkedHashMap<>(1024, 1024);

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
    @Nullable public KeyCacheObject toCacheKeyObject(CacheObjectContext ctx, Object obj, boolean userObj) {
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
            return new KeyCacheObjectImpl(obj, null);

        return new KeyCacheObjectImpl(obj, null) {
            @Nullable @Override public <T> T value(CacheObjectContext ctx, boolean cpy) {
                return super.value(ctx, false);  // Do not need copy since user value is not in cache.
            }

            @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
                try {
                    if (!ctx.processor().immutable(val)) {
                        if (valBytes == null)
                            valBytes = ctx.processor().marshal(ctx, val);

                        ClassLoader ldr = ctx.p2pEnabled() ?
                            IgniteUtils.detectClass(this.val).getClassLoader() : val.getClass().getClassLoader();

                        Object val = ctx.processor().unmarshal(ctx,
                            valBytes,
                            ldr);

                        return new KeyCacheObjectImpl(val, valBytes);
                    }

                    return new KeyCacheObjectImpl(val, valBytes);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException("Failed to marshal object: " + val, e);
                }
            }
        };
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
        }

        throw new IllegalArgumentException("Invalid object type: " + type);
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject toCacheObject(CacheObjectContext ctx,
        @Nullable Object obj,
        boolean userObj)
    {
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

            return new CacheObjectByteArrayImpl((byte[]) obj) {
                @Nullable @Override public <T> T value(CacheObjectContext ctx, boolean cpy) {
                    return super.value(ctx, false); // Do not need copy since user value is not in cache.
                }

                @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
                    byte[] valCpy = Arrays.copyOf(val, val.length);

                    return new CacheObjectByteArrayImpl(valCpy);
                }
            };
        }

        if (!userObj)
            return new CacheObjectImpl(obj, null);

        return new IgniteCacheObjectImpl(obj, null);
    }

    /** {@inheritDoc} */
    @Override public CacheObjectContext contextForCache(ClusterNode node, @Nullable String cacheName,
        @Nullable CacheConfiguration ccfg) {
        if (ccfg != null) {
            CacheMemoryMode memMode = ccfg.getMemoryMode();

            return new CacheObjectContext(ctx,
                new GridCacheDefaultAffinityKeyMapper(),
                ccfg.isCopyOnRead() && memMode == ONHEAP_TIERED,
                ctx.config().isPeerClassLoadingEnabled() || GridQueryProcessor.isEnabled(ccfg));
        }
        else
            return new CacheObjectContext(
                ctx,
                new GridCacheDefaultAffinityKeyMapper(),
                false,
                ctx.config().isPeerClassLoadingEnabled());
    }

    /** {@inheritDoc} */
    @Override public boolean immutable(Object obj) {
        assert obj != null;

        Class<?> cls = obj.getClass();

        if (IMMUTABLE_CLS.contains(cls))
            return true;

        Boolean immutable = reflectionCache.get(cls);

        if (immutable != null)
            return immutable;

        immutable = IgniteUtils.hasAnnotation(cls, IgniteImmutable.class);

        reflectionCache.putIfAbsent(cls, immutable);

        return immutable;
    }

    /** {@inheritDoc} */
    @Override public boolean keepPortableInStore(@Nullable String cacheName) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void onCacheProcessorStarted() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int typeId(String typeName) {
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
    @Override public int typeId(Object obj) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public Object field(Object obj, String fieldName) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean hasField(Object obj, String fieldName) {
        return false;
    }

    /**
     *
     */
    private static class IgniteCacheObjectImpl extends CacheObjectImpl {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         *
         */
        public IgniteCacheObjectImpl() {
            //No-op.
        }

        /**
         *
         */
        public IgniteCacheObjectImpl(Object val, byte[] valBytes) {
            super(val, valBytes);
        }

        /** {@inheritDoc} */
        @Nullable @Override public <T> T value(CacheObjectContext ctx, boolean cpy) {
            return super.value(ctx, false); // Do not need copy since user value is not in cache.
        }

        /** {@inheritDoc} */
        @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
            if (!ctx.processor().immutable(val)) {
                try {
                    if (valBytes == null)
                        valBytes = ctx.processor().marshal(ctx, val);

                    if (ctx.unmarshalValues()) {
                        ClassLoader ldr = ctx.p2pEnabled() ?
                            IgniteUtils.detectClass(this.val).getClassLoader() : val.getClass().getClassLoader();

                        Object val = ctx.processor().unmarshal(ctx, valBytes, ldr);

                        return new CacheObjectImpl(val, valBytes);
                    }

                    return new CacheObjectImpl(null, valBytes);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException("Failed to marshal object: " + val, e);
                }
            } else
                return new CacheObjectImpl(val, valBytes);
        }
    }
}
