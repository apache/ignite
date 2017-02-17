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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.UUID;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectByteArrayImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDefaultAffinityKeyMapper;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_VALUES;

/**
 *
 */
public class IgniteCacheObjectProcessorImpl extends GridProcessorAdapter implements IgniteCacheObjectProcessor {
    /** Immutable classes. */
    private static final Collection<Class<?>> IMMUTABLE_CLS = new HashSet<>();

    /** */
    private IgniteBinary noOpBinary = new NoOpBinary();

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
    @Override public String affinityField(String keyType) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteBinary binary() {
        return noOpBinary;
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject prepareForCache(@Nullable CacheObject obj, GridCacheContext cctx) {
        if (obj == null)
            return null;

        return obj.prepareForCache(cctx.cacheObjectContext());
    }

    /** {@inheritDoc} */
    @Override public byte[] marshal(CacheObjectContext ctx, Object val) throws IgniteCheckedException {
        return CU.marshal(ctx.kernalContext().cache().context(), ctx.addDeploymentInfo(), val);
    }

    /** {@inheritDoc} */
    @Override public Object unmarshal(CacheObjectContext ctx, byte[] bytes, ClassLoader clsLdr)
        throws IgniteCheckedException {
        return U.unmarshal(ctx.kernalContext(), bytes, U.resolveClassLoader(clsLdr, ctx.kernalContext().config()));
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject toCacheKeyObject(CacheObjectContext ctx,
        @Nullable GridCacheContext cctx,
        Object obj,
        boolean userObj) {
        if (obj instanceof KeyCacheObject) {
            KeyCacheObject key = (KeyCacheObject)obj;

            if (key.partition() == -1)
                // Assume all KeyCacheObjects except BinaryObject can not be reused for another cache.
                key.partition(partition(ctx, cctx, key));

            return (KeyCacheObject)obj;
        }

        return toCacheKeyObject0(ctx, cctx, obj, userObj);
    }

    /**
     * @param obj Object.
     * @param userObj If {@code true} then given object is object provided by user and should be copied
     *        before stored in cache.
     * @return Key cache object.
     */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    protected KeyCacheObject toCacheKeyObject0(CacheObjectContext ctx,
        @Nullable GridCacheContext cctx,
        Object obj,
        boolean userObj) {
        int part = partition(ctx, cctx, obj);

        if (!userObj)
            return new KeyCacheObjectImpl(obj, null, part);

        return new UserKeyCacheObjectImpl(obj, part);
    }

    /** {@inheritDoc} */
    @Override public CacheObject toCacheObject(GridCacheContext ctx, long valPtr, boolean tmp)
        throws IgniteCheckedException {
        assert valPtr != 0;

        int size = GridUnsafe.getInt(valPtr);

        byte type = GridUnsafe.getByte(valPtr + 4);

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
        boolean userObj) {
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
            return new CacheObjectImpl(obj, null);

        return new UserCacheObjectImpl(obj, null);
    }

    /**
     * @param ctx Cache objects context.
     * @param cctx Cache context.
     * @param obj Object.
     * @return Object partition.
     */
    protected final int partition(CacheObjectContext ctx, @Nullable GridCacheContext cctx, Object obj) {
        try {
            return cctx != null ?
                cctx.affinity().partition(obj, false) :
                ctx.kernalContext().affinity().partition0(ctx.cacheName(), obj, null);
        }
        catch (IgniteCheckedException ignored) {
            U.error(log, "Failed to get partition");

            return  -1;
        }
    }

    /** {@inheritDoc} */
    @Override public CacheObjectContext contextForCache(CacheConfiguration ccfg) throws IgniteCheckedException {
        assert ccfg != null;

        CacheMemoryMode memMode = ccfg.getMemoryMode();

        boolean storeVal = !ccfg.isCopyOnRead() || (!isBinaryEnabled(ccfg) &&
            (GridQueryProcessor.isEnabled(ccfg) || ctx.config().isPeerClassLoadingEnabled()));

        CacheObjectContext res = new CacheObjectContext(ctx,
            ccfg.getName(),
            ccfg.getAffinityMapper() != null ? ccfg.getAffinityMapper() : new GridCacheDefaultAffinityKeyMapper(),
            ccfg.isCopyOnRead() && memMode != OFFHEAP_VALUES,
            storeVal,
            ctx.config().isPeerClassLoadingEnabled() && !isBinaryEnabled(ccfg));

        ctx.resource().injectGeneric(res.defaultAffMapper());

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean immutable(Object obj) {
        assert obj != null;

        return IMMUTABLE_CLS.contains(obj.getClass());
    }

    /** {@inheritDoc} */
    @Override public void onContinuousProcessorStarted(GridKernalContext ctx) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onUtilityCacheStarted() throws IgniteCheckedException {
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
    @Override public boolean isBinaryObject(Object obj) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isBinaryEnabled(CacheConfiguration<?, ?> ccfg) {
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
            this(key, -1);
        }

        /**
         * @param key Key.
         */
        UserKeyCacheObjectImpl(Object key, int part) {
            super(key, null, part);
        }

        /**
         * @param key Key.
         */
        UserKeyCacheObjectImpl(Object key, byte[] valBytes, int part) {
            super(key, valBytes, part);
        }

        /** {@inheritDoc} */
        @Override public KeyCacheObject copy(int part) {
            if (this.partition() == part)
                return this;

            return new UserKeyCacheObjectImpl(val, valBytes, part);
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
