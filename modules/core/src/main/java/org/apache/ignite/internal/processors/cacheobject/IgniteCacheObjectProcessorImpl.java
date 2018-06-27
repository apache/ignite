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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.UUID;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectByteArrayImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDefaultAffinityKeyMapper;
import org.apache.ignite.internal.processors.cache.IncompleteCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IgniteCacheObjectProcessorImpl extends GridProcessorAdapter implements IgniteCacheObjectProcessor {
    /** Immutable classes. */
    private static final Collection<Class<?>> IMMUTABLE_CLS = new HashSet<>();

    /** */
    private IgniteBinary noOpBinary = new NoOpBinary();

    /*
     * Static initializer
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
    @Override public byte[] marshal(CacheObjectValueContext ctx, Object val) throws IgniteCheckedException {
        return CU.marshal(ctx.kernalContext().cache().context(), ctx.addDeploymentInfo(), val);
    }

    /** {@inheritDoc} */
    @Override public Object unmarshal(CacheObjectValueContext ctx, byte[] bytes, ClassLoader clsLdr)
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
    @Override public KeyCacheObject toKeyCacheObject(CacheObjectContext ctx, byte type, byte[] bytes) throws IgniteCheckedException {
        switch (type) {
            case CacheObject.TYPE_BYTE_ARR:
                throw new IllegalArgumentException("Byte arrays cannot be used as cache keys.");

            case CacheObject.TYPE_REGULAR:
                return new KeyCacheObjectImpl(ctx.kernalContext().cacheObjects().unmarshal(ctx, bytes, null), bytes, -1);
        }

        throw new IllegalArgumentException("Invalid object type: " + type);
    }

    /** {@inheritDoc} */
    @Override public CacheObject toCacheObject(CacheObjectContext ctx, ByteBuffer buf) {
        int len = buf.getInt();

        assert len >= 0 : len;

        byte type = buf.get();

        byte[] data = new byte[len];

        buf.get(data);

        return toCacheObject(ctx, type, data);
    }

    /** {@inheritDoc} */
    @Override public IncompleteCacheObject toCacheObject(
        final CacheObjectContext ctx,
        final ByteBuffer buf,
        @Nullable IncompleteCacheObject incompleteObj
    ) throws IgniteCheckedException {
        if (incompleteObj == null)
            incompleteObj = new IncompleteCacheObject(buf);

        if (incompleteObj.isReady())
            return incompleteObj;

        incompleteObj.readData(buf);

        if (incompleteObj.isReady())
            incompleteObj.object(toCacheObject(ctx, incompleteObj.type(), incompleteObj.data()));

        return incompleteObj;
    }

    /** {@inheritDoc} */
    @Override public IncompleteCacheObject toKeyCacheObject(
        final CacheObjectContext ctx,
        final ByteBuffer buf,
        @Nullable IncompleteCacheObject incompleteObj
    ) throws IgniteCheckedException {
        if (incompleteObj == null)
            incompleteObj = new IncompleteCacheObject(buf);

        if (incompleteObj.isReady())
            return incompleteObj;

        incompleteObj.readData(buf);

        if (incompleteObj.isReady())
            incompleteObj.object(toKeyCacheObject(ctx, incompleteObj.type(), incompleteObj.data()));

        return incompleteObj;
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject toCacheObject(CacheObjectContext ctx,
        @Nullable Object obj,
        boolean userObj) {
        return toCacheObject(ctx, obj, userObj, false);
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject toCacheObject(CacheObjectContext ctx, @Nullable Object obj, boolean userObj,
        boolean failIfUnregistered) {
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
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to get partition", e);

            return  -1;
        }
    }

    /** {@inheritDoc} */
    @Override public CacheObjectContext contextForCache(CacheConfiguration ccfg) throws IgniteCheckedException {
        assert ccfg != null;

        boolean storeVal = !ccfg.isCopyOnRead() || (!isBinaryEnabled(ccfg) &&
            (QueryUtils.isEnabled(ccfg) || ctx.config().isPeerClassLoadingEnabled()));

        CacheObjectContext res = new CacheObjectContext(ctx,
            ccfg.getName(),
            ccfg.getAffinityMapper() != null ? ccfg.getAffinityMapper() : new GridCacheDefaultAffinityKeyMapper(),
            ccfg.isCopyOnRead(),
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

}
