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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.UnregisteredBinaryTypeException;
import org.apache.ignite.internal.UnregisteredClassException;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Return value for cases where both, value and success flag need to be returned.
 */
public final class GridCacheReturn implements Message {
    /** Value. */
    @GridToStringInclude(sensitive = true)
    private volatile Object v;

    /** Cache object. */
    @Order(value = 0, method = "cacheObject")
    private @Nullable CacheObject cacheObj;

    /** Invoke direct results. */
    @Order(value = 1, method = "invokeDirectResults")
    private @Nullable Collection<CacheInvokeDirectResult> invokeResCol;

    /** Success flag. */
    @Order(2)
    private volatile boolean success;

    /** Invoke result flag. */
    @Order(value = 3, method = "invokeResult")
    private volatile boolean invokeRes;

    /** Local result flag, if non local then do not need unwrap cache objects. */
    private boolean loc;

    /** Cache Id. */
    @Order(4)
    private int cacheId;

    /**
     * Empty constructor.
     */
    public GridCacheReturn() {
        loc = true;
    }

    /**
     * @param loc {@code True} if created on the node initiated cache operation.
     */
    public GridCacheReturn(boolean loc) {
        this.loc = loc;
    }

    /**
     * @param loc {@code True} if created on the node initiated cache operation.
     * @param success Success flag.
     */
    public GridCacheReturn(boolean loc, boolean success) {
        this.loc = loc;
        this.success = success;
    }

    /**
     * @param cctx Cache context.
     * @param loc {@code True} if created on the node initiated cache operation.
     * @param keepBinary True is deserialize value from a binary representation, false otherwise.
     * @param ldr Class loader, used for deserialization from binary representation.
     * @param v Value.
     * @param success Success flag.
     */
    public GridCacheReturn(
        GridCacheContext<?, ?> cctx,
        boolean loc,
        boolean keepBinary,
        @Nullable ClassLoader ldr,
        Object v,
        boolean success
    ) {
        this.loc = loc;
        this.success = success;

        if (v != null) {
            if (v instanceof CacheObject)
                initValue(cctx, (CacheObject)v, keepBinary, ldr);
            else {
                assert loc;

                this.v = v;
            }
        }
    }

    /**
     * @return Value.
     */
    @Nullable public <V> V value() {
        return (V)v;
    }

    /**
     *
     */
    public boolean emptyResult() {
        return !invokeRes && v == null && cacheObj == null && success;
    }

    /**
     * @return If return is invoke result.
     */
    public boolean invokeResult() {
        return invokeRes;
    }

    /**
     * @param invokeRes Invoke result flag.
     */
    public void invokeResult(boolean invokeRes) {
        this.invokeRes = invokeRes;
    }

    /**
     * @param cctx Cache context.
     * @param v Value.
     * @param keepBinary Keep binary flag.
     * @param ldr Class loader, used for deserialization from binary representation.
     * @return This instance for chaining.
     */
    public GridCacheReturn value(GridCacheContext<?, ?> cctx, CacheObject v, boolean keepBinary, @Nullable ClassLoader ldr) {
        initValue(cctx, v, keepBinary, ldr);

        return this;
    }

    /**
     * @return Success flag.
     */
    public boolean success() {
        return success;
    }

    /**
     * @param cctx Cache context.
     * @param cacheObj Value to set.
     * @param success Success flag to set.
     * @param keepBinary Keep binary flag.
     * @param ldr Class loader, used for deserialization from binary representation.
     * @return This instance for chaining.
     */
    public GridCacheReturn set(
        GridCacheContext<?, ?> cctx,
        @Nullable CacheObject cacheObj,
        boolean success,
        boolean keepBinary,
        @Nullable ClassLoader ldr
    ) {
        this.success = success;

        initValue(cctx, cacheObj, keepBinary, ldr);

        return this;
    }

    /**
     * @param cctx Cache context.
     * @param cacheObj Cache object.
     * @param keepBinary Keep binary flag.
     * @param ldr Class loader, used for deserialization from binary representation.
     */
    private void initValue(
        GridCacheContext<?, ?> cctx,
        @Nullable CacheObject cacheObj,
        boolean keepBinary,
        @Nullable ClassLoader ldr
    ) {
        if (loc)
            v = cctx.cacheObjectContext().unwrapBinaryIfNeeded(cacheObj, keepBinary, true, ldr);
        else {
            assert cacheId == 0 || cacheId == cctx.cacheId();

            cacheId = cctx.cacheId();

            this.cacheObj = cacheObj;
        }
    }

    /**
     * @param success Success flag.
     * @return This instance for chaining.
     */
    public GridCacheReturn success(boolean success) {
        this.success = success;

        return this;
    }

    /**
     * @param cctx Context.
     * @param key Key.
     * @param key0 Key value.
     * @param res Result.
     * @param err Error.
     * @param keepBinary Keep binary.
     */
    public synchronized void addEntryProcessResult(
        GridCacheContext<?, ?> cctx,
        KeyCacheObject key,
        @Nullable Object key0,
        @Nullable Object res,
        @Nullable Exception err,
        boolean keepBinary) {
        assert v == null || v instanceof Map : v;
        assert key != null;
        assert res != null || err != null;

        invokeRes = true;

        if (loc) {
            HashMap<Object, EntryProcessorResult<?>> resMap = (HashMap<Object, EntryProcessorResult<?>>)v;

            if (resMap == null) {
                resMap = new HashMap<>();

                v = resMap;
            }

            // These exceptions mean that we should register class and call EntryProcessor again.
            if (err != null) {
                if (err instanceof UnregisteredClassException)
                    throw (UnregisteredClassException)err;
                else if (err instanceof UnregisteredBinaryTypeException)
                    throw (UnregisteredBinaryTypeException)err;
            }

            CacheInvokeResult<?> res0 = err == null ? CacheInvokeResult.fromResult(res) : CacheInvokeResult.fromError(err);

            Object resKey = key0 != null ? key0 :
                ((keepBinary && key instanceof BinaryObject) ? key : CU.value(key, cctx, true));

            resMap.put(resKey, res0);
        }
        else {
            assert v == null;
            assert cacheId == 0 || cacheId == cctx.cacheId();

            cacheId = cctx.cacheId();

            if (invokeResCol == null)
                invokeResCol = new ArrayList<>();

            CacheInvokeDirectResult res0 = err == null ?
                cctx.transactional() ?
                    new CacheInvokeDirectResult(key, cctx.toCacheObject(res)) :
                    CacheInvokeDirectResult.lazyResult(key, res) :
                new CacheInvokeDirectResult(key, err);

            invokeResCol.add(res0);
        }
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @param cacheId Cache ID.
     */
    public void cacheId(int cacheId) {
        this.cacheId = cacheId;
    }

    /**
     * @return Cache object.
     */
    public @Nullable CacheObject cacheObject() {
        return cacheObj;
    }

    /**
     * @param cacheObj Cache object.
     */
    public void cacheObject(@Nullable CacheObject cacheObj) {
        this.cacheObj = cacheObj;
    }

    /**
     * @return Invoke direct results.
     */
    public @Nullable Collection<CacheInvokeDirectResult> invokeDirectResults() {
        return invokeResCol;
    }

    /**
     * @param invokeResCol Invoke direct results.
     */
    public void invokeDirectResults(@Nullable Collection<CacheInvokeDirectResult> invokeResCol) {
        this.invokeResCol = invokeResCol;
    }

    /**
     * @param other Other result to merge with.
     */
    public synchronized void mergeEntryProcessResults(GridCacheReturn other) {
        assert invokeRes || v == null : "Invalid state to merge: " + this;
        assert other.invokeRes;
        assert loc == other.loc : loc;

        if (other.v == null)
            return;

        invokeRes = true;

        HashMap<Object, EntryProcessorResult<?>> resMap = (HashMap<Object, EntryProcessorResult<?>>)v;

        if (resMap == null) {
            resMap = new HashMap<>();

            v = resMap;
        }

        resMap.putAll((Map<Object, EntryProcessorResult<?>>)other.v);
    }

    /**
     * Converts entry processor invokation results to cache object instances.
     *
     * @param ctx Cache context.
     */
    public void marshalResult(GridCacheContext<?, ?> ctx) {
        if (invokeRes && invokeResCol != null) {
            for (CacheInvokeDirectResult directRes : invokeResCol)
                directRes.marshalResult(ctx);
        }
    }

    /**
     * @param ctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    public void prepareMarshal(GridCacheContext<?, ?> ctx) throws IgniteCheckedException {
        assert !loc;

        if (cacheObj != null)
            cacheObj.prepareMarshal(ctx.cacheObjectContext());

        if (invokeRes && invokeResCol != null) {
            for (CacheInvokeDirectResult res : invokeResCol)
                res.prepareMarshal(ctx);
        }
    }

    /**
     * @param ctx Cache context.
     * @param ldr Class loader.
     * @throws IgniteCheckedException If failed.
     */
    public void finishUnmarshal(GridCacheContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        loc = true;

        if (cacheObj != null) {
            cacheObj.finishUnmarshal(ctx.cacheObjectContext(), ldr);

            v = ctx.cacheObjectContext().unwrapBinaryIfNeeded(cacheObj, true, false, ldr);
        }

        if (invokeRes && invokeResCol != null) {
            for (CacheInvokeDirectResult res : invokeResCol)
                res.finishUnmarshal(ctx, ldr);

            Map<Object, CacheInvokeResult<?>> map0 = U.newHashMap(invokeResCol.size());

            for (CacheInvokeDirectResult res : invokeResCol) {
                CacheInvokeResult<?> res0 = res.error() == null ?
                    CacheInvokeResult.fromResult(ctx.cacheObjectContext().unwrapBinaryIfNeeded(res.result(), true, false, null)) :
                    CacheInvokeResult.fromError(res.error());

                map0.put(ctx.cacheObjectContext().unwrapBinaryIfNeeded(res.key(), true, false, null), res0);
            }

            v = map0;
        }
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 88;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheReturn.class, this);
    }
}
