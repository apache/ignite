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

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CacheInvokeDirectResult implements Message {
    /** Cache key. */
    @Order(0)
    KeyCacheObject key;

    /** */
    @GridToStringInclude
    private Object unprepareRes;

    /** Result. */
    @GridToStringInclude
    @Order(1)
    CacheObject res;

    /** Error message. */
    @GridToStringInclude(sensitive = true)
    @Order(value = 2, method = "errorMessage")
    ErrorMessage errMsg;

    /**
     * Default constructor.
     */
    public CacheInvokeDirectResult() {
        // No-op.
    }

    /**
     * @param key Key.
     * @param res Result.
     */
    public CacheInvokeDirectResult(KeyCacheObject key, CacheObject res) {
        this.key = key;
        this.res = res;
    }

    /**
     * Constructs CacheInvokeDirectResult with unprepared res, to avoid object marshaling while holding topology locks.
     *
     * @param key Key.
     * @param res Result.
     * @return a new instance of CacheInvokeDirectResult.
     */
    static CacheInvokeDirectResult lazyResult(KeyCacheObject key, Object res) {
        CacheInvokeDirectResult res0 = new CacheInvokeDirectResult();

        res0.key = key;
        res0.unprepareRes = res;

        return res0;
    }

    /**
     * @param key Key.
     * @param err Exception thrown by {@link EntryProcessor#process(MutableEntry, Object...)}.
     */
    public CacheInvokeDirectResult(KeyCacheObject key, Throwable err) {
        this.key = key;
        errMsg = new ErrorMessage(err);
    }

    /**
     * @return Key.
     */
    public KeyCacheObject key() {
        return key;
    }

    /**
     * @param key Key.
     */
    public void key(KeyCacheObject key) {
        this.key = key;
    }

    /**
     * @return Result.
     */
    public CacheObject result() {
        return res;
    }

    /**
     * @param res Result.
     */
    public void result(CacheObject res) {
        this.res = res;
    }

    /**
     * @return Error.
     */
    @Nullable public Throwable error() {
        return ErrorMessage.error(errMsg);
    }

    /**
     * @return Error message.
     */
    public ErrorMessage errorMessage() {
        return errMsg;
    }

    /**
     * @param errMsg Error message.
     */
    public void errorMessage(ErrorMessage errMsg) {
        this.errMsg = errMsg;
    }

    /**
     * @param ctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    public void prepareMarshal(GridCacheContext<?, ?> ctx) throws IgniteCheckedException {
        key.prepareMarshal(ctx.cacheObjectContext());

        assert unprepareRes == null : "marshalResult() was not called for the result: " + this;

        if (res != null)
            res.prepareMarshal(ctx.cacheObjectContext());
    }

    /**
     * Converts the entry processor unprepared result to a cache object instance.
     *
     * @param ctx Cache context.
     */
    public void marshalResult(GridCacheContext<?, ?> ctx) {
        try {
            if (unprepareRes != null)
                res = ctx.toCacheObject(unprepareRes);
        }
        finally {
            unprepareRes = null;
        }
    }

    /**
     * @param ctx Cache context.
     * @param ldr Class loader.
     * @throws IgniteCheckedException If failed.
     */
    public void finishUnmarshal(GridCacheContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        key.finishUnmarshal(ctx.cacheObjectContext(), ldr);

        if (res != null)
            res.finishUnmarshal(ctx.cacheObjectContext(), ldr);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 93;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheInvokeDirectResult.class, this);
    }
}
