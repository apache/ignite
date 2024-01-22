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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectTransformerUtils;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.jetbrains.annotations.Nullable;

/**
 * Wraps value provided by platform, must be transformed before stored in cache.
 */
public class PlatformCacheObjectImpl extends CacheObjectImpl {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridDirectTransient
    private byte[] arr;

    /**
     *
     */
    public PlatformCacheObjectImpl() {
        //No-op.
    }

    /**
     * @param val Value.
     * @param arr Value bytes.
     */
    public PlatformCacheObjectImpl(Object val, byte[] arr) {
        super(val, null);

        this.arr = arr;
    }

    /** {@inheritDoc} */
    @Override public <T> @Nullable T value(CacheObjectValueContext ctx, boolean cpy, ClassLoader ldr) {
        if (valBytes == null)
            valBytes = valueBytesFromArray(ctx);

        return super.value(ctx, cpy, ldr);
    }

    /** {@inheritDoc} */
    @Override public byte[] rawBytes(CacheObjectValueContext ctx) throws IgniteCheckedException {
        if (arr != null)
            return arr;

        assert valBytes != null;

        return CacheObjectTransformerUtils.restoreIfNecessary(arr, ctx);
    }

    /** {@inheritDoc} */
    @Override public byte[] valueBytes(CacheObjectValueContext ctx) throws IgniteCheckedException {
        if (valBytes == null)
            valBytes = valueBytesFromArray(ctx);

        return valBytes;
    }

    /**
     * @return Value bytes.
     */
    private byte[] valueBytesFromArray(CacheObjectValueContext ctx) {
        assert arr != null;

        return CacheObjectTransformerUtils.transformIfNecessary(arr, 0, arr.length, ctx);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(CacheObjectValueContext ctx) throws IgniteCheckedException {
        if (valBytes == null)
            valBytes = valueBytesFromArray(ctx);

        super.prepareMarshal(ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(CacheObjectValueContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        if (valBytes == null)
            valBytes = valueBytesFromArray(ctx);

        super.finishUnmarshal(ctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
        if (valBytes == null)
            valBytes = valueBytesFromArray(ctx);

        return this;
    }
}
