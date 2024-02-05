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
     * @param arr Value bytes.
     */
    public PlatformCacheObjectImpl(byte[] arr) {
        assert arr != null;

        this.arr = arr;
    }

    /** {@inheritDoc} */
    @Override public byte[] rawValueBytes(CacheObjectValueContext ctx) {
        return arr;
    }

    /** {@inheritDoc} */
    @Override protected byte[] getValueBytes(CacheObjectValueContext ctx) {
        return CacheObjectTransformerUtils.transformIfNecessary(arr, ctx);
    }

    /** {@inheritDoc} */
    @Override protected Object getValue(CacheObjectValueContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        return ctx.kernalContext().cacheObjects().unmarshal(ctx, arr, ldr);
    }

    /** {@inheritDoc} */
    @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
        if (valBytes == null)
            valBytes = getValueBytes(ctx);

        return this;
    }
}
