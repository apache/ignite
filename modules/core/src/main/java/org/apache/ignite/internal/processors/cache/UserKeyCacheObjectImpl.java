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

import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.internal.*;

/**
 * Cache object wrapping key provided by user. Need to be copied before stored in cache.
 */
public class UserKeyCacheObjectImpl extends KeyCacheObjectImpl {
    /**
     * @param val Key value.
     * @param bytes Bytes.
     */
    public UserKeyCacheObjectImpl(Object val, byte[] bytes) {
        super(val, bytes);
    }

    /**
     *
     */
    public UserKeyCacheObjectImpl() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public byte[] valueBytes(CacheObjectContext ctx) throws IgniteCheckedException {
        if (valBytes == null)
            valBytes = ctx.processor().marshal(ctx, val);

        return valBytes;
    }

    /** {@inheritDoc} */
    @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
        try {
            if (valBytes == null)
                valBytes = ctx.processor().marshal(ctx, val);

            if (needCopy(ctx)) {
                Object val = ctx.processor().unmarshal(ctx,
                    valBytes,
                    ctx.kernalContext().config().getClassLoader());

                return new KeyCacheObjectImpl(val, valBytes);
            }

            return new KeyCacheObjectImpl(val, valBytes);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to marshal object: " + val, e);
        }
    }
}
