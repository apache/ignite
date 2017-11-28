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
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Wraps value provided by user, must be serialized before stored in cache.
 */
public class UserCacheObjectImpl extends CacheObjectImpl {
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
    @Nullable @Override public <T> T value(CacheObjectValueContext ctx, boolean cpy) {
        return super.value(ctx, false); // Do not need copy since user value is not in cache.
    }

    /** {@inheritDoc} */
    @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
        try {
            IgniteCacheObjectProcessor proc = ctx.kernalContext().cacheObjects();

            if (valBytes == null)
                valBytes = proc.marshal(ctx, val);

            if (ctx.storeValue()) {
                boolean p2pEnabled = ctx.kernalContext().config().isPeerClassLoadingEnabled();

                ClassLoader ldr = p2pEnabled ?
                    IgniteUtils.detectClass(this.val).getClassLoader() : val.getClass().getClassLoader();

                Object val = this.val != null && proc.immutable(this.val) ? this.val :
                    proc.unmarshal(ctx, valBytes, ldr);

                return new CacheObjectImpl(val, valBytes);
            }

            return new CacheObjectImpl(null, valBytes);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to marshal object: " + val, e);
        }
    }
}
