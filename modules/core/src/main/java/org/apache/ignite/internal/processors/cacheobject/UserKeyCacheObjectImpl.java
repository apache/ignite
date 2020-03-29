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
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Wraps key provided by user, must be serialized before stored in cache.
 */
public class UserKeyCacheObjectImpl extends KeyCacheObjectImpl {
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
     * @param part Partition.
     */
    public UserKeyCacheObjectImpl(Object key, int part) {
        super(key, null, part);
    }

    /**
     * @param key Key.
     * @param valBytes Marshalled key.
     * @param part Partition.
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
            IgniteCacheObjectProcessor proc = ctx.kernalContext().cacheObjects();

            if (!proc.immutable(val)) {
                if (valBytes == null)
                    valBytes = proc.marshal(ctx, val);

                boolean p2pEnabled = ctx.kernalContext().config().isPeerClassLoadingEnabled();

                ClassLoader ldr = p2pEnabled ?
                    IgniteUtils.detectClassLoader(IgniteUtils.detectClass(this.val)) : U.gridClassLoader();

                Object val = proc.unmarshal(ctx, valBytes, ldr);

                KeyCacheObject key = new KeyCacheObjectImpl(val, valBytes, partition());

                key.partition(partition());

                return key;
            }

            KeyCacheObject key = new KeyCacheObjectImpl(val, valBytes, partition());

            key.partition(partition());

            return key;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to marshal object: " + val, e);
        }
    }
}
