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

import java.util.*;

/**
 * Cache object wrapping object provided by user. Need to be copied before stored in cache.
 */
public class UserCacheObjectImpl extends CacheObjectImpl {
    /**
     * @param val Value.
     */
    public UserCacheObjectImpl(Object val) {
        super(val, null);
    }

    /**
     *
     */
    public UserCacheObjectImpl() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public CacheObject prepareForCache(GridCacheContext ctx) {
        if (needCopy(ctx)) {
            if (val instanceof byte[]) {
                byte[] byteArr = (byte[])val;

                return new CacheObjectImpl(Arrays.copyOf(byteArr, byteArr.length), null);
            }
            else {
                try {
                    if (valBytes == null)
                        valBytes = ctx.portable().marshal(ctx.cacheObjectContext(), val);

                    return new CacheObjectImpl(null, valBytes);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException("Failed to marshal object: " + val, e);
                }
            }
        }
        else
            return new CacheObjectImpl(val, valBytes);
    }
}
