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

/**
 * Cache object wrapping key provided by user. Need to be copied before stored in cache.
 */
public class UserKeyCacheObjectImpl extends KeyCacheObjectImpl {
    /**
     * @param val Key value.
     */
    public UserKeyCacheObjectImpl(Object val) {
        super(val, null);
    }

    /**
     *
     */
    public UserKeyCacheObjectImpl() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public CacheObject prepareForCache(GridCacheContext ctx) {
        if (needCopy(ctx)) {
            try {
                if (valBytes == null)
                    valBytes = ctx.marshaller().marshal(val);

                return new KeyCacheObjectImpl(ctx.marshaller().unmarshal(valBytes, ctx.deploy().globalLoader()), valBytes);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to marshal object: " + val, e);
            }
        }
        else
            return new KeyCacheObjectImpl(val, valBytes);
    }
}
