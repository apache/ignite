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
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cacheobject.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.resources.*;

/**
 *
 */
public class CacheIndexedObjectDefaultAffinityMapper extends GridCacheDefaultAffinityKeyMapper {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name resource. */
    @CacheNameResource
    private String cacheName;

    /** Cache object context. */
    private transient CacheObjectContext objCtx;

    /** {@inheritDoc} */
    @Override public Object affinityKey(Object key) {
        IgniteEx kernal = (IgniteEx)ignite;

        if (objCtx == null)
            objCtx = kernal.context().cache().cache(cacheName).context().cacheObjectContext();

        IgniteCacheObjectProcessor proc = kernal.context().cacheObjects();

        try {
            key = proc.toCacheKeyObject(objCtx, key, true);
        }
        catch (IgniteException e) {
            U.error(log, "Failed to marshal key to portable: " + key, e);
        }

        if (proc.isIndexedObject(key))
            return proc.affinityKey((CacheIndexedObject)key);
        else
            return super.affinityKey(key);
    }
}
