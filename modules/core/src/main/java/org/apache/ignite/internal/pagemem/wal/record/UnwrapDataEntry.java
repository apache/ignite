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

package org.apache.ignite.internal.pagemem.wal.record;

import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Data Entry for automatic unwrapping key and value from Data Entry
 */
public class UnwrapDataEntry extends DataEntry {
    private final CacheObjectContext cacheObjCtx;

    /**
     * @param cacheId Cache ID.
     * @param key Key.
     * @param val Value.
     * @param op Operation.
     * @param nearXidVer Near transaction version.
     * @param writeVer Write version.
     * @param expireTime Expire time.
     * @param partId Partition ID.
     * @param partCnt Partition counter.
     * @param cacheObjCtx
     */
    public UnwrapDataEntry(int cacheId, KeyCacheObject key,
        CacheObject val,
        GridCacheOperation op,
        GridCacheVersion nearXidVer,
        GridCacheVersion writeVer, long expireTime, int partId,
        long partCnt,
        CacheObjectContext cacheObjCtx) {
        super(cacheId, key, val, op, nearXidVer, writeVer, expireTime, partId, partCnt);
        this.cacheObjCtx = cacheObjCtx;
    }

    /**
     * Unwraps key value from cache key object into primitive boxed type or source class. If client classes were used
     * in key, call of this method requires classes to be available in classpath
     *
     * @return Key which was placed into cache.
     */
    public Object unwrappedKey() {
        return key().value(cacheObjCtx, false);
    }

    /**
     * Unwraps value value from cache value object into primitive boxed type or source class. If client classes were
     * used in key, call of this method requires classes to be available in classpath
     *
     * @return Value which was placed into cache.
     */
    public Object unwrappedValue() {
        return value().value(cacheObjCtx, false);
    }
}
