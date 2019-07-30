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

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.Nullable;

/**
 * Data row implementation that can optionally hide cache identifier and set {@code null} as value.
 */
public class DataRowStoreAware extends DataRow {
    /** Store cache identifier flag. */
    private boolean storeCacheId;

    /**
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     * @param part Partition.
     * @param expireTime Expire time.
     * @param cacheId Cache ID.
     * @param storeCacheId Store cache ididentifier flag.
     */
    public DataRowStoreAware(KeyCacheObject key, @Nullable CacheObject val, GridCacheVersion ver, int part,
        long expireTime, int cacheId, boolean storeCacheId) {
        super(key, val, ver, part, expireTime, cacheId);

        storeCacheId(storeCacheId);
    }

    /**
     * @param storeCacheId Store cache identifier flag.
     */
    public void storeCacheId(boolean storeCacheId) {
        this.storeCacheId = storeCacheId;
    }

    /** {@inheritDoc} */
    @Override public int cacheId() {
        return storeCacheId ? cacheId : CU.UNDEFINED_CACHE_ID;
    }

    /** {@inheritDoc} */
    @Override public @Nullable CacheObject value() {
        return val;
    }
}
