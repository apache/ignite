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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.CacheIdAware;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Cache object and version.
 */
public class CacheVersionedValue implements Message, CacheIdAware {
    /** Value. */
    @Order(0)
    @GridToStringInclude
    CacheObject val;

    /** Cache version. */
    @Order(1)
    @GridToStringInclude
    GridCacheVersion ver;
    
    /** */
    private int cacheId;

    /** */
    public CacheVersionedValue() {
        // No-op.
    }

    /**
     * @param val Cache value.
     * @param ver Cache version.
     */
    public CacheVersionedValue(CacheObject val, GridCacheVersion ver, int cacheId) {
        this.val = val;
        this.ver = ver;
        this.cacheId = cacheId;
    }

    /**
     * @return Cache version.
     */
    public GridCacheVersion version() {
        return ver;
    }

    /**
     * @return Cache object.
     */
    public CacheObject value() {
        return val;
    }


    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheVersionedValue.class, this);
    }

    /** {@inheritDoc} */
    @Override public int cacheId() {
        return cacheId;
    }
}
