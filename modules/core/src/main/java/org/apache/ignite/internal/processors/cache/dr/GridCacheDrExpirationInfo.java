/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.dr;

import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Cache DR info used as argument in PUT cache internal interfaces with expiration info added.
 */
public class GridCacheDrExpirationInfo extends GridCacheDrInfo {
    /** */
    private static final long serialVersionUID = 0L;

    /** TTL. */
    private long ttl;

    /** Expire time. */
    private long expireTime;

    /**
     *
     */
    public GridCacheDrExpirationInfo() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param val Value.
     * @param ver Version.
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    public GridCacheDrExpirationInfo(CacheObject val, GridCacheVersion ver, long ttl, long expireTime) {
        super(val, ver);

        this.ttl = ttl;
        this.expireTime = expireTime;
    }

    /** {@inheritDoc} */
    @Override public long ttl() {
        return ttl;
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return expireTime;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheDrExpirationInfo.class, this);
    }
}