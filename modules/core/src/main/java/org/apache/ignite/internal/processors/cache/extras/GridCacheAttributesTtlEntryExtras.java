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

package org.apache.ignite.internal.processors.cache.extras;

import org.apache.ignite.internal.processors.cache.GridCacheMvcc;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Extras where attributes and TTL are set.
 */
public class GridCacheAttributesTtlEntryExtras extends GridCacheEntryExtrasAdapter {
    /** TTL. */
    private long ttl;

    /** Expire time. */
    private long expireTime;

    /**
     * Constructor.
     *
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    GridCacheAttributesTtlEntryExtras(long ttl, long expireTime) {
        assert expireTime != CU.EXPIRE_TIME_ETERNAL;

        this.ttl = ttl;
        this.expireTime = expireTime;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras mvcc(GridCacheMvcc mvcc) {
        return mvcc != null ? new GridCacheAttributesMvccTtlEntryExtras(mvcc, ttl, expireTime) : this;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras obsoleteVersion(GridCacheVersion obsoleteVer) {
        return obsoleteVer != null ? new GridCacheAttributesObsoleteTtlEntryExtras(obsoleteVer, ttl,
            expireTime) : this;
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
    @Override public GridCacheEntryExtras ttlAndExpireTime(long ttl, long expireTime) {
        if (expireTime != CU.EXPIRE_TIME_ETERNAL) {
            this.ttl = ttl;
            this.expireTime = expireTime;

            return this;
        }
        else
            return new GridCacheAttributesEntryExtras();
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 16;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAttributesTtlEntryExtras.class, this);
    }
}