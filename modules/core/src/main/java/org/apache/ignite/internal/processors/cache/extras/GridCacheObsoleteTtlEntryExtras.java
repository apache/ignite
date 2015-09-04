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

package org.apache.ignite.internal.processors.cache.extras;

import org.apache.ignite.internal.processors.cache.GridCacheMvcc;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Extras where obsolete version and TTL are set.
 */
public class GridCacheObsoleteTtlEntryExtras extends GridCacheEntryExtrasAdapter {
    /** Obsolete version. */
    private GridCacheVersion obsoleteVer;

    /** TTL. */
    private long ttl;

    /** Expire time. */
    private long expireTime;

    /**
     * Constructor.
     *
     * @param obsoleteVer Obsolete version.
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    public GridCacheObsoleteTtlEntryExtras(GridCacheVersion obsoleteVer, long ttl, long expireTime) {
        assert obsoleteVer != null;
        assert ttl != 0;

        this.obsoleteVer = obsoleteVer;
        this.ttl = ttl;
        this.expireTime = expireTime;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras mvcc(GridCacheMvcc mvcc) {
        return mvcc != null ? new GridCacheMvccObsoleteTtlEntryExtras(mvcc, obsoleteVer, ttl, expireTime) : this;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion obsoleteVersion() {
        return obsoleteVer;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras obsoleteVersion(GridCacheVersion obsoleteVer) {
        if (obsoleteVer != null) {
            this.obsoleteVer = obsoleteVer;

            return this;
        }
        else
            return new GridCacheTtlEntryExtras(ttl, expireTime);
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
        if (ttl != 0) {
            this.ttl = ttl;
            this.expireTime = expireTime;

            return this;
        }
        else
            return new GridCacheObsoleteEntryExtras(obsoleteVer);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 24;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheObsoleteTtlEntryExtras.class, this);
    }
}