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
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Extras where MVCC, obsolete version and TTL are set.
 */
public class GridCacheMvccObsoleteTtlEntryExtras extends GridCacheEntryExtrasAdapter {
    /** MVCC. */
    private GridCacheMvcc mvcc;

    /** Obsolete version. */
    private GridCacheVersion obsoleteVer;

    /** TTL. */
    private long ttl;

    /** Expire time. */
    private long expireTime;

    /**
     * Constructor.
     *
     * @param mvcc MVCC.
     * @param obsoleteVer Obsolete version.
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    GridCacheMvccObsoleteTtlEntryExtras(GridCacheMvcc mvcc, GridCacheVersion obsoleteVer, long ttl,
        long expireTime) {
        assert mvcc != null;
        assert obsoleteVer != null;
        assert expireTime != CU.EXPIRE_TIME_ETERNAL;

        this.mvcc = mvcc;
        this.obsoleteVer = obsoleteVer;
        this.ttl = ttl;
        this.expireTime = expireTime;
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvcc mvcc() {
        return mvcc;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras mvcc(GridCacheMvcc mvcc) {
        if (mvcc != null) {
            this.mvcc = mvcc;

            return this;
        }
        else
            return new GridCacheObsoleteTtlEntryExtras(obsoleteVer, ttl, expireTime);
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
            return new GridCacheMvccTtlEntryExtras(mvcc, ttl, expireTime);
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
            return new GridCacheMvccObsoleteEntryExtras(mvcc, obsoleteVer);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 32;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheMvccObsoleteTtlEntryExtras.class, this);
    }
}
