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
 * Extras where attributes and obsolete version are set.
 */
public class GridCacheAttributesObsoleteEntryExtras extends GridCacheEntryExtrasAdapter {
    /** Obsolete version. */
    private GridCacheVersion obsoleteVer;

    /**
     * Constructor.
     *
     * @param obsoleteVer Obsolete version.
     */
    GridCacheAttributesObsoleteEntryExtras(GridCacheVersion obsoleteVer) {
        assert obsoleteVer != null;

        this.obsoleteVer = obsoleteVer;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras mvcc(GridCacheMvcc mvcc) {
        return mvcc != null ? new GridCacheAttributesMvccObsoleteEntryExtras(mvcc, obsoleteVer) : this;
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
            return new GridCacheAttributesEntryExtras();
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras ttlAndExpireTime(long ttl, long expireTime) {
        return expireTime != CU.EXPIRE_TIME_ETERNAL ? new GridCacheAttributesObsoleteTtlEntryExtras(obsoleteVer, ttl, expireTime) :
            this;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 8;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAttributesObsoleteEntryExtras.class, this);
    }
}