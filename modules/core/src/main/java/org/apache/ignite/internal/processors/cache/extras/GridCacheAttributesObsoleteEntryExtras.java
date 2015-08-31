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
    public GridCacheAttributesObsoleteEntryExtras(GridCacheVersion obsoleteVer) {
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
        return ttl != 0 ? new GridCacheAttributesObsoleteTtlEntryExtras(obsoleteVer, ttl, expireTime) :
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