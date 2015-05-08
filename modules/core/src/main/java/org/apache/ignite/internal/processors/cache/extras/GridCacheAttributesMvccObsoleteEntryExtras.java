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

import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Extras where attributes, MVCC and obsolete version are set.
 */
public class GridCacheAttributesMvccObsoleteEntryExtras extends GridCacheEntryExtrasAdapter {
    /** Attributes data. */
    private GridLeanMap<UUID, Object> attrData;

    /** MVCC. */
    private GridCacheMvcc mvcc;

    /** Obsolete version. */
    private GridCacheVersion obsoleteVer;

    /**
     * Constructor.
     *
     * @param attrData Attributes data.
     * @param mvcc MVCC.
     * @param obsoleteVer Obsolete version.
     */
    public GridCacheAttributesMvccObsoleteEntryExtras(GridLeanMap<UUID, Object> attrData, GridCacheMvcc mvcc,
        GridCacheVersion obsoleteVer) {
        assert attrData != null;
        assert mvcc != null;
        assert obsoleteVer != null;

        this.attrData = attrData;
        this.mvcc = mvcc;
        this.obsoleteVer = obsoleteVer;
    }

    /** {@inheritDoc} */
    @Override public GridLeanMap<UUID, Object> attributesData() {
        return attrData;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras attributesData(@Nullable GridLeanMap<UUID, Object> attrData) {
        if (attrData != null) {
            this.attrData = attrData;

            return this;
        }
        else
            return new GridCacheMvccObsoleteEntryExtras(mvcc, obsoleteVer);
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvcc mvcc() {
        return mvcc;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras mvcc(@Nullable GridCacheMvcc mvcc) {
        if (mvcc != null) {
            this.mvcc = mvcc;

            return this;
        }
        else
            return new GridCacheAttributesObsoleteEntryExtras(attrData, obsoleteVer);
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
            return new GridCacheAttributesMvccEntryExtras(attrData, mvcc);
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras ttlAndExpireTime(long ttl, long expireTime) {
        return ttl != 0 ? new GridCacheAttributesMvccObsoleteTtlEntryExtras(attrData, mvcc, obsoleteVer, ttl,
            expireTime) : this;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 24;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAttributesMvccObsoleteEntryExtras.class, this);
    }
}
