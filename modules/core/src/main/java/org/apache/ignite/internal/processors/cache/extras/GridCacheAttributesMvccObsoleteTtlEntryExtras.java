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
 * Extras where attributes, MVCC, obsolete version and TTL are set.
 */
public class GridCacheAttributesMvccObsoleteTtlEntryExtras<K> extends GridCacheEntryExtrasAdapter<K> {
    /** Attributes data. */
    private GridLeanMap<UUID, Object> attrData;

    /** MVCC. */
    private GridCacheMvcc<K> mvcc;

    /** Obsolete version. */
    private GridCacheVersion obsoleteVer;

    /** TTL. */
    private long ttl;

    /** Expire time. */
    private long expireTime;

    /**
     * Constructor.
     *
     * @param attrData Attributes data.
     * @param mvcc MVCC.
     * @param obsoleteVer Obsolete version.
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    public GridCacheAttributesMvccObsoleteTtlEntryExtras(GridLeanMap<UUID, Object> attrData, GridCacheMvcc<K> mvcc,
        GridCacheVersion obsoleteVer, long ttl, long expireTime) {
        assert attrData != null;
        assert mvcc != null;
        assert obsoleteVer != null;
        assert ttl != 0;

        this.attrData = attrData;
        this.mvcc = mvcc;
        this.obsoleteVer = obsoleteVer;
        this.ttl = ttl;
        this.expireTime = expireTime;
    }

    /** {@inheritDoc} */
    @Override public GridLeanMap<UUID, Object> attributesData() {
        return attrData;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras<K> attributesData(@Nullable GridLeanMap<UUID, Object> attrData) {
        if (attrData != null) {
            this.attrData = attrData;

            return this;
        }
        else
            return new GridCacheMvccObsoleteTtlEntryExtras<>(mvcc, obsoleteVer, ttl ,expireTime);
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvcc<K> mvcc() {
        return mvcc;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras<K> mvcc(@Nullable GridCacheMvcc<K> mvcc) {
        if (mvcc != null) {
            this.mvcc = mvcc;

            return this;
        }
        else
            return new GridCacheAttributesObsoleteTtlEntryExtras<>(attrData, obsoleteVer, ttl, expireTime);
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion obsoleteVersion() {
        return obsoleteVer;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras<K> obsoleteVersion(GridCacheVersion obsoleteVer) {
        if (obsoleteVer != null) {
            this.obsoleteVer = obsoleteVer;

            return this;
        }
        else
            return new GridCacheAttributesMvccTtlEntryExtras<>(attrData, mvcc, ttl, expireTime);
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
    @Override public GridCacheEntryExtras<K> ttlAndExpireTime(long ttl, long expireTime) {
        if (ttl != 0) {
            this.ttl = ttl;
            this.expireTime = expireTime;

            return this;
        }
        else
            return new GridCacheAttributesMvccObsoleteEntryExtras<>(attrData, mvcc, obsoleteVer);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 40;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAttributesMvccObsoleteTtlEntryExtras.class, this);
    }
}
