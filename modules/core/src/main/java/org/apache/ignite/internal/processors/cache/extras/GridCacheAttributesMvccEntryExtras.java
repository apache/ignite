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
 * Extras where attributes and MVCC are set.
 */
public class GridCacheAttributesMvccEntryExtras extends GridCacheEntryExtrasAdapter {
    /** Attributes data. */
    private GridLeanMap<UUID, Object> attrData;

    /** MVCC. */
    private GridCacheMvcc mvcc;

    /**
     * Constructor.
     *
     * @param attrData Attributes data.
     * @param mvcc MVCC.
     */
    public GridCacheAttributesMvccEntryExtras(GridLeanMap<UUID, Object> attrData, GridCacheMvcc mvcc) {
        assert attrData != null;
        assert mvcc != null;

        this.attrData = attrData;
        this.mvcc = mvcc;
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
            return new GridCacheMvccEntryExtras(mvcc);
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
            return new GridCacheAttributesEntryExtras(attrData);
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras obsoleteVersion(GridCacheVersion obsoleteVer) {
        return obsoleteVer != null ? new GridCacheAttributesMvccObsoleteEntryExtras(attrData, mvcc, obsoleteVer) :
            this;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras ttlAndExpireTime(long ttl, long expireTime) {
        return ttl != 0 ? new GridCacheAttributesMvccTtlEntryExtras(attrData, mvcc, ttl, expireTime) : this;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 16;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAttributesMvccEntryExtras.class, this);
    }
}
