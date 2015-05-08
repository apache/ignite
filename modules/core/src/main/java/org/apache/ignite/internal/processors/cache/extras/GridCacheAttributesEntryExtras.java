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
 * Extras where attributes are set.
 */
public class GridCacheAttributesEntryExtras extends GridCacheEntryExtrasAdapter {
    /** Attributes data. */
    private GridLeanMap<UUID, Object> attrData;

    /**
     * Constructor.
     *
     * @param attrData Attributes data.
     */
    public GridCacheAttributesEntryExtras(GridLeanMap<UUID, Object> attrData) {
        assert attrData != null;

        this.attrData = attrData;
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
            return null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras mvcc(GridCacheMvcc mvcc) {
        return mvcc != null ? new GridCacheAttributesMvccEntryExtras(attrData, mvcc) : this;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras obsoleteVersion(GridCacheVersion obsoleteVer) {
        return obsoleteVer != null ? new GridCacheAttributesObsoleteEntryExtras(attrData, obsoleteVer) : this;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryExtras ttlAndExpireTime(long ttl, long expireTime) {
        return ttl != 0 ? new GridCacheAttributesTtlEntryExtras(attrData, ttl, expireTime) : this;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 8;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAttributesEntryExtras.class, this);
    }
}
