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

package org.apache.ignite.internal.processors.cache.tree;

import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;

/**
 *
 */
public final class CacheIdAwarePendingEntryInnerIO extends AbstractPendingEntryInnerIO {
    /** */
    public static final IOVersions<CacheIdAwarePendingEntryInnerIO> VERSIONS = new IOVersions<>(
        new CacheIdAwarePendingEntryInnerIO(1)
    );

    /**
     * @param ver Page format version.
     */
    private CacheIdAwarePendingEntryInnerIO(int ver) {
        super(T_CACHE_ID_AWARE_PENDING_REF_INNER, ver, true, 20);
    }

    /** {@inheritDoc} */
    @Override public int getCacheId(long pageAddr, int idx) {
        return PageUtils.getInt(pageAddr, offset(idx) + 16);
    }

    /** {@inheritDoc} */
    @Override protected boolean storeCacheId() {
        return true;
    }
}
