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

package org.apache.ignite.internal.processors.cache.tree.mvcc.search;

import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.tree.SearchRow;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Search row for minimum key version.
 */
public class MvccMinSearchRow extends SearchRow {
    /**
     * @param cacheId Cache ID.
     * @param key Key.
     */
    public MvccMinSearchRow(int cacheId, KeyCacheObject key) {
        super(cacheId, key);
    }

    /** {@inheritDoc} */
    @Override public long mvccCoordinatorVersion() {
        return 1L;
    }

    /** {@inheritDoc} */
    @Override public long mvccCounter() {
        return 1L;
    }

    /** {@inheritDoc} */
    @Override public int mvccOperationCounter() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccMinSearchRow.class, this);
    }
}
