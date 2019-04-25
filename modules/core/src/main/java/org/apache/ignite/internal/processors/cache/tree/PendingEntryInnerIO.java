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

package org.apache.ignite.internal.processors.cache.tree;

import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 *
 */
public final class PendingEntryInnerIO extends AbstractPendingEntryInnerIO {
    /** */
    public static final IOVersions<PendingEntryInnerIO> VERSIONS = new IOVersions<>(
        new PendingEntryInnerIO(1)
    );

    /**
     * @param ver Page format version.
     */
    private PendingEntryInnerIO(int ver) {
        super(T_PENDING_REF_INNER, ver, true, 16);
    }

    /** {@inheritDoc} */
    @Override public int getCacheId(long pageAddr, int idx) {
        return CU.UNDEFINED_CACHE_ID;
    }

    /** {@inheritDoc} */
    @Override protected boolean storeCacheId() {
        return false;
    }
}
