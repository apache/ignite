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

package org.apache.ignite.internal.processors.cache.tree.mvcc.data;

import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.tree.AbstractDataLeafIO;

/**
 *
 */
public final class MvccCacheIdAwareDataLeafIO extends AbstractDataLeafIO {
    /** */
    public static final IOVersions<MvccCacheIdAwareDataLeafIO> VERSIONS = new IOVersions<>(
        new MvccCacheIdAwareDataLeafIO(1)
    );

    /**
     * @param ver Page format version.
     */
    private MvccCacheIdAwareDataLeafIO(int ver) {
        super(T_CACHE_ID_DATA_REF_MVCC_LEAF, ver, 52);
    }

    /** {@inheritDoc} */
    @Override protected boolean storeCacheId() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected boolean storeMvccVersion() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public int getCacheId(long pageAddr, int idx) {
        return PageUtils.getInt(pageAddr, offset(idx) + 12);
    }

    /** {@inheritDoc} */
    @Override public long getMvccCoordinatorVersion(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx) + 16);
    }

    /** {@inheritDoc} */
    @Override public long getMvccCounter(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx) + 24);
    }

    /** {@inheritDoc} */
    @Override public int getMvccOperationCounter(long pageAddr, int idx) {
        return PageUtils.getInt(pageAddr, offset(idx) + 32);
    }

    /** {@inheritDoc} */
    @Override public long getMvccLockCoordinatorVersion(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx) + 36);
    }

    /** {@inheritDoc} */
    @Override public long getMvccLockCounter(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx) + 44);
    }

    /** {@inheritDoc} */
    @Override public void setMvccLockCoordinatorVersion(long pageAddr, int idx, long lockCrd) {
        PageUtils.putLong(pageAddr, offset(idx) + 36, lockCrd);
    }

    /** {@inheritDoc} */
    @Override public void setMvccLockCounter(long pageAddr, int idx, long lockCntr) {
        PageUtils.putLong(pageAddr, offset(idx) + 44, lockCntr);
    }
}
