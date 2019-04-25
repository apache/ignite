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

package org.apache.ignite.internal.processors.cache.tree.mvcc.data;

import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.tree.AbstractDataInnerIO;
import org.apache.ignite.lang.IgniteInClosure;

/**
 *
 */
public final class MvccDataInnerIO extends AbstractDataInnerIO {
    /** */
    public static final IOVersions<MvccDataInnerIO> VERSIONS = new IOVersions<>(
        new MvccDataInnerIO(1)
    );

    /**
     * @param ver Page format version.
     */
    private MvccDataInnerIO(int ver) {
        super(T_DATA_REF_MVCC_INNER, ver, true, 32);
    }

    /** {@inheritDoc} */
    @Override public void visit(long pageAddr, IgniteInClosure<CacheSearchRow> c) {
        int cnt = getCount(pageAddr);

        for (int i = 0; i < cnt; i++)
            c.apply(new MvccDataRow(getLink(pageAddr, i)));
    }

    /** {@inheritDoc} */
    @Override protected boolean storeMvccVersion() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getMvccCoordinatorVersion(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx) + 12);
    }

    /** {@inheritDoc} */
    @Override public long getMvccCounter(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx) + 20);
    }

    /** {@inheritDoc} */
    @Override public int getMvccOperationCounter(long pageAddr, int idx) {
        return PageUtils.getInt(pageAddr, offset(idx) + 28);
    }
}
