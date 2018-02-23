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

package org.apache.ignite.internal.processors.query.h2.database.io;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.h2.result.SearchRow;

/**
 * Inner page for H2 row references.
 */
public class H2InnerIO extends BPlusInnerIO<SearchRow> implements H2RowLinkIO {
    /** */
    public static final IOVersions<H2InnerIO> VERSIONS = new IOVersions<>(
        new H2InnerIO(1)
    );

    /**
     * @param ver Page format version.
     */
    private H2InnerIO(int ver) {
        super(T_H2_REF_INNER, ver, true, 8);
    }

    /** {@inheritDoc} */
    @Override public void storeByOffset(long pageAddr, int off, SearchRow row) {
        GridH2Row row0 = (GridH2Row)row;

        assert row0.link() != 0;

        PageUtils.putLong(pageAddr, off, row0.link());
    }

    /** {@inheritDoc} */
    @Override public SearchRow getLookupRow(BPlusTree<SearchRow,?> tree, long pageAddr, int idx)
        throws IgniteCheckedException {
        long link = getLink(pageAddr, idx);

        return ((H2Tree)tree).createRowFromLink(link);
    }

    /** {@inheritDoc} */
    @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<SearchRow> srcIo, long srcPageAddr, int srcIdx) {
        long link = ((H2RowLinkIO)srcIo).getLink(srcPageAddr, srcIdx);

        PageUtils.putLong(dstPageAddr, offset(dstIdx), link);
    }

    /** {@inheritDoc} */
    @Override public long getLink(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx));
    }
}
