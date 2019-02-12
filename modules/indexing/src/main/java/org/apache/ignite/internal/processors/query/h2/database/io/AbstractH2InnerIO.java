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
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
import org.apache.ignite.internal.processors.query.h2.opt.H2CacheRow;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;

/**
 * Inner page for H2 row references.
 */
public abstract class AbstractH2InnerIO extends BPlusInnerIO<H2Row> implements H2RowLinkIO {
    /**
     * @param type Page type.
     * @param ver Page format version.
     * @param itemSize Single item size on page.
     */
    AbstractH2InnerIO(int type, int ver, int itemSize) {
        super(type, ver, true, itemSize);
    }

    /** {@inheritDoc} */
    @Override public void storeByOffset(long pageAddr, int off, H2Row row) {
        H2CacheRow row0 = (H2CacheRow)row;

        H2IOUtils.storeRow(row0, pageAddr, off, storeMvccInfo());
    }

    /** {@inheritDoc} */
    @Override public H2Row getLookupRow(BPlusTree<H2Row, ?> tree, long pageAddr, int idx)
        throws IgniteCheckedException {
        long link = getLink(pageAddr, idx);

        if (storeMvccInfo()) {
            long mvccCrdVer = getMvccCoordinatorVersion(pageAddr, idx);
            long mvccCntr = getMvccCounter(pageAddr, idx);
            int mvccOpCntr = getMvccOperationCounter(pageAddr, idx);

            return ((H2Tree)tree).createMvccRow(link, mvccCrdVer, mvccCntr, mvccOpCntr);
        }

        return ((H2Tree)tree).createRow(link);
    }

    /** {@inheritDoc} */
    @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<H2Row> srcIo, long srcPageAddr, int srcIdx) {
        H2IOUtils.store(dstPageAddr, offset(dstIdx), srcIo, srcPageAddr, srcIdx, storeMvccInfo());
    }

    /** {@inheritDoc} */
    @Override public long getLink(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx));
    }
}
