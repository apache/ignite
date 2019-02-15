/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2.database.io;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
import org.apache.ignite.internal.processors.query.h2.opt.H2CacheRow;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;

/**
 * Leaf page for H2 row references.
 */
public abstract class AbstractH2LeafIO extends BPlusLeafIO<H2Row> implements H2RowLinkIO {
    /**
     * @param type Page type.
     * @param ver Page format version.
     * @param itemSize Single item size on page.
     */
    AbstractH2LeafIO(int type, int ver, int itemSize) {
        super(type, ver, itemSize);
    }

    /** {@inheritDoc} */
    @Override public final void storeByOffset(long pageAddr, int off, H2Row row) {
        H2CacheRow row0 = (H2CacheRow)row;

        H2IOUtils.storeRow(row0, pageAddr, off, storeMvccInfo());
    }

    /** {@inheritDoc} */
    @Override public final void store(long dstPageAddr, int dstIdx, BPlusIO<H2Row> srcIo, long srcPageAddr, int srcIdx) {
        assert srcIo == this;

        H2IOUtils.store(dstPageAddr, offset(dstIdx), srcIo, srcPageAddr, srcIdx, storeMvccInfo());
    }

    /** {@inheritDoc} */
    @Override public final H2Row getLookupRow(BPlusTree<H2Row,?> tree, long pageAddr, int idx)
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
    @Override public long getLink(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx));
    }
}
