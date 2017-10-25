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

package org.apache.ignite.internal.processors.query.h2.database;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCoordinatorVersion;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2RowLinkIO;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2SearchRow;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.processors.cache.mvcc.CacheCoordinatorsProcessor.assertMvccVersionValid;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheCoordinatorsProcessor.unmaskCoordinatorVersion;

/**
 *
 */
public class H2TreeMvccFilterClosure implements H2Tree.TreeRowClosure<GridH2SearchRow, GridH2Row> {
    /** */
    private final MvccCoordinatorVersion mvccVer;

    /**
     * @param mvccVer Mvcc version.
     */
    public H2TreeMvccFilterClosure(MvccCoordinatorVersion mvccVer) {
        assert mvccVer != null;

        this.mvccVer = mvccVer;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(BPlusTree<GridH2SearchRow, GridH2Row> tree,
        BPlusIO<GridH2SearchRow> io,
        long pageAddr,
        int idx)  throws IgniteCheckedException {
        H2RowLinkIO rowIo = (H2RowLinkIO)io;

        assert rowIo.storeMvccInfo() : rowIo;

        long rowCrdVer = rowIo.getMvccCoordinatorVersion(pageAddr, idx);

        assert unmaskCoordinatorVersion(rowCrdVer) == rowCrdVer : rowCrdVer;
        assert rowCrdVer > 0 : rowCrdVer;

        int cmp = Long.compare(mvccVer.coordinatorVersion(), rowCrdVer);

        if (cmp == 0) {
            long rowCntr = rowIo.getMvccCounter(pageAddr, idx);

            cmp = Long.compare(mvccVer.counter(), rowCntr);

            return cmp >= 0 &&
                !newVersionAvailable(rowIo, pageAddr, idx) &&
                !mvccVer.activeTransactions().contains(rowCntr);
        }
        else
            return cmp > 0;
    }

    /**
     * @param rowIo Row IO.
     * @param pageAddr Page address.
     * @param idx Item index.
     * @return {@code True}
     */
    private boolean newVersionAvailable(H2RowLinkIO rowIo, long pageAddr, int idx) {
        long newCrdVer = rowIo.getNewMvccCoordinatorVersion(pageAddr, idx);

        if (newCrdVer == 0)
            return false;

        int cmp = Long.compare(mvccVer.coordinatorVersion(), newCrdVer);

        if (cmp == 0) {
            long newCntr = rowIo.getNewMvccCounter(pageAddr, idx);

            assert assertMvccVersionValid(newCrdVer, newCntr);

            return newCntr <= mvccVer.counter() && !mvccVer.activeTransactions().contains(newCntr);
        }
        else
            return cmp < 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2TreeMvccFilterClosure.class, this);
    }
}
