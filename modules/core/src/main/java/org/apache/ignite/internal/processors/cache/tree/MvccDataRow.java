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

import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.processors.cache.mvcc.CacheCoordinatorsProcessor.assertMvccVersionValid;

/**
 *
 */
public class MvccDataRow extends DataRow {
    /** */
    private long crdVer;

    /** */
    private long mvccCntr;

    /**
     *
     */
    private MvccDataRow() {
        // No-op.
    }

    /**
     * @param grp Context.
     * @param hash Key hash.
     * @param link Link.
     * @param part Partition number.
     * @param rowData Data.
     * @param crdVer Mvcc coordinator version.
     * @param mvccCntr Mvcc counter.
     */
    public MvccDataRow(CacheGroupContext grp, int hash, long link, int part, RowData rowData, long crdVer, long mvccCntr) {
        super(grp, hash, link, part, rowData);

        assertMvccVersionValid(crdVer, mvccCntr);

        this.crdVer = crdVer;
        this.mvccCntr = mvccCntr;
    }

    /**
     * @param link Link.
     * @param part Partition.
     * @param cacheId Cache ID.
     * @param crdVer Mvcc coordinator version.
     * @param mvccCntr Mvcc counter.
     * @return Row.
     */
    static MvccDataRow removedRowNoKey(
        long link,
        int part,
        int cacheId,
        long crdVer,
        long mvccCntr) {
        MvccDataRow row = new MvccDataRow();

        row.link = link;
        row.cacheId = cacheId;
        row.part = part;
        row.crdVer = crdVer;
        row.mvccCntr = mvccCntr;

        return row;
    }

    /** {@inheritDoc} */
    @Override public long mvccCoordinatorVersion() {
        return crdVer;
    }

    /** {@inheritDoc} */
    @Override public long mvccCounter() {
        return mvccCntr;
    }

    /** {@inheritDoc} */
    @Override public void mvccVersion(long crdVer, long mvccCntr) {
        this.crdVer = crdVer;
        this.mvccCntr = mvccCntr;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccDataRow.class, this, "super", super.toString());
    }
}
