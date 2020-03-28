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
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.RowStore;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccDataRow;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 *
 */
public class CacheDataRowStore extends RowStore {
    /** Whether version should be skipped. */
    private static ThreadLocal<Boolean> SKIP_VER = ThreadLocal.withInitial(() -> false);

    /**
     * @return Skip version flag.
     */
    public static boolean getSkipVersion() {
        return SKIP_VER.get();
    }

    /**
     * @param skipVer Skip version flag.
     */
    public static void setSkipVersion(boolean skipVer) {
        SKIP_VER.set(skipVer);
    }

    /** */
    private final int partId;

    /**
     * @param grp Cache group.
     * @param freeList Free list.
     * @param partId Partition number.
     */
    public CacheDataRowStore(CacheGroupContext grp, FreeList freeList, int partId) {
        super(grp, freeList);

        this.partId = partId;
    }

    /**
     * @return Partition Id.
     */
    public int getPartitionId() {
        return partId;
    }

    /**
     * @param cacheId Cache ID.
     * @param hash Hash code.
     * @param link Link.
     * @return Search row.
     */
    CacheSearchRow keySearchRow(int cacheId, int hash, long link) {
        DataRow dataRow = new DataRow(
            grp,
            hash,
            link,
            partId,
            CacheDataRowAdapter.RowData.KEY_ONLY,
            SKIP_VER.get()
        );

        return initDataRow(dataRow, cacheId);
    }

    /**
     * @param cacheId Cache ID.
     * @param hash Hash code.
     * @param link Link.
     * @param rowData Required row data.
     * @param crdVer Mvcc coordinator version.
     * @param mvccCntr Mvcc counter.
     * @param opCntr Mvcc operation counter.
     * @return Search row.
     */
    MvccDataRow mvccRow(int cacheId, int hash, long link, CacheDataRowAdapter.RowData rowData, long crdVer, long mvccCntr, int opCntr) {
        MvccDataRow row = new MvccDataRow(
            grp,
            hash,
            link,
            partId,
            rowData,
            crdVer,
            mvccCntr,
            opCntr,
            SKIP_VER.get()
        );

        return initDataRow(row, cacheId);
    }

    /**
     * @param cacheId Cache ID.
     * @param hash Hash code.
     * @param link Link.
     * @param rowData Required row data.
     * @return Data row.
     */
    CacheDataRow dataRow(int cacheId, int hash, long link, CacheDataRowAdapter.RowData rowData) {
        DataRow dataRow = new DataRow(
            grp,
            hash,
            link,
            partId,
            rowData,
            SKIP_VER.get()
        );

        return initDataRow(dataRow, cacheId);
    }

    /**
     * @param dataRow Data row.
     * @param cacheId Cache ID.
     */
    private <T extends DataRow> T initDataRow(T dataRow, int cacheId) {
        if (dataRow.cacheId() == CU.UNDEFINED_CACHE_ID && grp.sharedGroup())
            dataRow.cacheId(cacheId);

        return dataRow;
    }
}
