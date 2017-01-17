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

package org.apache.ignite.internal.pagemem.wal.record.delta;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_PAGE_SET_FREE_LIST_PAGE;

/**
 *
 */
public class DataPageSetFreeListPageRecord extends PageDeltaRecord {
    /** */
    private long freeListPage;

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param freeListPage Free list page ID.
     */
    public DataPageSetFreeListPageRecord(int cacheId, long pageId, long freeListPage) {
        super(cacheId, pageId);

        this.freeListPage = freeListPage;
    }

    /**
     * @return Free list page ID.
     */
    public long freeListPage() {
        return freeListPage;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        DataPageIO io = DataPageIO.VERSIONS.forPage(pageAddr);

        io.setFreeListPageId(pageAddr, freeListPage);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return DATA_PAGE_SET_FREE_LIST_PAGE;
    }
}
