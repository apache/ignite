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
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListNodeIO;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class PagesListSetNextRecord extends PageDeltaRecord {
    /** */
    private final long nextPageId;

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param nextPageId Next page ID.
     */
    public PagesListSetNextRecord(int grpId, long pageId, long nextPageId) {
        super(grpId, pageId);

        this.nextPageId = nextPageId;
    }

    /**
     * @return Next page ID.
     */
    public long nextPageId() {
        return nextPageId;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(pageAddr);

        io.setNextId(pageAddr, nextPageId);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.PAGES_LIST_SET_NEXT;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PagesListSetNextRecord.class, this, "super", super.toString());
    }
}
