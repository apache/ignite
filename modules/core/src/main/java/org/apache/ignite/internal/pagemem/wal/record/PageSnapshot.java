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

package org.apache.ignite.internal.pagemem.wal.record;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;

/**
 *
 */
public class PageSnapshot extends WALRecord implements WalRecordCacheGroupAware{
    /** */
    @GridToStringExclude
    private byte[] pageData;

    /** */
    private FullPageId fullPageId;

    /**
     * PageSIze without encryption overhead.
     */
    private int realPageSize;

    /**
     * @param fullId Full page ID.
     * @param arr Read array.
     * @param realPageSize Page size without encryption overhead.
     */
    public PageSnapshot(FullPageId fullId, byte[] arr, int realPageSize) {
        this.fullPageId = fullId;
        this.pageData = arr;
        this.realPageSize = realPageSize;
    }

    /**
     * @param fullPageId Full page ID.
     * @param ptr Pointer to copy from.
     * @param pageSize Page size.
     * @param realPageSize Page size without encryption overhead.
     */
    public PageSnapshot(FullPageId fullPageId, long ptr, int pageSize, int realPageSize) {
        this.fullPageId = fullPageId;
        this.realPageSize = realPageSize;

        pageData = new byte[pageSize];

        GridUnsafe.copyMemory(null, ptr, pageData, GridUnsafe.BYTE_ARR_OFF, pageSize);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.PAGE_RECORD;
    }

    /**
     * @return Snapshot of page data.
     */
    public byte[] pageData() {
        return pageData;
    }

    /**
     * @return Full page ID.
     */
    public FullPageId fullPageId() {
        return fullPageId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        ByteBuffer buf = ByteBuffer.allocateDirect(pageData.length);
        buf.order(ByteOrder.nativeOrder());
        buf.put(pageData);

        long addr = GridUnsafe.bufferAddress(buf);

        try {
            return "PageSnapshot [fullPageId = " + fullPageId() + ", page = [\n"
                + PageIO.printPage(addr, realPageSize)
                + "],\nsuper = ["
                + super.toString() + "]]";
        }
        catch (IgniteCheckedException ignored) {
            return "Error during call'toString' of PageSnapshot [fullPageId=" + fullPageId() +
                ", pageData = " + Arrays.toString(pageData) + ", super=" + super.toString() + "]";
        }
        finally {
            GridUnsafe.cleanDirectBuffer(buf);
        }
    }

    /** {@inheritDoc} */
    @Override public int groupId() {
        return fullPageId.groupId();
    }
}
