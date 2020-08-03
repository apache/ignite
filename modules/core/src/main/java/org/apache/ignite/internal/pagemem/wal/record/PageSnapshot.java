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
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;

/**
 *
 */
public class PageSnapshot extends WALRecord implements WalRecordCacheGroupAware {
    /** */
    @GridToStringExclude
    private byte[] pageDataBytes;

    /** */
    private FullPageId fullPageId;

    /**
     * PageSize without encryption overhead.
     */
    private int realPageSize;

    /**
     * @param fullId Full page ID.
     * @param arr Read array.
     * @param realPageSize Page size without encryption overhead.
     */
    public PageSnapshot(FullPageId fullId, byte[] arr, int realPageSize) {
        this.fullPageId = fullId;
        this.pageDataBytes = arr;
        this.realPageSize = realPageSize;
    }

    /**
     * This constructor doesn't actually create a page snapshot (copy), it creates a wrapper over page memory region. A
     * created record should not be used after WAL manager writes it to log, since page content can be modified.
     *
     * @param fullPageId Full page ID.
     * @param ptr Pointer to wrap.
     * @param pageSize Page size.
     * @param realPageSize Page size without encryption overhead.
     */
    public PageSnapshot(FullPageId fullPageId, long ptr, int pageSize, int realPageSize) {
        this.fullPageId = fullPageId;
        this.realPageSize = realPageSize;

        pageDataBytes = toBytes(GridUnsafe.wrapPointer(ptr, pageSize));
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.PAGE_RECORD;
    }

    /**
     * @return Bytes which was copied from given byte buffer.
     */
    private byte[] toBytes(ByteBuffer pageData) {
        if (!pageData.isDirect())
            return Arrays.copyOf(pageData.array(), pageData.limit());

        // In case of direct buffer copy buffer content to new array.
        byte[] arr = new byte[pageData.limit()];

        GridUnsafe.copyMemory(null, GridUnsafe.bufferAddress(pageData), arr, GridUnsafe.BYTE_ARR_OFF,
            pageData.limit());

        return arr;
    }

    /**
     * @return Snapshot of page data.
     */
    public byte[] pageData() {
        return pageDataBytes;
    }

    /**
     * @return Size of page data.
     */
    public int pageDataSize() {
        return pageDataBytes.length;
    }

    /**
     * @return Page data byte buffer.
     */
    public ByteBuffer pageDataBuffer() {
        ByteBuffer buf = ByteBuffer.wrap(pageDataBytes).order(ByteOrder.nativeOrder());

        buf.rewind();

        return buf;
    }

    /**
     * @return Full page ID.
     */
    public FullPageId fullPageId() {
        return fullPageId;
    }

    /** {@inheritDoc} */
    @Override public int groupId() {
        return fullPageId.groupId();
    }

    /**
     * @return PageSize without encryption overhead.
     */
    public int realPageSize() {
        return realPageSize;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        ByteBuffer buf = ByteBuffer.allocateDirect(pageDataSize());
        buf.order(ByteOrder.nativeOrder());
        buf.put(pageDataBytes);

        long addr = GridUnsafe.bufferAddress(buf);

        try {
            return "PageSnapshot [fullPageId = " + fullPageId() + ", page = [\n"
                + PageIO.printPage(addr, realPageSize)
                + "],\nsuper = ["
                + super.toString() + "]]";
        }
        finally {
            GridUnsafe.cleanDirectBuffer(buf);
        }
    }
}
