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

package org.apache.ignite.internal.pagemem.impl;

import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.util.typedef.internal.SB;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 *
 */
public class PageNoStoreImpl implements Page {
    /** */
    private int segIdx;

    /** */
    private long absPtr;

    /** */
    private long pageId;

    /** */
    private int cacheId;

    /** */
    private PageMemoryNoStoreImpl pageMem;

    /** */
    private final ByteBuffer buf;

    /**
     * @param segIdx Segment index.
     * @param absPtr Absolute pointer.
     */
    public PageNoStoreImpl(PageMemoryNoStoreImpl pageMem, int segIdx, long absPtr, int cacheId, long pageId) {
        this.pageMem = pageMem;
        this.segIdx = segIdx;
        this.absPtr = absPtr;

        this.cacheId = cacheId;
        this.pageId = pageId;

        buf = pageMem.wrapPointer(absPtr + PageMemoryNoStoreImpl.PAGE_OVERHEAD, pageMem.pageSize());
    }

    /** {@inheritDoc} */
    @Override public long id() {
        return pageId;
    }

    /** {@inheritDoc} */
    @Override public FullPageId fullId() {
        return new FullPageId(pageId, cacheId);
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer getForRead() {
        pageMem.readLockPage(absPtr);

        return reset(buf.asReadOnlyBuffer());
    }

    /** {@inheritDoc} */
    @Override public void releaseRead() {
        pageMem.readUnlockPage(absPtr);
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer getForWrite() {
        pageMem.writeLockPage(absPtr);

        return reset(buf);
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer getForInitialWrite() {
        return reset(buf);
    }

    /** {@inheritDoc} */
    @Override public void finishInitialWrite() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void releaseWrite(boolean markDirty) {
        pageMem.writeUnlockPage(absPtr);
    }

    /** {@inheritDoc} */
    @Override public boolean isDirty() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        pageMem.releasePage(this);
    }

    /**
     * @return Segment index.
     */
    int segmentIndex() {
        return segIdx;
    }

    /**
     * @return Absolute pointer to the system page start.
     */
    long absolutePointer() {
        return absPtr;
    }

    /**
     * @param buf Byte buffer.
     * @return The given buffer back.
     */
    private ByteBuffer reset(ByteBuffer buf) {
        buf.order(ByteOrder.nativeOrder());

        buf.rewind();

        return buf;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        SB sb = new SB("PageNoStoreImpl [absPtr=0x");

        sb.appendHex(absPtr);
        sb.a(", segIdx=").a(segIdx);
        sb.a(", cacheId=").a(cacheId);
        sb.a(", pageId=0x").appendHex(pageId);
        sb.a("]");

        return sb.toString();
    }
}
