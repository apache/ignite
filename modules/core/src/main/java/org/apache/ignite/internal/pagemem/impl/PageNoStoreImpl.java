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
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.internal.SB;

/**
 *
 */
public class PageNoStoreImpl implements Page {
    /** */
    private long absPtr;

    /** */
    private long pageId;

    /** */
    private PageMemoryNoStoreImpl pageMem;

    /**
     * @param pageMem Page memory.
     * @param absPtr Absolute pointer.
     * @param pageId Page ID.
     */
    PageNoStoreImpl(PageMemoryNoStoreImpl pageMem, long absPtr, long pageId) {
        this.pageMem = pageMem;
        this.absPtr = absPtr;

        this.pageId = pageId;
    }

    /**
     * @return Data pointer.
     */
    private long pointer() {
        return absPtr + PageMemoryNoStoreImpl.PAGE_OVERHEAD;
    }

    /** {@inheritDoc} */
    @Override public long id() {
        return pageId;
    }

    /** {@inheritDoc} */
    @Override public FullPageId fullId() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public long getForReadPointer() {
        if (pageMem.readLockPage(absPtr, PageIdUtils.tag(pageId)))
            return pointer();

        return 0L;
    }

    /** {@inheritDoc} */
    @Override public void releaseRead() {
        pageMem.readUnlockPage(absPtr);
    }

    /** {@inheritDoc} */
    @Override public long getForWritePointer() {
        int tag = PageIdUtils.tag(pageId);
        boolean locked = pageMem.writeLockPage(absPtr, tag);

        if (!locked)
            return 0L;

        return pointer();
    }

    /** {@inheritDoc} */
    @Override public long tryGetForWritePointer() {
        int tag = PageIdUtils.tag(pageId);

        if (pageMem.tryWriteLockPage(absPtr, tag))
            return pointer();

        return 0L;
    }

    /** {@inheritDoc} */
    @Override public void releaseWrite(boolean markDirty) {
        long updatedPageId = PageIO.getPageId(pointer());

        pageMem.writeUnlockPage(absPtr, PageIdUtils.tag(updatedPageId));
    }

    /** {@inheritDoc} */
    @Override public boolean isDirty() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void fullPageWalRecordPolicy(Boolean plc) {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public Boolean fullPageWalRecordPolicy() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        pageMem.releasePage(this);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        SB sb = new SB("PageNoStoreImpl [absPtr=0x");

        sb.appendHex(absPtr);
        sb.a(", pageId=0x").appendHex(pageId);
        sb.a("]");

        return sb.toString();
    }
}
