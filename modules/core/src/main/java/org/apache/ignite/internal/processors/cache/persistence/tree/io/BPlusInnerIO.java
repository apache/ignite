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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;

/**
 * Abstract IO routines for B+Tree inner pages.
 */
public abstract class BPlusInnerIO<L> extends BPlusIO<L> {
    /** */
    private static final int SHIFT_LEFT = ITEMS_OFF;

    /** */
    private static final int SHIFT_LINK = SHIFT_LEFT + 8;

    /** */
    private final int SHIFT_RIGHT = SHIFT_LINK + itemSize;

    /**
     * @param type Page type.
     * @param ver Page format version.
     * @param canGetRow If we can get full row from this page.
     * @param itemSize Single item size on page.
     */
    protected BPlusInnerIO(int type, int ver, boolean canGetRow, int itemSize) {
        super(type, ver, false, canGetRow, itemSize);
    }

    /** {@inheritDoc} */
    @Override public int getMaxCount(long pageAddr, int pageSize) {
        // The structure of the page is the following:
        // |ITEMS_OFF|w|A|x|B|y|C|z|
        // where capital letters are data items, lowercase letters are 8 byte page references.
        return (pageSize - ITEMS_OFF - 8) / (getItemSize() + 8);
    }

    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @return Page ID.
     */
    public final long getLeft(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, (offset0(idx, SHIFT_LEFT)));
    }

    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @param pageId Page ID.
     */
    public final void setLeft(long pageAddr, int idx, long pageId) {
        PageUtils.putLong(pageAddr, offset0(idx, SHIFT_LEFT), pageId);

        assert pageId == getLeft(pageAddr, idx);
    }

    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @return Page ID.
     */
    public final long getRight(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset0(idx, SHIFT_RIGHT));
    }

    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @param pageId Page ID.
     */
    private void setRight(long pageAddr, int idx, long pageId) {
        PageUtils.putLong(pageAddr, offset0(idx, SHIFT_RIGHT), pageId);

        assert pageId == getRight(pageAddr, idx);
    }

    /** {@inheritDoc} */
    @Override public final void copyItems(long srcPageAddr, long dstPageAddr, int srcIdx, int dstIdx, int cnt,
        boolean cpLeft) throws IgniteCheckedException {
        assert srcIdx != dstIdx || srcPageAddr != dstPageAddr;

        cnt *= getItemSize() + 8; // From items to bytes.

        if (dstIdx > srcIdx) {
            PageHandler.copyMemory(srcPageAddr, offset(srcIdx), dstPageAddr, offset(dstIdx), cnt);

            if (cpLeft)
                PageUtils.putLong(dstPageAddr, offset0(dstIdx, SHIFT_LEFT), PageUtils.getLong(srcPageAddr, (offset0(srcIdx, SHIFT_LEFT))));
        }
        else {
            if (cpLeft)
                PageUtils.putLong(dstPageAddr, offset0(dstIdx, SHIFT_LEFT), PageUtils.getLong(srcPageAddr, (offset0(srcIdx, SHIFT_LEFT))));

            PageHandler.copyMemory(srcPageAddr, offset(srcIdx), dstPageAddr, offset(dstIdx), cnt);
        }
    }

    /**
     * @param idx Index of element.
     * @param shift It can be either link itself or left or right page ID.
     * @return Offset from byte buffer begin in bytes.
     */
    private int offset0(int idx, int shift) {
        return shift + (8 + getItemSize()) * idx;
    }

    /** {@inheritDoc} */
    @Override public final int offset(int idx) {
        return offset0(idx, SHIFT_LINK);
    }

    // Methods for B+Tree logic.

    /** {@inheritDoc} */
    @Override public byte[] insert(
        long pageAddr,
        int idx,
        L row,
        byte[] rowBytes,
        long rightId,
        boolean needRowBytes
    ) throws IgniteCheckedException {
        rowBytes = super.insert(pageAddr, idx, row, rowBytes, rightId, needRowBytes);

        // Setup reference to the right page on split.
        setRight(pageAddr, idx, rightId);

        return rowBytes;
    }

    /**
     * @param newRootPageAddr New root page address.
     * @param newRootId New root ID.
     * @param leftChildId Left child ID.
     * @param row Moved up row.
     * @param rowBytes Bytes.
     * @param rightChildId Right child ID.
     * @param pageSize Page size.
     * @param needRowBytes If we need row bytes back.
     * @return Row bytes.
     * @throws IgniteCheckedException If failed.
     */
    public byte[] initNewRoot(
        long newRootPageAddr,
        long newRootId,
        long leftChildId,
        L row,
        byte[] rowBytes,
        long rightChildId,
        int pageSize,
        boolean needRowBytes
    ) throws IgniteCheckedException {
        initNewPage(newRootPageAddr, newRootId, pageSize);

        setCount(newRootPageAddr, 1);
        setLeft(newRootPageAddr, 0, leftChildId);
        rowBytes = store(newRootPageAddr, 0, row, rowBytes, needRowBytes);
        setRight(newRootPageAddr, 0, rightChildId);

        return rowBytes;
    }
}
