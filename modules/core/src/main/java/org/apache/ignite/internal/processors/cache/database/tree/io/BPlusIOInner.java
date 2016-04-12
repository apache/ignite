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

package org.apache.ignite.internal.processors.cache.database.tree.io;

import java.nio.ByteBuffer;

/**
 * Abstract IO routines for B+Tree inner pages.
 */
public abstract class BPlusIOInner<L> extends BPlusIO<L> {
    /** */
    protected static final int ITEM_SIZE = 16;

    /** */
    protected static final int SHIFT_LEFT = ITEMS_OFF;

    /** */
    protected static final int SHIFT_LINK = ITEMS_OFF + 8;

    /** */
    protected static final int SHIFT_RIGHT = ITEMS_OFF + 16;

    /**
     * @param ver Page format version.
     */
    protected BPlusIOInner(int ver) {
        super(ver);
    }

    /** {@inheritDoc} */
    @Override public int getMaxCount(ByteBuffer buf) {
        //  (capacity - ITEMS_OFF - RIGHTMOST_PAGE_ID_SLOT_SIZE) / ITEM_SIZE
        return (buf.capacity() - ITEMS_OFF - 8) >>> 4;
    }

    /** {@inheritDoc} */
    @Override public boolean isLeaf() {
        return false;
    }

    /**
     * @param buf Buffer.
     * @param idx Index.
     * @return Page ID.
     */
    public long getLeft(ByteBuffer buf, int idx) {
        return buf.getLong(offset(idx, SHIFT_LEFT));
    }

    /**
     * @param buf Buffer.
     * @param idx Index.
     * @param pageId Page ID.
     */
    public void setLeft(ByteBuffer buf, int idx, long pageId) {
        buf.putLong(offset(idx, SHIFT_LEFT), pageId);

        assert pageId == getLeft(buf, idx);
    }

    /**
     * @param buf Buffer.
     * @param idx Index.
     * @return Page ID.
     */
    public long getRight(ByteBuffer buf, int idx) {
        return buf.getLong(offset(idx, SHIFT_RIGHT));
    }

    /**
     * @param buf Buffer.
     * @param idx Index.
     * @param pageId Page ID.
     */
    public void setRight(ByteBuffer buf, int idx, long pageId) {
        buf.putLong(offset(idx, SHIFT_RIGHT), pageId);

        assert pageId == getRight(buf, idx);
    }

    /** {@inheritDoc} */
    @Override public void copyItems(ByteBuffer src, ByteBuffer dst, int srcIdx, int dstIdx, int cnt,
        boolean cpLeft) {
        assert srcIdx != dstIdx || src != dst;

        if (dstIdx > srcIdx) {
            for (int i = cnt - 1; i >= 0; i--) {
                dst.putLong(offset(dstIdx + i, SHIFT_RIGHT), src.getLong(offset(srcIdx + i, SHIFT_RIGHT)));
                dst.putLong(offset(dstIdx + i, SHIFT_LINK), src.getLong(offset(srcIdx + i, SHIFT_LINK)));
            }

            if (cpLeft)
                dst.putLong(offset(dstIdx, SHIFT_LEFT), src.getLong(offset(srcIdx, SHIFT_LEFT)));
        }
        else {
            if (cpLeft)
                dst.putLong(offset(dstIdx, SHIFT_LEFT), src.getLong(offset(srcIdx, SHIFT_LEFT)));

            for (int i = 0; i < cnt; i++) {
                dst.putLong(offset(dstIdx + i, SHIFT_RIGHT), src.getLong(offset(srcIdx + i, SHIFT_RIGHT)));
                dst.putLong(offset(dstIdx + i, SHIFT_LINK), src.getLong(offset(srcIdx + i, SHIFT_LINK)));
            }
        }
    }

    /**
     * @param idx Index of element.
     * @param shift It can be either link itself or left or right page ID.
     * @return Offset from byte buffer begin in bytes.
     */
    protected static int offset(int idx, int shift) {
        assert idx >= 0: idx;

        return shift + ITEM_SIZE * idx;
    }
}
