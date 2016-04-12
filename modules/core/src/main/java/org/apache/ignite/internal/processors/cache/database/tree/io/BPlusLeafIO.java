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
 * Abstract IO routines for B+Tree leaf pages.
 */
public abstract class BPlusLeafIO<L> extends BPlusIO<L> {
    /** */
    protected static final int ITEM_SIZE = 8;

    /**
     * @param ver Page format version.
     */
    protected BPlusLeafIO(int ver) {
        super(ver);
    }

    /** {@inheritDoc} */
    @Override public boolean isLeaf() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public int getMaxCount(ByteBuffer buf) {
        return (buf.capacity() - ITEMS_OFF) >>> 3; // divide by ITEM_SIZE
    }

//        /** {@inheritDoc} */
//        @Override public long getLink(ByteBuffer buf, int idx) {
//            assert idx < getCount(buf): idx;
//
//            return buf.getLong(offset(idx));
//        }
//
//        /** {@inheritDoc} */
//        @Override public void setLink(ByteBuffer buf, int idx, long link) {
//            buf.putLong(offset(idx), link);
//
//            assert getLink(buf, idx) == link;
//        }

    /** {@inheritDoc} */
    @Override public void copyItems(ByteBuffer src, ByteBuffer dst, int srcIdx, int dstIdx, int cnt,
        boolean cpLeft) {
        assert srcIdx != dstIdx || src != dst;

        if (dstIdx > srcIdx) {
            for (int i = cnt - 1; i >= 0; i--)
                dst.putLong(offset(dstIdx + i), src.getLong(offset(srcIdx + i)));
        }
        else {
            for (int i = 0; i < cnt; i++)
                dst.putLong(offset(dstIdx + i), src.getLong(offset(srcIdx + i)));
        }
    }

    /**
     * @param idx Index of item.
     * @return Offset.
     */
    protected static int offset(int idx) {
        assert idx >= 0: idx;

        return ITEMS_OFF + idx * ITEM_SIZE;
    }
}
