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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;

/**
 * Abstract IO routines for B+Tree pages.
 */
public abstract class BPlusIO<L> extends PageIO {
    /** */
    private static final int CNT_OFF = COMMON_HEADER_END;

    /** */
    private static final int FORWARD_OFF = CNT_OFF + 2;

    /** */
    private static final int REMOVE_ID_OFF = FORWARD_OFF + 8;

    /** */
    protected static final int ITEMS_OFF = REMOVE_ID_OFF + 8;

    /** */
    private final boolean canGetRow;

    /** */
    private final boolean leaf;

    /** All the items must be of fixed size. */
    protected final int itemSize;

    /**
     * @param type Page type.
     * @param ver Page format version.
     * @param leaf If this is a leaf IO.
     * @param canGetRow If we can get full row from this page.
     * @param itemSize Single item size on page.
     */
    protected BPlusIO(int type, int ver, boolean leaf, boolean canGetRow, int itemSize) {
        super(type, ver);

        assert itemSize > 0 : itemSize;
        assert canGetRow || !leaf: "leaf page always must be able to get full row";

        this.leaf = leaf;
        this.canGetRow = canGetRow;
        this.itemSize = itemSize;
    }

    /**
     * @return Item size in bytes.
     */
    public final int getItemSize() {
        return itemSize;
    }

    /** {@inheritDoc} */
    @Override public void initNewPage(ByteBuffer buf, long pageId) {
        super.initNewPage(buf, pageId);

        setCount(buf, 0);
        setForward(buf, 0);
        setRemoveId(buf, 0);
    }

    /**
     * @param buf Buffer.
     * @return Forward page ID.
     */
    public final long getForward(ByteBuffer buf) {
        return buf.getLong(FORWARD_OFF);
    }

    /**
     * @param buf Buffer.
     * @param pageId Forward page ID.
     */
    public final void setForward(ByteBuffer buf, long pageId) {
        buf.putLong(FORWARD_OFF, pageId);

        assert getForward(buf) == pageId;
    }

    /**
     * @param buf Buffer.
     * @return Remove ID.
     */
    public final long getRemoveId(ByteBuffer buf) {
        return buf.getLong(REMOVE_ID_OFF);
    }

    /**
     * @param buf Buffer.
     * @param rmvId Remove ID.
     */
    public final void setRemoveId(ByteBuffer buf, long rmvId) {
        buf.putLong(REMOVE_ID_OFF, rmvId);

        assert getRemoveId(buf) == rmvId;
    }

    /**
     * @param buf Buffer.
     * @return Items count in the page.
     */
    public final int getCount(ByteBuffer buf) {
        int cnt = buf.getShort(CNT_OFF) & 0xFFFF;

        assert cnt >= 0: cnt;

        return cnt;
    }

    /**
     * @param buf Buffer.
     * @param cnt Count.
     */
    public final void setCount(ByteBuffer buf, int cnt) {
        assert cnt >= 0: cnt;

        buf.putShort(CNT_OFF, (short)cnt);

        assert getCount(buf) == cnt;
    }

    /**
     * @return {@code true} If we can get the full row from this page using
     * method {@link BPlusTree#getRow(BPlusIO, ByteBuffer, int)}.
     * Must always be {@code true} for leaf pages.
     */
    public final boolean canGetRow() {
        return canGetRow;
    }

    /**
     * @return {@code true} if it is a leaf page.
     */
    public final boolean isLeaf() {
        return leaf;
    }

    /**
     * @param buf Buffer.
     * @return Max items count.
     */
    public abstract int getMaxCount(ByteBuffer buf);

    /**
     * Store the needed info about the row in the page. Leaf and inner pages can store different info.
     *
     * @param buf Buffer.
     * @param idx Index.
     * @param row Lookup or full row.
     * @param rowBytes Row bytes.
     * @throws IgniteCheckedException If failed.
     */
    public final void store(ByteBuffer buf, int idx, L row, byte[] rowBytes) throws IgniteCheckedException {
        int off = offset(idx);

        if (rowBytes == null)
            storeByOffset(buf, off, row);
        else
            putBytes(buf, off, rowBytes);
    }

    /**
     * @param idx Index of element.
     * @return Offset from byte buffer begin in bytes.
     */
    protected abstract int offset(int idx);

    /**
     * Store the needed info about the row in the page. Leaf and inner pages can store different info.
     *
     * @param buf Buffer.
     * @param off Offset in bytes.
     * @param row Lookup or full row.
     * @throws IgniteCheckedException If failed.
     */
    public abstract void storeByOffset(ByteBuffer buf, int off, L row) throws IgniteCheckedException;

    /**
     * Store row info from the given source.
     *
     * @param dst Destination buffer
     * @param dstIdx Destination index.
     * @param srcIo Source IO.
     * @param src Source buffer.
     * @param srcIdx Source index.
     * @throws IgniteCheckedException If failed.
     */
    public abstract void store(ByteBuffer dst, int dstIdx, BPlusIO<L> srcIo, ByteBuffer src, int srcIdx)
        throws IgniteCheckedException;

    /**
     * Get lookup row.
     *
     * @param tree Tree.
     * @param buf Buffer.
     * @param idx Index.
     * @return Lookup row.
     * @throws IgniteCheckedException If failed.
     */
    public abstract L getLookupRow(BPlusTree<L, ?> tree, ByteBuffer buf, int idx) throws IgniteCheckedException;

    /**
     * Copy items from source buffer to destination buffer.
     * Both pages must be of the same type and the same version.
     *
     * @param src Source buffer.
     * @param dst Destination buffer.
     * @param srcIdx Source begin index.
     * @param dstIdx Destination begin index.
     * @param cnt Items count.
     * @param cpLeft Copy leftmost link (makes sense only for inner pages).
     * @throws IgniteCheckedException If failed.
     */
    public abstract void copyItems(ByteBuffer src, ByteBuffer dst, int srcIdx, int dstIdx, int cnt, boolean cpLeft)
        throws IgniteCheckedException;

    // Methods for B+Tree logic.

    /**
     * @param buf Buffer.
     * @param idx Index.
     * @param row Row to insert.
     * @param rowBytes Row bytes.
     * @param rightId Page ID which will be to the right child for the inserted item.
     * @throws IgniteCheckedException If failed.
     */
    public void insert(ByteBuffer buf, int idx, L row, byte[] rowBytes, long rightId)
        throws IgniteCheckedException {
        int cnt = getCount(buf);

        // Move right all the greater elements to make a free slot for a new row link.
        copyItems(buf, buf, idx, idx + 1, cnt - idx, false);

        setCount(buf, cnt + 1);

        store(buf, idx, row, rowBytes);
    }

    /**
     * @param buf Splitting buffer.
     * @param fwdId Forward page ID.
     * @param fwdBuf Forward buffer.
     * @param mid Bisection index.
     * @param cnt Initial elements count in the page being split.
     * @throws IgniteCheckedException If failed.
     */
    public void splitForwardPage(
        ByteBuffer buf,
        long fwdId,
        ByteBuffer fwdBuf,
        int mid,
        int cnt
    ) throws IgniteCheckedException {
        initNewPage(fwdBuf, fwdId);

        cnt -= mid;

        copyItems(buf, fwdBuf, mid, 0, cnt, true);

        setCount(fwdBuf, cnt);
        setForward(fwdBuf, getForward(buf));

        // Copy remove ID to make sure that if inner remove touched this page, then retry
        // will happen even for newly allocated forward page.
        setRemoveId(fwdBuf, getRemoveId(buf));
    }

    /**
     * @param buf Buffer.
     * @param mid Bisection index.
     * @param fwdId New forward page ID.
     */
    public void splitExistingPage(ByteBuffer buf, int mid, long fwdId) {
        setCount(buf, mid);
        setForward(buf, fwdId);
    }

    /**
     * @param buf Buffer.
     * @param idx Index.
     * @param cnt Count.
     * @param rmvId Remove ID or {@code 0} to ignore.
     * @throws IgniteCheckedException If failed.
     */
    public void remove(ByteBuffer buf, int idx, int cnt, long rmvId) throws IgniteCheckedException {
        cnt--;

        copyItems(buf, buf, idx + 1, idx, cnt - idx, false);
        setCount(buf, cnt);

        if (rmvId != 0)
            setRemoveId(buf, rmvId);
    }

    /**
     * @param prntIo Parent IO.
     * @param prnt Parent buffer.
     * @param prntIdx Split key index in parent.
     * @param left Left buffer.
     * @param right Right buffer.
     * @param emptyBranch We are merging an empty branch.
     * @return {@code false} If we were not able to merge.
     * @throws IgniteCheckedException If failed.
     */
    public boolean merge(
        BPlusIO<L> prntIo,
        ByteBuffer prnt,
        int prntIdx,
        ByteBuffer left,
        ByteBuffer right,
        boolean emptyBranch
    ) throws IgniteCheckedException {
        int prntCnt = prntIo.getCount(prnt);
        int leftCnt = getCount(left);
        int rightCnt = getCount(right);

        int newCnt = leftCnt + rightCnt;

        // Need to move down split key in inner pages. For empty branch merge parent key will be just dropped.
        if (!isLeaf() && !emptyBranch)
            newCnt++;

        if (newCnt > getMaxCount(left)) {
            assert !emptyBranch;

            return false;
        }

        setCount(left, newCnt);

        // Move down split key in inner pages.
        if (!isLeaf() && !emptyBranch) {
            assert prntIdx < prntCnt; // It must be adjusted already.

            // We can be sure that we have enough free space to store split key here,
            // because we've done remove already and did not release child locks.
            store(left, leftCnt, prntIo, prnt, prntIdx);

            leftCnt++;
        }

        copyItems(right, left, 0, leftCnt, rightCnt, !emptyBranch);
        setForward(left, getForward(right));

        long rmvId = getRemoveId(right);

        // Need to have maximum remove ID.
        if (rmvId > getRemoveId(left))
            setRemoveId(left, rmvId);

        return true;
    }

    /**
     * @param buf Buffer.
     * @param pos Position in buffer.
     * @param bytes Bytes.
     */
    private static void putBytes(ByteBuffer buf, int pos, byte[] bytes) {
        int oldPos = buf.position();

        try {
            buf.position(pos);
            buf.put(bytes);
        }
        finally {
            buf.position(oldPos);
        }
    }
}
