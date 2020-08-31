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

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Abstract IO routines for B+Tree pages.
 */
public abstract class BPlusIO<L> extends PageIO implements CompactablePageIO {
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
     */
    protected BPlusIO(int type, int ver, boolean leaf, boolean canGetRow, int itemSize) {
        super(type, ver);

        assert itemSize > 0 : itemSize;
        assert canGetRow || !leaf : "leaf page always must be able to get full row";

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
    @Override public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setCount(pageAddr, 0);
        setForward(pageAddr, 0);
        setRemoveId(pageAddr, 0);
    }

    /**
     * @param pageAddr Page address.
     * @return Forward page ID.
     */
    public final long getForward(long pageAddr) {
        return PageUtils.getLong(pageAddr, FORWARD_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param pageId Forward page ID.
     */
    public final void setForward(long pageAddr, long pageId) {
        PageUtils.putLong(pageAddr, FORWARD_OFF, pageId);

        assert getForward(pageAddr) == pageId;
    }

    /**
     * @param pageAddr Page address.
     * @return Remove ID.
     */
    public final long getRemoveId(long pageAddr) {
        return PageUtils.getLong(pageAddr, REMOVE_ID_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param rmvId Remove ID.
     */
    public final void setRemoveId(long pageAddr, long rmvId) {
        PageUtils.putLong(pageAddr, REMOVE_ID_OFF, rmvId);

        assert getRemoveId(pageAddr) == rmvId;
    }

    /**
     * @param pageAddr Page address.
     * @return Items count in the page.
     */
    public final int getCount(long pageAddr) {
        int cnt = PageUtils.getShort(pageAddr, CNT_OFF) & 0xFFFF;

        assert cnt >= 0 : cnt;

        return cnt;
    }

    /**
     * @param pageAddr Page address.
     * @param cnt Count.
     */
    public final void setCount(long pageAddr, int cnt) {
        assert cnt >= 0 : cnt;

        PageUtils.putShort(pageAddr, CNT_OFF, (short)cnt);

        assert getCount(pageAddr) == cnt;
    }

    /**
     * @return {@code true} If we can get the full row from this page using
     * method {@link BPlusTree#getRow(BPlusIO, long, int)}.
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
     * @param pageAddr Page address.
     * @param pageSize Page size without encryption overhead.
     * @return Max items count.
     */
    public abstract int getMaxCount(long pageAddr, int pageSize);

    /**
     * Store the needed info about the row in the page. Leaf and inner pages can store different info.
     *
     * @param pageAddr Page address.
     * @param idx Index.
     * @param row Lookup or full row.
     * @param rowBytes Row bytes.
     * @param needRowBytes If we need stored row bytes.
     * @return Stored row bytes.
     * @throws IgniteCheckedException If failed.
     */
    public final byte[] store(long pageAddr, int idx, L row, byte[] rowBytes, boolean needRowBytes)
        throws IgniteCheckedException {
        int off = offset(idx);

        if (rowBytes == null) {
            storeByOffset(pageAddr, off, row);

            if (needRowBytes)
                rowBytes = PageUtils.getBytes(pageAddr, off, getItemSize());
        }
        else
            putBytes(pageAddr, off, rowBytes);

        return rowBytes;
    }

    /**
     * @param idx Index of element.
     * @return Offset from byte buffer begin in bytes.
     */
    public abstract int offset(int idx);

    /**
     * Store the needed info about the row in the page. Leaf and inner pages can store different info.
     *
     * @param pageAddr Page address.
     * @param off Offset in bytes.
     * @param row Lookup or full row.
     * @throws IgniteCheckedException If failed.
     */
    public abstract void storeByOffset(long pageAddr, int off, L row) throws IgniteCheckedException;

    /**
     * Store row info from the given source.
     *
     * @param dstPageAddr Destination page address.
     * @param dstIdx Destination index.
     * @param srcIo Source IO.
     * @param srcPageAddr Source page address.
     * @param srcIdx Source index.
     * @throws IgniteCheckedException If failed.
     */
    public abstract void store(long dstPageAddr, int dstIdx, BPlusIO<L> srcIo, long srcPageAddr, int srcIdx)
        throws IgniteCheckedException;

    /**
     * Get lookup row.
     *
     * @param tree Tree.
     * @param pageAddr Page address.
     * @param idx Index.
     * @return Lookup row.
     * @throws IgniteCheckedException If failed.
     */
    public abstract L getLookupRow(BPlusTree<L, ?> tree, long pageAddr, int idx) throws IgniteCheckedException;

    /**
     * Copy items from source page to destination page.
     * Both pages must be of the same type and the same version.
     *
     * @param srcPageAddr Source page address.
     * @param dstPageAddr Destination page address.
     * @param srcIdx Source begin index.
     * @param dstIdx Destination begin index.
     * @param cnt Items count.
     * @param cpLeft Copy leftmost link (makes sense only for inner pages).
     * @throws IgniteCheckedException If failed.
     */
    public abstract void copyItems(long srcPageAddr, long dstPageAddr, int srcIdx, int dstIdx, int cnt, boolean cpLeft)
        throws IgniteCheckedException;

    // Methods for B+Tree logic.

    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @param row Row to insert.
     * @param rowBytes Row bytes.
     * @param rightId Page ID which will be to the right child for the inserted item.
     * @param needRowBytes If we need stored row bytes.
     * @return Row bytes.
     * @throws IgniteCheckedException If failed.
     */
    public byte[] insert(long pageAddr, int idx, L row, byte[] rowBytes, long rightId, boolean needRowBytes)
        throws IgniteCheckedException {
        int cnt = getCount(pageAddr);

        // Move right all the greater elements to make a free slot for a new row link.
        copyItems(pageAddr, pageAddr, idx, idx + 1, cnt - idx, false);

        setCount(pageAddr, cnt + 1);

        return store(pageAddr, idx, row, rowBytes, needRowBytes);
    }

    /**
     * @param pageAddr Splitting page address.
     * @param fwdId Forward page ID.
     * @param fwdPageAddr Forward page address.
     * @param mid Bisection index.
     * @param cnt Initial elements count in the page being split.
     * @param pageSize Page size.
     * @throws IgniteCheckedException If failed.
     */
    public void splitForwardPage(
        long pageAddr,
        long fwdId,
        long fwdPageAddr,
        int mid,
        int cnt,
        int pageSize
    ) throws IgniteCheckedException {
        initNewPage(fwdPageAddr, fwdId, pageSize);

        cnt -= mid;

        copyItems(pageAddr, fwdPageAddr, mid, 0, cnt, true);

        setCount(fwdPageAddr, cnt);
        setForward(fwdPageAddr, getForward(pageAddr));

        // Copy remove ID to make sure that if inner remove touched this page, then retry
        // will happen even for newly allocated forward page.
        setRemoveId(fwdPageAddr, getRemoveId(pageAddr));
    }

    /**
     * @param pageAddr Page address.
     * @param mid Bisection index.
     * @param fwdId New forward page ID.
     */
    public void splitExistingPage(long pageAddr, int mid, long fwdId) {
        setCount(pageAddr, mid);
        setForward(pageAddr, fwdId);
    }

    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @param cnt Count.
     * @throws IgniteCheckedException If failed.
     */
    public void remove(long pageAddr, int idx, int cnt) throws IgniteCheckedException {
        cnt--;

        copyItems(pageAddr, pageAddr, idx + 1, idx, cnt - idx, false);
        setCount(pageAddr, cnt);
    }

    /**
     * @param prntIo Parent IO.
     * @param prntPageAddr Parent page address.
     * @param prntIdx Split key index in parent.
     * @param leftPageAddr Left page address.
     * @param rightPageAddr Right page address.
     * @param emptyBranch We are merging an empty branch.
     * @param pageSize Page size without encryption overhead.
     * @return {@code false} If we were not able to merge.
     * @throws IgniteCheckedException If failed.
     */
    public boolean merge(
        BPlusIO<L> prntIo,
        long prntPageAddr,
        int prntIdx,
        long leftPageAddr,
        long rightPageAddr,
        boolean emptyBranch,
        int pageSize
    ) throws IgniteCheckedException {
        int prntCnt = prntIo.getCount(prntPageAddr);
        int leftCnt = getCount(leftPageAddr);
        int rightCnt = getCount(rightPageAddr);

        int newCnt = leftCnt + rightCnt;

        // Need to move down split key in inner pages. For empty branch merge parent key will be just dropped.
        if (!isLeaf() && !emptyBranch)
            newCnt++;

        if (newCnt > getMaxCount(leftPageAddr, pageSize)) {
            assert !emptyBranch;

            return false;
        }

        setCount(leftPageAddr, newCnt);

        // Move down split key in inner pages.
        if (!isLeaf() && !emptyBranch) {
            assert prntIdx >= 0 && prntIdx < prntCnt : prntIdx; // It must be adjusted already.

            // We can be sure that we have enough free space to store split key here,
            // because we've done remove already and did not release child locks.
            store(leftPageAddr, leftCnt, prntIo, prntPageAddr, prntIdx);

            leftCnt++;
        }

        copyItems(rightPageAddr, leftPageAddr, 0, leftCnt, rightCnt, !emptyBranch);
        setForward(leftPageAddr, getForward(rightPageAddr));

        long rmvId = getRemoveId(rightPageAddr);

        // Need to have maximum remove ID.
        if (rmvId > getRemoveId(leftPageAddr))
            setRemoveId(leftPageAddr, rmvId);

        return true;
    }

    /**
     * @param pageAddr Page address.
     * @param pos Position in page.
     * @param bytes Bytes.
     */
    private static void putBytes(long pageAddr, int pos, byte[] bytes) {
        PageUtils.putBytes(pageAddr, pos, bytes);
    }

    /**
     * @param pageAddr Page address.
     * @param c Closure.
     */
    public void visit(long pageAddr, IgniteInClosure<L> c) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void printPage(long addr, int pageSize, GridStringBuilder sb) throws IgniteCheckedException {
        sb.a("BPlusIO [\n\tcanGetRow=").a(canGetRow)
            .a(",\n\tleaf=").a(leaf)
            .a(",\n\titemSize=").a(itemSize)
            .a(",\n\tcnt=").a(getCount(addr))
            .a(",\n\tforward=").appendHex(getForward(addr))
            .a(",\n\tremoveId=").appendHex(getRemoveId(addr))
            .a("\n]");
    }

    /**
     * @param pageAddr Page address.
     * @return Offset after the last item.
     */
    public int getItemsEnd(long pageAddr) {
        int cnt = getCount(pageAddr);
        return offset(cnt);
    }

    /** {@inheritDoc} */
    @Override public void compactPage(ByteBuffer page, ByteBuffer out, int pageSize) {
        copyPage(page, out, pageSize);

        long pageAddr = GridUnsafe.bufferAddress(out);

        // Just drop all the extra garbage at the end.
        out.limit(getItemsEnd(pageAddr));
    }

    /** {@inheritDoc} */
    @Override public void restorePage(ByteBuffer compactPage, int pageSize) {
        assert compactPage.isDirect();
        assert compactPage.position() == 0;
        assert compactPage.limit() <= pageSize;

        compactPage.limit(pageSize); // Just add garbage to the end.
    }
}
