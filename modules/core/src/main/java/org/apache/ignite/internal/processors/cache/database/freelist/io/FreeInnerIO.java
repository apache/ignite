package org.apache.ignite.internal.processors.cache.database.freelist.io;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeItem;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeTree;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;

/**
 * Routines for free list inner pages.
 */
public class FreeInnerIO extends BPlusInnerIO<FreeItem> implements FreeIO {
    /** */
    public static final IOVersions<FreeInnerIO> VERSIONS = new IOVersions<>(
        new FreeInnerIO(1)
    );

    /**
     * @param ver Page format version.
     */
    protected FreeInnerIO(int ver) {
        super(T_FREE_INNER, ver, false, 6); // freeSpace(2) + pageIndex(4)
    }

    /** {@inheritDoc} */
    @Override public void store(ByteBuffer buf, int idx, FreeItem row) {
        int off = offset(idx);

        buf.putShort(off, row.freeSpace());
        buf.putInt(off + 2, PageIdUtils.pageIndex(row.pageId()));
    }

    /** {@inheritDoc} */
    @Override public void store(ByteBuffer dst, int dstIdx, BPlusIO<FreeItem> srcIo, ByteBuffer src, int srcIdx) {
        FreeIO srcFreeIo = (FreeIO)srcIo;

        int off = offset(dstIdx);

        dst.putShort(off, srcFreeIo.getFreeSpace(src, srcIdx));
        dst.putInt(off + 2, srcFreeIo.getPageIndex(src, srcIdx));
    }

    /** {@inheritDoc} */
    @Override public short getFreeSpace(ByteBuffer buf, int idx) {
        int off = offset(idx);

        return buf.getShort(off);
    }

    /** {@inheritDoc} */
    @Override public int getPageIndex(ByteBuffer buf, int idx) {
        int off = offset(idx);

        return buf.getInt(off + 2);
    }

    /** {@inheritDoc} */
    @Override public FreeItem getLookupRow(BPlusTree<FreeItem, ?> tree, ByteBuffer buf, int idx) {
        int off = offset(idx);

        short freeSpace = buf.getShort(off);
        int pageIdx = buf.getInt(off + 2);

        long pageId = PageIdUtils.pageId(((FreeTree)tree).getPartId(), PageIdAllocator.FLAG_DATA, pageIdx);

        return new FreeItem(freeSpace, pageId, tree.getCacheId());
    }
}
