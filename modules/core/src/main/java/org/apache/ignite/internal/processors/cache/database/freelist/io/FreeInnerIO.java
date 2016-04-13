package org.apache.ignite.internal.processors.cache.database.freelist.io;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeItem;
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
        super(T_FREE_INNER, ver, false, 4); // freeSpace(2) + dispersion(2)
    }

    /** {@inheritDoc} */
    @Override public void store(ByteBuffer buf, int idx, FreeItem row) {
        store(buf, idx, row.dispersedFreeSpace());
    }

    /** {@inheritDoc} */
    @Override public void store(ByteBuffer dst, int dstIdx, BPlusIO<FreeItem> srcIo, ByteBuffer src, int srcIdx) {
        store(dst, dstIdx, ((FreeIO)srcIo).dispersedFreeSpace(src, srcIdx));
    }

    /**
     * @param buf Buffer.
     * @param idx Index.
     * @param dispersedFreeSpace Dispersed free space.
     */
    private void store(ByteBuffer buf, int idx, int dispersedFreeSpace) {
        int off = offset(idx, SHIFT_LINK);

        buf.putInt(off, dispersedFreeSpace);
    }

    /** {@inheritDoc} */
    @Override public int dispersedFreeSpace(ByteBuffer buf, int idx) {
        int off = offset(idx, SHIFT_LINK);

        return buf.getInt(off);
    }

    /** {@inheritDoc} */
    @Override public FreeItem getLookupRow(BPlusTree<FreeItem, ?> tree, ByteBuffer buf, int idx) {
        int off = offset(idx, SHIFT_LINK);

        return new FreeItem(buf.getShort(off), buf.getShort(off + 2), 0, 0);
    }
}
