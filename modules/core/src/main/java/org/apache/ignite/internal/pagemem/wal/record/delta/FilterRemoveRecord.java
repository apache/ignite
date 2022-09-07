package org.apache.ignite.internal.pagemem.wal.record.delta;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Remove by filter.
 *
 * @see BPlusTree#remove(BPlusTree.TreeRowClosure) 
 */
public class FilterRemoveRecord extends PageDeltaRecord {
    /** Indexes of items to remove from the page. */
    private final int[] items;

    /** Number of used elements in items array. */
    private final short itemsCnt;

    /** Count of items that should remain on the page. */
    private final short cnt;

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param items Indexes of items to remove from the page.
     * @param itemsCnt Number of used elements in {@code items} array.
     * @param cnt Count of items that should remain on the page.
     */
    public FilterRemoveRecord(int grpId, long pageId, int[] items, int itemsCnt, int cnt) {
        super(grpId, pageId);

        assert itemsCnt >= Short.MIN_VALUE && itemsCnt <= Short.MAX_VALUE;
        assert cnt >= Short.MIN_VALUE && cnt <= Short.MAX_VALUE;
        assert itemsCnt > 0 && itemsCnt <= items.length;

        this.items = items;
        this.itemsCnt = (short)itemsCnt;
        this.cnt = (short)cnt;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        BPlusIO<?> io = PageIO.getBPlusIO(pageAddr);

        int exp = cnt + itemsCnt;
        int actual = io.getCount(pageAddr);

        if (actual != exp)
            throw new DeltaApplicationException("Count is wrong [exp=" + exp + ", actual=" + actual + ']');

        io.remove(pageAddr, items, itemsCnt);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BTREE_FILTER_REMOVE;
    }

    /**
     * @return Indexes of items.
     */
    public int[] items() {
        return items;
    }

    /**
     * @return Number of used elements in the items array.
     */
    public int itemsCount() {
        return itemsCnt;
    }

    /**
     * @return Resulting count of items that should remain on the page.
     */
    public int count() {
        return cnt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FilterRemoveRecord.class, this, "super", super.toString());
    }
}
