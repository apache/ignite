/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.result.SearchRow;
import org.h2.store.Data;
import org.h2.store.Page;

/**
 * A page that contains index data.
 */
public abstract class PageBtree extends Page {

    /**
     * This is a root page.
     */
    static final int ROOT = 0;

    /**
     * Indicator that the row count is not known.
     */
    static final int UNKNOWN_ROWCOUNT = -1;

    /**
     * The index.
     */
    protected final PageBtreeIndex index;

    /**
     * The page number of the parent.
     */
    protected int parentPageId;

    /**
     * The data page.
     */
    protected final Data data;

    /**
     * The row offsets.
     */
    protected int[] offsets;

    /**
     * The number of entries.
     */
    protected int entryCount;

    /**
     * The index data
     */
    protected SearchRow[] rows;

    /**
     * The start of the data area.
     */
    protected int start;

    /**
     * If only the position of the row is stored in the page
     */
    protected boolean onlyPosition;

    /**
     * Whether the data page is up-to-date.
     */
    protected boolean written;

    /**
     * The estimated memory used by this object.
     */
    private final int memoryEstimated;

    PageBtree(PageBtreeIndex index, int pageId, Data data) {
        this.index = index;
        this.data = data;
        setPos(pageId);
        memoryEstimated = index.getMemoryPerPage();
    }

    /**
     * Get the real row count. If required, this will read all child pages.
     *
     * @return the row count
     */
    abstract int getRowCount();

    /**
     * Set the stored row count. This will write the page.
     *
     * @param rowCount the stored row count
     */
    abstract void setRowCountStored(int rowCount);

    /**
     * Find an entry.
     *
     * @param compare the row
     * @param bigger if looking for a larger row
     * @param add if the row should be added (check for duplicate keys)
     * @param compareKeys compare the row keys as well
     * @return the index of the found row
     */
    int find(SearchRow compare, boolean bigger, boolean add, boolean compareKeys) {
        if (compare == null) {
            return 0;
        }
        int l = 0, r = entryCount;
        int comp = 1;
        while (l < r) {
            int i = (l + r) >>> 1;
            SearchRow row = getRow(i);
            comp = index.compareRows(row, compare);
            if (comp == 0) {
                if (add && index.indexType.isUnique()) {
                    if (!index.mayHaveNullDuplicates(compare)) {
                        throw index.getDuplicateKeyException(compare.toString());
                    }
                }
                if (compareKeys) {
                    comp = index.compareKeys(row, compare);
                    if (comp == 0) {
                        return i;
                    }
                }
            }
            if (comp > 0 || (!bigger && comp == 0)) {
                r = i;
            } else {
                l = i + 1;
            }
        }
        return l;
    }

    /**
     * Add a row if possible. If it is possible this method returns -1,
     * otherwise the split point. It is always possible to add one row.
     *
     * @param row the row to add
     * @return the split point of this page, or -1 if no split is required
     */
    abstract int addRowTry(SearchRow row);

    /**
     * Find the first row.
     *
     * @param cursor the cursor
     * @param first the row to find
     * @param bigger if the row should be bigger
     */
    abstract void find(PageBtreeCursor cursor, SearchRow first, boolean bigger);

    /**
     * Find the last row.
     *
     * @param cursor the cursor
     */
    abstract void last(PageBtreeCursor cursor);

    /**
     * Get the row at this position.
     *
     * @param at the index
     * @return the row
     */
    SearchRow getRow(int at) {
        SearchRow row = rows[at];
        if (row == null) {
            row = index.readRow(data, offsets[at], onlyPosition, true);
            memoryChange();
            rows[at] = row;
        } else if (!index.hasData(row)) {
            row = index.readRow(row.getKey());
            memoryChange();
            rows[at] = row;
        }
        return row;
    }

    /**
     * The memory usage of this page was changed. Propagate the change if
     * needed.
     */
    protected void memoryChange() {
        // nothing to do
    }

    /**
     * Split the index page at the given point.
     *
     * @param splitPoint the index where to split
     * @return the new page that contains about half the entries
     */
    abstract PageBtree split(int splitPoint);

    /**
     * Change the page id.
     *
     * @param id the new page id
     */
    void setPageId(int id) {
        changeCount = index.getPageStore().getChangeCount();
        written = false;
        index.getPageStore().removeFromCache(getPos());
        setPos(id);
        index.getPageStore().logUndo(this, null);
        remapChildren();
    }

    /**
     * Get the first child leaf page of a page.
     *
     * @return the page
     */
    abstract PageBtreeLeaf getFirstLeaf();

    /**
     * Get the first child leaf page of a page.
     *
     * @return the page
     */
    abstract PageBtreeLeaf getLastLeaf();

    /**
     * Change the parent page id.
     *
     * @param id the new parent page id
     */
    void setParentPageId(int id) {
        index.getPageStore().logUndo(this, data);
        changeCount = index.getPageStore().getChangeCount();
        written = false;
        parentPageId = id;
    }

    /**
     * Update the parent id of all children.
     */
    abstract void remapChildren();

    /**
     * Remove a row.
     *
     * @param row the row to remove
     * @return null if the last row didn't change,
     *          the deleted row if the page is now empty,
     *          otherwise the new last row of this page
     */
    abstract SearchRow remove(SearchRow row);

    /**
     * Free this page and all child pages.
     */
    abstract void freeRecursive();

    /**
     * Ensure all rows are read in memory.
     */
    protected void readAllRows() {
        for (int i = 0; i < entryCount; i++) {
            SearchRow row = rows[i];
            if (row == null) {
                row = index.readRow(data, offsets[i], onlyPosition, false);
                rows[i] = row;
            }
        }
    }

    /**
     * Get the estimated memory size.
     *
     * @return number of double words (4 bytes)
     */
    @Override
    public int getMemory() {
        // need to always return the same value for the same object (otherwise
        // the cache size would change after adding and then removing the same
        // page from the cache) but index.getMemoryPerPage() can adopt according
        // to how much memory a row needs on average
        return memoryEstimated;
    }

    @Override
    public boolean canRemove() {
        return changeCount < index.getPageStore().getChangeCount();
    }

}
