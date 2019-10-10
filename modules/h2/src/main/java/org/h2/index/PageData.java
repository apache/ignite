/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.store.Data;
import org.h2.store.Page;

/**
 * A page that contains data rows.
 */
abstract class PageData extends Page {

    /**
     * The position of the parent page id.
     */
    static final int START_PARENT = 3;

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
    protected final PageDataIndex index;

    /**
     * The page number of the parent.
     */
    protected int parentPageId;

    /**
     * The data page.
     */
    protected final Data data;

    /**
     * The number of entries.
     */
    protected int entryCount;

    /**
     * The row keys.
     */
    protected long[] keys;

    /**
     * Whether the data page is up-to-date.
     */
    protected boolean written;

    /**
     * The estimated heap memory used by this object, in number of double words
     * (4 bytes each).
     */
    private final int memoryEstimated;

    PageData(PageDataIndex index, int pageId, Data data) {
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
     * Get the used disk space for this index.
     *
     * @return the estimated number of bytes
     */
    abstract long getDiskSpaceUsed();

    /**
     * Find an entry by key.
     *
     * @param key the key (may not exist)
     * @return the matching or next index
     */
    int find(long key) {
        int l = 0, r = entryCount;
        while (l < r) {
            int i = (l + r) >>> 1;
            long k = keys[i];
            if (k == key) {
                return i;
            } else if (k > key) {
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
     * @param row the now to add
     * @return the split point of this page, or -1 if no split is required
     */
    abstract int addRowTry(Row row);

    /**
     * Get a cursor.
     *
     * @param session the session
     * @param minKey the smallest key
     * @param maxKey the largest key
     * @param multiVersion if the delta should be used
     * @return the cursor
     */
    abstract Cursor find(Session session, long minKey, long maxKey,
            boolean multiVersion);

    /**
     * Get the key at this position.
     *
     * @param at the index
     * @return the key
     */
    long getKey(int at) {
        return keys[at];
    }

    /**
     * Split the index page at the given point.
     *
     * @param splitPoint the index where to split
     * @return the new page that contains about half the entries
     */
    abstract PageData split(int splitPoint);

    /**
     * Change the page id.
     *
     * @param id the new page id
     */
    void setPageId(int id) {
        int old = getPos();
        index.getPageStore().removeFromCache(getPos());
        setPos(id);
        index.getPageStore().logUndo(this, null);
        remapChildren(old);
    }

    /**
     * Get the last key of a page.
     *
     * @return the last key
     */
    abstract long getLastKey();

    /**
     * Get the first child leaf page of a page.
     *
     * @return the page
     */
    abstract PageDataLeaf getFirstLeaf();

    /**
     * Change the parent page id.
     *
     * @param id the new parent page id
     */
    void setParentPageId(int id) {
        index.getPageStore().logUndo(this, data);
        parentPageId = id;
        if (written) {
            changeCount = index.getPageStore().getChangeCount();
            data.setInt(START_PARENT, parentPageId);
        }
    }

    /**
     * Update the parent id of all children.
     *
     * @param old the previous position
     */
    abstract void remapChildren(int old);

    /**
     * Remove a row.
     *
     * @param key the key of the row to remove
     * @return true if this page is now empty
     */
    abstract boolean remove(long key);

    /**
     * Free this page and all child pages.
     */
    abstract void freeRecursive();

    /**
     * Get the row for the given key.
     *
     * @param key the key
     * @return the row
     */
    abstract Row getRowWithKey(long key);

    /**
     * Get the estimated heap memory size.
     *
     * @return number of double words (4 bytes each)
     */
    @Override
    public int getMemory() {
        // need to always return the same value for the same object (otherwise
        // the cache size would change after adding and then removing the same
        // page from the cache) but index.getMemoryPerPage() can adopt according
        // to how much memory a row needs on average
        return memoryEstimated;
    }

    int getParentPageId() {
        return parentPageId;
    }

    @Override
    public boolean canRemove() {
        return changeCount < index.getPageStore().getChangeCount();
    }

}
