/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import org.h2.engine.Session;
import org.h2.util.BitField;

/**
 * The list of free pages of a page store. The format of a free list trunk page
 * is:
 * <ul>
 * <li>page type: byte (0)</li>
 * <li>checksum: short (1-2)</li>
 * <li>data (3-)</li>
 * </ul>
 */
public class PageFreeList extends Page {

    private static final int DATA_START = 3;

    private final PageStore store;
    private final BitField used;
    private final int pageCount;
    private boolean full;
    private Data data;

    private PageFreeList(PageStore store, int pageId) {
        // kept in cache, and array list in page store
        setPos(pageId);
        this.store = store;
        pageCount = (store.getPageSize() - DATA_START) * 8;
        used = new BitField(pageCount);
        used.set(0);
    }

    /**
     * Read a free-list page.
     *
     * @param store the page store
     * @param data the data
     * @param pageId the page id
     * @return the page
     */
    static PageFreeList read(PageStore store, Data data, int pageId) {
        PageFreeList p = new PageFreeList(store, pageId);
        p.data = data;
        p.read();
        return p;
    }

    /**
     * Create a new free-list page.
     *
     * @param store the page store
     * @param pageId the page id
     * @return the page
     */
    static PageFreeList create(PageStore store, int pageId) {
        return new PageFreeList(store, pageId);
    }

    /**
     * Allocate a page from the free list.
     *
     * @param exclude the exclude list or null
     * @param first the first page to look for
     * @return the page, or -1 if all pages are used
     */
    int allocate(BitField exclude, int first) {
        if (full) {
            return -1;
        }
        // TODO cache last result
        int start = Math.max(0, first - getPos());
        while (true) {
            int free = used.nextClearBit(start);
            if (free >= pageCount) {
                if (start == 0) {
                    full = true;
                }
                return -1;
            }
            if (exclude != null && exclude.get(free + getPos())) {
                start = exclude.nextClearBit(free + getPos()) - getPos();
                if (start >= pageCount) {
                    return -1;
                }
            } else {
                // set the bit first, because logUndo can
                // allocate other pages, and we must not
                // return the same page twice
                used.set(free);
                store.logUndo(this, data);
                store.update(this);
                return free + getPos();
            }
        }
    }

    /**
     * Get the first free page starting at the given offset.
     *
     * @param first the page number to start the search
     * @return the page number, or -1
     */
    int getFirstFree(int first) {
        if (full) {
            return -1;
        }
        int start = Math.max(0, first - getPos());
        int free = used.nextClearBit(start);
        if (free >= pageCount) {
            return -1;
        }
        return free + getPos();
    }

    int getLastUsed() {
        int last = used.length() - 1;
        return last <= 0 ? -1 : last + getPos();
    }

    /**
     * Mark a page as used.
     *
     * @param pageId the page id
     */
    void allocate(int pageId) {
        int idx = pageId - getPos();
        if (idx >= 0 && !used.get(idx)) {
            // set the bit first, because logUndo can
            // allocate other pages, and we must not
            // return the same page twice
            used.set(idx);
            store.logUndo(this, data);
            store.update(this);
        }
    }

    /**
     * Add a page to the free list.
     *
     * @param pageId the page id to add
     */
    void free(int pageId) {
        full = false;
        store.logUndo(this, data);
        used.clear(pageId - getPos());
        store.update(this);
    }

    /**
     * Read the page from the disk.
     */
    private void read() {
        data.reset();
        data.readByte();
        data.readShortInt();
        for (int i = 0; i < pageCount; i += 8) {
            int x = data.readByte() & 255;
            used.setByte(i, x);
        }
        full = false;
    }

    @Override
    public void write() {
        data = store.createData();
        data.writeByte((byte) Page.TYPE_FREE_LIST);
        data.writeShortInt(0);
        for (int i = 0; i < pageCount; i += 8) {
            data.writeByte((byte) used.getByte(i));
        }
        store.writePage(getPos(), data);
    }

    /**
     * Get the number of pages that can fit in a free list.
     *
     * @param pageSize the page size
     * @return the number of pages
     */
    public static int getPagesAddressed(int pageSize) {
        return (pageSize - DATA_START) * 8;
    }

    /**
     * Get the estimated memory size.
     *
     * @return number of double words (4 bytes)
     */
    @Override
    public int getMemory() {
        return store.getPageSize() >> 2;
    }

    /**
     * Check if a page is already in use.
     *
     * @param pageId the page to check
     * @return true if it is in use
     */
    boolean isUsed(int pageId) {
        return used.get(pageId - getPos());
    }

    @Override
    public void moveTo(Session session, int newPos) {
        // the old data does not need to be copied, as free-list pages
        // at the end of the file are not required
        store.free(getPos(), false);
    }

    @Override
    public String toString() {
        return "page [" + getPos() + "] freeList" + (full ? "full" : "");
    }

    @Override
    public boolean canRemove() {
        return true;
    }

    @Override
    public boolean canMove() {
        return false;
    }

}
