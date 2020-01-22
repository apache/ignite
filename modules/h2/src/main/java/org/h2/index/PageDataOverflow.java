/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.store.Data;
import org.h2.store.Page;
import org.h2.store.PageStore;

/**
 * Overflow data for a leaf page. Format:
 * <ul>
 * <li>page type: byte (0)</li>
 * <li>checksum: short (1-2)</li>
 * <li>parent page id (0 for root): int (3-6)</li>
 * <li>more data: next overflow page id: int (7-10)</li>
 * <li>last remaining size: short (7-8)</li>
 * <li>data (11-/9-)</li>
 * </ul>
 */
public class PageDataOverflow extends Page {

    /**
     * The start of the data in the last overflow page.
     */
    static final int START_LAST = 9;

    /**
     * The start of the data in a overflow page that is not the last one.
     */
    static final int START_MORE = 11;

    private static final int START_NEXT_OVERFLOW = 7;

    /**
     * The page store.
     */
    private final PageStore store;

    /**
     * The page type.
     */
    private int type;

    /**
     * The parent page (overflow or leaf).
     */
    private int parentPageId;

    /**
     * The next overflow page, or 0.
     */
    private int nextPage;

    private final Data data;

    private int start;
    private int size;

    /**
     * Create an object from the given data page.
     *
     * @param store the page store
     * @param pageId the page id
     * @param data the data page
     */
    private PageDataOverflow(PageStore store, int pageId, Data data) {
        this.store = store;
        setPos(pageId);
        this.data = data;
    }

    /**
     * Read an overflow page.
     *
     * @param store the page store
     * @param data the data
     * @param pageId the page id
     * @return the page
     */
    public static Page read(PageStore store, Data data, int pageId) {
        PageDataOverflow p = new PageDataOverflow(store, pageId, data);
        p.read();
        return p;
    }

    /**
     * Create a new overflow page.
     *
     * @param store the page store
     * @param page the page id
     * @param type the page type
     * @param parentPageId the parent page id
     * @param next the next page or 0
     * @param all the data
     * @param offset the offset within the data
     * @param size the number of bytes
     * @return the page
     */
    static PageDataOverflow create(PageStore store, int page,
            int type, int parentPageId, int next,
            Data all, int offset, int size) {
        Data data = store.createData();
        PageDataOverflow p = new PageDataOverflow(store, page, data);
        store.logUndo(p, null);
        data.writeByte((byte) type);
        data.writeShortInt(0);
        data.writeInt(parentPageId);
        if (type == Page.TYPE_DATA_OVERFLOW) {
            data.writeInt(next);
        } else {
            data.writeShortInt(size);
        }
        p.start = data.length();
        data.write(all.getBytes(), offset, size);
        p.type = type;
        p.parentPageId = parentPageId;
        p.nextPage = next;
        p.size = size;
        return p;
    }

    /**
     * Read the page.
     */
    private void read() {
        data.reset();
        type = data.readByte();
        data.readShortInt();
        parentPageId = data.readInt();
        if (type == (Page.TYPE_DATA_OVERFLOW | Page.FLAG_LAST)) {
            size = data.readShortInt();
            nextPage = 0;
        } else if (type == Page.TYPE_DATA_OVERFLOW) {
            nextPage = data.readInt();
            size = store.getPageSize() - data.length();
        } else {
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1, "page:" +
                    getPos() + " type:" + type);
        }
        start = data.length();
    }

    /**
     * Read the data into a target buffer.
     *
     * @param target the target data page
     * @return the next page, or 0 if no next page
     */
    int readInto(Data target) {
        target.checkCapacity(size);
        if (type == (Page.TYPE_DATA_OVERFLOW | Page.FLAG_LAST)) {
            target.write(data.getBytes(), START_LAST, size);
            return 0;
        }
        target.write(data.getBytes(), START_MORE, size);
        return nextPage;
    }

    int getNextOverflow() {
        return nextPage;
    }

    private void writeHead() {
        data.writeByte((byte) type);
        data.writeShortInt(0);
        data.writeInt(parentPageId);
    }

    @Override
    public void write() {
        writeData();
        store.writePage(getPos(), data);
    }


    private void writeData() {
        data.reset();
        writeHead();
        if (type == Page.TYPE_DATA_OVERFLOW) {
            data.writeInt(nextPage);
        } else {
            data.writeShortInt(size);
        }
    }


    @Override
    public String toString() {
        return "page[" + getPos() + "] data leaf overflow parent:" +
                parentPageId + " next:" + nextPage;
    }

    /**
     * Get the estimated memory size.
     *
     * @return number of double words (4 bytes)
     */
    @Override
    public int getMemory() {
        return (Constants.MEMORY_PAGE_DATA_OVERFLOW + store.getPageSize()) >> 2;
    }

    void setParentPageId(int parent) {
        store.logUndo(this, data);
        this.parentPageId = parent;
    }

    @Override
    public void moveTo(Session session, int newPos) {
        // load the pages into the cache, to ensure old pages
        // are written
        Page parent = store.getPage(parentPageId);
        if (parent == null) {
            throw DbException.throwInternalError();
        }
        PageDataOverflow next = null;
        if (nextPage != 0) {
            next = (PageDataOverflow) store.getPage(nextPage);
        }
        store.logUndo(this, data);
        PageDataOverflow p2 = PageDataOverflow.create(store, newPos, type,
                parentPageId, nextPage, data, start, size);
        store.update(p2);
        if (next != null) {
            next.setParentPageId(newPos);
            store.update(next);
        }
        if (parent instanceof PageDataOverflow) {
            PageDataOverflow p1 = (PageDataOverflow) parent;
            p1.setNext(getPos(), newPos);
        } else {
            PageDataLeaf p1 = (PageDataLeaf) parent;
            p1.setOverflow(getPos(), newPos);
        }
        store.update(parent);
        store.free(getPos());
    }

    private void setNext(int old, int nextPage) {
        if (SysProperties.CHECK && old != this.nextPage) {
            DbException.throwInternalError("move " + this + " " + nextPage);
        }
        store.logUndo(this, data);
        this.nextPage = nextPage;
        data.setInt(START_NEXT_OVERFLOW, nextPage);
    }

    /**
     * Free this page.
     */
    void free() {
        store.logUndo(this, data);
        store.free(getPos());
    }

    @Override
    public boolean canRemove() {
        return true;
    }

    @Override
    public boolean isStream() {
        return true;
    }

}
