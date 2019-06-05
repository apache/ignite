/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.util.BitField;
import org.h2.util.IntArray;

/**
 * An output stream that writes into a page store.
 */
public class PageOutputStream {

    private PageStore store;
    private final Trace trace;
    private final BitField exclude;
    private final boolean atEnd;
    private final int minPageId;

    private int trunkPageId;
    private int trunkNext;
    private IntArray reservedPages = new IntArray();
    private PageStreamTrunk trunk;
    private int trunkIndex;
    private PageStreamData data;
    private int reserved;
    private boolean needFlush;
    private boolean writing;
    private int pageCount;
    private int logKey;

    /**
     * Create a new page output stream.
     *
     * @param store the page store
     * @param trunkPage the first trunk page (already allocated)
     * @param exclude the pages not to use
     * @param logKey the log key of the first trunk page
     * @param atEnd whether only pages at the end of the file should be used
     */
    public PageOutputStream(PageStore store, int trunkPage, BitField exclude,
            int logKey, boolean atEnd) {
        this.trace = store.getTrace();
        this.store = store;
        this.trunkPageId = trunkPage;
        this.exclude = exclude;
        // minus one, because we increment before creating a trunk page
        this.logKey = logKey - 1;
        this.atEnd = atEnd;
        minPageId = atEnd ? trunkPage : 0;
    }

    /**
     * Allocate the required pages so that no pages need to be allocated while
     * writing.
     *
     * @param minBuffer the number of bytes to allocate
     */
    void reserve(int minBuffer) {
        if (reserved < minBuffer) {
            int pageSize = store.getPageSize();
            int capacityPerPage = PageStreamData.getCapacity(pageSize);
            int pages = PageStreamTrunk.getPagesAddressed(pageSize);
            int pagesToAllocate = 0, totalCapacity = 0;
            do {
                // allocate x data pages plus one trunk page
                pagesToAllocate += pages + 1;
                totalCapacity += pages * capacityPerPage;
            } while (totalCapacity < minBuffer);
            int firstPageToUse = atEnd ? trunkPageId : 0;
            store.allocatePages(reservedPages, pagesToAllocate, exclude, firstPageToUse);
            reserved += totalCapacity;
            if (data == null) {
                initNextData();
            }
        }
    }

    private void initNextData() {
        int nextData = trunk == null ? -1 : trunk.getPageData(trunkIndex++);
        if (nextData == -1) {
            int parent = trunkPageId;
            if (trunkNext != 0) {
                trunkPageId = trunkNext;
            }
            int len = PageStreamTrunk.getPagesAddressed(store.getPageSize());
            int[] pageIds = new int[len];
            for (int i = 0; i < len; i++) {
                pageIds[i] = reservedPages.get(i);
            }
            trunkNext = reservedPages.get(len);
            logKey++;
            trunk = PageStreamTrunk.create(store, parent, trunkPageId,
                    trunkNext, logKey, pageIds);
            trunkIndex = 0;
            pageCount++;
            trunk.write();
            reservedPages.removeRange(0, len + 1);
            nextData = trunk.getPageData(trunkIndex++);
        }
        data = PageStreamData.create(store, nextData, trunk.getPos(), logKey);
        pageCount++;
        data.initWrite();
    }

    /**
     * Write the data.
     *
     * @param b the buffer
     * @param off the offset
     * @param len the length
     */
    public void write(byte[] b, int off, int len) {
        if (len <= 0) {
            return;
        }
        if (writing) {
            DbException.throwInternalError("writing while still writing");
        }
        try {
            reserve(len);
            writing = true;
            while (len > 0) {
                int l = data.write(b, off, len);
                if (l < len) {
                    storePage();
                    initNextData();
                }
                reserved -= l;
                off += l;
                len -= l;
            }
            needFlush = true;
        } finally {
            writing = false;
        }
    }

    private void storePage() {
        if (trace.isDebugEnabled()) {
            trace.debug("pageOut.storePage " + data);
        }
        data.write();
    }

    /**
     * Write all data.
     */
    public void flush() {
        if (needFlush) {
            storePage();
            needFlush = false;
        }
    }

    /**
     * Close the stream.
     */
    public void close() {
        store = null;
    }

    int getCurrentDataPageId() {
        return data.getPos();
    }

    /**
     * Fill the data page with zeros and write it.
     * This is required for a checkpoint.
     */
    void fillPage() {
        if (trace.isDebugEnabled()) {
            trace.debug("pageOut.storePage fill " + data.getPos());
        }
        reserve(data.getRemaining() + 1);
        reserved -= data.getRemaining();
        data.write();
        initNextData();
    }

    long getSize() {
        return pageCount * store.getPageSize();
    }

    /**
     * Remove a trunk page from the stream.
     *
     * @param t the trunk page
     */
    void free(PageStreamTrunk t) {
        pageCount -= t.free(0);
    }

    /**
     * Free up all reserved pages.
     */
    void freeReserved() {
        if (reservedPages.size() > 0) {
            int[] array = new int[reservedPages.size()];
            reservedPages.toArray(array);
            reservedPages = new IntArray();
            reserved = 0;
            for (int p : array) {
                store.free(p, false);
            }
        }
    }

    /**
     * Get the smallest possible page id used. This is the trunk page if only
     * appending at the end of the file, or 0.
     *
     * @return the smallest possible page.
     */
    int getMinPageId() {
        return minPageId;
    }

}
