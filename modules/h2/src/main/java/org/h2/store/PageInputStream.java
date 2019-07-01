/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.util.BitField;

/**
 * An input stream that reads from a page store.
 */
public class PageInputStream extends InputStream {

    private final PageStore store;
    private final Trace trace;
    private final int firstTrunkPage;
    private final PageStreamTrunk.Iterator trunkIterator;
    private int dataPage;
    private PageStreamTrunk trunk;
    private int trunkIndex;
    private PageStreamData data;
    private int dataPos;
    private boolean endOfFile;
    private int remaining;
    private final byte[] buffer = { 0 };
    private int logKey;

    PageInputStream(PageStore store, int logKey, int firstTrunkPage, int dataPage) {
        this.store = store;
        this.trace = store.getTrace();
        // minus one because we increment before comparing
        this.logKey = logKey - 1;
        this.firstTrunkPage = firstTrunkPage;
        trunkIterator = new PageStreamTrunk.Iterator(store, firstTrunkPage);
        this.dataPage = dataPage;
    }

    @Override
    public int read() throws IOException {
        int len = read(buffer);
        return len < 0 ? -1 : (buffer[0] & 255);
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        int read = 0;
        while (len > 0) {
            int r = readBlock(b, off, len);
            if (r < 0) {
                break;
            }
            read += r;
            off += r;
            len -= r;
        }
        return read == 0 ? -1 : read;
    }

    private int readBlock(byte[] buff, int off, int len) throws IOException {
        try {
            fillBuffer();
            if (endOfFile) {
                return -1;
            }
            int l = Math.min(remaining, len);
            data.read(dataPos, buff, off, l);
            remaining -= l;
            dataPos += l;
            return l;
        } catch (DbException e) {
            throw new EOFException();
        }
    }

    private void fillBuffer() {
        if (remaining > 0 || endOfFile) {
            return;
        }
        int next;
        while (true) {
            if (trunk == null) {
                trunk = trunkIterator.next();
                trunkIndex = 0;
                logKey++;
                if (trunk == null || trunk.getLogKey() != logKey) {
                    endOfFile = true;
                    return;
                }
            }
            if (trunk != null) {
                next = trunk.getPageData(trunkIndex++);
                if (next == -1) {
                    trunk = null;
                } else if (dataPage == -1 || dataPage == next) {
                    break;
                }
            }
        }
        if (trace.isDebugEnabled()) {
            trace.debug("pageIn.readPage " + next);
        }
        dataPage = -1;
        data = null;
        Page p = store.getPage(next);
        if (p instanceof PageStreamData) {
            data = (PageStreamData) p;
        }
        if (data == null || data.getLogKey() != logKey) {
            endOfFile = true;
            return;
        }
        dataPos = PageStreamData.getReadStart();
        remaining = store.getPageSize() - dataPos;
    }

    /**
     * Set all pages as 'allocated' in the page store.
     *
     * @return the bit set
     */
    BitField allocateAllPages() {
        BitField pages = new BitField();
        int key = logKey;
        PageStreamTrunk.Iterator it = new PageStreamTrunk.Iterator(
                store, firstTrunkPage);
        while (true) {
            PageStreamTrunk t = it.next();
            key++;
            if (it.canDelete()) {
                store.allocatePage(it.getCurrentPageId());
            }
            if (t == null || t.getLogKey() != key) {
                break;
            }
            pages.set(t.getPos());
            for (int i = 0;; i++) {
                int n = t.getPageData(i);
                if (n == -1) {
                    break;
                }
                pages.set(n);
                store.allocatePage(n);
            }
        }
        return pages;
    }

    int getDataPage() {
        return data.getPos();
    }

    @Override
    public void close() {
        // nothing to do
    }

}
