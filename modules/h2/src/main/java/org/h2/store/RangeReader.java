/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.IOException;
import java.io.Reader;

import org.h2.util.IOUtils;

/**
 * Reader that reads only a specified range from the source reader.
 */
public final class RangeReader extends Reader {
    private final Reader r;

    private long limit;

    /**
     * Creates new instance of range reader.
     *
     * @param r
     *            source reader
     * @param offset
     *            offset of the range
     * @param limit
     *            length of the range
     * @throws IOException
     *             on I/O exception during seeking to the specified offset
     */
    public RangeReader(Reader r, long offset, long limit) throws IOException {
        this.r = r;
        this.limit = limit;
        IOUtils.skipFully(r, offset);
    }

    @Override
    public int read() throws IOException {
        if (limit <= 0) {
            return -1;
        }
        int c = r.read();
        if (c >= 0) {
            limit--;
        }
        return c;
    }

    @Override
    public int read(char cbuf[], int off, int len) throws IOException {
        if (limit <= 0) {
            return -1;
        }
        if (len > limit) {
            len = (int) limit;
        }
        int cnt = r.read(cbuf, off, len);
        if (cnt > 0) {
            limit -= cnt;
        }
        return cnt;
    }

    @Override
    public long skip(long n) throws IOException {
        if (n > limit) {
            n = (int) limit;
        }
        n = r.skip(n);
        limit -= n;
        return n;
    }

    @Override
    public boolean ready() throws IOException {
        if (limit > 0) {
            return r.ready();
        }
        return false;
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void mark(int readAheadLimit) throws IOException {
        throw new IOException("mark() not supported");
    }

    @Override
    public void reset() throws IOException {
        throw new IOException("reset() not supported");
    }

    @Override
    public void close() throws IOException {
        r.close();
    }
}
