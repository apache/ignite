/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.h2.util.IOUtils;

/**
 * Input stream that reads only a specified range from the source stream.
 */
public final class RangeInputStream extends FilterInputStream {
    private long limit;

    /**
     * Creates new instance of range input stream.
     *
     * @param in
     *            source stream
     * @param offset
     *            offset of the range
     * @param limit
     *            length of the range
     * @throws IOException
     *             on I/O exception during seeking to the specified offset
     */
    public RangeInputStream(InputStream in, long offset, long limit) throws IOException {
        super(in);
        this.limit = limit;
        IOUtils.skipFully(in, offset);
    }

    @Override
    public int read() throws IOException {
        if (limit <= 0) {
            return -1;
        }
        int b = in.read();
        if (b >= 0) {
            limit--;
        }
        return b;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        if (limit <= 0) {
            return -1;
        }
        if (len > limit) {
            len = (int) limit;
        }
        int cnt = in.read(b, off, len);
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
        n = in.skip(n);
        limit -= n;
        return n;
    }

    @Override
    public int available() throws IOException {
        int cnt = in.available();
        if (cnt > limit) {
            return (int) limit;
        }
        return cnt;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public void mark(int readlimit) {
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new IOException("mark/reset not supported");
    }

    @Override
    public boolean markSupported() {
        return false;
    }
}
