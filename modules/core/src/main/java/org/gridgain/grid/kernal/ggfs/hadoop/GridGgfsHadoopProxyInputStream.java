/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop;

import org.apache.hadoop.fs.*;

import java.io.*;

/**
 * Secondary Hadoop file system input stream wrapper.
 */
public class GridGgfsHadoopProxyInputStream extends InputStream implements Seekable, PositionedReadable {
    /** Actual input stream to the secondary file system. */
    private final FSDataInputStream is;

    /** Client logger. */
    private final GridGgfsHadoopLogger clientLog;

    /** Log stream ID. */
    private final long logStreamId;

    /** Read time. */
    private long readTime;

    /** User time. */
    private long userTime;

    /** Last timestamp. */
    private long lastTs;

    /** Amount of read bytes. */
    private long total;

    /** Closed flag. */
    private boolean closed;

    /**
     * Constructor.
     *
     * @param is Actual input stream to the secondary file system.
     * @param clientLog Client log.
     */
    public GridGgfsHadoopProxyInputStream(FSDataInputStream is, GridGgfsHadoopLogger clientLog, long logStreamId) {
        assert is != null;
        assert clientLog != null;

        this.is = is;
        this.clientLog = clientLog;
        this.logStreamId = logStreamId;

        lastTs = System.nanoTime();
    }

    /** {@inheritDoc} */
    @Override public synchronized int read(byte[] b) throws IOException {
        readStart();

        int res;

        try {
            res = is.read(b);
        }
        finally {
            readEnd();
        }

        if (res != -1)
            total += res;

        return res;
    }

    /** {@inheritDoc} */
    @Override public synchronized int read(byte[] b, int off, int len) throws IOException {
        readStart();

        int res;

        try {
            res = super.read(b, off, len);
        }
        finally {
            readEnd();
        }

        if (res != -1)
            total += res;

        return res;
    }

    /** {@inheritDoc} */
    @Override public synchronized long skip(long n) throws IOException {
        readStart();

        long res;

        try {
            res =  is.skip(n);
        }
        finally {
            readEnd();
        }

        if (clientLog.isLogEnabled())
            clientLog.logSkip(logStreamId, res);

        return res;
    }

    /** {@inheritDoc} */
    @Override public synchronized int available() throws IOException {
        readStart();

        try {
            return is.available();
        }
        finally {
            readEnd();
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() throws IOException {
        if (!closed) {
            closed = true;

            readStart();

            try {
                is.close();
            }
            finally {
                readEnd();
            }

            if (clientLog.isLogEnabled())
                clientLog.logCloseIn(logStreamId, userTime, readTime, total);
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void mark(int readLimit) {
        readStart();

        try {
            is.mark(readLimit);
        }
        finally {
            readEnd();
        }

        if (clientLog.isLogEnabled())
            clientLog.logMark(logStreamId, readLimit);
    }

    /** {@inheritDoc} */
    @Override public synchronized void reset() throws IOException {
        readStart();

        try {
            is.reset();
        }
        finally {
            readEnd();
        }

        if (clientLog.isLogEnabled())
            clientLog.logReset(logStreamId);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean markSupported() {
        readStart();

        try {
            return is.markSupported();
        }
        finally {
            readEnd();
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized int read() throws IOException {
        readStart();

        int res;

        try {
            res = is.read();
        }
        finally {
            readEnd();
        }

        if (res != -1)
            total++;

        return res;
    }

    /** {@inheritDoc} */
    @Override public synchronized int read(long pos, byte[] buf, int off, int len) throws IOException {
        readStart();

        int res;

        try {
            res = is.read(pos, buf, off, len);
        }
        finally {
            readEnd();
        }

        if (res != -1)
            total += res;

        if (clientLog.isLogEnabled())
            clientLog.logRandomRead(logStreamId, pos, res);

        return res;
    }

    /** {@inheritDoc} */
    @Override public synchronized void readFully(long pos, byte[] buf, int off, int len) throws IOException {
        readStart();

        try {
            is.readFully(pos, buf, off, len);
        }
        finally {
            readEnd();
        }

        total += len;

        if (clientLog.isLogEnabled())
            clientLog.logRandomRead(logStreamId, pos, len);
    }

    /** {@inheritDoc} */
    @Override public synchronized void readFully(long pos, byte[] buf) throws IOException {
        readStart();

        try {
            is.readFully(pos, buf);
        }
        finally {
            readEnd();
        }

        total += buf.length;

        if (clientLog.isLogEnabled())
            clientLog.logRandomRead(logStreamId, pos, buf.length);
    }

    /** {@inheritDoc} */
    @Override public synchronized void seek(long pos) throws IOException {
        readStart();

        try {
            is.seek(pos);
        }
        finally {
            readEnd();
        }

        if (clientLog.isLogEnabled())
            clientLog.logSeek(logStreamId, pos);
    }

    /** {@inheritDoc} */
    @Override public synchronized long getPos() throws IOException {
        readStart();

        try {
            return is.getPos();
        }
        finally {
            readEnd();
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean seekToNewSource(long targetPos) throws IOException {
        readStart();

        try {
            return is.seekToNewSource(targetPos);
        }
        finally {
            readEnd();
        }
    }

    /**
     * Read start.
     */
    private void readStart() {
        long now = System.nanoTime();

        userTime += now - lastTs;

        lastTs = now;
    }

    /**
     * Read end.
     */
    private void readEnd() {
        long now = System.nanoTime();

        readTime += now - lastTs;

        lastTs = now;
    }
}
