/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop;

import org.apache.commons.logging.*;
import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.ggfs.common.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * GGFS Hadoop output stream implementation.
 */
public class GridGgfsHadoopOutputStream extends OutputStream implements GridGgfsHadoopStreamEventListener {
    /** Log instance. */
    private Log log;

    /** Client logger. */
    private GridGgfsLogger clientLog;

    /** Log stream ID. */
    private long logStreamId;

    /** Server stream delegate. */
    private GridGgfsHadoopStreamDelegate delegate;

    /** Closed flag. */
    private volatile boolean closed;

    /** Flag set if stream was closed due to connection breakage. */
    private boolean connBroken;

    /** Error message. */
    private volatile String errMsg;

    /** Read time. */
    private long writeTime;

    /** User time. */
    private long userTime;

    /** Last timestamp. */
    private long lastTs;

    /** Amount of written bytes. */
    private long total;

    /**
     * Creates light output stream.
     *
     * @param delegate Server stream delegate.
     * @param log Logger to use.
     * @param clientLog Client logger.
     */
    public GridGgfsHadoopOutputStream(GridGgfsHadoopStreamDelegate delegate, Log log,
        GridGgfsLogger clientLog, long logStreamId) {
        this.delegate = delegate;
        this.log = log;
        this.clientLog = clientLog;
        this.logStreamId = logStreamId;

        lastTs = System.nanoTime();

        delegate.hadoop().addEventListener(delegate, this);
    }

    /**
     * Read start.
     */
    private void writeStart() {
        long now = System.nanoTime();

        userTime += now - lastTs;

        lastTs = now;
    }

    /**
     * Read end.
     */
    private void writeEnd() {
        long now = System.nanoTime();

        writeTime += now - lastTs;

        lastTs = now;
    }

    /** {@inheritDoc} */
    @Override public void write(@NotNull byte[] b, int off, int len) throws IOException {
        check();

        writeStart();

        try {
            delegate.hadoop().writeData(delegate, b, off, len);

            total += len;
        }
        finally {
            writeEnd();
        }
    }

    /** {@inheritDoc} */
    @Override public void write(int b) throws IOException {
        write(new byte[] {(byte)b});

        total++;
    }

    /** {@inheritDoc} */
    @Override public void flush() throws IOException {
        delegate.hadoop().flush(delegate);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        if (!closed) {
            if (log.isDebugEnabled())
                log.debug("Closing output stream: " + delegate);

            writeStart();

            delegate.hadoop().closeStream(delegate);

            markClosed(false);

            writeEnd();

            if (clientLog.isLogEnabled())
                clientLog.logCloseOut(logStreamId, userTime, writeTime, total);

            if (log.isDebugEnabled())
                log.debug("Closed output stream [delegate=" + delegate + ", writeTime=" + writeTime / 1000 +
                    ", userTime=" + userTime / 1000 + ']');
        }
        else if(connBroken)
            throw new IOException(
                "Failed to close stream, because connection was broken (data could have been lost).");
    }

    /**
     * Marks stream as closed.
     *
     * @param connBroken {@code True} if connection with server was lost.
     */
    private void markClosed(boolean connBroken) {
        // It is ok to have race here.
        if (!closed) {
            closed = true;

            delegate.hadoop().removeEventListener(delegate);

            this.connBroken = connBroken;
        }
    }

    /**
     * @throws IOException If check failed.
     */
    private void check() throws IOException {
        String errMsg0 = errMsg;

        if (errMsg0 != null)
            throw new IOException(errMsg0);

        if (closed) {
            if (connBroken)
                throw new IOException("Server connection was lost.");
            else
                throw new IOException("Stream is closed.");
        }
    }

    /** {@inheritDoc} */
    @Override public void onClose() throws IgniteCheckedException {
        markClosed(true);
    }

    /** {@inheritDoc} */
    @Override public void onError(String errMsg) {
        this.errMsg = errMsg;
    }
}
