/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.hadoop.igfs;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.commons.logging.Log;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.igfs.common.IgfsLogger;
import org.jetbrains.annotations.NotNull;

/**
 * IGFS Hadoop output stream implementation.
 */
public class HadoopIgfsOutputStream extends OutputStream implements HadoopIgfsStreamEventListener {
    /** Log instance. */
    private Log log;

    /** Client logger. */
    private IgfsLogger clientLog;

    /** Log stream ID. */
    private long logStreamId;

    /** Server stream delegate. */
    private HadoopIgfsStreamDelegate delegate;

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
    public HadoopIgfsOutputStream(HadoopIgfsStreamDelegate delegate, Log log,
        IgfsLogger clientLog, long logStreamId) {
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