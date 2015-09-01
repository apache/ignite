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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.ignite.internal.igfs.common.IgfsLogger;

/**
 * Secondary Hadoop file system output stream wrapper.
 */
public class HadoopIgfsProxyOutputStream extends OutputStream {
    /** Actual output stream. */
    private FSDataOutputStream os;

    /** Client logger. */
    private final IgfsLogger clientLog;

    /** Log stream ID. */
    private final long logStreamId;

    /** Read time. */
    private long writeTime;

    /** User time. */
    private long userTime;

    /** Last timestamp. */
    private long lastTs;

    /** Amount of written bytes. */
    private long total;

    /** Closed flag. */
    private boolean closed;

    /**
     * Constructor.
     *
     * @param os Actual output stream.
     * @param clientLog Client logger.
     * @param logStreamId Log stream ID.
     */
    public HadoopIgfsProxyOutputStream(FSDataOutputStream os, IgfsLogger clientLog, long logStreamId) {
        assert os != null;
        assert clientLog != null;

        this.os = os;
        this.clientLog = clientLog;
        this.logStreamId = logStreamId;

        lastTs = System.nanoTime();
    }

    /** {@inheritDoc} */
    @Override public synchronized void write(int b) throws IOException {
        writeStart();

        try {
            os.write(b);
        }
        finally {
            writeEnd();
        }

        total++;
    }

    /** {@inheritDoc} */
    @Override public synchronized void write(byte[] b) throws IOException {
        writeStart();

        try {
            os.write(b);
        }
        finally {
            writeEnd();
        }

        total += b.length;
    }

    /** {@inheritDoc} */
    @Override public synchronized void write(byte[] b, int off, int len) throws IOException {
        writeStart();

        try {
            os.write(b, off, len);
        }
        finally {
            writeEnd();
        }

        total += len;
    }

    /** {@inheritDoc} */
    @Override public synchronized void flush() throws IOException {
        writeStart();

        try {
            os.flush();
        }
        finally {
            writeEnd();
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() throws IOException {
        if (!closed) {
            closed = true;

            writeStart();

            try {
                os.close();
            }
            finally {
                writeEnd();
            }

            if (clientLog.isLogEnabled())
                clientLog.logCloseOut(logStreamId, userTime, writeTime, total);
        }
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
}