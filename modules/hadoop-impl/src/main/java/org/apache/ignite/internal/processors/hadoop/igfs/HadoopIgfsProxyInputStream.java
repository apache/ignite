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
import java.io.InputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.ignite.internal.igfs.common.IgfsLogger;

/**
 * Secondary Hadoop file system input stream wrapper.
 */
public class HadoopIgfsProxyInputStream extends InputStream implements Seekable, PositionedReadable {
    /** Actual input stream to the secondary file system. */
    private final FSDataInputStream is;

    /** Client logger. */
    private final IgfsLogger clientLog;

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
    public HadoopIgfsProxyInputStream(FSDataInputStream is, IgfsLogger clientLog, long logStreamId) {
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