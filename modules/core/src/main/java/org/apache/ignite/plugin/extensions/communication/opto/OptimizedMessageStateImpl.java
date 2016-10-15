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

package org.apache.ignite.plugin.extensions.communication.opto;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.nio.GridNioMetricsListener;
import org.apache.ignite.internal.util.nio.GridSelectorNioSessionImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.LinkedList;

/**
 * Message state implementation.
 */
public class OptimizedMessageStateImpl implements OptimizedMessageState {
    /** Socket channel. */
    private final WritableByteChannel sockCh;

    /** First buffer. */
    private final ByteBuffer sockChBuf;

    /** Session. */
    private final GridSelectorNioSessionImpl ses;

    /** Metrics listener. */
    private final GridNioMetricsListener metricsLsnr;

    /** Logger. */
    private final IgniteLogger log;

    /** Additional buffers. */
    private LinkedList<ByteBuffer> bufs;

    /** Whether channel write was performed. */
    private boolean chWritten;

    /**
     * Constructor.
     *
     * @param sockCh Channel.
     * @param sockChBuf Channel buffer.
     * @param ses Session.
     * @param metricsLsnr Metrics listener.
     * @param log Logger.
     */
    public OptimizedMessageStateImpl(WritableByteChannel sockCh, ByteBuffer sockChBuf, GridSelectorNioSessionImpl ses,
        GridNioMetricsListener metricsLsnr, IgniteLogger log) {
        this.sockCh = sockCh;
        this.sockChBuf = sockChBuf;
        this.ses = ses;
        this.metricsLsnr = metricsLsnr;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer currentBuffer() {
        return bufs == null ? sockChBuf : bufs.getLast();
    }

    /** {@inheritDoc} */
    @Override public void pushBuffer() {
        if (currentBuffer() == sockChBuf && !finished())
            writeToChannel();
        else
            addHeapBuffer();
    }

    /**
     * Callback invoked on write cycle start.
     */
    public void onBeforeWrite() {
        chWritten = false;

        while (bufs != null && sockChBuf.hasRemaining()) {
            ByteBuffer buf = bufs.getFirst();

            int sockRemaining = sockChBuf.remaining();
            int bufRemaining = buf.remaining();

            sockChBuf.put(buf.array(), buf.position(), Math.min(sockRemaining, bufRemaining));

            if (buf.hasRemaining()) {
                // Buffer is written partially, compact and stop.
                buf.compact();

                break;
            }
            else {
                // Buffer is written completely.
                bufs.removeFirst();

                if (bufs.size() == 0)
                    bufs = null;
            }

            if (!sockChBuf.hasRemaining()) {
                writeToChannel();
            }
        }
    }

    /**
     * Write socket buffer to the channel.
     */
    private void writeToChannel() {
        sockChBuf.flip();

        int cnt;

        try {
            cnt = sockCh.write(sockChBuf);
        }
        catch (IOException e) {
            throw new IgniteException("Failed to write to socket channel.", e);
        }

        if (log.isTraceEnabled())
            log.trace("Bytes sent [sockCh=" + sockCh + ", cnt=" + cnt + ']');

        if (metricsLsnr != null)
            metricsLsnr.onBytesSent(cnt);

        ses.bytesSent(cnt);

        if (sockChBuf.hasRemaining())
            sockChBuf.compact();
        else
            sockChBuf.clear();

        chWritten = true;
    }

    /**
     * Callback invoked on write cycle finish.
     */
    public void onAfterWrite() {
        if (chWritten) {
            if (bufs != null)
                bufs.getLast().flip();
        }
        else {
            writeToChannel();
        }
    }

    /**
     * @return Channel written flag.
     */
    public boolean finished() {
        return chWritten;
    }

    /**
     * Create new byte buffer.
     *
     * @return New byte buffer.
     */
    private ByteBuffer addHeapBuffer() {
        ByteBuffer newBuf = ByteBuffer.allocate(sockChBuf.capacity());

        if (bufs == null)
            bufs = new LinkedList<>();
        else
            bufs.getLast().flip();

        bufs.add(newBuf);

        return newBuf;
    }
}
