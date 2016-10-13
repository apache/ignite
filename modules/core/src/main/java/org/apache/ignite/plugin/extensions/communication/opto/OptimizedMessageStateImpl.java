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
import java.nio.channels.SocketChannel;
import java.util.LinkedList;

/**
 * Message state implementation.
 */
public class OptimizedMessageStateImpl implements OptimizedMessageState {
    /** Socket channel. */
    private final SocketChannel sockCh;

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
    public OptimizedMessageStateImpl(SocketChannel sockCh, ByteBuffer sockChBuf, GridSelectorNioSessionImpl ses,
        GridNioMetricsListener metricsLsnr, IgniteLogger log) {
        this.sockCh = sockCh;
        this.sockChBuf = sockChBuf;
        this.ses = ses;
        this.metricsLsnr = metricsLsnr;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer buffer() {
        return bufs == null ? sockChBuf : bufs.getLast();
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer pushBuffer() {
        if (bufs == null) {
            sockChBuf.flip();

            if (chWritten)
                // We already written something to the socket before. Hence, the only option is to use new buffer.
                return newByteBuffer();
            else {
                // Socket write attempt. Try to re-use the buffer.
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

                return sockChBuf;
            }
        }
        else {
            bufs.getLast().flip();

            return newByteBuffer();
        }
    }

    /**
     * Create new byte buffer.
     *
     * @return New byte buffer.
     */
    private ByteBuffer newByteBuffer() {
        ByteBuffer newBuf = ByteBuffer.allocate(sockChBuf.capacity());

        if (bufs == null)
            bufs = new LinkedList<>();

        bufs.add(newBuf);

        return newBuf;
    }
}
