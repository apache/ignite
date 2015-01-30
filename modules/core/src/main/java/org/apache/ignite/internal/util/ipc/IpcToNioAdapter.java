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

package org.apache.ignite.internal.util.ipc;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.internal.util.nio.*;

import java.io.*;
import java.nio.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Allows to re-use existing {@link GridNioFilter}s on IPC (specifically shared memory IPC)
 * communications.
 *
 * Note that this class consumes an entire thread inside {@link #serve()} method
 * in order to serve one {@link IpcEndpoint}.
 */
public class IpcToNioAdapter<T> {
    /** */
    private final IpcEndpoint endp;

    /** */
    private final GridNioFilterChain<T> chain;

    /** */
    private final GridNioSessionImpl ses;

    /** */
    private final AtomicReference<CountDownLatch> latchRef = new AtomicReference<>();

    /** */
    private final ByteBuffer writeBuf;

    /** */
    private final GridNioMetricsListener metricsLsnr;

    /** */
    private final GridNioMessageWriter msgWriter;

    /**
     * @param metricsLsnr Metrics listener.
     * @param log Log.
     * @param endp Endpoint.
     * @param msgWriter Message writer.
     * @param lsnr Listener.
     * @param filters Filters.
     */
    public IpcToNioAdapter(GridNioMetricsListener metricsLsnr, IgniteLogger log, IpcEndpoint endp,
                           GridNioMessageWriter msgWriter, GridNioServerListener<T> lsnr, GridNioFilter... filters) {
        assert metricsLsnr != null;
        assert msgWriter != null;

        this.metricsLsnr = metricsLsnr;
        this.endp = endp;
        this.msgWriter = msgWriter;

        chain = new GridNioFilterChain<>(log, lsnr, new HeadFilter(), filters);
        ses = new GridNioSessionImpl(chain, null, null, true);

        writeBuf = ByteBuffer.allocate(8 << 10);

        writeBuf.order(ByteOrder.nativeOrder());
    }

    /**
     * Serves given set of listeners repeatedly reading data from the endpoint.
     *
     * @throws InterruptedException If interrupted.
     */
    public void serve() throws InterruptedException {
        try {
            chain.onSessionOpened(ses);

            InputStream in = endp.inputStream();

            ByteBuffer readBuf = ByteBuffer.allocate(8 << 10);

            readBuf.order(ByteOrder.nativeOrder());

            assert readBuf.hasArray();

            while (!Thread.interrupted()) {
                int pos = readBuf.position();

                int read = in.read(readBuf.array(), pos, readBuf.remaining());

                if (read > 0) {
                    metricsLsnr.onBytesReceived(read);

                    readBuf.position(0);
                    readBuf.limit(pos + read);

                    chain.onMessageReceived(ses, readBuf);

                    if (readBuf.hasRemaining())
                        readBuf.compact();
                    else
                        readBuf.clear();

                    CountDownLatch latch = latchRef.get();

                    if (latch != null)
                        latch.await();
                }
                else if (read < 0) {
                    endp.close();

                    break; // And close below.
                }
            }
        }
        catch (Exception e) {
            chain.onExceptionCaught(ses, new IgniteCheckedException("Failed to read from IPC endpoint.", e));
        }
        finally {
            try {
                // Assuming remote end closed connection - pushing event from head to tail.
                chain.onSessionClosed(ses);
            }
            catch (IgniteCheckedException e) {
                chain.onExceptionCaught(ses, new IgniteCheckedException("Failed to process session close event " +
                    "for IPC endpoint.", e));
            }
        }
    }

    /**
     * Handles write events on chain.
     *
     * @param msg Buffer to send.
     * @return Send result.
     */
    private GridNioFuture<?> send(GridTcpCommunicationMessageAdapter msg) {
        assert writeBuf.hasArray();

        try {
            // This method is called only on handshake,
            // so we don't need to provide node ID for
            // rolling updates support.
            int cnt = msgWriter.writeFully(null, msg, endp.outputStream(), writeBuf);

            metricsLsnr.onBytesSent(cnt);
        }
        catch (IOException | IgniteCheckedException e) {
            return new GridNioFinishedFuture<Object>(e);
        }

        return new GridNioFinishedFuture<>((Object)null);
    }

    /**
     * Filter forwarding messages from chain's head to this server.
     */
    private class HeadFilter extends GridNioFilterAdapter {
        /**
         * Assigns filter name.
         */
        protected HeadFilter() {
            super("HeadFilter");
        }

        /** {@inheritDoc} */
        @Override public void onSessionOpened(GridNioSession ses) throws IgniteCheckedException {
            proceedSessionOpened(ses);
        }

        /** {@inheritDoc} */
        @Override public void onSessionClosed(GridNioSession ses) throws IgniteCheckedException {
            proceedSessionClosed(ses);
        }

        /** {@inheritDoc} */
        @Override public void onExceptionCaught(GridNioSession ses, IgniteCheckedException ex) throws IgniteCheckedException {
            proceedExceptionCaught(ses, ex);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg) {
            assert ses == IpcToNioAdapter.this.ses;

            return send((GridTcpCommunicationMessageAdapter)msg);
        }

        /** {@inheritDoc} */
        @Override public void onMessageReceived(GridNioSession ses, Object msg) throws IgniteCheckedException {
            proceedMessageReceived(ses, msg);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<?> onPauseReads(GridNioSession ses) throws IgniteCheckedException {
            // This call should be synced externally to avoid races.
            boolean b = latchRef.compareAndSet(null, new CountDownLatch(1));

            assert b;

            return new GridNioFinishedFuture<>(b);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<?> onResumeReads(GridNioSession ses) throws IgniteCheckedException {
            // This call should be synced externally to avoid races.
            CountDownLatch latch = latchRef.getAndSet(null);

            if (latch != null)
                latch.countDown();

            return new GridNioFinishedFuture<Object>(latch != null);
        }

        /** {@inheritDoc} */
        @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) {
            assert ses == IpcToNioAdapter.this.ses;

            boolean closed = IpcToNioAdapter.this.ses.setClosed();

            if (closed)
                endp.close();

            return new GridNioFinishedFuture<>(closed);
        }

        /** {@inheritDoc} */
        @Override public void onSessionIdleTimeout(GridNioSession ses) throws IgniteCheckedException {
            proceedSessionIdleTimeout(ses);
        }

        /** {@inheritDoc} */
        @Override public void onSessionWriteTimeout(GridNioSession ses) throws IgniteCheckedException {
            proceedSessionWriteTimeout(ses);
        }
    }
}
