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

package org.apache.ignite.internal.util.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.WritableByteChannel;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.NIO_OPERATION;

/**
 * Client worker for byte buffer mode.
 */
@SuppressWarnings({"unchecked"})
class ByteBufferNioClientWorker<T> extends AbstractNioClientWorker {
    /** Metrics listener. */
    @GridToStringExclude
    private final GridNioMetricsListener metricsLsnr;

    /** Read buffer. */
    @GridToStringExclude
    private final ByteBuffer readBuf;

    /**
     * @param nio Nio server.
     * @param idx Worker index
     * @param igniteInstanceName Ignite instance name.
     * @param name Worker name.
     * @param filterChain Filter chain.
     * @param metricsLsnr Metrics listener.
     * @param selectorSpins Selector spins count.
     * @param writeTimeout Write timeout.
     * @param idleTimeout Idle timeout.
     * @param directBuf Is the worker uses a direct ByteBuffer.
     * @param order Byte order for ByteBuffer.
     * @param sndQueueLimit Session send queue limit.
     * @param log Logger.
     * @throws IgniteCheckedException If selector could not be created.
     */
    private ByteBufferNioClientWorker(GridNioServer<T> nio,
        int idx,
        String igniteInstanceName,
        String name,
        GridNioFilterChain<T> filterChain,
        GridNioMetricsListener metricsLsnr,
        long selectorSpins,
        long writeTimeout,
        long idleTimeout,
        boolean directBuf,
        ByteOrder order,
        int sndQueueLimit,
        IgniteLogger log)
        throws IgniteCheckedException {
        super(
            nio,
            idx,
            igniteInstanceName,
            name,
            filterChain,
            selectorSpins,
            writeTimeout,
            idleTimeout,
            false,
            directBuf,
            order,
            sndQueueLimit,
            log);

        this.metricsLsnr = metricsLsnr;

        readBuf = directBuf ? ByteBuffer.allocateDirect(8 << 10) : ByteBuffer.allocate(8 << 10);

        readBuf.order(order);
    }

    /** {@inheritDoc} */
    @Override protected void processRead(SelectionKey key) throws IOException {
        if (skipRead) {
            try {
                U.sleep(50);
            }
            catch (IgniteInterruptedCheckedException ignored) {
                U.warn(log, "Sleep has been interrupted.");
            }

            return;
        }

        ReadableByteChannel sockCh = (ReadableByteChannel)key.channel();

        final GridSelectorNioSessionImpl ses = (GridSelectorNioSessionImpl)key.attachment();

        // Reset buffer to read bytes up to its capacity.
        readBuf.clear();

        // Attempt to read off the channel
        int cnt = sockCh.read(readBuf);

        if (cnt == -1) {
            if (log.isDebugEnabled())
                log.debug("Remote client closed connection: " + ses);

            close(ses, null);

            return;
        }
        else if (cnt == 0)
            return;

        if (log.isTraceEnabled())
            log.trace("Bytes received [sockCh=" + sockCh + ", cnt=" + cnt + ']');

        if (metricsLsnr != null)
            metricsLsnr.onBytesReceived(cnt);

        ses.bytesReceived(cnt);
        onRead(cnt);

        // Sets limit to current position and
        // resets position to 0.
        readBuf.flip();

        try {
            assert readBuf.hasRemaining();

            filterChain.onMessageReceived(ses, readBuf);

            if (readBuf.remaining() > 0) {
                LT.warn(log, "Read buffer contains data after filter chain processing (will discard " +
                    "remaining bytes) [ses=" + ses + ", remainingCnt=" + readBuf.remaining() + ']');

                readBuf.clear();
            }
        }
        catch (IgniteCheckedException e) {
            close(ses, e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void processWrite(SelectionKey key) throws IOException {
        WritableByteChannel sockCh = (WritableByteChannel)key.channel();

        final GridSelectorNioSessionImpl ses = (GridSelectorNioSessionImpl)key.attachment();

        while (true) {
            ByteBuffer buf = ses.removeMeta(GridNioServer.BUF_META_KEY);
            SessionWriteRequest req = ses.removeMeta(NIO_OPERATION.ordinal());

            // Check if there were any pending data from previous writes.
            if (buf == null) {
                assert req == null;

                req = ses.pollFuture();

                if (req == null) {
                    if (ses.procWrite.get()) {
                        ses.procWrite.set(false);

                        if (ses.writeQueue().isEmpty()) {
                            if ((key.interestOps() & SelectionKey.OP_WRITE) != 0)
                                key.interestOps(key.interestOps() & (~SelectionKey.OP_WRITE));
                        }
                        else
                            ses.procWrite.set(true);
                    }

                    break;
                }

                buf = (ByteBuffer)req.message();
            }

            if (!skipWrite) {
                int cnt = sockCh.write(buf);

                if (log.isTraceEnabled())
                    log.trace("Bytes sent [sockCh=" + sockCh + ", cnt=" + cnt + ']');

                if (metricsLsnr != null)
                    metricsLsnr.onBytesSent(cnt);

                ses.bytesSent(cnt);
                onWrite(cnt);
            }
            else {
                // For test purposes only (skipWrite is set to true in tests only).
                try {
                    U.sleep(50);
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw new IOException("Thread has been interrupted.", e);
                }
            }

            if (buf.remaining() > 0) {
                // Not all data was written.
                ses.addMeta(GridNioServer.BUF_META_KEY, buf);
                ses.addMeta(NIO_OPERATION.ordinal(), req);

                break;
            }
            else {
                // Message was successfully written.
                assert req != null;

                req.onMessageWritten();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ByteBufferNioClientWorker.class, this, super.toString());
    }

    /**
     * Creates and returns a builder for a new instance of this class.
     *
     * @return Builder for new instance.
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /** */
    @SuppressWarnings({"PublicInnerClass", "WeakerAccess"})
    public static class Builder<T> {
        /** */
        private GridNioServer<T> nio;
        /** */
        private int idx;
        /** */
        private String igniteInstanceName;
        /** */
        private String name;
        /** */
        private GridNioFilterChain<T> filterChain;
        /** */
        private GridNioMetricsListener metricsLsnr;
        /** */
        private long selectorSpins;
        /** */
        private long writeTimeout;
        /** */
        private long idleTimeout;
        /** */
        private boolean directBuf;
        /** */
        private ByteOrder order;
        /** */
        private int sndQueueLimit;
        /** */
        private IgniteLogger log;

        /**
         * @param nio Nio server.
         * @return This for chaining.
         */
        public Builder<T> nio(GridNioServer<T> nio) {
            this.nio = nio;
            return this;
        }

        /**
         * @param idx Index.
         * @return This for chaining.
         */
        public Builder<T> idx(int idx) {
            this.idx = idx;
            return this;
        }

        /**
         * @param igniteInstanceName Ignite instance name.
         * @return This for chaining.
         */
        public Builder<T> igniteInstanceName(String igniteInstanceName) {
            this.igniteInstanceName = igniteInstanceName;
            return this;
        }

        /**
         * @param name Worker name.
         * @return This for chaining.
         */
        public Builder<T> name(String name) {
            this.name = name;
            return this;
        }

        /**
         * @param filterChain Filter chain.
         * @return This for chaining.
         */
        public Builder<T> filterChain(GridNioFilterChain<T> filterChain) {
            this.filterChain = filterChain;
            return this;
        }

        /**
         * @param metricsLsnr Metrics listener.
         * @return This for chaining.
         */
        public Builder<T> metricsLsnr(GridNioMetricsListener metricsLsnr) {
            this.metricsLsnr = metricsLsnr;
            return this;
        }

        /**
         * @param selectorSpins Selector spins count.
         * @return This for chaining.
         */
        public Builder<T> selectorSpins(long selectorSpins) {
            this.selectorSpins = selectorSpins;
            return this;
        }

        /**
         * @param writeTimeout Write timeout.
         * @return This for chaining.
         */
        public Builder<T> writeTimeout(long writeTimeout) {
            this.writeTimeout = writeTimeout;
            return this;
        }

        /**
         * @param idleTimeout Idle timeout.
         * @return This for chaining.
         */
        public Builder<T> idleTimeout(long idleTimeout) {
            this.idleTimeout = idleTimeout;
            return this;
        }

        /**
         * @param directBuf Is the worker uses a direct ByteBuffer.
         * @return This for chaining.
         */
        public Builder<T> directBuf(boolean directBuf) {
            this.directBuf = directBuf;
            return this;
        }

        /**
         * @param order Byte order for ByteBuffer.
         * @return This for chaining.
         */
        public Builder<T> order(ByteOrder order) {
            this.order = order;
            return this;
        }

        /**
         * @param sndQueueLimit Session send queue limit.
         * @return This for chaining.
         */
        public Builder<T> sndQueueLimit(int sndQueueLimit) {
            this.sndQueueLimit = sndQueueLimit;
            return this;
        }

        /**
         * @param log Logger.
         * @return This for chaining.
         */
        public Builder<T> log(IgniteLogger log) {
            this.log = log;
            return this;
        }

        /**
         * Finishes building the instance.
         *
         * @return Final instance of {@link ByteBufferNioClientWorker}.
         * @throws IgniteCheckedException If NIO client worker creation failed or address is already in use.
         */
        public ByteBufferNioClientWorker<T> build() throws IgniteCheckedException {
            return new ByteBufferNioClientWorker<>(nio, idx, igniteInstanceName, name, filterChain, metricsLsnr, selectorSpins, writeTimeout, idleTimeout, directBuf, order, sndQueueLimit, log);
        }
    }
}
