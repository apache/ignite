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
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.nio.ssl.GridNioSslFilter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.MSG_WRITER;
import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.NIO_OPERATION;

/**
 * Client worker for direct mode.
 */
@SuppressWarnings("unchecked")
class DirectNioClientWorker<T> extends AbstractNioClientWorker {
    /** Metrics listener. */
    private final GridNioMetricsListener metricsLsnr;

    /** Ssl filter. */
    private final GridNioSslFilter sslFilter;

    /** Writer factory. */
    private final GridNioMessageWriterFactory writerFactory;

    /**
     * @param nio Nio server.
     * @param idx Worker index
     * @param igniteInstanceName Ignite instance name.
     * @param name Worker name.
     * @param filterChain Filter chain.
     * @param metricsLsnr Metrics listener.
     * @param sslFilter Ssl filter.
     * @param writerFactory Writer factory.
     * @param selectorSpins Selector spins count.
     * @param writeTimeout Write timeout.
     * @param idleTimeout Idle timeout.
     * @param directBuf Is the worker uses a direct ByteBuffer.
     * @param order Byte order for ByteBuffer.
     * @param sndQueueLimit Session send queue limit.
     * @param log Logger.
     * @throws IgniteCheckedException If selector could not be created.
     */
    private DirectNioClientWorker(
        GridNioServer<T> nio,
        int idx,
        String igniteInstanceName,
        String name,
        GridNioFilterChain<T> filterChain,
        GridNioMetricsListener metricsLsnr,
        GridNioSslFilter sslFilter,
        GridNioMessageWriterFactory writerFactory,
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
            true,
            directBuf,
            order,
            sndQueueLimit,
            log);
        this.metricsLsnr = metricsLsnr;
        this.sslFilter = sslFilter;
        this.writerFactory = writerFactory;
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

        ByteBuffer readBuf = ses.readBuffer();

        // Attempt to read off the channel.
        int cnt = sockCh.read(readBuf);

        if (cnt == -1) {
            if (log.isDebugEnabled())
                log.debug("Remote client closed connection: " + ses);

            closeSession(ses, null);

            return;
        }

        if (log.isTraceEnabled())
            log.trace("Bytes received [sockCh=" + sockCh + ", cnt=" + cnt + ']');

        if (cnt == 0)
            return;

        if (metricsLsnr != null)
            metricsLsnr.onBytesReceived(cnt);

        ses.bytesReceived(cnt);
        onRead(cnt);

        readBuf.flip();

        assert readBuf.hasRemaining();

        try {
            filterChain.onMessageReceived(ses, readBuf);

            if (readBuf.hasRemaining())
                readBuf.compact();
            else
                readBuf.clear();

            if (ses.hasSystemMessage())
                registerWrite(ses);
        }
        catch (IgniteCheckedException e) {
            closeSession(ses, e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void processWrite(SelectionKey key) throws IOException {
        if (sslFilter != null)
            processWriteSsl(key);
        else
            processWrite0(key);
    }

    /**
     * Processes write-ready event on the key.
     *
     * @param key Key that is ready to be written.
     * @throws IOException If write failed.
     */
    private void processWriteSsl(SelectionKey key) throws IOException {
        WritableByteChannel sockCh = (WritableByteChannel)key.channel();

        GridSelectorNioSessionImpl ses = (GridSelectorNioSessionImpl)key.attachment();

        MessageWriter writer = ses.meta(MSG_WRITER.ordinal());

        if (writer == null) {
            try {
                ses.addMeta(MSG_WRITER.ordinal(), writer = writerFactory.writer(ses));
            }
            catch (IgniteCheckedException e) {
                throw new IOException("Failed to create message writer.", e);
            }
        }

        boolean handshakeFinished = sslFilter.lock(ses);

        try {
            writeSslSystem(ses, sockCh);

            if (!handshakeFinished)
                return;

            ByteBuffer sslNetBuf = ses.removeMeta(GridNioServer.BUF_META_KEY);

            if (sslNetBuf != null) {
                int cnt = sockCh.write(sslNetBuf);

                if (metricsLsnr != null)
                    metricsLsnr.onBytesSent(cnt);

                ses.bytesSent(cnt);

                if (sslNetBuf.hasRemaining()) {
                    ses.addMeta(GridNioServer.BUF_META_KEY, sslNetBuf);

                    return;
                }
            }

            ByteBuffer buf = ses.writeBuffer();

            if (ses.meta(GridNioServer.WRITE_BUF_LIMIT) != null)
                buf.limit((int)ses.meta(GridNioServer.WRITE_BUF_LIMIT));

            SessionWriteRequest req = ses.removeMeta(NIO_OPERATION.ordinal());

            while (true) {
                if (req == null) {
                    req = systemMessage(ses);

                    if (req == null) {
                        req = ses.pollFuture();

                        if (req == null && buf.position() == 0) {
                            if ((key.interestOps() & SelectionKey.OP_WRITE) != 0)
                                key.interestOps(key.interestOps() & (~SelectionKey.OP_WRITE));

                            break;
                        }
                    }
                }

                Message msg;
                boolean finished = false;

                if (req != null) {
                    msg = (Message)req.message();

                    assert msg != null;

                    if (writer != null)
                        writer.setCurrentWriteClass(msg.getClass());

                    finished = msg.writeTo(buf, writer);

                    if (finished && writer != null)
                        writer.reset();
                }

                // Fill up as many messages as possible to write buffer.
                while (finished) {
                    req.onMessageWritten();

                    req = systemMessage(ses);

                    if (req == null)
                        req = ses.pollFuture();

                    if (req == null)
                        break;

                    msg = (Message)req.message();

                    assert msg != null;

                    if (writer != null)
                        writer.setCurrentWriteClass(msg.getClass());

                    finished = msg.writeTo(buf, writer);

                    if (finished && writer != null)
                        writer.reset();
                }

                int sesBufLimit = buf.limit();
                int sesCap = buf.capacity();

                buf.flip();

                buf = sslFilter.encrypt(ses, buf);

                ByteBuffer sesBuf = ses.writeBuffer();

                sesBuf.clear();

                if (sesCap - buf.limit() < 0) {
                    int limit = sesBufLimit + (sesCap - buf.limit()) - 100;

                    ses.addMeta(GridNioServer.WRITE_BUF_LIMIT, limit);

                    sesBuf.limit(limit);
                }

                assert buf.hasRemaining();

                if (!skipWrite) {
                    int cnt = sockCh.write(buf);

                    if (log.isTraceEnabled())
                        log.trace("Bytes sent [sockCh=" + sockCh + ", cnt=" + cnt + ']');

                    if (metricsLsnr != null)
                        metricsLsnr.onBytesSent(cnt);

                    ses.bytesSent(cnt);
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

                ses.addMeta(NIO_OPERATION.ordinal(), req);

                if (buf.hasRemaining()) {
                    ses.addMeta(GridNioServer.BUF_META_KEY, buf);

                    break;
                }
                else {
                    buf = ses.writeBuffer();

                    if (ses.meta(GridNioServer.WRITE_BUF_LIMIT) != null)
                        buf.limit((int)ses.meta(GridNioServer.WRITE_BUF_LIMIT));
                }
            }
        }
        finally {
            sslFilter.unlock(ses);
        }
    }

    /**
     * @param ses NIO session.
     * @param sockCh Socket channel.
     * @throws IOException If failed.
     */
    private void writeSslSystem(GridSelectorNioSessionImpl ses, WritableByteChannel sockCh)
        throws IOException {
        ConcurrentLinkedQueue<ByteBuffer> queue = ses.meta(GridNioServer.BUF_SSL_SYSTEM_META_KEY);

        assert queue != null;

        ByteBuffer buf;

        while ((buf = queue.peek()) != null) {
            int cnt = sockCh.write(buf);

            if (metricsLsnr != null)
                metricsLsnr.onBytesSent(cnt);

            ses.bytesSent(cnt);

            if (!buf.hasRemaining())
                queue.poll();
            else
                break;
        }
    }

    /**
     * @param ses Session.
     * @return System message request.
     */
    private SessionWriteRequest systemMessage(GridSelectorNioSessionImpl ses) {
        if (ses.hasSystemMessage()) {
            Object msg = ses.systemMessage();

            SessionWriteRequest req = new SessionWriteRequestImpl(ses, msg, true, true);

            assert !ses.hasSystemMessage();

            return req;
        }

        return null;
    }

    /**
     * Processes write-ready event on the key.
     *
     * @param key Key that is ready to be written.
     * @throws IOException If write failed.
     */
    private void processWrite0(SelectionKey key) throws IOException {
        WritableByteChannel sockCh = (WritableByteChannel)key.channel();

        GridSelectorNioSessionImpl ses = (GridSelectorNioSessionImpl)key.attachment();
        ByteBuffer buf = ses.writeBuffer();
        SessionWriteRequest req = ses.removeMeta(NIO_OPERATION.ordinal());

        MessageWriter writer = ses.meta(MSG_WRITER.ordinal());

        if (writer == null) {
            try {
                ses.addMeta(MSG_WRITER.ordinal(), writer = writerFactory.writer(ses));
            }
            catch (IgniteCheckedException e) {
                throw new IOException("Failed to create message writer.", e);
            }
        }

        if (req == null) {
            req = systemMessage(ses);

            if (req == null) {
                req = ses.pollFuture();

                if (req == null && buf.position() == 0) {
                    if ((key.interestOps() & SelectionKey.OP_WRITE) != 0)
                        key.interestOps(key.interestOps() & (~SelectionKey.OP_WRITE));

                    return;
                }
            }
        }

        Message msg;
        boolean finished = false;

        if (req != null) {
            msg = (Message)req.message();

            assert msg != null : req;

            if (writer != null)
                writer.setCurrentWriteClass(msg.getClass());

            finished = msg.writeTo(buf, writer);

            if (finished && writer != null)
                writer.reset();
        }

        // Fill up as many messages as possible to write buffer.
        while (finished) {
            req.onMessageWritten();

            req = systemMessage(ses);

            if (req == null)
                req = ses.pollFuture();

            if (req == null)
                break;

            msg = (Message)req.message();

            assert msg != null;

            if (writer != null)
                writer.setCurrentWriteClass(msg.getClass());

            finished = msg.writeTo(buf, writer);

            if (finished && writer != null)
                writer.reset();
        }

        buf.flip();

        assert buf.hasRemaining();

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

        if (buf.hasRemaining() || !finished) {
            buf.compact();

            ses.addMeta(NIO_OPERATION.ordinal(), req);
        }
        else
            buf.clear();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DirectNioClientWorker.class, this, super.toString());
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
        private GridNioSslFilter sslFilter;
        /** */
        private GridNioMessageWriterFactory writerFactory;
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
         * @param idx Worker index.
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
         * @param sslFilter Ssl filter.
         * @return This for chaining.
         */
        public Builder<T> sslFilter(GridNioSslFilter sslFilter) {
            this.sslFilter = sslFilter;
            return this;
        }

        /**
         * @param writerFactory Writer factory.
         * @return This for chaining.
         */
        public Builder<T> writerFactory(GridNioMessageWriterFactory writerFactory) {
            this.writerFactory = writerFactory;
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
         * @return Final instance of {@link DirectNioClientWorker}.
         * @throws IgniteCheckedException If NIO client worker creation failed or address is already in use.
         */
        public DirectNioClientWorker build() throws IgniteCheckedException {
            return new DirectNioClientWorker<>(nio, idx, igniteInstanceName, name, filterChain, metricsLsnr, sslFilter, writerFactory, selectorSpins, writeTimeout, idleTimeout, directBuf, order, sndQueueLimit, log);
        }
    }
}
