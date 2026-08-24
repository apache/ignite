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

package org.apache.ignite.spi.discovery.tcp;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import java.util.concurrent.locks.ReentrantLock;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSocket;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.managers.communication.DiscoveryMarshalling;
import org.apache.ignite.internal.managers.communication.UnknownMessageException;
import org.apache.ignite.internal.util.CommonUtils;
import org.apache.ignite.internal.util.nio.MessageSerialization;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Handles I/O operations between discovery nodes in the cluster. This class encapsulates the socket connection used
 * by the {@link TcpDiscoverySpi} to exchange discovery protocol messages between nodes.
 * <p>
 * Currently, there are two modes for message serialization:
 * <ul>
 *     <li>Using {@link MessageSerializer} for messages implementing the {@link Message} interface.</li>
 *     <li>Deprecated: Using {@link JdkMarshaller} for messages that have not yet been refactored.</li>
 * </ul>
 * A leading byte is used to distinguish between the modes. The byte will be removed in future.
 */
public class TcpDiscoveryIoSession implements AutoCloseable {
    /** Default size of buffer used for buffering socket in/out. */
    private static final int DFLT_SOCK_BUFFER_SIZE = 8192;

    /** Size of the intermediate buffer a message is deserialized through. */
    private static final int READ_BUFFER_SIZE = 100;

    /** */
    private final GridKernalContext ctx;

    /** */
    private final MessageFactory<?> msgFactory;

    /** */
    private final IgniteLogger log;

    /** */
    private final Socket sock;

    /** */
    private final TcpDiscoveryMessageSerializer msgSer;

    /** */
    private final DirectMessageReader msgReader;

    /** */
    private final ByteBuffer readBuf;

    /** Buffered socket output stream. */
    private final OutputStream out;

    /** Buffered socket input stream. */
    private final CompositeInputStream in;

    /** */
    private final ReentrantLock sesWriteLock = new ReentrantLock();

    /**
     * Creates a new discovery I/O session bound to the given socket.
     *
     * @param ctx Kernal context.
     * @param sock Socket connected to a remote discovery node.
     * @throws IgniteException If an I/O error occurs while initializing buffers.
     */
    TcpDiscoveryIoSession(GridKernalContext ctx, Socket sock) {
        this.sock = sock;
        this.ctx = ctx;
        this.msgFactory = ctx.messageFactory();
        this.log = ctx.log(getClass());

        readBuf = ByteBuffer.allocate(READ_BUFFER_SIZE);
        msgReader = new DirectMessageReader(msgFactory, null);

        msgSer = new TcpDiscoveryMessageSerializer(ctx);

        try {
            int sendBufSize = sock.getSendBufferSize() > 0 ? sock.getSendBufferSize() : DFLT_SOCK_BUFFER_SIZE;
            int rcvBufSize = sock.getReceiveBufferSize() > 0 ? sock.getReceiveBufferSize() : DFLT_SOCK_BUFFER_SIZE;

            out = new BufferedOutputStream(sock.getOutputStream(), sendBufSize);
            in = new CompositeInputStream(new BufferedInputStream(sock.getInputStream(), rcvBufSize));
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Writes a discovery message to the underlying socket output stream.
     *
     * @param msg Message to send to the remote node.
     * @throws IgniteCheckedException If serialization fails.
     */
    void writeMessage(TcpDiscoveryAbstractMessage msg) throws IgniteCheckedException, IOException {
        sesWriteLock.lock();

        try {
            msgSer.writeTo(msg, out);

            out.flush();
        }
        catch (Exception e) {
            // See Message#directType()
            if (X.hasCause(e, UnknownMessageException.class))
                throw e;

            // Keep logic similar to `U.marshal(...)`.
            if (e instanceof IgniteCheckedException)
                throw (IgniteCheckedException)e;

            throw new IgniteCheckedException(e);
        }
        finally {
            sesWriteLock.unlock();
        }
    }

    /**
     * Reads the next discovery message from the socket input stream limiting read time.
     *
     * @param timeout Socket read timeout for this operation, {@code 0} means infinite.
     * @param <T> Type of the expected message.
     * @return Deserialized message instance.
     * @throws IgniteCheckedException If deserialization fails.
     */
    <T extends Message> T readMessage(long timeout) throws IgniteCheckedException, IOException {
        try (SocketTimeoutScope ignored = withTimeout(timeout)) {
            return readMessage();
        }
    }

    /**
     * Reads the next discovery message from the socket input stream.
     *
     * @param <T> Type of the expected message.
     * @return Deserialized message instance.
     * @throws IgniteCheckedException If deserialization fails.
     */
    <T extends Message> T readMessage() throws IgniteCheckedException, IOException {
        try {
            byte b0 = (byte)in.read();
            byte b1 = (byte)in.read();

            short msgType = CommonUtils.makeMessageType(b0, b1);

            Message msg;

            try {
                msg = msgFactory.create(msgType);
            }
            catch (IgniteException e) {
                detectSslAlert(b0, b1);

                // 'Invalid message type' should not be lost.
                throw e;
            }

            msgReader.reset();
            msgReader.setBuffer(readBuf);

            boolean finished;

            do {
                readBuf.clear();

                int read = in.read(readBuf.array(), readBuf.position(), readBuf.remaining());

                if (read == -1)
                    throw new EOFException("Connection closed before message was fully read.");

                readBuf.limit(read);

                finished = MessageSerialization.readFrom(msgFactory, msg, msgReader);

                // Server Discovery only sends next message to next Server upon receiving a receipt for the previous one.
                // This behaviour guarantees that we never read a next message from the buffer right after the end of
                // the previous message. But it is not guaranteed with Client Discovery where messages aren't acknowledged.
                // Thus, we have to keep the uprocessed bytes read from the socket. It won't return them again.
                if (readBuf.hasRemaining()) {
                    byte[] unprocessedReadTail = new byte[readBuf.remaining()];

                    readBuf.get(unprocessedReadTail, 0, readBuf.remaining());

                    in.attachByteArray(unprocessedReadTail);
                }
            }
            while (!finished);

            DiscoveryMarshalling.unmarshal(msg, ctx);

            return (T)msg;
        }
        catch (Exception e) {
            if (e instanceof UnknownMessageException)
                throw e;

            // Keep logic similar to `U.marshal(...)`.
            if (e instanceof IgniteCheckedException)
                throw (IgniteCheckedException)e;

            throw new IgniteCheckedException("Failed to read a discovery message.", e);
        }
    }

    /** @return SSL certificate this session is established with. {@code null} if SSL is disabled or certificate validation failed. */
    @Nullable Certificate[] extractCertificates() {
        if (!(sock instanceof SSLSocket))
            return null;

        try {
            return ((SSLSocket)sock).getSession().getPeerCertificates();
        }
        catch (SSLPeerUnverifiedException e) {
            U.error(log, "Failed to extract discovery IO session certificates", e);

            return null;
        }
    }

    /** @return Socket. */
    public Socket socket() {
        return sock;
    }

    /**
     * Writes raw data to the underlying socket output stream.
     *
     * @param data Raw data to write.
     * @throws IOException If failed.
     */
    void write(byte[] data) throws IOException {
        sesWriteLock.lock();

        try {
            out.write(data);

            out.flush();
        }
        finally {
            sesWriteLock.unlock();
        }
    }

    /**
     * Writes a single byte response to the underlying socket output stream.
     *
     * @param b Integer response.
     * @throws IOException If failed.
     */
    void write(int b) throws IOException {
        sesWriteLock.lock();

        try {
            out.write(b);

            out.flush();
        }
        finally {
            sesWriteLock.unlock();
        }
    }

    /**
     * Reads a single byte from the underlying socket input stream limiting read time.
     *
     * @param timeout Socket read timeout for this operation, {@code 0} means infinite.
     * @return Receipt.
     * @throws IOException If failed.
     * @throws EOFException If the connection has been closed.
     */
    int read(long timeout) throws IOException {
        try (SocketTimeoutScope ignored = withTimeout(timeout)) {
            int res = in.read();

            if (res == -1)
                throw new EOFException();

            return res;
        }
    }

    /**
     * Reads {@code data.length} bytes from the underlying socket stream into the given array limiting
     * read time.
     *
     * @param data Array to read the data into.
     * @param timeout Socket read timeout for this operation, {@code 0} means infinite.
     * @return Number of bytes read, less than {@code data.length} only if the connection has been closed.
     * @throws IOException If failed.
     */
    int read(byte[] data, long timeout) throws IOException {
        try (SocketTimeoutScope ignored = withTimeout(timeout)) {
            return in.readNBytes(data, 0, data.length);
        }
    }

    /**
     * Applies the given read timeout to the session socket until the returned scope is closed.
     *
     * @param timeout Socket read timeout, {@code 0} means infinite.
     * @return Scope restoring the previous socket read timeout when closed.
     * @throws SocketException If the timeout can not be applied.
     */
    private SocketTimeoutScope withTimeout(long timeout) throws SocketException {
        SocketTimeoutScope scope = new SocketTimeoutScope(sock.getSoTimeout());

        sock.setSoTimeout((int)timeout);

        return scope;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        U.closeQuiet(sock);
    }

    /** */
    void close(IgniteLogger log) {
        U.close(sock, log);
    }

    /**
     * Checks whether input stream contains SSL alert.
     * See handling {@code StreamCorruptedException} in {@link #readMessage()}.
     * Keeps logic similar to {@link java.io.ObjectInputStream#readStreamHeader}.
     */
    private void detectSslAlert(byte b0, byte b1) throws IOException {
        byte[] hdr = new byte[4];
        hdr[0] = b0;
        hdr[1] = b1;
        int read = in.readNBytes(hdr, 2, 2);

        if (read < 2)
            throw new EOFException();

        String hex = String.format("%02x%02x%02x%02x", hdr[0], hdr[1], hdr[2], hdr[3]);

        if (hex.matches("15....00"))
            throw new StreamCorruptedException("invalid stream header: " + hex);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "TcpDiscoveryIoSession [sock=" + sock + ']';
    }

    /** Restores the socket read timeout changed for the duration of a single operation. */
    private final class SocketTimeoutScope implements AutoCloseable {
        /** */
        private final int oldTimeout;

        /** */
        private SocketTimeoutScope(int oldTimeout) {
            this.oldTimeout = oldTimeout;
        }

        /** {@inheritDoc} */
        @Override public void close() {
            try {
                sock.setSoTimeout(oldTimeout);
            }
            catch (SocketException ignored) {
                // No-op.
            }
        }
    }

    /**
     * Input stream implementation that combines a byte array and a regular InputStream allowing to read bytes
     * from the array first and then proceed with reading from InputStream.
     * Supports only basic read methods.
     */
    private static class CompositeInputStream extends BufferedInputStream {
        /** Prefix data input stream to read before the original input stream. */
        @Nullable private ByteArrayInputStream attachedBytesIs;

        /** @param srcIs Original input stream to read when {@link #attachedBytesIs} is empty. */
        private CompositeInputStream(InputStream srcIs) {
            super(srcIs);
        }

        /** @param prefixData Prefix data to read before the original input stream. */
        private void attachByteArray(byte[] prefixData) {
            assert prefixBytesLeft() == 0;

            attachedBytesIs = new ByteArrayInputStream(prefixData);
        }

        /** {@inheritDoc} */
        @Override public int read() throws IOException {
            if (prefixBytesLeft() > 0) {
                int res = attachedBytesIs.read();

                checkPrefixBufferExhausted();

                return res;
            }

            return super.read();
        }

        /** {@inheritDoc} */
        @Override public int read(@NotNull byte[] b, int off, int len) throws IOException {
            int len0 = readPrefixBuffer(b, off, len);

            assert len0 <= len;

            if (len0 == len)
                return len0;

            int read = super.read(b, off + len0, len - len0);

            if (read < 0)
                return len0 > 0 ? len0 : read;

            return len0 + read;
        }

        /** {@inheritDoc} */
        @Override public int read(@NotNull byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        /** {@inheritDoc} */
        @Override public int readNBytes(byte[] b, int off, int len) throws IOException {
            int len0 = readPrefixBuffer(b, off, len);

            assert len0 <= len;

            return len0 + super.readNBytes(b, off + len0, len - len0);
        }

        /** {@inheritDoc} */
        @Override public int available() throws IOException {
            // Original input stream may return Integer#MAX_VALUE.
            if (super.available() > Integer.MAX_VALUE - prefixBytesLeft())
                return super.available();

            return super.available() + prefixBytesLeft();
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            if (attachedBytesIs != null) {
                attachedBytesIs.close();

                attachedBytesIs = null;
            }

            super.close();
        }

        /** */
        private int readPrefixBuffer(byte[] b, int off, int len) {
            int res = 0;

            int prefixBytesLeft = prefixBytesLeft();

            if (prefixBytesLeft > 0) {
                if (len > b.length - off)
                    len = b.length - off;

                res = attachedBytesIs.read(b, off, Math.min(len, prefixBytesLeft));

                checkPrefixBufferExhausted();
            }

            return res;
        }

        /** */
        private int prefixBytesLeft() {
            return attachedBytesIs == null ? 0 : attachedBytesIs.available();
        }

        /** */
        private void checkPrefixBufferExhausted() {
            if (attachedBytesIs != null && attachedBytesIs.available() == 0)
                attachedBytesIs = null;
        }

        /** {@inheritDoc} */
        @Override public void mark(int readlimit) {
            throw new UnsupportedOperationException("mark() is not supported.");
        }

        /** {@inheritDoc} */
        @Override public boolean markSupported() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            throw new UnsupportedOperationException("reset() is not supported.");
        }

        /** {@inheritDoc} */
        @Override public long skip(long n) {
            throw new UnsupportedOperationException("skip() is not supported.");
        }

        /** {@inheritDoc} */
        @Override public long transferTo(OutputStream out) {
            throw new UnsupportedOperationException("transferTo() is not supported.");
        }

        /** {@inheritDoc} */
        @Override public @NotNull byte[] readAllBytes() {
            throw new UnsupportedOperationException("readAllBytes() is not supported.");
        }

        /** {@inheritDoc} */
        @Override public @NotNull byte[] readNBytes(int len) {
            throw new UnsupportedOperationException("readNBytes() is not supported.");
        }
    }
}

