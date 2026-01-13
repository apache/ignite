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
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSocket;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.makeMessageType;

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
public class TcpDiscoveryIoSession {
    /** Default size of buffer used for buffering socket in/out. */
    private static final int DFLT_SOCK_BUFFER_SIZE = 8192;

    /** Size for an intermediate buffer for serializing discovery messages. */
    private static final int MSG_BUFFER_SIZE = 100;

    /** Leading byte for messages use {@link JdkMarshaller} for serialization. */
    // TODO: remove these flags after refactoring all discovery messages.
    static final byte JAVA_SERIALIZATION = (byte)1;

    /** Leading byte for messages use {@link MessageSerializer} for serialization. */
    static final byte MESSAGE_SERIALIZATION = (byte)2;

    /** */
    private final TcpDiscoverySpi spi;

    /** Loads discovery messages classes during java deserialization. */
    private final ClassLoader clsLdr;

    /** */
    private final Socket sock;

    /** */
    private final DirectMessageWriter msgWriter;

    /** */
    private final DirectMessageReader msgReader;

    /** Buffered socket output stream. */
    private final BufferedOutputStream out;

    /** Buffered socket input stream. */
    private final PrefixedBufferedInputStream in;

    /** Intermediate buffer for deserializing discovery messages. */
    private final ByteBuffer msgInBuf;

    /** Intermediate buffer for serializing discovery messages. */
    private final ByteBuffer msgOutBuf;

    /**
     * Creates a new discovery I/O session bound to the given socket.
     *
     * @param sock Socket connected to a remote discovery node.
     * @param spi  Discovery SPI instance owning this session.
     * @throws IgniteException If an I/O error occurs while initializing buffers.
     */
    TcpDiscoveryIoSession(Socket sock, TcpDiscoverySpi spi) {
        this.sock = sock;
        this.spi = spi;

        clsLdr = U.resolveClassLoader(spi.ignite().configuration());

        msgInBuf = ByteBuffer.allocate(MSG_BUFFER_SIZE);
        msgOutBuf = ByteBuffer.allocate(MSG_BUFFER_SIZE);

        msgWriter = new DirectMessageWriter(spi.messageFactory());
        msgReader = new DirectMessageReader(spi.messageFactory(), null);

        try {
            int sendBufSize = sock.getSendBufferSize() > 0 ? sock.getSendBufferSize() : DFLT_SOCK_BUFFER_SIZE;
            int rcvBufSize = sock.getReceiveBufferSize() > 0 ? sock.getReceiveBufferSize() : DFLT_SOCK_BUFFER_SIZE;

            out = new BufferedOutputStream(sock.getOutputStream(), sendBufSize);
            in = new PrefixedBufferedInputStream(new BufferedInputStream(sock.getInputStream(), rcvBufSize));
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
        if (!(msg instanceof Message)) {
            out.write(JAVA_SERIALIZATION);

            U.marshal(spi.marshaller(), msg, out);

            return;
        }

        try {
            out.write(MESSAGE_SERIALIZATION);

            serializeMessage((Message)msg, out);

            out.flush();
        }
        catch (Exception e) {
            // Keep logic similar to `U.marshal(...)`.
            if (e instanceof IgniteCheckedException)
                throw (IgniteCheckedException)e;

            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Reads the next discovery message from the socket input stream.
     *
     * @param <T> Type of the expected message.
     * @return Deserialized message instance.
     * @throws IgniteCheckedException If deserialization fails.
     */
    <T> T readMessage() throws IgniteCheckedException, IOException {
        ByteBuffer msgBuf = msgInBuf;

        byte serMode = (byte)in.read();

        if (JAVA_SERIALIZATION == serMode)
            return U.unmarshal(spi.marshaller(), in, clsLdr);

        try {
            if (MESSAGE_SERIALIZATION != serMode) {
                detectSslAlert(serMode, in);

                // IOException type is important for ServerImpl. It may search the cause (X.hasCause).
                // The connection error processing behavior depends on it.
                throw new IOException("Received unexpected byte while reading discovery message: " + serMode);
            }

            msgBuf.clear();

            Message msg = spi.messageFactory().create(makeMessageType((byte)in.read(), (byte)in.read()));

            msgReader.reset();
            msgReader.setBuffer(msgBuf);

            MessageSerializer msgSer = spi.messageFactory().serializer(msg.directType());

            boolean finished;

            do {
                int read = in.read(msgBuf.array(), msgBuf.position(), msgBuf.remaining());

                if (read == -1)
                    throw new EOFException("Connection closed before message was fully read.");

                msgBuf.limit(read);

                finished = msgSer.readFrom(msg, msgReader);

                // Server Discovery only sends next message to next Server upon receiving a receipt for the previous one.
                // This behaviour guarantees that we never read a next message from the buffer right after the end of
                // the previous message. But it is not guaranteed with Client Discovery where messages aren't acknowledged.
                // Thus, we have to keep the uprocessed bytes read from the socket. It won't return them again.
                if (msgBuf.remaining() > 0) {
                    byte[] unprocessedReadTail = new byte[msgBuf.remaining()];

                    msgBuf.get(unprocessedReadTail, 0, msgBuf.remaining());

                    in.acceptPrefixBuffer(unprocessedReadTail);
                }

                if (finished)
                    break;

                msgBuf.clear();
            }
            while (true);

            return (T)msg;
        }
        catch (Exception e) {
            // Keep logic similar to `U.marshal(...)`.
            if (e instanceof IgniteCheckedException)
                throw (IgniteCheckedException)e;

            throw new IgniteCheckedException(e);
        }
    }

    /** @return SSL certificate this session is established with. {@code null} if SSL is disabled or certificate validation failed. */
    @Nullable Certificate[] extractCertificates() {
        if (!spi.isSslEnabled())
            return null;

        try {
            return ((SSLSocket)sock).getSession().getPeerCertificates();
        }
        catch (SSLPeerUnverifiedException e) {
            U.error(spi.log, "Failed to extract discovery IO session certificates", e);

            return null;
        }
    }

    /**
     * Serializes a discovery message into a byte array.
     *
     * @param msg Discovery message to serialize.
     * @return Serialized byte array containing the message data.
     * @throws IgniteCheckedException If serialization fails.
     * @throws IOException If serialization fails.
     */
    byte[] serializeMessage(TcpDiscoveryAbstractMessage msg) throws IgniteCheckedException, IOException {
        if (!(msg instanceof Message))
            return U.marshal(spi.marshaller(), msg);

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serializeMessage((Message)msg, out);

            return out.toByteArray();
        }
    }

    /** @return Socket. */
    public Socket socket() {
        return sock;
    }

    /**
     * Serializes a discovery message into given output stream.
     *
     * @param m Discovery message to serialize.
     * @param out Output stream to write serialized message.
     * @throws IOException If serialization fails.
     */
    private void serializeMessage(Message m, OutputStream out) throws IOException {
        ByteBuffer msgBuf = msgOutBuf;

        MessageSerializer msgSer = spi.messageFactory().serializer(m.directType());

        msgWriter.reset();
        msgWriter.setBuffer(msgBuf);

        boolean finished;

        do {
            // Should be cleared before first operation.
            msgBuf.clear();

            finished = msgSer.writeTo(m, msgWriter);

            out.write(msgBuf.array(), 0, msgBuf.position());
        }
        while (!finished);
    }

    /**
     * Checks wheter input stream contains SSL alert.
     * See handling {@code StreamCorruptedException} in {@link #readMessage()}.
     * Keeps logic similar to {@link java.io.ObjectInputStream#readStreamHeader}.
     */
    private void detectSslAlert(byte firstByte, InputStream in) throws IOException {
        byte[] hdr = new byte[4];
        hdr[0] = firstByte;
        int read = in.readNBytes(hdr, 1, 3);

        if (read < 3)
            throw new EOFException();

        String hex = String.format("%02x%02x%02x%02x", hdr[0], hdr[1], hdr[2], hdr[3]);

        if (hex.matches("15....00"))
            throw new StreamCorruptedException("invalid stream header: " + hex);
    }

    /** */
    private static class PrefixedBufferedInputStream extends BufferedInputStream {
        /** */
        private ByteArrayInputStream bais;

        /** */
        private PrefixedBufferedInputStream(InputStream in) {
            super(in);
        }

        /** */
        public void acceptPrefixBuffer(byte[] prefixBuf) {
            assert prefixBytesLeft() == 0;

            bais = new ByteArrayInputStream(prefixBuf);
        }

        /** {@inheritDoc} */
        @Override public synchronized int read() throws IOException {
//            if (prefLeft() > 0) {
//                ++prefOffset;
//
//                return prefixBuf[prefOffset - 1];
//            }
//
//            return super.read();
            if (prefixBytesLeft() > 0) {
                int res = bais.read();

                checkPrefixBufferExhausted();

                return res;
            }

            return super.read();
        }

        /** */
        private int prefixBytesLeft() {
            return bais == null ? 0 : bais.available();
            //return prefixBuf.length - prefOffset;
        }

        /** */
        private void checkPrefixBufferExhausted() {
            if (bais != null && bais.available() == 0)
                bais = null;
        }

        /** {@inheritDoc} */
        @Override public int read(@NotNull byte[] b, int off, int len) throws IOException {
            int len0 = readPrefixBuffer(b, off, len);

            if (len0 == len)
                return len0;

//            int len0 = 0;
//
//            if (len > b.length - off)
//                len = b.length - off;
//
//            if (prefLeft() > 0) {
//                len0 = Math.min(len, prefLeft());
//
//                System.arraycopy(prefixBuf, prefOffset, b, off, len0);
//
//                prefOffset += len0;
//
//                assert prefLeft() >= 0;
//
//                if (len0 == len)
//                    return len0;
//            }
//
            return len0 + super.read(b, off + len0, len - len0);
        }

        /** */
        private int readPrefixBuffer(byte[] b, int off, int len) {
            int res = 0;

            int prefixBytesLeft = prefixBytesLeft();

            if (prefixBytesLeft > 0) {
                if (len > b.length - off)
                    len = b.length - off;

                res = bais.read(b, off, Math.min(len, prefixBytesLeft));

                checkPrefixBufferExhausted();
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public int read(@NotNull byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        /** {@inheritDoc} */
        @Override public int available() throws IOException {
            return super.available() + prefixBytesLeft();
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
        @Override public void reset() throws IOException {
            throw new UnsupportedOperationException("reset() is not supported.");
        }

        /** {@inheritDoc} */
        @Override public long skip(long n) throws IOException {
            throw new UnsupportedOperationException("skip() is not supported.");
        }

        /** {@inheritDoc} */
        @Override public long transferTo(OutputStream out) throws IOException {
            throw new UnsupportedOperationException("transferTo() is not supported.");
        }

        /** {@inheritDoc} */
        @Override public int readNBytes(byte[] b, int off, int len) throws IOException {
            int len0 = readPrefixBuffer(b, off, len);

            return super.readNBytes(b, off + len0, len - len0);
        }

        /** {@inheritDoc} */
        @Override public @NotNull byte[] readAllBytes() throws IOException {
            throw new UnsupportedOperationException("readAllBytes() is not supported.");
        }

        /** {@inheritDoc} */
        @Override public @NotNull byte[] readNBytes(int len) throws IOException {
            throw new UnsupportedOperationException("readNBytes() is not supported.");
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            if (bais != null) {
                bais.close();

                bais = null;
            }

            super.close();
        }
    }
}

