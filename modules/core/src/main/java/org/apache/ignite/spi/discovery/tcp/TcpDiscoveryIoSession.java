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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;

import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.makeMessageType;

/**
 * Wrapper of Socket + reader/writer + setter for streams.
 */
public class TcpDiscoveryIoSession {
    /** */
    private static final int DEFAULT_BUFFER_SIZE = 8192;

    /** TODO: remove these flags after refactoring all discovery messages. */
    static final byte JAVA_SERIALIZATION = (byte)0;

    /** */
    static final byte MESSAGE_SERIALIZATION = (byte)1;

    /** */
    private final TcpDiscoverySpi spi;

    /** */
    private final Socket sock;

    /** */
    private final DirectMessageWriter msgWriter;

    /** */
    private final DirectMessageReader msgReader;

    /** */
    private final ByteBuffer sendBuf;

    /** */
    private final ByteBuffer rcvBuf;

    /** */
    TcpDiscoveryIoSession(Socket sock, TcpDiscoverySpi spi) {
        this.sock = sock;
        this.spi = spi;

        msgWriter = new DirectMessageWriter(spi.messageFactory());
        msgReader = new DirectMessageReader(spi.messageFactory(), null);

        try {
            int sendBufSize = sock.getSendBufferSize() > 0 ? sock.getSendBufferSize() : DEFAULT_BUFFER_SIZE;
            int rcvBufSize = sock.getReceiveBufferSize() > 0 ? sock.getReceiveBufferSize() : DEFAULT_BUFFER_SIZE;

            sendBuf = ByteBuffer.allocate(sendBufSize);
            rcvBuf = ByteBuffer.allocate(rcvBufSize);
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    byte[] serializeMessage(TcpDiscoveryAbstractMessage msg) throws IgniteCheckedException {
        if (!(msg instanceof Message))
            return U.marshal(spi.marshaller(), msg);

        Message m = (Message)msg;

        ByteBuffer ser = ByteBuffer.allocate(sendBuf.limit());

        MessageSerializer msgSer = spi.messageFactory().serializer(m.directType());

        msgWriter.reset();
        msgWriter.setBuffer(ser);

        byte[] bytes = new byte[ser.remaining()];
        int total = 0;

        boolean finished = false;

        while (!finished) {
            finished = msgSer.writeTo(m, msgWriter);

            int chunkSize = sendBuf.position();

            if (total + chunkSize > bytes.length)
                bytes = Arrays.copyOf(bytes, total + chunkSize);

            sendBuf.flip();
            sendBuf.get(bytes, total, chunkSize);
            total += chunkSize;

            sendBuf.clear();
        }

        return bytes;
    }

    /** */
    void writeMessage(TcpDiscoveryAbstractMessage msg) throws IgniteCheckedException, IOException {
        OutputStream out = sock.getOutputStream();

        if (!(msg instanceof Message)) {
            out.write(JAVA_SERIALIZATION);

            U.marshal(spi.marshaller(), msg, out);

            return;
        }

        try {
            Message m = (Message)msg;
            MessageSerializer msgSer = spi.messageFactory().serializer(m.directType());

            msgWriter.reset();
            msgWriter.setBuffer(sendBuf);

            out.write(MESSAGE_SERIALIZATION);

            boolean finished;

            do {
                finished = msgSer.writeTo(m, msgWriter);

                out.write(sendBuf.array(), 0, sendBuf.position());

                sendBuf.clear();
            }
            while (!finished);

            out.flush();
        }
        catch (Exception e) {
            // Keep logic similar to `U.marshal(...)`.
            if (e instanceof IgniteCheckedException)
                throw (IgniteCheckedException)e;

            throw new IgniteCheckedException(e);
        }
    }

    /** */
    <T> T readMessage() throws IgniteCheckedException, IOException {
        InputStream in = sock.getInputStream();

        byte serMode = (byte)in.read();

        if (JAVA_SERIALIZATION == serMode)
            return U.unmarshal(spi.marshaller(), in, spi.classLoader());

        try {
            if (MESSAGE_SERIALIZATION != serMode) {
                detectSslAlert(serMode, in);

                throw new EOFException();
            }

            byte b0 = (byte)in.read();
            byte b1 = (byte)in.read();

            Message msg = spi.messageFactory().create(makeMessageType(b0, b1));

            msgReader.reset();
            msgReader.setBuffer(rcvBuf);

            MessageSerializer msgSer = spi.messageFactory().serializer(msg.directType());

            boolean finish;

            int pos = 0;

            do {
                rcvBuf.put((byte)in.read());

                rcvBuf.position(pos);
                rcvBuf.limit(pos + 1);

                finish = msgSer.readFrom(msg, msgReader);

                rcvBuf.limit(rcvBuf.capacity());
                pos = rcvBuf.position();

                // TODO: big message.

            } while (!finish);

            return (T)msg;
        }
        catch (Exception e) {
            // Keep logic similar to `U.marshal(...)`.
            if (e instanceof IgniteCheckedException)
                throw (IgniteCheckedException)e;

            throw new IgniteCheckedException(e);
        }
    }

    /** @return Socket. */
    public Socket socket() {
        return sock;
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
}
