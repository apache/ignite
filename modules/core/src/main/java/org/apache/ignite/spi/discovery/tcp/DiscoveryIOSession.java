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
import java.net.SocketException;
import java.nio.ByteBuffer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * TODO
 */
public class DiscoveryIOSession {
    /** */
    private static final byte[] JAVA_SERIALIZATION = new byte[] { 0 };

    /** */
    private static final byte[] MESSAGE_SERIALIZATION = new byte[] { 1 };

    /** */
    private final Socket sock;

    /** */
    private final MessageWriter msgWriter;

    /** */
    private final MessageReader msgReader;

    /** */
    private final ByteBuffer buf;

    /** */
    private final MessageFactory msgFactory;

    /** */
    DiscoveryIOSession(MessageFactory msgFactory, Socket sock) throws SocketException {
        this.msgFactory = msgFactory;
        this.sock = sock;

        msgWriter = new DirectMessageWriter(msgFactory);
        msgReader = new DirectMessageReader(msgFactory);

        buf = ByteBuffer.allocate(sock.getSendBufferSize());
    }

    /** */
    void writeMessage(Object msg, OutputStream out, Marshaller marshaller) throws IgniteCheckedException {
        try {
            if (!(msg instanceof Message)) {
                out.write(JAVA_SERIALIZATION);

                U.marshal(marshaller, msg, out);

                return;
            }

            out.write(MESSAGE_SERIALIZATION);

            Message m = (Message)msg;
            MessageSerializer msgSer = msgFactory.serializer(m.directType());
            msgWriter.reset();

            boolean finish;

            do {
                buf.clear();

                finish = msgSer.writeTo(m, buf, msgWriter);

                out.write(buf.array(), 0, buf.position());
            }
            while (!finish);

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
    <T> T readMessage(InputStream in, Marshaller marshaller, Ignite ign) throws IgniteCheckedException {
        try {
            buf.clear();

            readNBytes(in, 1);

            byte serMode = buf.array()[0];

            if (JAVA_SERIALIZATION[0] == serMode)
                return U.unmarshal(marshaller, in, U.resolveClassLoader(ign.configuration()));

            else if (MESSAGE_SERIALIZATION[0] == serMode) {
                readNBytes(in, 2);

                msgReader.reset();
                msgReader.setBuffer(buf);

                short msgType = msgReader.readShort();

                MessageSerializer msgSer = msgFactory.serializer(msgType);
                Message msg = msgFactory.create(msgType);

                boolean finish;

                do {
                    readNBytes(in, 1);

                    buf.position(0);
                    buf.limit(1);

                    finish = msgSer.readFrom(msg, buf, msgReader);
                } while (!finish);

                return (T)msg;
            }

            detectSslAlert(serMode, in);

            throw new EOFException();
        }
        catch (Exception e) {
            // Keep logic similar to `U.marshal(...)`.
            if (e instanceof IgniteCheckedException)
                throw (IgniteCheckedException)e;

            throw new IgniteCheckedException(e);
        }
    }

    /** */
    public Socket socket() {
        return sock;
    }

    /** */
    private void readNBytes(InputStream in, int nbytes) throws IOException {
        if (in.readNBytes(buf.array(), 0, nbytes) < nbytes)
            throw new EOFException();
    }

    /**
     * Checks wheter input stream contains SSL alert.
     * See handling {@code StreamCorruptedException} in {@link #readMessage(Socket, InputStream, long)}.
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
