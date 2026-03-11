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

package org.apache.ignite.spi.discovery.zk.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.spi.IgniteSpiException;

import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.makeMessageType;

/**
 * Class is responsible for serializing discovery messages using RU-ready {@link MessageSerializer} mechanism.
 */
public class DiscoveryMessageParser {
    /** Leading byte for messages use {@link JdkMarshaller} for serialization. */
    // TODO: remove these flags after refactoring all discovery messages.
    private static final byte JAVA_SERIALIZATION = (byte)1;

    /** Leading byte for messages use {@link MessageSerializer} for serialization. */
    private static final byte MESSAGE_SERIALIZATION = (byte)2;

    /** Size for an intermediate buffer for serializing discovery messages. */
    private static final int MSG_BUFFER_SIZE = 100;

    /** */
    private final MessageFactory msgFactory;

    /** */
    private final Marshaller marsh;

    /** */
    public DiscoveryMessageParser(Marshaller marsh) {
        this.marsh = marsh;
        this.msgFactory = new IgniteMessageFactoryImpl(
            new MessageFactoryProvider[] { new DiscoveryMessageFactory(null, null) });
    }

    /** Marshals discovery message to bytes array. */
    public byte[] marshalZip(DiscoveryCustomMessage msg) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (DeflaterOutputStream out = new DeflaterOutputStream(baos)) {
            if (msg instanceof Message) {
                out.write(MESSAGE_SERIALIZATION);

                serializeMessage((Message)msg, out);
            }
            else {
                out.write(JAVA_SERIALIZATION);

                U.marshal(marsh, msg, out);
            }
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to serialize message: " + msg, e);
        }

        return baos.toByteArray();
    }

    /** Unmarshals discovery message from bytes array. */
    public DiscoveryCustomMessage unmarshalZip(byte[] bytes) {
        try (
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            InflaterInputStream in = new InflaterInputStream(bais)
        ) {
            byte mode = (byte)in.read();

            if (mode == JAVA_SERIALIZATION)
                return U.unmarshal(marsh, in, U.gridClassLoader());

            if (MESSAGE_SERIALIZATION != mode)
                throw new IOException("Received unexpected byte while reading discovery message: " + mode);

            return (DiscoveryCustomMessage)deserializeMessage(in);
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to deserialize message.", e);
        }
    }

    /** */
    private void serializeMessage(Message m, OutputStream out) throws IOException {
        DirectMessageWriter msgWriter = new DirectMessageWriter(msgFactory);
        ByteBuffer msgBuf = ByteBuffer.allocate(MSG_BUFFER_SIZE);

        msgWriter.setBuffer(msgBuf);

        MessageSerializer msgSer = msgFactory.serializer(m.directType());

        boolean finished;

        do {
            msgBuf.clear();

            finished = msgSer.writeTo(m, msgWriter);

            out.write(msgBuf.array(), 0, msgBuf.position());
        }
        while (!finished);
    }

    /** */
    private Message deserializeMessage(InputStream in) throws IOException {
        DirectMessageReader msgReader = new DirectMessageReader(msgFactory, null);
        ByteBuffer msgBuf = ByteBuffer.allocate(MSG_BUFFER_SIZE);

        msgReader.setBuffer(msgBuf);

        Message msg = msgFactory.create(makeMessageType((byte)in.read(), (byte)in.read()));
        MessageSerializer msgSer = msgFactory.serializer(msg.directType());

        boolean finished;

        do {
            int read = in.read(msgBuf.array(), msgBuf.position(), msgBuf.remaining());

            if (read == -1)
                throw new EOFException("Stream closed before message was fully read.");

            msgBuf.limit(msgBuf.position() + read);
            msgBuf.rewind();

            finished = msgSer.readFrom(msg, msgReader);

            if (!finished)
                msgBuf.compact();
        }
        while (!finished);

        return msg;
    }
}
