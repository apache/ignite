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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;

/**
 * Class is responsible for marshalling discovery messages using RU-ready {@link MessageSerializer} mechanism.
 */
public class TcpDiscoveryMessageMarshaller {
    /** Size for an intermediate buffer for serializing discovery messages. */
    static final int MSG_BUFFER_SIZE = 100;

    /** */
    private final TcpDiscoverySpi spi;

    /** */
    private final DirectMessageWriter msgWriter;

    /** Intermediate buffer for serializing discovery messages. */
    final ByteBuffer msgBuf;

    /**
     * @param spi Discovery SPI instance.
     */
    public TcpDiscoveryMessageMarshaller(TcpDiscoverySpi spi) {
        this.spi = spi;

        msgBuf = ByteBuffer.allocate(MSG_BUFFER_SIZE);

        msgWriter = new DirectMessageWriter(spi.messageFactory());
    }

    /**
     * Serializes a discovery message into a byte array.
     *
     * @param msg Discovery message to serialize.
     * @return Serialized byte array containing the message data.
     * @throws IgniteCheckedException If serialization fails.
     * @throws IOException If serialization fails.
     */
    byte[] marshal(TcpDiscoveryAbstractMessage msg) throws IgniteCheckedException, IOException {
        if (!(msg instanceof Message))
            return U.marshal(spi.marshaller(), msg);

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            marshal((Message)msg, out);

            return out.toByteArray();
        }
    }


    /**
     * Serializes a discovery message into given output stream.
     *
     * @param m Discovery message to serialize.
     * @param out Output stream to write serialized message.
     * @throws IOException If serialization fails.
     */
    void marshal(Message m, OutputStream out) throws IOException {
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
}
