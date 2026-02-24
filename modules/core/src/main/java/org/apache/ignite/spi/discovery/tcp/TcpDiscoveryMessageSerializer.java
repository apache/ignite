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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;

/**
 * Class is responsible for serializing discovery messages using RU-ready {@link MessageSerializer} mechanism.
 * <p>
 * It is used in a special case: when server wants to send discovery messages to clients, it may not have a {@link TcpDiscoveryIoSession}
 * to serialize the messages.
 * This class enables server to serialize discovery messages anyway, duplicating serialization code from {@link TcpDiscoveryIoSession}.
 */
class TcpDiscoveryMessageSerializer extends TcpDiscoveryIoSession {
    /**
     * @param spi Discovery SPI instance.
     */
    public TcpDiscoveryMessageSerializer(TcpDiscoverySpi spi) {
        super(new Socket() {
            @Override public OutputStream getOutputStream() throws IOException {
                return null;
            }

            @Override public InputStream getInputStream() throws IOException {
                return null;
            }
        }, spi);
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
}
