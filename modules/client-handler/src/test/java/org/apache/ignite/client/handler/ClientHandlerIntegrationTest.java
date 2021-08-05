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

package org.apache.ignite.client.handler;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;
import io.netty.channel.ChannelFuture;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.schemas.clientconnector.ClientConnectorConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessagePack;
import org.slf4j.helpers.NOPLogger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Client connector integration tests with real sockets.
 */
public class ClientHandlerIntegrationTest {
    /** Magic bytes. */
    private static final byte[] MAGIC = new byte[]{0x49, 0x47, 0x4E, 0x49};

    private ChannelFuture serverFuture;

    private ConfigurationRegistry configurationRegistry;

    private int serverPort;

    @BeforeEach
    public void setUp() throws Exception {
        serverFuture = startServer();
        serverPort = ((InetSocketAddress)serverFuture.channel().localAddress()).getPort();
    }

    @AfterEach
    public void tearDown() throws Exception {
        serverFuture.cancel(true);
        serverFuture.await();
        serverFuture.channel().closeFuture().await();
        configurationRegistry.stop();
    }

    @Test
    void testHandshakeInvalidMagicHeaderDropsConnection() throws Exception {
        try (var sock = new Socket("127.0.0.1", serverPort)) {
            OutputStream out = sock.getOutputStream();
            out.write(new byte[]{63, 64, 65, 66, 67});
            out.flush();

            assertThrows(IOException.class, () -> writeAndFlushLoop(sock));
        }
    }

    @Test
    void testHandshakeValidReturnsSuccess() throws Exception {
        try (var sock = new Socket("127.0.0.1", serverPort)) {
            OutputStream out = sock.getOutputStream();

            // Magic: IGNI
            out.write(MAGIC);

            // Handshake.
            var packer = MessagePack.newDefaultBufferPacker();
            packer.packInt(0);
            packer.packInt(0);
            packer.packInt(0);
            packer.packInt(7); // Size.

            packer.packInt(3); // Major.
            packer.packInt(0); // Minor.
            packer.packInt(0); // Patch.

            packer.packInt(2); // Client type: general purpose.

            packer.packBinaryHeader(0); // Features.
            packer.packMapHeader(0); // Extensions.

            out.write(packer.toByteArray());
            out.flush();

            // Read response.
            var unpacker = MessagePack.newDefaultUnpacker(sock.getInputStream());
            var magic = unpacker.readPayload(4);
            unpacker.skipValue(3); // LE int zeros.
            var len = unpacker.unpackInt();
            var major = unpacker.unpackInt();
            var minor = unpacker.unpackInt();
            var patch = unpacker.unpackInt();
            var errorCode = unpacker.unpackInt();

            var featuresLen = unpacker.unpackBinaryHeader();
            unpacker.skipValue(featuresLen);

            var extensionsLen = unpacker.unpackMapHeader();
            unpacker.skipValue(extensionsLen);

            assertArrayEquals(MAGIC, magic);
            assertEquals(7, len);
            assertEquals(3, major);
            assertEquals(0, minor);
            assertEquals(0, patch);
            assertEquals(0, errorCode);
        }
    }

    @Test
    void testHandshakeInvalidVersionReturnsError() throws Exception {
        try (var sock = new Socket("127.0.0.1", serverPort)) {
            OutputStream out = sock.getOutputStream();

            // Magic: IGNI
            out.write(MAGIC);

            // Handshake.
            var packer = MessagePack.newDefaultBufferPacker();
            packer.packInt(0);
            packer.packInt(0);
            packer.packInt(0);
            packer.packInt(7); // Size.

            packer.packInt(2); // Major.
            packer.packInt(8); // Minor.
            packer.packInt(0); // Patch.

            packer.packInt(2); // Client type: general purpose.

            packer.packBinaryHeader(0); // Features.
            packer.packMapHeader(0); // Extensions.

            out.write(packer.toByteArray());
            out.flush();

            // Read response.
            var unpacker = MessagePack.newDefaultUnpacker(sock.getInputStream());
            var magic = unpacker.readPayload(4);
            unpacker.skipValue(3);
            var len = unpacker.unpackInt();
            var major = unpacker.unpackInt();
            var minor = unpacker.unpackInt();
            var patch = unpacker.unpackInt();
            var errorCode = unpacker.unpackInt();
            var err = unpacker.unpackString();

            assertArrayEquals(MAGIC, magic);
            assertEquals(31, len);
            assertEquals(3, major);
            assertEquals(0, minor);
            assertEquals(0, patch);
            assertEquals(1, errorCode);
            assertEquals("Unsupported version: 2.8.0", err);
        }
    }

    private ChannelFuture startServer() throws InterruptedException {
        configurationRegistry = new ConfigurationRegistry(
                Collections.singletonList(ClientConnectorConfiguration.KEY),
                Collections.emptyMap(),
                Collections.singletonList(new TestConfigurationStorage(ConfigurationType.LOCAL))
        );

        configurationRegistry.start();

        var module = new ClientHandlerModule(mock(Ignite.class), NOPLogger.NOP_LOGGER);

        module.prepareStart(configurationRegistry);

        return module.start();
    }

    private void writeAndFlushLoop(Socket socket) throws Exception {
        var stop = System.currentTimeMillis() + 5000;
        var out = socket.getOutputStream();

        while (System.currentTimeMillis() < stop) {
            out.write(1);
            out.flush();
        }
    }
}
