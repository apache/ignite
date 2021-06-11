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

package org.apache.ignite.network.internal.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import io.netty.handler.codec.DecoderException;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.NetworkMessagesFactory;
import org.apache.ignite.network.TestMessage;
import org.apache.ignite.network.TestMessageSerializationRegistryImpl;
import org.apache.ignite.network.TestMessagesFactory;
import org.apache.ignite.network.internal.recovery.RecoveryClientHandshakeManager;
import org.apache.ignite.network.internal.recovery.RecoveryServerHandshakeManager;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ConnectionManager}.
 */
public class ConnectionManagerTest {
    /** Started connection managers. */
    private final List<ConnectionManager> startedManagers = new ArrayList<>();

    /** Message factory. */
    private final TestMessagesFactory messageFactory = new TestMessagesFactory();

    /** */
    @AfterEach
    final void tearDown() {
        startedManagers.forEach(ConnectionManager::stop);
    }

    /**
     * Tests that a message is sent successfully using the ConnectionManager.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSentSuccessfully() throws Exception {
        String msgText = "test";

        int port1 = 4000;
        int port2 = 4001;

        ConnectionManager manager1 = startManager(port1);
        ConnectionManager manager2 = startManager(port2);

        var fut = new CompletableFuture<NetworkMessage>();

        manager2.addListener((address, message) -> fut.complete(message));

        NettySender sender = manager1.channel(null, new InetSocketAddress(port2)).get(3, TimeUnit.SECONDS);

        TestMessage testMessage = messageFactory.testMessage().msg(msgText).build();

        sender.send(testMessage).get(3, TimeUnit.SECONDS);

        NetworkMessage receivedMessage = fut.get(3, TimeUnit.SECONDS);

        assertEquals(msgText, ((TestMessage) receivedMessage).msg());
    }

    /**
     * Tests that incoming connection is reused for sending messages.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReuseIncomingConnection() throws Exception {
        String msgText = "test";

        TestMessage testMessage = messageFactory.testMessage().msg("test").build();

        int port1 = 4000;
        int port2 = 4001;

        ConnectionManager manager1 = startManager(port1);
        ConnectionManager manager2 = startManager(port2);

        var fut = new CompletableFuture<NetworkMessage>();

        manager1.addListener((address, message) -> fut.complete(message));

        NettySender senderFrom1to2 = manager1.channel(null, new InetSocketAddress(port2)).get(3, TimeUnit.SECONDS);

        // Ensure a handshake has finished on both sides by sending a message.
        // TODO: IGNITE-14085 When the recovery protocol is implemented replace this with simple
        // CompletableFuture#get called on the send future.
        var messageReceivedOn2 = new CompletableFuture<Void>();

        // If the message is received, that means that the handshake was successfully performed.
        manager2.addListener((address, message) -> messageReceivedOn2.complete(null));

        senderFrom1to2.send(testMessage);

        messageReceivedOn2.get(3, TimeUnit.SECONDS);

        NettySender senderFrom2to1 = manager2.channel(manager1.consistentId(), new InetSocketAddress(port1)).get(3, TimeUnit.SECONDS);

        InetSocketAddress clientLocalAddress = (InetSocketAddress) senderFrom1to2.channel().localAddress();

        InetSocketAddress clientRemoteAddress = (InetSocketAddress) senderFrom2to1.channel().remoteAddress();

        assertEquals(clientLocalAddress, clientRemoteAddress);

        senderFrom2to1.send(testMessage).get(3, TimeUnit.SECONDS);

        NetworkMessage receivedMessage = fut.get(3, TimeUnit.SECONDS);

        assertEquals(msgText, ((TestMessage) receivedMessage).msg());
    }

    /**
     * Tests that the resources of a connection manager are closed after a shutdown.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testShutdown() throws Exception {
        int port1 = 4000;
        int port2 = 4001;

        ConnectionManager manager1 = startManager(port1);
        ConnectionManager manager2 = startManager(port2);

        NettySender sender1 = manager1.channel(null, new InetSocketAddress(port2)).get(3, TimeUnit.SECONDS);
        NettySender sender2 = manager2.channel(null, new InetSocketAddress(port1)).get(3, TimeUnit.SECONDS);

        assertNotNull(sender1);
        assertNotNull(sender2);

        Stream.of(manager1, manager2).forEach(manager -> {
            NettyServer server = manager.server();
            Collection<NettyClient> clients = manager.clients();

            manager.stop();

            assertFalse(server.isRunning());

            boolean clientsStopped = clients.stream().allMatch(NettyClient::isDisconnected);

            assertTrue(clientsStopped);
        });
    }

    /**
     * Tests that after a channel was closed, a new channel is opened upon a request.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCanReconnectAfterFail() throws Exception {
        String msgText = "test";

        int port1 = 4000;
        int port2 = 4001;

        ConnectionManager manager1 = startManager(port1);
        ConnectionManager manager2 = startManager(port2);

        NettySender sender = manager1.channel(null, new InetSocketAddress(port2)).get(3, TimeUnit.SECONDS);

        TestMessage testMessage = messageFactory.testMessage().msg(msgText).build();

        manager2.stop();

        final NettySender finalSender = sender;

        assertThrows(ClosedChannelException.class, () -> {
            try {
                finalSender.send(testMessage).get(3, TimeUnit.SECONDS);
            }
            catch (Exception e) {
                throw e.getCause();
            }
        });

        manager2 = startManager(port2);

        var fut = new CompletableFuture<NetworkMessage>();

        manager2.addListener((address, message) -> fut.complete(message));

        sender = manager1.channel(null, new InetSocketAddress(port2)).get(3, TimeUnit.SECONDS);

        sender.send(testMessage).get(3, TimeUnit.SECONDS);

        NetworkMessage receivedMessage = fut.get(3, TimeUnit.SECONDS);

        assertEquals(msgText, ((TestMessage) receivedMessage).msg());
    }

    /**
     * Tests that a connection to a misconfigured server results in a connection close and an exception on the client
     * side.
     */
    @Test
    public void testConnectMisconfiguredServer() throws Exception {
        ConnectionManager client = startManager(4000);

        ConnectionManager server = startManager(4001, mockSerializationRegistry());

        try {
            client.channel(null, server.getLocalAddress()).get(3, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            assertThat(e.getCause(), isA(IOException.class));
        }
    }

    /**
     * Tests that a connection from a misconfigured client results in an exception.
     */
    @Test
    public void testConnectMisconfiguredClient() throws Exception {
        ConnectionManager client = startManager(4000, mockSerializationRegistry());

        ConnectionManager server = startManager(4001);

        try {
            client.channel(null, server.getLocalAddress()).get(3, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            assertThat(e.getCause(), isA(DecoderException.class));
        }
    }

    /**
     * Creates a mock {@link MessageSerializationRegistry} that throws an exception when trying to get a serializer
     * or a deserializer.
     */
    private static MessageSerializationRegistry mockSerializationRegistry() {
        var mockRegistry = mock(MessageSerializationRegistry.class);

        when(mockRegistry.createDeserializer(anyShort(), anyShort())).thenThrow(RuntimeException.class);
        when(mockRegistry.createSerializer(anyShort(), anyShort())).thenThrow(RuntimeException.class);

        return mockRegistry;
    }

    /**
     * Creates and starts a {@link ConnectionManager} listening on the given port.
     *
     * @param port Port for the connection manager to listen on.
     * @return Connection manager.
     */
    private ConnectionManager startManager(int port) {
        return startManager(port, new TestMessageSerializationRegistryImpl());
    }

    /**
     * Creates and starts a {@link ConnectionManager} listening on the given port, configured with the provided
     * serialization registry.
     *
     * @param port Port for the connection manager to listen on.
     * @param registry Serialization registry.
     * @return Connection manager.
     */
    private ConnectionManager startManager(int port, MessageSerializationRegistry registry) {
        UUID launchId = UUID.randomUUID();
        String consistentId = UUID.randomUUID().toString();

        var messageFactory = new NetworkMessagesFactory();

        var manager = new ConnectionManager(
            port,
            registry,
            consistentId,
            () -> new RecoveryServerHandshakeManager(launchId, consistentId, messageFactory),
            () -> new RecoveryClientHandshakeManager(launchId, consistentId, messageFactory)
        );

        manager.start();

        startedManagers.add(manager);

        return manager;
    }
}
