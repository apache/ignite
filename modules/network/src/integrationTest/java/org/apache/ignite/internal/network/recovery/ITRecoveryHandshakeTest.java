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

package org.apache.ignite.internal.network.recovery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import io.netty.channel.Channel;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.configuration.schemas.network.NetworkView;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.handshake.HandshakeAction;
import org.apache.ignite.internal.network.netty.ConnectionManager;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.TestMessageSerializationRegistryImpl;
import org.apache.ignite.network.TestMessagesFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.apache.ignite.internal.network.recovery.ITRecoveryHandshakeTest.FailingRecoveryClientHandshakeManager.ClientStageFail;
import static org.apache.ignite.internal.network.recovery.ITRecoveryHandshakeTest.FailingRecoveryClientHandshakeManager.ClientStageFail.CLIENT_CONNECTION_OPENED;
import static org.apache.ignite.internal.network.recovery.ITRecoveryHandshakeTest.FailingRecoveryClientHandshakeManager.ClientStageFail.CLIENT_DOESNT_FAIL;
import static org.apache.ignite.internal.network.recovery.ITRecoveryHandshakeTest.FailingRecoveryClientHandshakeManager.ClientStageFail.CLIENT_INIT;
import static org.apache.ignite.internal.network.recovery.ITRecoveryHandshakeTest.FailingRecoveryClientHandshakeManager.ClientStageFail.CLIENT_SERVER_RESPONDED;
import static org.apache.ignite.internal.network.recovery.ITRecoveryHandshakeTest.FailingRecoveryServerHandshakeManager.ServerStageFail;
import static org.apache.ignite.internal.network.recovery.ITRecoveryHandshakeTest.FailingRecoveryServerHandshakeManager.ServerStageFail.SERVER_CLIENT_RESPONDED;
import static org.apache.ignite.internal.network.recovery.ITRecoveryHandshakeTest.FailingRecoveryServerHandshakeManager.ServerStageFail.SERVER_CONNECTION_OPENED;
import static org.apache.ignite.internal.network.recovery.ITRecoveryHandshakeTest.FailingRecoveryServerHandshakeManager.ServerStageFail.SERVER_DOESNT_FAIL;
import static org.apache.ignite.internal.network.recovery.ITRecoveryHandshakeTest.FailingRecoveryServerHandshakeManager.ServerStageFail.SERVER_INIT;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Recovery protocol handshake tests.
 */
@ExtendWith(ConfigurationExtension.class)
public class ITRecoveryHandshakeTest {
    /** Started connection managers. */
    private final List<ConnectionManager> startedManagers = new ArrayList<>();

    private final TestMessagesFactory messageFactory = new TestMessagesFactory();

    /** Reusable network configuration object. */
    @InjectConfiguration
    private NetworkConfiguration networkConfiguration;

    /** */
    @AfterEach
    final void tearDown() {
        startedManagers.forEach(ConnectionManager::stop);
    }

    /**
     * Tests handshake scenarios in which some of the parts of handshake protocol can fail.
     *
     * @param scenario Handshake scenario.
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @MethodSource("handshakeScenarios")
    public void testHandshakeScenario(HandshakeScenario scenario) throws Exception {
        ConnectionManager manager1 = startManager(
            4000,
            scenario.serverFailAt,
            CLIENT_DOESNT_FAIL
        );

        ConnectionManager manager2 = startManager(
            4001,
            SERVER_DOESNT_FAIL,
            scenario.clientFailAt
        );

        NettySender from2to1;

        try {
            from2to1 = manager2.channel(manager1.consistentId(), manager1.getLocalAddress()).get(3, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            if (scenario.clientFailAt == CLIENT_DOESNT_FAIL &&
                scenario.serverFailAt == SERVER_DOESNT_FAIL)
                Assertions.fail(e);

            return;
        }

        if (scenario.clientFailAt != CLIENT_DOESNT_FAIL || scenario.serverFailAt != SERVER_DOESNT_FAIL)
            Assertions.fail("Handshake should've failed");

        assertNotNull(from2to1);

        // Ensure the handshake has finished on both sides.
        from2to1.send(messageFactory.testMessage().msg("test").build()).get(3, TimeUnit.SECONDS);

        NettySender from1to2 = manager1.channel(manager2.consistentId(), manager2.getLocalAddress()).get(3, TimeUnit.SECONDS);

        assertNotNull(from1to2);

        assertEquals(from2to1.channel().localAddress(), from1to2.channel().remoteAddress());
    }

    /**
     * Tests special handshake scenario: the client assumes a handshake has been finished, but the server fails
     * on client's response. The server will then close a connection and the client should get the
     * "connection closed event".
     *
     * @throws Exception If failed.
     */
    @Test
    public void testHandshakeFailsOnServerWhenClientResponded() throws Exception {
        ConnectionManager manager1 = startManager(
            4000,
            SERVER_CLIENT_RESPONDED,
            CLIENT_DOESNT_FAIL
        );

        ConnectionManager manager2 = startManager(
            4001,
            SERVER_DOESNT_FAIL,
            CLIENT_DOESNT_FAIL
        );

        NettySender from2to1 = manager2.channel(manager1.consistentId(), manager1.getLocalAddress()).get(3, TimeUnit.SECONDS);

        from2to1.channel().closeFuture().get(3, TimeUnit.SECONDS);
    }

    /**
     * Tests that one server can handle multiple incoming connections and perform the handshake operation successfully.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testHandshakeWithMultipleClients() throws Exception {
        ConnectionManager server = startManager(4000);

        List<ConnectionManager> clients = new ArrayList<>();

        int clientCount = 10;

        for (int i = 0; i < clientCount; i++)
            clients.add(startManager(4001 + i));

        // A key is the client's consistent id, a value is the channel between the client and the server
        var channelsToServer = new HashMap<String, NettySender>();

        for (ConnectionManager client : clients) {
            channelsToServer.put(
                client.consistentId(),
                client.channel(server.consistentId(), server.getLocalAddress()).get(3, TimeUnit.SECONDS)
            );
        }

        assertTrue(waitForCondition(() -> server.channels().size() == clientCount, TimeUnit.SECONDS.toMillis(3)));

        Map<String, NettySender> channels = server.channels();

        // Assert that server's channels are inbound connections opened from clients
        channelsToServer.forEach((consistentId, toServer) -> {
            NettySender toClient = channels.get(consistentId);

            assertNotNull(toClient);

            assertEquals(toServer.channel().localAddress(), toClient.channel().remoteAddress());
        });
    }

    /**
     * @return Generates handshake scenarios.
     */
    private static List<HandshakeScenario> handshakeScenarios() {
        ServerStageFail[] serverOpts = ServerStageFail.values();

        ClientStageFail[] clientOpts = ClientStageFail.values();

        List<HandshakeScenario> res = new ArrayList<>();

        for (ServerStageFail serverOpt : serverOpts)
            for (ClientStageFail clientOpt : clientOpts)
                // The case in if statement is handled in separate test
                if (serverOpt != SERVER_CLIENT_RESPONDED && clientOpt != CLIENT_DOESNT_FAIL)
                    res.add(new HandshakeScenario(serverOpt, clientOpt));

        return res;
    }

    /** Handshake scenario. */
    private static class HandshakeScenario {
        /** Stage to fail server handshake at. */
        private final ServerStageFail serverFailAt;

        /** Stage to fail client handshake at. */
        private final ClientStageFail clientFailAt;

        /** Constructor. */
        private HandshakeScenario(ServerStageFail serverFailAt, ClientStageFail clientFailAt) {
            this.serverFailAt = serverFailAt;
            this.clientFailAt = clientFailAt;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return String.format("server=%s, client=%s", serverFailAt, clientFailAt);
        }
    }

    /**
     * {@link RecoveryServerHandshakeManager} that can fail at specific stage of the handshake.
     */
    static class FailingRecoveryServerHandshakeManager extends RecoveryServerHandshakeManager {
        /**
         * At what stage to fail the handshake.
         */
        private final ServerStageFail failAtStage;

        /** Constructor. */
        private FailingRecoveryServerHandshakeManager(
            UUID launchId,
            String consistentId,
            ServerStageFail failAtStage,
            NetworkMessagesFactory messageFactory
        ) {
            super(launchId, consistentId, messageFactory);
            this.failAtStage = failAtStage;
        }

        /** {@inheritDoc} */
        @Override public HandshakeAction init(Channel channel) {
            if (failAtStage == SERVER_INIT) {
                handshakeFuture().completeExceptionally(new RuntimeException());
                return HandshakeAction.FAIL;
            }

            return super.init(channel);
        }

        /** {@inheritDoc} */
        @Override public HandshakeAction onConnectionOpen(Channel channel) {
            if (failAtStage == SERVER_CONNECTION_OPENED) {
                handshakeFuture().completeExceptionally(new RuntimeException());
                return HandshakeAction.FAIL;
            }

            return super.onConnectionOpen(channel);
        }

        /** {@inheritDoc} */
        @Override public HandshakeAction onMessage(Channel channel, NetworkMessage message) {
            if (failAtStage == SERVER_CLIENT_RESPONDED) {
                handshakeFuture().completeExceptionally(new RuntimeException());
                return HandshakeAction.FAIL;
            }

            return super.onMessage(channel, message);
        }

        /** Server handshake stage to fail at. */
        enum ServerStageFail {
            /** Don't fail at all. */
            SERVER_DOESNT_FAIL,

            /** Fail on init. */
            SERVER_INIT,

            /** Fail on connection open */
            SERVER_CONNECTION_OPENED,

            /** Fail on client response. */
            SERVER_CLIENT_RESPONDED
        }
    }

    /**
     * {@link RecoveryClientHandshakeManager} that can fail at specific stage of the handshake.
     */
    static class FailingRecoveryClientHandshakeManager extends RecoveryClientHandshakeManager {
        /**
         * At what stage to fail the handshake.
         */
        private final ClientStageFail failAtStage;

        /** Constructor. */
        private FailingRecoveryClientHandshakeManager(
            UUID launchId,
            String consistentId,
            ClientStageFail failAtStage,
            NetworkMessagesFactory messageFactory
        ) {
            super(launchId, consistentId, messageFactory);
            this.failAtStage = failAtStage;
        }

        /** {@inheritDoc} */
        @Override public HandshakeAction init(Channel channel) {
            if (failAtStage == CLIENT_INIT) {
                handshakeFuture().completeExceptionally(new RuntimeException());
                return HandshakeAction.FAIL;
            }

            return super.init(channel);
        }

        /** {@inheritDoc} */
        @Override public HandshakeAction onConnectionOpen(Channel channel) {
            if (failAtStage == CLIENT_CONNECTION_OPENED) {
                handshakeFuture().completeExceptionally(new RuntimeException());
                return HandshakeAction.FAIL;
            }

            return super.onConnectionOpen(channel);
        }

        /** {@inheritDoc} */
        @Override public HandshakeAction onMessage(Channel channel, NetworkMessage message) {
            if (failAtStage == CLIENT_SERVER_RESPONDED) {
                handshakeFuture().completeExceptionally(new RuntimeException());
                return HandshakeAction.FAIL;
            }

            return super.onMessage(channel, message);
        }

        /** Client handshake stage to fail at. */
        enum ClientStageFail {
            /** Don't fail at all. */
            CLIENT_DOESNT_FAIL,

            /** Fail on init. */
            CLIENT_INIT,

            /** Fail on connection open. */
            CLIENT_CONNECTION_OPENED,

            /** Fail on server response. */
            CLIENT_SERVER_RESPONDED
        }
    }

    /**
     * Starts a {@link ConnectionManager} adding it to the {@link #startedManagers} list.
     *
     * @param port Port for the {@link ConnectionManager#server}.
     * @param serverHandshakeFailAt At what stage to fail server handshake.
     * @param clientHandshakeFailAt At what stage to fail client handshake.
     * @return Connection manager.
     */
    private ConnectionManager startManager(
        int port,
        ServerStageFail serverHandshakeFailAt,
        ClientStageFail clientHandshakeFailAt
    ) {
        var registry = new TestMessageSerializationRegistryImpl();

        var messageFactory = new NetworkMessagesFactory();

        UUID launchId = UUID.randomUUID();
        String consistentId = UUID.randomUUID().toString();

        networkConfiguration.port().update(port).join();

        NetworkView cfg = networkConfiguration.value();

        var manager = new ConnectionManager(
            cfg,
            registry,
            consistentId,
            () -> new FailingRecoveryServerHandshakeManager(launchId, consistentId, serverHandshakeFailAt, messageFactory),
            () -> new FailingRecoveryClientHandshakeManager(launchId, consistentId, clientHandshakeFailAt, messageFactory)
        );

        manager.start();

        startedManagers.add(manager);

        return manager;
    }

    /**
     * Starts a {@link ConnectionManager} with a normal handshake manager adding it to the {@link #startedManagers} list.
     *
     * @param port Port.
     * @return Connection manager
     */
    private ConnectionManager startManager(int port) {
        var registry = new TestMessageSerializationRegistryImpl();

        var messageFactory = new NetworkMessagesFactory();

        UUID launchId = UUID.randomUUID();
        String consistentId = UUID.randomUUID().toString();

        networkConfiguration.port().update(port).join();

        NetworkView cfg = networkConfiguration.value();

        var manager = new ConnectionManager(
            cfg,
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
