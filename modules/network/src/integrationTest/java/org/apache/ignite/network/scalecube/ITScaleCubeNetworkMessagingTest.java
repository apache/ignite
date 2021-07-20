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
package org.apache.ignite.network.scalecube;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.transport.api.Transport;
import org.apache.ignite.internal.network.NetworkMessageTypes;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.TestMessage;
import org.apache.ignite.network.TestMessageSerializationRegistryImpl;
import org.apache.ignite.network.TestMessageTypes;
import org.apache.ignite.network.TestMessagesFactory;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.annotations.MessageGroup;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** */
class ITScaleCubeNetworkMessagingTest {
    /**
     * Test cluster.
     * <p>
     * Each test should create its own cluster with the required number of nodes.
     */
    private Cluster testCluster;

    /** Message factory. */
    private final TestMessagesFactory messageFactory = new TestMessagesFactory();

    /** */
    @AfterEach
    public void afterEach() {
        testCluster.shutdown();
    }

    /**
     * Tests sending and receiving messages.
     */
    @Test
    public void messageWasSentToAllMembersSuccessfully() throws Exception {
        Map<String, TestMessage> messageStorage = new ConcurrentHashMap<>();

        var messageReceivedLatch = new CountDownLatch(3);

        testCluster = new Cluster(3);

        for (ClusterService member : testCluster.members) {
            member.messagingService().addMessageHandler(
                TestMessageTypes.class,
                (message, senderAddr, correlationId) -> {
                    messageStorage.put(member.localConfiguration().getName(), (TestMessage)message);
                    messageReceivedLatch.countDown();
                }
            );
        }

        testCluster.startAwait();

        var testMessage = messageFactory.testMessage().msg("Message from Alice").build();

        ClusterService alice = testCluster.members.get(0);

        for (ClusterNode member : alice.topologyService().allMembers())
            alice.messagingService().weakSend(member, testMessage);

        boolean messagesReceived = messageReceivedLatch.await(3, TimeUnit.SECONDS);
        assertTrue(messagesReceived);

        testCluster.members.stream()
            .map(member -> member.localConfiguration().getName())
            .map(messageStorage::get)
            .forEach(msg -> assertThat(msg.msg(), is(testMessage.msg())));
    }

    /**
     * Tests graceful shutdown.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testShutdown() throws Exception {
        testShutdown0(false);
    }

    /**
     * Tests forceful shutdown.
     * @throws Exception If failed.
     */
    @Test
    public void testForcefulShutdown() throws Exception {
        testShutdown0(true);
    }

    /**
     * Sends a message from a node to itself and verifies that it gets delivered successfully.
     */
    @Test
    public void testSendMessageToSelf() throws Exception {
        testCluster = new Cluster(1);
        testCluster.startAwait();

        ClusterService member = testCluster.members.get(0);

        ClusterNode self = member.topologyService().localMember();

        class Data {
            private final TestMessage message;

            private final NetworkAddress sender;

            private final String correlationId;

            private Data(TestMessage message, NetworkAddress sender, String correlationId) {
                this.message = message;
                this.sender = sender;
                this.correlationId = correlationId;
            }
        }

        var dataFuture = new CompletableFuture<Data>();

        member.messagingService().addMessageHandler(
            TestMessageTypes.class,
            (message, senderAddr, correlationId) ->
                dataFuture.complete(new Data((TestMessage)message, senderAddr, correlationId))
        );

        var requestMessage = messageFactory.testMessage().msg("request").build();
        var correlationId = "foobar";

        member.messagingService().send(self, requestMessage, correlationId);

        Data actualData = dataFuture.get(3, TimeUnit.SECONDS);

        assertThat(actualData.message.msg(), is(requestMessage.msg()));
        assertThat(actualData.sender, is(self.address()));
        assertThat(actualData.correlationId, is(correlationId));
    }

    /**
     * Sends a messages from a node to itself and awaits the response.
     */
    @Test
    public void testInvokeMessageToSelf() throws Exception {
        testCluster = new Cluster(1);
        testCluster.startAwait();

        ClusterService member = testCluster.members.get(0);

        ClusterNode self = member.topologyService().localMember();

        var requestMessage = messageFactory.testMessage().msg("request").build();
        var responseMessage = messageFactory.testMessage().msg("response").build();

        member.messagingService().addMessageHandler(
            TestMessageTypes.class,
            (message, senderAddr, correlationId) -> {
                if (message.equals(requestMessage))
                    member.messagingService().send(self, responseMessage, correlationId);
            }
        );

        TestMessage actualResponseMessage = member.messagingService()
            .invoke(self, requestMessage, 1000)
            .thenApply(TestMessage.class::cast)
            .get(3, TimeUnit.SECONDS);

        assertThat(actualResponseMessage.msg(), is(responseMessage.msg()));
    }

    /**
     * Serializable message that belongs to the {@link NetworkMessageTypes} message group.
     */
    private static class MockNetworkMessage implements NetworkMessage, Serializable {
        /** {@inheritDoc} */
        @Override public short messageType() {
            return 666;
        }

        /** {@inheritDoc} */
        @Override public short groupType() {
            return NetworkMessageTypes.class.getAnnotation(MessageGroup.class).groupType();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return getClass() == obj.getClass();
        }
    }

    /**
     * Tests that messages from different message groups can be delivered to different sets of handlers.
     */
    @Test
    public void testMessageGroupsHandlers() throws Exception {
        testCluster = new Cluster(2);
        testCluster.startAwait();

        ClusterService node1 = testCluster.members.get(0);
        ClusterService node2 = testCluster.members.get(1);

        var testMessageFuture1 = new CompletableFuture<NetworkMessage>();
        var testMessageFuture2 = new CompletableFuture<NetworkMessage>();
        var networkMessageFuture = new CompletableFuture<NetworkMessage>();

        // register multiple handlers for the same group
        node1.messagingService().addMessageHandler(
            TestMessageTypes.class,
            (message, senderAddr, correlationId) -> assertTrue(testMessageFuture1.complete(message))
        );

        node1.messagingService().addMessageHandler(
            TestMessageTypes.class,
            (message, senderAddr, correlationId) -> assertTrue(testMessageFuture2.complete(message))
        );

        // register a different handle for the second group
        node1.messagingService().addMessageHandler(
            NetworkMessageTypes.class,
            (message, senderAddr, correlationId) -> assertTrue(networkMessageFuture.complete(message))
        );

        var testMessage = messageFactory.testMessage().msg("foo").build();

        var networkMessage = new MockNetworkMessage();

        // test that a message gets delivered to both handlers
        node2.messagingService()
            .send(node1.topologyService().localMember(), testMessage)
            .get(1, TimeUnit.SECONDS);

        // test that a message from the other group is only delivered to a single handler
        node2.messagingService()
            .send(node1.topologyService().localMember(), networkMessage)
            .get(1, TimeUnit.SECONDS);

        assertThat(testMessageFuture1, willBe(equalTo(testMessage)));
        assertThat(testMessageFuture2, willBe(equalTo(testMessage)));
        assertThat(networkMessageFuture, willBe(equalTo(networkMessage)));
    }

    /**
     * Tests shutdown.
     *
     * @param forceful Whether shutdown should be forceful.
     * @throws Exception If failed.
     */
    private void testShutdown0(boolean forceful) throws Exception {
        testCluster = new Cluster(2);
        testCluster.startAwait();

        ClusterService alice = testCluster.members.get(0);
        ClusterService bob = testCluster.members.get(1);
        String aliceName = alice.localConfiguration().getName();

        var aliceShutdownLatch = new CountDownLatch(1);

        bob.topologyService().addEventHandler(new TopologyEventHandler() {
            /** {@inheritDoc} */
            @Override public void onAppeared(ClusterNode member) {
                // No-op.
            }

            /** {@inheritDoc} */
            @Override public void onDisappeared(ClusterNode member) {
                if (aliceName.equals(member.name()))
                    aliceShutdownLatch.countDown();
            }
        });

        if (forceful)
            stopForcefully(alice);
        else
            alice.shutdown();

        boolean aliceShutdownReceived = aliceShutdownLatch.await(forceful ? 10 : 3, TimeUnit.SECONDS);
        assertTrue(aliceShutdownReceived);

        Collection<ClusterNode> networkMembers = bob.topologyService().allMembers();

        assertEquals(1, networkMembers.size());
    }

    /**
     * Find the cluster's transport and force it to stop.
     *
     * @param cluster Cluster to be shutdown.
     * @throws Exception If failed to stop.
     */
    private static void stopForcefully(ClusterService cluster) throws Exception {
        Field clusterImplField = cluster.getClass().getDeclaredField("val$cluster");
        clusterImplField.setAccessible(true);

        ClusterImpl clusterImpl = (ClusterImpl) clusterImplField.get(cluster);
        Field transportField = clusterImpl.getClass().getDeclaredField("transport");
        transportField.setAccessible(true);

        Transport transport = (Transport) transportField.get(clusterImpl);
        Method stop = transport.getClass().getDeclaredMethod("stop");
        stop.setAccessible(true);

        Mono<?> invoke = (Mono<?>) stop.invoke(transport);
        invoke.block();
    }

    /**
     * Wrapper for a cluster.
     */
    private static final class Cluster {
        /** */
        private final ClusterServiceFactory networkFactory = new TestScaleCubeClusterServiceFactory();

        /** */
        private final MessageSerializationRegistry serializationRegistry = new TestMessageSerializationRegistryImpl();

        /** */
        final List<ClusterService> members;

        /** */
        private final CountDownLatch startupLatch;

        /** Constructor. */
        Cluster(int numOfNodes) {
            startupLatch = new CountDownLatch(numOfNodes - 1);

            int initialPort = 3344;

            List<NetworkAddress> addresses = IntStream.range(0, numOfNodes)
                .mapToObj(i -> new NetworkAddress("localhost", initialPort + i))
                .collect(Collectors.toUnmodifiableList());

            members = IntStream.range(0, numOfNodes)
                .mapToObj(i -> startNode("Node #" + i, initialPort + i, addresses, i == 0))
                .collect(Collectors.toUnmodifiableList());
        }

        /**
         * Start cluster node.
         *
         * @param name Node name.
         * @param port Node port.
         * @param addresses Addresses of other nodes.
         * @param initial Whether this node is the first one.
         * @return Started cluster node.
         */
        private ClusterService startNode(String name, int port, List<NetworkAddress> addresses, boolean initial) {
            var context = new ClusterLocalConfiguration(name, port, addresses, serializationRegistry);

            ClusterService clusterService = networkFactory.createClusterService(context);

            if (initial)
                clusterService.topologyService().addEventHandler(new TopologyEventHandler() {
                    /** {@inheritDoc} */
                    @Override public void onAppeared(ClusterNode member) {
                        startupLatch.countDown();
                    }

                    /** {@inheritDoc} */
                    @Override public void onDisappeared(ClusterNode member) {
                    }
                });

            return clusterService;
        }

        /**
         * Start and wait for cluster to come up.
         *
         * @throws InterruptedException If failed.
         */
        void startAwait() throws InterruptedException {
            members.forEach(ClusterService::start);

            if (!startupLatch.await(3, TimeUnit.SECONDS))
                throw new AssertionError();
        }

        /**
         * Shutdown cluster.
         */
        void shutdown() {
            members.forEach(ClusterService::shutdown);
        }
    }
}
