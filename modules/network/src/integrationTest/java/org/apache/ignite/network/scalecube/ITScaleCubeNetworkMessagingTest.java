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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.NetworkMessageHandler;
import org.apache.ignite.network.TestMessage;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.apache.ignite.network.message.NetworkMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** */
class ITScaleCubeNetworkMessagingTest {
    /** */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistry();

    /** */
    private static final ClusterServiceFactory NETWORK_FACTORY = new ScaleCubeClusterServiceFactory();

    /** */
    private final Map<String, NetworkMessage> messageStorage = new ConcurrentHashMap<>();

    /** */
    private final List<ClusterService> startedMembers = new ArrayList<>();

    /** */
    @AfterEach
    public void afterEach() {
        startedMembers.forEach(ClusterService::shutdown);
    }

    /**
     * Test sending and receiving messages.
     */
    @Test
    public void messageWasSentToAllMembersSuccessfully() throws Exception {
        //Given: Three started member which are gathered to cluster.
        List<String> addresses = List.of("localhost:3344", "localhost:3345", "localhost:3346");

        CountDownLatch messageReceivedLatch = new CountDownLatch(3);

        String aliceName = "Alice";

        ClusterService alice = startNetwork(aliceName, 3344, addresses);
        ClusterService bob = startNetwork("Bob", 3345, addresses);
        ClusterService carol = startNetwork("Carol", 3346, addresses);

        NetworkMessageHandler messageWaiter = (message, sender, correlationId) -> messageReceivedLatch.countDown();

        alice.messagingService().addMessageHandler(messageWaiter);
        bob.messagingService().addMessageHandler(messageWaiter);
        carol.messagingService().addMessageHandler(messageWaiter);

        TestMessage testMessage = new TestMessage("Message from Alice", Collections.emptyMap());

        //When: Send one message to all members in cluster.
        for (ClusterNode member : alice.topologyService().allMembers()) {
            alice.messagingService().weakSend(member, testMessage);
        }

        boolean messagesReceived = messageReceivedLatch.await(3, TimeUnit.SECONDS);
        assertTrue(messagesReceived);

        //Then: All members successfully received message.
        assertThat(getLastMessage(alice), is(testMessage));
        assertThat(getLastMessage(bob), is(testMessage));
        assertThat(getLastMessage(carol), is(testMessage));

        CountDownLatch aliceShutdownLatch = new CountDownLatch(1);

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

        alice.shutdown();

        boolean aliceShutdownReceived = aliceShutdownLatch.await(3, TimeUnit.SECONDS);
        assertTrue(aliceShutdownReceived);

        Collection<ClusterNode> networkMembers = bob.topologyService().allMembers();

        assertEquals(2, networkMembers.size());
    }

    /** */
    private NetworkMessage getLastMessage(ClusterService clusterService) {
        return messageStorage.get(clusterService.localConfiguration().getName());
    }

    /** */
    private ClusterService startNetwork(String name, int port, List<String> addresses) {
        var context = new ClusterLocalConfiguration(name, port, addresses, SERIALIZATION_REGISTRY);

        ClusterService clusterService = NETWORK_FACTORY.createClusterService(context);

        clusterService.messagingService().addMessageHandler((message, sender, correlationId) -> {
            messageStorage.put(name, message);
        });

        clusterService.start();

        startedMembers.add(clusterService);

        return clusterService;
    }
}
