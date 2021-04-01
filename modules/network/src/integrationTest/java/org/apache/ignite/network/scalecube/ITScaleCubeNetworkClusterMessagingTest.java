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

import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.network.Network;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkClusterEventHandler;
import org.apache.ignite.network.NetworkHandlersProvider;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.network.message.NetworkMessage;
import org.apache.ignite.network.NetworkMessageHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/** */
class ITScaleCubeNetworkClusterMessagingTest {
    /** */
    private static final long CHECK_INTERVAL = 200;

    /** */
    private static final long CLUSTER_TIMEOUT = TimeUnit.SECONDS.toMillis(3);

    /** */
    private final Queue<NetworkCluster> startedMembers = new ConcurrentLinkedQueue<>();

    /** */
    @AfterEach
    public void afterEach() throws Exception {
        Iterator<NetworkCluster> iterator = startedMembers.iterator();

        while (iterator.hasNext()) {
            iterator.next().shutdown();

            iterator.remove();
        }

        TestNetworkHandlersProvider.MESSAGE_STORAGE.clear();
    }

    /** */
    @Test
    public void messageWasSentToAllMembersSuccessfully() throws Exception {
        //Given: Three started member which are gathered to cluster.
        List<String> addresses = List.of("localhost:3344", "localhost:3345", "localhost:3346");

        CountDownLatch latch = new CountDownLatch(3);

        NetworkCluster alice = startMember("Alice", 3344, addresses);
        NetworkCluster bob = startMember("Bob", 3345, addresses);
        NetworkCluster carol = startMember("Carol", 3346, addresses);

        final NetworkHandlersProvider messageWaiter = new NetworkHandlersProvider() {
            /** {@inheritDoc} */
            @Override public NetworkMessageHandler messageHandler() {
                return message -> {
                    latch.countDown();
                };
            }
        };

        alice.addHandlersProvider(messageWaiter);
        bob.addHandlersProvider(messageWaiter);
        carol.addHandlersProvider(messageWaiter);

        waitForCluster(alice);

        TestMessage sentMessage = new TestMessage("Message from Alice");

        //When: Send one message to all members in cluster.
        for (NetworkMember member : alice.allMembers()) {
            System.out.println("SEND : " + member);

            alice.weakSend(member, sentMessage);
        }

        latch.await(3, TimeUnit.SECONDS);

        //Then: All members successfully received message.
        assertThat(getLastMessage(alice), is(sentMessage));
        assertThat(getLastMessage(bob), is(sentMessage));
        assertThat(getLastMessage(carol), is(sentMessage));
    }

    /** */
    private NetworkMessage getLastMessage(NetworkCluster alice) {
        return TestNetworkHandlersProvider.MESSAGE_STORAGE.get(alice.localMember().name());
    }

    /**
     * @return Started member.
     */
    private NetworkCluster startMember(String name, int port, List<String> addresses) {
        Network network = new Network(
            new ScaleCubeNetworkClusterFactory(name, port, addresses, new ScaleCubeMemberResolver())
        );

        network.registerMessageMapper(TestMessage.TYPE, new TestMessageMapperProvider());
        network.registerMessageMapper(TestRequest.TYPE, new TestRequestMapperProvider());
        network.registerMessageMapper(TestResponse.TYPE, new TestResponseMapperProvider());

        NetworkCluster member = network.start();

        member.addHandlersProvider(new TestNetworkHandlersProvider(name));

        System.out.println("-----" + name + " started");

        startedMembers.add(member);

        return member;
    }

    /**
     * Wait for cluster to come up.
     * @param cluster Network cluster.
     */
    private void waitForCluster(NetworkCluster cluster) {
        AtomicInteger integer = new AtomicInteger(0);

        cluster.addHandlersProvider(new NetworkHandlersProvider() {
            /** {@inheritDoc} */
            @Override public NetworkClusterEventHandler clusterEventHandler() {
                return new NetworkClusterEventHandler() {
                    /** {@inheritDoc} */
                    @Override public void onAppeared(NetworkMember member) {
                        integer.set(cluster.allMembers().size());
                    }

                    /** {@inheritDoc} */
                    @Override public void onDisappeared(NetworkMember member) {
                        integer.decrementAndGet();
                    }
                };
            }
        });

        integer.set(cluster.allMembers().size());

        long curTime = System.currentTimeMillis();
        long endTime = curTime + CLUSTER_TIMEOUT;

        while (curTime < endTime) {
            if (integer.get() == startedMembers.size()) {
                return;
            }

            if (CHECK_INTERVAL > 0) {
                try {
                    Thread.sleep(CHECK_INTERVAL);
                }
                catch (InterruptedException ignored) {
                }
            }

            curTime = System.currentTimeMillis();
        }

        throw new RuntimeException("Failed to wait for cluster startup");
    }

}
