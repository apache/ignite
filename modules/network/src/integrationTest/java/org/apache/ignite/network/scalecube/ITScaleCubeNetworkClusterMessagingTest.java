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
import org.apache.ignite.network.MessageHandlerHolder;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkClusterFactory;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.network.NetworkMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/** */
class ITScaleCubeNetworkClusterMessagingTest {
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
    @Disabled
    public void messageWasSentToAllMembersSuccessfully() {
        //Given: Three started member which are gathered to cluster.
        List<String> addresses = List.of("localhost:3344", "localhost:3345", "localhost:3346");

        NetworkCluster alice = startMember("Alice", 3344, addresses);
        NetworkCluster bob = startMember("Bob", 3345, addresses);
        NetworkCluster carol = startMember("Carol", 3346, addresses);

        TestMessage sentMessage = new TestMessage("Message from Alice");

        //When: Send one message to all members in cluster.
        for (NetworkMember member : alice.allMembers()) {
            System.out.println("SEND : " + member);

            alice.weakSend(member, sentMessage);
        }

        //Then: All members successfully received message.
        assertThat(getLastMessage(alice).data(), is(sentMessage));
        assertThat(getLastMessage(bob).data(), is(sentMessage));
        assertThat(getLastMessage(carol).data(), is(sentMessage));
    }

    /** */
    private NetworkMessage getLastMessage(NetworkCluster alice) {
        return TestNetworkHandlersProvider.MESSAGE_STORAGE.get(alice.localMember().name());
    }

    /**
     * @return Started member.
     */
    private NetworkCluster startMember(String name, int port, List<String> addresses) {
        NetworkCluster member = new NetworkClusterFactory(name, port, addresses)
            .startScaleCubeBasedCluster(new ScaleCubeMemberResolver(), new MessageHandlerHolder());

        member.addHandlersProvider(new TestNetworkHandlersProvider(name));

        System.out.println("-----" + name + " started");

        startedMembers.add(member);

        return member;
    }

}