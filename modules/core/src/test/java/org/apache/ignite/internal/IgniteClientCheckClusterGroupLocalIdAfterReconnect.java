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

package org.apache.ignite.internal;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Test;

/**
 * Tests that localId for cluster groups will be changed after reconnect.
 */
public class IgniteClientCheckClusterGroupLocalIdAfterReconnect extends GridCommonAbstractTest {
    /** Latch timeout*/
    private final int LATCH_TIMEOUT = 10_000;

    /** Object for messaging*/
    private static class External implements Externalizable {
        /** */
        private External() {}

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {}

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {}
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureDetectionTimeout(5000);
        cfg.setClientFailureDetectionTimeout(5000);

        return cfg;
    }

    /** Stop all nodes after test */
    @After
    public void tearDown() {
        stopAllGrids();
    }

    /**
     * Test checks that local id in cluster group was change and client
     * will be able to send the message to itself after reconnect.
     */
    @Test
    public void testClusterGroupLocalIdAfterClientReconnect() throws Exception {
        Ignite server = startGrid(0);

        Ignite client = startClientGrid(1);

        UUID clientId = client.cluster().node().id();

        ClusterGroup cg1 = client.cluster().forLocal();

        assertNotNull("Local client ID is different with local ClusterGroup node id. ", cg1.node(clientId));

        // check sending messages is possible while connected
        IgniteMessaging messaging = client.message(client.cluster().forLocal());

        CountDownLatch topicSignal = new CountDownLatch(2);

        messaging.localListen("topic", (IgniteBiPredicate<UUID, Object>)(uuid, n) -> {
            topicSignal.countDown();

            return true;
        });

        // countDown latch = 1
        messaging.send("topic", new External());

        CountDownLatch discSignal = new CountDownLatch(1);

        client.events().localListen((IgnitePredicate<DiscoveryEvent>)evt -> {
            discSignal.countDown();

            return true;
        }, EventType.EVT_CLIENT_NODE_DISCONNECTED);

        server.close();

        assertTrue("client did not disconnect", discSignal.await(LATCH_TIMEOUT, TimeUnit.SECONDS));

        startGrid(0);

        // wait for client reconnect
        IgniteFuture future = client.cluster().clientReconnectFuture();

        assertNotNull(future);

        future.get(20_000);   // throws if times out

        ClusterGroup cg2 = client.cluster().forLocal();

        UUID newClientId = client.cluster().localNode().id();

        assertNotNull("Local client ID wasn't changed for local ClusterGroup.", cg2.node(newClientId));

        awaitPartitionMapExchange();

        // check sending messages is possible after reconnecting
        // countDown latch = 0
        messaging = client.message(client.cluster().forLocal());

        messaging.send("topic", new External());

        assertTrue("Message wasn't received", topicSignal.await(LATCH_TIMEOUT, TimeUnit.SECONDS));
    }
}
