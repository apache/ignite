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

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Tests for Zookeeper SPI discovery.
 */
public class ZookeeperDiscoveryCustomEventsTest extends ZookeeperDiscoverySpiTestBase {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCustomEventsSimple1_SingleNode() throws Exception {
        ZookeeperDiscoverySpiTestHelper.ackEveryEventSystemProperty();

        Ignite srv0 = startGrid(0);

        srv0.createCache(new CacheConfiguration<>("c1"));

        helper.waitForEventsAcks(srv0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCustomEventsSimple1_5_Nodes() throws Exception {
        ZookeeperDiscoverySpiTestHelper.ackEveryEventSystemProperty();

        Ignite srv0 = startGrids(5);

        srv0.createCache(new CacheConfiguration<>("c1"));

        awaitPartitionMapExchange();

        helper.waitForEventsAcks(srv0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCustomEvents_FastStopProcess_1() throws Exception {
        customEvents_FastStopProcess(1, 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCustomEvents_FastStopProcess_2() throws Exception {
        customEvents_FastStopProcess(5, 5);
    }

    /**
     * @param srvs Servers number.
     * @param clients Clients number.
     * @throws Exception If failed.
     */
    private void customEvents_FastStopProcess(int srvs, int clients) throws Exception {
        ZookeeperDiscoverySpiTestHelper.ackEveryEventSystemProperty();

        Map<UUID, List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>>> rcvdMsgs =
            new ConcurrentHashMap<>();

        Ignite crd = startGrid(0);

        UUID crdId = crd.cluster().localNode().id();

        if (srvs > 1)
            startGridsMultiThreaded(1, srvs - 1);

        if (clients > 0) {
            helper.clientMode(true);

            startGridsMultiThreaded(srvs, clients);
        }

        awaitPartitionMapExchange();

        List<Ignite> nodes = G.allGrids();

        assertEquals(srvs + clients, nodes.size());

        for (Ignite node : nodes)
            registerTestEventListeners(node, rcvdMsgs);

        int payload = 0;

        AffinityTopologyVersion topVer = ((IgniteKernal)crd).context().discovery().topologyVersionEx();

        for (Ignite node : nodes) {
            UUID sndId = node.cluster().localNode().id();

            info("Send from node: " + sndId);

            GridDiscoveryManager discoveryMgr = ((IgniteKernal)node).context().discovery();

            {
                List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>> expCrdMsgs = new ArrayList<>();
                List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>> expNodesMsgs = Collections.emptyList();

                TestFastStopProcessCustomMessage msg = new TestFastStopProcessCustomMessage(false, payload++);

                expCrdMsgs.add(new T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>(topVer, sndId, msg));

                discoveryMgr.sendCustomEvent(msg);

                doSleep(200); // Wait some time to check extra messages are not received.

                checkEvents(crd, rcvdMsgs, expCrdMsgs);

                for (Ignite node0 : nodes) {
                    if (node0 != crd)
                        checkEvents(node0, rcvdMsgs, expNodesMsgs);
                }

                rcvdMsgs.clear();
            }
            {
                List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>> expCrdMsgs = new ArrayList<>();
                List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>> expNodesMsgs = new ArrayList<>();

                TestFastStopProcessCustomMessage msg = new TestFastStopProcessCustomMessage(true, payload++);

                expCrdMsgs.add(new T3<>(topVer, sndId, msg));

                discoveryMgr.sendCustomEvent(msg);

                TestFastStopProcessCustomMessageAck ackMsg = new TestFastStopProcessCustomMessageAck(msg.payload);

                expCrdMsgs.add(new T3<>(topVer, crdId, ackMsg));
                expNodesMsgs.add(new T3<>(topVer, crdId, ackMsg));

                doSleep(200); // Wait some time to check extra messages are not received.

                checkEvents(crd, rcvdMsgs, expCrdMsgs);

                for (Ignite node0 : nodes) {
                    if (node0 != crd)
                        checkEvents(node0, rcvdMsgs, expNodesMsgs);
                }

                rcvdMsgs.clear();
            }

            helper.waitForEventsAcks(crd);
        }
    }

    /**
     * @param node Node to check.
     * @param rcvdMsgs Received messages.
     * @param expMsgs Expected messages.
     * @throws Exception If failed.
     */
    private void checkEvents(
        Ignite node,
        final Map<UUID, List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>>> rcvdMsgs,
        final List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>> expMsgs) throws Exception {
        final UUID nodeId = node.cluster().localNode().id();

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>> msgs = rcvdMsgs.get(nodeId);

                int size = msgs == null ? 0 : msgs.size();

                return size >= expMsgs.size();
            }
        }, 5000));

        List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>> msgs = rcvdMsgs.get(nodeId);

        if (msgs == null)
            msgs = Collections.emptyList();

        assertEqualsCollections(expMsgs, msgs);
    }

    /**
     * @param node Node.
     * @param rcvdMsgs Map to store received events.
     */
    private void registerTestEventListeners(Ignite node,
        final Map<UUID, List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>>> rcvdMsgs) {
        GridDiscoveryManager discoveryMgr = ((IgniteKernal)node).context().discovery();

        final UUID nodeId = node.cluster().localNode().id();

        discoveryMgr.setCustomEventListener(TestFastStopProcessCustomMessage.class,
            new CustomEventListener<TestFastStopProcessCustomMessage>() {
                @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, TestFastStopProcessCustomMessage msg) {
                    List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>> list = rcvdMsgs.get(nodeId);

                    if (list == null)
                        rcvdMsgs.put(nodeId, list = new ArrayList<>());

                    list.add(new T3<>(topVer, snd.id(), (DiscoveryCustomMessage)msg));
                }
            }
        );
        discoveryMgr.setCustomEventListener(TestFastStopProcessCustomMessageAck.class,
            new CustomEventListener<TestFastStopProcessCustomMessageAck>() {
                @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd, TestFastStopProcessCustomMessageAck msg) {
                    List<T3<AffinityTopologyVersion, UUID, DiscoveryCustomMessage>> list = rcvdMsgs.get(nodeId);

                    if (list == null)
                        rcvdMsgs.put(nodeId, list = new ArrayList<>());

                    list.add(new T3<>(topVer, snd.id(), (DiscoveryCustomMessage)msg));
                }
            }
        );
    }

    /** */
    private static class TestFastStopProcessCustomMessage implements DiscoveryCustomMessage {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final IgniteUuid id = IgniteUuid.randomUuid();

        /** */
        private final boolean createAck;

        /** */
        private final int payload;

        /**
         * @param createAck Create ack message flag.
         * @param payload Payload.
         */
        TestFastStopProcessCustomMessage(boolean createAck, int payload) {
            this.createAck = createAck;
            this.payload = payload;

        }

        /** {@inheritDoc} */
        @Override public IgniteUuid id() {
            return id;
        }

        /** {@inheritDoc} */
        @Nullable @Override public DiscoveryCustomMessage ackMessage() {
            return createAck ? new TestFastStopProcessCustomMessageAck(payload) : null;
        }

        /** {@inheritDoc} */
        @Override public boolean isMutable() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean stopProcess() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr,
            AffinityTopologyVersion topVer,
            DiscoCache discoCache) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestFastStopProcessCustomMessage that = (TestFastStopProcessCustomMessage)o;

            return createAck == that.createAck && payload == that.payload;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(createAck, payload);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestFastStopProcessCustomMessage.class, this);
        }
    }

    /** */
    private static class TestFastStopProcessCustomMessageAck implements DiscoveryCustomMessage {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final IgniteUuid id = IgniteUuid.randomUuid();

        /** */
        private final int payload;

        /**
         * @param payload Payload.
         */
        TestFastStopProcessCustomMessageAck(int payload) {
            this.payload = payload;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public @Nullable  DiscoveryCustomMessage ackMessage() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isMutable() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean stopProcess() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr,
            AffinityTopologyVersion topVer,
            DiscoCache discoCache) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestFastStopProcessCustomMessageAck that = (TestFastStopProcessCustomMessageAck)o;
            return payload == that.payload;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(payload);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestFastStopProcessCustomMessageAck.class, this);
        }
    }
}
