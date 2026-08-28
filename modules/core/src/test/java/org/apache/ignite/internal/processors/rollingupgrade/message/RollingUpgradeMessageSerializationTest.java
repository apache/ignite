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

package org.apache.ignite.internal.processors.rollingupgrade.message;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.rollingupgrade.AbstractRollingUpgradeTest;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.MessagesPluginProvider;
import org.junit.Test;

import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessage.A;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessage.B;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessage.C;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessage.D;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessage.E;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessage.F;

/** */
public class RollingUpgradeMessageSerializationTest extends AbstractRollingUpgradeTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName, String ver) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName, ver);

        cfg.setPluginProviders(org.apache.ignite.internal.util.typedef.F.concat(
            cfg.getPluginProviders(),
            new MessagesPluginProvider(
                TestCoreMessage.class,
                TestPluginMessage.class,
                TestDefaultRegistryMessage.class))
        );

        return cfg;
    }

    /** */
    @Test
    public void testSameOldVersion() throws Exception {
        checkMutualCoreMessageSend("2.19.0", "2.19.0", A, B, C, null, null, null);
    }

    /** */
    @Test
    public void testMixedPair() throws Exception {
        checkMutualCoreMessageSend("2.19.0", "2.20.0", A, B, C, null, null, null);
    }

    /** */
    @Test
    public void testSameNewVersion() throws Exception {
        checkMutualCoreMessageSend("2.20.0", "2.20.0", A, B, C, D, E, null);
    }

    /** */
    @Test
    public void testWindowOpenSameVersion() throws Exception {
        checkMutualCoreMessageSend("2.19.2", "2.19.2", A, B, C, D, null, null);
    }

    /** */
    @Test
    public void testWindowOpenMixedPair() throws Exception {
        checkMutualCoreMessageSend("2.19.2", "2.20.0", A, B, C, D, null, null);
    }

    /** */
    @Test
    public void testWindowClosed() throws Exception {
        checkMutualCoreMessageSend("2.20.0", "2.20.1", A, null, C, null, E, null);
    }

    /** */
    @Test
    public void testDiscoveryNewerClient() throws Exception {
        IgniteEx srv = startGrid(0, "2.19.0");

        ru(srv).enableVersionUpgrade();

        startClientGrid(1, "2.20.0");

        checkCoreMessageBroadcast(srv, A, B, C, null, null, null);
    }

    /** */
    @Test
    public void testDiscoveryClientOriginated() throws Exception {
        IgniteEx srv = startGrid(0, "2.19.0");

        ru(srv).enableVersionUpgrade();

        IgniteEx cli1 = startClientGrid(1, "2.20.0");

        startClientGrid(2, "2.19.0");

        checkCoreMessageBroadcast(cli1, A, B, C, null, null, null);
    }

    /** */
    @Test
    public void testDiscoveryClientsOnDifferentVersions() throws Exception {
        startGrid(0, "2.19.0");
        startGrid(1, "2.19.0");

        ru(1).enableVersionUpgrade();

        upgradeNodeVersion(0, "2.20.0");
        upgradeNodeVersion(1, "2.20.0");

        IgniteEx newVerCli = startClientGrid(2, "2.20.0");
        IgniteEx oldVerCli = startClientGrid(3, "2.19.0");

        Map<String, TestCoreMessage> receivedMsgs = sendOverDiscovery(grid(1), TestCoreMessage.build());

        assertFields(A, B, C, D, E, null, receivedMsgs.get(newVerCli.name()));
        assertFields(A, B, C, null, null, null, receivedMsgs.get(oldVerCli.name()));
    }

    /** */
    @Test
    public void testCommunicationWithClient() throws Exception {
        IgniteEx srv = startGrid(0, "2.19.0");

        ru(srv).enableVersionUpgrade();

        IgniteEx client = startClientGrid(1, "2.20.0");

        checkMutualCoreMessageSend(srv, client, A, B, C, null, null, null);
    }

    /** */
    @Test
    public void testDefaultRegistryMixedPair() throws Exception {
        startServerNodes("2.19.0", "2.20.0");

        checkMutualMessageSend(grid(0), grid(1), TestDefaultRegistryMessage::build, A, null, C, D, E, F);
    }

    /** */
    @Test
    public void testDiscoveryUniformRing() throws Exception {
        startGrid(0, "2.20.0");
        startGrid(1, "2.20.0");
        startGrid(2, "2.20.0");

        checkCoreMessageBroadcast(grid(1), A, B, C, D, E, null);
    }

    /** */
    @Test
    public void testDiscoveryMixedRing() throws Exception {
        startGrid(0, "2.19.0");

        ru(grid(0)).enableVersionUpgrade();

        startGrid(1, "2.20.0");
        startGrid(2, "2.20.0");

        checkCoreMessageBroadcast(grid(1), A, B, C, null, null, null);
    }


    /** */
    @Test
    public void testCommunicationUpgradeOpensWindow() throws Exception {
        startGrid(0, "2.19.0");
        startGrid(1, "2.19.0");

        checkMutualCoreMessageSend(grid(0), grid(1), A, B, C, null, null, null);

        ru(1).enableVersionUpgrade();

        upgradeNodeVersion(0, "2.19.2");
        upgradeNodeVersion(1, "2.19.2");

        checkMutualCoreMessageSend(grid(0), grid(1), A, B, C, D, null, null);
    }

    /** */
    @Test
    public void testCommunicationUpgradeAgreesNewFeature() throws Exception {
        startGrid(0, "2.19.2");
        startGrid(1, "2.19.2");

        checkMutualCoreMessageSend(grid(0), grid(1), A, B, C, D, null, null);

        ru(1).enableVersionUpgrade();

        upgradeNodeVersion(0, "2.19.2", "2.20.0");

        checkMutualCoreMessageSend(grid(0), grid(1), A, B, C, D, null, null);

        upgradeNodeVersion(1, "2.19.2", "2.20.0");

        checkMutualCoreMessageSend(grid(0), grid(1), A, B, C, D, E, null);
    }

    /** */
    @Test
    public void testPluginDiffersCoreMatches() throws Exception {
        startServerNodes("2.20.0 | 1.0.0", "2.20.0 | 2.0.0");

        checkMutualCoreMessageSend(grid(0), grid(1), A, B, C, D, E, null);

        checkMutualMessageSend(grid(0), grid(1), TestPluginMessage::build, A, B, C, D, null, null);
    }

    /** */
    @Test
    public void testPluginSameVersion() throws Exception {
        startServerNodes("2.20.0 | 2.0.0", "2.20.0 | 2.0.0");

        checkMutualCoreMessageSend(grid(0), grid(1), A, B, C, D, E, null);

        checkMutualMessageSend(grid(0), grid(1), TestPluginMessage::build, A, B, C, D, E, null);
    }

    /** */
    @Test
    public void testPluginMissingOnClient() throws Exception {
        IgniteEx srv = startGrid(0, "2.20.0 | 2.0.0");

        ru(srv).enableVersionUpgrade();

        IgniteEx cli = startClientGrid(1, "2.20.0");

        checkMutualMessageSend(srv, cli, TestPluginMessage::build, A, B, C, null, null, null);

        checkMutualCoreMessageSend(srv, cli, A, B, C, D, E, null);
    }

    /** */
    @Test
    public void testWholeUpgradeProcess() throws Exception {
        startGrid(0, "2.19.0");
        startGrid(1, "2.19.0");
        startClientGrid(2, "2.19.0");

        checkMessagesTransmissionBetweenAllNodes(A, B, C, null, null, null);

        ru(1).enableVersionUpgrade();

        checkMessagesTransmissionBetweenAllNodes(A, B, C, null, null, null);

        upgradeNodeVersion(0, "2.19.0", "2.19.2");

        checkMessagesTransmissionBetweenAllNodes(A, B, C, null, null, null);

        upgradeNodeVersion(1, "2.19.0", "2.19.2");

        checkMutualCoreMessageSend(grid(0), grid(1), A, B, C, D, null, null);
        checkMutualCoreMessageSend(grid(0), grid(2), A, B, C, null, null, null);
        checkMutualCoreMessageSend(grid(1), grid(2), A, B, C, null, null, null);

        upgradeNodeVersion(2, "2.19.0", "2.19.2");

        checkMessagesTransmissionBetweenAllNodes(A, B, C, D, null, null);

        finalizeClusterVersion(0, "2.19.2");

        checkMessagesTransmissionBetweenAllNodes(A, B, C, D, null, null);

        ru(1).enableVersionUpgrade();

        upgradeNodeVersion(0, "2.19.2", "2.20.0");

        checkMessagesTransmissionBetweenAllNodes(A, B, C, D, null, null);

        upgradeNodeVersion(1, "2.19.2", "2.20.0");

        checkMutualCoreMessageSend(grid(0), grid(1), A, B, C, D, E, null);
        checkMutualCoreMessageSend(grid(0), grid(2), A, B, C, D, null, null);
        checkMutualCoreMessageSend(grid(1), grid(2), A, B, C, D, null, null);

        upgradeNodeVersion(2, "2.19.2", "2.20.0");

        checkMessagesTransmissionBetweenAllNodes(A, B, C, D, E, null);

        finalizeClusterVersion(0, "2.20.0");

        checkMessagesTransmissionBetweenAllNodes(A, B, C, D, E, null);

        ru(1).enableVersionUpgrade();

        upgradeNodeVersion(0, "2.20.0", "2.20.1");

        checkMutualCoreMessageSend(grid(0), grid(1), A, null, C, null, E, null);
        checkMutualCoreMessageSend(grid(0), grid(2), A, null, C, null, E, null);
        checkMutualCoreMessageSend(grid(1), grid(2), A, B, C, D, E, null);

        upgradeNodeVersion(1, "2.20.0", "2.20.1");

        checkMutualCoreMessageSend(grid(0), grid(1), A, B, C, D, E, F);
        checkMutualCoreMessageSend(grid(0), grid(2), A, null, C, null, E, null);
        checkMutualCoreMessageSend(grid(1), grid(2), A, null, C, null, E, null);

        upgradeNodeVersion(2, "2.20.0", "2.20.1");

        checkMessagesTransmissionBetweenAllNodes(A, B, C, D, E, F);

        finalizeClusterVersion(0, "2.20.1");

        checkMessagesTransmissionBetweenAllNodes(A, B, C, D, E, F);
    }

    /** */
    private void checkMessagesTransmissionBetweenAllNodes(
        String expA,
        String expB,
        String expC,
        String expD,
        String expE,
        String expF
    ) throws Exception {
        List<Ignite> clusterNodes = Ignition.allGrids();

        for (int i = 0; i < clusterNodes.size(); i++) {
            for (int j = i + 1; j < clusterNodes.size(); j++) {
                checkMutualCoreMessageSend(
                    (IgniteEx)clusterNodes.get(i), (IgniteEx)clusterNodes.get(j), expA, expB, expC, expD, expE, expF);
            }
        }
    }

    /** */
    private void checkMutualCoreMessageSend(
        String firstVer,
        String secondVer,
        String expA,
        String expB,
        String expC,
        String expD,
        String expE,
        String expF
    ) throws Exception {
        startServerNodes(firstVer, secondVer);

        checkMutualCoreMessageSend(grid(0), grid(1), expA, expB, expC, expD, expE, expF);
    }

    /** */
    private void checkMutualCoreMessageSend(
        IgniteEx first,
        IgniteEx second,
        String expA,
        String expB,
        String expC,
        String expD,
        String expE,
        String expF
    ) throws Exception {
        checkMutualMessageSend(first, second, TestCoreMessage::build, expA, expB, expC, expD, expE, expF);
    }

    /** */
    private void checkCoreMessageBroadcast(
        IgniteEx from,
        String expA,
        String expB,
        String expC,
        String expD,
        String expE,
        String expF
    ) throws Exception {
        Collection<TestCoreMessage> receivedMsgs = sendOverDiscovery(from, TestCoreMessage.build()).values();

        for (TestCoreMessage msg : receivedMsgs)
            assertFields(expA, expB, expC, expD, expE, expF, msg);
    }

    /** */
    private <T extends DiscoveryCustomMessage & TestMessage> void checkMutualMessageSend(
        IgniteEx first,
        IgniteEx second,
        Supplier<T> msgFactory,
        String expA,
        String expB,
        String expC,
        String expD,
        String expE,
        String expF
    ) throws Exception {
        checkReceivedMessageFields(first, second, msgFactory, expA, expB, expC, expD, expE, expF);
        checkReceivedMessageFields(second, first, msgFactory, expA, expB, expC, expD, expE, expF);
    }

    /** */
    private <T extends DiscoveryCustomMessage & TestMessage> void checkReceivedMessageFields(
        IgniteEx from,
        IgniteEx to,
        Supplier<T> msgFactory,
        String expA,
        String expB,
        String expC,
        String expD,
        String expE,
        String expF
    ) throws Exception {
        assertFields(expA, expB, expC, expD, expE, expF, send(from, to, msgFactory.get()));

        assertFields(expA, expB, expC, expD, expE, expF, sendOverDiscovery(from, msgFactory.get()).get(to.name()));
    }

    /** */
    private <T extends Message & TestMessage> T send(IgniteEx from, IgniteEx to, T msg) throws Exception {
        AtomicReference<T> got = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        String topic = msg.getClass().getName();

        to.context().io().addMessageListener(topic, (nodeId, rcvd, plc) -> {
            got.set((T)rcvd);

            latch.countDown();
        });

        ClusterNode rcvNode = from.context().discovery().node(to.localNode().id());

        from.context().io().sendToCustomTopic(rcvNode, topic, msg, GridIoPolicy.PUBLIC_POOL);

        assertTrue(latch.await(getTestTimeout(), TimeUnit.MILLISECONDS));

        return got.get();
    }

    /** */
    private <T extends DiscoveryCustomMessage & TestMessage> Map<String, T> sendOverDiscovery(
        IgniteEx from,
        T msg
    ) throws Exception {
        List<Ignite> clusterNodes = Ignition.allGrids();

        Map<String, T> receivedMsgs = new ConcurrentHashMap<>();

        CountDownLatch latch = new CountDownLatch(clusterNodes.size());

        for (Ignite rcv : clusterNodes) {
            String name = rcv.name();

            ((IgniteEx)rcv).context().discovery().setCustomEventListener((Class<T>)msg.getClass(),
                (v, n, m) -> {
                    receivedMsgs.put(name, m);

                    latch.countDown();
                });
        }

        from.context().discovery().sendCustomEvent(msg);

        assertTrue(latch.await(getTestTimeout(), TimeUnit.MILLISECONDS));

        receivedMsgs.remove(from.name());

        return receivedMsgs;
    }

    /** */
    private void startServerNodes(String firstVer, String secondVer) throws Exception {
        IgniteEx first = startGrid(0, firstVer);

        if (!firstVer.equals(secondVer))
            ru(first).enableVersionUpgrade();

        startGrid(1, secondVer);
    }

    /** */
    private static void assertFields(
        String expA,
        String expB,
        String expC,
        String expD,
        String expE,
        String expF,
        TestMessage msg
    ) {
        assertEquals(expA, msg.fldA());
        assertEquals(expB, msg.fldB());
        assertEquals(expC, msg.fldC());
        assertEquals(expD, msg.fldD());
        assertEquals(expE, msg.fldE());
        assertEquals(expF, msg.fldF());
    }
}
