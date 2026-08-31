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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
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
    public void testDiscoveryNewerClient() throws Exception {
        IgniteEx srv = startGrid(0, "2.19.0");

        ru(srv).enableVersionUpgrade();

        IgniteEx client = startClientGrid(1, "2.20.0");

        TestCoreMessage receivedMsg = sendOverDiscovery(srv, TestCoreMessage.build()).get(client.name());

        assertFields(B, null, null, null, receivedMsg);
    }

    /** */
    @Test
    public void testDiscoveryClientOriginated() throws Exception {
        IgniteEx srv = startGrid(0, "2.19.0");

        ru(srv).enableVersionUpgrade();

        IgniteEx cli1 = startClientGrid(1, "2.20.0");
        IgniteEx cli2 = startClientGrid(2, "2.19.0");

        Map<String, TestCoreMessage> receivedMsgs = sendOverDiscovery(cli1, TestCoreMessage.build());

        assertFields(B, null, null, null, receivedMsgs.get(cli2.name()));
        assertFields(B, null, null, null, receivedMsgs.get(srv.name()));
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

        assertFields(B, D, E, null, receivedMsgs.get(newVerCli.name()));
        assertFields(B, null, null, null, receivedMsgs.get(oldVerCli.name()));
    }

    /** */
    @Test
    public void testDiscoveryClientsOnOneVersion() throws Exception {
        IgniteEx srv = startGrid(0, "2.20.0");

        startClientGrid(1, "2.20.0");
        startClientGrid(2, "2.20.0");

        Map<String, TestCoreMessage> receivedMsgs = sendOverDiscovery(srv, TestCoreMessage.build());

        for (TestCoreMessage msg : receivedMsgs.values())
            assertFields(B, D, E, null, msg);
    }

    /** */
    @Test
    public void testCommunicationWithClient() throws Exception {
        IgniteEx srv = startGrid(0, "2.19.0");

        ru(srv).enableVersionUpgrade();

        IgniteEx client = startClientGrid(1, "2.20.0");

        TestCoreMessage receivedMsg = send(srv, client, TestCoreMessage.build());

        assertFields(B, null, null, null, receivedMsg);

        receivedMsg = send(client, srv, TestCoreMessage.build());

        assertFields(B, null, null, null, receivedMsg);
    }

    /** */
    @Test
    public void testDefaultRegistrySameOldVersion() throws Exception {
        startPair("2.19.0", "2.19.0");

        assertFields(B, D, null, null, send(grid(0), grid(1), TestDefaultRegistryMessage.build()));
    }

    /** */
    @Test
    public void testDefaultRegistryMixedPair() throws Exception {
        startPair("2.19.0", "2.20.0");

        assertFields(null, D, null, null, send(grid(1), grid(0), TestDefaultRegistryMessage.build()));

        assertFields(null, D, null, null, send(grid(0), grid(1), TestDefaultRegistryMessage.build()));
    }

    /** */
    @Test
    public void testDefaultRegistrySameNewVersion() throws Exception {
        startPair("2.20.0", "2.20.0");

        assertFields(B, D, null, null, send(grid(0), grid(1), TestDefaultRegistryMessage.build()));
    }

    /** */
    @Test
    public void testCommunicationSameOldVersion() throws Exception {
        startPair("2.19.0", "2.19.0");

        TestCoreMessage receivedMsg = send(grid(0), grid(1), TestCoreMessage.build());

        assertFields(B, null, null, null, receivedMsg);
    }

    /** */
    @Test
    public void testCommunicationNewerSender() throws Exception {
        startPair("2.19.0", "2.20.0");

        TestCoreMessage receivedMsg = send(grid(1), grid(0), TestCoreMessage.build());

        assertFields(B, null, null, null, receivedMsg);
    }

    /** */
    @Test
    public void testCommunicationOlderSender() throws Exception {
        startPair("2.19.0", "2.20.0");

        TestCoreMessage receivedMsg = send(grid(0), grid(1), TestCoreMessage.build());

        assertFields(B, null, null, null, receivedMsg);
    }

    /** */
    @Test
    public void testCommunicationSameNewVersion() throws Exception {
        startPair("2.20.0", "2.20.0");

        TestCoreMessage receivedMsg = send(grid(0), grid(1), TestCoreMessage.build());

        assertFields(B, D, E, null, receivedMsg);
    }

    /** */
    @Test
    public void testDiscoverySameOldVersion() throws Exception {
        startPair("2.19.0", "2.19.0");

        for (TestCoreMessage msg : sendOverDiscovery(grid(1), TestCoreMessage.build()).values())
            assertFields(B, null, null, null, msg);
    }

    /** */
    @Test
    public void testDiscoveryMixedPair() throws Exception {
        startPair("2.19.0", "2.20.0");

        for (TestCoreMessage msg : sendOverDiscovery(grid(1), TestCoreMessage.build()).values())
            assertFields(B, null, null, null, msg);
    }

    /** */
    @Test
    public void testDiscoveryUniformRing() throws Exception {
        startGrid(0, "2.20.0");
        startGrid(1, "2.20.0");
        startGrid(2, "2.20.0");

        Map<String, TestCoreMessage> receivedMsgs = sendOverDiscovery(grid(1), TestCoreMessage.build());

        for (TestCoreMessage msg : receivedMsgs.values())
            assertFields(B, D, E, null, msg);
    }

    /** */
    @Test
    public void testDiscoveryMixedRing() throws Exception {
        startGrid(0, "2.19.0");

        ru(grid(0)).enableVersionUpgrade();

        startGrid(1, "2.20.0");
        startGrid(2, "2.20.0");

        Map<String, TestCoreMessage> receivedMsgs = sendOverDiscovery(grid(1), TestCoreMessage.build());

        for (TestCoreMessage msg : receivedMsgs.values())
            assertFields(B, null, null, null, msg);
    }


    /** */
    @Test
    public void testCommunicationWindowOpenSameVersion() throws Exception {
        startPair("2.19.2", "2.19.2");

        TestCoreMessage receivedMsg = send(grid(0), grid(1), TestCoreMessage.build());

        assertFields(B, D, null, null, receivedMsg);
    }

    /** */
    @Test
    public void testCommunicationWindowOpenNewerSender() throws Exception {
        startPair("2.19.2", "2.20.0");

        TestCoreMessage receivedMsg = send(grid(1), grid(0), TestCoreMessage.build());

        assertFields(B, D, null, null, receivedMsg);
    }

    /** */
    @Test
    public void testCommunicationWindowOpenOlderSender() throws Exception {
        startPair("2.19.2", "2.20.0");

        TestCoreMessage receivedMsg = send(grid(0), grid(1), TestCoreMessage.build());

        assertFields(B, D, null, null, receivedMsg);
    }


    /** */
    @Test
    public void testCommunicationWindowClosed() throws Exception {
        startPair("2.20.0", "2.20.1");

        TestCoreMessage receivedMsg = send(grid(1), grid(0), TestCoreMessage.build());

        assertFields(null, null, E, null, receivedMsg);
    }

    /** */
    @Test
    public void testCommunicationUpgradeOpensWindow() throws Exception {
        startGrid(0, "2.19.0");
        startGrid(1, "2.19.0");

        TestCoreMessage receivedMsg = send(grid(0), grid(1), TestCoreMessage.build());

        assertFields(B, null, null, null, receivedMsg);

        ru(1).enableVersionUpgrade();

        upgradeNodeVersion(0, "2.19.2");
        upgradeNodeVersion(1, "2.19.2");

        receivedMsg = send(grid(0), grid(1), TestCoreMessage.build());

        assertFields(B, D, null, null, receivedMsg);
    }

    /** */
    @Test
    public void testCommunicationUpgradeAgreesNewFeature() throws Exception {
        startGrid(0, "2.19.2");
        startGrid(1, "2.19.2");

        TestCoreMessage receivedMsg = send(grid(0), grid(1), TestCoreMessage.build());

        assertFields(B, D, null, null, receivedMsg);

        ru(1).enableVersionUpgrade();

        upgradeNodeVersion(0, "2.19.2", "2.20.0");

        receivedMsg = send(grid(0), grid(1), TestCoreMessage.build());

        assertFields(B, D, null, null, receivedMsg);

        upgradeNodeVersion(1, "2.19.2", "2.20.0");

        receivedMsg = send(grid(0), grid(1), TestCoreMessage.build());

        assertFields(B, D, E, null, receivedMsg);
    }

    /** */
    @Test
    public void testPluginDiffersCoreMatches() throws Exception {
        startPair("2.20.0 | 1.0.0", "2.20.0 | 2.0.0");

        checkPluginDiffersCoreMatches(grid(1), grid(0));

        checkPluginDiffersCoreMatches(grid(1), grid(0));
    }

    /** */
    @Test
    public void testPluginSameVersion() throws Exception {
        startPair("2.20.0 | 2.0.0", "2.20.0 | 2.0.0");

        TestCoreMessage receivedCoreMsg = send(grid(0), grid(1), TestCoreMessage.build());

        assertFields(B, D, E, null, receivedCoreMsg);

        TestPluginMessage receivedPluginMsg = send(grid(0), grid(1), TestPluginMessage.build());

        assertFields(B, D, E, null, receivedPluginMsg);
    }

    /** */
    @Test
    public void testPluginMissingOnClient() throws Exception {
        IgniteEx srv = startGrid(0, "2.20.0 | 2.0.0");

        ru(srv).enableVersionUpgrade();

        IgniteEx cli = startClientGrid(1, "2.20.0");

        checkPluginMissingOnClient(srv, cli);

        checkPluginMissingOnClient(cli, srv);
    }

    /** */
    @Test
    public void testWholeUpgradeProcess() throws Exception {
        startGrid(0, "2.19.0");
        startGrid(1, "2.19.0");
        startClientGrid(2, "2.19.0");

        checkMessagesTransmissionBetweenAllNodes(B, null, null, null);

        ru(1).enableVersionUpgrade();

        checkMessagesTransmissionBetweenAllNodes(B, null, null, null);

        upgradeNodeVersion(0, "2.19.0", "2.19.2");

        checkMessagesTransmissionBetweenAllNodes(B, null, null, null);

        upgradeNodeVersion(1, "2.19.0", "2.19.2");

        checkMutualMessageTransmission(grid(0), grid(1), B, D, null, null);
        checkMutualMessageTransmission(grid(0), grid(2), B, null, null, null);
        checkMutualMessageTransmission(grid(1), grid(2), B, null, null, null);

        upgradeNodeVersion(2, "2.19.0", "2.19.2");

        checkMessagesTransmissionBetweenAllNodes(B, D, null, null);

        finalizeClusterVersion(0, "2.19.2");

        checkMessagesTransmissionBetweenAllNodes(B, D, null, null);

        ru(1).enableVersionUpgrade();

        upgradeNodeVersion(0, "2.19.2", "2.20.0");

        checkMessagesTransmissionBetweenAllNodes(B, D, null, null);

        upgradeNodeVersion(1, "2.19.2", "2.20.0");

        checkMutualMessageTransmission(grid(0), grid(1), B, D, E, null);
        checkMutualMessageTransmission(grid(0), grid(2), B, D, null, null);
        checkMutualMessageTransmission(grid(1), grid(2), B, D, null, null);

        upgradeNodeVersion(2, "2.19.2", "2.20.0");

        checkMessagesTransmissionBetweenAllNodes(B, D, E, null);

        finalizeClusterVersion(0, "2.20.0");

        checkMessagesTransmissionBetweenAllNodes(B, D, E, null);

        ru(1).enableVersionUpgrade();

        upgradeNodeVersion(0, "2.20.0", "2.20.1");

        checkMutualMessageTransmission(grid(0), grid(1), null, null, E, null);
        checkMutualMessageTransmission(grid(0), grid(2), null, null, E, null);
        checkMutualMessageTransmission(grid(1), grid(2), B, D, E, null);

        upgradeNodeVersion(1, "2.20.0", "2.20.1");

        checkMutualMessageTransmission(grid(0), grid(1), B, D, E, F);
        checkMutualMessageTransmission(grid(0), grid(2), null, null, E, null);
        checkMutualMessageTransmission(grid(1), grid(2), null, null, E, null);

        upgradeNodeVersion(2, "2.20.0", "2.20.1");

        checkMessagesTransmissionBetweenAllNodes(B, D, E, F);

        finalizeClusterVersion(0, "2.20.1");

        checkMessagesTransmissionBetweenAllNodes(B, D, E, F);
    }

    /** */
    private void checkMessagesTransmissionBetweenAllNodes(String expB, String expD, String expE, String expF) throws Exception {
        List<Ignite> clusterNodes = Ignition.allGrids();

        for (int i = 0; i < clusterNodes.size(); i++) {
            for (int j = i + 1; j < clusterNodes.size(); j++) {
                checkMutualMessageTransmission(
                    (IgniteEx)clusterNodes.get(i), (IgniteEx)clusterNodes.get(j), expB, expD, expE, expF);
            }
        }
    }

    /** */
    private void checkMutualMessageTransmission(
        IgniteEx first,
        IgniteEx second,
        String expB,
        String expD,
        String expE,
        String expF
    ) throws Exception {
        checkMessageTransmission(first, second, expB, expD, expE, expF);
        checkMessageTransmission(second, first, expB, expD, expE, expF);
    }

    /** */
    private void checkMessageTransmission(
        IgniteEx from,
        IgniteEx to,
        String expB,
        String expD,
        String expE,
        String expF
    ) throws Exception {
        TestCoreMessage receivedCommunicationMsg = send(from, to, TestCoreMessage.build());

        assertFields(expB, expD, expE, expF, receivedCommunicationMsg);

        TestCoreMessage receivedDiscoveryMsg = sendOverDiscovery(from, TestCoreMessage.build()).get(to.name());

        assertFields(expB, expD, expE, expF, receivedDiscoveryMsg);
    }

    /** */
    private void checkPluginDiffersCoreMatches(IgniteEx snd, IgniteEx rcv) throws Exception {
        TestCoreMessage receivedCoreMsg = send(snd, rcv, TestCoreMessage.build());

        assertFields(B, D, E, null, receivedCoreMsg);

        TestPluginMessage receivedPluginMsg = send(snd, rcv, TestPluginMessage.build());

        assertFields(B, D, null, null, receivedPluginMsg);
    }

    /** */
    private void checkPluginMissingOnClient(IgniteEx snd, IgniteEx rcv) throws Exception {
        TestPluginMessage receivedPluginMsg = send(snd, rcv, TestPluginMessage.build());

        assertFields(B, null, null, null, receivedPluginMsg);

        TestCoreMessage receivedCoreMsg = send(snd, rcv, TestCoreMessage.build());

        assertFields(B, D, E, null, receivedCoreMsg);
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
    private Map<String, TestCoreMessage> sendOverDiscovery(
        IgniteEx from,
        TestCoreMessage msg
    ) throws Exception {
        List<Ignite> clusterNodes = Ignition.allGrids();

        Map<String, TestCoreMessage> receivedMsgs = new ConcurrentHashMap<>();

        CountDownLatch latch = new CountDownLatch(clusterNodes.size());

        for (Ignite rcv : clusterNodes) {
            String name = rcv.name();

            ((IgniteEx)rcv).context().discovery().setCustomEventListener(TestCoreMessage.class,
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
    private void startPair(String firstVer, String secondVer) throws Exception {
        IgniteEx first = startGrid(0, firstVer);

        if (!firstVer.equals(secondVer))
            ru(first).enableVersionUpgrade();

        startGrid(1, secondVer);
    }

    /** */
    private static void assertFields(String expB, String expD, String expE, String expF, TestMessage msg) {
        assertEquals(A, msg.fldA());
        assertEquals(C, msg.fldC());
        assertEquals(expB, msg.fldB());
        assertEquals(expD, msg.fldD());
        assertEquals(expE, msg.fldE());
        assertEquals(expF, msg.fldF());
    }
}
