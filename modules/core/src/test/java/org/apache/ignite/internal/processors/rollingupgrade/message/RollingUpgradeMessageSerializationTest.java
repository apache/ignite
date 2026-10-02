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
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.junit.Test;

import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessage.A;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessage.B;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessage.C;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessage.D;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessage.E;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessage.F;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessageType.CONTAINER_MSG;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessageType.CORE_MSG;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessageType.DEFAULT_REGISTRY_MSG;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessageType.PLUGIN_MSG;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class RollingUpgradeMessageSerializationTest extends AbstractRollingUpgradeMessageTest {
    /** */
    @Test
    public void testSameOldVersion() throws Exception {
        checkMutualCoreMessageSend("2.19.0", "2.19.0", A, B, C, D, null, null);
    }

    /** */
    @Test
    public void testIntroducedField() throws Exception {
        checkMutualCoreMessageSend("2.18.0", "2.19.0", A, B, C, null, null, null);
    }

    /** */
    @Test
    public void testMixedPair() throws Exception {
        checkMutualCoreMessageSend("2.19.0", "2.20.0", A, B, C, D, null, null);
    }

    /** */
    @Test
    public void testSameNewVersion() throws Exception {
        checkMutualCoreMessageSend("2.20.0", "2.20.0", A, null, null, null, E, null);
    }

    /** */
    @Test
    public void testDeprecatedFieldEmptyAfterFinalization() throws Exception {
        checkMutualCoreMessageSend("2.19.2", "2.19.2", A, B, null, D, null, null);
    }

    /** */
    @Test
    public void testDeprecationUnknownToOlderPeer() throws Exception {
        checkMutualCoreMessageSend("2.19.2", "2.20.0", A, B, null, D, null, null);
    }

    /** */
    @Test
    public void testDeprecationKnownToOlderPeer() throws Exception {
        checkMutualCoreMessageSend("2.20.0", "2.20.1", A, null, null, null, E, null);
    }

    /** */
    @Test
    public void testDeprecatedFieldDropped() throws Exception {
        checkMutualCoreMessageSend("2.20.0", "2.21.0", A, null, null, null, E, null);
    }

    /** */
    @Test
    public void testDeprecatedFieldDroppedNewFieldShared() throws Exception {
        checkMutualCoreMessageSend("2.20.1", "2.21.0", A, null, null, null, E, F);
    }

    /** */
    @Test
    public void testBackportedFeature() throws Exception {
        checkMutualCoreMessageSend("2.19.3", "2.20.1", A, B, null, D, null, F);
    }

    /** */
    @Test
    public void testNestedMessages() throws Exception {
        startServerNodes("2.20.0", "2.21.0");

        checkNestedMessages(grid(0), grid(1), A, null, null, null, E, null);
        checkNestedMessages(grid(1), grid(0), A, null, null, null, E, null);
    }

    /** */
    @Test
    public void testDiscoveryNewerClient() throws Exception {
        IgniteEx srv = startGrid(0, "2.19.0");

        ru(srv).enableVersionUpgrade();

        startClientGrid(1, "2.20.0");

        checkCoreMessageBroadcast(srv, A, B, C, D, null, null);
    }

    /** */
    @Test
    public void testDiscoveryClientOriginated() throws Exception {
        IgniteEx srv = startGrid(0, "2.19.0");

        ru(srv).enableVersionUpgrade();

        IgniteEx cli1 = startClientGrid(1, "2.20.0");

        startClientGrid(2, "2.19.0");

        checkCoreMessageBroadcast(cli1, A, B, C, D, null, null);
    }

    /** */
    @Test
    public void testDiscoveryClientsOnDifferentVersions() throws Exception {
        startGrid(0, "2.19.0");
        startGrid(1, "2.19.0");

        ru(1).enableVersionUpgrade();

        upgradeNodeVersion(0, "2.20.0");
        upgradeNodeVersion(1, "2.20.0");

        stopGrid(0);

        IgniteEx newVerCli = startClientGrid(2, "2.20.0");
        IgniteEx oldVerCli = startClientGrid(3, "2.19.0");

        Map<String, TestDiscoveryMessage> receivedMsgs = sendOverDiscovery(grid(1), CORE_MSG);

        assertFields(A, B, C, D, E, null, receivedMsgs.get(newVerCli.name()));
        assertFields(A, B, C, D, null, null, receivedMsgs.get(oldVerCli.name()));

        checkMutualCoreMessageSend(newVerCli, oldVerCli, A, B, C, D, null, null);
    }

    /** */
    @Test
    public void testDiscoveryClientRouterChange() throws Exception {
        IgniteEx oldVerSrv = startGrid(0, "2.19.0");

        ru(oldVerSrv).enableVersionUpgrade();

        IgniteEx cli = startClientGrid(1, "2.20.0");
        IgniteEx newVerSrv = startGrid(2, "2.20.0");

        assertEquals(oldVerSrv.localNode().id(), routerId(cli));

        assertFields(A, B, C, D, null, null, sendOverDiscovery(newVerSrv, CORE_MSG).get(cli.name()));

        stopGrid(0);

        assertTrue(waitForCondition(() -> newVerSrv.localNode().id().equals(routerId(cli)), getTestTimeout()));

        assertFields(A, B, C, D, E, null, sendOverDiscovery(newVerSrv, CORE_MSG).get(cli.name()));
    }

    /** */
    @Test
    public void testCommunicationWithClient() throws Exception {
        IgniteEx srv = startGrid(0, "2.19.0");

        ru(srv).enableVersionUpgrade();

        IgniteEx client = startClientGrid(1, "2.20.0");

        checkMutualCoreMessageSend(srv, client, A, B, C, D, null, null);
    }

    /** */
    @Test
    public void testDefaultRegistryMixedPair() throws Exception {
        startServerNodes("2.19.0", "2.20.0");

        checkMutualMessageSend(grid(0), grid(1), DEFAULT_REGISTRY_MSG, A, null, C, D, E, F);
    }

    /** */
    @Test
    public void testDiscoveryUniformRing() throws Exception {
        startGrid(0, "2.20.0");
        startGrid(1, "2.20.0");
        startGrid(2, "2.20.0");

        checkCoreMessageBroadcast(grid(1), A, null, null, null, E, null);
    }

    /** */
    @Test
    public void testDiscoveryMixedRing() throws Exception {
        startGrid(0, "2.19.0");

        ru(grid(0)).enableVersionUpgrade();

        startGrid(1, "2.20.0");
        startGrid(2, "2.20.0");

        checkCoreMessageBroadcast(grid(1), A, B, C, D, null, null);
    }

    /** */
    @Test
    public void testDiscoveryRingSendFromNewerNode() throws Exception {
        startGrid(0, "2.19.0");

        ru(0).enableVersionUpgrade();

        IgniteEx newVerCrd = startGrid(1, "2.20.0");
        IgniteEx oldVerSrv = startGrid(2, "2.19.0");

        stopGrid(0);

        IgniteEx newVerSrv = startGrid(3, "2.20.0");

        Map<String, TestDiscoveryMessage> receivedMsgs = sendOverDiscovery(newVerSrv, CORE_MSG);

        assertFields(A, B, C, D, E, null, receivedMsgs.get(newVerCrd.name()));
        assertFields(A, B, C, D, null, null, receivedMsgs.get(oldVerSrv.name()));
    }

    /** */
    @Test
    public void testDeprecatedFieldKeptUntilFinalization() throws Exception {
        startGrid(0, "2.19.0");
        startGrid(1, "2.19.0");

        checkMutualCoreMessageSend(grid(0), grid(1), A, B, C, D, null, null);

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

        checkMutualCoreMessageSend(grid(0), grid(1), A, B, null, D, null, null);

        ru(1).enableVersionUpgrade();

        upgradeNodeVersion(0, "2.19.2", "2.20.0");

        checkMutualCoreMessageSend(grid(0), grid(1), A, B, null, D, null, null);

        upgradeNodeVersion(1, "2.19.2", "2.20.0");

        checkMutualCoreMessageSend(grid(0), grid(1), A, B, null, D, E, null);
    }

    /** */
    @Test
    public void testPluginDiffersCoreMatches() throws Exception {
        startServerNodes("2.20.0 | 1.0.0", "2.20.0 | 2.0.0");

        checkMutualCoreMessageSend(grid(0), grid(1), A, null, null, null, E, null);

        checkMutualMessageSend(grid(0), grid(1), PLUGIN_MSG, A, B, C, D, null, null);
    }

    /** */
    @Test
    public void testPluginSameVersion() throws Exception {
        startServerNodes("2.20.0 | 2.0.0", "2.20.0 | 2.0.0");

        checkMutualCoreMessageSend(grid(0), grid(1), A, null, null, null, E, null);

        checkMutualMessageSend(grid(0), grid(1), PLUGIN_MSG, A, null, C, null, E, null);
    }

    /** */
    @Test
    public void testPluginDeprecatedFieldDropped() throws Exception {
        startServerNodes("2.20.0 | 2.0.0", "2.20.0 | 3.0.0");

        checkMutualMessageSend(grid(0), grid(1), PLUGIN_MSG, A, null, C, null, E, null);
    }

    /** */
    @Test
    public void testCoreAndPluginDiffer() throws Exception {
        startServerNodes("2.19.2 | 1.0.0", "2.20.0 | 2.0.0");

        checkMutualCoreMessageSend(grid(0), grid(1), A, B, null, D, null, null);

        checkMutualMessageSend(grid(0), grid(1), PLUGIN_MSG, A, B, C, D, null, null);
    }

    /** */
    @Test
    public void testPluginWithoutFeaturesOnClient() throws Exception {
        IgniteEx srv = startGrid(0, "2.20.0 | 1.1.0");

        ru(srv).enableVersionUpgrade();

        IgniteEx cli = startClientGrid(1, "2.20.0 | 0.9.0");

        checkReceivedMessageFields(srv, cli, PLUGIN_MSG, A, null, C, null, null, null);
        checkReceivedMessageFields(cli, srv, PLUGIN_MSG, A, B, C, null, null, null);

        checkMutualCoreMessageSend(srv, cli, A, null, null, null, E, null);
    }

    /** */
    @Test
    public void testWholeUpgradeProcess() throws Exception {
        startGrid(0, "2.18.0");
        startGrid(1, "2.18.0");
        startClientGrid(2, "2.18.0");

        checkMessagesTransmissionBetweenAllNodes(A, B, C, null, null, null);

        ru(1).enableVersionUpgrade();

        upgradeNodeVersion(0, "2.18.0", "2.19.0");

        checkMessagesTransmissionBetweenAllNodes(A, B, C, null, null, null);

        upgradeNodeVersion(1, "2.18.0", "2.19.0");

        checkMutualCoreMessageSend(grid(0), grid(1), A, B, C, D, null, null);
        checkMutualCoreMessageSend(grid(0), grid(2), A, B, C, null, null, null);
        checkMutualCoreMessageSend(grid(1), grid(2), A, B, C, null, null, null);

        upgradeNodeVersion(2, "2.18.0", "2.19.0");

        checkMessagesTransmissionBetweenAllNodes(A, B, C, D, null, null);

        finalizeClusterVersion(0, "2.19.0");

        checkMessagesTransmissionBetweenAllNodes(A, B, C, D, null, null);

        ru(1).enableVersionUpgrade();

        upgradeNodeVersion(0, "2.19.0", "2.19.2");

        checkMessagesTransmissionBetweenAllNodes(A, B, C, D, null, null);

        upgradeNodeVersion(1, "2.19.0", "2.19.2");

        checkMessagesTransmissionBetweenAllNodes(A, B, C, D, null, null);

        upgradeNodeVersion(2, "2.19.0", "2.19.2");

        checkMessagesTransmissionBetweenAllNodes(A, B, C, D, null, null);

        finalizeClusterVersion(0, "2.19.2");

        checkMessagesTransmissionBetweenAllNodes(A, B, null, D, null, null);

        ru(1).enableVersionUpgrade();

        upgradeNodeVersion(0, "2.19.2", "2.20.0");

        checkMessagesTransmissionBetweenAllNodes(A, B, null, D, null, null);

        upgradeNodeVersion(1, "2.19.2", "2.20.0");

        checkMutualCoreMessageSend(grid(0), grid(1), A, B, null, D, E, null);
        checkMutualCoreMessageSend(grid(0), grid(2), A, B, null, D, null, null);
        checkMutualCoreMessageSend(grid(1), grid(2), A, B, null, D, null, null);

        upgradeNodeVersion(2, "2.19.2", "2.20.0");

        checkMessagesTransmissionBetweenAllNodes(A, B, null, D, E, null);

        finalizeClusterVersion(0, "2.20.0");

        checkMessagesTransmissionBetweenAllNodes(A, null, null, null, E, null);

        ru(1).enableVersionUpgrade();

        upgradeNodeVersion(0, "2.20.0", "2.20.1");

        checkMessagesTransmissionBetweenAllNodes(A, null, null, null, E, null);

        upgradeNodeVersion(1, "2.20.0", "2.20.1");

        checkMutualCoreMessageSend(grid(0), grid(1), A, null, null, null, E, F);
        checkMutualCoreMessageSend(grid(0), grid(2), A, null, null, null, E, null);
        checkMutualCoreMessageSend(grid(1), grid(2), A, null, null, null, E, null);

        upgradeNodeVersion(2, "2.20.0", "2.20.1");

        checkMessagesTransmissionBetweenAllNodes(A, null, null, null, E, F);

        finalizeClusterVersion(0, "2.20.1");

        checkMessagesTransmissionBetweenAllNodes(A, null, null, null, E, F);

        ru(1).enableVersionUpgrade();

        upgradeNodeVersion(0, "2.20.1", "2.21.0");

        checkMessagesTransmissionBetweenAllNodes(A, null, null, null, E, F);

        upgradeNodeVersion(1, "2.20.1", "2.21.0");

        checkMessagesTransmissionBetweenAllNodes(A, null, null, null, E, F);

        upgradeNodeVersion(2, "2.20.1", "2.21.0");

        checkMessagesTransmissionBetweenAllNodes(A, null, null, null, E, F);

        finalizeClusterVersion(0, "2.21.0");

        checkMessagesTransmissionBetweenAllNodes(A, null, null, null, E, F);
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
        checkMutualMessageSend(first, second, CORE_MSG, expA, expB, expC, expD, expE, expF);
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
        Collection<TestDiscoveryMessage> receivedMsgs = sendOverDiscovery(from, CORE_MSG).values();

        for (TestDiscoveryMessage rcvd : receivedMsgs)
            assertFields(expA, expB, expC, expD, expE, expF, rcvd);
    }

    /** */
    private void checkMutualMessageSend(
        IgniteEx first,
        IgniteEx second,
        TestMessageType msgType,
        String expA,
        String expB,
        String expC,
        String expD,
        String expE,
        String expF
    ) throws Exception {
        checkReceivedMessageFields(first, second, msgType, expA, expB, expC, expD, expE, expF);
        checkReceivedMessageFields(second, first, msgType, expA, expB, expC, expD, expE, expF);
    }

    /** */
    private void checkReceivedMessageFields(
        IgniteEx from,
        IgniteEx to,
        TestMessageType msgType,
        String expA,
        String expB,
        String expC,
        String expD,
        String expE,
        String expF
    ) throws Exception {
        assertFields(expA, expB, expC, expD, expE, expF, send(from, to, msgType));

        assertFields(expA, expB, expC, expD, expE, expF, sendOverDiscovery(from, msgType).get(to.name()));
    }

    /** */
    private void checkNestedMessages(
        IgniteEx from,
        IgniteEx to,
        String expA,
        String expB,
        String expC,
        String expD,
        String expE,
        String expF
    ) throws Exception {
        List<TestMessage> receivedMsgs = List.of(
            send(from, to, CONTAINER_MSG),
            sendOverDiscovery(from, CONTAINER_MSG).get(to.name())
        );

        for (TestMessage rcvd : receivedMsgs) {
            List<TestMessage> nestedMsgs = rcvd.nestedMessages();

            assertEquals(6, nestedMsgs.size());

            for (TestMessage nestedMsg : nestedMsgs)
                assertFields(expA, expB, expC, expD, expE, expF, nestedMsg);
        }
    }

    /** */
    private static UUID routerId(IgniteEx cli) {
        return ((TcpDiscoveryNode)cli.localNode()).clientRouterNodeId();
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
