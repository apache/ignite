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
package org.apache.ignite.spi.discovery.tcp;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class TcpDiscoveryNodeJoinAndFailureTest extends GridCommonAbstractTest {
    /** */
    private static final String NODE_WITH_PORT_ID_0 = "node0-47500";

    /** */
    private static final String NODE_WITH_PORT_ID_1 = "node1-47501";

    /** */
    private static final String NODE_WITH_PORT_ID_2 = "node2-47502";

    /** */
    private static final String NODE_WITH_PORT_ID_3 = "node3-47503";

    /** */
    private boolean usePortFromNodeName;

    /** */
    private TcpDiscoverySpi specialSpi;

    /** */
    private TcpDiscoveryIpFinder specialIpFinder0;

    /** */
    private TcpDiscoveryIpFinder specialIpFinder1;

    private UUID nodeId;

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi spi = specialSpi != null ? specialSpi : new TcpDiscoverySpi();

        if (usePortFromNodeName)
            spi.setLocalPort(Integer.parseInt(igniteInstanceName.split("-")[1]));

        if (specialIpFinder0 != null && igniteInstanceName.equals(NODE_WITH_PORT_ID_2))
            spi.setIpFinder(specialIpFinder0);
        else if (specialIpFinder1 != null && igniteInstanceName.equals(NODE_WITH_PORT_ID_3))
            spi.setIpFinder(specialIpFinder1);
        else
            spi.setIpFinder(sharedStaticIpFinder);

        spi.setNetworkTimeout(2500);

        spi.setIpFinderCleanFrequency(5000);

        spi.setJoinTimeout(5000);

        spi.setConnectionRecoveryTimeout(0);

        cfg.setDiscoverySpi(spi);

        cfg.setFailureDetectionTimeout(7500);

        if (nodeId != null && igniteInstanceName.equals(NODE_WITH_PORT_ID_2))
            cfg.setNodeId(nodeId);

        return cfg;
    }

    /**
     * If whole ring fails but two server nodes both in CONNECTING state remain alive they should not hang
     * indefinitely sending join requests to each other.
     *
     * @see <a href="https://issues.apache.org/jira/browse/IGNITE-11621">IGNITE-11621</a> with comments provides detailed description of this corner case.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE, value = "false")
    public void testConnectingNodesStopIfNoConnectedNodeIsPresented() throws Exception {
        /*
            Test reproduces the needed behavior (two nodes in CONNECTING state) doing the following:
                - it starts two regular nodes, node0 (coordinator) and node1 (just another server) with special
                  discovery SPIs;
                - when node1 receives NodeAddFinished for subsequently started node2, it doesn't send it to the node
                  but closes disco socket to node2 leaving it in CONNECTING state and generating NodeFailed for it.
                  Also at this moment node3 is started;
                - when node0 receives this NodeFailed it fails (because special SPI throws an exception) and stops;
                - when node1 receives another join request from node2 or NodeAdded from node3 reaches it back,
                  node1's special SPI also throws an exception so it goes down as well;
                - as a result, both node2 and node3 get stuck in CONNECTING state and as they use special IpFinders
                  they see each other and are able to send join requests to each other back and forth.

            The whole purpose of the test is to verify that these two nodes won't stuck in CONNECTING state forever
            and will eventually stop.
         */

        usePortFromNodeName = true;

        final AtomicInteger joinReqsCntr = new AtomicInteger(0);

        final AtomicReference<IgniteInternalFuture> futureRef = new AtomicReference();

        final UUID node2Id = UUID.randomUUID();

        final TcpDiscoverySpi node0SpecialSpi = new TcpDiscoverySpi() {
            @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
                if (msg instanceof TcpDiscoveryNodeFailedMessage) {
                    TcpDiscoveryNodeFailedMessage failedMsg = (TcpDiscoveryNodeFailedMessage)msg;

                    UUID failedNodeId = failedMsg.failedNodeId();

                    if (failedNodeId.equals(node2Id))
                        throw new RuntimeException("Stop node0 exception");
                }

                if (msg instanceof TcpDiscoveryJoinRequestMessage) {
                    TcpDiscoveryJoinRequestMessage joinReq = (TcpDiscoveryJoinRequestMessage)msg;

                    if (joinReq.node().id().equals(node2Id))
                        joinReqsCntr.incrementAndGet();
                }
            }
        };

        final TcpDiscoverySpi node1SpecialSpi = new TcpDiscoverySpi() {
            @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
                if (msg instanceof TcpDiscoveryNodeAddFinishedMessage) {
                    TcpDiscoveryNodeAddFinishedMessage finishedMsg = (TcpDiscoveryNodeAddFinishedMessage)msg;

                    UUID nodeId = finishedMsg.nodeId();

                    if (nodeId.equals(node2Id)) {
                        Object workerObj = GridTestUtils.getFieldValue(impl, "msgWorker");

                        OutputStream out = GridTestUtils.getFieldValue(workerObj, "out");

                        try {
                            out.close();

                            log.warning("Out to 'sick' node closed");
                        }
                        catch (Exception ignored) {
                            // No-op.
                        }

                        futureRef.set(GridTestUtils.runAsync(() -> {
                            try {
                                startGrid(NODE_WITH_PORT_ID_3);
                            } catch (Exception ignored) {
                                //NO-op.
                            }
                        }));
                    }
                }

                if (msg instanceof TcpDiscoveryJoinRequestMessage) {
                    TcpDiscoveryJoinRequestMessage joinReq = (TcpDiscoveryJoinRequestMessage)msg;

                    int joinReqsCount = joinReqsCntr.get();

                    if (joinReq.node().id().equals(node2Id) && joinReqsCount == 1)
                        throw new RuntimeException("Stop node1 exception by subsequent join req");
                }

                if (msg instanceof TcpDiscoveryNodeAddedMessage) {
                    TcpDiscoveryNodeAddedMessage addedMsg = (TcpDiscoveryNodeAddedMessage)msg;

                    if (addedMsg.node().discoveryPort() == 47503)
                        throw new RuntimeException("Stop node1 exception by new node added msg");
                }
            }
        };

        specialSpi = node0SpecialSpi;

        startGrid(NODE_WITH_PORT_ID_0);

        specialSpi = node1SpecialSpi;

        startGrid(NODE_WITH_PORT_ID_1);

        specialIpFinder0 = new TcpDiscoveryVmIpFinder(false);

        ((TcpDiscoveryVmIpFinder)specialIpFinder0).setAddresses(Arrays.asList("127.0.0.1:47501","127.0.0.1:47503"));

        specialIpFinder1 = new TcpDiscoveryVmIpFinder(false);

        ((TcpDiscoveryVmIpFinder)specialIpFinder1).setAddresses(Arrays.asList("127.0.0.1:47502"));

        specialSpi = null;

        nodeId = node2Id;

        boolean expectedExceptionThrown = false;

        try {
            startGrid(NODE_WITH_PORT_ID_2);
        }
        catch (Exception e) {
            Throwable cause0 = e.getCause();

            assertNotNull(cause0);

            Throwable cause1 = cause0.getCause();

            assertNotNull(cause1);

            String errorMsg = cause1.getMessage();

            assertTrue("Expected error message was not found: " + errorMsg, errorMsg.contains("Failed to connect to any address from IP finder"));

            expectedExceptionThrown = true;
        }

        assertTrue("Expected exception was not thrown.", expectedExceptionThrown);

        IgniteInternalFuture startGridFut = futureRef.get();

        if (startGridFut != null)
            startGridFut.get();
    }
}
