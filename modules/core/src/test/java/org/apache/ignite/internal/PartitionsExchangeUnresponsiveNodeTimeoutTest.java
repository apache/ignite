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

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeFinishedCheckRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeFinishedCheckResponse;
import org.apache.ignite.internal.util.lang.GridAbsPredicateX;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class PartitionsExchangeUnresponsiveNodeTimeoutTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestCommunicationSpi());
        cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.setExchangeHardTimeout(10_000);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (Ignite ig : G.allGrids()) {
            ((TestCommunicationSpi)ig.configuration().getCommunicationSpi()).blockExchangeMessages = false;
            ((TestCommunicationSpi)ig.configuration().getCommunicationSpi()).sndOutdatedCheckMessages = false;
        }

        List<Ignite> nodes = G.allGrids();

        while (!nodes.isEmpty()) {
            for (Ignite node : nodes)
                stopGrid(node.name(), true, true);

            nodes = G.allGrids();
        }

        super.afterTest();
    }

    /**
     * Tests situation when non-coordinator node becomes unresponsive. This node should be kicked from cluster.
     *
     * @throws Exception if failed.
     */
    public void testNonCoordinatorUnresponsive() throws Exception {
        final Ignite ig = startGrids(4);

        ig.cluster().active(true);

        final ClusterNode failNode = ignite(1).cluster().localNode();

        ((TestCommunicationSpi) ignite(1).configuration().getCommunicationSpi()).blockExchangeMessages = true;

        GridTestUtils.runAsync(() -> startGrid(4));

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                Collection<ClusterNode> nodes = ig.cluster().nodes();

                return nodes.size() == 4 && !nodes.contains(failNode);
            }
        }, 60_000));
    }

    /**
     * Tests situation when coordinator node becomes unresponsive to exchange messages. Other nodes should leave cluster.
     *
     * @throws Exception if failed.
     */
    public void testCoordinatorUnresponsive() throws Exception {
        final Ignite ig = startGrids(4);

        ig.cluster().active(true);

        ((TestCommunicationSpi)ignite(0).configuration().getCommunicationSpi()).blockExchangeMessages = true;

        GridTestUtils.runAsync(() -> startGrid(4));

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                return ig.cluster().nodes().size() == 1;
            }
        }, 3 * 60_000));
    }

    /**
     * Tests situation when coordinator node becomes unresponsive to exchange and version check messages.
     * Other nodes should leave cluster.
     *
     * @throws Exception if failed.
     */
    public void testCoordinatorUnresponsiveWithCheckMessagesBlocked() throws Exception {
        final Ignite ig = startGrids(4);

        ig.cluster().active(true);

        ((TestCommunicationSpi)ig.configuration().getCommunicationSpi()).blockExchangeMessages = true;
        ((TestCommunicationSpi)ig.configuration().getCommunicationSpi()).blockCheckMessages = true;

        GridTestUtils.runAsync(() -> startGrid(4));

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                return ig.cluster().nodes().size() == 1;
            }
        }, 3 * 60_000));
    }

    /**
     * Tests situation when coordinator node becomes unresponsive to exchange messages, but continues to reply to
     * version check messages with outdated version. Coordinator should be kicked from cluster.
     *
     * @throws Exception if failed.
     */
    public void testCoordinatorUnresponsiveWithOutdatedCheckMessages() throws Exception {
        startGrids(4);

        final Ignite ig = grid(1);

        ig.cluster().active(true);

        UUID failNode = ignite(0).cluster().localNode().id();

        ((TestCommunicationSpi)ignite(0).configuration().getCommunicationSpi()).blockExchangeMessages = true;
        ((TestCommunicationSpi)ignite(0).configuration().getCommunicationSpi()).sndOutdatedCheckMessages = true;

        GridTestUtils.runAsync(() -> startGrid(4));

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                Collection<ClusterNode> nodes = ig.cluster().nodes();

                return nodes.size() == 4 && !nodes.contains(failNode);
            }
        }, 60_000));
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** Block exchange messages. */
        private volatile boolean blockExchangeMessages;

        /** Block check messages. */
        private volatile boolean blockCheckMessages;

        /** Send outdated check messages. */
        private volatile boolean sndOutdatedCheckMessages;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg,
            IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {

            Message msg0 = ((GridIoMessage)msg).message();
            if ((msg0 instanceof GridDhtPartitionsSingleMessage || msg0 instanceof GridDhtPartitionsFullMessage)
                && blockExchangeMessages)
                return;

            if ((msg0 instanceof PartitionsExchangeFinishedCheckRequest ||
                msg0 instanceof PartitionsExchangeFinishedCheckResponse)
                && blockCheckMessages)
                return;

            if (msg0 instanceof PartitionsExchangeFinishedCheckResponse && sndOutdatedCheckMessages)
                GridTestUtils.setFieldValue(msg0, "topVer", AffinityTopologyVersion.ZERO);

            super.sendMessage(node, msg, ackC);
        }
    }
}
