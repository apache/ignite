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

package org.apache.ignite.internal.processors.service;

import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assume;
import org.junit.Before;

/**
 * Abstract class for tests of service deployment process.
 */
public abstract class ServiceDeploymentProcessAbstractTest extends GridCommonAbstractTest {
    /** Timeout to avoid tests hang. */
    protected static final long TEST_FUTURE_WAIT_TIMEOUT = 60_000;

    /** */
    @Before
    public void check() {
        Assume.assumeTrue(isEventDrivenServiceProcessorEnabled());
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new BlockingTcpDiscoverySpi();

        discoSpi.setIpFinder(((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder());

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCommunicationSpi(new BlockingTcpCommunicationSpi());

        return cfg;
    }

    /**
     * Stops given node depend on {@link #failNode()} flag.
     *
     * @param ignite Node to stop.
     * @see #failNode()
     */
    protected void stopNode(IgniteEx ignite) {
        if (!failNode())
            ignite.close();
        else
            ((TestTcpDiscoverySpi)ignite.context().discovery().getInjectedDiscoverySpi()).simulateNodeFailure();
    }

    /**
     * @return {@code true} if an intended stopping node shoud be failed, otherwise the node will be stopped normally.
     * @see #stopNode(IgniteEx)
     */
    protected boolean failNode() {
        return false;
    }

    /** */
    protected static class BlockingTcpCommunicationSpi extends TcpCommunicationSpi {
        /** Block flag. */
        private volatile boolean block;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
            if (block && (msg instanceof GridIoMessage &&
                ((GridIoMessage)msg).message() instanceof ServiceSingleNodeDeploymentResultBatch))
                return;

            super.sendMessage(node, msg, ackC);
        }

        /**
         * Set {@link #block} flag to {@code true}.
         */
        void block() {
            block = true;
        }
    }

    /** */
    protected static class BlockingTcpDiscoverySpi extends TestTcpDiscoverySpi {
        /** Block flag. */
        private volatile boolean block;

        /** {@inheritDoc} */
        @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
            if (block && GridTestUtils.getFieldValue(msg, "delegate") instanceof ServiceClusterDeploymentResultBatch)
                return;

            super.sendCustomEvent(msg);
        }

        /**
         * Set {@link #block} flag to {@code true}.
         */
        void block() {
            block = true;
        }
    }
}
