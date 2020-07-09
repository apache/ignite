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

package org.apache.ignite.spi.discovery;

import java.util.Arrays;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.DiscoveryServerOnlyCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 *
 */
public class FilterDataForClientNodeDiscoveryTest extends GridCommonAbstractTest {
    /** Join servers count. */
    private int joinSrvCnt;

    /** Join clients count. */
    private int joinCliCnt;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDataBag() throws Exception {
        startGrid(configuration(0));
        startGrid(configuration(1));

        assertEquals(3, joinSrvCnt);
        assertEquals(0, joinCliCnt);

        startClientGrid(configuration(2));
        startClientGrid(configuration(3));

        assertEquals(5, joinSrvCnt);
        assertEquals(4, joinCliCnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDiscoveryServerOnlyCustomMessage() throws Exception {
        startGrid(configuration(0));
        startGrid(configuration(1));
        startClientGrid(configuration(2));
        startClientGrid(configuration(3));

        final boolean[] recvMsg = new boolean[4];

        for (int i = 0; i < 4; ++i) {
            final int idx0 = i;

            grid(i).context().discovery().setCustomEventListener(
                MessageForServer.class, new CustomEventListener<MessageForServer>() {
                @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd,
                    MessageForServer msg) {

                    recvMsg[idx0] = true;
                }
            });
        }

        for (int i = 0; i < 4; ++i) {
            Arrays.fill(recvMsg, false);

            grid(i).context().discovery().sendCustomEvent(new MessageForServer());

            Thread.sleep(500);

            assertEquals(true, recvMsg[0]);
            assertEquals(true, recvMsg[1]);
            assertEquals(false, recvMsg[2]);
            assertEquals(false, recvMsg[3]);
        }
    }

    /**
     * @param nodeIdx Node index.
     * @return Ignite configuration.
     * @throws Exception On error.
     */
    private IgniteConfiguration configuration(int nodeIdx) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(nodeIdx));

        TcpDiscoverySpi testSpi = new TestDiscoverySpi();

        testSpi.setIpFinder(sharedStaticIpFinder);

        cfg.setDiscoverySpi(testSpi);

        return cfg;
    }

    /**
     *
     */
    private class TestDiscoverySpi extends TcpDiscoverySpi {
        /** Test exchange. */
        private TestDiscoveryDataExchange testEx = new TestDiscoveryDataExchange();

        /**
         *
         */
        public TestDiscoverySpi() {
            exchange = testEx;
        }


        /** {@inheritDoc} */
        @Override public void setDataExchange(DiscoverySpiDataExchange exchange) {
            testEx.setExchange(exchange);
        }
    }

    /**
     *
     */
    private class TestDiscoveryDataExchange implements DiscoverySpiDataExchange {
        /** Real exchange. */
        private DiscoverySpiDataExchange ex;

        /** {@inheritDoc} */
        @Override public DiscoveryDataBag collect(DiscoveryDataBag dataBag) {
            if (dataBag.isJoiningNodeClient())
                joinCliCnt++;
            else
                joinSrvCnt++;

            return ex.collect(dataBag);
        }

        /** {@inheritDoc} */
        @Override public void onExchange(DiscoveryDataBag dataBag) {
            ex.onExchange(dataBag);
        }

        /**
         * @param ex Exchange.
         */
        public void setExchange(DiscoverySpiDataExchange ex) {
            this.ex = ex;
        }
    }

    /**
     *
     */
    private static class MessageForServer implements DiscoveryServerOnlyCustomMessage {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final IgniteUuid id = IgniteUuid.randomUuid();

        /** {@inheritDoc} */
        @Override public IgniteUuid id() {
            return id;
        }

        /** {@inheritDoc} */
        @Nullable @Override public DiscoveryCustomMessage ackMessage() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isMutable() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
            DiscoCache discoCache) {
            return null;
        }
    }
}
