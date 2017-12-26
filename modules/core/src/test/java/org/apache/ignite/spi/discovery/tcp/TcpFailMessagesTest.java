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

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessage;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;
import static org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi.DFLT_EVT_HISTORY_SIZE;

/**
 * Tests {@link TcpDiscoveryNodeFailedMessage} warning message and last events logging when node fails.
 */
public class TcpFailMessagesTest extends GridCommonAbstractTest {
    /** */
    private GridStringLogger log0 = new GridStringLogger(false, log);

    /** */
    private GridStringLogger log1 = new GridStringLogger(false, log);

    /** */
    private GridStringLogger log2 = new GridStringLogger(false, log);

    /** */
    private final String FAIL_MSG = "Forced fail.";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (getTestIgniteInstanceName(0).equals(igniteInstanceName))
            cfg.setGridLogger(log0);

        if (getTestIgniteInstanceName(1).equals(igniteInstanceName))
            cfg.setGridLogger(log1);

        if (getTestIgniteInstanceName(2).equals(igniteInstanceName))
            cfg.setGridLogger(log2);

        return cfg;
    }

    /**
     * i0 - another node, where additional info (warning message) should be logged.
     * i1 - failure detector.
     * i2 - failing node.
     *
     * @throws Exception If failed.
     */
    public void testFailMsgOnOtherNode() throws Exception{
        Ignite i0 = startGrid(0);
        Ignite i1 = startGrid(1);
        Ignite i2 = startGrid(2);

        CountDownLatch latch = new CountDownLatch(3);

        EventListener prd = new EventListener(latch);

        i0.events().localListen(prd, EVT_NODE_FAILED);
        i1.events().localListen(prd, EVT_NODE_FAILED);
        i2.events().localListen(prd, EVT_NODE_SEGMENTED);

        awaitPartitionMapExchange();

        i1.configuration().getDiscoverySpi().failNode(i2.cluster().localNode().id(), FAIL_MSG);

        awaitPartitionMapExchange();

        assertTrue(latch.await(10, SECONDS));

        assertTrue(log0.toString().contains(FAIL_MSG));
        assertTrue(log0.toString().contains("Last " + DFLT_EVT_HISTORY_SIZE + " discovery events:"));
        assertTrue(log1.toString().contains("Last " + DFLT_EVT_HISTORY_SIZE + " discovery events:"));
        assertTrue(log2.toString().contains("Last " + DFLT_EVT_HISTORY_SIZE + " discovery events:"));
    }

    /**
     * Counts down latch to be sure that all necessary events were occurred.
     */
    private class EventListener implements IgnitePredicate<Event> {
        /** */
        private final CountDownLatch latch;

        /**
         * @param latch CountDownLatch.
         */
        private EventListener(CountDownLatch latch) {
            this.latch = latch;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            latch.countDown();

            return true;
        }
    }
}
