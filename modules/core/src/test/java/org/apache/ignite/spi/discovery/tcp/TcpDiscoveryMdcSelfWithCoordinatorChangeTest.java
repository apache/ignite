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

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.segmentation.SegmentationPolicy;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;

/**
 * Test for {@link TcpDiscoverySpi} with Multi Data Centers where coordinator changed on second node join.
 */
public class TcpDiscoveryMdcSelfWithCoordinatorChangeTest extends TcpDiscoveryMdcSelfPlainTest {
    /**
     * @throws Exception If fails.
     */
    public TcpDiscoveryMdcSelfWithCoordinatorChangeTest() throws Exception {
    }

    /** */
    @Override protected void applyDC() {
        String prev = System.getProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID);

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, prev == null ? DC_ID_1 : DC_ID_0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Override public void testDiscoveryEventsDiscard() throws Exception {
        try {
            // disable metrics to avoid sending metrics messages as they may mess with pending messages counting.
            metricsEnabled = false;

            TestEventDiscardSpi spi = new TestEventDiscardSpi();

            Ignite ignite0 = startGrid(0);

            nodeSpi.set(spi);

            startGrid(1);

            ignite0.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)); // Send custom message.

            ignite0.destroyCache(DEFAULT_CACHE_NAME); // Send custom message.

            // We need to wait for discartion of pending messages from asynchronous processes like removing cache
            // metrics from DMS. Initially the test was correct as createCache/destroyCache methods are synchronous
            // and block test-runner thread for long enough for pending messages to be discarded.
            // But at some point aforementioned operations were added and implicit assumption the test relies on was broken.
            boolean pendingMsgsDiscarded = waitPendingMessagesDiscarded(spi);
            assertTrue(pendingMsgsDiscarded);

            stopGrid(0);

            log.info("Start new node.");

            spi.checkDuplicates = true;

            startGrid(0);

            spi.checkDuplicates = false;

            assertFalse(spi.failed);
        }
        finally {
            stopAllGrids();

            metricsEnabled = true;
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    @Test
    @Override public void testDuplicateId() throws Exception {
        try {
            startGrid(0);

            super.testDuplicateId();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test verifies Ignite nodes don't exchange system types on discovery phase but only user types.
     */
    @Test
    @Override public void testSystemMarshallerTypesFilteredOut() throws Exception {
        try {
            nodeSpi.set(new TestTcpDiscoveryMarshallerDataSpi());

            Ignite srv0 = startGrid(0);

            IgniteCache<Object, Object> organizations = srv0.createCache("organizations");

            organizations.put(1, new Organization());

            startGrid(1);

            assertEquals("Expected items in marshaller discovery data: 1, actual: "
                    + TestTcpDiscoveryMarshallerDataSpi.marshalledItems,
                1, TestTcpDiscoveryMarshallerDataSpi.marshalledItems);

            IgniteCache<Object, Object> employees = srv0.createCache("employees");

            employees.put(1, new Employee());

            startGrid(2);

            assertEquals("Expected items in marshaller discovery data: 2, actual: "
                    + TestTcpDiscoveryMarshallerDataSpi.marshalledItems,
                2, TestTcpDiscoveryMarshallerDataSpi.marshalledItems);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param segPlc Segmentation policy.
     * @throws Exception If failed.
     */
    @Override protected void checkFailedCoordinatorNode(SegmentationPolicy segPlc) throws Exception {
        try {
            this.segPlc = segPlc;

            for (int i = 0; i < 4; i++)
                startGrid(i);

            IgniteEx coord = grid(1);

            UUID coordId = coord.localNode().id();

            IgniteEx ignite = grid(2);

            AtomicBoolean coordSegmented = new AtomicBoolean();

            coord.events().localListen(evt -> {
                assertEquals(EVT_NODE_SEGMENTED, evt.type());

                UUID nodeId = ((DiscoveryEvent)evt).eventNode().id();

                if (coordId.equals(nodeId))
                    coordSegmented.set(true);

                return true;
            }, EVT_NODE_SEGMENTED);

            CountDownLatch failedLatch = new CountDownLatch(2);

            IgnitePredicate<Event> failLsnr = evt -> {
                assertEquals(EVT_NODE_FAILED, evt.type());

                UUID nodeId = ((DiscoveryEvent)evt).eventNode().id();

                if (coordId.equals(nodeId))
                    failedLatch.countDown();

                return true;
            };

            ignite.events().localListen(failLsnr, EVT_NODE_FAILED);

            grid(3).events().localListen(failLsnr, EVT_NODE_FAILED);

            ignite.configuration().getDiscoverySpi().failNode(coordId, null);

            assertTrue(failedLatch.await(2000, MILLISECONDS));

            assertTrue(coordSegmented.get());

            if (segPlc == SegmentationPolicy.STOP) {
                assertTrue(coord.context().isStopping());

                waitNodeStop(coord.name());
            }
            else
                assertFalse(coord.context().isStopping());

            assertEquals(3, ignite.context().discovery().allNodes().size());
        }
        finally {
            stopAllGrids();
        }
    }
}
