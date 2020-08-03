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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVTS_DISCOVERY;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Checks that client will not process previous cluster events after reconnect.
 */
public class IgniteClientReconnectEventHandlingTest extends GridCommonAbstractTest {
    /** */
    private final CountDownLatch latch = new CountDownLatch(1);

    /** */
    private final CountDownLatch reconnect = new CountDownLatch(1);

    /** */
    private final ConcurrentLinkedQueue<Event> evtQueue = new ConcurrentLinkedQueue<>();

    /** */
    private static final int RECONNECT_DELAY = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DiscoverySpi discoSpi = cfg.getDiscoverySpi();

        // For optimization test duration.
        if (discoSpi instanceof TcpDiscoverySpi)
            ((TcpDiscoverySpi)discoSpi).setReconnectDelay(RECONNECT_DELAY);

        if (igniteInstanceName.contains("client")) {
            Map<IgnitePredicate<? extends Event>, int[]> lsnrs = new HashMap<>();

            lsnrs.put(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    try {
                        // Wait for the discovery notifier worker processed client disconnection.
                        latch.await(cfg.getFailureDetectionTimeout(), MILLISECONDS);
                    }
                    catch (InterruptedException e) {
                        log.error("Unexpected exception.", e);

                        fail("Unexpected exception: " + e.getMessage());
                    }

                    return true;
                }
            }, new int[] {EVT_NODE_JOINED});

            lsnrs.put(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    reconnect.countDown();

                    return true;
                }
            }, new int[] {EVT_CLIENT_NODE_RECONNECTED});

            lsnrs.put(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    evtQueue.add(evt);

                    return true;
                }
            }, EVTS_DISCOVERY);

            cfg.setLocalEventListeners(lsnrs);
        }

        return cfg;
    }

    /** @throws Exception If failed. */
    @Test
    public void testClientReconnect() throws Exception {
        startGrid(0);

        IgniteEx client = startClientGrid("client");

        // Creates the join event and hold up it on the client.
        startGrid(1);

        stopGrid(0);

        stopGrid(1);

        // Wait for the discovery notifier worker processed client disconnection.
        assertTrue("Failed to wait for client disconnected.",
            waitForCondition(() -> client.cluster().clientReconnectFuture() != null, 10_000));

        assertTrue(client.context().clientDisconnected());

        IgniteFuture<?> fut = client.cluster().clientReconnectFuture();

        fut.listen(f -> evtQueue.clear());

        // Starts a new cluster.
        startGrid(0);

        // The client shouldn't connect to the new cluster until processed previous cluster events.
        U.sleep(RECONNECT_DELAY * 2);

        assertTrue(client.context().clientDisconnected());

        // Continue processing events from the previous cluster.
        latch.countDown();

        fut.get();

        assertTrue(!client.context().clientDisconnected());

        assertTrue("Failed to wait for client reconnect event.", reconnect.await(10, SECONDS));

        awaitPartitionMapExchange();

        assertEquals("Only reconnect event should be processed after the client reconnects to cluster.",
            1, evtQueue.size());

        assertEquals(EVT_CLIENT_NODE_RECONNECTED, evtQueue.poll().type());
    }
}
