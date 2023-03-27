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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccCoordinator.DISCONNECTED_COORDINATOR;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests Mvcc coordinator change on client reconnect.
 */
public class CacheMvccClientReconnectTest extends GridCommonAbstractTest {
    /** */
    final CountDownLatch latch = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.contains("client")) {
            Map<IgnitePredicate<? extends Event>, int[]> lsnrs = new HashMap<>();

            lsnrs.put(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    try {
                        // Wait for the discovery notifier worker processed client disconnection.
                        latch.await();
                    }
                    catch (InterruptedException e) {
                        log.error("Unexpected exception.", e);

                        fail("Unexpected exception: " + e.getMessage());
                    }

                    return true;
                }
            }, new int[] {EVT_NODE_JOINED});

            cfg.setLocalEventListeners(lsnrs);
        }

        return cfg;
    }

    /**
     * Checks that events processed after client disconnect will not change coordinator until client reconnected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnect() throws Exception {
        startGrid(0);

        IgniteEx client = startClientGrid("client");

        MvccProcessor coordProc = client.context().coordinators();

        // Creates the join event.
        startGrid(1);

        stopGrid(0, true);
        stopGrid(1, true);

        client.context().discovery().reconnect();

        // Wait for the discovery notifier worker processed client disconnection.
        assertTrue("Failed to wait for client disconnected.",
            waitForCondition(() -> client.cluster().clientReconnectFuture() != null, 10_000));

        assertTrue("Failed to wait for setting disconnected coordinator.", waitForCondition(
            () -> DISCONNECTED_COORDINATOR.equals(coordProc.currentCoordinator()), 2000));

        // The discovery event thread may continue processing events when the notifier worker already processed
        // the client disconnection or a local join. It may lead to setting a wrong coordinator.
        latch.countDown();

        startGrid(0);

        client.cluster().clientReconnectFuture().get(10, SECONDS);

        assertEquals(grid(0).localNode().id(), coordProc.currentCoordinator().nodeId());
    }
}
