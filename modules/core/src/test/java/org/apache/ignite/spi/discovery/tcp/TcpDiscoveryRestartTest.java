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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.eclipse.jetty.util.ConcurrentHashSet;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 *
 */
public class TcpDiscoveryRestartTest extends GridCommonAbstractTest {
    /** */
    private TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static AtomicReference<String> err;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        int[] evts = {EVT_NODE_JOINED, EVT_NODE_FAILED, EVT_NODE_LEFT};

        cfg.setIncludeEventTypes(evts);

        Map<IgnitePredicate<? extends Event>, int[]> lsnrs = new HashMap<>();

        lsnrs.put(new TestEventListener(), evts);

        cfg.setLocalEventListeners(lsnrs);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestart() throws Exception {
        err = new AtomicReference<>();

        final int NODE_CNT = 3;

        startGrids(NODE_CNT);

        final ConcurrentHashSet<UUID> nodeIds = new ConcurrentHashSet<>();

        final AtomicInteger id = new AtomicInteger(NODE_CNT);

        final IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                int nodeIdx = id.getAndIncrement();

                for (int i = 0; i < 10 && err.get() == null; i++) {
                    Ignite ignite = startGrid(nodeIdx);

                    UUID nodeId = ignite.cluster().localNode().id();

                    if (!nodeIds.add(nodeId))
                        failed("Duplicated node ID: " + nodeId);

                    stopGrid(nodeIdx);
                }

                return null;
            }
        }, 5, "restart-thread");

        IgniteInternalFuture<?> loadFut = GridTestUtils.runMultiThreadedAsync(new Callable<Long>() {
            @Override public Long call() throws Exception {
                long dummyRes = 0;

                List<String> list = new ArrayList<>();

                while (!fut.isDone()) {
                    for (int i = 0; i < 100; i++) {
                        String str = new String(new byte[i]);

                        list.add(str);

                        dummyRes += str.hashCode();
                    }

                    if (list.size() > 1000_000) {
                        list = new ArrayList<>();

                        System.gc();
                    }
                }

                return dummyRes;
            }
        }, 2, "test-load");

        fut.get();

        loadFut.get();

        assertNull(err.get());

        for (int i = 0; i < NODE_CNT; i++) {
            Ignite ignite = ignite(i);

            TestEventListener lsnr = (TestEventListener)F.firstKey(ignite.configuration().getLocalEventListeners());

            assertNotNull(lsnr);

            for (UUID nodeId : nodeIds)
                lsnr.checkEvents(nodeId);
        }
    }


    /**
     * @param msg Message.
     */
    private void failed(String msg) {
        info(msg);

        err.compareAndSet(null, msg);
    }

    /**
     *
     */
    private class TestEventListener implements IgnitePredicate<Event> {
        /** */
        private final ConcurrentHashSet<UUID> joinIds = new ConcurrentHashSet<>();

        /** */
        private final ConcurrentHashSet<UUID> leftIds = new ConcurrentHashSet<>();

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            DiscoveryEvent evt0 = (DiscoveryEvent)evt;

            if (evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT) {
                if (!leftIds.add(evt0.eventNode().id()))
                    failed("Duplicated failed node ID: " + evt0.eventNode().id());
            }
            else {
                assertEquals(EVT_NODE_JOINED, evt.type());

                if (!joinIds.add(evt0.eventNode().id()))
                    failed("Duplicated joined node ID: " + evt0.eventNode().id());
            }

            return true;
        }

        /**
         * @param nodeId Node ID.
         */
        void checkEvents(UUID nodeId) {
            assertTrue("No join event: " + nodeId, joinIds.contains(nodeId));

            assertTrue("No left event: " + nodeId, leftIds.contains(nodeId));
        }
    }
}