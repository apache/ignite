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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 * Tests discovery event topology snapshots.
 */
public class GridDiscoveryEventSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Daemon flag. */
    private boolean daemon;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        daemon = false;
    }

    /** */
    private static final IgniteClosure<ClusterNode, UUID> NODE_2ID = new IgniteClosure<ClusterNode, UUID>() {
        @Override public UUID apply(ClusterNode n) {
            return n.id();
        }

        @Override public String toString() {
            return "Grid node shadow to node ID transformer closure.";
        }
    };

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.setDaemon(daemon);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        c.setConnectorConfiguration(null);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinSequenceEvents() throws Exception {
        try {
            Ignite g0 = startGrid(0);

            UUID id0 = g0.cluster().localNode().id();

            final ConcurrentMap<Integer, Collection<ClusterNode>> evts = new ConcurrentHashMap<>();

            final CountDownLatch latch = new CountDownLatch(3);

            g0.events().localListen(new IgnitePredicate<Event>() {
                private AtomicInteger cnt = new AtomicInteger();

                @Override public boolean apply(Event evt) {
                    assert evt.type() == EVT_NODE_JOINED : evt;

                    evts.put(cnt.getAndIncrement(), ((DiscoveryEvent)evt).topologyNodes());

                    latch.countDown();

                    return true;
                }
            }, EVT_NODE_JOINED);

            UUID id1 = startGrid(1).cluster().localNode().id();
            UUID id2 = startGrid(2).cluster().localNode().id();
            UUID id3 = startGrid(3).cluster().localNode().id();

            assertTrue("Wrong count of events received: " + evts, latch.await(3000, MILLISECONDS));

            Collection<ClusterNode> top0 = evts.get(0);

            assertNotNull(top0);
            assertEquals(2, top0.size());
            assertTrue(F.viewReadOnly(top0, NODE_2ID).contains(id0));
            assertTrue(F.viewReadOnly(top0, NODE_2ID).contains(id1));

            Collection<ClusterNode> top1 = evts.get(1);

            assertNotNull(top1);
            assertEquals(3, top1.size());
            assertTrue(F.viewReadOnly(top1, NODE_2ID).contains(id0));
            assertTrue(F.viewReadOnly(top1, NODE_2ID).contains(id1));
            assertTrue(F.viewReadOnly(top1, NODE_2ID).contains(id2));

            Collection<ClusterNode> top2 = evts.get(2);

            assertNotNull(top2);
            assertEquals(4, top2.size());
            assertTrue(F.viewReadOnly(top2, NODE_2ID).contains(id0));
            assertTrue(F.viewReadOnly(top2, NODE_2ID).contains(id1));
            assertTrue(F.viewReadOnly(top2, NODE_2ID).contains(id2));
            assertTrue(F.viewReadOnly(top2, NODE_2ID).contains(id3));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLeaveSequenceEvents() throws Exception {
        try {
            Ignite g0 = startGrid(0);

            UUID id0 = g0.cluster().localNode().id();
            UUID id1 = startGrid(1).cluster().localNode().id();
            UUID id2 = startGrid(2).cluster().localNode().id();
            UUID id3 = startGrid(3).cluster().localNode().id();

            final ConcurrentMap<Integer, Collection<ClusterNode>> evts = new ConcurrentHashMap<>();

            final CountDownLatch latch = new CountDownLatch(3);

            g0.events().localListen(new IgnitePredicate<Event>() {
                private AtomicInteger cnt = new AtomicInteger();

                @Override public boolean apply(Event evt) {
                    assert evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED : evt;

                    evts.put(cnt.getAndIncrement(), ((DiscoveryEvent) evt).topologyNodes());

                    latch.countDown();

                    return true;
                }
            }, EVT_NODE_LEFT, EVT_NODE_FAILED);

            stopGrid(3);
            stopGrid(2);
            stopGrid(1);

            assertTrue("Wrong count of events received: " + evts, latch.await(3000, MILLISECONDS));

            Collection<ClusterNode> top2 = evts.get(0);

            assertNotNull(top2);
            assertEquals(3, top2.size());
            assertTrue(F.viewReadOnly(top2, NODE_2ID).contains(id0));
            assertTrue(F.viewReadOnly(top2, NODE_2ID).contains(id1));
            assertTrue(F.viewReadOnly(top2, NODE_2ID).contains(id2));
            assertFalse(F.viewReadOnly(top2, NODE_2ID).contains(id3));

            Collection<ClusterNode> top1 = evts.get(1);

            assertNotNull(top1);
            assertEquals(2, top1.size());
            assertTrue(F.viewReadOnly(top1, NODE_2ID).contains(id0));
            assertTrue(F.viewReadOnly(top1, NODE_2ID).contains(id1));
            assertFalse(F.viewReadOnly(top1, NODE_2ID).contains(id2));
            assertFalse(F.viewReadOnly(top1, NODE_2ID).contains(id3));

            Collection<ClusterNode> top0 = evts.get(2);

            assertNotNull(top0);
            assertEquals(1, top0.size());
            assertTrue(F.viewReadOnly(top0, NODE_2ID).contains(id0));
            assertFalse(F.viewReadOnly(top0, NODE_2ID).contains(id1));
            assertFalse(F.viewReadOnly(top0, NODE_2ID).contains(id2));
            assertFalse(F.viewReadOnly(top0, NODE_2ID).contains(id3));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMixedSequenceEvents() throws Exception {
        try {
            Ignite g0 = startGrid(0);

            UUID id0 = g0.cluster().localNode().id();

            final ConcurrentMap<Integer, Collection<ClusterNode>> evts = new ConcurrentHashMap<>();

            final CountDownLatch latch = new CountDownLatch(8);

            g0.events().localListen(new IgnitePredicate<Event>() {
                private AtomicInteger cnt = new AtomicInteger();

                @Override public boolean apply(Event evt) {
                    assert evt.type() == EVT_NODE_JOINED
                        || evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED : evt;

                    evts.put(cnt.getAndIncrement(), ((DiscoveryEvent) evt).topologyNodes());

                    latch.countDown();

                    return true;
                }
            }, EVT_NODE_JOINED, EVT_NODE_LEFT, EVT_NODE_FAILED);

            UUID id1 = startGrid(1).cluster().localNode().id();
            UUID id2 = startGrid(2).cluster().localNode().id();
            UUID id3 = startGrid(3).cluster().localNode().id();

            stopGrid(3);
            stopGrid(2);
            stopGrid(1);

            UUID id4 = startGrid(4).cluster().localNode().id();

            stopGrid(4);

            assertTrue("Wrong count of events received [cnt= " + evts.size() + ", evts=" + evts + ']',
                latch.await(3000, MILLISECONDS));

            Collection<ClusterNode> top0 = evts.get(0);

            assertNotNull(top0);
            assertEquals(2, top0.size());
            assertTrue(F.viewReadOnly(top0, NODE_2ID).contains(id0));
            assertTrue(F.viewReadOnly(top0, NODE_2ID).contains(id1));

            Collection<ClusterNode> top1 = evts.get(1);

            assertNotNull(top1);
            assertEquals(3, top1.size());
            assertTrue(F.viewReadOnly(top1, NODE_2ID).contains(id0));
            assertTrue(F.viewReadOnly(top1, NODE_2ID).contains(id1));
            assertTrue(F.viewReadOnly(top1, NODE_2ID).contains(id2));

            Collection<ClusterNode> top2 = evts.get(2);

            assertNotNull(top2);
            assertEquals(4, top2.size());
            assertTrue(F.viewReadOnly(top2, NODE_2ID).contains(id0));
            assertTrue(F.viewReadOnly(top2, NODE_2ID).contains(id1));
            assertTrue(F.viewReadOnly(top2, NODE_2ID).contains(id2));
            assertTrue(F.viewReadOnly(top2, NODE_2ID).contains(id3));

            Collection<ClusterNode> top3 = evts.get(3);

            assertNotNull(top3);
            assertEquals(3, top3.size());
            assertTrue(F.viewReadOnly(top3, NODE_2ID).contains(id0));
            assertTrue(F.viewReadOnly(top3, NODE_2ID).contains(id1));
            assertTrue(F.viewReadOnly(top3, NODE_2ID).contains(id2));
            assertFalse(F.viewReadOnly(top3, NODE_2ID).contains(id3));

            Collection<ClusterNode> top4 = evts.get(4);

            assertNotNull(top4);
            assertEquals(2, top4.size());
            assertTrue(F.viewReadOnly(top4, NODE_2ID).contains(id0));
            assertTrue(F.viewReadOnly(top4, NODE_2ID).contains(id1));
            assertFalse(F.viewReadOnly(top4, NODE_2ID).contains(id2));
            assertFalse(F.viewReadOnly(top4, NODE_2ID).contains(id3));

            Collection<ClusterNode> top5 = evts.get(5);

            assertNotNull(top5);
            assertEquals(1, top5.size());
            assertTrue(F.viewReadOnly(top5, NODE_2ID).contains(id0));
            assertFalse(F.viewReadOnly(top5, NODE_2ID).contains(id1));
            assertFalse(F.viewReadOnly(top5, NODE_2ID).contains(id2));
            assertFalse(F.viewReadOnly(top5, NODE_2ID).contains(id3));

            Collection<ClusterNode> top6 = evts.get(6);

            assertNotNull(top6);
            assertEquals(2, top6.size());
            assertTrue(F.viewReadOnly(top6, NODE_2ID).contains(id0));
            assertTrue(F.viewReadOnly(top6, NODE_2ID).contains(id4));
            assertFalse(F.viewReadOnly(top6, NODE_2ID).contains(id1));
            assertFalse(F.viewReadOnly(top6, NODE_2ID).contains(id2));
            assertFalse(F.viewReadOnly(top6, NODE_2ID).contains(id3));

            Collection<ClusterNode> top7 = evts.get(7);

            assertNotNull(top7);
            assertEquals(1, top7.size());
            assertTrue(F.viewReadOnly(top7, NODE_2ID).contains(id0));
            assertFalse(F.viewReadOnly(top7, NODE_2ID).contains(id1));
            assertFalse(F.viewReadOnly(top7, NODE_2ID).contains(id2));
            assertFalse(F.viewReadOnly(top7, NODE_2ID).contains(id3));
            assertFalse(F.viewReadOnly(top7, NODE_2ID).contains(id4));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentJoinEvents() throws Exception {
        try {
            Ignite g0 = startGrid(0);

            UUID id0 = g0.cluster().localNode().id();

            final ConcurrentMap<Integer, Collection<ClusterNode>> evts = new ConcurrentHashMap<>();

            g0.events().localListen(new IgnitePredicate<Event>() {
                private AtomicInteger cnt = new AtomicInteger();

                @Override public boolean apply(Event evt) {
                    assert evt.type() == EVT_NODE_JOINED : evt;

                    X.println(">>>>>>> Joined " + F.viewReadOnly(((DiscoveryEvent) evt).topologyNodes(),
                        NODE_2ID));

                    evts.put(cnt.getAndIncrement(), ((DiscoveryEvent) evt).topologyNodes());

                    return true;
                }
            }, EVT_NODE_JOINED);

            U.sleep(100);

            startGridsMultiThreaded(1, 10);

            U.sleep(100);

            assertEquals(10, evts.size());

            for (int i = 0; i < 10; i++) {
                Collection<ClusterNode> snapshot = evts.get(i);

                assertEquals(2 + i, snapshot.size());
                assertTrue(F.viewReadOnly(snapshot, NODE_2ID).contains(id0));

                for (ClusterNode n : snapshot)
                    assertTrue("Wrong node order in snapshot [i=" + i + ", node=" + n + ']', n.order() <= 2 + i);
            }

            Collection<UUID> ids = F.viewReadOnly(evts.get(9), NODE_2ID);

            for (int i = 1; i <= 10; i++)
                assertTrue(ids.contains(grid(i).localNode().id()));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDaemonNodeJoin() throws Exception {
        try {
            startGridsMultiThreaded(3);

            final AtomicReference<IgniteCheckedException> err = new AtomicReference<>();

            for (int i = 0; i < 3; i++) {
                Ignite g = grid(i);

                g.events().localListen(new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event evt) {
                        DiscoveryEvent discoEvt = (DiscoveryEvent) evt;

                        if (discoEvt.topologyNodes().size() != 3)
                            err.compareAndSet(null, new IgniteCheckedException("Invalid discovery event [evt=" + discoEvt +
                                ", nodes=" + discoEvt.topologyNodes() + ']'));

                        return true;
                    }
                }, EventType.EVT_NODE_JOINED);
            }

            daemon = true;

            IgniteKernal daemon = (IgniteKernal)startGrid(3);

            DiscoveryEvent join = daemon.context().discovery().localJoinEvent();

            assertEquals(3, join.topologyNodes().size());

            U.sleep(100);

            if (err.get() != null)
                throw err.get();
        }
        finally {
            stopAllGrids();
        }
    }
}