/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 * Tests discovery event topology snapshots.
 */
public class GridDiscoveryEventSelfTest extends GridCommonAbstractTest {
    /** */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** Daemon flag. */
    private boolean daemon;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        daemon = false;
    }

    /** */
    private static final GridClosure<GridNode, UUID> NODE_2ID = new GridClosure<GridNode, UUID>() {
        @Override public UUID apply(GridNode n) {
            return n.id();
        }

        @Override public String toString() {
            return "Grid node shadow to node ID transformer closure.";
        }
    };

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration c = super.getConfiguration(gridName);

        c.setDaemon(daemon);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        c.setRestEnabled(false);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinSequenceEvents() throws Exception {
        try {
            Ignite g0 = startGrid(0);

            UUID id0 = g0.cluster().localNode().id();

            final ConcurrentMap<Integer, Collection<GridNode>> evts = new ConcurrentHashMap<>();

            g0.events().localListen(new GridPredicate<GridEvent>() {
                private AtomicInteger cnt = new AtomicInteger();

                @Override public boolean apply(GridEvent evt) {
                    assert evt.type() == EVT_NODE_JOINED;

                    evts.put(cnt.getAndIncrement(), ((GridDiscoveryEvent) evt).topologyNodes());

                    return true;
                }
            }, EVT_NODE_JOINED);

            UUID id1 = startGrid(1).cluster().localNode().id();
            UUID id2 = startGrid(2).cluster().localNode().id();
            UUID id3 = startGrid(3).cluster().localNode().id();

            U.sleep(100);

            assertEquals("Wrong count of events received", 3, evts.size());

            Collection<GridNode> top0 = evts.get(0);

            assertNotNull(top0);
            assertEquals(2, top0.size());
            assertTrue(F.viewReadOnly(top0, NODE_2ID).contains(id0));
            assertTrue(F.viewReadOnly(top0, NODE_2ID).contains(id1));

            Collection<GridNode> top1 = evts.get(1);

            assertNotNull(top1);
            assertEquals(3, top1.size());
            assertTrue(F.viewReadOnly(top1, NODE_2ID).contains(id0));
            assertTrue(F.viewReadOnly(top1, NODE_2ID).contains(id1));
            assertTrue(F.viewReadOnly(top1, NODE_2ID).contains(id2));

            Collection<GridNode> top2 = evts.get(2);

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

            final ConcurrentMap<Integer, Collection<GridNode>> evts = new ConcurrentHashMap<>();

            g0.events().localListen(new GridPredicate<GridEvent>() {
                private AtomicInteger cnt = new AtomicInteger();

                @Override public boolean apply(GridEvent evt) {
                    assert evt.type() == EVT_NODE_LEFT;

                    evts.put(cnt.getAndIncrement(), ((GridDiscoveryEvent) evt).topologyNodes());

                    return true;
                }
            }, EVT_NODE_LEFT);

            stopGrid(3);
            stopGrid(2);
            stopGrid(1);

            U.sleep(100);

            assertEquals("Wrong count of events received", 3, evts.size());

            Collection<GridNode> top2 = evts.get(0);

            assertNotNull(top2);
            assertEquals(3, top2.size());
            assertTrue(F.viewReadOnly(top2, NODE_2ID).contains(id0));
            assertTrue(F.viewReadOnly(top2, NODE_2ID).contains(id1));
            assertTrue(F.viewReadOnly(top2, NODE_2ID).contains(id2));
            assertFalse(F.viewReadOnly(top2, NODE_2ID).contains(id3));

            Collection<GridNode> top1 = evts.get(1);

            assertNotNull(top1);
            assertEquals(2, top1.size());
            assertTrue(F.viewReadOnly(top1, NODE_2ID).contains(id0));
            assertTrue(F.viewReadOnly(top1, NODE_2ID).contains(id1));
            assertFalse(F.viewReadOnly(top1, NODE_2ID).contains(id2));
            assertFalse(F.viewReadOnly(top1, NODE_2ID).contains(id3));

            Collection<GridNode> top0 = evts.get(2);

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

            final ConcurrentMap<Integer, Collection<GridNode>> evts = new ConcurrentHashMap<>();

            g0.events().localListen(new GridPredicate<GridEvent>() {
                private AtomicInteger cnt = new AtomicInteger();

                @Override public boolean apply(GridEvent evt) {
                    assert evt.type() == EVT_NODE_JOINED || evt.type() == EVT_NODE_LEFT;

                    evts.put(cnt.getAndIncrement(), ((GridDiscoveryEvent) evt).topologyNodes());

                    return true;
                }
            }, EVT_NODE_JOINED, EVT_NODE_LEFT);

            UUID id1 = startGrid(1).cluster().localNode().id();
            UUID id2 = startGrid(2).cluster().localNode().id();
            UUID id3 = startGrid(3).cluster().localNode().id();

            stopGrid(3);
            stopGrid(2);
            stopGrid(1);

            UUID id4 = startGrid(4).cluster().localNode().id();

            stopGrid(4);

            U.sleep(100);

            assertEquals("Wrong count of events received", 8, evts.size());

            Collection<GridNode> top0 = evts.get(0);

            assertNotNull(top0);
            assertEquals(2, top0.size());
            assertTrue(F.viewReadOnly(top0, NODE_2ID).contains(id0));
            assertTrue(F.viewReadOnly(top0, NODE_2ID).contains(id1));

            Collection<GridNode> top1 = evts.get(1);

            assertNotNull(top1);
            assertEquals(3, top1.size());
            assertTrue(F.viewReadOnly(top1, NODE_2ID).contains(id0));
            assertTrue(F.viewReadOnly(top1, NODE_2ID).contains(id1));
            assertTrue(F.viewReadOnly(top1, NODE_2ID).contains(id2));

            Collection<GridNode> top2 = evts.get(2);

            assertNotNull(top2);
            assertEquals(4, top2.size());
            assertTrue(F.viewReadOnly(top2, NODE_2ID).contains(id0));
            assertTrue(F.viewReadOnly(top2, NODE_2ID).contains(id1));
            assertTrue(F.viewReadOnly(top2, NODE_2ID).contains(id2));
            assertTrue(F.viewReadOnly(top2, NODE_2ID).contains(id3));

            Collection<GridNode> top3 = evts.get(3);

            assertNotNull(top3);
            assertEquals(3, top3.size());
            assertTrue(F.viewReadOnly(top3, NODE_2ID).contains(id0));
            assertTrue(F.viewReadOnly(top3, NODE_2ID).contains(id1));
            assertTrue(F.viewReadOnly(top3, NODE_2ID).contains(id2));
            assertFalse(F.viewReadOnly(top3, NODE_2ID).contains(id3));

            Collection<GridNode> top4 = evts.get(4);

            assertNotNull(top4);
            assertEquals(2, top4.size());
            assertTrue(F.viewReadOnly(top4, NODE_2ID).contains(id0));
            assertTrue(F.viewReadOnly(top4, NODE_2ID).contains(id1));
            assertFalse(F.viewReadOnly(top4, NODE_2ID).contains(id2));
            assertFalse(F.viewReadOnly(top4, NODE_2ID).contains(id3));

            Collection<GridNode> top5 = evts.get(5);

            assertNotNull(top5);
            assertEquals(1, top5.size());
            assertTrue(F.viewReadOnly(top5, NODE_2ID).contains(id0));
            assertFalse(F.viewReadOnly(top5, NODE_2ID).contains(id1));
            assertFalse(F.viewReadOnly(top5, NODE_2ID).contains(id2));
            assertFalse(F.viewReadOnly(top5, NODE_2ID).contains(id3));

            Collection<GridNode> top6 = evts.get(6);

            assertNotNull(top6);
            assertEquals(2, top6.size());
            assertTrue(F.viewReadOnly(top6, NODE_2ID).contains(id0));
            assertTrue(F.viewReadOnly(top6, NODE_2ID).contains(id4));
            assertFalse(F.viewReadOnly(top6, NODE_2ID).contains(id1));
            assertFalse(F.viewReadOnly(top6, NODE_2ID).contains(id2));
            assertFalse(F.viewReadOnly(top6, NODE_2ID).contains(id3));

            Collection<GridNode> top7 = evts.get(7);

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

            final ConcurrentMap<Integer, Collection<GridNode>> evts = new ConcurrentHashMap<>();

            g0.events().localListen(new GridPredicate<GridEvent>() {
                private AtomicInteger cnt = new AtomicInteger();

                @Override public boolean apply(GridEvent evt) {
                    assert evt.type() == EVT_NODE_JOINED;

                    X.println(">>>>>>> Joined " + F.viewReadOnly(((GridDiscoveryEvent) evt).topologyNodes(),
                        NODE_2ID));

                    evts.put(cnt.getAndIncrement(), ((GridDiscoveryEvent) evt).topologyNodes());

                    return true;
                }
            }, EVT_NODE_JOINED);

            U.sleep(100);

            startGridsMultiThreaded(1, 10);

            U.sleep(100);

            assertEquals(10, evts.size());

            for (int i = 0; i < 10; i++) {
                Collection<GridNode> snapshot = evts.get(i);

                assertEquals(2 + i, snapshot.size());
                assertTrue(F.viewReadOnly(snapshot, NODE_2ID).contains(id0));

                for (GridNode n : snapshot)
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

            final AtomicReference<GridException> err = new AtomicReference<>();

            for (int i = 0; i < 3; i++) {
                Ignite g = grid(i);

                g.events().localListen(new GridPredicate<GridEvent>() {
                    @Override public boolean apply(GridEvent evt) {
                        GridDiscoveryEvent discoEvt = (GridDiscoveryEvent) evt;

                        if (discoEvt.topologyNodes().size() != 3)
                            err.compareAndSet(null, new GridException("Invalid discovery event [evt=" + discoEvt +
                                ", nodes=" + discoEvt.topologyNodes() + ']'));

                        return true;
                    }
                }, GridEventType.EVT_NODE_JOINED);
            }

            daemon = true;

            GridKernal daemon = (GridKernal)startGrid(3);

            GridDiscoveryEvent join = daemon.context().discovery().localJoinEvent();

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
