/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.events.GridEventType.*;

/**
 * Client-based discovery tests.
 */
public class GridTcpClientDiscoverySelfTest extends GridCommonAbstractTest {
    /** */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static Collection<UUID> srvNodeIds;

    /** */
    private static Collection<UUID> clientNodeIds;

    /** */
    private static CountDownLatch joinLatch;

    /** */
    private static CountDownLatch leftLatch;

    /** */
    private static CountDownLatch failedLatch;

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLocalHost("127.0.0.1");

        if (gridName.startsWith("server")) {
            GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

            disco.setIpFinder(IP_FINDER);

            cfg.setDiscoverySpi(disco);
        }
        else if (gridName.startsWith("client")) {
            GridTcpClientDiscoverySpi disco = new GridTcpClientDiscoverySpi();

            disco.setIpFinder(IP_FINDER);

            cfg.setDiscoverySpi(disco);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        srvNodeIds = new GridConcurrentHashSet<>();
        clientNodeIds = new GridConcurrentHashSet<>();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientNodeJoin() throws Exception {
        joinLatch = new CountDownLatch(3);

        startServerNodes(3);
        startClientNodes(1);

        assertTrue(joinLatch.await(1000, MILLISECONDS));

        checkNodes(3, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientNodeLeave() throws Exception {
        leftLatch = new CountDownLatch(3);

        startServerNodes(3);
        startClientNodes(1);

        stopGrid("client-0");

        assertTrue(leftLatch.await(1000, MILLISECONDS));

        checkNodes(3, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientNodeFail() throws Exception {
        failedLatch = new CountDownLatch(3);

        startServerNodes(3);
        startClientNodes(1);

        forceClientFail();

        stopGrid("client-0");

        assertTrue(failedLatch.await(5000, MILLISECONDS));

        checkNodes(3, 0);
    }

    private void forceClientFail() throws Exception {
        Field f = GridTcpClientDiscoverySpi.class.getDeclaredField("fail");

        assert Modifier.isStatic(f.getModifiers());

        f.setAccessible(true);

        f.set(null, true);
    }

    /**
     * @param cnt Number of nodes.
     * @throws Exception In case of error.
     */
    private void startServerNodes(int cnt) throws Exception {
        for (int i = 0; i < cnt; i++) {
            Grid g = startGrid("server-" + i);

            srvNodeIds.add(g.localNode().id());
        }

        if (joinLatch != null) {
            for (int i = 0; i < cnt; i++) {
                G.grid("server-" + i).events().localListen(new GridPredicate<GridEvent>() {
                    @Override public boolean apply(GridEvent evt) {
                        info("Event fired: " + evt);

                        joinLatch.countDown();

                        return true;
                    }
                }, EVT_NODE_JOINED);
            }
        }

        if (leftLatch != null) {
            for (int i = 0; i < cnt; i++) {
                G.grid("server-" + i).events().localListen(new GridPredicate<GridEvent>() {
                    @Override public boolean apply(GridEvent evt) {
                        info("Event fired: " + evt);

                        leftLatch.countDown();

                        return true;
                    }
                }, EVT_NODE_LEFT);
            }
        }

        if (failedLatch != null) {
            for (int i = 0; i < cnt; i++) {
                G.grid("server-" + i).events().localListen(new GridPredicate<GridEvent>() {
                    @Override public boolean apply(GridEvent evt) {
                        info("Event fired: " + evt);

                        failedLatch.countDown();

                        return true;
                    }
                }, EVT_NODE_FAILED);
            }
        }
    }

    /**
     * @param cnt Number of nodes.
     * @throws Exception In case of error.
     */
    private void startClientNodes(int cnt) throws Exception {
        for (int i = 0; i < cnt; i++) {
            Grid g = startGrid("client-" + i);

            clientNodeIds.add(g.localNode().id());
        }
    }

    /**
     * @param srvCnt Number of server nodes.
     * @param clientCnt Number of client nodes.
     */
    private void checkNodes(int srvCnt, int clientCnt) {
        for (int i = 0; i < srvCnt; i++) {
            Grid g = G.grid("server-" + i);

            assertTrue(srvNodeIds.contains(g.localNode().id()));

            assertFalse(g.localNode().isClient());

            checkRemoteNodes(g, srvCnt + clientCnt - 1);
        }

        for (int i = 0; i < clientCnt; i++) {
            Grid g = G.grid("client-" + i);

            assertTrue(clientNodeIds.contains(g.localNode().id()));

            assertTrue(g.localNode().isClient());

            checkRemoteNodes(g, srvCnt + clientCnt - 1);
        }
    }

    /**
     * @param grid Grid.
     * @param expCnt Expected nodes count.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private void checkRemoteNodes(Grid grid, int expCnt) {
        Collection<GridNode> nodes = grid.forRemotes().nodes();

        assertEquals(expCnt, nodes.size());

        for (GridNode node : nodes) {
            UUID id = node.id();

            if (clientNodeIds.contains(id))
                assertTrue(node.isClient());
            else if (srvNodeIds.contains(id))
                assertFalse(node.isClient());
            else
                assert false : "Unexpected node ID: " + id;
        }
    }
}
