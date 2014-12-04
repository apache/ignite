/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.discovery;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 *
 */
public class GridDiscoveryManagerAliveCacheSelfTest extends GridCommonAbstractTest {
    /** */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static final int PERM_NODES_CNT = 5;

    /** */
    private static final int TMP_NODES_CNT = 3;

    /** */
    private static final int ITERATIONS = 20;

    /** */
    private int gridCntr;

    /** */
    private List<Ignite> alive = new ArrayList<>(PERM_NODES_CNT + TMP_NODES_CNT);

    /** */
    private volatile CountDownLatch latch;

    /** */
    private final GridPredicate<GridEvent> lsnr = new GridPredicate<GridEvent>() {
        @Override public boolean apply(GridEvent evt) {
            assertNotNull("Topology lost nodes before stopTempNodes() was called.", latch);

            latch.countDown();

            return true;
        }
    };

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration cCfg = defaultCacheConfiguration();

        cCfg.setCacheMode(PARTITIONED);
        cCfg.setBackups(1);
        cCfg.setDistributionMode(NEAR_PARTITIONED);
        cCfg.setPreloadMode(SYNC);
        cCfg.setQueryIndexEnabled(false);
        cCfg.setWriteSynchronizationMode(FULL_SYNC);

        GridTcpDiscoverySpi disc = new GridTcpDiscoverySpi();

        disc.setIpFinder(IP_FINDER);

        cfg.setCacheConfiguration(cCfg);
        cfg.setDiscoverySpi(disc);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        for (int i = 0; i < PERM_NODES_CNT; i++) {
            Ignite g = startGrid(gridCntr++);

            g.events().localListen(lsnr, GridEventType.EVT_NODE_LEFT);

            alive.add(g);
        }

        for (int i = 0; i < PERM_NODES_CNT + TMP_NODES_CNT; i++)
            F.rand(alive).cache(null).put(i, String.valueOf(i));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAlives() throws Exception {
        for (int i = 0; i < ITERATIONS; i++) {
            info("Performing iteration: " + i);

            // Clear latch reference, so any unexpected EVT_NODE_LEFT would fail the test.
            latch = null;

            startTempNodes();

            awaitDiscovery(PERM_NODES_CNT + TMP_NODES_CNT);

            // When temporary nodes stop every permanent node should receive TMP_NODES_CNT events.
            latch = new CountDownLatch(PERM_NODES_CNT * TMP_NODES_CNT);

            stopTempNodes();

            latch.await();

            validateAlives();
        }
    }

    /**
     * Waits while topology on all nodes became equals to the expected size.
     *
     * @param nodesCnt Expected nodes count.
     * @throws InterruptedException If interrupted.
     */
    @SuppressWarnings("BusyWait")
    private void awaitDiscovery(long nodesCnt) throws InterruptedException {
        for (Ignite g : alive) {
            while (g.cluster().nodes().size() != nodesCnt)
                Thread.sleep(10);
        }
    }

    /**
     * Validates that all node collections contain actual information.
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    private void validateAlives() {
        for (Ignite g : alive)
            assertEquals(PERM_NODES_CNT, g.cluster().nodes().size());

        for (final Ignite g : alive) {
            GridKernal k = (GridKernal)g;

            GridDiscoveryManager discoMgr = k.context().discovery();

            final Collection<GridNode> currTop = g.cluster().nodes();

            long currVer = discoMgr.topologyVersion();

            for (long v = currVer; v > currVer - GridDiscoveryManager.DISCOVERY_HISTORY_SIZE && v > 0; v--) {
                F.forAll(discoMgr.aliveCacheNodes(null, v),
                    new GridPredicate<GridNode>() {
                        @Override public boolean apply(GridNode e) {
                            return currTop.contains(e);
                        }
                    });

                F.forAll(discoMgr.aliveRemoteCacheNodes(null, v),
                    new GridPredicate<GridNode>() {
                        @Override public boolean apply(GridNode e) {
                            return currTop.contains(e) || g.cluster().localNode().equals(e);
                        }
                    });

                assertTrue(
                    currTop.contains(GridCacheUtils.oldest(k.internalCache().context(), currVer)));
            }
        }
    }

    /**
     * Starts temporary nodes.
     *
     * @throws Exception If failed.
     */
    private void startTempNodes() throws Exception {
        for (int j = 0; j < TMP_NODES_CNT; j++) {
            Ignite newNode = startGrid(gridCntr++);

            info("New node started: " + newNode.name());

            alive.add(newNode);

            newNode.events().localListen(lsnr, GridEventType.EVT_NODE_LEFT);
        }
    }

    /**
     * Stops temporary nodes.
     */
    private void stopTempNodes() {
        int rmv = 0;

        Collection<Ignite> toRmv = new ArrayList<>(TMP_NODES_CNT);

        for (Iterator<Ignite> iter = alive.iterator(); iter.hasNext() && rmv < TMP_NODES_CNT;) {
            toRmv.add(iter.next());

            iter.remove();

            rmv++;
        }

        // Remove listeners to avoid receiving events from stopping nodes.
        for (Ignite g : toRmv)
            g.events().stopLocalListen(lsnr, GridEventType.EVT_NODE_LEFT);

        for (Ignite g : toRmv)
            G.stop(g.name(), false);
    }
}
