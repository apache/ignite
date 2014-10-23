/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.GridDeploymentMode.*;
import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.grid.cache.GridCacheConfiguration.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;
import static org.gridgain.grid.kernal.processors.cache.distributed.dht.GridDhtPartitionState.*;

/**
 * Test cases for partitioned cache {@link GridDhtPreloader preloader}.
 */
public class GridCacheDhtPreloadSelfTest extends GridCommonAbstractTest {
    /** Flag to print preloading events. */
    private static final boolean DEBUG = false;

    /** */
    private static final long TEST_TIMEOUT = 5 * 60 * 1000;

    /** Default backups. */
    private static final int DFLT_BACKUPS = 1;

    /** Partitions. */
    private static final int DFLT_PARTITIONS = 521;

    /** Preload batch size. */
    private static final int DFLT_BATCH_SIZE = DFLT_PRELOAD_BATCH_SIZE;

    /** Number of key backups. Each test method can set this value as required. */
    private int backups = DFLT_BACKUPS;

    /** Preload mode. */
    private GridCachePreloadMode preloadMode = ASYNC;

    /** */
    private int preloadBatchSize = DFLT_BATCH_SIZE;

    /** Number of partitions. */
    private int partitions = DFLT_PARTITIONS;

    /** IP finder. */
    private GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /**
     *
     */
    public GridCacheDhtPreloadSelfTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);
        cfg.setCacheConfiguration(cacheConfiguration(gridName));
        cfg.setDeploymentMode(CONTINUOUS);

        return cfg;
    }

    /**
     * Gets cache configuration for grid with given name.
     *
     * @param gridName Grid name.
     * @return Cache configuration.
     */
    protected GridCacheConfiguration cacheConfiguration(String gridName) {
        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setPreloadBatchSize(preloadBatchSize);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setPreloadMode(preloadMode);
        cacheCfg.setAffinity(new GridCacheConsistentHashAffinityFunction(false, partitions));
        cacheCfg.setBackups(backups);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
//        resetLog4j(Level.DEBUG, true,
//            // Categories.
//            GridDhtPreloader.class.getPackage().getName(),
//            GridDhtPartitionTopologyImpl.class.getName(),
//            GridDhtLocalPartition.class.getName());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        backups = DFLT_BACKUPS;
        partitions = DFLT_PARTITIONS;
        preloadMode = ASYNC;
        preloadBatchSize = DFLT_BATCH_SIZE;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /**
     * @param cache Cache.
     * @return Affinity.
     */
    @SuppressWarnings({"unchecked"})
    private GridCacheAffinity<Integer> affinity(GridCache<Integer, ?> cache) {
        return cache.affinity();
    }

    /**
     * @param c Cache.
     * @return {@code True} if synchronous preloading.
     */
    private boolean isSync(GridCache<?, ?> c) {
        return c.configuration().getPreloadMode() == SYNC;
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivePartitionTransferSyncSameCoordinator() throws Exception {
        preloadMode = SYNC;

        checkActivePartitionTransfer(1000, 4, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivePartitionTransferAsyncSameCoordinator() throws Exception {
        checkActivePartitionTransfer(1000, 4, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivePartitionTransferSyncChangingCoordinator() throws Exception {
        preloadMode = SYNC;

        checkActivePartitionTransfer(1000, 4, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivePartitionTransferAsyncChangingCoordinator() throws Exception {
        checkActivePartitionTransfer(1000, 4, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivePartitionTransferSyncRandomCoordinator() throws Exception {
        preloadMode = SYNC;

        checkActivePartitionTransfer(1000, 4, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivePartitionTransferAsyncRandomCoordinator() throws Exception {
        checkActivePartitionTransfer(1000, 4, false, true);
    }

    /**
     * @param keyCnt Key count.
     * @param nodeCnt Node count.
     * @param sameCoord Same coordinator flag.
     * @param shuffle Shuffle flag.
     * @throws Exception If failed.
     */
    private void checkActivePartitionTransfer(int keyCnt, int nodeCnt, boolean sameCoord, boolean shuffle)
        throws Exception {
//        resetLog4j(Level.DEBUG, true,
//            // Categories.
//            GridDhtPreloader.class.getPackage().getName(),
//            GridDhtPartitionTopologyImpl.class.getName(),
//            GridDhtLocalPartition.class.getName());

        try {
            Grid grid1 = startGrid(0);

            GridCache<Integer, String> cache1 = grid1.cache(null);

            putKeys(cache1, keyCnt);
            checkKeys(cache1, keyCnt, F.asList(grid1));

            List<Grid> grids = new ArrayList<>(nodeCnt + 1);

            startGrids(nodeCnt, 1, grids);

            // Check all nodes.
            for (Grid g : grids) {
                GridCache<Integer, String> c = g.cache(null);

                checkKeys(c, keyCnt, grids);
            }

            if (shuffle)
                Collections.shuffle(grids);

            if (sameCoord)
                // Add last.
                grids.add(grid1);
            else
                // Add first.
                grids.add(0, grid1);

            if (!sameCoord && shuffle)
                Collections.shuffle(grids);

            checkActiveState(grids);

            info(">>> Finished checking nodes [keyCnt=" + keyCnt + ", nodeCnt=" + nodeCnt + ", grids=" +
                U.grids2names(grids) + ']');

            Collection<GridFuture<?>> futs = new LinkedList<>();

            Grid last = F.last(grids);

            for (Iterator<Grid> it = grids.iterator(); it.hasNext(); ) {
                Grid g = it.next();

                if (!it.hasNext()) {
                    assert last == g;

                    break;
                }

                checkActiveState(grids);

                final UUID nodeId = g.localNode().id();

                it.remove();

                futs.add(waitForLocalEvent(last.events(), new P1<GridEvent>() {
                    @Override public boolean apply(GridEvent e) {
                        GridCachePreloadingEvent evt = (GridCachePreloadingEvent)e;

                        GridNode node = evt.discoveryNode();

                        return evt.type() == EVT_CACHE_PRELOAD_STOPPED && node.id().equals(nodeId) &&
                            evt.discoveryEventType() == EVT_NODE_LEFT;
                    }
                }, EVT_CACHE_PRELOAD_STOPPED));

                info("Before grid stop [name=" + g.name() + ", fullTop=" + top2string(grids));

                stopGrid(g.name());

                info("After grid stop [name=" + g.name() + ", fullTop=" + top2string(grids));

                // Check all left nodes.
                checkActiveState(grids);
            }

            info("Waiting for preload futures: " + F.view(futs, F.unfinishedFutures()));

            X.waitAll(futs);

            info("Finished waiting for preload futures.");

            assert last != null;

            GridCache<Integer, String> lastCache = last.cache(null);

            GridDhtCacheAdapter<Integer, String> dht = dht(lastCache);

            GridCacheAffinity<Integer> aff = affinity(lastCache);

            info("Finished waiting for all exchange futures...");

            for (int i = 0; i < keyCnt; i++) {
                if (aff.mapPartitionToPrimaryAndBackups(aff.partition(i)).contains(last.localNode())) {
                    GridDhtPartitionTopology<Integer, String> top = dht.topology();

                    for (GridDhtLocalPartition<Integer, String> p : top.localPartitions()) {
                        Collection<GridNode> moving = top.moving(p.id());

                        assert moving.isEmpty() : "Nodes with partition in moving state [part=" + p +
                            ", moving=" + moving + ']';

                        assert OWNING == p.state() : "Invalid partition state for partition [part=" + p + ", map=" +
                            top.partitionMap(false) + ']';
                    }
                }
            }

            checkActiveState(grids);
        }
        catch (Error | Exception e) {
            error("Test failed.", e);

            throw e;
        } finally {
            stopAllGrids();
        }
    }

    /**
     * @param grids Grids.
     */
    private void checkActiveState(Iterable<Grid> grids) {
        // Check that nodes don't have non-active information about other nodes.
        for (Grid g : grids) {
            GridCache<Integer, String> c = g.cache(null);

            GridDhtCacheAdapter<Integer, String> dht = dht(c);

            GridDhtPartitionFullMap allParts = dht.topology().partitionMap(false);

            for (GridDhtPartitionMap parts : allParts.values()) {
                if (!parts.nodeId().equals(g.localNode().id())) {
                    for (Map.Entry<Integer, GridDhtPartitionState> e : parts.entrySet()) {
                        int p = e.getKey();

                        GridDhtPartitionState state = e.getValue();

                        assert state == OWNING || state == MOVING || state == RENTING :
                            "Invalid state [grid=" + g.name() + ", part=" + p + ", state=" + state +
                                ", parts=" + parts + ']';

                        assert state.active();
                    }
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultiplePartitionBatchesSyncPreload() throws Exception {
        preloadMode = SYNC;
        preloadBatchSize = 100;
        partitions = 2;

        checkNodes(1000, 1, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultiplePartitionBatchesAsyncPreload() throws Exception {
        preloadBatchSize = 100;
        partitions = 2;

        checkNodes(1000, 1, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleNodesSyncPreloadSameCoordinator() throws Exception {
        preloadMode = SYNC;

        checkNodes(1000, 4, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleNodesAsyncPreloadSameCoordinator() throws Exception {
        checkNodes(1000, 4, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleNodesSyncPreloadChangingCoordinator() throws Exception {
        preloadMode = SYNC;

        checkNodes(1000, 4, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleNodesAsyncPreloadChangingCoordinator() throws Exception {
        checkNodes(1000, 4, false, false);
    }


    /**
     * @throws Exception If failed.
     */
    public void testMultipleNodesSyncPreloadRandomCoordinator() throws Exception {
        preloadMode = SYNC;

        checkNodes(1000, 4, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleNodesAsyncPreloadRandomCoordinator() throws Exception {
        checkNodes(1000, 4, false, true);
    }

    /**
     * @param cnt Number of grids.
     * @param startIdx Start node index.
     * @param list List of started grids.
     * @throws Exception If failed.
     */
    private void startGrids(int cnt, int startIdx, Collection<Grid> list) throws Exception {
        for (int i = 0; i < cnt; i++) {
            final Grid g = startGrid(startIdx++);

            if (DEBUG)
                g.events().localListen(new GridPredicate<GridEvent>() {
                    @Override public boolean apply(GridEvent evt) {
                        info("\n>>> Preload event [grid=" + g.name() + ", evt=" + evt + ']');

                        return true;
                    }
                }, EVTS_CACHE_PRELOAD);

            list.add(g);
        }
    }

    /**
     * @param grids Grids to stop.
     */
    private void stopGrids(Iterable<Grid> grids) {
        for (Grid g : grids)
            stopGrid(g.name());
    }

    /**
     * @param keyCnt Key count.
     * @param nodeCnt Node count.
     * @param sameCoord Same coordinator flag.
     * @param shuffle Shuffle flag.
     * @throws Exception If failed.
     */
    private void checkNodes(int keyCnt, int nodeCnt, boolean sameCoord, boolean shuffle)
        throws Exception {
//        resetLog4j(Level.DEBUG, true,
//            // Categories.
//            GridDhtPreloader.class.getPackage().getName(),
//            GridDhtPartitionTopologyImpl.class.getName(),
//            GridDhtLocalPartition.class.getName());

        try {
            Grid grid1 = startGrid(0);

            GridCache<Integer, String> cache1 = grid1.cache(null);

            putKeys(cache1, keyCnt);
            checkKeys(cache1, keyCnt, F.asList(grid1));

            List<Grid> grids = new ArrayList<>(nodeCnt + 1);

            startGrids(nodeCnt, 1, grids);

            // Check all nodes.
            for (Grid g : grids) {
                GridCache<Integer, String> c = g.cache(null);

                checkKeys(c, keyCnt, grids);
            }

            if (shuffle)
                Collections.shuffle(grids);

            if (sameCoord)
                // Add last.
                grids.add(grid1);
            else
                // Add first.
                grids.add(0, grid1);

            if (!sameCoord && shuffle)
                Collections.shuffle(grids);

            info(">>> Finished checking nodes [keyCnt=" + keyCnt + ", nodeCnt=" + nodeCnt + ", grids=" +
                U.grids2names(grids) + ']');

            Grid last = null;

            for (Iterator<Grid> it = grids.iterator(); it.hasNext(); ) {
                Grid g = it.next();

                if (!it.hasNext()) {
                    last = g;

                    break;
                }

                final UUID nodeId = g.localNode().id();

                it.remove();

                Collection<GridFuture<?>> futs = new LinkedList<>();

                for (Grid gg : grids)
                    futs.add(waitForLocalEvent(gg.events(), new P1<GridEvent>() {
                            @Override public boolean apply(GridEvent e) {
                                GridCachePreloadingEvent evt = (GridCachePreloadingEvent)e;

                                GridNode node = evt.discoveryNode();

                                return evt.type() == EVT_CACHE_PRELOAD_STOPPED && node.id().equals(nodeId) &&
                                    evt.discoveryEventType() == EVT_NODE_LEFT;
                            }
                        }, EVT_CACHE_PRELOAD_STOPPED));


                info("Before grid stop [name=" + g.name() + ", fullTop=" + top2string(grids));

                stopGrid(g.name());

                info(">>> Waiting for preload futures [leftNode=" + g.name() + ", remaining=" + U.grids2names(grids) + ']');

                X.waitAll(futs);

                info("After grid stop [name=" + g.name() + ", fullTop=" + top2string(grids));

                // Check all left nodes.
                for (Grid gg : grids) {
                    GridCache<Integer, String> c = gg.cache(null);

                    checkKeys(c, keyCnt, grids);
                }
            }

            assert last != null;

            GridCache<Integer, String> lastCache = last.cache(null);

            GridDhtCacheAdapter<Integer, String> dht = dht(lastCache);

            GridCacheAffinity<Integer> aff = affinity(lastCache);

            for (int i = 0; i < keyCnt; i++) {
                if (aff.mapPartitionToPrimaryAndBackups(aff.partition(i)).contains(last.localNode())) {
                    GridDhtPartitionTopology<Integer, String> top = dht.topology();

                    for (GridDhtLocalPartition<Integer, String> p : top.localPartitions()) {
                        Collection<GridNode> moving = top.moving(p.id());

                        assert moving.isEmpty() : "Nodes with partition in moving state [part=" + p +
                            ", moving=" + moving + ']';

                        assert OWNING == p.state() : "Invalid partition state for partition [part=" + p + ", map=" +
                            top.partitionMap(false) + ']';
                    }
                }
            }
        }
        catch (Error | Exception e) {
            error("Test failed.", e);

            throw e;
        } finally {
            stopAllGrids();
        }
    }

    /**
     * @param c Cache.
     * @param cnt Key count.
     * @throws GridException If failed.
     */
    private void putKeys(GridCache<Integer, String> c, int cnt) throws GridException {
        for (int i = 0; i < cnt; i++)
            c.put(i, Integer.toString(i));
    }

    /**
     * @param cache Cache.
     * @param cnt Key count.
     * @param grids Grids.
     * @throws GridException If failed.
     */
    private void checkKeys(GridCache<Integer, String> cache, int cnt, Iterable<Grid> grids) throws GridException {
        GridCacheAffinity<Integer> aff = affinity(cache);

        Grid grid = cache.gridProjection().grid();

        GridNode loc = grid.localNode();

        boolean sync = cache.configuration().getPreloadMode() == SYNC;

        for (int i = 0; i < cnt; i++) {
            Collection<GridNode> nodes = grid.nodes();

            Collection<GridNode> affNodes = aff.mapPartitionToPrimaryAndBackups(aff.partition(i));

            assert !affNodes.isEmpty();

            if (affNodes.contains(loc)) {
                String val = sync ? cache.peek(i) : cache.get(i);

                GridNode primaryNode = F.first(affNodes);

                assert primaryNode != null;

                boolean primary = primaryNode.equals(loc);

                assert Integer.toString(i).equals(val) : "Key check failed [grid=" + grid.name() +
                    ", cache=" + cache.name() + ", key=" + i + ", expected=" + i + ", actual=" + val +
                    ", part=" + aff.partition(i) + ", primary=" + primary + ", affNodes=" + U.nodeIds(affNodes) +
                    ", locId=" + loc.id() + ", allNodes=" + U.nodeIds(nodes) + ", allParts=" + top2string(grids) + ']';
            }
        }
    }

    /**
     * @param grids Grids
     * @return String representation of all partitions and their state.
     */
    @SuppressWarnings( {"ConstantConditions"})
    private String top2string(Iterable<Grid> grids) {
        Map<String, String> map = new HashMap<>();

        for (Grid g : grids) {
            GridCache<Integer, String> c = g.cache(null);

            GridDhtCacheAdapter<Integer, String> dht = dht(c);

            GridDhtPartitionFullMap fullMap = dht.topology().partitionMap(false);

            map.put(g.name(), DEBUG ? fullMap.toFullString() : fullMap.toString());
        }

        return "Grid partition maps [" + map.toString() + ']';
    }
}
