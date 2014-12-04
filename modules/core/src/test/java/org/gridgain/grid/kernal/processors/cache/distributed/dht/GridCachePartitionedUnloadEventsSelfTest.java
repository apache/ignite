/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.events.GridEventType.*;

/**
 */
public class GridCachePartitionedUnloadEventsSelfTest extends GridCommonAbstractTest {
    /** */
    private GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();
        disco.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    protected GridCacheConfiguration cacheConfiguration() {
        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setPreloadMode(SYNC);
        cacheCfg.setAffinity(new GridCacheConsistentHashAffinityFunction(false, 10));
        cacheCfg.setBackups(0);
        return cacheCfg;
    }

    /**
     * @throws Exception if failed.
     */
    public void testUnloadEvents() throws Exception {
        final Ignite g1 = startGrid("g1");

        Collection<Integer> allKeys = new ArrayList<>(100);

        GridCache<Integer, String> cache = g1.cache(null);

        for (int i = 0; i < 100; i++) {
            cache.put(i, "val");
            allKeys.add(i);
        }

        Ignite g2 = startGrid("g2");

        Map<GridNode, Collection<Object>> keysMap = g1.cache(null).affinity().mapKeysToNodes(allKeys);
        Collection<Object> g2Keys = keysMap.get(g2.cluster().localNode());

        assertNotNull(g2Keys);
        assertFalse("There are no keys assigned to g2", g2Keys.isEmpty());

        Thread.sleep(5000);

        Collection<GridEvent> objEvts =
            g1.events().localQuery(F.<GridEvent>alwaysTrue(), EVT_CACHE_PRELOAD_OBJECT_UNLOADED);

        checkObjectUnloadEvents(objEvts, g1, g2Keys);

        Collection <GridEvent> partEvts =
            g1.events().localQuery(F.<GridEvent>alwaysTrue(), EVT_CACHE_PRELOAD_PART_UNLOADED);

        checkPartitionUnloadEvents(partEvts, g1, dht(g2.cache(null)).topology().localPartitions());
    }

    /**
     * @param evts Events.
     * @param g Grid.
     * @param keys Keys.
     */
    private void checkObjectUnloadEvents(Collection<GridEvent> evts, Ignite g, Collection<?> keys) {
        assertEquals(keys.size(), evts.size());

        for (GridEvent evt : evts) {
            GridCacheEvent cacheEvt = ((GridCacheEvent)evt);

            assertEquals(EVT_CACHE_PRELOAD_OBJECT_UNLOADED, cacheEvt.type());
            assertEquals(g.cache(null).name(), cacheEvt.cacheName());
            assertEquals(g.cluster().localNode().id(), cacheEvt.node().id());
            assertEquals(g.cluster().localNode().id(), cacheEvt.eventNode().id());
            assertTrue("Unexpected key: " + cacheEvt.key(), keys.contains(cacheEvt.key()));
        }
    }

    /**
     * @param evts Events.
     * @param g Grid.
     * @param parts Parts.
     */
    private void checkPartitionUnloadEvents(Collection<GridEvent> evts, Ignite g,
        Collection<GridDhtLocalPartition<Object, Object>> parts) {
        assertEquals(parts.size(), evts.size());

        for (GridEvent evt : evts) {
            GridCachePreloadingEvent unloadEvt = (GridCachePreloadingEvent)evt;

            final int part = unloadEvt.partition();

            assertNotNull("Unexpected partition: " + part, F.find(parts, null,
                new GridPredicate<GridDhtLocalPartition<Object, Object>>() {
                    @Override
                    public boolean apply(GridDhtLocalPartition<Object, Object> e) {
                        return e.id() == part;
                    }
                }));

            assertEquals(g.cache(null).name(), unloadEvt.cacheName());
            assertEquals(g.cluster().localNode().id(), unloadEvt.node().id());
        }
    }
}
