/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Checks that readers are properly handled.
 */
public class GridCacheNearReadersSelfTest extends GridCommonAbstractTest {
    /** Number of grids. */
    private int grids = 2;

    /** */
    private GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** Grid counter. */
    private static AtomicInteger cntr = new AtomicInteger(0);

    /** Test cache affinity. */
    private GridCacheModuloAffinityFunction aff = new GridCacheModuloAffinityFunction();

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setPreloadMode(NONE);

        cacheCfg.setAffinity(aff);
        cacheCfg.setSwapEnabled(false);
        cacheCfg.setEvictSynchronized(true);
        cacheCfg.setEvictNearSynchronized(true);
        cacheCfg.setAtomicityMode(atomicityMode());
        cacheCfg.setDistributionMode(NEAR_PARTITIONED);
        cacheCfg.setBackups(aff.backups());

        cfg.setCacheConfiguration(cacheCfg);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setUserAttributes(F.asMap(GridCacheModuloAffinityFunction.IDX_ATTR, cntr.getAndIncrement()));

        return cfg;
    }

    /**
     * @return Atomicity mode.
     */
    protected GridCacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** @throws Exception If failed. */
    private void startGrids() throws Exception {
        assert grids > 0;
        assert aff.backups() >= 0;

        startGrids(grids);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        grids = -1;

        aff.reset();

        cntr.set(0);
    }

    /**
     * @param nodeId Node ID.
     * @return Grid.
     */
    private Ignite grid(UUID nodeId) {
        return G.grid(nodeId);
    }

    /** @throws Exception If failed. */
    public void testTwoNodesTwoKeysNoBackups() throws Exception {
        aff.backups(0);
        grids = 2;
        aff.partitions(grids);

        startGrids();

        ClusterNode n1 = F.first(aff.nodes(aff.partition(1), grid(0).nodes()));
        ClusterNode n2 = F.first(aff.nodes(aff.partition(2), grid(0).nodes()));

        assertNotNull(n1);
        assertNotNull(n2);
        assertNotSame(n1, n2);
        assertFalse("Nodes cannot be equal: " + n1, n1.equals(n2));

        Ignite g1 = grid(n1.id());
        Ignite g2 = grid(n2.id());

        GridCache<Integer, String> cache1 = g1.cache(null);
        GridCache<Integer, String> cache2 = g2.cache(null);

        // Store some values in cache.
        assertNull(cache1.put(1, "v1"));
        assertNull(cache1.put(2, "v2"));

        GridDhtCacheEntry<Integer, String> e1 = (GridDhtCacheEntry<Integer, String>)dht(cache1).entryEx(1);
        GridDhtCacheEntry<Integer, String> e2 = (GridDhtCacheEntry<Integer, String>)dht(cache2).entryEx(2);

        assertNotNull(e1.readers());

        assertTrue(cache1.containsKey(1));
        assertTrue(cache1.containsKey(2));

        assertNotNull(near(cache1).peek(1));
        assertNotNull(near(cache1).peek(2));
        assertNotNull(dht(cache1).peek(1));
        assertNull(dht(cache1).peek(2));

        assertNull(near(cache2).peek(1));
        assertNotNull(dht(cache2).peek(2));

        // Node2 should have node1 in reader's map, since request to
        // put key 2 came from node1.
        assertTrue(e2.readers().contains(n1.id()));

        // Node1 should not have node2 in readers map yet.
        assertFalse(e1.readers().contains(n2.id()));

        // Get key1 on node2.
        assertEquals("v1", cache2.get(1));

        // Check that key1 is in near cache of cache2.
        assertNotNull(near(cache2).peek(1));

        // Now node1 should have node2 in readers map.
        assertTrue(e1.readers().contains(n2.id()));

        // Evict locally from cache2.
        assertTrue(cache2.evict(1));

        assertNull(near(cache2).peek(1));
        assertNull(dht(cache2).peek(1));

        // Node 1 still has node2 in readers map.
        assertTrue(e1.readers().contains(n2.id()));

        assertNotNull((cache1.put(1, "z1")));

        // Node 1 still has node2 in readers map.
        assertFalse(e1.readers().contains(n2.id()));
    }

    /** @throws Exception If failed. */
    public void testTwoNodesTwoKeysOneBackup() throws Exception {
        aff.backups(1);
        grids = 2;
        aff.partitions(grids);

        startGrids();

        ClusterNode n1 = F.first(aff.nodes(aff.partition(1), grid(0).nodes()));
        ClusterNode n2 = F.first(aff.nodes(aff.partition(2), grid(0).nodes()));

        assertNotNull(n1);
        assertNotNull(n2);
        assertNotSame(n1, n2);
        assertFalse("Nodes cannot be equal: " + n1, n1.equals(n2));

        Ignite g1 = grid(n1.id());
        Ignite g2 = grid(n2.id());

        awaitPartitionMapExchange();

        ((GridKernal)g1).internalCache(null).preloader().request(F.asList(1, 2), 2).get();
        ((GridKernal)g2).internalCache(null).preloader().request(F.asList(1, 2), 2).get();

        GridCache<Integer, String> cache1 = g1.cache(null);
        GridCache<Integer, String> cache2 = g2.cache(null);

        assertEquals(cache1.affinity().mapKeyToNode(1), g1.cluster().localNode());
        assertFalse(cache1.affinity().mapKeyToNode(2).equals(g1.cluster().localNode()));

        assertEquals(cache2.affinity().mapKeyToNode(2), g2.cluster().localNode());
        assertFalse(cache2.affinity().mapKeyToNode(1).equals(g2.cluster().localNode()));

        // Store first value in cache.
        assertNull(cache1.put(1, "v1"));

        assertTrue(cache1.containsKey(1));
        assertTrue(cache2.containsKey(1));

        assertEquals("v1", near(cache1).peek(1));
        assertEquals("v1", near(cache2).peek(1));
        assertEquals("v1", dht(cache1).peek(1));
        assertEquals("v1", dht(cache2).peek(1));

        assertNull(near(cache1).peekNearOnly(1));
        assertNull(near(cache2).peekNearOnly(1));

        GridDhtCacheEntry<Integer, String> e1 = (GridDhtCacheEntry<Integer, String>)dht(cache1).entryEx(1);

        // Store second value in cache.
        assertNull(cache1.put(2, "v2"));

        assertTrue(cache1.containsKey(2));
        assertTrue(cache2.containsKey(2));

        assertEquals("v2", near(cache1).peek(2));
        assertEquals("v2", near(cache2).peek(2));
        assertEquals("v2", dht(cache1).peek(2));
        assertEquals("v2", dht(cache2).peek(2));

        assertNull(near(cache1).peekNearOnly(2));
        assertNull(near(cache2).peekNearOnly(2));

        GridDhtCacheEntry<Integer, String> c2e2 = (GridDhtCacheEntry<Integer, String>)dht(cache2).entryEx(2);

        // Nodes are backups of each other, so no readers should be added.
        assertFalse(c2e2.readers().contains(n1.id()));
        assertFalse(e1.readers().contains(n2.id()));

        // Get key1 on node2 (value should come from local DHT cache, as it has a backup).
        assertEquals("v1", cache2.get(1));

        // Since DHT cache2 has the value, Near cache2 should not have it.
        assertNull(near(cache2).peekNearOnly(1));

        // Since v1 was retrieved locally from cache2, cache1 should not know about it.
        assertFalse(e1.readers().contains(n2.id()));

        // Evict locally from cache2.
        // It should not be successful since it's not allowed to evict entry on backup node.
        assertFalse(cache2.evict(1));

        assertNull(near(cache2).peekNearOnly(1));
        assertEquals("v1", dht(cache2).peek(1));

        assertEquals("v1", cache1.put(1, "z1"));

        // Node 1 should not have node2 in readers map.
        assertFalse(e1.readers().contains(n2.id()));

        assertNull(near(cache2).peekNearOnly(1));
        assertEquals("z1", dht(cache2).peek(1));
    }

    /** @throws Exception If failed. */
    public void testPutAllManyKeysOneReader() throws Exception {
        aff.backups(1);
        grids = 4;
        aff.partitions(grids);

        startGrids();

        try {
            GridCache<Object, Object> prj0 = grid(0).cache(null);
            GridCache<Object, Object> prj1 = grid(1).cache(null);

            Map<Integer, Integer> putMap = new HashMap<>();

            int size = 100;

            for (int i = 0; i < size; i++)
                putMap.put(i, i);

            prj0.putAll(putMap);

            for (int i = 0; i < size; i++)
                putMap.put(i, i * i);

            prj1.putAll(putMap);

            for (int i = 0; i < size; i++) {
                assertEquals(i * i, prj0.get(i));
                assertEquals(i * i, prj1.get(i));
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testPutAllManyKeysTwoReaders() throws Exception {
        aff.backups(1);
        grids = 5;
        aff.partitions(grids);

        startGrids();

        try {
            GridCache<Object, Object> prj0 = grid(0).cache(null);
            GridCache<Object, Object> prj1 = grid(1).cache(null);
            GridCache<Object, Object> prj2 = grid(2).cache(null);

            Map<Integer, Integer> putMap = new HashMap<>();

            int size = 100;

            for (int i = 0; i < size; i++)
                putMap.put(i, i);

            prj0.putAll(putMap);

            for (int i = 0; i < size; i++)
                putMap.put(i, i * i);

            prj1.putAll(putMap);

            for (int i = 0; i < size; i++)
                putMap.put(i, i * i * i);

            prj2.putAll(putMap);

            for (int i = 0; i < size; i++) {
                assertEquals(i * i * i, prj0.get(i));
                assertEquals(i * i * i, prj1.get(i));
                assertEquals(i * i * i, prj2.get(i));
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testBackupEntryReaders() throws Exception {
        aff.backups(1);
        grids = 2;
        aff.partitions(grids);

        startGrids();

        Collection<ClusterNode> nodes = new ArrayList<>(aff.nodes(aff.partition(1), grid(0).nodes()));

        ClusterNode primary = F.first(nodes);

        assert primary != null;

        nodes.remove(primary);

        ClusterNode backup = F.first(nodes);

        assert backup != null;

        assertNotSame(primary, backup);

        assertFalse("Nodes cannot be equal: " + primary, primary.equals(backup));

        GridCache<Integer, String> cache1 = grid(primary.id()).cache(null);
        GridCache<Integer, String> cache2 = grid(backup.id()).cache(null);

        // Store a values in cache.
        assertNull(cache1.put(1, "v1"));

        GridDhtCacheEntry<Integer, String> e1 = (GridDhtCacheEntry<Integer, String>)dht(cache1).peekEx(1);
        GridDhtCacheEntry<Integer, String> e2 = (GridDhtCacheEntry<Integer, String>)dht(cache2).peekEx(1);

        assert e1 != null;
        assert e2 != null;

        // Check entry on primary node.
        assertTrue(e1.wrap(false).primary());
        assertNotNull(e1.readers());
        assertTrue(e1.readers().isEmpty());

        // Check entry on backup node.
        assertFalse(e2.wrap(false).primary());
        assertNotNull(e2.readers());
        assertTrue(e2.readers().isEmpty());
    }

    /** @throws Exception If failed. */
    @SuppressWarnings({"SizeReplaceableByIsEmpty"})
    public void testImplicitLockReaders() throws Exception {
        grids = 3;
        aff.reset(grids, 1);

        startGrids();

        int key1 = 3;
        String val1 = Integer.toString(key1);

        assertEquals(grid(0).localNode(), F.first(aff.nodes(aff.partition(key1), grid(0).nodes())));

        int key2 = 1;
        String val2 = Integer.toString(key2);

        assertEquals(grid(1).localNode(), F.first(aff.nodes(aff.partition(key2), grid(1).nodes())));

        GridCache<Integer, String> cache = cache(0);

        assertNull(cache.put(key1, val1));

        assertEquals(val1, dht(0).peek(key1));
        assertEquals(val1, dht(1).peek(key1));
        assertNull(dht(2).peek(key1));

        assertNull(near(0).peekNearOnly(key1));
        assertNull(near(1).peekNearOnly(key1));
        assertNull(near(2).peekNearOnly(key1));

        assertTrue(cache.putx(key2, val2));

        assertNull(dht(0).peek(key2));
        assertEquals(val2, dht(1).peek(key2));
        assertEquals(val2, dht(2).peek(key2));

        assertEquals(val2, near(0).peekNearOnly(key2));
        assertNull(near(1).peekNearOnly(key2));
        assertNull(near(2).peekNearOnly(key2));

        String val22 = val2 + "2";

        cache.put(key2, val22);

        assertNull(dht(0).peek(key2));
        assertEquals(val22, dht(1).peek(key2));
        assertEquals(val22, dht(2).peek(key2));

        assertEquals(val22, near(0).peekNearOnly(key2));
        assertNull(near(1).peekNearOnly(key2));
        assertNull(near(2).peekNearOnly(key2));

        cache.remove(key2);

        assertNull(dht(0).peek(key2));
        assertNull(dht(1).peek(key2));
        assertNull(dht(2).peek(key2));

        assertNull(near(0).peekNearOnly(key2));
        assertNull(near(1).peekNearOnly(key2));
        assertNull(near(2).peekNearOnly(key2));

        cache.remove(key1);

        assertNull(dht(0).peek(key1));
        assertNull(dht(1).peek(key1));
        assertNull(dht(2).peek(key1));

        assertNull(near(0).peekNearOnly(key1));
        assertNull(near(1).peekNearOnly(key1));
        assertNull(near(2).peekNearOnly(key1));

        for (int i = 0; i < grids; i++) {
            assert !cache(i).isLocked(key1);
            assert !cache(i).isLocked(key2);

            assert cache(i).size() == 0;
            assert cache(i).isEmpty();
            assert cache(i).size() == 0;
        }
    }

    /** @throws Exception If failed. */
    public void testExplicitLockReaders() throws Exception {
        if (atomicityMode() == ATOMIC)
            return;

        grids = 3;
        aff.reset(grids, 1);

        startGrids();

        int key1 = 3;
        String val1 = Integer.toString(key1);

        assertEquals(grid(0).localNode(), F.first(aff.nodes(aff.partition(key1), grid(0).nodes())));

        int key2 = 1;
        String val2 = Integer.toString(key2);

        assertEquals(grid(1).localNode(), F.first(aff.nodes(aff.partition(key2), grid(1).nodes())));

        GridCache<Integer, String> cache = cache(0);

        assertTrue(cache.lock(key1, 0L));

        try {
            // Nested lock.
            assertTrue(cache.lock(key2, 0L));

            try {
                assertNull(cache.put(key1, val1));

                assertEquals(val1, dht(0).peek(key1));
                assertEquals(val1, dht(1).peek(key1));
                assertNull(dht(2).peek(key1));

                // Since near entry holds the lock, it should
                // contain correct value.
                assertEquals(val1, near(0).peekNearOnly(key1));

                assertNull(near(1).peekNearOnly(key1));
                assertNull(near(2).peekNearOnly(key1));

                assertTrue(cache.putx(key2, val2));

                assertNull(dht(0).peek(key2));
                assertEquals(val2, dht(1).peek(key2));
                assertEquals(val2, dht(2).peek(key2));

                assertEquals(val2, near(0).peekNearOnly(key2));
                assertNull(near(1).peekNearOnly(key2));
                assertNull(near(2).peekNearOnly(key2));

                String val22 = val2 + "2";

                cache.put(key2, val22);

                assertNull(dht(0).peek(key2));
                assertEquals(val22, dht(1).peek(key2));
                assertEquals(val22, dht(2).peek(key2));

                assertEquals(val22, near(0).peekNearOnly(key2));
                assertNull(near(1).peekNearOnly(key2));
                assertNull(near(2).peekNearOnly(key2));

                cache.remove(key2);

                assertNull(dht(0).peek(key2));
                assertNull(dht(1).peek(key2));
                assertNull(dht(2).peek(key2));

                assertNull(dht(0).peekEx(key2));
                assertNotNull(dht(1).peekEx(key2));
                assertNotNull(dht(2).peekEx(key2));

                assertNotNull(near(0).peekEx(key2));
                assertNull(near(1).peekEx(key2));
                assertNull(near(2).peekEx(key2));
            }
            finally {
                cache.unlock(key2);
            }
        }
        finally {
            cache.unlock(key1);
        }
    }
}
