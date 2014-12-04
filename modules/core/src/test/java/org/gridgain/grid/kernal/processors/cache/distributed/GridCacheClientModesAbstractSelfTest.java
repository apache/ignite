/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.*;

import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 * Tests near-only cache.
 */
public abstract class GridCacheClientModesAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** Grid cnt. */
    private static AtomicInteger gridCnt;

    /** Near-only cache grid name. */
    private static String nearOnlyGridName;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        gridCnt = new AtomicInteger();

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cfg = super.cacheConfiguration(gridName);

        if (gridCnt.getAndIncrement() == 0) {
            cfg.setDistributionMode(clientOnly() ? CLIENT_ONLY : NEAR_ONLY);

            nearOnlyGridName = gridName;
        }

        cfg.setStore(null);
        cfg.setAffinity(new GridCacheConsistentHashAffinityFunction(false, 32));
        cfg.setBackups(1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        gridCnt.set(0);
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @return If {@code true} then uses CLIENT_ONLY mode, otherwise NEAR_ONLY.
     */
    protected abstract boolean clientOnly();

    /**
     * @throws Exception If failed.
     */
    public void testPutFromClientNode() throws Exception {
        GridCache<Object, Object> nearOnly = nearOnlyCache();

        for (int i = 0; i < 5; i++)
            nearOnly.put(i, i);

        nearOnly.putAll(F.asMap(5, 5, 6, 6, 7, 7, 8, 8, 9, 9));

        for (int key = 0; key < 10; key++) {
            for (int i = 1; i < gridCount(); i++) {
                if (grid(i).cache(null).affinity().isPrimaryOrBackup(grid(i).localNode(), key))
                    assertEquals(key, grid(i).cache(null).peek(key));
            }

            if (nearEnabled())
                assertEquals(key, nearOnly.peek(key));

            assertNull(nearOnly.peek(key, F.asList(GridCachePeekMode.PARTITIONED_ONLY)));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetFromClientNode() throws Exception {
        GridCache<Object, Object> dht = dhtCache();

        for (int i = 0; i < 10; i++)
            dht.put(i, i);

        GridCache<Object, Object> nearOnly = nearOnlyCache();

        assert dht != nearOnly;

        for (int key = 0; key < 10; key++) {
            // At start near only cache does not have any values.
            if (nearEnabled())
                assertNull(nearOnly.peek(key));

            // Get should succeed.
            assertEquals(key, nearOnly.get(key));

            // Now value should be cached.
            if (nearEnabled())
                assertEquals(key, nearOnly.peek(key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearOnlyAffinity() throws Exception {
        for (int i = 0; i < gridCount(); i++) {
            Grid g = grid(i);

            if (F.eq(g.name(), nearOnlyGridName)) {
                for (int k = 0; k < 10000; k++) {
                    GridCache<Object, Object> cache = g.cache(null);

                    String key = "key" + k;

                    if (cacheMode() == PARTITIONED)
                        assertFalse(cache.entry(key).primary() || cache.entry(key).backup());

                    assertFalse(cache.affinity().mapKeyToPrimaryAndBackups(key).contains(g.cluster().localNode()));
                }
            }
            else {
                boolean foundEntry = false;
                boolean foundAffinityNode = false;

                for (int k = 0; k < 10000; k++) {
                    GridCache<Object, Object> cache = g.cache(null);

                    String key = "key" + k;

                    if (cache.entry(key).primary() || cache.entry(key).backup())
                        foundEntry = true;

                    if (cache.affinity().mapKeyToPrimaryAndBackups(key).contains(g.cluster().localNode()))
                        foundAffinityNode = true;
                }

                assertTrue("Did not found primary or backup entry for grid: " + i, foundEntry);
                assertTrue("Did not found affinity node for grid: " + i, foundAffinityNode);
            }
        }
    }

    /**
     * @return Near only cache for this test.
     */
    protected GridCache<Object, Object> nearOnlyCache() {
        assert nearOnlyGridName != null;

        return G.grid(nearOnlyGridName).cache(null);
    }

    /**
     * @return DHT cache for this test.
     */
    protected GridCache<Object, Object> dhtCache() {
        for (int i = 0; i < gridCount(); i++) {
            if (!nearOnlyGridName.equals(grid(i).name()))
                return grid(i).cache(null);
        }

        assert false : "Cannot find DHT cache for this test.";

        return null;
    }
}
