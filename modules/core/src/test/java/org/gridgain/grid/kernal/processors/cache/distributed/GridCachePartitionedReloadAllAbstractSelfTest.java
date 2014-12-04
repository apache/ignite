/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicWriteOrderMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Check reloadAll() on partitioned cache.
 */
public abstract class GridCachePartitionedReloadAllAbstractSelfTest extends GridCommonAbstractTest {
    /** Amount of nodes in the grid. */
    private static final int GRID_CNT = 4;

    /** Amount of backups in partitioned cache. */
    private static final int BACKUP_CNT = 1;

    /** IP finder. */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** Map where dummy cache store values are stored. */
    private final Map<Integer, String> map = new ConcurrentHashMap8<>();

    /** Collection of caches, one per grid node. */
    private List<GridCache<Integer, String>> caches;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setDistributionMode(nearEnabled() ? NEAR_PARTITIONED : PARTITIONED_ONLY);

        cc.setCacheMode(cacheMode());

        cc.setAtomicityMode(atomicityMode());

        cc.setBackups(BACKUP_CNT);

        cc.setWriteSynchronizationMode(FULL_SYNC);

        cc.setStore(cacheStore());

        cc.setAtomicWriteOrderMode(atomicWriteOrderMode());

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @return Cache mode.
     */
    protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @return Atomicity mode.
     */
    protected GridCacheAtomicityMode atomicityMode() {
        return GridCacheAtomicityMode.TRANSACTIONAL;
    }

    /**
     * @return Write order mode for atomic cache.
     */
    protected GridCacheAtomicWriteOrderMode atomicWriteOrderMode() {
        return CLOCK;
    }

    /**
     * @return {@code True} if near cache is enabled.
     */
    protected abstract boolean nearEnabled();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        caches = new ArrayList<>(GRID_CNT);

        for (int i = 0; i < GRID_CNT; i++)
            caches.add(startGrid(i).<Integer, String>cache(null));

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        map.clear();

        caches = null;
    }

    /**
     * Create new cache store.
     *
     * @return Write through storage emulator.
     */
    protected GridCacheStore<?, ?> cacheStore() {
        return new GridCacheStoreAdapter<Integer, String>() {
            @GridInstanceResource
            private Ignite g;

            @Override public void loadCache(GridBiInClosure<Integer, String> c,
                Object... args) {
                X.println("Loading all on: " + caches.indexOf(g.<Integer, String>cache(null)));

                for (Map.Entry<Integer, String> e : map.entrySet())
                    c.apply(e.getKey(), e.getValue());
            }

            @Override public String load(GridCacheTx tx, Integer key) {
                X.println("Loading on: " + caches.indexOf(g.<Integer, String>cache(null)) + " key=" + key);

                return map.get(key);
            }

            @Override public void put(GridCacheTx tx, Integer key, @Nullable String val) {
                fail("Should not be called within the test.");
            }

            @Override public void remove(GridCacheTx tx, Integer key) {
                fail("Should not be called within the test.");
            }
        };
    }

    /**
     * Ensure that reloadAll() with disabled near cache reloads data only on a node
     * on which reloadAll() has been called.
     *
     * @throws Exception If test failed.
     */
    public void testReloadAll() throws Exception {
        // Fill caches with values.
        for (GridCache<Integer, String> cache : caches) {
            Iterable<Integer> keys = primaryKeysForCache(cache, 100);

            info("Values [cache=" + caches.indexOf(cache) + ", size=" + F.size(keys.iterator()) +  ", keys=" + keys + "]");

            for (Integer key : keys)
                map.put(key, "val" + key);
        }

        Collection<GridCache<Integer, String>> emptyCaches = new ArrayList<>(caches);

        for (GridCache<Integer, String> cache : caches) {
            info("Reloading cache: " + caches.indexOf(cache));

            // Check data is reloaded only on the nodes on which reloadAll() has been called.
            if (!nearEnabled()) {
                for (GridCache<Integer, String> eCache : emptyCaches)
                    assertEquals("Non-null values found in cache [cache=" + caches.indexOf(eCache) +
                        ", size=" + eCache.size() + ", size=" + eCache.size() +
                        ", entrySetSize=" + eCache.entrySet().size() + "]",
                        0, eCache.size());
            }

            cache.reloadAll(map.keySet());

            for (Integer key : map.keySet()) {
                GridCacheEntry entry = cache.entry(key);

                if (entry.primary() || entry.backup() || nearEnabled())
                    assertEquals(map.get(key), cache.peek(key));
                else
                    assertNull(cache.peek(key));
            }

            emptyCaches.remove(cache);
        }
    }

    /**
     * Create list of keys for which the given cache is primary.
     *
     * @param cache Cache.
     * @param cnt Keys count.
     * @return Collection of keys for which given cache is primary.
     */
    private Iterable<Integer> primaryKeysForCache(GridCacheProjection<Integer,String> cache, int cnt) {
        Collection<Integer> found = new ArrayList<>(cnt);

        for (int i = 0; i < 10000; i++) {
            if (cache.entry(i).primary()) {
                found.add(i);

                if (found.size() == cnt)
                    return found;
            }
        }

        throw new IllegalStateException("Unable to find " + cnt + " keys as primary for cache.");
    }
}
