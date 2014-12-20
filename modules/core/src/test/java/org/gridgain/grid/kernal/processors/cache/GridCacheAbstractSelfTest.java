/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMemoryMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Abstract class for cache tests.
 */
public abstract class GridCacheAbstractSelfTest extends GridCommonAbstractTest {
    /** Test timeout */
    private static final long TEST_TIMEOUT = 30 * 1000;

    /** Store map. */
    protected static final Map<Object, Object> map = new ConcurrentHashMap8<>();

    /** VM ip finder for TCP discovery. */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     * @return Grids count to start.
     */
    protected abstract int gridCount();

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        int cnt = gridCount();

        assert cnt >= 1 : "At least one grid must be started";

        startGridsMultiThreaded(cnt);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        map.clear();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        assert cache().tx() == null;
        assert cache().isEmpty() : "Cache is not empty: " + cache().entrySet();
        assert cache().keySet().isEmpty() : "Key set is not empty: " + cache().keySet();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        GridCacheTx tx = cache().tx();

        if (tx != null) {
            tx.close();

            fail("Cache transaction remained after test completion: " + tx);
        }

        for (int i = 0; i < gridCount(); i++) {
            while (true) {
                try {
                    final int fi = i;

                    assertTrue(
                        "Cache is not empty: " + cache(i).entrySet(),
                        GridTestUtils.waitForCondition(
                            // Preloading may happen as nodes leave, so we need to wait.
                            new GridAbsPredicateX() {
                                @Override public boolean applyx() throws IgniteCheckedException {
                                    GridCache<String, Integer> cache = cache(fi);

                                    cache.removeAll();

                                    if (offheapTiered(cache)) {
                                        Iterator it = cache.offHeapIterator();

                                        while (it.hasNext()) {
                                            it.next();

                                            it.remove();
                                        }

                                        if (cache.offHeapIterator().hasNext())
                                            return false;
                                    }

                                    return cache.isEmpty();
                                }
                            },
                            getTestTimeout()));

                    int primaryKeySize = cache(i).primarySize();
                    int keySize = cache(i).size();
                    int size = cache(i).size();
                    int globalSize = cache(i).globalSize();
                    int globalPrimarySize = cache(i).globalPrimarySize();

                    info("Size after [idx=" + i +
                        ", size=" + size +
                        ", keySize=" + keySize +
                        ", primarySize=" + primaryKeySize +
                        ", globalSize=" + globalSize +
                        ", globalPrimarySize=" + globalPrimarySize +
                        ", keySet=" + cache(i).keySet() + ']');

                    assertEquals("Cache is not empty [idx=" + i + ", entrySet=" + cache(i).entrySet() + ']',
                        0, cache(i).size());

                    break;
                }
                catch (Exception e) {
                    if (X.hasCause(e, ClusterTopologyException.class)) {
                        info("Got topology exception while tear down (will retry in 1000ms).");

                        U.sleep(1000);
                    }
                    else
                        throw e;
                }
            }

            Iterator<Map.Entry<String, Integer>> it = cache(i).swapIterator();

            while (it.hasNext()) {
                Map.Entry<String, Integer> entry = it.next();

                cache(i).remove(entry.getKey());
            }
        }

        assert cache().tx() == null;
        assert cache().isEmpty() : "Cache is not empty: " + cache().entrySet();
        assertEquals("Cache is not empty: " + cache().entrySet(), 0, cache().size());
        assert cache().keySet().isEmpty() : "Key set is not empty: " + cache().keySet();

        resetStore();
    }

    /**
     * Cleans up cache store.
     */
    protected void resetStore() {
        map.clear();
    }

    /**
     * Put entry to cache store.
     *
     * @param key Key.
     * @param val Value.
     */
    protected void putToStore(Object key, Object val) {
        map.put(key, val);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setMaxMissedHeartbeats(Integer.MAX_VALUE);

        disco.setIpFinder(ipFinder);

        if (isDebug())
            disco.setAckTimeout(Integer.MAX_VALUE);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(cacheConfiguration(gridName));

        cfg.setMarshaller(new IgniteOptimizedMarshaller(false));

        return cfg;
    }

    /**
     * @param gridName Grid name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setStore(cacheStore());
        cfg.setSwapEnabled(swapEnabled());
        cfg.setCacheMode(cacheMode());
        cfg.setAtomicityMode(atomicityMode());
        cfg.setWriteSynchronizationMode(writeSynchronization());
        cfg.setDistributionMode(distributionMode());
        cfg.setPortableEnabled(portableEnabled());

        if (cacheMode() == PARTITIONED)
            cfg.setBackups(1);

        return cfg;
    }

    /**
     * @return Default cache mode.
     */
    protected GridCacheMode cacheMode() {
        return GridCacheConfiguration.DFLT_CACHE_MODE;
    }

    /**
     * @return Cache atomicity mode.
     */
    protected GridCacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @return Partitioned mode.
     */
    protected GridCacheDistributionMode distributionMode() {
        return NEAR_PARTITIONED;
    }

    /**
     * @return Write synchronization.
     */
    protected GridCacheWriteSynchronizationMode writeSynchronization() {
        return FULL_SYNC;
    }

    /**
     * @return Write through storage emulator.
     */
    protected GridCacheStore<?, ?> cacheStore() {
        return new GridCacheStoreAdapter<Object, Object>() {
            @Override public void loadCache(IgniteBiInClosure<Object, Object> clo,
                Object... args) {
                for (Map.Entry<Object, Object> e : map.entrySet())
                    clo.apply(e.getKey(), e.getValue());
            }

            @Override public Object load(GridCacheTx tx, Object key) {
                return map.get(key);
            }

            @Override public void put(GridCacheTx tx, Object key, @Nullable Object val) {
                map.put(key, val);
            }

            @Override public void remove(GridCacheTx tx, Object key) {
                map.remove(key);
            }
        };
    }

    /**
     * @return {@code true} if swap should be enabled.
     */
    protected boolean swapEnabled() {
        return true;
    }

    /**
     * @return {@code true} if near cache should be enabled.
     */
    protected boolean nearEnabled() {
        return distributionMode() == NEAR_ONLY || distributionMode() == NEAR_PARTITIONED;
    }

    /**
     * @return {@code True} if transactions are enabled.
     */
    protected boolean txEnabled() {
        return true;
    }

    /**
     * @return {@code True} if locking is enabled.
     */
    protected boolean lockingEnabled() {
        return true;
    }

    /**
     * @return Whether portable mode is enabled.
     */
    protected boolean portableEnabled() {
        return false;
    }

    /**
     * @return {@code True} for partitioned caches.
     */
    protected final boolean partitionedMode() {
        return cacheMode() == PARTITIONED;
    }

    /**
     * @param idx Index of grid.
     * @return Cache instance casted to work with string and integer types for convenience.
     */
    @SuppressWarnings({"unchecked"})
    @Override protected GridCache<String, Integer> cache(int idx) {
        return grid(idx).cache(null);
    }

    /**
     * @return Default cache instance casted to work with string and integer types for convenience.
     */
    @SuppressWarnings({"unchecked"})
    @Override protected GridCache<String, Integer> cache() {
        return cache(0);
    }

    /**
     * @param idx Index of grid.
     * @return Cache context.
     */
    protected GridCacheContext<String, Integer> context(int idx) {
        return ((GridKernal)grid(idx)).<String, Integer>internalCache().context();
    }

    /**
     * @param key Key.
     * @param idx Node index.
     * @return {@code True} if key belongs to node with index idx.
     */
    protected boolean belongs(String key, int idx) {
        return context(idx).cache().affinity().isPrimaryOrBackup(context(idx).localNode(), key);
    }

    /**
     * @param cache Cache.
     * @return {@code True} if cache has OFFHEAP_TIERED memory mode.
     */
    protected boolean offheapTiered(GridCache cache) {
        return cache.configuration().getMemoryMode() == OFFHEAP_TIERED;
    }

    /**
     * Executes regular peek or peek from swap.
     *
     * @param prj Cache projection.
     * @param key Key.
     * @return Value.
     * @throws Exception If failed.
     */
    @Nullable protected <K, V> V peek(GridCacheProjection<K, V> prj, K key) throws Exception {
        return offheapTiered(prj.cache()) ? prj.peek(key, F.asList(GridCachePeekMode.SWAP)) : prj.peek(key);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @return {@code True} if cache contains given key.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    protected boolean containsKey(GridCache cache, Object key) throws Exception {
        return offheapTiered(cache) ? containsOffheapKey(cache, key) : cache.containsKey(key);
    }

    /**
     * @param cache Cache.
     * @param val Value.
     * @return {@code True} if cache contains given value.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    protected boolean containsValue(GridCache cache, Object val) throws Exception {
        return offheapTiered(cache) ? containsOffheapValue(cache, val) : cache.containsValue(val);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @return {@code True} if offheap contains given key.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    protected boolean containsOffheapKey(GridCache cache, Object key) throws Exception {
        for (Iterator<Map.Entry> it = cache.offHeapIterator(); it.hasNext();) {
            Map.Entry e = it.next();

            if (key.equals(e.getKey()))
                return true;
        }

        return false;
    }

    /**
     * @param cache Cache.
     * @param val Value.
     * @return {@code True} if offheap contains given value.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    protected boolean containsOffheapValue(GridCache cache, Object val) throws Exception {
        for (Iterator<Map.Entry> it = cache.offHeapIterator(); it.hasNext();) {
            Map.Entry e = it.next();

            if (val.equals(e.getValue()))
                return true;
        }

        return false;
    }

    /**
     * Filters cache entry projections leaving only ones with keys containing 'key'.
     */
    protected static IgnitePredicate<GridCacheEntry<String, Integer>> entryKeyFilter =
        new P1<GridCacheEntry<String, Integer>>() {
        @Override public boolean apply(GridCacheEntry<String, Integer> entry) {
            return entry.getKey().contains("key");
        }
    };

    /**
     * Filters cache entry projections leaving only ones with keys not containing 'key'.
     */
    protected static IgnitePredicate<GridCacheEntry<String, Integer>> entryKeyFilterInv =
        new P1<GridCacheEntry<String, Integer>>() {
        @Override public boolean apply(GridCacheEntry<String, Integer> entry) {
            return !entry.getKey().contains("key");
        }
    };

    /**
     * Filters cache entry projections leaving only ones with values less than 50.
     */
    protected static final IgnitePredicate<GridCacheEntry<String, Integer>> lt50 =
        new P1<GridCacheEntry<String, Integer>>() {
            @Override public boolean apply(GridCacheEntry<String, Integer> entry) {
                Integer i = entry.peek();

                return i != null && i < 50;
            }
        };

    /**
     * Filters cache entry projections leaving only ones with values greater or equal than 100.
     */
    protected static final IgnitePredicate<GridCacheEntry<String, Integer>> gte100 =
        new P1<GridCacheEntry<String, Integer>>() {
            @Override public boolean apply(GridCacheEntry<String, Integer> entry) {
                Integer i = entry.peek();

                return i != null && i >= 100;
            }

            @Override public String toString() {
                return "gte100";
            }
        };

    /**
     * Filters cache entry projections leaving only ones with values greater or equal than 200.
     */
    protected static final IgnitePredicate<GridCacheEntry<String, Integer>> gte200 =
        new P1<GridCacheEntry<String, Integer>>() {
            @Override public boolean apply(GridCacheEntry<String, Integer> entry) {
                Integer i = entry.peek();

                return i != null && i >= 200;
            }

            @Override public String toString() {
                return "gte200";
            }
        };

    /**
     * {@link org.apache.ignite.lang.IgniteInClosure} for calculating sum.
     */
    @SuppressWarnings({"PublicConstructorInNonPublicClass"})
    protected static final class SumVisitor implements CI1<GridCacheEntry<String, Integer>> {
        /** */
        private final AtomicInteger sum;

        /**
         * @param sum {@link AtomicInteger} instance for accumulating sum.
         */
        public SumVisitor(AtomicInteger sum) {
            this.sum = sum;
        }

        /** {@inheritDoc} */
        @Override public void apply(GridCacheEntry<String, Integer> entry) {
            if (entry.getValue() != null) {
                Integer i = entry.getValue();

                assert i != null : "Value cannot be null for entry: " + entry;

                sum.addAndGet(i);
            }
        }
    }

    /**
     * {@link org.apache.ignite.lang.IgniteReducer} for calculating sum.
     */
    @SuppressWarnings({"PublicConstructorInNonPublicClass"})
    protected static final class SumReducer implements R1<GridCacheEntry<String, Integer>, Integer> {
        /** */
        private int sum;

        /** */
        public SumReducer() {
            // no-op
        }

        /** {@inheritDoc} */
        @Override public boolean collect(GridCacheEntry<String, Integer> entry) {
            if (entry.getValue() != null) {
                Integer i = entry.getValue();

                assert i != null;

                sum += i;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce() {
            return sum;
        }
    }
}
