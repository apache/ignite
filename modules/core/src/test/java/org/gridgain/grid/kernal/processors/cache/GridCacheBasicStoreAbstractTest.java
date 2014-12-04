/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Basic store test.
 */
public abstract class GridCacheBasicStoreAbstractTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Cache store. */
    private static final GridCacheTestStore store = new GridCacheTestStore();

    /**
     *
     */
    protected GridCacheBasicStoreAbstractTest() {
        super(true /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        store.resetTimestamp();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cache().clearAll();

        store.reset();
    }

    /** @return Caching mode. */
    protected abstract GridCacheMode cacheMode();

    /** {@inheritDoc} */
    @Override protected final IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(cacheMode());
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setSwapEnabled(false);
        cc.setAtomicityMode(atomicityMode());
        cc.setDistributionMode(distributionMode());
        cc.setPreloadMode(SYNC);

        cc.setStore(store);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @return Distribution mode.
     */
    protected GridCacheDistributionMode distributionMode() {
        return NEAR_PARTITIONED;
    }

    /**
     * @return Cache atomicity mode.
     */
    protected GridCacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @throws GridException If failed.
     */
    public void testNotExistingKeys() throws GridException {
        GridCache<Integer, String> cache = cache();
        Map<Integer, String> map = store.getMap();

        cache.put(100, "hacuna matata");
        assertEquals(1, map.size());

        cache.evict(100);
        assertEquals(1, map.size());

        assertEquals("hacuna matata", cache.remove(100));
        assertTrue(map.isEmpty());

        store.resetLastMethod();
        assertNull(store.getLastMethod());

        cache.remove(200);
        assertEquals("remove", store.getLastMethod());

        cache.get(300);
        assertEquals("load", store.getLastMethod());
    }

    /** @throws Exception If test fails. */
    public void testWriteThrough() throws Exception {
        GridCache<Integer, String> cache = cache();

        Map<Integer, String> map = store.getMap();

        assert map.isEmpty();

        if (atomicityMode() == TRANSACTIONAL) {
            try (GridCacheTx tx = cache.txStart(OPTIMISTIC, REPEATABLE_READ)) {
                for (int i = 1; i <= 10; i++) {
                    cache.putx(i, Integer.toString(i));

                    checkLastMethod(null);
                }

                tx.commit();
            }
        }
        else {
            Map<Integer, String> putMap = new HashMap<>();

            for (int i = 1; i <= 10; i++)
                putMap.put(i, Integer.toString(i));

            cache.putAll(putMap);
        }

        checkLastMethod("putAll");

        assert cache.size() == 10;

        for (int i = 1; i <= 10; i++) {
            String val = map.get(i);

            assert val != null;
            assert val.equals(Integer.toString(i));
        }

        store.resetLastMethod();

        if (atomicityMode() == TRANSACTIONAL) {
            try (GridCacheTx tx = cache.txStart()) {
                for (int i = 1; i <= 10; i++) {
                    String val = cache.remove(i);

                    checkLastMethod(null);

                    assert val != null;
                    assert val.equals(Integer.toString(i));
                }

                tx.commit();

                checkLastMethod("removeAll");
            }
        }
        else {
            Collection<Integer> keys = new ArrayList<>(10);

            for (int i = 1; i <= 10; i++)
                keys.add(i);

            cache.removeAll(keys);

            checkLastMethod("removeAll");
        }

        assert map.isEmpty();
    }

    /** @throws Exception If test failed. */
    public void testReadThrough() throws Exception {
        GridCache<Integer, String> cache = cache();

        Map<Integer, String> map = store.getMap();

        assert map.isEmpty();

        if (atomicityMode() == TRANSACTIONAL) {
            try (GridCacheTx tx = cache.txStart(OPTIMISTIC, REPEATABLE_READ)) {
                for (int i = 1; i <= 10; i++)
                    cache.putx(i, Integer.toString(i));

                checkLastMethod(null);

                tx.commit();
            }
        }
        else {
            Map<Integer, String> putMap = new HashMap<>();

            for (int i = 1; i <= 10; i++)
                putMap.put(i, Integer.toString(i));

            cache.putAll(putMap);
        }

        checkLastMethod("putAll");

        for (int i = 1; i <= 10; i++) {
            String val = map.get(i);

            assert val != null;
            assert val.equals(Integer.toString(i));
        }

        cache.clearAll();

        assert cache.isEmpty();
        assert cache.isEmpty();

        assert map.size() == 10;

        for (int i = 1; i <= 10; i++) {
            // Read through.
            String val = cache.get(i);

            checkLastMethod("load");

            assert val != null;
            assert val.equals(Integer.toString(i));
        }

        assert cache.size() == 10;

        cache.clearAll();

        assert cache.isEmpty();
        assert cache.isEmpty();

        assert map.size() == 10;

        Collection<Integer> keys = new ArrayList<>();

        for (int i = 1; i <= 10; i++)
            keys.add(i);

        // Read through.
        Map<Integer, String> vals = cache.getAll(keys);

        checkLastMethod("loadAll");

        assert vals != null;
        assert vals.size() == 10;

        for (int i = 1; i <= 10; i++) {
            String val = vals.get(i);

            assert val != null;
            assert val.equals(Integer.toString(i));
        }

        // Write through.
        cache.removeAll(keys);

        checkLastMethod("removeAll");

        assert cache.isEmpty();
        assert cache.isEmpty();

        assert map.isEmpty();
    }

    /** @throws Exception If test failed. */
    public void testLoadCache() throws Exception {
        GridCache<Integer, String> cache = cache();

        int cnt = 1;

        cache.loadCache(null, 0, cnt);

        checkLastMethod("loadAllFull");

        assert !cache.isEmpty();

        Map<Integer, String> map = cache.getAll(cache.keySet());

        assert map.size() == cnt : "Invalid map size: " + map.size();

        // Recheck last method to make sure
        // values were read from cache.
        checkLastMethod("loadAllFull");

        int start = store.getStart();

        for (int i = start; i < start + cnt; i++) {
            String val = map.get(i);

            assert val != null;
            assert val.equals(Integer.toString(i));
        }
    }

    /** @throws Exception If test failed. */
    public void testLoadCacheWithPredicate() throws Exception {
        GridCache<Integer, String> cache = cache();

        int cnt = 10;

        cache.loadCache(new P2<Integer, String>() {
            @Override public boolean apply(Integer key, String val) {
                // Accept only even numbers.
                return key % 2 == 0;
            }
        }, 0, cnt);

        checkLastMethod("loadAllFull");

        Map<Integer, String> map = cache.getAll(cache.keySet());

        assert map.size() == cnt / 2 : "Invalid map size: " + map.size();

        // Recheck last method to make sure
        // values were read from cache.
        checkLastMethod("loadAllFull");

        int start = store.getStart();

        for (int i = start; i < start + cnt; i++) {
            String val = map.get(i);

            if (i % 2 == 0) {
                assert val != null;
                assert val.equals(Integer.toString(i));
            }
            else
                assert val == null;
        }
    }

    /** @throws Exception If test failed. */
    public void testReloadCache() throws Exception {
        GridCache<Integer, String> cache = cache();

        cache.loadCache(null, 0, 0);

        assert cache.isEmpty();

        checkLastMethod("loadAllFull");

        for (int i = 1; i <= 10; i++) {
            cache.put(i, Integer.toString(i));

            checkLastMethod("put");
        }

        assert cache.size() == 10;

        cache.reloadAll();

        checkLastMethod("loadAll");

        assert cache.size() == 10;

        store.resetLastMethod();

        for (int i = 1; i <= 10; i++) {
            String val = cache.get(i);

            assert val != null;
            assert val.equals(Integer.toString(i));

            // Make sure that value is coming from cache, not from store.
            checkLastMethod(null);
        }

        cache.clearAll();

        cache.loadCache(new P2<Integer, String>() {
            @Override public boolean apply(Integer k, String v) {
                // Only accept even numbers.
                return k % 2 == 0;
            }
        }, 0, 10);

        checkLastMethod("loadAllFull");

        store.resetLastMethod();

        assertEquals(5, cache.size());

        cache.forEach(new CIX1<GridCacheEntry<Integer, String>>() {
            @Override public void applyx(GridCacheEntry<Integer, String> entry) throws GridException {
                String val = entry.get();

                assert val != null;
                assert val.equals(Integer.toString(entry.getKey()));
                assert entry.getKey() % 2 == 0;

                // Make sure that value is coming from cache, not from store.
                checkLastMethod(null);
            }
        });

        // Make sure that value is coming from cache, not from store.
        checkLastMethod(null);
    }

    /** @throws Exception If test failed. */
    public void testReloadAll() throws Exception {
        GridCache<Integer, String> cache = cache();

        assert cache.isEmpty();

        Map<Integer, String> vals = new HashMap<>();

        for (int i = 1; i <= 10; i++)
            vals.put(i, Integer.toString(i));

        cache.reloadAll(vals.keySet());

        assert cache.isEmpty() : "Cache is not empty: " + cache.values();

        checkLastMethod("loadAll");

        cache.putAll(vals);

        checkLastMethod("putAll");

        assert cache.size() == 10;

        cache.reloadAll(vals.keySet());

        checkLastMethod("loadAll");

        assert cache.size() == 10;

        store.resetLastMethod();

        for (int i = 1; i <= 10; i++) {
            String val = cache.get(i);

            assert val != null;
            assert val.equals(Integer.toString(i));

            // Make sure that value is coming from cache, not from store.
            checkLastMethod(null);
        }

        for (int i = 1; i <= 10; i++)
            store.put(null, i, "reloaded-" + i);

        cache.reloadAll(vals.keySet());

        checkLastMethod("loadAll");

        store.resetLastMethod();

        assert cache.size() == 10;

        for (int i = 1; i <= 10; i++) {
            String val = cache.get(i);

            assert val != null;
            assert val.equals("reloaded-" + i);

            // Make sure that value is coming from cache, not from store.
            checkLastMethod(null);
        }
    }

    /** @throws Exception If test failed. */
    @SuppressWarnings("StringEquality")
    public void testReload() throws Exception {
        GridCache<Integer, String> cache = cache();

        assert cache.isEmpty();

        Map<Integer, String> vals = new HashMap<>();

        for (int i = 1; i <= 10; i++)
            vals.put(i, Integer.toString(i));

        cache.reloadAll(vals.keySet());

        assert cache.isEmpty();

        checkLastMethod("loadAll");

        cache.putAll(vals);

        checkLastMethod("putAll");

        assert cache.size() == 10;

        String val = cache.reload(1);

        assert val != null;
        assert "1".equals(val);

        checkLastMethod("load");

        assert cache.size() == 10;

        store.resetLastMethod();

        for (int i = 1; i <= 10; i++) {
            val = cache.get(i);

            assert val != null;
            assert val.equals(Integer.toString(i));

            // Make sure that value is coming from cache, not from store.
            checkLastMethod(null);
        }

        for (int i = 1; i <= 10; i++)
            store.put(null, i, "reloaded-" + i);

        store.resetLastMethod();

        assert cache.size() == 10;

        for (int i = 1; i <= 10; i++) {
            val = cache.reload(i);

            checkLastMethod("load");

            assert val != null;
            assert val.equals("reloaded-" + i);

            store.resetLastMethod();

            String cached = cache.get(i);

            assert cached != null;

            assert cached == val : "Cached value mismatch [expected=" + val + ", cached=" + cached + ']';

            // Make sure that value is coming from cache, not from store.
            checkLastMethod(null);
        }
    }

    /** @param mtd Expected last method value. */
    private void checkLastMethod(@Nullable String mtd) {
        String lastMtd = store.getLastMethod();

        if (mtd == null)
            assert lastMtd == null : "Last method must be null: " + lastMtd;
        else {
            assert lastMtd != null : "Last method must be not null";
            assert lastMtd.equals(mtd) : "Last method does not match [expected=" + mtd + ", lastMtd=" + lastMtd + ']';
        }
    }
}
