/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.local;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Tests for local cache.
 */
public class GridCacheLocalFullApiSelfTest extends GridCacheAbstractFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return LOCAL;
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration c = super.getConfiguration(gridName);

        c.getTransactionsConfiguration().setTxSerializableEnabled(true);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setSwapEnabled(true);

        return cfg;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testMapKeysToNodes() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 2);

        Map<GridNode, Collection<String>> map = cache().affinity().mapKeysToNodes(F.asList("key1", "key2"));

        assert map.size() == 1;

        Collection<String> keys = map.get(dfltIgnite.cluster().localNode());

        assert keys != null;
        assert keys.size() == 2;

        for (String key : keys)
            assert "key1".equals(key) || "key2".equals(key);

        map = cache().affinity().mapKeysToNodes(F.asList("key1", "key2"));

        assert map.size() == 1;

        keys = map.get(dfltIgnite.cluster().localNode());

        assert keys != null;
        assert keys.size() == 2;

        for (String key : keys)
            assert "key1".equals(key) || "key2".equals(key);
    }

    /**
     * Based on issue GG-2864
     *
     * @throws Exception In case of error.
     */
    public void testFilteredKeySet() throws Exception {
        if (!txEnabled() || portableEnabled())
            return;

        final GridCache<String, Integer> myCache = cache();

        final AtomicLong cntr = new AtomicLong();

        // Some counter.
        myCache.dataStructures().atomicLong("some_counter", 0L, true).incrementAndGet();

        // I would like to filter from key set all entities which key name is not started with "a_".
        GridPredicate<GridCacheEntry<String, Integer>> aPred = new GridPredicate<GridCacheEntry<String, Integer>>() {
            @Override public boolean apply(GridCacheEntry<String, Integer> entry) {
                cntr.incrementAndGet();

                assert entry.getKey() instanceof String;

                return entry.getKey().startsWith("a_");
            }
        };

        Set<String> aKeySet = myCache.projection(aPred).keySet();

        aKeySet.size(); // Initiate lazy iteration.

        assertEquals(0, cntr.get());

        // Key set is empty as expected - no entities in cache except atomic counter !!!
        assertTrue(aKeySet.isEmpty());

        // Add some entities to cache.
        myCache.putx("a_1", 1);
        myCache.putx("a_2", 2);
        myCache.putx("b_1", 3);

        // Repeat key set filtering.
        aKeySet = myCache.projection(aPred).keySet();

        // This will cause iteration and counter will get incremented.
        assertEquals(2, aKeySet.size());

        assertEquals(3, cntr.get());
        assertTrue(aKeySet.containsAll(F.asList("a_1", "a_2")));
    }
}
