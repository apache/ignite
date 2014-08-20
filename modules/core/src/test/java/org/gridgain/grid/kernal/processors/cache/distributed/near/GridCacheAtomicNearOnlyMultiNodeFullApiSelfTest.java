/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.ATOMIC;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 * Tests NEAR_ONLY cache.
 */
public class GridCacheAtomicNearOnlyMultiNodeFullApiSelfTest extends GridCacheNearOnlyMultiNodeFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cfg = super.cacheConfiguration(gridName);

        if (cfg.getDistributionMode() != NEAR_ONLY)
            cfg.setDistributionMode(PARTITIONED_ONLY);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean nearEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected boolean lockingEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean txEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return PARTITIONED_ONLY;
    }

    @Override protected void afterTest() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            grid(i).cache(null).removeAll();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override public void testClear() throws Exception {
        GridCache<String, Integer> nearCache = cache();
        GridCache<String, Integer> primary = fullCache();

        Collection<String> keys = primaryKeysForCache(primary, 3);

        Map<String, Integer> vals = new HashMap<>(keys.size());

        int i = 0;

        for (String key : keys) {
            nearCache.put(key, i);

            vals.put(key, i);

            i++;
        }

        i = 0;

        for (String key : keys)
            assertEquals((Integer)i++, nearCache.peek(key));

        nearCache.clearAll();

        for (String key : keys)
            assertNull(nearCache.peek(key));

        for (Map.Entry<String, Integer> entry : vals.entrySet())
            nearCache.put(entry.getKey(), entry.getValue());

        i = 0;

        for (String key : keys)
            assertEquals((Integer)i++, nearCache.peek(key));

        String first = F.first(keys);

        nearCache.projection(gte100).clear(first);

        assertEquals((Integer)0, nearCache.peek(first));
        assertEquals(vals.get(first), primary.peek(first));

        nearCache.put(first, 101);

        nearCache.projection(gte100).clear(first);

        assertNull(nearCache.peek(first));
        assertFalse(primary.isEmpty());

        i = 0;

        for (String key : keys) {
            nearCache.put(key, i);

            vals.put(key, i);

            i++;
        }

        nearCache.put(first, 101);
        vals.put(first, 101);

        nearCache.projection(gte100).clear(first);

        for (String key : keys)
            assertEquals(vals.get(key), primary.peek(key));

        for (String key : keys) {
            if (first.equals(key))
                assertNull(nearCache.peek(key));
            else
                assertEquals(vals.get(key), nearCache.peek(key));
        }
    }

    /** {@inheritDoc} */
    @Override public void testEvictExpired() throws Exception {
        GridCache<String, Integer> cache = cache();

        String key = primaryKeysForCache(cache, 1).get(0);

        cache.put(key, 1);

        assertEquals((Integer)1, cache.get(key));

        GridCacheEntry<String, Integer> entry = cache.entry(key);

        assert entry != null;

        long ttl = 500;

        entry.timeToLive(ttl);

        // Update is required for TTL to have effect.
        entry.set(1);

        Thread.sleep(ttl + 100);

        // Expired entry should not be swapped.
        assertTrue(cache.evict(key));

        assertNull(cache.peek(key));

        assertNull(cache.promote(key));

        assertNull(cache.peek(key));

        assertTrue(cache.isEmpty());

        // Force reload on primary node.
        for (int i = 0; i < gridCount(); i++) {
            if (cache(i).entry(key).primary())
                cache(i).reload(key);
        }

        // Will do near get request.
        cache.reload(key);

        assertEquals((Integer)1, cache.peek(key));
    }
}
