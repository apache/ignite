/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import javax.cache.expiry.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.apache.ignite.events.IgniteEventType.*;

/**
 *
 */
public class GridCacheAtomicClientOnlyMultiNodeFullApiSelfTest extends GridCacheNearOnlyMultiNodeFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cfg = super.cacheConfiguration(gridName);

        if (cfg.getDistributionMode() == NEAR_ONLY)
            cfg.setDistributionMode(CLIENT_ONLY);
        else
            cfg.setDistributionMode(PARTITIONED_ONLY);

        return cfg;
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

    /** {@inheritDoc} */
    @Override public void testSize() throws Exception {
        GridCache<String, Integer> cache = cache();

        int size = 10;

        Map<String, Integer> map = U.newLinkedHashMap(size);

        for (int i = 0; i < size; i++)
            map.put("key" + i, i);

        cache.putAll(map);

        affinityNodes(); // Just to ack cache configuration to log..

        checkKeySize(map.keySet());

        checkSize(map.keySet());

        assertEquals("Primary keys found in client-only cache [" +
            "primaryEntries=" + cache.primaryEntrySet() + ", dht=" + cache(nearIdx).entrySet() + "]",
            0, cache.primarySize());

        int fullCacheSize = 0;

        for (int i = 0; i < gridCount(); i++)
            fullCacheSize += cache(i).primarySize();

        assertEquals("Invalid cache size", 10, fullCacheSize);
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

        for (String key : keys)
            assertEquals(null, nearCache.peek(key));

        nearCache.clearAll();

        for (String key : keys)
            assertNull(nearCache.peek(key));

        for (Map.Entry<String, Integer> entry : vals.entrySet())
            nearCache.put(entry.getKey(), entry.getValue());

        for (String key : keys)
            assertEquals(null, nearCache.peek(key));

        String first = F.first(keys);

        nearCache.projection(gte100).clear(first);

        assertEquals(null, nearCache.peek(first));
        assertEquals(vals.get(first), primary.peek(first));

        nearCache.put(first, 101);

        nearCache.projection(gte100).clear(first);

        assertTrue(nearCache.isEmpty());
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
                assertEquals(null, nearCache.peek(key));
        }
    }

    /** {@inheritDoc} */
    @Override public void testClearKeys() throws Exception {
        GridCache<String, Integer> nearCache = cache();
        GridCache<String, Integer> primary = fullCache();

        Collection<String> keys = primaryKeysForCache(primary, 3);

        for (String key : keys)
            assertNull(nearCache.get(key));

        String lastKey = F.last(keys);

        Collection<String> subKeys = new ArrayList<>(keys);

        subKeys.remove(lastKey);

        Map<String, Integer> vals = new HashMap<>(keys.size());

        int i = 0;

        for (String key : keys)
            vals.put(key, i++);

        nearCache.putAll(vals);

        for (String subKey : subKeys)
            nearCache.clear(subKey);

        for (String key : subKeys) {
            assertNull(nearCache.peek(key));
            assertNotNull(primary.peek(key));
        }

        assertEquals(null, nearCache.peek(lastKey));

        nearCache.clearAll();

        vals.put(lastKey, 102);

        nearCache.putAll(vals);

        for (String key : keys)
            nearCache.projection(gte100).clear(key);

        assertNull(nearCache.peek(lastKey));

        for (String key : subKeys)
            assertEquals(null, nearCache.peek(key));
    }

    /** {@inheritDoc} */
    @Override public void testEvictExpired() throws Exception {
        GridCache<String, Integer> cache = cache();

        String key = primaryKeysForCache(cache, 1).get(0);

        cache.put(key, 1);

        assertEquals((Integer)1, cache.get(key));

        long ttl = 500;

        grid(0).jcache(null).
            withExpiryPolicy(new TouchedExpiryPolicy(new Duration(MILLISECONDS, ttl))).put(key, 1);

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

        assertEquals(null, cache.peek(key));
    }

    /** {@inheritDoc} */
    @Override public void testEvictAll() throws Exception {
        List<String> keys = primaryKeysForCache(cache(), 3);

        String key1 = keys.get(0);
        String key2 = keys.get(1);
        String key3 = keys.get(2);

        cache().put(key1, 1);
        cache().put(key2, 2);
        cache().put(key3, 3);

        assert cache().peek(key1) == null;
        assert cache().peek(key2) == null;
        assert cache().peek(key3) == null;

        cache().evictAll(F.asList(key1, key2));

        assert cache().peek(key1) == null;
        assert cache().peek(key2) == null;
        assert cache().peek(key3) == null;

        cache().reloadAll(F.asList(key1, key2));

        assert cache().peek(key1) == null;
        assert cache().peek(key2) == null;
        assert cache().peek(key3) == null;

        cache().evictAll(F.asList(key1, key2));

        assert cache().peek(key1) == null;
        assert cache().peek(key2) == null;
        assert cache().peek(key3) == null;

        cache().reloadAll(F.asList(key1, key2));

        assert cache().peek(key1) == null;
        assert cache().peek(key2) == null;
        assert cache().peek(key3) == null;

        cache().evictAll();

        assert cache().peek(key1) == null;
        assert cache().peek(key2) == null;
        assert cache().peek(key3) == null;

        cache().put(key1, 1);
        cache().put(key2, 102);
        cache().put(key3, 3);

        cache().projection(gte100).evictAll();

        assert cache().peek(key1) == null;
        assert cache().peek(key2) == null;
        assert cache().peek(key3) == null;

        cache().put(key1, 1);
        cache().put(key2, 102);
        cache().put(key3, 3);

        cache().projection(gte100).evictAll(F.asList(key1, key2, key3));

        assert cache().peek(key1) == null;
        assert cache().peek(key2) == null;
        assert cache().peek(key3) == null;
    }

    /** {@inheritDoc} */
    @Override public void testPeekExpired() throws Exception {
        GridCache<String, Integer> c = cache();

        String key = primaryKeysForCache(c, 1).get(0);

        info("Using key: " + key);

        c.put(key, 1);

        assertEquals(null, c.peek(key));

        long ttl = 500;

        grid(0).jcache(null).
            withExpiryPolicy(new TouchedExpiryPolicy(new Duration(MILLISECONDS, ttl))).put(key, 1);

        Thread.sleep(ttl + 100);

        assert c.peek(key) == null;

        assert c.isEmpty() : "Cache is not empty: " + c.values();
    }

    /** {@inheritDoc} */
    @Override public void testEvict() throws Exception {
        GridCache<String, Integer> cache = cache();

        List<String> keys = primaryKeysForCache(cache, 2);

        String key = keys.get(0);
        String key2 = keys.get(1);

        cache.put(key, 1);

        assertEquals((Integer)1, cache.get(key));

        assertTrue(cache.evict(key));

        assertNull(cache.peek(key));

        cache.reload(key);

        assertEquals(null, cache.peek(key));

        cache.remove(key);

        cache.put(key, 1);
        cache.put(key2, 102);

        assertTrue(cache.projection(gte100).evict(key));

        assertEquals((Integer)1, cache.get(key));

        assertTrue(cache.projection(gte100).evict(key2));

        assertNull(cache.peek(key2));

        assertTrue(cache.evict(key));

        assertNull(cache.peek(key));
    }

    /** {@inheritDoc} */
    @Override public void testUnswap() throws Exception {
        List<String> keys = primaryKeysForCache(cache(), 3);

        String k1 = keys.get(0);
        String k2 = keys.get(1);
        String k3 = keys.get(2);

        cache().put(k1, 1);
        cache().put(k2, 2);
        cache().put(k3, 3);

        final AtomicInteger swapEvts = new AtomicInteger(0);
        final AtomicInteger unswapEvts = new AtomicInteger(0);

        Collection<String> locKeys = new HashSet<>();

        for (int i = 0; i < gridCount(); i++) {
            grid(i).events().localListen(new IgnitePredicate<IgniteEvent>() {
                @Override public boolean apply(IgniteEvent evt) {
                    info("Received event: " + evt);

                    switch (evt.type()) {
                        case EVT_CACHE_OBJECT_SWAPPED:
                            swapEvts.incrementAndGet();

                            break;
                        case EVT_CACHE_OBJECT_UNSWAPPED:
                            unswapEvts.incrementAndGet();

                            break;
                    }

                    return true;
                }
            }, EVT_CACHE_OBJECT_SWAPPED, EVT_CACHE_OBJECT_UNSWAPPED);
        }

        assert cache().evict(k2);
        assert cache().evict(k3);

        assert !cache().containsKey(k1);
        assert !cache().containsKey(k2);
        assert !cache().containsKey(k3);

        int cnt = 0;

        if (locKeys.contains(k2)) {
            assertEquals((Integer)2, cache().promote(k2));

            cnt++;
        }
        else
            assertNull(cache().promote(k2));

        if (locKeys.contains(k3)) {
            assertEquals((Integer)3, cache().promote(k3));

            cnt++;
        }
        else
            assertNull(cache().promote(k3));

        assertEquals(cnt, swapEvts.get());
        assertEquals(cnt, unswapEvts.get());

        assert cache().evict(k1);

        assertEquals((Integer)1, cache().get(k1));

        if (locKeys.contains(k1))
            cnt++;

        assertEquals(cnt, swapEvts.get());
        assertEquals(cnt, unswapEvts.get());

        cache().clearAll();

        // Check with multiple arguments.
        cache().put(k1, 1);
        cache().put(k2, 2);
        cache().put(k3, 3);

        swapEvts.set(0);
        unswapEvts.set(0);

        cache().evict(k2);
        cache().evict(k3);

        assert !cache().containsKey(k1);
        assert !cache().containsKey(k2);
        assert !cache().containsKey(k3);

        cache().promoteAll(F.asList(k2, k3));

        cnt = 0;

        if (locKeys.contains(k2))
            cnt++;

        if (locKeys.contains(k3))
            cnt++;

        assertEquals(cnt, swapEvts.get());
        assertEquals(cnt, unswapEvts.get());
    }
}
