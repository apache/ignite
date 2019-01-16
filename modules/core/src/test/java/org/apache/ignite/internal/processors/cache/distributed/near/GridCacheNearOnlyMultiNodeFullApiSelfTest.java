/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.processors.cache.GatewayProtectedCacheProxy;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.util.lang.GridAbsPredicateX;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_LOCKED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_UNLOCKED;

/**
 *
 */
@RunWith(JUnit4.class)
public class GridCacheNearOnlyMultiNodeFullApiSelfTest extends GridCachePartitionedMultiNodeFullApiSelfTest {
    /** */
    private static AtomicInteger cnt;

    /** Index of the near-only instance. */
    protected Integer nearIdx;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cnt = new AtomicInteger();

        super.beforeTestsStarted();

        for (int i = 0; i < gridCount(); i++) {
            if (ignite(i).configuration().isClientMode()) {
                if (clientHasNearCache())
                    ignite(i).getOrCreateCache(defaultCacheConfiguration(), new NearCacheConfiguration<>());
                else
                    ignite(i).getOrCreateCache(DEFAULT_CACHE_NAME);

                break;
            }
        }
    }

    /**
     * @return If client node has near cache.
     */
    protected boolean clientHasNearCache() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (cnt.getAndIncrement() == 0 || (cnt.get() > gridCount() && cnt.get() % gridCount() == 0)) {
            info("Use grid '" + igniteInstanceName + "' as near-only.");

            cfg.setClientMode(true);

            cfg.setCacheConfiguration();
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(igniteInstanceName);

        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        for (int i = 0; i < gridCount(); i++) {
            if (ignite(i).configuration().isClientMode()) {
                nearIdx = i;

                break;
            }
        }

        assert nearIdx != null : "Didn't find a near-only node.";

        dfltIgnite = grid(nearIdx);

        info("Near-only node: " + dfltIgnite.cluster().localNode().id());

        super.beforeTest(); // Doing initial asserts.
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> affinityNodes() {
        info("Near node ID: " + grid(nearIdx).localNode().id());

        return F.view(super.affinityNodes(), new P1<ClusterNode>() {
            @Override public boolean apply(ClusterNode n) {
                return !F.eq(grid(n).name(), grid(nearIdx).name());
            }
        });
    }

    /**
     * @return A not near-only cache.
     */
    @Override protected IgniteCache<String, Integer> fullCache() {
        return nearIdx == 0 ? jcache(1) : jcache(0);
    }

    /**
     * @return For the purpose of this test returns the near-only instance.
     */
    @Override protected IgniteCache<String, Integer> jcache() {
        return jcache(nearIdx);
    }

    /** {@inheritDoc} */
    @Override protected IgniteTransactions transactions() {
        return grid(nearIdx).transactions();
    }

    /** {@inheritDoc} */
    @Override protected List<String> primaryKeysForCache(IgniteCache<String, Integer> cache, int cnt)
        throws IgniteCheckedException {
        if (cache instanceof GatewayProtectedCacheProxy &&
            ((GatewayProtectedCacheProxy) cache).internalProxy().delegate().equals(
                ((GatewayProtectedCacheProxy) jcache()).internalProxy().delegate()))
            return super.primaryKeysForCache(fullCache(), cnt);

        return super.primaryKeysForCache(cache, cnt);
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testSize() throws Exception {
        IgniteCache<String, Integer> nearCache = jcache();

        int size = 10;

        Map<String, Integer> map = U.newLinkedHashMap(size);

        for (int i = 0; i < size; i++)
            map.put("key" + i, i);

        nearCache.putAll(map);

        affinityNodes(); // Just to ack cache configuration to log..

        checkKeySize(map.keySet());

        checkSize(map.keySet());

        assertEquals(10, nearCache.localSize(CachePeekMode.ALL));

        int fullCacheSize = 0;

        for (int i = 0; i < gridCount(); i++)
            fullCacheSize += jcache(i).localSize();

        assertEquals("Invalid cache size", fullCacheSize, nearCache.size());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLoadAll() throws Exception {
        // Not needed for near-only cache.
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReaderTtlTx() throws Exception {
        // IgniteProcessProxy#transactions is not implemented.
        if (isMultiJvm() || !txShouldBeUsed())
            return;

        checkReaderTtl(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReaderTtlNoTx() throws Exception {
        checkReaderTtl(false);
    }

    /**
     * @param cache Cache.
     * @param key Entry key.
     * @return Pair [ttl, expireTime] for near cache entry; both values null if entry not found
     */
    protected IgnitePair<Long> nearEntryTtl(IgniteCache cache, String key) {
        return executeOnLocalOrRemoteJvm(cache, new EntryTtlTask(key, false));
    }

    /**
     * @param inTx If {@code true} starts explicit transaction.
     * @throws Exception If failed.
     */
    private void checkReaderTtl(boolean inTx) throws Exception {
        int ttl = 1000;

        final ExpiryPolicy expiry = new TouchedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, ttl));

        final IgniteCache<String, Integer> c = jcache();

        final String key = primaryKeysForCache(fullCache(), 1).get(0);

        fullCache().put(key, 1);

        info("Finished first put.");

        {
            IgnitePair<Long> entryTtl = entryTtl(fullCache(), key);

            assertEquals((Integer)1, c.get(key));

            assertNotNull(entryTtl.get1());
            assertNotNull(entryTtl.get2());
            assertEquals(0, (long)entryTtl.get1());
            assertEquals(0, (long)entryTtl.get2());
        }

        long startTime = System.currentTimeMillis();

        int fullIdx = nearIdx == 0 ? 1 : 0;

        // Now commit transaction and check that ttl and expire time have been saved.
        Transaction tx = inTx ? grid(fullIdx).transactions().txStart() : null;

        try {
            jcache(fullIdx).withExpiryPolicy(expiry).put(key, 1);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        jcache(nearIdx).get(key); // Create entry on near node.

        long[] expireTimes = new long[gridCount()];

        for (int i = 0; i < gridCount(); i++) {
            info("Checking grid: " + grid(i).localNode().id());

            IgnitePair<Long> entryTtl = null;

            if (grid(i).affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(grid(i).localNode(), key))
                entryTtl = entryTtl(jcache(i), key);
            else if (i == nearIdx)
                entryTtl = nearEntryTtl(jcache(i), key);

            if (entryTtl != null) {
                assertNotNull(entryTtl.get1());
                assertNotNull(entryTtl.get2());
                assertTrue("Invalid expire time [expire=" + entryTtl.get2() + ", start=" + startTime + ']',
                    entryTtl.get2() > startTime);
                expireTimes[i] = entryTtl.get2();
            }
        }

        // One more update from the same cache entry to ensure that expire time is shifted forward.
        U.sleep(100);

        tx = inTx ? grid(fullIdx).transactions().txStart() : null;

        try {
            jcache(fullIdx).withExpiryPolicy(expiry).put(key, 2);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        for (int i = 0; i < gridCount(); i++) {
            IgnitePair<Long> entryTtl = null;

            if (grid(i).affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(grid(i).localNode(), key))
                entryTtl = entryTtl(jcache(i), key);
            else if (i == nearIdx)
                entryTtl = nearEntryTtl(jcache(i), key);

            if (entryTtl != null) {
                assertNotNull(entryTtl.get1());
                assertNotNull(entryTtl.get2());
                assertTrue(entryTtl.get2() > startTime);
                expireTimes[i] = entryTtl.get2();
            }
        }

        // And one more update to ensure that ttl is not changed and expire time is not shifted forward.
        U.sleep(100);

        tx = inTx ? grid(fullIdx).transactions().txStart() : null;

        try {
            jcache(fullIdx).put(key, 4);
        }
        finally {
            if (tx != null)
                tx.commit();
        }

        for (int i = 0; i < gridCount(); i++) {
            IgnitePair<Long> entryTtl = null;

            if (grid(i).affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(grid(i).localNode(), key))
                entryTtl = entryTtl(jcache(i), key);
            else if (i == nearIdx)
                entryTtl = nearEntryTtl(jcache(i), key);

            if (entryTtl != null) {
                assertNotNull(entryTtl.get1());
                assertNotNull(entryTtl.get2());
                assertEquals(expireTimes[i], (long)entryTtl.get2());
            }
        }

        // Avoid reloading from store.
        storeStgy.removeFromStore(key);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                try {
                    Integer val = c.get(key);

                    if (val != null) {
                        info("Value is in cache [key=" + key + ", val=" + val + ']');

                        return false;
                    }

                    if (!internalCache(c).context().deferredDelete()) {
                        GridCacheEntryEx e0 = internalCache(c).peekEx(key);

                        return e0 == null || (e0.rawGet() == null && e0.valueBytes() == null);
                    }
                    else
                        return true;
                }
                catch (GridCacheEntryRemovedException ignored) {
                    // If e0.valueBytes() thrown this exception then entry has been removed.
                    return true;
                }
            }
        }, Math.min(ttl * 10, getTestTimeout())));

        // Ensure that old TTL and expire time are not longer "visible".
        {
            IgnitePair<Long> entryTtl = entryTtl(fullCache(), key);

            assertNotNull(entryTtl.get1());
            assertNotNull(entryTtl.get2());
            assertEquals(0, (long)entryTtl.get1());
            assertEquals(0, (long)entryTtl.get2());
        }

        // Ensure that next update will not pick old expire time.
        tx = inTx ? transactions().txStart() : null;

        try {
            c.put(key, 10);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        U.sleep(2000);

        {
            IgnitePair<Long> entryTtl = entryTtl(fullCache(), key);

            assertNotNull(entryTtl.get1());
            assertNotNull(entryTtl.get2());
            assertEquals(0, (long)entryTtl.get1());
            assertEquals(0, (long)entryTtl.get2());
        }
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testClear() throws Exception {
        IgniteCache<String, Integer> nearCache = jcache();
        IgniteCache<String, Integer> primary = fullCache();

        Collection<String> keys = primaryKeysForCache(primary, 3);

        info("Keys: " + keys);

        Map<String, Integer> vals = new HashMap<>();

        int i = 0;

        for (String key : keys) {
            nearCache.put(key, i);

            vals.put(key, i);

            i++;
        }

        for (String key : keys)
            assertEquals(vals.get(key), nearCache.localPeek(key, CachePeekMode.ONHEAP));

        nearCache.clear();

        for (String key : keys)
            assertNull(nearCache.localPeek(key, CachePeekMode.ONHEAP));

        for (Map.Entry<String, Integer> entry : vals.entrySet())
            nearCache.put(entry.getKey(), entry.getValue());

        for (String key : keys)
            assertEquals(vals.get(key), nearCache.localPeek(key, CachePeekMode.ONHEAP));

        String first = F.first(keys);

        Lock lock = nearCache.lock(first);

        lock.lock();

        try {
            nearCache.clear();

            assertEquals(vals.get(first), nearCache.localPeek(first, CachePeekMode.ONHEAP));
            assertEquals(vals.get(first), primary.localPeek(first, CachePeekMode.ONHEAP));
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLocalClearKeys() throws Exception {
        IgniteCache<String, Integer> nearCache = jcache();
        IgniteCache<String, Integer> primary = fullCache();

        Collection<String> keys = primaryKeysForCache(primary, 3);

        int i = 0;

        for (String key : keys)
            nearCache.put(key, i++);

        String lastKey = F.last(keys);

        Set<String> keysToRmv = new HashSet<>(keys);

        keysToRmv.remove(lastKey);

        assert keysToRmv.size() > 1;

        nearCache.localClearAll(keysToRmv);

        for (String key : keys) {
            boolean found = nearCache.localPeek(key, CachePeekMode.ONHEAP) != null;

            if (keysToRmv.contains(key))
                assertFalse("Found removed key " + key, found);
            else
                assertTrue("Not found key " + key, found);
        }
    }

    /**
     * @param async If {@code true} uses async method.
     * @throws Exception If failed.
     */
    @Override protected void globalClearAll(boolean async, boolean oldAsync) throws Exception {
        // Save entries only on their primary nodes. If we didn't do so, clearLocally() will not remove all entries
        // because some of them were blocked due to having readers.
        for (int i = 0; i < gridCount(); i++) {
            if (i != nearIdx) {
                for (String key : primaryKeysForCache(jcache(i), 3, 100_000))
                    jcache(i).put(key, 1);
            }
        }

        if (async) {
            if (oldAsync) {
                IgniteCache<String, Integer> asyncCache = jcache(nearIdx).withAsync();

                asyncCache.clear();

                asyncCache.future().get();
            } else
                jcache(nearIdx).clearAsync().get();
        }
        else
            jcache(nearIdx).clear();

        for (int i = 0; i < gridCount(); i++) {
            assertEquals("Unexpected size [node=" + ignite(i).name() + ", nearIdx=" + nearIdx + ']',
                0,
                jcache(i).localSize());
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("BusyWait")
    @Test
    @Override public void testLockUnlock() throws Exception {
        if (lockingEnabled()) {
            final CountDownLatch lockCnt = new CountDownLatch(1);
            final CountDownLatch unlockCnt = new CountDownLatch(1);

            grid(0).events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    switch (evt.type()) {
                        case EVT_CACHE_OBJECT_LOCKED:
                            lockCnt.countDown();

                            break;
                        case EVT_CACHE_OBJECT_UNLOCKED:
                            unlockCnt.countDown();

                            break;
                    }

                    return true;
                }
            }, EVT_CACHE_OBJECT_LOCKED, EVT_CACHE_OBJECT_UNLOCKED);

            IgniteCache<String, Integer> nearCache = jcache();
            IgniteCache<String, Integer> cache = fullCache();

            String key = primaryKeysForCache(cache, 1).get(0);

            nearCache.put(key, 1);

            assert !nearCache.isLocalLocked(key, false);
            assert !cache.isLocalLocked(key, false);

            Lock lock = nearCache.lock(key);

            lock.lock();

            try {
                lockCnt.await();

                assert nearCache.isLocalLocked(key, false);
                assert cache.isLocalLocked(key, false);
            }
            finally {
                lock.unlock();
            }

            unlockCnt.await();

            for (int i = 0; i < 100; i++) {
                if (cache.isLocalLocked(key, false))
                    Thread.sleep(10);
                else
                    break;
            }

            assert !nearCache.isLocalLocked(key, false);
            assert !cache.isLocalLocked(key, false);
        }
    }
}
