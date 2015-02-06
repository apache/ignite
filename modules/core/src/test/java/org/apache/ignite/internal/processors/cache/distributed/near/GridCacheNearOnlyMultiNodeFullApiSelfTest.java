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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.events.IgniteEventType.*;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.*;

/**
 *
 */
public class GridCacheNearOnlyMultiNodeFullApiSelfTest extends GridCachePartitionedMultiNodeFullApiSelfTest {
    /** */
    private static AtomicInteger cnt;

    /** Index of the near-only instance. */
    protected Integer nearIdx;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cnt = new AtomicInteger();

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        if (cnt.getAndIncrement() == 0) {
            info("Use grid '" + gridName + "' as near-only.");

            cfg.setDistributionMode(NEAR_ONLY);
        }

        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setAtomicWriteOrderMode(PRIMARY);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        for (int i = 0; i < gridCount(); i++) {
            if (!isAffinityNode(cacheConfiguration(grid(i).configuration(), null))) {
                nearIdx = i;

                break;
            }
        }

        assert nearIdx != null : "Didn't find a near-only node.";

        dfltIgnite = grid(nearIdx);

        super.beforeTest(); // Doing initial asserts.
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> affinityNodes() {
        info("Near node ID: " + grid(nearIdx).localNode().id());

        for (int i = 0; i < gridCount(); i++) {
            ClusterNode node = grid(i).localNode();

            GridCacheAttributes[] nodeAttrs = node.attribute(IgniteNodeAttributes.ATTR_CACHE);

            info("Cache attribtues for node [nodeId=" + node.id() + ", attrs=" +
                Arrays.asList(nodeAttrs) + ']');
        }

        return F.view(super.affinityNodes(), new P1<ClusterNode>() {
            @Override public boolean apply(ClusterNode n) {
                return !F.eq(G.ignite(n.id()).name(), grid(nearIdx).name());
            }
        });
    }

    /**
     * @return For the purpose of this test returns the near-only instance.
     */
    @Override protected GridCache<String, Integer> cache() {
        return cache(nearIdx);
    }

    /**
     * @return A not near-only cache.
     */
    protected IgniteCache<String, Integer> fullCache() {
        return nearIdx == 0 ? jcache(1) : jcache(0);
    }

    /**
     * @return For the purpose of this test returns the near-only instance.
     */
    @Override protected IgniteCache<String, Integer> jcache() {
        return jcache(nearIdx);
    }

    /**
     * Returns primary keys for node of the given cache,
     * handling possible near-only argument by selecting a single real cache instead.
     *
     * {@inheritDoc}
     */
    @Override protected List<String> primaryKeysForCache(CacheProjection<String, Integer> cache, int cnt)
        throws IgniteCheckedException {
        return cache.equals(cache()) ?
            super.primaryKeysForCache(fullCache(), cnt) : super.primaryKeysForCache(cache, cnt);
    }

    /** {@inheritDoc} */
    @Override protected List<String> primaryKeysForCache(IgniteCache<String, Integer> cache, int cnt)
        throws IgniteCheckedException {
        if (cache.equals(jcache()))
            return super.primaryKeysForCache(fullCache(), cnt);

        return super.primaryKeysForCache(cache, cnt);
    }

    /** {@inheritDoc} */
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

        assertEquals(10, nearCache.localSize());

        int fullCacheSize = 0;

        for (int i = 0; i < gridCount(); i++)
            fullCacheSize += jcache(i).localSize();

        assertEquals("Invalid cache size", fullCacheSize, nearCache.size());
    }

    /** {@inheritDoc} */
    @Override public void testLoadAll() throws Exception {
        // Not needed for near-only cache.
    }

    /** {@inheritDoc} */
    @Override public void testNearDhtKeySize() throws Exception {
        // TODO fix this test for client mode.
    }

    /** {@inheritDoc} */
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
    @Override public void testGlobalClearAll() throws Exception {
        // Save entries only on their primary nodes. If we didn't do so, clearLocally() will not remove all entries
        // because some of them were blocked due to having readers.
        for (int i = 0; i < gridCount(); i++) {
            if (i != nearIdx)
                for (String key : primaryKeysForCache(jcache(i), 3))
                    jcache(i).put(key, 1);
        }

        jcache().clear();

        for (int i = 0; i < gridCount(); i++)
            assertTrue(String.valueOf(jcache(i)), jcache(i).localSize() == 0);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("BusyWait")
    @Override public void testLockUnlock() throws Exception {
        if (lockingEnabled()) {
            final CountDownLatch lockCnt = new CountDownLatch(1);
            final CountDownLatch unlockCnt = new CountDownLatch(1);

            grid(0).events().localListen(new IgnitePredicate<IgniteEvent>() {
                @Override public boolean apply(IgniteEvent evt) {
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

    /** {@inheritDoc} */
    @Override public void testPrimaryData() throws Exception {
        // Not needed for near-only cache.
    }
}
