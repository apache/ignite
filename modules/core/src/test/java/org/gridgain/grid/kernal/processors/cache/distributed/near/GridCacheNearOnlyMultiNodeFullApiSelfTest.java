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

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.events.IgniteEventType.*;
import static org.gridgain.grid.cache.GridCacheAtomicWriteOrderMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheUtils.*;

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
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cfg = super.cacheConfiguration(gridName);

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

            GridCacheAttributes[] nodeAttrs = node.attribute(GridNodeAttributes.ATTR_CACHE);

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
    protected GridCache<String, Integer> fullCache() {
        return nearIdx == 0 ? cache(1) : cache(0);
    }

    /**
     * Returns primary keys for node of the given cache,
     * handling possible near-only argument by selecting a single real cache instead.
     *
     * {@inheritDoc}
     */
    @Override protected List<String> primaryKeysForCache(GridCacheProjection<String, Integer> cache, int cnt)
        throws IgniteCheckedException {
        return cache.equals(cache()) ?
            super.primaryKeysForCache(fullCache(), cnt) : super.primaryKeysForCache(cache, cnt);
    }

    /** {@inheritDoc} */
    @Override public void testSize() throws Exception {
        GridCache<String, Integer> nearCache = cache();

        int size = 10;

        Map<String, Integer> map = U.newLinkedHashMap(size);

        for (int i = 0; i < size; i++)
            map.put("key" + i, i);

        nearCache.putAll(map);

        affinityNodes(); // Just to ack cache configuration to log..

        checkKeySize(map.keySet());

        checkSize(map.keySet());

        assertEquals("Primary keys found in near-only cache [" +
            "primaryEntries=" + nearCache.primaryEntrySet() + ", dht=" + dht(nearIdx).allEntries() + "]",
            0, nearCache.primarySize());

        int fullCacheSize = 0;

        for (int i = 0; i < gridCount(); i++)
            fullCacheSize += cache(i).primarySize();

        assertEquals("Invalid cache size", 10, fullCacheSize);
    }

    /** {@inheritDoc} */
    @Override public void testReload() throws Exception {
        // Not needed for near-only cache.
    }

    /** {@inheritDoc} */
    @Override public void testReloadAsync() throws Exception {
        // Not needed for near-only cache.
    }

    /** {@inheritDoc} */
    @Override public void testReloadFiltered() throws Exception {
        // Not needed for near-only cache.
    }

    /** {@inheritDoc} */
    @Override public void testReloadAsyncFiltered() throws Exception {
        // Not needed for near-only cache.
    }

    /** {@inheritDoc} */
    @Override public void testReloadAll() throws Exception {
        // Not needed for near-only cache.
    }

    /** {@inheritDoc} */
    @Override public void testReloadAllAsync() throws Exception {
        // Not needed for near-only cache.
    }

    /** {@inheritDoc} */
    @Override public void testReloadAllFiltered() throws Exception {
        // Not needed for near-only cache.
    }

    /** {@inheritDoc} */
    @Override public void testReloadAllAsyncFiltered() throws Exception {
        // Not needed for near-only cache.
    }

    /** {@inheritDoc} */
    @Override public void testNearDhtKeySize() throws Exception {
        // TODO fix this test for client mode.
    }

    /** {@inheritDoc} */
    @Override public void testClear() throws Exception {
        GridCache<String, Integer> nearCache = cache();
        GridCache<String, Integer> primary = fullCache();

        Collection<String> keys = primaryKeysForCache(primary, 3);

        info("Keys: " + keys);

        Map<String, Integer> vals = new HashMap<>(keys.size());

        int i = 0;

        for (String key : keys) {
            nearCache.put(key, i);

            vals.put(key, i);

            i++;
        }

        for (String key : keys)
            assertEquals(vals.get(key), nearCache.peek(key));

        nearCache.clearAll();

        for (String key : keys)
            assertNull(nearCache.peek(key));

        for (Map.Entry<String, Integer> entry : vals.entrySet())
            nearCache.put(entry.getKey(), entry.getValue());

        for (String key : keys)
            assertEquals(vals.get(key), nearCache.peek(key));

        String first = F.first(keys);

        assertTrue(nearCache.lock(first, 0L));

        nearCache.clearAll();

        assertEquals(vals.get(first), nearCache.peek(first));
        assertEquals(vals.get(first), primary.peek(first));

        nearCache.unlock(first);

        nearCache.projection(gte100).clear(first);

        assertEquals(vals.get(first), nearCache.peek(first));
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
                assertEquals(vals.get(key), nearCache.peek(key));
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

        assertEquals(vals.get(lastKey), nearCache.peek(lastKey));

        nearCache.clearAll();

        vals.put(lastKey, 102);

        nearCache.putAll(vals);

        for (String key : keys)
            nearCache.projection(gte100).clear(key);

        assertNull(nearCache.peek(lastKey));

        for (String key : subKeys)
            assertEquals(vals.get(key), nearCache.peek(key));
    }

    /** {@inheritDoc} */
    @Override public void testGlobalClearAll() throws Exception {
        // Save entries only on their primary nodes. If we didn't do so, clearAll() will not remove all entries
        // because some of them were blocked due to having readers.
        for (int i = 0; i < gridCount(); i++) {
            if (i != nearIdx)
                for (String key : primaryKeysForCache(cache(i), 3))
                    cache(i).put(key, 1);
        }

        cache().globalClearAll();

        for (int i = 0; i < gridCount(); i++)
            assertTrue(String.valueOf(cache(i).entrySet()), cache(i).isEmpty());
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

            GridCache<String, Integer> nearCache = cache();
            GridCache<String, Integer> cache = fullCache();

            String key = primaryKeysForCache(cache, 1).get(0);

            nearCache.put(key, 1);

            assert !nearCache.isLocked(key);
            assert !cache.isLocked(key);

            nearCache.lock(key, 0L);

            lockCnt.await();

            assert nearCache.isLocked(key);
            assert cache.isLocked(key);

            nearCache.unlock(key);

            unlockCnt.await();

            for (int i = 0; i < 100; i++) {
                if (cache.isLocked(key))
                    Thread.sleep(10);
                else
                    break;
            }

            assert !nearCache.isLocked(key);
            assert !cache.isLocked(key);
        }
    }

    /** {@inheritDoc} */
    @Override public void testPrimaryData() throws Exception {
        // Not needed for near-only cache.
    }
}
