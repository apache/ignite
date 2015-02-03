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

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CachePreloadMode.*;
import static org.apache.ignite.cache.GridCachePeekMode.*;
import static org.apache.ignite.events.IgniteEventType.*;

/**
 * Multi-node tests for partitioned cache.
 */
public class GridCachePartitionedMultiNodeFullApiSelfTest extends GridCachePartitionedFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cc = super.cacheConfiguration(gridName);

        cc.setPreloadMode(SYNC);

        return cc;
    }

    /**
     * @return Affinity nodes for this cache.
     */
    public Collection<ClusterNode> affinityNodes() {
        return grid(0).nodes();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllRemoveAll() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            info(">>>>> Grid" + i + ": " + grid(i).localNode().id());

        Map<Integer, Integer> putMap = new LinkedHashMap<>();

        int size = 100;

        for (int i = 0; i < size; i++)
            putMap.put(i, i * i);

        GridCache<Object, Object> prj0 = grid(0).cache(null);
        GridCache<Object, Object> prj1 = grid(1).cache(null);

        prj0.putAll(putMap);

        prj1.removeAll(putMap.keySet());

        for (int i = 0; i < size; i++) {
            assertNull(prj0.get(i));
            assertNull(prj1.get(i));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllPutAll() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            info(">>>>> Grid" + i + ": " + grid(i).localNode().id());

        Map<Integer, Integer> putMap = new LinkedHashMap<>();

        int size = 100;

        for (int i = 0; i < size; i++)
            putMap.put(i, i);

        GridCache<Object, Object> prj0 = grid(0).cache(null);
        GridCache<Object, Object> prj1 = grid(1).cache(null);

        prj0.putAll(putMap);

        for (int i = 0; i < size; i++) {
            assertEquals(i, prj0.get(i));
            assertEquals(i, prj1.get(i));
        }

        for (int i = 0; i < size; i++)
            putMap.put(i, i * i);

        info(">>> Before second put.");

        prj1.putAll(putMap);

        info(">>> After second put.");

        for (int i = 0; i < size; i++) {
            assertEquals(i * i, prj0.get(i));
            assertEquals(i * i, prj1.get(i));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutDebug() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            info(">>>>> Grid" + i + ": " + grid(i).localNode().id());

        int size = 10;

        GridCache<Object, Object> prj0 = grid(0).cache(null);

        for (int i = 0; i < size; i++) {
            info("Putting value [i=" + i + ']');

            assertNull(prj0.put(i, i));

            info("Finished putting value [i=" + i + ']');
        }

        for (int i = 0; i < gridCount(); i++) {
            assertEquals(0, context(i).tm().idMapSize());

            GridCache<Object, Object> cache = grid(i).cache(null);
            ClusterNode node = grid(i).localNode();

            for (int k = 0; k < size; k++) {
                if (cache.affinity().isPrimaryOrBackup(node, k))
                    assertEquals("Check failed for node: " + node.id(), k, cache.peek(k));
            }
        }

        for (int i = 0; i < size; i++) {
            info("Putting value 2 [i=" + i + ']');

            assertEquals(i, prj0.putIfAbsent(i, i * i));

            info("Finished putting value 2 [i=" + i + ']');
        }

        for (int i = 0; i < size; i++)
            assertEquals(i, prj0.get(i));
    }

    /**
     * @throws Exception If failed.
     */
    public void testUnswapShort() throws Exception {
        final AtomicInteger swapEvts = new AtomicInteger(0);
        final AtomicInteger unswapEvts = new AtomicInteger(0);

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

        jcache().put("key", 1);

        for (int i = 0; i < gridCount(); i++) {
            CacheEntry<String, Integer> e = cache(i).entry("key");

            if (e.backup()) {
                assert cache(i).evict("key") : "Entry was not evicted [idx=" + i + ", entry=" +
                    (nearEnabled() ? dht(i).entryEx("key") : colocated(i).entryEx("key")) + ']';

                assert cache(i).peek("key") == null;

                assert cache(i).get("key") == 1;

                assert swapEvts.get() == 1 : "Swap events: " + swapEvts.get();

                assert unswapEvts.get() == 1 : "Unswap events: " + unswapEvts.get();

                break;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPeekPartitionedModes() throws Exception {
        jcache().put("key", 1);

        for (int i = 0; i < gridCount(); i++) {
            boolean nearEnabled = nearEnabled(cache(i));

            Integer nearPeekVal = nearEnabled ? 1 : null;

            GridCache<String, Integer> c = cache(i);

            CacheEntry<String, Integer> e = c.entry("key");

            if (e.backup()) {
                assertNull("NEAR_ONLY for cache: " + i, e.peek(F.asList(NEAR_ONLY)));
                assertEquals((Integer)1, e.peek(F.asList(PARTITIONED_ONLY)));

                assertNull(c.peek("key", F.asList(NEAR_ONLY)));

                assertEquals((Integer)1, c.peek("key", F.asList(PARTITIONED_ONLY)));
            }
            else if (!e.primary() && !e.backup()) {
                assertEquals((Integer)1, e.get());

                assertEquals(nearPeekVal, e.peek(Arrays.asList(NEAR_ONLY)));

                assert e.peek(Arrays.asList(PARTITIONED_ONLY)) == null;

                assertEquals(nearPeekVal, c.peek("key", Arrays.asList(NEAR_ONLY)));

                assert c.peek("key", Arrays.asList(PARTITIONED_ONLY)) == null;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPeekAsyncPartitionedModes() throws Exception {
        jcache().put("key", 1);

        for (int i = 0; i < gridCount(); i++) {
            boolean nearEnabled = nearEnabled(cache(i));

            Integer nearPeekVal = nearEnabled ? 1 : null;

            GridCache<String, Integer> c = cache(i);

            CacheEntry<String, Integer> e = c.entry("key");

            if (e.backup()) {
                assert e.peek(F.asList(NEAR_ONLY)) == null;

                assert e.peek(Arrays.asList(PARTITIONED_ONLY)) == 1;

                assert c.peek("key", Arrays.asList(NEAR_ONLY)) == null;

                assert c.peek("key", Arrays.asList(PARTITIONED_ONLY)) == 1;
            }
            else if (!e.primary() && !e.backup()) {
                assert e.get() == 1;

                assertEquals(nearPeekVal, e.peek(Arrays.asList(NEAR_ONLY)));

                assert e.peek(Arrays.asList(PARTITIONED_ONLY)) == null;

                assertEquals(nearPeekVal, c.peek("key", Arrays.asList(NEAR_ONLY)));

                assert c.peek("key", Arrays.asList(PARTITIONED_ONLY)) == null;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testNearDhtKeySize() throws Exception {
        List<String> keys = new ArrayList<>(5);

        info("Generating keys for test...");

        GridCache<String, Integer> cache0 = cache(0);

        for (int i = 0; i < 5; i++) {
            while (true) {
                String key = UUID.randomUUID().toString();

                if (ignite(0).affinity(null).isPrimary(grid(0).localNode(), key) &&
                    ignite(0).affinity(null).isBackup(grid(1).localNode(), key)) {
                    keys.add(key);

                    assertTrue(cache0.putx(key, i));

                    break;
                }
            }
        }

        info("Finished generating keys for test.");

        GridCache<String, Integer> cache2 = cache(2);

        assertEquals(Integer.valueOf(0), cache2.get(keys.get(0)));
        assertEquals(Integer.valueOf(1), cache2.get(keys.get(1)));

        assertEquals(0, cache0.nearSize());
        assertEquals(5, cache0.size() - cache0.nearSize());

        GridCache<String, Integer> cache1 = cache(1);

        assertEquals(0, cache1.nearSize());
        assertEquals(5, cache1.size() - cache1.nearSize());

        assertEquals(nearEnabled() ? 2 : 0, cache2.nearSize());
        assertEquals(0, cache2.size() - cache2.nearSize());

        IgniteBiPredicate<String, Integer> prjFilter = new P2<String, Integer>() {
            @Override public boolean apply(String key, Integer val) {
                return val >= 1 && val <= 3;
            }
        };

        assertEquals(0, cache0.projection(prjFilter).nearSize());
        assertEquals(3, cache0.projection(prjFilter).size() - cache0.projection(prjFilter).nearSize());

        assertEquals(0, cache1.projection(prjFilter).nearSize());
        assertEquals(3, cache1.projection(prjFilter).size() - cache1.projection(prjFilter).nearSize());

        assertEquals(nearEnabled() ? 1 : 0, cache2.projection(prjFilter).nearSize());
        assertEquals(0, cache2.projection(prjFilter).size() - cache2.projection(prjFilter).nearSize());
    }

    /** {@inheritDoc} */
    @Override public void testLockAsyncWithTimeoutEntry() throws Exception {
        // No-op, since all cases are tested separately.
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinity() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            info("Grid " + i + ": " + grid(i).localNode().id());

        final Object affKey = new Object() {
            @Override public boolean equals(Object obj) {
                return obj == this;
            }

            @Override public int hashCode() {
                return 1;
            }
        };

        Object key = new Object() {
            /** */
            @SuppressWarnings("UnusedDeclaration")
            @CacheAffinityKeyMapped
            private final Object key0 = affKey;

            @Override public boolean equals(Object obj) {
                return obj == this;
            }

            @Override public int hashCode() {
                return 2;
            }
        };

        info("All affinity nodes: " + affinityNodes());

        GridCache<Object, Object> cache = grid(0).cache(null);

        info("Cache affinity nodes: " + cache.affinity().mapKeyToPrimaryAndBackups(key));

        CacheAffinity<Object> aff = cache.affinity();

        Collection<ClusterNode> nodes = aff.mapKeyToPrimaryAndBackups(key);

        info("Got nodes from affinity: " + nodes);

        assertEquals(cacheMode() == PARTITIONED ? 2 : affinityNodes().size(), nodes.size());

        ClusterNode primary = F.first(nodes);
        ClusterNode backup = F.last(nodes);

        assertNotSame(primary, backup);

        ClusterNode other = null;

        for (int i = 0; i < gridCount(); i++) {
            ClusterNode node = grid(i).localNode();

            if (!node.equals(primary) && !node.equals(backup)) {
                other = node;

                break;
            }
        }

        assertNotSame(other, primary);
        assertNotSame(other, backup);

        assertNotNull(primary);
        assertNotNull(backup);
        assertNotNull(other);

        assertTrue(cache.affinity().isPrimary(primary, key));
        assertFalse(cache.affinity().isBackup(primary, key));
        assertTrue(cache.affinity().isPrimaryOrBackup(primary, key));

        assertFalse(cache.affinity().isPrimary(backup, key));
        assertTrue(cache.affinity().isBackup(backup, key));
        assertTrue(cache.affinity().isPrimaryOrBackup(backup, key));

        assertFalse(cache.affinity().isPrimary(other, key));

        if (cacheMode() == PARTITIONED) {
            assertFalse(cache.affinity().isBackup(other, key));
            assertFalse(cache.affinity().isPrimaryOrBackup(other, key));
        }
    }
}
