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
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;

import javax.cache.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CachePeekMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.events.EventType.*;

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

        cc.setRebalanceMode(SYNC);

        return cc;
    }

    /**
     * @return Affinity nodes for this cache.
     */
    public Collection<ClusterNode> affinityNodes() {
        return grid(0).cluster().nodes();
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

        IgniteCache<Object, Object> c0 = grid(0).cache(null);
        IgniteCache<Object, Object> c1 = grid(1).cache(null);

        c0.putAll(putMap);

        atomicClockModeDelay(c0);

        c1.removeAll(putMap.keySet());

        for (int i = 0; i < size; i++) {
            assertNull(c0.get(i));
            assertNull(c1.get(i));
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

        IgniteCache<Object, Object> prj0 = grid(0).cache(null);
        IgniteCache<Object, Object> prj1 = grid(1).cache(null);

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

        IgniteCache<Object, Object> prj0 = grid(0).cache(null);

        for (int i = 0; i < size; i++) {
            info("Putting value [i=" + i + ']');

            prj0.put(i, i);

            info("Finished putting value [i=" + i + ']');
        }

        for (int i = 0; i < gridCount(); i++) {
            assertEquals(0, context(i).tm().idMapSize());

            IgniteCache<Object, Object> cache = grid(i).cache(null);
            ClusterNode node = grid(i).localNode();

            for (int k = 0; k < size; k++) {
                if (affinity(cache).isPrimaryOrBackup(node, k))
                    assertEquals("Check failed for node: " + node.id(), k, cache.localPeek(k, ONHEAP));
            }
        }

        for (int i = 0; i < size; i++) {
            info("Putting value 2 [i=" + i + ']');

            assertEquals(i, prj0.getAndPutIfAbsent(i, i * i));

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
            grid(i).events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
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
            if (grid(i).affinity(null).isBackup(grid(i).localNode(), "key")) {
                jcache(i).localEvict(Collections.singleton("key"));

                assert jcache(i).localPeek("key", ONHEAP) == null;

                assert jcache(i).get("key") == 1;

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
            IgniteCache<String, Integer> c = jcache(i);

            assertEquals((Integer)1, c.get("key"));

            boolean nearEnabled = nearEnabled(c);

            if (nearEnabled)
                assertTrue(((IgniteKernal)ignite(i)).internalCache().context().isNear());

            Integer nearPeekVal = nearEnabled ? 1 : null;

            Affinity<Object> aff = ignite(i).affinity(null);

            info("Affinity nodes [nodes=" + F.nodeIds(aff.mapKeyToPrimaryAndBackups("key")) +
                ", locNode=" + ignite(i).cluster().localNode().id() + ']');

            if (aff.isBackup(grid(i).localNode(), "key")) {
                assertNull(c.localPeek("key", NEAR));

                assertEquals((Integer)1, c.localPeek("key", BACKUP));
            }
            else if (!aff.isPrimaryOrBackup(grid(i).localNode(), "key")) {
                // Initialize near reader.
                assertEquals((Integer)1, c.get("key"));

                assertEquals("Failed to validate near value for node: " + i, nearPeekVal, c.localPeek("key", NEAR));

                assertNull(c.localPeek("key", PRIMARY, BACKUP));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPeekAsyncPartitionedModes() throws Exception {
        jcache().put("key", 1);

        for (int i = 0; i < gridCount(); i++) {
            boolean nearEnabled = nearEnabled(jcache(i));

            Integer nearPeekVal = nearEnabled ? 1 : null;

            IgniteCache<String, Integer> c = jcache(i);

            if (grid(i).affinity(null).isBackup(grid(i).localNode(), "key")) {
                assert c.localPeek("key", NEAR) == null;

                assert c.localPeek("key", PRIMARY, BACKUP) == 1;
            }
            else if (!grid(i).affinity(null).isPrimaryOrBackup(grid(i).localNode(), "key")) {
                // Initialize near reader.
                assertEquals((Integer)1, jcache(i).get("key"));

                assertEquals(nearPeekVal, c.localPeek("key", NEAR));

                assert c.localPeek("key", PRIMARY, BACKUP) == null;
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

        IgniteCache<String, Integer> cache0 = jcache(0);

        for (int i = 0; i < 5; i++) {
            while (true) {
                String key = UUID.randomUUID().toString();

                if (ignite(0).affinity(null).isPrimary(grid(0).localNode(), key) &&
                    ignite(0).affinity(null).isBackup(grid(1).localNode(), key)) {
                    keys.add(key);

                    cache0.put(key, i);

                    break;
                }
            }
        }

        info("Finished generating keys for test.");

        IgniteCache<String, Integer> cache2 = jcache(2);

        assertEquals(Integer.valueOf(0), cache2.get(keys.get(0)));
        assertEquals(Integer.valueOf(1), cache2.get(keys.get(1)));

        assertEquals(0, cache0.localSize(NEAR));
        assertEquals(5, cache0.localSize(CachePeekMode.ALL) - cache0.localSize(NEAR));

        IgniteCache<String, Integer> cache1 = jcache(1);

        assertEquals(0, cache1.localSize(NEAR));
        assertEquals(5, cache1.localSize(CachePeekMode.ALL) - cache1.localSize(NEAR));

        assertEquals(nearEnabled() ? 2 : 0, cache2.localSize(NEAR));
        assertEquals(0, cache2.localSize(CachePeekMode.ALL) - cache2.localSize(NEAR));
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
            @AffinityKeyMapped
            private final Object key0 = affKey;

            @Override public boolean equals(Object obj) {
                return obj == this;
            }

            @Override public int hashCode() {
                return 2;
            }
        };

        info("All affinity nodes: " + affinityNodes());

        IgniteCache<Object, Object> cache = grid(0).cache(null);

        info("Cache affinity nodes: " + affinity(cache).mapKeyToPrimaryAndBackups(key));

        Affinity<Object> aff = affinity(cache);

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

        assertTrue(affinity(cache).isPrimary(primary, key));
        assertFalse(affinity(cache).isBackup(primary, key));
        assertTrue(affinity(cache).isPrimaryOrBackup(primary, key));

        assertFalse(affinity(cache).isPrimary(backup, key));
        assertTrue(affinity(cache).isBackup(backup, key));
        assertTrue(affinity(cache).isPrimaryOrBackup(backup, key));

        assertFalse(affinity(cache).isPrimary(other, key));

        if (cacheMode() == PARTITIONED) {
            assertFalse(affinity(cache).isBackup(other, key));
            assertFalse(affinity(cache).isPrimaryOrBackup(other, key));
        }
    }
}
