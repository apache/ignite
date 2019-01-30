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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CachePeekMode.BACKUP;
import static org.apache.ignite.cache.CachePeekMode.NEAR;
import static org.apache.ignite.cache.CachePeekMode.PRIMARY;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

/**
 * Multi-node tests for partitioned cache.
 */
public class GridCachePartitionedMultiNodeFullApiSelfTest extends GridCachePartitionedFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cc = super.cacheConfiguration(igniteInstanceName);

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
    @Test
    public void testPutAllRemoveAll() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            info(">>>>> Grid" + i + ": " + grid(i).localNode().id());

        Map<Integer, Integer> putMap = new LinkedHashMap<>();

        int size = 100;

        for (int i = 0; i < size; i++)
            putMap.put(i, i * i);

        IgniteCache<Object, Object> c0 = grid(0).cache(DEFAULT_CACHE_NAME);
        IgniteCache<Object, Object> c1 = grid(1).cache(DEFAULT_CACHE_NAME);

        c0.putAll(putMap);

        c1.removeAll(new HashSet<>(putMap.keySet()));

        for (int i = 0; i < size; i++) {
            assertNull(c0.get(i));
            assertNull(c1.get(i));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAllPutAll() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            info(">>>>> Grid" + i + ": " + grid(i).localNode().id());

        Map<Integer, Integer> putMap = new LinkedHashMap<>();

        int size = 100;

        for (int i = 0; i < size; i++)
            putMap.put(i, i);

        IgniteCache<Object, Object> prj0 = grid(0).cache(DEFAULT_CACHE_NAME);
        IgniteCache<Object, Object> prj1 = grid(1).cache(DEFAULT_CACHE_NAME);

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
    @Test
    public void testPutDebug() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            info(">>>>> Grid" + i + ": " + grid(i).localNode().id());

        final int size = 10;

        IgniteCache<Object, Object> cache0 = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < size; i++) {
            info("Putting value [i=" + i + ']');

            cache0.put(i, i);

            info("Finished putting value [i=" + i + ']');
        }

        for (int i = 0; i < gridCount(); i++)
            executeOnLocalOrRemoteJvm(i, new CheckAffinityTask(size));

        for (int i = 0; i < size; i++) {
            info("Putting value 2 [i=" + i + ']');

            assertEquals(i, cache0.getAndPutIfAbsent(i, i * i));

            info("Finished putting value 2 [i=" + i + ']');
        }

        for (int i = 0; i < size; i++)
            assertEquals(i, cache0.get(i));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPeekPartitionedModes() throws Exception {
        jcache().put("key", 1);

        for (int i = 0; i < gridCount(); i++) {
            IgniteCache<String, Integer> c = jcache(i);

            assertEquals((Integer)1, c.get("key"));

            boolean nearEnabled = nearEnabled(c);

            if (nearEnabled)
                executeOnLocalOrRemoteJvm(i, new IsNearTask());

            Integer nearPeekVal = nearEnabled ? 1 : null;

            Affinity<Object> aff = ignite(i).affinity(DEFAULT_CACHE_NAME);

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
    @Test
    public void testPeekAsyncPartitionedModes() throws Exception {
        jcache().put("key", 1);

        for (int i = 0; i < gridCount(); i++) {
            boolean nearEnabled = nearEnabled(jcache(i));

            Integer nearPeekVal = nearEnabled ? 1 : null;

            IgniteCache<String, Integer> c = jcache(i);

            if (grid(i).affinity(DEFAULT_CACHE_NAME).isBackup(grid(i).localNode(), "key")) {
                assert c.localPeek("key", NEAR) == null;

                assert c.localPeek("key", PRIMARY, BACKUP) == 1;
            }
            else if (!grid(i).affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(grid(i).localNode(), "key")) {
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
    @Test
    public void testNearDhtKeySize() throws Exception {
        List<String> keys = new ArrayList<>(5);

        info("Generating keys for test...");

        IgniteEx ignite0 = null;
        IgniteEx ignite1 = null;
        IgniteEx ignite2 = null;

        for (int i = 0; i < gridCount(); i++) {
            IgniteEx ignite = grid(i);

            if (!Boolean.TRUE.equals(ignite.configuration().isClientMode())) {
                if (ignite0 == null)
                    ignite0 = ignite;
                else if (ignite1 == null)
                    ignite1 = ignite;
                else {
                    ignite2 = ignite;

                    break;
                }

            }
        }

        assertNotNull(ignite0);
        assertNotNull(ignite1);
        assertNotNull(ignite2);

        info("Generating keys for test [nodes=" + ignite0.name() + ", " + ignite1.name() + ", " + ignite2.name() + ']');

        IgniteCache<String, Integer> cache0 = ignite0.cache(DEFAULT_CACHE_NAME);

        int val = 0;

        for (int i = 0; i < 10_000 && keys.size() < 5; i++) {
            String key = String.valueOf(i);

            if (ignite(0).affinity(DEFAULT_CACHE_NAME).isPrimary(ignite0.localNode(), key) &&
                ignite(0).affinity(DEFAULT_CACHE_NAME).isBackup(ignite1.localNode(), key)) {
                keys.add(key);

                cache0.put(key, val++);
            }
        }

        assertEquals(5, keys.size());

        info("Finished generating keys for test.");

        IgniteCache<String, Integer> cache2 = ignite2.cache(DEFAULT_CACHE_NAME);

        assertEquals(Integer.valueOf(0), cache2.get(keys.get(0)));
        assertEquals(Integer.valueOf(1), cache2.get(keys.get(1)));

        assertEquals(0, cache0.localSize(NEAR));
        assertEquals(5, cache0.localSize(CachePeekMode.ALL) - cache0.localSize(NEAR));

        IgniteCache<String, Integer> cache1 = ignite1.cache(DEFAULT_CACHE_NAME);

        assertEquals(0, cache1.localSize(NEAR));
        assertEquals(5, cache1.localSize(CachePeekMode.ALL) - cache1.localSize(NEAR));

        boolean nearEnabled = cache2.getConfiguration(CacheConfiguration.class).getNearConfiguration() != null;

        assertEquals(nearEnabled ? 2 : 0, cache2.localSize(NEAR));

        if (cacheMode() != REPLICATED)
            assertEquals(0, cache2.localSize(CachePeekMode.ALL) - cache2.localSize(NEAR));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
            @AffinityKeyMapped
            private final Object key0 = affKey;

            @Override public boolean equals(Object obj) {
                return obj == this;
            }

            @Override public int hashCode() {
                return 2;
            }
        };

        if (!isMultiJvm())
            info("All affinity nodes: " + affinityNodes());

        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

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

    /**
     *
     */
    private static class CheckAffinityTask extends TestIgniteIdxRunnable {
        /** Size. */
        private final int size;

        /**
         * @param size Size.
         */
        public CheckAffinityTask(int size) {
            this.size = size;
        }

        /** {@inheritDoc} */
        @Override public void run(int idx) throws Exception {
            assertEquals(0, ((IgniteKernal)ignite).<String, Integer>internalCache(DEFAULT_CACHE_NAME).context().tm().idMapSize());

            IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);
            ClusterNode node = ((IgniteKernal)ignite).localNode();

            for (int k = 0; k < size; k++) {
                if (affinity(cache).isPrimaryOrBackup(node, k))
                    assertEquals("Check failed for node: " + node.id(), k,
                        cache.localPeek(k, CachePeekMode.ONHEAP, CachePeekMode.OFFHEAP));
            }
        }
    }

    /**
     *
     */
    private static class IsNearTask extends TestIgniteIdxRunnable {
        /** {@inheritDoc} */
        @Override public void run(int idx) throws Exception {
            assertTrue(((IgniteKernal)ignite).internalCache(DEFAULT_CACHE_NAME).context().isNear());
        }
    }
}
