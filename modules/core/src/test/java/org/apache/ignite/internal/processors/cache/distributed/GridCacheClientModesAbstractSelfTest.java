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

package org.apache.ignite.internal.processors.cache.distributed;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests near-only cache.
 */
public abstract class GridCacheClientModesAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** Grid cnt. */
    private static AtomicInteger gridCnt;

    /** Near-only cache Ignite instance name. */
    private static String nearOnlyIgniteInstanceName;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** */
    @Before
    public void beforeCacheStoreListenerRWThroughDisabledTransactionalCacheTest() {
        if (nearEnabled())
            MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.NEAR_CACHE);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        gridCnt = new AtomicInteger();

        super.beforeTestsStarted();

        if (nearEnabled())
            grid(nearOnlyIgniteInstanceName).createNearCache(DEFAULT_CACHE_NAME, nearConfiguration());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        int cnt = gridCnt.incrementAndGet();

        if ((cnt == gridCount() && isClientStartedLast()) || (cnt == 1 && !isClientStartedLast())) {
            cfg.setClientMode(true);

            nearOnlyIgniteInstanceName = igniteInstanceName;
        }

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        if (nearEnabled())
            MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.NEAR_CACHE);

        CacheConfiguration cfg = super.cacheConfiguration(igniteInstanceName);

        cfg.setCacheStoreFactory(null);
        cfg.setReadThrough(false);
        cfg.setWriteThrough(false);
        cfg.setBackups(1);

        if (cfg.getCacheMode() == REPLICATED)
            cfg.setAffinity(null);
        else
            cfg.setAffinity(new RendezvousAffinityFunction(false, 32));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @return boolean {@code True} if client's grid must be started last, {@code false} if it must be started first.
     */
    protected boolean isClientStartedLast() {
        return false;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutFromClientNode() throws Exception {
        IgniteCache<Object, Object> nearOnly = nearOnlyCache();

        for (int i = 0; i < 5; i++)
            nearOnly.put(i, i);

        nearOnly.putAll(F.asMap(5, 5, 6, 6, 7, 7, 8, 8, 9, 9));

        for (int key = 0; key < 10; key++) {
            for (int i = 0; i < gridCount(); i++) {
                if (grid(i).affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(grid(i).localNode(), key))
                    assertEquals(key, grid(i).cache(DEFAULT_CACHE_NAME).localPeek(key));
            }

            if (nearEnabled())
                assertEquals(key, nearOnly.localPeek(key, CachePeekMode.ONHEAP));

            assertNull(nearOnly.localPeek(key, CachePeekMode.PRIMARY, CachePeekMode.BACKUP));
        }

        Integer key = 1000;

        nearOnly.put(key, new TestClass1(key));

        if (nearEnabled())
            assertNotNull(nearOnly.localPeek(key, CachePeekMode.ALL));
        else
            assertNull(nearOnly.localPeek(key, CachePeekMode.ALL));

        for (int i = 0; i < gridCount(); i++) {
            if (grid(i).affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(grid(i).localNode(), key)) {
                TestClass1 val = (TestClass1)grid(i).cache(DEFAULT_CACHE_NAME).localPeek(key);

                assertNotNull(val);
                assertEquals(key.intValue(), val.val);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetFromClientNode() throws Exception {
        IgniteCache<Object, Object> dht = dhtCache();

        for (int i = 0; i < 10; i++)
            dht.put(i, i);

        IgniteCache<Object, Object> nearOnly = nearOnlyCache();

        assert dht != nearOnly;

        for (int key = 0; key < 10; key++) {
            // At start near only cache does not have any values.
            if (nearEnabled())
                assertNull(nearOnly.localPeek(key, CachePeekMode.ONHEAP));

            // Get should succeed.
            assertEquals(key, nearOnly.get(key));

            // Now value should be cached.
            if (nearEnabled())
                assertEquals(key, nearOnly.localPeek(key, CachePeekMode.ONHEAP));
        }

        Integer key = 2000;

        dht.put(key, new TestClass2(key));

        TestClass2 val = (TestClass2)nearOnly.get(key);

        assertNotNull(val);
        assertEquals(key.intValue(), val.val);

        if (nearEnabled())
            assertNotNull(nearOnly.localPeek(key, CachePeekMode.ONHEAP));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNearOnlyAffinity() throws Exception {
        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            if (F.eq(g.name(), nearOnlyIgniteInstanceName)) {
                for (int k = 0; k < 10000; k++) {
                    IgniteCache<Object, Object> cache = g.cache(DEFAULT_CACHE_NAME);

                    String key = "key" + k;

                    if (cacheMode() == PARTITIONED)
                        assertFalse(affinity(cache).isPrimaryOrBackup(g.cluster().localNode(), key));

                    assertFalse(affinity(cache).mapKeyToPrimaryAndBackups(key).contains(g.cluster().localNode()));
                }
            }
            else {
                boolean foundEntry = false;
                boolean foundAffinityNode = false;

                for (int k = 0; k < 10000; k++) {
                    String key = "key" + k;

                    if (g.affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(g.cluster().localNode(), key))
                        foundEntry = true;

                    if (g.affinity(DEFAULT_CACHE_NAME).mapKeyToPrimaryAndBackups(key).contains(g.cluster().localNode()))
                        foundAffinityNode = true;
                }

                assertTrue("Did not found primary or backup entry for grid: " + i, foundEntry);
                assertTrue("Did not found affinity node for grid: " + i, foundAffinityNode);
            }
        }
    }

    /**
     * @return Near only cache for this test.
     */
    protected IgniteCache<Object, Object> nearOnlyCache() {
        assert nearOnlyIgniteInstanceName != null;

        return G.ignite(nearOnlyIgniteInstanceName).cache(DEFAULT_CACHE_NAME);
    }

    /**
     * @return DHT cache for this test.
     */
    protected IgniteCache<Object, Object> dhtCache() {
        for (int i = 0; i < gridCount(); i++) {
            if (!nearOnlyIgniteInstanceName.equals(grid(i).name()))
                return grid(i).cache(DEFAULT_CACHE_NAME);
        }

        assert false : "Cannot find DHT cache for this test.";

        return null;
    }

    /**
     *
     */
    static class TestClass1 implements Serializable {
        /** */
        int val;

        /**
         * @param val Value.
         */
        public TestClass1(int val) {
            this.val = val;
        }
    }

    /**
     *
     */
    static class TestClass2 implements Serializable {
        /** */
        int val;

        /**
         * @param val Value.
         */
        public TestClass2(int val) {
            this.val = val;
        }
    }
}
