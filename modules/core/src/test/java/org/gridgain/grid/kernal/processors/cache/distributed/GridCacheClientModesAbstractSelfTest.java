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

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.*;

import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 * Tests near-only cache.
 */
public abstract class GridCacheClientModesAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** Grid cnt. */
    private static AtomicInteger gridCnt;

    /** Near-only cache grid name. */
    private static String nearOnlyGridName;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        gridCnt = new AtomicInteger();

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        if (gridCnt.getAndIncrement() == 0) {
            cfg.setDistributionMode(clientOnly() ? CLIENT_ONLY : NEAR_ONLY);

            nearOnlyGridName = gridName;
        }

        cfg.setCacheStoreFactory(null);
        cfg.setAffinity(new GridCacheConsistentHashAffinityFunction(false, 32));
        cfg.setBackups(1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        gridCnt.set(0);
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @return If {@code true} then uses CLIENT_ONLY mode, otherwise NEAR_ONLY.
     */
    protected abstract boolean clientOnly();

    /**
     * @throws Exception If failed.
     */
    public void testPutFromClientNode() throws Exception {
        GridCache<Object, Object> nearOnly = nearOnlyCache();

        for (int i = 0; i < 5; i++)
            nearOnly.put(i, i);

        nearOnly.putAll(F.asMap(5, 5, 6, 6, 7, 7, 8, 8, 9, 9));

        for (int key = 0; key < 10; key++) {
            for (int i = 1; i < gridCount(); i++) {
                if (grid(i).cache(null).affinity().isPrimaryOrBackup(grid(i).localNode(), key))
                    assertEquals(key, grid(i).cache(null).peek(key));
            }

            if (nearEnabled())
                assertEquals(key, nearOnly.peek(key));

            assertNull(nearOnly.peek(key, F.asList(GridCachePeekMode.PARTITIONED_ONLY)));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetFromClientNode() throws Exception {
        GridCache<Object, Object> dht = dhtCache();

        for (int i = 0; i < 10; i++)
            dht.put(i, i);

        GridCache<Object, Object> nearOnly = nearOnlyCache();

        assert dht != nearOnly;

        for (int key = 0; key < 10; key++) {
            // At start near only cache does not have any values.
            if (nearEnabled())
                assertNull(nearOnly.peek(key));

            // Get should succeed.
            assertEquals(key, nearOnly.get(key));

            // Now value should be cached.
            if (nearEnabled())
                assertEquals(key, nearOnly.peek(key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearOnlyAffinity() throws Exception {
        for (int i = 0; i < gridCount(); i++) {
            Ignite g = grid(i);

            if (F.eq(g.name(), nearOnlyGridName)) {
                for (int k = 0; k < 10000; k++) {
                    GridCache<Object, Object> cache = g.cache(null);

                    String key = "key" + k;

                    if (cacheMode() == PARTITIONED)
                        assertFalse(cache.entry(key).primary() || cache.entry(key).backup());

                    assertFalse(cache.affinity().mapKeyToPrimaryAndBackups(key).contains(g.cluster().localNode()));
                }
            }
            else {
                boolean foundEntry = false;
                boolean foundAffinityNode = false;

                for (int k = 0; k < 10000; k++) {
                    GridCache<Object, Object> cache = g.cache(null);

                    String key = "key" + k;

                    if (cache.entry(key).primary() || cache.entry(key).backup())
                        foundEntry = true;

                    if (cache.affinity().mapKeyToPrimaryAndBackups(key).contains(g.cluster().localNode()))
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
    protected GridCache<Object, Object> nearOnlyCache() {
        assert nearOnlyGridName != null;

        return G.ignite(nearOnlyGridName).cache(null);
    }

    /**
     * @return DHT cache for this test.
     */
    protected GridCache<Object, Object> dhtCache() {
        for (int i = 0; i < gridCount(); i++) {
            if (!nearOnlyGridName.equals(grid(i).name()))
                return grid(i).cache(null);
        }

        assert false : "Cannot find DHT cache for this test.";

        return null;
    }
}
