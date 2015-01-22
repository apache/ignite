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

package org.gridgain.grid.kernal.processors.cache.local;

import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.GridCacheMode.*;

/**
 * Tests for local cache.
 */
public class GridCacheLocalFullApiSelfTest extends GridCacheAbstractFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return LOCAL;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.getTransactionsConfiguration().setTxSerializableEnabled(true);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setSwapEnabled(true);

        return cfg;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testMapKeysToNodes() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 2);

        Map<ClusterNode, Collection<String>> map = cache().affinity().mapKeysToNodes(F.asList("key1", "key2"));

        assert map.size() == 1;

        Collection<String> keys = map.get(dfltIgnite.cluster().localNode());

        assert keys != null;
        assert keys.size() == 2;

        for (String key : keys)
            assert "key1".equals(key) || "key2".equals(key);

        map = cache().affinity().mapKeysToNodes(F.asList("key1", "key2"));

        assert map.size() == 1;

        keys = map.get(dfltIgnite.cluster().localNode());

        assert keys != null;
        assert keys.size() == 2;

        for (String key : keys)
            assert "key1".equals(key) || "key2".equals(key);
    }

    /**
     * Based on issue GG-2864
     *
     * @throws Exception In case of error.
     */
    public void testFilteredKeySet() throws Exception {
        if (!txEnabled() || portableEnabled())
            return;

        final GridCache<String, Integer> myCache = cache();

        final AtomicLong cntr = new AtomicLong();

        // Some counter.
        myCache.dataStructures().atomicLong("some_counter", 0L, true).incrementAndGet();

        // I would like to filter from key set all entities which key name is not started with "a_".
        IgnitePredicate<GridCacheEntry<String, Integer>> aPred = new IgnitePredicate<GridCacheEntry<String, Integer>>() {
            @Override public boolean apply(GridCacheEntry<String, Integer> entry) {
                cntr.incrementAndGet();

                assert entry.getKey() instanceof String;

                return entry.getKey().startsWith("a_");
            }
        };

        Set<String> aKeySet = myCache.projection(aPred).keySet();

        aKeySet.size(); // Initiate lazy iteration.

        assertEquals(0, cntr.get());

        // Key set is empty as expected - no entities in cache except atomic counter !!!
        assertTrue(aKeySet.isEmpty());

        // Add some entities to cache.
        myCache.putx("a_1", 1);
        myCache.putx("a_2", 2);
        myCache.putx("b_1", 3);

        // Repeat key set filtering.
        aKeySet = myCache.projection(aPred).keySet();

        // This will cause iteration and counter will get incremented.
        assertEquals(2, aKeySet.size());

        assertEquals(3, cntr.get());
        assertTrue(aKeySet.containsAll(F.asList("a_1", "a_2")));
    }
}
