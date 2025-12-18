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
 *
 */

package org.apache.ignite.cache.store;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Checks that array keys are supported.
 */
public class StoreArrayKeyTest extends GridCommonAbstractTest {
    /** Cache. */
    private static final String CACHE = "cache-1";

    /** Cache with backups. */
    private static final String CACHE_WITH_BACKUPS = "cache-with-backup";

    /** Node 1. */
    private IgniteEx node1;

    /** */
    private static Collection<Arguments> allTypesArgs() {
        return List.of(
            Arguments.of(new byte[] {1, 2, 3}, new byte[] {3, 2, 1}, new byte[] {1, 2, 3},
            Arguments.of(new short[] {1, 2, 3}, new short[] {3, 2, 1}, new short[] {1, 2, 3}),
            Arguments.of(new int[] {1, 2, 3}, new int[] {3, 2, 1}, new int[] {1, 2, 3}),
            Arguments.of(new long[] {1, 2, 3}, new long[] {3, 2, 1}, new long[] {1, 2, 3}),
            Arguments.of(new float[] {1, 2, 3}, new float[] {3, 2, 1}, new float[] {1, 2, 3}),
            Arguments.of(new double[] {1, 2, 3}, new double[] {3, 2, 1}, new double[] {1, 2, 3}),
            Arguments.of(new char[] {1, 2, 3}, new char[] {3, 2, 1}, new char[] {1, 2, 3}),
            Arguments.of(new boolean[] {true, false, true}, new boolean[] {false, true, false}, new boolean[] {true, false, true}),
            Arguments.of(new String[] {"a", "b", "c"}, new String[] {"c", "b", "a"}, new String[] {"a", "b", "c"}),
            Arguments.of(
                new String[][]{
                    new String[]{"a", "b", null},
                    new String[]{"a", null, "c"},
                    new String[]{null, "b", "c"}
                },
                new String[] {null, null, null},
                new String[][] {
                    new String[] {"a", "b", null},
                    new String[] {"a", null, "c"},
                    new String[] {null, "b", "c"}
                }
            )
        ));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDataRegionConfigurations(new DataRegionConfiguration()
                    .setName("pdr")
                    .setPersistenceEnabled(true))
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration(CACHE_WITH_BACKUPS)
                .setDataRegionName("pdr")
                .setBackups(1)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC),
            new CacheConfiguration(CACHE)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
        );

        return cfg;
    }

    /**
     *
     */
    @BeforeEach
    public void setUp() throws Exception {
        node1 = startGrid(0);
        startGrid(1);

        node1.cluster().state(ClusterState.ACTIVE);
    }

    /**
     *
     */
    @AfterEach
    public void tearDown() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /**
     *
     */
    @ParameterizedTest
    @MethodSource("allTypesArgs")
    public void shouldReadWriteKey(Object firstKey, Object secondKey, Object likeFirstKey) {
        IgniteCache<Object, Object> cache = node1.getOrCreateCache(CACHE);

        cache.put(firstKey, 1);
        cache.put(secondKey, 2);
        cache.put(likeFirstKey, 3);

        assertEquals(2, cache.size());

        assertEquals(3, cache.get(likeFirstKey));
        assertEquals(3, cache.get(firstKey));
        assertEquals(2, cache.get(secondKey));

        assertTrue(cache.containsKey(likeFirstKey));
        assertTrue(cache.containsKey(firstKey));
        assertTrue(cache.containsKey(secondKey));
    }

    /**
     *
     */
    @ParameterizedTest
    @MethodSource("allTypesArgs")
    public void shouldRemoveBySameKey(Object firstKey, Object secondKey, Object likeFirstKey) {
        IgniteCache<Object, Object> cache = node1.getOrCreateCache(CACHE);

        cache.put(firstKey, 1);
        cache.put(secondKey, 2);
        cache.put(likeFirstKey, 3);

        assertEquals(2, cache.size());

        cache.remove(firstKey);
        cache.remove(secondKey);

        assertEquals(0, cache.size());
    }

    /**
     *
     */
    @ParameterizedTest
    @MethodSource("allTypesArgs")
    public void shouldRemoveAllCache(Object firstKey) {
        IgniteCache<Object, Object> cache = node1.getOrCreateCache(CACHE);

        cache.put(firstKey, "val");

        cache.removeAll();

        assertEquals(0, cache.size());

        Iterator<Cache.Entry<Object, Object>> it = cache.iterator();

        assertFalse(it.hasNext());
    }

    /**
     *
     */
    @ParameterizedTest
    @MethodSource("allTypesArgs")
    public void shouldClearCache(Object firstKey) {
        IgniteCache<Object, Object> cache = node1.getOrCreateCache(CACHE);

        cache.put(firstKey, "val");

        cache.clear();

        assertEquals(0, cache.size());

        Iterator<Cache.Entry<Object, Object>> it = cache.iterator();

        assertFalse(it.hasNext());
    }
}
