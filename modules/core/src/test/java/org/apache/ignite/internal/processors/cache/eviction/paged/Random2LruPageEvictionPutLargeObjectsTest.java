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
package org.apache.ignite.internal.processors.cache.eviction.paged;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;
import static org.apache.ignite.internal.processors.cache.eviction.paged.PageEvictionAbstractTest.setEvictionMode;

/** */
public class Random2LruPageEvictionPutLargeObjectsTest extends GridCommonAbstractTest {
    /** Offheap size for memory policy. */
    private static final int SIZE = 1024 * 1024 * 1024;

    /** Record size. */
    private static final int RECORD_SIZE = 80 * 1024 * 1024;

    /** Number of entries. */
    static final int ENTRIES = 50;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setInitialSize(SIZE)
                    .setMaxSize(SIZE)
                )
                .setPageSize(DFLT_PAGE_SIZE)
            );

        return setEvictionMode(DataPageEvictionMode.RANDOM_2_LRU, cfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = "IGNITE_DUMP_THREADS_ON_FAILURE", value = "false")
    public void testPutLargeObjects() throws Exception {
        IgniteEx ignite = startGrids(2);

        IgniteCache<Integer, Object> cache = ignite.createCache(DEFAULT_CACHE_NAME);

        Object val = new byte[RECORD_SIZE];

        for (Integer key : primaryKeys(grid(1).cache(DEFAULT_CACHE_NAME), ENTRIES))
            cache.put(key, val);

        assertTrue(cache.size() < ENTRIES);
    }
}
