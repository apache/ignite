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

package org.apache.ignite.internal.processors.cache;

import java.util.Map;
import java.util.TreeMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Tests various cache operations with indexing enabled.
 * Cache contain multiple types.
 */
public class CacheOffheapBatchIndexingMultiTypeTest extends CacheOffheapBatchIndexingBaseTest {
    /**
     * Test putAll with multiple indexed entites and streamer pre-loading with low off-heap cache size.
     * The test fails in remove call.
     */
    public void testPutAllMultupleEntitiesAndStreamer() {
        //fail("IGNITE-2982");
        doStreamerBatchTest(50, 1_000, new Class<?>[] {Integer.class, CacheOffheapBatchIndexingBaseTest.Person.class,
            Integer.class, CacheOffheapBatchIndexingBaseTest.Organization.class}, 1, true);
    }

    /**
     * Test putAll with multiple indexed entites and streamer preloading with default off-heap cache size.
     * The test fails on remove and often hangs in putAll call.
     */
    public void testPutAllMultupleEntitiesAndStreamerDfltOffHeapRowCacheSize() {
        //fail("IGNITE-2982");
        doStreamerBatchTest(50, 1_000, new Class<?>[] {Integer.class, CacheOffheapBatchIndexingBaseTest.Person.class,
                Integer.class, CacheOffheapBatchIndexingBaseTest.Organization.class},
            CacheConfiguration.DFLT_SQL_ONHEAP_ROW_CACHE_SIZE, true);
    }

    /**
     * Test putAll after with streamer batch load with one entity.
     * The test fails in putAll.
     */
    public void testPuAllSingleEntity() {
        //fail("IGNITE-2982");
        doStreamerBatchTest(50, 1_000, new Class<?>[] {Integer.class, CacheOffheapBatchIndexingBaseTest.Organization.class}, 1, false);
    }

    /**
     * Test putAll after with streamer batch load with one entity.
     */
    private void doStreamerBatchTest(int iterations, int entitiesCnt, Class<?>[] entityClasses, int onHeapRowCacheSize, boolean preloadInStreamer) {
        Ignite ignite = grid(0);

        final IgniteCache<Object, Object> cache =
            ignite.createCache(cacheConfiguration(onHeapRowCacheSize, entityClasses));

        try {
            if (preloadInStreamer)
                preload(cache.getName());

            while (iterations-- >= 0) {
                Map<Integer, Person> putMap1 = new TreeMap<>();

                for (int i = 0; i < entitiesCnt; i++)
                    putMap1.put(i, new Person(i, i + 1, String.valueOf(i), String.valueOf(i + 1), salary(i)));

                cache.putAll(putMap1);

                Map<Integer, Organization> putMap2 = new TreeMap<>();

                for (int i = entitiesCnt / 2; i < entitiesCnt * 3 / 2; i++) {
                    cache.remove(i);

                    putMap2.put(i, new Organization(i, String.valueOf(i)));
                }

                cache.putAll(putMap2);
            }
        } finally {
            cache.destroy();
        }
    }
}