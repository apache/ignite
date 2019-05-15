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
import org.junit.Test;

/**
 * Tests various cache operations with indexing enabled.
 * Cache contain multiple types.
 */
public class CacheOffheapBatchIndexingMultiTypeTest extends CacheOffheapBatchIndexingBaseTest {
    /**
     * Tests putAll with multiple indexed entities and streamer pre-loading with low off-heap cache size.
     */
    @Test
    public void testPutAllMultupleEntitiesAndStreamer() {
        doStreamerBatchTest(50, 1_000, new Class<?>[] {
            Integer.class, CacheOffheapBatchIndexingBaseTest.Person.class,
            Integer.class, CacheOffheapBatchIndexingBaseTest.Organization.class},
            true);
    }

    /**
     * Tests putAll after with streamer batch load with one entity.
     */
    @Test
    public void testPuAllSingleEntity() {
        doStreamerBatchTest(50,
            1_000,
            new Class<?>[] {Integer.class, CacheOffheapBatchIndexingBaseTest.Organization.class},
            false);
    }

    /**
     * @param iterations Number of iterations.
     * @param entitiesCnt Number of entities to put.
     * @param entityClasses Entity classes.
     * @param preloadInStreamer Data preload flag.
     */
    private void doStreamerBatchTest(int iterations,
        int entitiesCnt,
        Class<?>[] entityClasses,
        boolean preloadInStreamer) {
        Ignite ignite = grid(0);

        final IgniteCache<Object, Object> cache =
            ignite.createCache(cacheConfiguration(entityClasses));

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
        }
        finally {
            cache.destroy();
        }
    }
}
