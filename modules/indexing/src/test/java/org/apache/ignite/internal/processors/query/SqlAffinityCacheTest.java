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
package org.apache.ignite.internal.processors.query;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;

/**
 *
 */
public class SqlAffinityCacheTest extends AbstractIndexingCommonTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setIndexedTypes(Integer.class, String.class));
    }

    /**
     * Test that affinity assignment instance (required for queries) is not removed from affinity cache
     * after numerous client node left/join events.
     */
    @Test
    public void testAffinityCache() throws Exception {
        startGrid(0);

        for (int i = 0; i < 300; i++) {
            startClientGrid(1);
            stopGrid(1);
        }

        IgniteCache<Integer, String> cache = grid(0).cache(DEFAULT_CACHE_NAME);
        cache.put(0, "0");

        assertEquals(1, cache.query(new SqlFieldsQuery("SELECT * FROM String")).getAll().size());
    }
}
