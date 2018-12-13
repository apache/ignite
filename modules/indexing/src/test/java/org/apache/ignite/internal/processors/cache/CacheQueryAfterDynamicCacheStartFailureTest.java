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

import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class CacheQueryAfterDynamicCacheStartFailureTest extends IgniteAbstractDynamicCacheStartFailTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(gridCount());

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    protected CacheConfiguration createCacheConfiguration(String cacheName) {
        CacheConfiguration cfg = new CacheConfiguration()
            .setName(cacheName)
            .setIndexedTypes(Integer.class, Value.class);

        return cfg;
    }

    protected void checkCacheOperations(IgniteCache<Integer, Value> cache) throws Exception {
        super.checkCacheOperations(cache);

        // Check SQL API.
        String sql = "fieldVal >= ? and fieldVal <= ?";
        List<Cache.Entry<Integer, Value>> res = cache.query(
            new SqlQuery<Integer, Value>(Value.class, sql).setArgs(1, 100)).getAll();

        assertNotNull(res);
        assertEquals(100, res.size());
    }
}
