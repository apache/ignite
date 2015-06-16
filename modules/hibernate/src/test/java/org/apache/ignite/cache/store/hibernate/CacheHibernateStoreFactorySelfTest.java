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

package org.apache.ignite.cache.store.hibernate;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

/**
 * Test for Cache jdbc blob store factory.
 */
public class CacheHibernateStoreFactorySelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testCacheConfiguration() throws Exception {
        try (Ignite ignite1 = startGrid(0)) {
            IgniteCache<Integer, String> cache1 = ignite1.getOrCreateCache(cacheConfiguration());

            checkStore(cache1);
        }
    }

    /**
     * @return Cache configuration with store.
     */
    private CacheConfiguration<Integer, String> cacheConfiguration() {
        CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>();

        CacheHibernateBlobStoreFactory<Integer, String> factory = new CacheHibernateBlobStoreFactory();

        factory.setHibernateConfigurationPath("/org/apache/ignite/cache/store/hibernate/hibernate.cfg.xml");

        cfg.setCacheStoreFactory(factory);

        return cfg;
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If store parameters is not the same as in configuration xml.
     */
    private void checkStore(IgniteCache<Integer, String> cache) throws Exception {
        CacheHibernateBlobStore store = (CacheHibernateBlobStore)cache.getConfiguration(CacheConfiguration.class)
            .getCacheStoreFactory().create();

        assertEquals("/org/apache/ignite/cache/store/hibernate/hibernate.cfg.xml",
            GridTestUtils.getFieldValue(store, CacheHibernateBlobStore.class, "hibernateCfgPath"));
    }
}
