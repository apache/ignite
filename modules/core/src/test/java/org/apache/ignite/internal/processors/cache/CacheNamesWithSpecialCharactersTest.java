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

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Collection;

/**
 * Test that validates {@link Ignite#cacheNames()} implementation.
 */
public class CacheNamesWithSpecialCharactersTest extends GridCommonAbstractTest {

    public static final String CACHE_NAME_1 = "--№=+:(replicated)";
    public static final String CACHE_NAME_2 = ":_&:: (partitioned)";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cacheCfg1 = new CacheConfiguration();
        cacheCfg1.setCacheMode(CacheMode.REPLICATED);
        cacheCfg1.setName(CACHE_NAME_1);

        CacheConfiguration cacheCfg2 = new CacheConfiguration();
        cacheCfg2.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg2.setName(CACHE_NAME_2);

        cfg.setCacheConfiguration(cacheCfg1, cacheCfg2);

        return cfg;
    }

    /**
     * @throws Exception In case of failure.
     */
    public void testCacheNames() throws Exception {
        try {
            startGridsMultiThreaded(2);

            Collection<String> names = grid(0).cacheNames();

            assertEquals(2, names.size());

            for (String name : names)
                assertTrue(name.equals(CACHE_NAME_1) || name.equals(CACHE_NAME_2));
        }
        finally {
            stopAllGrids();
        }
    }
}
