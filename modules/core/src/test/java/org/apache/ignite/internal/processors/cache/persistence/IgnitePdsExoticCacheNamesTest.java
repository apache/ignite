/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for persistent caches with tricky names: with special characters, non-ASCII symbols, and with names that
 * are equal ignoring case.
 */
public class IgnitePdsExoticCacheNamesTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);
        cfg.setDataStorageConfiguration(storageCfg);

        return cfg;
    }

    /** */
    @Before
    public void setUp() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    @After
    public void tearDown() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** Test a persistent cache with slashes in the name */
    @Test
    public void testNameWithSlashes() throws Exception {
        checkPersistentCaches("my/cool/cache");
    }

    /** Test a persistent cache with slashes in the name */
    @Test
    public void testNameWithBackslashes() throws Exception {
        checkPersistentCaches("my\\cool\\cache");
    }

    /** Test a persistent cache with special symbols in the name */
    @Test
    public void testNameWithSpecialSymbols() throws Exception {
        checkPersistentCaches("my!@#$%^&*()cache");
    }

    /** Test a persistent cache with control symbols in the name */
    @Test
    public void testNameWithControlSymbols() throws Exception {
        checkPersistentCaches("my\0\1\2\3\4\5\6\7cache");
    }

    /** Test a persistent cache with control symbols in the name */
    @Test
    public void testNameWithUnicodeSymbols() throws Exception {
        checkPersistentCaches("my\u0431\u0430\u0431\u0443\u0448\u043a\u0430cache");
    }

    /**
     * Checks that persistent caches with the specified names can be created and work as expected.
     * The method takes variable arguments for the cases when multiple caches need to be checked together
     * (e.g. when {@code name1.equalsIgnoreCase(name2)})
     *
     * @param cacheNames cache names to test
     * @throws Exception If failed.
     */
    private void checkPersistentCaches(String... cacheNames) throws Exception {
        Ignite ignite = startGrid();
        ignite.cluster().state(ClusterState.ACTIVE);

        for (String cacheName : cacheNames) {
            IgniteCache<String, String> cache = ignite.getOrCreateCache(cacheName);
            cache.put(cacheName + "::foo", cacheName + "::bar");
        }

        stopGrid();

        ignite = startGrid();

        for (String cacheName : cacheNames) {
            IgniteCache<String, String> cache = ignite.cache(cacheName);
            String val = cache.get(cacheName + "::foo");
            assertEquals(cacheName + "::bar", val);
        }

        stopGrid();
    }
}
