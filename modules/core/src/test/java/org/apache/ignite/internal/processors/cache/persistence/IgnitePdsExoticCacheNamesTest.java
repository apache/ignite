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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests for persistent caches with tricky names: with special characters, non-ASCII symbols, etc.
 */
@RunWith(Parameterized.class)
public class IgnitePdsExoticCacheNamesTest extends GridCommonAbstractTest {
    @Parameterized.Parameters(name = "persistent={0}")
    public static Iterable<Boolean> data() {
        return Arrays.asList(true, false);
    }

    @Parameterized.Parameter(0)
    public boolean isPersistent;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(isPersistent);
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

    /** */
    @Test
    public void testNameWithSlashes() throws Exception {
        checkBadName("my/cool/cache");
    }

    /** */
    @Test
    public void testNameWithBackslashes() throws Exception {
        checkBadName("my\\cool\\cache");
    }

    /** */
    @Test
    public void testNameWithControlSymbols() throws Exception {
        checkBadName("my\0\1\2\3\4\5\6\7cache");
    }

    /** */
    @Test
    public void testNameWithNewLines() throws Exception {
        checkBadName("my \n cool \n cache");
    }

    /** */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_VALIDATE_CACHE_NAMES, value = "false")
    public void testNameWithSlashesNoValidation() throws Exception {
        Assume.assumeFalse(isPersistent); // bad characters would break persistence
        checkGoodName("my/cool/cache");
    }

    /** */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_VALIDATE_CACHE_NAMES, value = "false")
    public void testNameWithBackslashesNoValidation() throws Exception {
        Assume.assumeFalse(isPersistent); // bad characters would break persistence
        checkGoodName("my\\cool\\cache");
    }

    /** */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_VALIDATE_CACHE_NAMES, value = "false")
    public void testNameWithControlSymbolsNoValidation() throws Exception {
        Assume.assumeFalse(isPersistent); // bad characters would break persistence
        checkGoodName("my\0\1\2\3\4\5\6\7cache");
    }

    /** */
    @Test
    public void testNameWithSpecialSymbols() throws Exception {
        checkGoodName("my!@#$%^&*()cache");
    }

    /** */
    @Test
    public void testNameWithUnicodeSymbols() throws Exception {
        checkGoodName("my\u0431\u0430\u0431\u0443\u0448\u043a\u0430cache");
    }

    /** */
    @Test
    public void testNameWithWhiteSpaces() throws Exception {
        checkGoodName("my \t cool \t cache");
    }

    /**
     * @param cacheName cache name to test
     * @throws Exception If failed.
     */
    private void checkBadName(String cacheName) throws Exception {
        startGrid();

        if (isPersistent)
            grid().cluster().state(ClusterState.ACTIVE);

        try {
            grid().createCache(cacheName);

            fail("Expected IllegalArgumentException was not thrown");
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains(
                    "Cache name cannot contain slashes (/), backslashes (\\), line separators (\\n), " +
                            "or null characters (\\0)"))
                return; //expected

            throw e;
        } finally {
            stopGrid();
        }
    }

    /**
     * @param cacheName cache name to test
     * @throws Exception If failed.
     */
    private void checkGoodName(String cacheName) throws Exception {
        startGrid();

        if (isPersistent)
            grid().cluster().state(ClusterState.ACTIVE);

        IgniteCache<String, String> cache = grid().createCache(cacheName);
        cache.put(cacheName + "::foo", cacheName + "::bar");
        assertEquals(cacheName + "::bar", cache.get(cacheName + "::foo"));

        stopGrid();

        if (isPersistent) {
            // check that restart works
            startGrid();

            cache = grid().cache(cacheName);
            assertEquals(cacheName + "::bar", cache.get(cacheName + "::foo"));

            stopGrid();
        }
    }
}
