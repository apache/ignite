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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.http.GridEmbeddedHttpServer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for dynamic cache start from config file.
 */
public class IgniteDynamicCacheConfigTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_NAME = "TestDynamicCache";

    /** */
    private static final String STATIC_CACHE_NAME = "TestStaticCache";

    /** */
    private static final String TEST_ATTRIBUTE_NAME = TestNodeFilter.TEST_ATTRIBUTE_NAME;

    /** */
    private boolean testAttribute = true;

    /**
     * @return Number of nodes for this test.
     */
    public int nodeCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(nodeCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setUserAttributes(F.asMap(TEST_ATTRIBUTE_NAME, testAttribute));

        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setCacheMode(CacheMode.REPLICATED);

        cacheCfg.setName(STATIC_CACHE_NAME);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDynamicCacheStartFromConfig() throws Exception {
        IgniteCache cache = ignite(0).createCache(load(
            "modules/spring/src/test/java/org/apache/ignite/internal/cache.xml"));

        assertEquals("TestDynamicCache", cache.getName());

        IgniteCache cache1 = ignite(0).getOrCreateCache(load(
            "modules/spring/src/test/java/org/apache/ignite/internal/cache.xml"));

        assertEquals(cache, cache1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDynamicNearCacheStartFromConfig() throws Exception {
        testAttribute = false;

        try {
            startGrid(nodeCount() + 1);

            IgniteCache cache = ignite(0).createCache(load(
                "modules/spring/src/test/java/org/apache/ignite/internal/filtered-cache.xml"));

            assertEquals(CACHE_NAME, cache.getName());

            IgniteCache clientCache1 = ignite(nodeCount() + 1).createNearCache(CACHE_NAME,
                loadNear("modules/spring/src/test/java/org/apache/ignite/internal/cache.xml"));

            IgniteCache clientCache2 = ignite(nodeCount() + 1).getOrCreateNearCache(CACHE_NAME,
                loadNear("modules/spring/src/test/java/org/apache/ignite/internal/cache.xml"));

            assertEquals(clientCache1, clientCache2);

            clientCache1.put(1, 1);
        }
        finally {
            stopGrid(nodeCount() + 1);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateNearCache() throws Exception {
        testAttribute = false;

        try {
            int clientNode = nodeCount() + 1;

            startGrid(clientNode);

            IgniteCache cache = ignite(clientNode).createCache(
                load("modules/spring/src/test/java/org/apache/ignite/internal/filtered-cache.xml"),
                loadNear("modules/spring/src/test/java/org/apache/ignite/internal/cache.xml"));

            assertEquals(cache.getName(), CACHE_NAME);

            IgniteCache clientCache1 = ignite(clientNode).createNearCache(CACHE_NAME,
                loadNear("modules/spring/src/test/java/org/apache/ignite/internal/cache.xml"));

            IgniteCache clientCache2 = ignite(clientNode).getOrCreateNearCache(CACHE_NAME,
                loadNear("modules/spring/src/test/java/org/apache/ignite/internal/cache.xml"));

            assertEquals(clientCache1, clientCache2);

            clientCache1.put(1, 1);
        }
        finally {
            stopGrid(nodeCount() + 1);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetOrCreateNearCache() throws Exception {
        testAttribute = false;

        IgniteCache cache = ignite(0).createCache(load(
            "modules/spring/src/test/java/org/apache/ignite/internal/filtered-cache.xml"));

        try {
            int clientNode = nodeCount() + 1;

            startGrid(clientNode);

            IgniteCache cache1 = ignite(clientNode).getOrCreateCache(
                load("modules/spring/src/test/java/org/apache/ignite/internal/filtered-cache.xml"),
                loadNear("modules/spring/src/test/java/org/apache/ignite/internal/cache.xml"));

            assertEquals(cache.getName(), cache1.getName());

            IgniteCache clientCache1 = ignite(clientNode).createNearCache(CACHE_NAME,
                loadNear("modules/spring/src/test/java/org/apache/ignite/internal/cache.xml"));

            IgniteCache clientCache2 = ignite(clientNode).getOrCreateNearCache(CACHE_NAME,
                loadNear("modules/spring/src/test/java/org/apache/ignite/internal/cache.xml"));

            assertEquals(clientCache1, clientCache2);

            clientCache1.put(1, 1);
        }
        finally {
            stopGrid(nodeCount() + 1);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDynamicCacheStartFromNotExistConfig() throws Exception {
        try {
            ignite(0).getOrCreateCache(load("config/cache.xml"));

            fail();
        }
        catch (IgniteException ignored) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartCachedWithConfigUrlString() throws Exception {
        GridEmbeddedHttpServer srv = null;

        try {
            srv = GridEmbeddedHttpServer.startHttpServer().withFileDownloadingHandler(null,
                GridTestUtils.resolveIgnitePath("modules/spring/src/test/java/org/apache/ignite/internal/cache.xml"));

            IgniteCache cache = ignite(0).createCache(load(srv.getBaseUrl()));

            assertEquals("TestDynamicCache", cache.getName());
        }
        finally {
            if (srv != null)
                srv.stop(1);
        }
    }

    /**
     * @param path Path.
     * @return Configuration.
     */
    private CacheConfiguration load(String path) {
        return Ignition.loadSpringBean(path, "cache-configuration");
    }

    /**
     * @param path Path.
     * @return Configuration.
     */
    private NearCacheConfiguration loadNear(String path) {
        return Ignition.loadSpringBean(path, "nearCache-configuration");
    }
}
