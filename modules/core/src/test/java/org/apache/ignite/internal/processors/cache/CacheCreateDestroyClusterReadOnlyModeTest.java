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

import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.cacheConfigurations;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.cacheNames;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Tests that {@link Ignite#createCache(CacheConfiguration)}/{@link Ignite#destroyCache(String)}/{@link IgniteCache#clear()}
 * denied if cluster in a {@link ClusterState#ACTIVE_READ_ONLY} state.
 */
public class CacheCreateDestroyClusterReadOnlyModeTest extends CacheCreateDestroyClusterReadOnlyModeAbstractTest {
    /** */
    @Test
    public void testCacheCreateDenied() {
        grid(0).cluster().state(ACTIVE_READ_ONLY);

        G.allGrids().forEach(n -> assertEquals(n.name(), ACTIVE_READ_ONLY, n.cluster().state()));

        for (CacheConfiguration cfg : cacheConfigurations()) {
            for (Ignite node : G.allGrids()) {
                executeAndCheckException(() -> node.createCache(cfg), cfg.getName());
                executeAndCheckException(() -> node.createCache(cfg.getName()), cfg.getName());
            }
        }
    }

    /** */
    @Test
    public void testGetOrCreateNotExistedCacheDenied() {
        grid(0).cluster().state(ACTIVE_READ_ONLY);

        G.allGrids().forEach(n -> assertEquals(n.name(), ACTIVE_READ_ONLY, n.cluster().state()));

        for (CacheConfiguration cfg : cacheConfigurations()) {
            for (Ignite node : G.allGrids()) {
                executeAndCheckException(() -> node.getOrCreateCache(cfg), cfg.getName());
                executeAndCheckException(() -> node.getOrCreateCache(cfg.getName()), cfg.getName());
            }
        }
    }

    /** */
    @Test
    public void testGetOrCreateExistedCacheAllowed() {
        grid(0).getOrCreateCaches(asList(cacheConfigurations()));

        grid(0).cluster().state(ACTIVE_READ_ONLY);

        G.allGrids().forEach(n -> assertEquals(n.name(), ACTIVE_READ_ONLY, n.cluster().state()));

        for (CacheConfiguration cfg : cacheConfigurations()) {
            for (Ignite node : G.allGrids()) {
                try (IgniteCache cache = node.getOrCreateCache(cfg)) {
                    assertNotNull(cfg.getName() + " " + node.name(), cache);
                }

                try (IgniteCache cache = node.getOrCreateCache(cfg.getName())) {
                    assertNotNull(cfg.getName() + " " + node.name(), cache);
                }
            }
        }
    }

    /** */
    @Test
    public void testCachesCreateDenied() {
        grid(0).cluster().state(ACTIVE_READ_ONLY);

        G.allGrids().forEach(n -> assertEquals(n.name(), ACTIVE_READ_ONLY, n.cluster().state()));

        List<CacheConfiguration> cfgs = asList(cacheConfigurations());

        for (Ignite node : G.allGrids())
            executeAndCheckException(() -> node.createCaches(cfgs), cacheNames().toString());
    }

    /** */
    @Test
    public void testGetOrCreateNotExistedCachesDenied() {
        grid(0).cluster().state(ACTIVE_READ_ONLY);

        G.allGrids().forEach(n -> assertEquals(n.name(), ACTIVE_READ_ONLY, n.cluster().state()));

        List<CacheConfiguration> cfgs = asList(cacheConfigurations());

        for (Ignite node : G.allGrids())
            executeAndCheckException(() -> node.getOrCreateCaches(cfgs), cacheNames().toString());
    }

    /** */
    @Test
    public void testGetOrCreateExistedCachesAllowed() {
        List<CacheConfiguration> cfgs = asList(cacheConfigurations());

        grid(0).getOrCreateCaches(cfgs);

        grid(0).cluster().state(ACTIVE_READ_ONLY);

        G.allGrids().forEach(n -> assertEquals(n.name(), ACTIVE_READ_ONLY, n.cluster().state()));

        for (Ignite node : G.allGrids()) {
            Collection<IgniteCache> caches = node.getOrCreateCaches(cfgs);

            assertEquals(cfgs.toString(), cfgs.size(), caches.size());
        }
    }

    /** */
    @Test
    public void testGetCreatedCacheAllowed() {
        grid(0).createCaches(asList(cacheConfigurations()));

        grid(0).cluster().state(ACTIVE_READ_ONLY);

        G.allGrids().forEach(n -> assertEquals(n.name(), ACTIVE_READ_ONLY, n.cluster().state()));

        for (String cacheName : cacheNames()) {
            for (Ignite node : G.allGrids())
                assertNotNull(cacheName + " " + node.name(), node.cache(cacheName));
        }
    }

    /** */
    @Test
    public void testCacheDestroyDenied() {
        grid(0).createCaches(asList(cacheConfigurations()));

        grid(0).cluster().state(ACTIVE_READ_ONLY);

        G.allGrids().forEach(n -> assertEquals(n.name(), ACTIVE_READ_ONLY, n.cluster().state()));

        for (String cacheName : cacheNames()) {
            for (Ignite node : G.allGrids())
                executeAndCheckException(() -> node.destroyCache(cacheName), cacheName);
        }
    }

    /** */
    @Test
    public void testCachesDestroyDenied() {
        grid(0).createCaches(asList(cacheConfigurations()));

        grid(0).cluster().state(ACTIVE_READ_ONLY);

        G.allGrids().forEach(n -> assertEquals(n.name(), ACTIVE_READ_ONLY, n.cluster().state()));

        for (Ignite node : G.allGrids())
            executeAndCheckException(() -> node.destroyCaches(cacheNames()), cacheNames().toString());
    }

    /** */
    private void executeAndCheckException(GridTestUtils.RunnableX clo, String cacheName) {
        Throwable ex = assertThrows(log, clo, Exception.class, null);

        ClusterReadOnlyModeTestUtils.checkRootCause(ex, cacheName);
    }
}
