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
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.processors.cache.IgniteTopologyValidatorAbstractCacheTest.TestCacheTopologyValidatorPluginProvider;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.plugin.CacheTopologyValidatorProvider;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;

/** */
public class CacheTopologyValidatorProviderTest extends GridCommonAbstractTest {
    /** */
    private IgniteConfiguration getConfiguration(
        int idx,
        boolean isPersistenceEnabled,
        PluginProvider<?>... providers
    ) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(idx));

        cfg.setPluginProviders(providers);

        if (isPersistenceEnabled) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(100 * (1 << 20))
                    .setPersistenceEnabled(true)));
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testTopologyValidatorProviderWithPersistence() throws Exception {
        startGrid(getConfiguration(0, true, new TestPluginProvider("top-validator", 0)));

        grid(0).cluster().state(ACTIVE);

        awaitPartitionMapExchange();

        grid(0).createCache(DEFAULT_CACHE_NAME);

        checkCachePut(DEFAULT_CACHE_NAME, true);

        stopAllGrids();

        PluginProvider<?> pluginProvider = new TestPluginProvider("top-validator", 1);

        startGrid(getConfiguration(0, true, pluginProvider));

        grid(0).cluster().state(ACTIVE);

        checkCachePut(DEFAULT_CACHE_NAME, false);

        assertEquals(0, grid(0).cache(DEFAULT_CACHE_NAME).get(0));

        startGrid(getConfiguration(1, true, pluginProvider));

        checkCachePut(DEFAULT_CACHE_NAME, true);

        stopAllGrids();

        startGrid(getConfiguration(0, true));

        grid(0).cluster().state(ACTIVE);

        checkCachePut(DEFAULT_CACHE_NAME, true);
    }

    /** */
    @Test
    public void testCacheConfigurationValidatorAlongsidePluginValidators() throws Exception {
        PluginProvider<?> firstPluginProvider = new TestPluginProvider("first-top-validator", 2);
        PluginProvider<?> secondPluginProvider = new TestPluginProvider("second-top-validator", 3);

        startGrid(getConfiguration(0, false, firstPluginProvider, secondPluginProvider));

        grid(0).createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setTopologyValidator(new TestTopologyValidator()));

        checkCachePut(DEFAULT_CACHE_NAME, false);

        startGrid(getConfiguration(1, false, firstPluginProvider, secondPluginProvider));

        checkCachePut(DEFAULT_CACHE_NAME, false);

        startGrid(getConfiguration(2, false, firstPluginProvider, secondPluginProvider));

        checkCachePut(DEFAULT_CACHE_NAME, false);

        startGrid(getConfiguration(3, false, firstPluginProvider, secondPluginProvider));

        checkCachePut(DEFAULT_CACHE_NAME, true);
    }

    /** */
    private void checkCachePut(String cacheName, boolean isSuccessExpected) {
        for (Ignite ignite : G.allGrids()) {
            if (isSuccessExpected) {
                ignite.cache(cacheName).put(0, 0);

                assertEquals(0, grid(0).cache(cacheName).get(0));
            }
            else {
                GridTestUtils.assertThrows(
                    log,
                    () -> {
                        ignite.cache(cacheName).put(0, 0);

                        return null;
                    },
                    CacheInvalidStateException.class,
                    "Failed to perform cache operation"
                );
            }
        }
    }

    /** */
    private static class TestTopologyValidator implements TopologyValidator {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean validate(Collection<ClusterNode> nodes) {
            return nodes.size() > 1;
        }
    }

    /** */
    private static class TestPluginProvider extends TestCacheTopologyValidatorPluginProvider {
        /** */
        private final String name;

        /** {@inheritDoc} */
        @Override public String name() {
            return name;
        }

        /** */
        private TestPluginProvider(String name, int validationThreshold) {
            super(new TestCacheTopologyValidatorProvider(validationThreshold));

            this.name = name;
        }

        /** */
        private static class TestCacheTopologyValidatorProvider implements CacheTopologyValidatorProvider {
            /** */
            private final int validationThreshold;

            /** */
            public TestCacheTopologyValidatorProvider(int validationThreshold) {
                this.validationThreshold = validationThreshold;
            }

            /** {@inheritDoc} */
            @Override public TopologyValidator topologyValidator(String cacheName) {
                return nodes -> nodes.size() > validationThreshold;
            }
        }
    }
}
