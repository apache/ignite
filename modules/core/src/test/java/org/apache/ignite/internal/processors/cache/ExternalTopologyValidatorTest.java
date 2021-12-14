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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.CacheTopologyValidatorProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/** */
public class ExternalTopologyValidatorTest extends GridCommonAbstractTest {
    /** */
    private static CountDownLatch validatorInvokedLatch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return getConfiguration(igniteInstanceName, false, true);
    }

    /** */
    private IgniteConfiguration getConfiguration(
        String igniteInstanceName,
        boolean persistenceEnabled,
        boolean configurePlugin
    ) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (configurePlugin)
            cfg.setPluginProviders(new TestPluginProvider());

        if (persistenceEnabled) {
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

    /** */
    @Test
    public void testExternalValidator() throws Exception {
        startGrids(2);

        validatorInvokedLatch = new CountDownLatch(2);

        grid(0).createCache(DEFAULT_CACHE_NAME);

        assertTrue(validatorInvokedLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));
    }

    /** */
    @Test
    public void testExternalValidatorWithPersistence() throws Exception {
        startGrid(getConfiguration(getTestIgniteInstanceName(0), true, true));
        startGrid(getConfiguration(getTestIgniteInstanceName(1), true, true));

        grid(0).cluster().state(ClusterState.ACTIVE);

        validatorInvokedLatch = new CountDownLatch(2);

        IgniteCache<Object, Object> cache = grid(0).createCache(DEFAULT_CACHE_NAME);

        assertTrue(validatorInvokedLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));

        cache.put(0, 0);

        stopAllGrids();

        startGrid(getConfiguration(getTestIgniteInstanceName(0), true, true));
        startGrid(getConfiguration(getTestIgniteInstanceName(1), true, true));

        validatorInvokedLatch = new CountDownLatch(2);

        grid(0).cluster().state(ClusterState.ACTIVE);

        assertTrue(validatorInvokedLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));

        assertEquals(0, grid(0).cache(DEFAULT_CACHE_NAME).get(0));

        stopAllGrids();

        startGrid(getConfiguration(getTestIgniteInstanceName(0), true, false));
        startGrid(getConfiguration(getTestIgniteInstanceName(1), true, false));

        assertEquals(0, grid(0).cache(DEFAULT_CACHE_NAME).get(0));
    }

    /** */
    private static class TestPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "TestPluginProvider";
        }

        /** {@inheritDoc} */
        @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
            registry.registerExtension(CacheTopologyValidatorProvider.class, new TestTopologyValidatorProvider());
        }
    }

    /** */
    private static class TestTopologyValidatorProvider implements CacheTopologyValidatorProvider {
        /** {@inheritDoc} */
        @Override public @Nullable TopologyValidator create(String cacheName) {
            if (DEFAULT_CACHE_NAME.equals(cacheName)) {
                return new TopologyValidator() {
                    @Override public boolean validate(Collection<ClusterNode> nodes) {
                        if (validatorInvokedLatch != null)
                            validatorInvokedLatch.countDown();

                        return true;
                    }
                };
            }

            return null;
        }
    }
}
