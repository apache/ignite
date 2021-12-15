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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluggableCacheTopologyValidator;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;

/** */
public class ExternalTopologyValidatorTest extends GridCommonAbstractTest {
    /** */
    private static final List<T2<String, Collection<UUID>>> topValidatorInvocations = new ArrayList<>();

    /** */
    public static boolean pluginTopValidationResult = true;

    /** */
    public static boolean cacheTopValidationResult = true;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return getConfiguration(igniteInstanceName, false, true);
    }

    /** */
    private IgniteConfiguration getConfiguration(
        String igniteInstanceName,
        boolean isPersistenceEnabled,
        boolean isPluginEnabled
    ) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (isPluginEnabled)
            cfg.setPluginProviders(new TestCacheTopologyValidatorPluginProvider());

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

        topValidatorInvocations.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testPluggableTopologyValidator() throws Exception {
        startGrids(2);

        awaitPartitionMapExchange();

        grid(0).createCache(DEFAULT_CACHE_NAME);

        checkTopologyValidatorInvocations(DEFAULT_CACHE_NAME, 1);

        topValidatorInvocations.clear();

        grid(0).createCache(new CacheConfiguration<>().setName("test_cache_0").setGroupName("test_cache_group"));
        grid(0).createCache(new CacheConfiguration<>().setName("test_cache_1").setGroupName("test_cache_group"));

        checkTopologyValidatorInvocations(DEFAULT_CACHE_NAME, 2);
        checkTopologyValidatorInvocations("test_cache_group", 2);
    }

    /** */
    @Test
    public void testPluggableTopologyValidatorWithPersistence() throws Exception {
        startGrid(getConfiguration(getTestIgniteInstanceName(0), true, true));
        startGrid(getConfiguration(getTestIgniteInstanceName(1), true, true));

        grid(0).cluster().state(ACTIVE);

        awaitPartitionMapExchange();

        grid(0).createCache(DEFAULT_CACHE_NAME);

        checkTopologyValidatorInvocations(DEFAULT_CACHE_NAME, 1);

        grid(0).cache(DEFAULT_CACHE_NAME).put(0, 0);

        stopAllGrids();

        startGrid(getConfiguration(getTestIgniteInstanceName(0), true, true));
        startGrid(getConfiguration(getTestIgniteInstanceName(1), true, true));

        topValidatorInvocations.clear();

        grid(0).cluster().state(ACTIVE);

        checkTopologyValidatorInvocations(DEFAULT_CACHE_NAME, 1);

        assertEquals(0, grid(0).cache(DEFAULT_CACHE_NAME).get(0));

        stopAllGrids();

        startGrid(getConfiguration(getTestIgniteInstanceName(0), true, false));
        startGrid(getConfiguration(getTestIgniteInstanceName(1), true, false));

        topValidatorInvocations.clear();

        grid(0).cluster().state(ACTIVE);

        checkTopologyValidatorInvocations(DEFAULT_CACHE_NAME, 0);

        assertEquals(0, grid(0).cache(DEFAULT_CACHE_NAME).get(0));
    }

    /** */
    @Test
    public void testCacheReadOnlyState() throws Exception {
        startGrid(0);

        pluginTopValidationResult = false;
        cacheTopValidationResult = false;

        try {
            grid(0).createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setTopologyValidator(new TestTopologyValidator()));

            checkCachePut(DEFAULT_CACHE_NAME, false);

            pluginTopValidationResult = true;

            startGrid(1);

            checkCachePut(DEFAULT_CACHE_NAME, false);

            cacheTopValidationResult = true;

            stopGrid(1);

            checkCachePut(DEFAULT_CACHE_NAME, true);
        }
        finally {
            pluginTopValidationResult = true;
            cacheTopValidationResult = true;
        }
    }

    /** */
    private void checkTopologyValidatorInvocations(String cacheName, long expCnt) throws Exception {
        awaitPartitionMapExchange();

        List<T2<String, Collection<UUID>>> invocations = topValidatorInvocations.stream()
            .filter(i -> cacheName.equals(i.get1()))
            .collect(Collectors.toList());

        assertEquals(expCnt * grid(0).cluster().nodes().size(), invocations.size());

        assertTrue(invocations.stream().allMatch(i ->
            i.get2().equals(grid(0).cluster().nodes().stream().map(ClusterNode::id).collect(Collectors.toList()))));
    }

    /** */
    private void checkCachePut(String cacheName, boolean isSuccessExpected) {
        if (isSuccessExpected) {
            grid(0).cache(cacheName).put(0, 0);

            assertEquals(0, grid(0).cache(cacheName).get(0));
        }
        else {
            GridTestUtils.assertThrows(
                log,
                () -> {
                    grid(0).cache(cacheName).put(0, 0);

                    return null;
                },
                CacheInvalidStateException.class,
                "Failed to perform cache operation"
            );
        }
    }

    /** */
    private static class TestTopologyValidator implements TopologyValidator {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean validate(Collection<ClusterNode> nodes) {
            return cacheTopValidationResult;
        }
    }

    /** */
    private static class TestCacheTopologyValidatorPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "PluggableCacheTopologyValidator";
        }

        /** {@inheritDoc} */
        @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
            registry.registerExtension(PluggableCacheTopologyValidator.class, new TestCacheTopologyValidator());
        }
    }

    /** */
    private static class TestCacheTopologyValidator implements PluggableCacheTopologyValidator {
        /** {@inheritDoc} */
        @Override public boolean validate(String cacheName, Collection<ClusterNode> nodes) {
            topValidatorInvocations.add(new T2<>(
                cacheName,
                nodes.stream().map(ClusterNode::id).collect(Collectors.toList())
            ));

            return pluginTopValidationResult;
        }
    }
}
