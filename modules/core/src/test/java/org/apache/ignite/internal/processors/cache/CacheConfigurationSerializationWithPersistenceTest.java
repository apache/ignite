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

import javax.cache.configuration.FactoryBuilder;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test suite to check that user-defined parameters for static cache configurations are not explicitly deserialized
 * on non-affinity nodes.
 */
public class CacheConfigurationSerializationWithPersistenceTest extends GridCommonAbstractTest {
    /** Client mode. */
    private boolean clientMode;

    /** Caches. */
    private CacheConfiguration[] caches;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setClientMode(clientMode);

        if (caches != null)
            cfg.setCacheConfiguration(caches);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(256 * 1024 * 1024))
        );

        return cfg;
    }

    /**
     *
     */
    @Before
    public void before() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    @After
    public void after() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Creates configuration for cache which affinity belongs only to given node index.
     *
     * @param nodeIdx Node index.
     * @return Cache configuration.
     */
    private CacheConfiguration onlyOnNode(int nodeIdx) {
        return new CacheConfiguration("cache-" + getTestIgniteInstanceName(nodeIdx))
            .setNodeFilter(new OnlyOneNodeFilter(getTestIgniteInstanceName(nodeIdx)))
            .setCacheStoreFactory(FactoryBuilder.factoryOf(GridCacheTestStore.class));
    }

    /**
     *
     */
    @Test
    public void testSerializationForCachesConfiguredOnCoordinator() throws Exception {
        caches = new CacheConfiguration[] {onlyOnNode(0), onlyOnNode(1), onlyOnNode(2)};

        IgniteEx crd = startGrid(0);

        caches = null;

        startGridsMultiThreaded(1, 2);

        crd.cluster().active(true);

        awaitPartitionMapExchange();

        for (Ignite node : G.allGrids())
            checkCaches((IgniteEx) node);
    }

    /**
     *
     */
    @Test
    public void testSerializationForCachesConfiguredOnDifferentNodes1() throws Exception {
        startGrid(0);

        caches = new CacheConfiguration[] {onlyOnNode(0), onlyOnNode(1)};

        startGrid(1);

        caches = new CacheConfiguration[] {onlyOnNode(2)};

        startGrid(2);

        awaitPartitionMapExchange();

        for (Ignite node : G.allGrids())
            checkCaches((IgniteEx) node);
    }

    /**
     *
     */
    @Test
    public void testSerializationForCachesConfiguredOnDifferentNodes2() throws Exception {
        caches = new CacheConfiguration[] {onlyOnNode(0)};

        startGrid(0);

        caches = new CacheConfiguration[] {onlyOnNode(1)};

        startGrid(1);

        caches = new CacheConfiguration[] {onlyOnNode(2)};

        startGrid(2);

        awaitPartitionMapExchange();

        for (Ignite node : G.allGrids())
            checkCaches((IgniteEx) node);
    }

    /**
     *
     */
    @Test
    public void testSerializationForCachesConfiguredOnDifferentNodes3() throws Exception {
        caches = new CacheConfiguration[] {onlyOnNode(1)};

        startGrid(0);

        caches = new CacheConfiguration[] {onlyOnNode(2)};

        startGrid(1);

        caches = new CacheConfiguration[] {onlyOnNode(0)};

        startGrid(2);

        awaitPartitionMapExchange();

        for (Ignite node : G.allGrids())
            checkCaches((IgniteEx) node);
    }

    /**
     *
     */
    @Test
    public void testSerialzationForCachesOnClientNode() throws Exception {
        startGrid(0);

        caches = new CacheConfiguration[] {onlyOnNode(0), onlyOnNode(1)};

        startGrid(1);

        caches = new CacheConfiguration[] {onlyOnNode(2)};

        startGrid(2);

        caches = null;
        clientMode = true;

        startGrid(3);

        awaitPartitionMapExchange();

        for (Ignite node : G.allGrids())
            checkCaches((IgniteEx) node);
    }

    /**
     * @param node Node.
     */
    private void checkCaches(IgniteEx node) {
        ClusterNode clusterNode = node.localNode();
        GridCacheProcessor cacheProcessor = node.context().cache();

        for (DynamicCacheDescriptor cacheDesc : cacheProcessor.cacheDescriptors().values()) {
            if (CU.isUtilityCache(cacheDesc.cacheName()))
                continue;

            boolean affinityNode = CU.affinityNode(clusterNode, cacheDesc.cacheConfiguration().getNodeFilter())
                || cacheDesc.receivedFrom().equals(node.localNode().id());

            if (affinityNode) {
                IgniteInternalCache cache = cacheProcessor.cache(cacheDesc.cacheName());

                Assert.assertTrue("Cache is not started " + cacheDesc.cacheName() + ", node " + node.name(), cache != null);

                CacheConfiguration ccfg = cache.configuration();

                Assert.assertTrue("Cache store factory is null " + cacheDesc.cacheName() + ", node " + node.name(), ccfg.getCacheStoreFactory() != null);
            }
            else {
                Assert.assertTrue("Cache is started " + cacheDesc.cacheName() + ", node " + node.name(), cacheProcessor.cache(cacheDesc.cacheName()) == null);
                Assert.assertTrue("Cache configuration is enriched " + cacheDesc.cacheName() + ", node " + node.name(), !cacheDesc.isConfigurationEnriched());
                Assert.assertTrue("Cache store factory is not null " + cacheDesc.cacheName() + ", node " + node.name(), cacheDesc.cacheConfiguration().getCacheStoreFactory() == null);
            }
        }
    }

    /**
     *
     */
    private static class OnlyOneNodeFilter implements IgnitePredicate<ClusterNode> {
        /** Consistent id. */
        private final String consistentId;

        /**
         * @param consistentId Consistent id.
         */
        private OnlyOneNodeFilter(String consistentId) {
            this.consistentId = consistentId;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return node.consistentId().equals(consistentId);
        }
    }
}
