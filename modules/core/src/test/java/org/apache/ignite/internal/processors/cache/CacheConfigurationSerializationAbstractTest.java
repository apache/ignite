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
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runners.Parameterized;

/**
 * A base class to check that user-defined parameters (marked as {@link org.apache.ignite.configuration.SerializeSeparately})
 * for cache configurations are not explicitly deserialized on non-affinity nodes.
 */
public class CacheConfigurationSerializationAbstractTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameters(name = "Persistence enabled = {0}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        params.add(new Object[]{false});
        params.add(new Object[]{true});

        return params;
    }

    /** Persistence enabled. */
    @Parameterized.Parameter
    public boolean persistenceEnabled;

    /** Jdk marshaller */
    private final JdkMarshaller marsh = MarshallerUtils.jdkMarshaller(null);

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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        if (persistenceEnabled)
            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                            new DataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(256 * 1024 * 1024))
            );

        return cfg;
    }

    /**
     * Creates configuration for cache which affinity belongs only to given node index.
     *
     * @param nodeIdx Node index.
     * @return Cache configuration.
     */
    protected CacheConfiguration onlyOnNode(int nodeIdx) {
        return new CacheConfiguration("cache-" + getTestIgniteInstanceName(nodeIdx))
                .setNodeFilter(new OnlyOneNodeFilter(getTestIgniteInstanceName(nodeIdx)))
                .setWriteBehindEnabled(true)
                .setWriteThrough(true)
                .setReadThrough(true)
                .setCacheStoreFactory(FactoryBuilder.factoryOf(GridCacheTestStore.class));
    }

    /**
     * @param stopCrd If {@code true}, coordinator will be stopped.
     * @throws Exception If failed.
     */
    protected void restartNodesAndCheck(boolean stopCrd) throws Exception {
        if (!stopCrd) {
            Collection<Ignite> srvs = new ArrayList<>();

            for (Ignite g : G.allGrids()) {
                if (!g.configuration().getDiscoverySpi().isClientMode()
                        && !g.name().equals(getTestIgniteInstanceName(0)))
                    srvs.add(g);
            }

            for (Ignite g : srvs)
                stopGrid(g.name(), true, false);
        }
        else
            stopAllGrids();

        if (stopCrd) {
            startGridsMultiThreaded(3);

            startClientGrid(3);
        }
        else {
            for (int i = 1; i < 3; i++)
                startGrid(i);
        }

        awaitPartitionMapExchange();

        for (Ignite node : G.allGrids())
            checkCaches((IgniteEx) node);
    }

    /**
     * @param node Node.
     */
    protected void checkCaches(IgniteEx node) throws Exception {
        ClusterNode clusterNode = node.localNode();
        GridCacheProcessor cacheProc = node.context().cache();

        for (DynamicCacheDescriptor cacheDesc : cacheProc.cacheDescriptors().values()) {
            if (CU.isUtilityCache(cacheDesc.cacheName()))
                continue;

            boolean affNode = CU.affinityNode(clusterNode, cacheDesc.cacheConfiguration().getNodeFilter());

            IgniteInternalCache cache = cacheProc.cache(cacheDesc.cacheName());

            if (affNode) {
                Assert.assertNotNull("Cache is not started " + cacheDesc.cacheName() + ", node " + node.name(), cache);

                CacheConfiguration ccfg = cache.configuration();

                Assert.assertNotNull("Cache store factory is null " + cacheDesc.cacheName() + ", node " + node.name(),
                        ccfg.getCacheStoreFactory());
            }
            else {
                Assert.assertTrue("Cache is started " + cacheDesc.cacheName() + ", node " + node.name(),
                        cache == null || !cache.context().affinityNode());

                if (cache == null) {
                    Assert.assertFalse("Cache configuration is enriched " + cacheDesc.cacheName() + ", node " + node.name(),
                            cacheDesc.isConfigurationEnriched());
                    Assert.assertNull("Cache store factory is not null " + cacheDesc.cacheName() + ", node " + node.name(),
                            cacheDesc.cacheConfiguration().getCacheStoreFactory());
                }
            }

            // Checks that in enrichment stay an actual serialized class instead of null.
            if (cacheDesc.cacheConfigurationEnrichment() != null) {
                CacheConfigurationEnrichment enrichment = cacheDesc.cacheConfigurationEnrichment();

                byte[] data = enrichment.getFieldSerializedValue("storeFactory");

                Assert.assertNotNull("storeFactory is null for cache: " + cacheDesc.cacheName(),
                        marsh.unmarshal(data, getClass().getClassLoader()));
            }
        }
    }

    /**
     *
     */
    static class OnlyOneNodeFilter implements IgnitePredicate<ClusterNode> {
        /** Consistent id. */
        private final String consistentId;

        /**
         * @param consistentId Consistent id.
         */
        OnlyOneNodeFilter(String consistentId) {
            this.consistentId = consistentId;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return node.consistentId().equals(consistentId);
        }
    }
}

