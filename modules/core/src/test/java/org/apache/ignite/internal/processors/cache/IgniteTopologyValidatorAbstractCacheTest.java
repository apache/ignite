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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.CacheTopologyValidatorProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Topology validator test.
 */
@RunWith(Parameterized.class)
public abstract class IgniteTopologyValidatorAbstractCacheTest extends IgniteCacheAbstractTest implements Serializable {
    /** */
    @Parameterized.Parameter()
    public Boolean isPluginTopValidatorProvider;

    /** */
    @Parameterized.Parameters(name = "isPluginTopValidatorProvider={0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[] {true}, new Object[] {false});
    }

    /** key-value used at test. */
    private static String KEY_VAL = "1";

    /** cache name 1. */
    static String CACHE_NAME_1 = "cache1";

    /** cache name 2. */
    protected static String CACHE_NAME_2 = "cache2";

    /** {@inheritDoc} */
    @Override protected final int gridCount() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (!cfg.isClientMode()) {
            CacheConfiguration cCfg0 = cacheConfiguration(igniteInstanceName);

            CacheConfiguration cCfg1 = cacheConfiguration(igniteInstanceName);
            cCfg1.setName(CACHE_NAME_1);

            CacheConfiguration cCfg2 = cacheConfiguration(igniteInstanceName);
            cCfg2.setName(CACHE_NAME_2);

            cfg.setCacheConfiguration(cCfg0, cCfg1, cCfg2);

            TestCacheTopologyValidatorProvider topValidatorProvider = new TestCacheTopologyValidatorProvider();

            if (!isPluginTopValidatorProvider) {
                for (CacheConfiguration cCfg : cfg.getCacheConfiguration())
                    cCfg.setTopologyValidator(topValidatorProvider.topologyValidator(cCfg.getName()));
            }
            else
                cfg.setPluginProviders(new TestCacheTopologyValidatorPluginProvider(topValidatorProvider));
        }

        return cfg;
    }

    /**
     * @param nodes Nodes.
     * @return Number of server nodes.
     */
    private static int servers(Collection<ClusterNode> nodes) {
        int c = 0;

        for (ClusterNode node : nodes) {
            if (!node.isClient())
                c++;
        }

        return c;
    }

    /**
     * Puts when topology is invalid.
     *
     * @param cacheName cache name.
     */
    void putInvalid(String cacheName) {
        try {
            List<Ignite> nodes = nodes();

            assertFalse(nodes.isEmpty());

            for (Ignite node : nodes)
                node.cache(cacheName).put(KEY_VAL, KEY_VAL);

            fail("Topology validation broken");
        }
        catch (CacheException ex) {
            assert ex.getCause() instanceof IgniteCheckedException &&
                ex.getCause().getMessage().contains("cache topology is not valid");
        }
    }

    /**
     * Puts when topology is valid.
     *
     * @param cacheName cache name.
     */
    void putValid(String cacheName) {
        try {
            List<Ignite> nodes = nodes();

            assertFalse(nodes.isEmpty());

            for (Ignite node : nodes)
                node.cache(cacheName).put(KEY_VAL, KEY_VAL);

            for (Ignite node : nodes)
                assertEquals(KEY_VAL, node.cache(cacheName).get(KEY_VAL));
        }
        catch (CacheException ignored) {
            assert false : "topology validation broken";
        }
    }

    /**
     * Gets when topology is invalid.
     *
     * @param cacheName cache name.
     */
    void getInvalid(String cacheName) {
        List<Ignite> nodes = nodes();

        assertFalse(nodes.isEmpty());

        for (Ignite node : nodes)
            assertEquals(KEY_VAL, node.cache(cacheName).get(KEY_VAL));
    }

    /**
     * Remove when topology is invalid.
     *
     * @param cacheName cache name.
     */
    void removeInvalid(String cacheName) {
        try {
            List<Ignite> nodes = nodes();

            assertFalse(nodes.isEmpty());

            for (Ignite node : nodes)
                node.cache(cacheName).remove(KEY_VAL);

            fail("Topology validation broken");
        }
        catch (CacheException ex) {
            assert ex.getCause() instanceof IgniteCheckedException &&
                ex.getCause().getMessage().contains("cache topology is not valid");
        }
    }

    /**
     * @return Nodes.
     */
    private List<Ignite> nodes() {
        if (this instanceof IgniteTopologyValidatorAbstractTxCacheTest ||
            this instanceof IgniteTopologyValidatorAbstractTxCacheGroupsTest)
            return Collections.singletonList(ignite(0));
        else
            return G.allGrids();
    }

    /**
     * Commits with error.
     *
     * @param tx transaction.
     */
    void commitFailed(Transaction tx) {
        try {
            tx.commit();

            fail();
        }
        catch (IgniteException ex) {
            assert ex.getCause() instanceof IgniteCheckedException &&
                ex.getCause().getMessage().contains("cache topology is not valid");
        }
    }

    /**
     * Removes key-value.
     *
     * @param cacheName cache name.
     */
    public void remove(String cacheName) {
        List<Ignite> nodes = G.allGrids();

        assertFalse(nodes.isEmpty());

        for (Ignite node : nodes)
            assertNotNull(node.cache(cacheName).get(KEY_VAL));

        grid(0).cache(cacheName).remove(KEY_VAL);
    }

    /**
     * Asserts that cache doesn't contains key.
     *
     * @param cacheName cache name.
     */
    void assertEmpty(String cacheName) {
        assertNull(grid(0).cache(cacheName).get(KEY_VAL));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTopologyValidator() throws Exception {
        startGrid(0);

        putValid(DEFAULT_CACHE_NAME);
        remove(DEFAULT_CACHE_NAME);

        putInvalid(CACHE_NAME_1);
        removeInvalid(CACHE_NAME_1);

        putInvalid(CACHE_NAME_2);
        removeInvalid(CACHE_NAME_2);

        startGrid(1);

        putValid(DEFAULT_CACHE_NAME);
        remove(DEFAULT_CACHE_NAME);

        putValid(CACHE_NAME_1);

        putValid(CACHE_NAME_2);
        remove(CACHE_NAME_2);

        startGrid(2);

        putValid(DEFAULT_CACHE_NAME);
        remove(DEFAULT_CACHE_NAME);

        getInvalid(CACHE_NAME_1);
        putInvalid(CACHE_NAME_1);
        removeInvalid(CACHE_NAME_1);

        putValid(CACHE_NAME_2);
        remove(CACHE_NAME_2);

        startClientGrid(3);

        putValid(DEFAULT_CACHE_NAME);
        remove(DEFAULT_CACHE_NAME);

        getInvalid(CACHE_NAME_1);
        putInvalid(CACHE_NAME_1);
        removeInvalid(CACHE_NAME_1);

        putValid(CACHE_NAME_2);
        remove(CACHE_NAME_2);
    }

    /** */
    public static class TestCacheTopologyValidatorPluginProvider extends AbstractTestPluginProvider {
        /** */
        private final CacheTopologyValidatorProvider provider;

        /** */
        public TestCacheTopologyValidatorPluginProvider(CacheTopologyValidatorProvider provider) {
            this.provider = provider;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return "CacheTopologyValidatorProviderPlugin";
        }

        /** {@inheritDoc} */
        @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
            registry.registerExtension(CacheTopologyValidatorProvider.class, provider);
        }
    }

    /** */
    private static class TestCacheTopologyValidatorProvider implements CacheTopologyValidatorProvider, Serializable {
        /** {@inheritDoc} */
        @Override public TopologyValidator topologyValidator(String cacheName) {
            if (CACHE_NAME_1.equals(cacheName)) {
                return new TopologyValidator() {
                    @Override public boolean validate(Collection<ClusterNode> nodes) {
                        return servers(nodes) == 2;
                    }
                };
            }
            else if (CACHE_NAME_2.equals(cacheName)) {
                return new TopologyValidator() {
                    @Override public boolean validate(Collection<ClusterNode> nodes) {
                        return servers(nodes) >= 2;
                    }
                };
            }
            else
                return null;
        }
    }
}
