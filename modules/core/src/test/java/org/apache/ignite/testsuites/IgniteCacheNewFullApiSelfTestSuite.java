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

package org.apache.ignite.testsuites;

import junit.framework.TestSuite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheNewFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.GridNewCacheAbstractSelfTest;
import org.apache.ignite.testframework.GridTestSuite;
import org.apache.ignite.testframework.NewTestsConfiguration;
import org.apache.ignite.testframework.config.ConfigurationFactory;

import static org.apache.ignite.testframework.junits.GridAbstractTest.defaultCacheConfiguration;

/**
 * Test suite for cache API.
 */
public class IgniteCacheNewFullApiSelfTestSuite extends TestSuite {
    /**
     * @return Cache API test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Cache New Full API Test Suite");

        suite.addTest(new GridTestSuite(GridCacheNewFullApiSelfTest.class,
            new NewTestsConfiguration(new SimpleConfigurationFactory() {
                @Override public CacheConfiguration cacheConfiguration(String name) {
                    CacheConfiguration cc = super.cacheConfiguration(name);

                    cc.setCacheMode(CacheMode.LOCAL);

                    return cc;
                }
            }, "local-1", 1)));

        suite.addTest(new GridTestSuite(GridCacheNewFullApiSelfTest.class,
            new NewTestsConfiguration(new SimpleConfigurationFactory() {
                @Override public CacheConfiguration cacheConfiguration(String name) {
                    CacheConfiguration cc = super.cacheConfiguration(name);

                    cc.setCacheMode(CacheMode.LOCAL);

                    return cc;
                }
            }, "partitioned-1", true, 1)));

        suite.addTest(new GridTestSuite(GridCacheNewFullApiSelfTest.class,
            new NewTestsConfiguration(new SimpleConfigurationFactory() {
                @Override public CacheConfiguration cacheConfiguration(String name) {
                    CacheConfiguration cc = super.cacheConfiguration(name);

                    cc.setCacheMode(CacheMode.LOCAL);

                    return cc;
                }
            }, "partitioned-1", true, 2)));

        return suite;
    }

    private static IgniteConfiguration copyDefaults(IgniteConfiguration fromCfg, IgniteConfiguration toCfg) {
        toCfg.setGridName(fromCfg.getGridName());
        toCfg.setNodeId(fromCfg.getNodeId());
        toCfg.setConsistentId(fromCfg.getConsistentId());
        toCfg.setDiscoverySpi(fromCfg.getDiscoverySpi());
        toCfg.setCommunicationSpi(fromCfg.getCommunicationSpi());
        toCfg.setGridLogger(fromCfg.getGridLogger());
        toCfg.setMBeanServer(fromCfg.getMBeanServer());

        return toCfg;
    }

    private static CacheConfiguration cacheConfiguration() {
        CacheConfiguration cfg = defaultCacheConfiguration();

        CacheStore<?, ?> store = GridNewCacheAbstractSelfTest.cacheStore();

        if (store != null) {
            cfg.setCacheStoreFactory(new GridNewCacheAbstractSelfTest.TestStoreFactory());
            cfg.setReadThrough(true);
            cfg.setWriteThrough(true);
            cfg.setLoadPreviousValue(true);
        }

        return cfg;
    }

    private static class SimpleConfigurationFactory implements ConfigurationFactory {
        /** {@inheritDoc} */
        @Override public IgniteConfiguration getConfiguration(String gridName, IgniteConfiguration cfg) {
            return copyDefaults(cfg, new IgniteConfiguration());
        }

        /** {@inheritDoc} */
        @Override public CacheConfiguration cacheConfiguration(String name) {
            return IgniteCacheNewFullApiSelfTestSuite.cacheConfiguration();
        }
    }
}
