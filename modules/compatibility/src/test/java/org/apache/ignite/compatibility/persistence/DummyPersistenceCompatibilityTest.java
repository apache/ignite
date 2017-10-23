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

package org.apache.ignite.compatibility.persistence;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

/** */
public class DummyPersistenceCompatibilityTest extends IgnitePersistenceCompatibilityAbstractTest {
    /** */
    private static final String TEST_CACHE_NAME = DummyPersistenceCompatibilityTest.class.getSimpleName();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true));

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeStartByOldVersionPersistenceData() throws Exception {
        try {
            startGrid(1, "2.2.0", new ConfigurationClosure(), new PostStartupClosure());

            stopAllGrids();

            IgniteEx ignite = startGrid(0);

            assertEquals(1, ignite.context().discovery().topologyVersion());

            ignite.active(true);

            IgniteCache<Integer, String> cache = ignite.getOrCreateCache(TEST_CACHE_NAME);

            for (int i = 0; i < 10; i++)
                assertEquals("data" + i, cache.get(i));
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private static class PostStartupClosure implements IgniteInClosure<Ignite> {
        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ignite.active(true);

            CacheConfiguration<Integer, String> cacheCfg = new CacheConfiguration<>();
            cacheCfg.setName(TEST_CACHE_NAME);
            cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cacheCfg.setBackups(1);
            cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

            IgniteCache<Integer, String> cache = ignite.createCache(cacheCfg);

            for (int i = 0; i < 10; i++)
                cache.put(i, "data" + i);
        }
    }

    /** */
    private static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            cfg.setLocalHost("127.0.0.1");

            TcpDiscoverySpi disco = new TcpDiscoverySpi();
            disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

            cfg.setDiscoverySpi(disco);

            cfg.setPeerClassLoadingEnabled(false);

            cfg.setPersistentStoreConfiguration(new PersistentStoreConfiguration());
        }
    }
}
