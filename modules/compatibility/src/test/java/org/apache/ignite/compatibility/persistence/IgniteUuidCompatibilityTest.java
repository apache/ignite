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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

/**
 * Checks that index and caches with IgniteUuid are compatible when IgniteUuid becomes predefined type.
 *
 * @see BinaryContext#registerPredefinedType(Class, int)
 */
public class IgniteUuidCompatibilityTest extends IgnitePersistenceCompatibilityAbstractTest {
    /** */
    protected static final String TEST_CACHE_NAME = IgniteUuidCompatibilityTest.class.getSimpleName();

    /** */
    protected volatile boolean compactFooter;

    /** */
    private static Set<String> VALUES = new HashSet<>(Arrays.asList("one", "two", "three"));

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "binary_meta", false));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                ));

        cfg.setBinaryConfiguration(
            new BinaryConfiguration()
                .setCompactFooter(compactFooter)
        );

        return cfg;
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version for cache contains IgniteUuid instances.
     *
     * @throws Exception If failed.
     */
    public void testIgniteUuid_2_3() throws Exception {
        doTestIgniteUuidCompatibility("2.3.0", false);
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version for cache contains IgniteUuid instances.
     *
     * @throws Exception If failed.
     */
    public void testIgniteUuidWithCompactFooter_2_3() throws Exception {
        doTestIgniteUuidCompatibility("2.3.0", true);
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @param igniteVer 3-digits version of ignite
     * @throws Exception If failed.
     */
    private void doTestIgniteUuidCompatibility(String igniteVer, boolean compactFooter) throws Exception {
        boolean prev = this.compactFooter;

        try {
            this.compactFooter = compactFooter;

            startGrid(1, igniteVer, new ConfigurationClosure(compactFooter), new PutData());

            stopAllGrids();

            IgniteEx ignite = startGrid(0);

            assertEquals(1, ignite.context().discovery().topologyVersion());

            ignite.active(true);

            IgniteCache<IgniteUuid, String> cache = ignite.cache(TEST_CACHE_NAME);

            assertEquals(cache.size(CachePeekMode.ALL), VALUES.size());

            Set<String> values = new HashSet<>();

            for (Cache.Entry<IgniteUuid, String> e : cache)
                values.add(e.getValue());

            assertEquals(values, VALUES);
        }
        finally {
            stopAllGrids();

            this.compactFooter = prev;
        }
    }

    /** */
    public static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        private boolean compactFooter;

        public ConfigurationClosure(boolean compactFooter) {
            this.compactFooter = compactFooter;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            cfg.setLocalHost("127.0.0.1");

            TcpDiscoverySpi disco = new TcpDiscoverySpi();
            disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

            cfg.setDiscoverySpi(disco);

            cfg.setPeerClassLoadingEnabled(false);

            cfg.setPersistentStoreConfiguration(new PersistentStoreConfiguration());

            if (!compactFooter)
                cfg.setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(compactFooter));
        }
    }

    /** */
    public static class PutData implements IgniteInClosure<Ignite> {
        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ignite.active(true);

            CacheConfiguration<IgniteUuid, String> cacheCfg = new CacheConfiguration<>();

            cacheCfg.setName(TEST_CACHE_NAME);
            cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cacheCfg.setBackups(1);
            cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

            IgniteCache<IgniteUuid, String> cache = ignite.createCache(cacheCfg);

            for (String v : VALUES)
                cache.put(IgniteUuid.randomUuid(), v);
        }
    }
}
