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

package org.apache.ignite.compatibility.cache;

import java.io.File;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.compatibility.persistence.IgnitePersistenceCompatibilityAbstractTest;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Tests that local cache with persistence enabled can be started on a new version of AI.
 * This test should be removed along with GridCacheProcessor#LocalAffinityFunction.
 */
public class LocalCacheTest extends IgnitePersistenceCompatibilityAbstractTest {
    /** Consistent id. */
    private static final String CONSISTENT_ID = "test-local-cache-id";

    /** Ignite version. */
    private static final String IGNITE_VERSION = "2.8.0";

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMigrationLocalCacheWithPersistenceEnabled() throws Exception {
        try {
            U.delete(new File(U.defaultWorkDirectory()));

            startGrid(1, IGNITE_VERSION, new ConfigurationClosure(CONSISTENT_ID), new ActivateClosure());

            stopAllGrids();

            Ignite ig0 = IgnitionEx.start(prepareConfig(getConfiguration(), CONSISTENT_ID));

            ig0.close();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Updates the given ignite configuration and specifies a local cache with persistence enabled.
     *
     * @param cfg Ignite configuration to be updated.
     * @param consistentId Consistent id.
     * @return Updated configuration.
     */
    private static IgniteConfiguration prepareConfig(IgniteConfiguration cfg, @Nullable String consistentId) {
        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi disco = new TcpDiscoverySpi();
        disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setPeerClassLoadingEnabled(false);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setInitialSize(10L * 1024 * 1024)
                    .setMaxSize(10L * 1024 * 1024))
            .setPageSize(4096);

        cfg.setDataStorageConfiguration(memCfg);

        if (consistentId != null) {
            cfg.setIgniteInstanceName(consistentId);
            cfg.setConsistentId(consistentId);
        }

        CacheConfiguration<Object, Object> locCacheCfg = new CacheConfiguration<>("test-local-cache");
        locCacheCfg.setCacheMode(CacheMode.LOCAL);
        cfg.setCacheConfiguration(locCacheCfg);

        return cfg;
    }

    /** */
    private static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** Consistent id. */
        private final String consistentId;

        /**
         * Creates a new instance of Configuration closure.
         *
         * @param consistentId Consistent id.
         */
        public ConfigurationClosure(String consistentId) {
            this.consistentId = consistentId;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            prepareConfig(cfg, consistentId);
        }
    }

    /**
     * Post-startup close that activates the grid.
     */
    private static class ActivateClosure implements IgniteInClosure<Ignite> {
        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ignite.cluster().active(true);
        }
    }
}
