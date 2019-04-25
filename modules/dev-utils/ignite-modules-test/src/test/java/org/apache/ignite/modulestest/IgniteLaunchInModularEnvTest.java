/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.modulestest;

import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 *
 */
public class IgniteLaunchInModularEnvTest {
    /**
     * Tests ignite startup without any features used.
     */
    @Test
    public void testSimpleLaunch() {
        IgniteConfiguration cfg = igniteConfiguration();

        Ignite ignite = Ignition.start(cfg);

        ignite.close();
    }

    @Test
    public void testPdsEnabledSimpleLaunch() {
        IgniteConfiguration cfg = igniteConfiguration();

        DataRegionConfiguration regCfg = new DataRegionConfiguration()
            .setMaxSize(256L * 1024 * 1024)
            .setPersistenceEnabled(true);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(regCfg));

        Ignite ignite = Ignition.start(cfg);

        ignite.cluster().active(true);

        String cacheName = "CACHE";
        ignite.getOrCreateCache(cacheName).put("key", "value");
        ignite.close();
    }


    @Before
    public void cleanPersistenceDir() throws Exception {
        assertTrue("Grids are not stopped", F.isEmpty(G.allGrids()));

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "cp", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "marshaller", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "binary_meta", false));
    }

    /**
     * @return default configuration for test without spring module.
     */
    private IgniteConfiguration igniteConfiguration() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder();
        finder.setAddresses(Collections.singletonList("127.0.0.1"));
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(finder));
        return cfg;
    }
}
