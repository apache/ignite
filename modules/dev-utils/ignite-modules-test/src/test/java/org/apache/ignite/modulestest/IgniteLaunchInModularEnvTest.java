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

package org.apache.ignite.modulestest;

import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.junit.Test;

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

        DataRegionConfiguration regCfg = new DataRegionConfiguration();
        regCfg.setMaxSize(256L*1024*1024);
        regCfg.setPersistenceEnabled(true);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(regCfg));

        Ignite ignite = Ignition.start(cfg);

        ignite.cluster().active(true);

        String cacheName = "CACHE";
        ignite.getOrCreateCache(cacheName).put("key", "value");
        ignite.close();
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
