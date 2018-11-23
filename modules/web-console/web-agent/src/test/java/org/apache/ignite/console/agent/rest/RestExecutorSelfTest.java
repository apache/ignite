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

package org.apache.ignite.console.agent.rest;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.junit.Test;

/**
 * Test for RestExecutor.
 */
public class RestExecutorSelfTest {
    /** Name of the cache created by default in the cluster. */
    private static final String DEFAULT_CACHE_NAME = "default";

    /** */
    private static final List<String> NODE_URI = Collections.singletonList("http://localhost:8080");

    /** */
    public static IgniteConfiguration getServerConfiguration() {
        TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.registerAddresses(Collections.singletonList(new InetSocketAddress("127.0.0.1", 47500)));

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();

        discoverySpi.setIpFinder(ipFinder);

        IgniteConfiguration igniteCfg = new IgniteConfiguration();

        igniteCfg.setDiscoverySpi(discoverySpi);

        CacheConfiguration<Integer, String> dfltCacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        igniteCfg.setCacheConfiguration(dfltCacheCfg);

        igniteCfg.setIgniteInstanceName(UUID.randomUUID().toString());

        return igniteCfg;
    }

    /**
     * Test REST commands.
     */
    @Test
    public void testRest() throws Exception {
        IgniteConfiguration srvCfg = getServerConfiguration();

        try(Ignite ignite = Ignition.getOrStart(srvCfg)) {
            RestExecutor exec = new RestExecutor(null, null, null, null);

            Map<String, Object> params = new HashMap<>();
            params.put("cmd", "top");
            params.put("attr", false);
            params.put("mtr", false);
            params.put("caches", false);

            RestResult res = exec.sendRequest(NODE_URI, params, null);

            exec.close();
        }
    }
}
