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

package org.apache.ignite.client;

import java.util.UUID;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/** Reusable system tests configuration. */
public class Config {
    /** Host. */
    public static final String SERVER = "127.0.0.1:10800";

    /** Name of the cache created by default in the cluster. */
    public static final String DEFAULT_CACHE_NAME = "default";

    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder().setShared(true);

    /** */
    public static IgniteConfiguration getServerConfiguration() {
        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();

        discoverySpi.setIpFinder(ipFinder);

        IgniteConfiguration igniteCfg = new IgniteConfiguration();

        igniteCfg.setDiscoverySpi(discoverySpi);

        CacheConfiguration<Integer, Person> dfltCacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        dfltCacheCfg.setIndexedTypes(Integer.class, Person.class);

        igniteCfg.setCacheConfiguration(dfltCacheCfg);

        igniteCfg.setIgniteInstanceName(UUID.randomUUID().toString());

        return igniteCfg;
    }
}
