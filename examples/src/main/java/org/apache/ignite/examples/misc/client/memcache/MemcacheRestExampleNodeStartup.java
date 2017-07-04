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

package org.apache.ignite.examples.misc.client.memcache;

import java.util.Collections;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.DeploymentMode.SHARED;

/**
 * Starts up an empty node with cache configuration that contains default cache.
 * <p>
 * The difference is that running this class from IDE adds all example classes to classpath
 * but running from command line doesn't.
 */
public class MemcacheRestExampleNodeStartup {
    /**
     * Start up an empty node with specified cache configuration.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        Ignition.start(configuration());
    }

    /**
     * Create Ignite configuration with IGFS and enabled IPC.
     *
     * @return Ignite configuration.
     * @throws IgniteException If configuration creation failed.
     */
    public static IgniteConfiguration configuration() throws IgniteException {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setLocalHost("127.0.0.1");
        cfg.setDeploymentMode(SHARED);
        cfg.setPeerClassLoadingEnabled(true);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName("default");
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setRebalanceMode(SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(cacheCfg);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }
}