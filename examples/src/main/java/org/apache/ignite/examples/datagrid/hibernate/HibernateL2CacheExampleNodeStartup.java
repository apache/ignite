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

package org.apache.ignite.examples.datagrid.hibernate;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;

import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Starts up an empty node with example cache configuration.
 */
public class HibernateL2CacheExampleNodeStartup {
    /**
     * Start up an empty node with specified cache configuration.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws IgniteCheckedException {
        Ignition.start(configuration());
    }

    /**
     * Create Grid configuration with GGFS and enabled IPC.
     *
     * @return Grid configuration.
     * @throws IgniteCheckedException If configuration creation failed.
     */
    public static IgniteConfiguration configuration() throws IgniteCheckedException {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName("hibernate-grid");
        cfg.setLocalHost("127.0.0.1");
        cfg.setRestEnabled(false);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration(
            cacheConfiguration("org.hibernate.cache.spi.UpdateTimestampsCache", ATOMIC),
            cacheConfiguration("org.hibernate.cache.internal.StandardQueryCache", ATOMIC),
            cacheConfiguration("org.apache.ignite.examples.datagrid.hibernate.User", TRANSACTIONAL),
            cacheConfiguration("org.apache.ignite.examples.datagrid.hibernate.User.posts", TRANSACTIONAL),
            cacheConfiguration("org.apache.ignite.examples.datagrid.hibernate.Post", TRANSACTIONAL)
        );

        return cfg;
    }

    /**
     * Create cache configuration.
     *
     * @param name Cache name.
     * @param atomicityMode Atomicity mode.
     * @return Cache configuration.
     */
    private static CacheConfiguration cacheConfiguration(String name, CacheAtomicityMode atomicityMode) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        return ccfg;
    }
}
