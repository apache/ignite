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

package org.gridgain.examples.datagrid.store;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.gridgain.examples.datagrid.store.dummy.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;

import javax.cache.configuration.*;
import java.util.*;

import static org.apache.ignite.cache.GridCacheAtomicityMode.*;

/**
 * Starts up an empty node with example cache configuration.
 */
public class CacheNodeWithStoreStartup {
    /**
     * Start up an empty node with specified cache configuration.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws IgniteCheckedException {
        Ignition.start(configure());
    }

    /**
     * Configure grid.
     *
     * @return Grid configuration.
     * @throws IgniteCheckedException If failed.
     */
    public static IgniteConfiguration configure() throws IgniteCheckedException {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setLocalHost("127.0.0.1");

        // Discovery SPI.
        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();

        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500..47509"));

        discoSpi.setIpFinder(ipFinder);

        CacheConfiguration cacheCfg = new CacheConfiguration();

        // Set atomicity as transaction, since we are showing transactions in example.
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheStore store;

        // Uncomment other cache stores to try them.
        store = new CacheDummyPersonStore();
        // store = new CacheJdbcPersonStore();
        // store = new CacheHibernatePersonStore();

        cacheCfg.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(store));
        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }
}
