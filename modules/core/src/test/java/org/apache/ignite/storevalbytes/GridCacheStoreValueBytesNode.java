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

package org.apache.ignite.storevalbytes;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;

import java.util.*;

import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;

/**
 *
 */
public class GridCacheStoreValueBytesNode {
    /**
     * @return Discovery SPI.
     * @throws Exception If failed.
     */
    static TcpDiscoverySpi discovery() throws Exception {
        TcpDiscoverySpi disc = new TcpDiscoverySpi();

        disc.setLocalAddress("localhost");

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        Collection<String> addrs = new ArrayList<>();

        for (int i = 0; i < 10; i++)
            addrs.add("localhost:" + (TcpDiscoverySpi.DFLT_PORT + i));

        ipFinder.setAddresses(addrs);

        disc.setIpFinder(ipFinder);

        return disc;
    }

    /**
     * @param size Size.
     * @return Value.
     */
    static String createValue(int size) {
        StringBuilder str = new StringBuilder();

        str.append(new char[size]);

        return str.toString();
    }

    /**
     * @param args Arguments.
     * @param nearOnly Near only flag.
     * @return Configuration.
     * @throws Exception If failed.
     */
    static IgniteConfiguration parseConfiguration(String[] args, boolean nearOnly) throws Exception {
        boolean p2pEnabled = false;

        boolean storeValBytes = false;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            switch (arg) {
                case "-p2p":
                    p2pEnabled = Boolean.parseBoolean(args[++i]);

                    break;

                case "-storeValBytes":
                    storeValBytes = Boolean.parseBoolean(args[++i]);

                    break;
            }
        }

        X.println("Peer class loading enabled: " + p2pEnabled);
        X.println("Store value bytes: " + storeValBytes);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setDiscoverySpi(discovery());

        cfg.setPeerClassLoadingEnabled(p2pEnabled);

        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);

        cacheCfg.setBackups(1);

        if (nearOnly) {
            cacheCfg.setNearEvictionPolicy(new GridCacheAlwaysEvictionPolicy());

            cacheCfg.setDistributionMode(NEAR_ONLY);
        }

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        Ignition.start(parseConfiguration(args, false));
    }
}
