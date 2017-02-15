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

package org.apache.ignite.tests.p2p.cache;

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 *
 */
public class CacheNoValueClassOnServerTestClient {
    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        System.out.println("Starting test client node.");

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setPeerClassLoadingEnabled(false);

        cfg.setClientMode(true);

        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500..47509"));

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        try (Ignite ignite = Ignition.start(cfg)) {
            System.out.println("Test external node started");

            int nodes = ignite.cluster().nodes().size();

            if (nodes != 2)
                throw new Exception("Unexpected nodes number: " + nodes);

            IgniteCache<Integer, Person> cache = ignite.cache(null);

            for (int i = 0; i < 100; i++)
                cache.put(i, new Person("name-" + i));

            for (int i = 0; i < 100; i++) {
                Person p = cache.get(i);

                if (p == null)
                    throw new Exception("Null result key: " + i);

                String expName = "name-" + i;

                if (!expName.equals(p.name()))
                    throw new Exception("Unexpected data: " + p.name());

                if (i % 10 == 0)
                    System.out.println("Get expected value: " + p.name());
            }
        }
    }
}
