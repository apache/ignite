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

package org.apache.ignite.tests.p2p.startcache;

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 *
 */
public class CacheConfigurationP2PTestClient {
    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        System.out.println("Starting test client node.");

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setPeerClassLoadingEnabled(true);

        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500..47509"));

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        try (Ignite ignite = Ignition.start(cfg)) {
            System.out.println("Test external node started");

            int nodes = ignite.cluster().nodes().size();

            if (nodes != 4)
                throw new Exception("Unexpected nodes number: " + nodes);

            CacheConfiguration<Integer, Organization1> ccfg1 = new CacheConfiguration<>();

            ccfg1.setName("cache1");

            ccfg1.setNodeFilter(new CacheAllNodesFilter());

            ccfg1.setIndexedTypes(Integer.class, Organization1.class);

            System.out.println("Create cache1.");

            IgniteCache<Integer, Organization1> cache1 = ignite.createCache(ccfg1);

            for (int i = 0; i < 500; i++)
                cache1.put(i, new Organization1("org-" + i));

            SqlQuery<Integer, Organization1> qry1 = new SqlQuery<>(Organization1.class, "_key >= 0");

            int cnt = cache1.query(qry1).getAll().size();

            if (cnt != 500)
                throw new Exception("Unexpected query result: " + cnt);

            System.out.println("Sleep some time.");

            Thread.sleep(5000); // Sleep some time to wait when connection of p2p loader is closed.

            System.out.println("Create cache2.");

            CacheConfiguration<Integer, Organization2> ccfg2 = new CacheConfiguration<>();

            ccfg2.setName("cache2");

            ccfg2.setIndexedTypes(Integer.class, Organization2.class);

            IgniteCache<Integer, Organization2> cache2 = ignite.createCache(ccfg2);

            for (int i = 0; i < 600; i++)
                cache2.put(i, new Organization2("org-" + i));

            SqlQuery<Integer, Organization2> qry2 = new SqlQuery<>(Organization2.class, "_key >= 0");

            cnt = cache2.query(qry2).getAll().size();

            if (cnt != 600)
                throw new Exception("Unexpected query result: " + cnt);

            cache1.destroy();

            cache2.destroy();
        }
    }
}
