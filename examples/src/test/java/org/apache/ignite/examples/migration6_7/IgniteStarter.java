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

package org.apache.ignite.examples.migration6_7;

import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 */
public class IgniteStarter {
    /** */
    public static final String PARTITIONED = "partitioned";

    /**
     * @param args Args.
     */
    public static void main(String[] args) throws Exception{
        try(Ignite ignore = Ignition.start(configuration())) {
            Thread.sleep(Migration6_7.SLEEP);
        }
    }

    /**
     */
    public static IgniteConfiguration configuration() {
        CacheConfiguration cc = new CacheConfiguration()
            .setName(PARTITIONED);

        CacheConfiguration personCc = new CacheConfiguration()
            .setName("personCache");

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Collections.singleton("127.0.0.1:47600"));

        TcpDiscoverySpi disco = new TcpDiscoverySpi().setIpFinder(ipFinder);
        disco.setLocalPort(47600);

        IgniteConfiguration c = new IgniteConfiguration()
            .setCacheConfiguration(cc, personCc)
            .setDiscoverySpi(disco)
            .setLocalHost("127.0.0.1")
            ;

        return c;
    }
}
