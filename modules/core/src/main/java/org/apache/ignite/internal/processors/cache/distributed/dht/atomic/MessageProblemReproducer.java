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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

/**
 * Message problem reproducer.
 */
public class MessageProblemReproducer {
    /**
     * Entry point.
     */
    public static void main(String[] args) throws Exception {
        Ignite node1 = Ignition.start(config("1"));
        Ignite node2 = Ignition.start(config("2"));

        node1.message().localListen("topic", new IgniteBiPredicate<UUID, byte[]>() {
            @Override public boolean apply(UUID uuid, byte[] o) {
                System.out.println("Received: " + Arrays.toString(o));

                return true;
            }
        });

        node2.message().send("topic", new byte[150000]);

        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * Create configuration.
     *
     * @param name Name.
     * @return Configuration.
     */
    private static IgniteConfiguration config(String name) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName(name);
        cfg.setLocalHost("127.0.0.1");

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Collections.singleton("127.0.0.1:47500"));

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi().setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }
}
