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
package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.junit.jupiter.api.Test;

public class ClientNodes {

    @Test
    void disableReconnection() {

        //tag::disable-reconnection[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        discoverySpi.setClientReconnectDisabled(true);

        cfg.setDiscoverySpi(discoverySpi);
        //end::disable-reconnection[]

        try (Ignite ignite = Ignition.start(cfg)) {

        }
    }

    void slowClient() {
        //tag::slow-clients[]
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(true);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
        commSpi.setSlowClientQueueLimit(1000);

        cfg.setCommunicationSpi(commSpi);
        //end::slow-clients[]
    }

    void reconnect() {
        Ignite ignite = Ignition.start();

        //tag::reconnect[]

        IgniteCache cache = ignite.getOrCreateCache(new CacheConfiguration<>("myCache"));

        try {
            cache.put(1, "value");
        } catch (IgniteClientDisconnectedException e) {
            if (e.getCause() instanceof IgniteClientDisconnectedException) {
                IgniteClientDisconnectedException cause = (IgniteClientDisconnectedException) e.getCause();

                cause.reconnectFuture().get(); // Wait until the client is reconnected. 
                // proceed
            }
        }
        //end::reconnect[]

        ignite.close();
    }
}
