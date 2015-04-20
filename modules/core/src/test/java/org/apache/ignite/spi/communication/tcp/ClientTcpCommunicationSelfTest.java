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

package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.testframework.junits.spi.*;

import java.util.*;

/**
 *
 */
@GridSpiTest(spi = TcpCommunicationSpi.class, group = "Communication SPI")
public class ClientTcpCommunicationSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (client) {
            TcpCommunicationSpi spi = new TcpCommunicationSpi();
            spi.useRouter(true);

            cfg.setCommunicationSpi(spi);

            TcpDiscoveryVmIpFinder clientIpFinder = new TcpDiscoveryVmIpFinder();

            String addr = new ArrayList<>(ipFinder.getRegisteredAddresses()).iterator().next().toString();

            if (addr.startsWith("/"))
                addr = addr.substring(1);

            clientIpFinder.setAddresses(Arrays.asList(addr));

            TcpClientDiscoverySpi discoSpi = new TcpClientDiscoverySpi();

            discoSpi.setIpFinder(clientIpFinder);

            cfg.setDiscoverySpi(discoSpi);

            cfg.setClientMode(true);
        }
        else {
            TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();

            discoverySpi.setIpFinder(ipFinder);

            cfg.setDiscoverySpi(discoverySpi);
        }

        return cfg;
    }

    /**
     * @throws Exception
     */
    public void testClientCommunication() throws Exception {
        startGrid(0);
        startGrid(1);

        client = true;

        startGrid(2);
        startGrid(3);

        Collection<UUID> uuids = ignite(1).compute().broadcast(new IdCollector());

        for (int i = 0; i < 4; i++)
            assert uuids.contains(ignite(i).cluster().localNode().id());
    }

    /**
     *
     */
    private static class IdCollector implements IgniteCallable<UUID> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public UUID call() throws Exception {
            return ignite.cluster().localNode().id();
        }
    }

}
