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

package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;

/**
 * Test for {@link TcpDiscoverySpi}.
 */
public class GridTcpDiscoveryConcurrentStartTest extends GridCommonAbstractTest {
    /** */
    private static final int TOP_SIZE = 1;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static volatile boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg =  super.getConfiguration(gridName);

        if (client) {
            TcpDiscoveryVmIpFinder clientIpFinder = new TcpDiscoveryVmIpFinder();

            String addr = new ArrayList<>(ipFinder.getRegisteredAddresses()).iterator().next().toString();

            if (addr.startsWith("/"))
                addr = addr.substring(1);

            clientIpFinder.setAddresses(Arrays.asList(addr));

            TcpClientDiscoverySpi discoSpi = new TcpClientDiscoverySpi();

            discoSpi.setIpFinder(clientIpFinder);

            cfg.setDiscoverySpi(discoSpi);
        }
        else {
            TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

            discoSpi.setIpFinder(ipFinder);

            cfg.setDiscoverySpi(discoSpi);
        }

        cfg.setLocalHost("127.0.0.1");

        cfg.setCacheConfiguration();

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentStart() throws Exception {
        for (int i = 0; i < 50; i++) {
            try {
                startGridsMultiThreaded(TOP_SIZE);
            }
            finally {
                stopAllGrids();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentStartClients() throws Exception {
        for (int i = 0; i < 50; i++) {
            try {
                client = false;

                startGrid();

                client = true;

                startGridsMultiThreaded(TOP_SIZE);
            }
            finally {
                stopAllGrids();
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }
}
