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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import java.net.*;
import java.util.*;

/**
 * Tests {@link TcpClientDiscoverySpi}.
 */
public abstract class GridCacheClientModesTcpClientDiscoveryAbstractTest extends GridCacheClientModesAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected boolean isClientStartedLast() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (cfg.isClientMode() != null && cfg.isClientMode()) {
            TcpDiscoveryVmIpFinder clientFinder = new TcpDiscoveryVmIpFinder();

            ArrayList<String> addrs = new ArrayList<>(ipFinder.getRegisteredAddresses().size());

            for (InetSocketAddress sockAddr : ipFinder.getRegisteredAddresses())
                addrs.add(sockAddr.getHostString() + ":" + sockAddr.getPort());

            clientFinder.setAddresses(addrs);

            TcpClientDiscoverySpi discoverySpi = new TcpClientDiscoverySpi();
            discoverySpi.setIpFinder(clientFinder);

            cfg.setDiscoverySpi(discoverySpi);
        }

        cfg.setLocalHost("127.0.0.1");

        return cfg;
    }
}
