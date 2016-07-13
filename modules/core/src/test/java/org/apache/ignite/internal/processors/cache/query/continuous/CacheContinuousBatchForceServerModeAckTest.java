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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.io.Serializable;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 * Continuous queries tests.
 */
public class CacheContinuousBatchForceServerModeAckTest extends CacheContinuousBatchAckTest implements Serializable {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (gridName.endsWith(CLIENT)) {
            cfg.setClientMode(true);

            FailedTcpCommunicationSpi spi = new FailedTcpCommunicationSpi(true, false);

            cfg.setCommunicationSpi(spi);

            TcpDiscoverySpi disco = new TcpDiscoverySpi();

            disco.setForceServerMode(true);

            disco.setIpFinder(IP_FINDER);

            cfg.setDiscoverySpi(disco);
        }
        else if (gridName.endsWith(SERVER2)) {
            cfg.setCommunicationSpi(new FailedTcpCommunicationSpi(false, true));

            TcpDiscoverySpi disco = new TcpDiscoverySpi();

            disco.setIpFinder(IP_FINDER);

            cfg.setDiscoverySpi(disco);
        }
        else {
            cfg.setCommunicationSpi(new FailedTcpCommunicationSpi(false, false));

            TcpDiscoverySpi disco = new TcpDiscoverySpi();

            disco.setIpFinder(IP_FINDER);

            cfg.setDiscoverySpi(disco);
        }

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }
}
