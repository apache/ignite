/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Collections;

public class Runner {
    public static void main(String[] args) {
        ClientConnectorConfiguration connectorConfiguration = new ClientConnectorConfiguration().setPort(10890);

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder()
                .setAddresses(Collections.singleton("127.0.0.1:47500..47501"));

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi().setIpFinder(ipFinder).setSocketTimeout(300);

        IgniteConfiguration cfg = new IgniteConfiguration()
                .setClientConnectorConfiguration(connectorConfiguration)
                .setDiscoverySpi(discoSpi);

        Ignition.start(cfg);
    }
}
