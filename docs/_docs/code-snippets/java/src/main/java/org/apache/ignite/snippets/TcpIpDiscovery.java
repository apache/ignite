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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.Collections;
import java.util.logging.Logger;
import javax.sql.DataSource;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.jdbc.TcpDiscoveryJdbcIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.zk.TcpDiscoveryZookeeperIpFinder;
import org.junit.jupiter.api.Test;

public class TcpIpDiscovery {

    @Test
    void multicastIpFinderDemo() {
        //tag::multicast[]
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();

        ipFinder.setMulticastGroup("228.10.10.157");

        spi.setIpFinder(ipFinder);

        IgniteConfiguration cfg = new IgniteConfiguration();

        // Override default discovery SPI.
        cfg.setDiscoverySpi(spi);

        // Start the node.
        Ignite ignite = Ignition.start(cfg);
        //end::multicast[]
        ignite.close();
    }

    @Test
    void failureDetectionTimeout() {
        //tag::failure-detection-timeout[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setFailureDetectionTimeout(5_000);

        cfg.setClientFailureDetectionTimeout(10_000);
        //end::failure-detection-timeout[]
    }

    @Test
    void staticIpFinderDemo() {
        //tag::static[]
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        // Set initial IP addresses.
        // Note that you can optionally specify a port or a port range.
        ipFinder.setAddresses(Arrays.asList("1.2.3.4", "1.2.3.5:47500..47509"));

        spi.setIpFinder(ipFinder);

        IgniteConfiguration cfg = new IgniteConfiguration();

        // Override default discovery SPI.
        cfg.setDiscoverySpi(spi);

        // Start a node.
        Ignite ignite = Ignition.start(cfg);
        //end::static[]
        ignite.close();
    }

    @Test
    void multicastAndStaticDemo() {
        //tag::multicastAndStatic[]
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();

        // Set Multicast group.
        ipFinder.setMulticastGroup("228.10.10.157");

        // Set initial IP addresses.
        // Note that you can optionally specify a port or a port range.
        ipFinder.setAddresses(Arrays.asList("1.2.3.4", "1.2.3.5:47500..47509"));

        spi.setIpFinder(ipFinder);

        IgniteConfiguration cfg = new IgniteConfiguration();

        // Override default discovery SPI.
        cfg.setDiscoverySpi(spi);

        // Start a node.
        Ignite ignite = Ignition.start(cfg);
        //end::multicastAndStatic[]
        ignite.close();
    }

    @Test
    void isolatedClustersDemo() {
        //tag::isolated1[]
        IgniteConfiguration firstCfg = new IgniteConfiguration();

        firstCfg.setIgniteInstanceName("first");

        // Explicitly configure TCP discovery SPI to provide list of initial nodes
        // from the first cluster.
        TcpDiscoverySpi firstDiscoverySpi = new TcpDiscoverySpi();

        // Initial local port to listen to.
        firstDiscoverySpi.setLocalPort(48500);

        // Changing local port range. This is an optional action.
        firstDiscoverySpi.setLocalPortRange(20);

        TcpDiscoveryVmIpFinder firstIpFinder = new TcpDiscoveryVmIpFinder();

        // Addresses and port range of the nodes from the first cluster.
        // 127.0.0.1 can be replaced with actual IP addresses or host names.
        // The port range is optional.
        firstIpFinder.setAddresses(Collections.singletonList("127.0.0.1:48500..48520"));

        // Overriding IP finder.
        firstDiscoverySpi.setIpFinder(firstIpFinder);

        // Explicitly configure TCP communication SPI by changing local port number for
        // the nodes from the first cluster.
        TcpCommunicationSpi firstCommSpi = new TcpCommunicationSpi();

        firstCommSpi.setLocalPort(48100);

        // Overriding discovery SPI.
        firstCfg.setDiscoverySpi(firstDiscoverySpi);

        // Overriding communication SPI.
        firstCfg.setCommunicationSpi(firstCommSpi);

        // Starting a node.
        Ignition.start(firstCfg);
        //end::isolated1[]

        //tag::isolated2[]
        IgniteConfiguration secondCfg = new IgniteConfiguration();

        secondCfg.setIgniteInstanceName("second");

        // Explicitly configure TCP discovery SPI to provide list of initial nodes
        // from the second cluster.
        TcpDiscoverySpi secondDiscoverySpi = new TcpDiscoverySpi();

        // Initial local port to listen to.
        secondDiscoverySpi.setLocalPort(49500);

        // Changing local port range. This is an optional action.
        secondDiscoverySpi.setLocalPortRange(20);

        TcpDiscoveryVmIpFinder secondIpFinder = new TcpDiscoveryVmIpFinder();

        // Addresses and port range of the nodes from the second cluster.
        // 127.0.0.1 can be replaced with actual IP addresses or host names.
        // The port range is optional.
        secondIpFinder.setAddresses(Collections.singletonList("127.0.0.1:49500..49520"));

        // Overriding IP finder.
        secondDiscoverySpi.setIpFinder(secondIpFinder);

        // Explicitly configure TCP communication SPI by changing local port number for
        // the nodes from the second cluster.
        TcpCommunicationSpi secondCommSpi = new TcpCommunicationSpi();

        secondCommSpi.setLocalPort(49100);

        // Overriding discovery SPI.
        secondCfg.setDiscoverySpi(secondDiscoverySpi);

        // Overriding communication SPI.
        secondCfg.setCommunicationSpi(secondCommSpi);

        // Starting a node.
        Ignition.start(secondCfg);
        //end::isolated2[]

        Ignition.ignite("first").close();
        Ignition.ignite("second").close();
    }

    static class MySampleDataSource implements DataSource {

        @Override
        public Connection getConnection() throws SQLException {
            return null;
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return null;
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return null;
        }

        @Override
        public void setLogWriter(PrintWriter out) throws SQLException {

        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException {

        }

        @Override
        public int getLoginTimeout() throws SQLException {
            return 0;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return null;
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return null;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return false;
        }
    }

    @Test
    void jdbcIpFinderDemo() {
        //tag::jdbc[]
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        // Configure your DataSource.
        DataSource someDs = new MySampleDataSource();

        TcpDiscoveryJdbcIpFinder ipFinder = new TcpDiscoveryJdbcIpFinder();

        ipFinder.setDataSource(someDs);

        spi.setIpFinder(ipFinder);

        IgniteConfiguration cfg = new IgniteConfiguration();

        // Override default discovery SPI.
        cfg.setDiscoverySpi(spi);

        // Start the node.
        Ignite ignite = Ignition.start(cfg);
        //end::jdbc[]
        ignite.close();
    }

    void sharedFileSystemIpFinderDemo() {

        //tag::sharedFS[]
        // Configuring discovery SPI.
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        // Configuring IP finder.
        TcpDiscoverySharedFsIpFinder ipFinder = new TcpDiscoverySharedFsIpFinder();

        ipFinder.setPath("/var/ignite/addresses");

        spi.setIpFinder(ipFinder);

        IgniteConfiguration cfg = new IgniteConfiguration();

        // Override default discovery SPI.
        cfg.setDiscoverySpi(spi);

        // Start the node.
        Ignite ignite = Ignition.start(cfg);
        //end::sharedFS[]
        ignite.close();
    }

    @Test
    void zookeeperIpFinderDemo() {

        //tag::zk[]
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        TcpDiscoveryZookeeperIpFinder ipFinder = new TcpDiscoveryZookeeperIpFinder();

        // Specify ZooKeeper connection string.
        ipFinder.setZkConnectionString("127.0.0.1:2181");

        spi.setIpFinder(ipFinder);

        IgniteConfiguration cfg = new IgniteConfiguration();

        // Override default discovery SPI.
        cfg.setDiscoverySpi(spi);

        // Start the node.
        Ignite ignite = Ignition.start(cfg);
        //end::zk[]

        ignite.close();
    }
}
