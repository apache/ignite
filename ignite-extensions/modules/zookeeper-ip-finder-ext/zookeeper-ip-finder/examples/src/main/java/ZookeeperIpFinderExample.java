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

import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.zk.TcpDiscoveryZookeeperIpFinder;

/**
 * This example demonstrates starting Ignite cluster with configured {@link TcpDiscoveryZookeeperIpFinder}
 * to discover other nodes.
 * <p>
 * To start the example, you should:
 * <ol>
 *  <li>Start Apache ZooKeeper. See <a href="https://zookeeper.apache.org">Apache ZooKeeper</a>.</li>
 *  <li>Make sure that the Apache ZooKeeper connection string is correct. See {@link ZK_CONNECT_STRING}.</li>
 *  <li>Start example using {@link ZookeeperIpFinderExample}.</li>
 * </ol>
 */
public class ZookeeperIpFinderExample {
    /** The Apache ZooKeeper connection string. Comma separated host:port pairs, each corresponding to a zk server. */
    private static final String ZK_CONNECT_STRING = "localhost:2181";

    /**
     * Start example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        try (Ignite server1 = Ignition.start(configuration("server1"));
             Ignite server2 =  Ignition.start(configuration("server2"))) {
            System.out.println();
            System.out.println("Zookeeper Ip Finder example started.");

            System.out.println();
            System.out.format(">>> Nodes identifiers [%s].\n",
                server1.cluster().nodes().stream().map(ClusterNode::id).collect(Collectors.toList()));
        }
    }

    /**
     * Returns a new instance of Ignite configuration.
     *
     * @param igniteInstanceName Ignite instance name.
     */
    private static IgniteConfiguration configuration(String igniteInstanceName) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setDiscoverySpi(new TcpDiscoverySpi()
            .setIpFinder(new TcpDiscoveryZookeeperIpFinder()
                .setZkConnectionString(ZK_CONNECT_STRING)));

        cfg.setIgniteInstanceName(igniteInstanceName);

        return cfg;
    }
}
