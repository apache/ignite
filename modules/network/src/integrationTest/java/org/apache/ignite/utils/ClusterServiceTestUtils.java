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

package org.apache.ignite.utils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.configuration.schemas.network.NodeFinderType;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeFinder;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.junit.jupiter.api.TestInfo;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;

/**
 * Test utils that provide sort of cluster service mock that manages required node configuration internally.
 */
public class ClusterServiceTestUtils {
    /**
     * Creates a cluster service and required node configuration manager beneath it.
     * Populates node configuration with specified port.
     * Manages configuration manager lifecycle: on cluster service start starts node configuration manager,
     * on cluster service stop - stops node configuration manager.
     *
     * @param testInfo Test info.
     * @param port Local port.
     * @param nodeFinder Node finder.
     * @param msgSerializationRegistry Message serialization registry.
     * @param clusterSvcFactory Cluster service factory.
     */
    public static ClusterService clusterService(
        TestInfo testInfo,
        int port,
        NodeFinder nodeFinder,
        MessageSerializationRegistry msgSerializationRegistry,
        ClusterServiceFactory clusterSvcFactory
    ) {
        var ctx = new ClusterLocalConfiguration(testNodeName(testInfo, port), msgSerializationRegistry);

        ConfigurationManager nodeConfigurationMgr = new ConfigurationManager(
            Collections.singleton(NetworkConfiguration.KEY),
            Map.of(),
            new TestConfigurationStorage(ConfigurationType.LOCAL),
            List.of(),
            List.of()
        );

        var clusterSvc = clusterSvcFactory.createClusterService(
            ctx,
            nodeConfigurationMgr.configurationRegistry().getConfiguration(NetworkConfiguration.KEY)
        );

        assert nodeFinder instanceof StaticNodeFinder : "Only StaticNodeFinder is supported at the moment";

        return new ClusterService() {
            @Override public TopologyService topologyService() {
                return clusterSvc.topologyService();
            }

            @Override public MessagingService messagingService() {
                return clusterSvc.messagingService();
            }

            @Override public ClusterLocalConfiguration localConfiguration() {
                return clusterSvc.localConfiguration();
            }

            @Override public boolean isStopped() {
                return clusterSvc.isStopped();
            }

            @Override public void start() {
                nodeConfigurationMgr.start();

                NetworkConfiguration configuration = nodeConfigurationMgr.configurationRegistry()
                    .getConfiguration(NetworkConfiguration.KEY);

                configuration.
                    change(netCfg ->
                        netCfg
                            .changePort(port)
                            .changeNodeFinder(c -> c
                                .changeType(NodeFinderType.STATIC.toString())
                                .changeNetClusterNodes(
                                    nodeFinder.findNodes().stream().map(NetworkAddress::toString).toArray(String[]::new)
                                )
                            )
                    ).join();

                clusterSvc.start();
            }

            @Override public void stop() {
                clusterSvc.stop();
                nodeConfigurationMgr.stop();
            }
        };
    }

    /**
     * Creates a list of {@link NetworkAddress}es within a given port range.
     *
     * @param startPort Start port (inclusive).
     * @param endPort End port (exclusive).
     * @return Configuration closure.
     */
    public static List<NetworkAddress> findLocalAddresses(int startPort, int endPort) {
        return IntStream.range(startPort, endPort)
            .mapToObj(port -> new NetworkAddress("localhost", port))
            .collect(toUnmodifiableList());
    }
}
