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

package org.apache.ignite.raft.server;

import java.util.List;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.raft.client.message.RaftClientMessagesFactory;

/**
 * Abstract test for raft server.
 */
abstract class RaftServerAbstractTest {
    /** */
    protected static final IgniteLogger LOG = IgniteLogger.forClass(RaftServerAbstractTest.class);

    /** */
    protected static final RaftClientMessagesFactory FACTORY = new RaftClientMessagesFactory();

    /** Network factory. */
    protected static final ClusterServiceFactory NETWORK_FACTORY = new TestScaleCubeClusterServiceFactory();

    /**
     * Server port offset.
     */
    protected static final int PORT = 20010;

    /** */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistryImpl();

    /**
     * @param name Node name.
     * @param port Local port.
     * @param servers Server nodes of the cluster.
     * @return The client cluster view.
     */
    protected ClusterService clusterService(String name, int port, List<NetworkAddress> servers, boolean start) {
        var context = new ClusterLocalConfiguration(name, port, servers, SERIALIZATION_REGISTRY);

        var network = NETWORK_FACTORY.createClusterService(context);

        if (start)
            network.start();

        return network;
    }
}
