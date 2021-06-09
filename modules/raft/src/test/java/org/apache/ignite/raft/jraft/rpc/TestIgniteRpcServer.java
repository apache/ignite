/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.raft.jraft.rpc;

import java.util.List;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.scalecube.ScaleCubeClusterServiceFactory;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.raft.client.message.RaftClientMessagesFactory;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcServer;
import org.apache.ignite.raft.jraft.util.Endpoint;

/**
 * RPC server configured for integration tests.
 */
public class TestIgniteRpcServer extends IgniteRpcServer {
    /**
     * The registry.
     */
    /** */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistryImpl();

    /**
     * Service factory.
     */
    private final static ScaleCubeClusterServiceFactory SERVICE_FACTORY = new TestScaleCubeClusterServiceFactory();

    /**
     * Message factory.
     */
    private static final RaftClientMessagesFactory MSG_FACTORY = new RaftClientMessagesFactory();

    /**
     * @param endpoint The endpoint.
     * @param nodeManager The node manager.
     */
    public TestIgniteRpcServer(Endpoint endpoint, NodeManager nodeManager) {
        this(endpoint.getIp() + ":" + endpoint.getPort(), endpoint.getPort(), List.of(), nodeManager);
    }

    /**
     * @param endpoint The endpoint.
     * @param servers Server list.
     * @param nodeManager The node manager.
     */
    public TestIgniteRpcServer(Endpoint endpoint, List<String> servers, NodeManager nodeManager) {
        this(endpoint.getIp() + ":" + endpoint.getPort(), endpoint.getPort(), servers, nodeManager);
    }

    /**
     * @param name The name.
     * @param port The port.
     * @param servers Server list.
     * @param nodeManager The node manager.
     */
    public TestIgniteRpcServer(String name, int port, List<String> servers, NodeManager nodeManager) {
        super(SERVICE_FACTORY.createClusterService(new ClusterLocalConfiguration(name, port, servers, SERIALIZATION_REGISTRY)),
            false, nodeManager, MSG_FACTORY, JRaftUtils.createRequestExecutor(new NodeOptions()));
    }
}
