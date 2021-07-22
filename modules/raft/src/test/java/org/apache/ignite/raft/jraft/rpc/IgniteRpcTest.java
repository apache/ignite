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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcClient;
import org.apache.ignite.raft.jraft.util.Endpoint;

import static org.apache.ignite.raft.jraft.JRaftUtils.addressFromEndpoint;

/**
 *
 */
public class IgniteRpcTest extends AbstractRpcTest {
    /** The counter. */
    private final AtomicInteger cntr = new AtomicInteger();

    /** {@inheritDoc} */
    @Override public RpcServer<?> createServer(Endpoint endpoint) {
        ClusterService service = createService(endpoint.toString(), endpoint.getPort());

        var server = new TestIgniteRpcServer(service, List.of(), new NodeManager(), new NodeOptions()) {
            @Override public void shutdown() {
                super.shutdown();

                service.shutdown();
            }
        };

        service.start();

        return server;
    }

    /** {@inheritDoc} */
    @Override public RpcClient createClient0() {
        int i = cntr.incrementAndGet();

        ClusterService service = createService("client" + i, endpoint.getPort() - i, addressFromEndpoint(endpoint));

        IgniteRpcClient client = new IgniteRpcClient(service) {
            @Override public void shutdown() {
                super.shutdown();

                service.shutdown();
            }
        };

        service.start();

        waitForTopology(client, 1 + i, 5_000);

        return client;
    }

    /**
     * @param name Node name.
     * @param port Local port.
     * @param servers Server nodes of the cluster.
     * @return The client cluster view.
     */
    private static ClusterService createService(String name, int port, NetworkAddress... servers) {
        var registry = new MessageSerializationRegistryImpl();
        var context = new ClusterLocalConfiguration(name, port, List.of(servers), registry);
        var factory = new TestScaleCubeClusterServiceFactory();

        return factory.createClusterService(context);
    }

    /** {@inheritDoc} */
    @Override protected boolean waitForTopology(RpcClient client, int expected, long timeout) {
        IgniteRpcClient client0 = (IgniteRpcClient) client;

        ClusterService service = client0.clusterService();

        return waitForTopology(service, expected, timeout);
    }

    /**
     * @param service The service.
     * @param expected Expected count.
     * @param timeout The timeout.
     * @return Wait status.
     */
    protected boolean waitForTopology(ClusterService service, int expected, long timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while (System.currentTimeMillis() < stop) {
            if (service.topologyService().allMembers().size() == expected)
                return true;

            try {
                Thread.sleep(50);
            }
            catch (InterruptedException e) {
                return false;
            }
        }

        return false;
    }
}
