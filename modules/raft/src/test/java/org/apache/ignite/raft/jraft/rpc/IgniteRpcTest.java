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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcClient;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.AfterEach;

import static org.apache.ignite.raft.jraft.JRaftUtils.addressFromEndpoint;

/**
 *
 */
public class IgniteRpcTest extends AbstractRpcTest {
    /** The counter. */
    private final AtomicInteger cntr = new AtomicInteger();

    /** Requests executor. */
    private ExecutorService requestExecutor;

    /** {@inheritDoc} */
    @AfterEach
    @Override public void tearDown() {
        super.tearDown();

        ExecutorServiceHelper.shutdownAndAwaitTermination(requestExecutor);
    }

    /** {@inheritDoc} */
    @Override public RpcServer<?> createServer(Endpoint endpoint) {
        ClusterService service = ClusterServiceTestUtils.clusterService(
            endpoint.toString(),
            endpoint.getPort(),
            new StaticNodeFinder(Collections.emptyList()),
            new MessageSerializationRegistryImpl(),
            new TestScaleCubeClusterServiceFactory()
        );

        NodeOptions nodeOptions = new NodeOptions();

        requestExecutor = JRaftUtils.createRequestExecutor(nodeOptions);

        var server = new TestIgniteRpcServer(service, new NodeManager(), nodeOptions, requestExecutor) {
            @Override public void shutdown() {
                super.shutdown();

                service.stop();
            }
        };

        service.start();

        return server;
    }

    /** {@inheritDoc} */
    @Override public RpcClient createClient0() {
        int i = cntr.incrementAndGet();

        ClusterService service = ClusterServiceTestUtils.clusterService(
            "client" + i,
            endpoint.getPort() - i,
            new StaticNodeFinder(List.of(addressFromEndpoint(endpoint))),
            new MessageSerializationRegistryImpl(),
            new TestScaleCubeClusterServiceFactory()
        );

        IgniteRpcClient client = new IgniteRpcClient(service) {
            @Override public void shutdown() {
                super.shutdown();

                service.stop();
            }
        };

        service.start();

        waitForTopology(client, 1 + i, 5_000);

        return client;
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
