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

package org.apache.ignite.internal.processors.service;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.service.inner.LongInitializedTestService;
import org.apache.ignite.internal.util.lang.gridfunc.AlwaysTruePredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.services.ServiceConfiguration;
import org.junit.Test;

/**
 * Tests that requests of change service's state won't be missed and will be handled correctly on a node left.
 *
 * It uses {@link LongInitializedTestService} with long running #init method to delay requests processing and blocking
 * communication spi to be sure that single deployment message won't be sent by a node at shutdown.
 */
public class ServiceDeploymentProcessingOnNodesLeftTest extends ServiceDeploymentProcessAbstractTest {
    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testDeploymentProcessingOnServersLeaveTopology() throws Exception {
        try {
            startGrids(4);

            IgniteEx client = startClientGrid(getConfiguration("client"));

            IgniteEx ignite1 = grid(1);
            IgniteEx ignite2 = grid(2);

            ((BlockingTcpCommunicationSpi)ignite1.configuration().getCommunicationSpi()).block();
            ((BlockingTcpCommunicationSpi)ignite2.configuration().getCommunicationSpi()).block();

            IgniteFuture fut = client.services().deployNodeSingletonAsync("testService",
                new LongInitializedTestService(5000L));
            IgniteFuture fut2 = client.services().deployNodeSingletonAsync("testService2",
                new LongInitializedTestService(5000L));

            stopNode(ignite1);
            stopNode(ignite2);

            fut.get(TEST_FUTURE_WAIT_TIMEOUT);
            fut2.get(TEST_FUTURE_WAIT_TIMEOUT);

            IgniteEx ignite3 = grid(3);

            assertNotNull(ignite3.services().service("testService"));
            assertNotNull(ignite3.services().service("testService2"));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception In case of an error.
     */
    @Test
    public void testDeploymentProcessingOnServersAndClientsLeaveTopology() throws Exception {
        try {
            Ignite ignite0 = startGrids(4);

            IgniteEx client1 = startClientGrid(getConfiguration("client1"));
            IgniteEx client2 = startClientGrid(getConfiguration("client2"));

            IgniteEx ignite1 = grid(1);

            ((BlockingTcpCommunicationSpi)client1.configuration().getCommunicationSpi()).block();
            ((BlockingTcpCommunicationSpi)client2.configuration().getCommunicationSpi()).block();
            ((BlockingTcpCommunicationSpi)ignite1.configuration().getCommunicationSpi()).block();

            ServiceConfiguration srvcCfg = new ServiceConfiguration();

            srvcCfg.setName("testService");
            srvcCfg.setMaxPerNodeCount(1);
            srvcCfg.setService(new LongInitializedTestService(10_000));
            srvcCfg.setNodeFilter(new AlwaysTruePredicate<>());

            IgniteFuture fut = ignite0.services().deployAsync(srvcCfg);

            stopNode(client1);
            stopNode(client2);
            stopNode(ignite1);

            fut.get(TEST_FUTURE_WAIT_TIMEOUT);

            assertNotNull(ignite0.services().service("testService"));

            IgniteEx ignite3 = grid(3);

            assertNotNull(ignite3.services().service("testService"));
        }
        finally {
            stopAllGrids();
        }
    }
}
