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

package org.apache.ignite.internal.raft;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link Loza} functionality.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ITLozaTest {
    /** Network factory. */
    private static final ClusterServiceFactory NETWORK_FACTORY = new TestScaleCubeClusterServiceFactory();

    /** Server port offset. */
    private static final int PORT = 20010;

    /** */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistryImpl();

    /** */
    @WorkDirectory
    private Path dataPath;

    /**
     * Starts a raft group service with a provided group id on a provided Loza instance.
     *
     * @return Raft group service.
     */
    private RaftGroupService startClient(String groupId, ClusterNode node, Loza loza) throws Exception {
        return loza.prepareRaftGroup(groupId,
            List.of(node), () -> mock(RaftGroupListener.class)
        ).get(10, TimeUnit.SECONDS);
    }

    /**
     * @param testInfo Test info.
     * @param port Local port.
     * @param srvs Server nodes of the cluster.
     * @return The client cluster view.
     */
    private static ClusterService clusterService(TestInfo testInfo, int port, List<NetworkAddress> srvs) {
        var network = ClusterServiceTestUtils.clusterService(
            testInfo,
            port,
            new StaticNodeFinder(srvs),
            SERIALIZATION_REGISTRY,
            NETWORK_FACTORY
        );

        network.start();

        return network;
    }

    /**
     * Tests that RaftGroupServiceImpl uses shared executor for retrying RaftGroupServiceImpl#sendWithRetry()
     */
    @Test
    public void testRaftServiceUsingSharedExecutor(TestInfo testInfo) throws Exception {
        ClusterService service = null;

        Loza loza = null;

        RaftGroupService[] grpSrvcs = new RaftGroupService[5];

        try {
            service = spy(clusterService(testInfo, PORT, List.of()));

            MessagingService messagingServiceMock = spy(service.messagingService());

            when(service.messagingService()).thenReturn(messagingServiceMock);

            CompletableFuture<NetworkMessage> exception = CompletableFuture.failedFuture(new Exception(new IOException()));

            loza = new Loza(service, dataPath);

            loza.start();

            for (int i = 0; i < grpSrvcs.length; i++) {
                // return an error on first invocation
                doReturn(exception)
                    // assert that a retry has been issued on the executor
                    .doAnswer(invocation -> {
                        assertThat(Thread.currentThread().getName(), containsString(Loza.CLIENT_POOL_NAME));

                        return exception;
                    })
                    // finally call the real method
                    .doCallRealMethod()
                    .when(messagingServiceMock).invoke(any(NetworkAddress.class), any(), anyLong());

                grpSrvcs[i] = startClient(Integer.toString(i), service.topologyService().localMember(), loza);

                verify(messagingServiceMock, times(3 * (i + 1)))
                    .invoke(any(NetworkAddress.class), any(), anyLong());
            }
        }
        finally {
            for (RaftGroupService srvc : grpSrvcs) {
                srvc.shutdown();

                loza.stopRaftGroup(srvc.groupId());
            }

            if (loza != null)
                loza.stop();

            if (service != null)
                service.stop();
        }
    }
}
