/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.network.scalecube;

import static org.apache.ignite.utils.ClusterServiceTestUtils.clusterService;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.StaticNodeFinder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Tests for ScaleCube based {@link ClusterService}.
 */
public class ItClusterServiceTest {
    @Test
    void testShutdown(TestInfo testInfo) {
        var addr = new NetworkAddress("localhost", 10000);

        ClusterService service = clusterService(
                testInfo,
                addr.port(),
                new StaticNodeFinder(List.of(addr)),
                new TestScaleCubeClusterServiceFactory()
        );

        service.start();

        service.stop();

        assertThat(service.isStopped(), is(true));

        ExecutionException e = assertThrows(
                ExecutionException.class,
                () -> service.messagingService()
                        .send(mock(ClusterNode.class), mock(NetworkMessage.class))
                        .get(5, TimeUnit.SECONDS)
        );

        assertThat(e.getCause(), isA(NodeStoppingException.class));
    }
}
