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

package org.apache.ignite.internal.util.distributed;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.DiscoverySpiTestListener;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.MessagesPluginProvider;
import org.apache.ignite.spi.discovery.tcp.IgniteDiscoverySpiInternalListenerSupport;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.TEST_PROCESS;

/**
 * Tests {@link DistributedProcess#startByCoordinator} in case of coordinator node left.
 */
public class DistributedProcessStartByCoordinatorTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPluginProviders(new MessagesPluginProvider(TestIntegerMessage.class));

        return cfg;
    }

    /** Coordinator fails after the first process is finished and before it sends the initial request of the next one. */
    @Test
    public void testCoordinatorFailsBetweenProcesses() throws Exception {
        startGrids(NODES_CNT);

        DiscoverySpiTestListener lsnr = new DiscoverySpiTestListener();

        ((IgniteDiscoverySpiInternalListenerSupport)grid(0).configuration().getDiscoverySpi()).setInternalListener(lsnr);

        lsnr.blockCustomEvent(InitMessage.class);

        CountDownLatch finishLatch = new CountDownLatch(NODES_CNT - 1);

        // The first process starts the next one with the same id, the request value is the process number.
        Map<String, DistributedProcess<TestIntegerMessage, TestIntegerMessage>> procByNode = new HashMap<>();

        for (Ignite grid : G.allGrids()) {
            procByNode.put(grid.name(), new DistributedProcess<>(
                ((IgniteEx)grid).context(),
                TEST_PROCESS,
                (ignored, req) -> new GridFinishedFuture<>(req),
                (id, res, err) -> {
                    if (res.values().stream().allMatch(msg -> msg.value() == 1))
                        procByNode.get(grid.name()).startByCoordinator(id, new TestIntegerMessage(2));
                    else if (res.size() == NODES_CNT - 1 && res.values().stream().allMatch(msg -> msg.value() == 2))
                        finishLatch.countDown();
                    else
                        fail("Unexpected process result [res=" + res + ", err=" + err + ']');
                }));
        }

        procByNode.get(grid(1).name()).start(UUID.randomUUID(), new TestIntegerMessage(1));

        lsnr.waitCustomEvent();

        stopGrid(0);

        assertTrue(finishLatch.await(getTestTimeout(), MILLISECONDS));
    }
}
