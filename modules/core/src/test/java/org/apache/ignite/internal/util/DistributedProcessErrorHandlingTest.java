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

package org.apache.ignite.internal.util;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.TEST_PROCESS;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/** */
public class DistributedProcessErrorHandlingTest extends GridCommonAbstractTest {
    /** */
    private static final int SRV_NODES = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setFailureHandler(new StopNodeFailureHandler());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(SRV_NODES);

        startClientGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** */
    @ParameterizedTest(name = "failClient={0}")
    @ValueSource(booleans = {true, false})
    public void testBackgroundExecFailureHandled(boolean failClient) throws Exception {
        checkDistributedProcess((ign, latch) ->
            new DistributedProcess<>(ign.context(), TEST_PROCESS,
                req -> runAsync(() -> {
                    failOnNode(ign, failClient);  // Fails processing request in a spawned thread.

                    return 0;
                }),
                (id, res, err) -> {
                    if (failClient)
                        assertEquals(SRV_NODES, res.values().size());
                    else {
                        assertEquals(SRV_NODES - 1, res.values().size());
                        assertEquals(1, err.size());
                        assertTrue(err.get(grid(1).localNode().id()) instanceof AssertionError);
                    }

                    latch.countDown();
                }));
    }

    /** */
    @ParameterizedTest(name = "failClient={0}")
    @ValueSource(booleans = {true, false})
    public void testExecFailureHandled(boolean failClient) throws Exception {
        checkDistributedProcess((ign, latch) ->
            new DistributedProcess<>(ign.context(), TEST_PROCESS,
                req -> {
                    failOnNode(ign, failClient);  // Fails processing request in the discovery thread.

                    return new GridFinishedFuture<>(0);
                },
                (id, res, err) -> {
                    if (failClient)
                        assertEquals(SRV_NODES, res.values().size());
                    else {
                        assertEquals(SRV_NODES - 1, res.values().size());
                        assertEquals(1, err.size());
                        assertTrue(err.get(grid(1).localNode().id()) instanceof AssertionError);
                    }

                    latch.countDown();
                }));
    }

    /** */
    @ParameterizedTest(name = "failClient={0}")
    @ValueSource(booleans = {true, false})
    public void testFinishFailureHandled(boolean failClient) throws Exception {
        checkDistributedProcess((ign, latch) ->
            new DistributedProcess<>(ign.context(), TEST_PROCESS,
                req -> new GridFinishedFuture<>(0),
                (uuid, res, err) -> {
                    assertEquals(SRV_NODES, res.values().size());
                    latch.countDown();

                    failOnNode(ign, failClient);
                }));
    }

    /** */
    private void checkDistributedProcess(
        BiFunction<IgniteEx, CountDownLatch, DistributedProcess<Integer, Integer>> processFactory
    ) throws Exception {
        DistributedProcess<Integer, Integer> proc = null;

        CountDownLatch latch = new CountDownLatch(SRV_NODES + 1);

        for (Ignite g: G.allGrids())
            proc = processFactory.apply((IgniteEx)g, latch);

        proc.start(UUID.randomUUID(), 0);

        assertTrue(latch.await(5, TimeUnit.SECONDS));

        // Just checks that node is alive.
        grid(1).cluster().state(ClusterState.INACTIVE);

        awaitPartitionMapExchange();

        waitForTopology(SRV_NODES + 1);
    }

    /** Checks whether to fail on specified node. */
    private void failOnNode(IgniteEx ign, boolean failClient) {
        if (failClient)
            assert !ign.configuration().isClientMode();
        else
            assert getTestIgniteInstanceIndex(ign.name()) != 1;
    }
}
