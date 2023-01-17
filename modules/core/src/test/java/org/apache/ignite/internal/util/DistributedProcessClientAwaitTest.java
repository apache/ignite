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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.distributed.InitMessage;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.TEST_PROCESS;

/**
 * Tests {@link DistributedProcess} awaiting client results.
 */
public class DistributedProcessClientAwaitTest extends GridCommonAbstractTest {
    /** Nodes count. */
    public static final int NODES_CNT = 3;

    /** */
    private static final AtomicReference<Throwable> failRef = new AtomicReference<>();

    /** */
    private static final AtomicReference<CountDownLatch> finishLatchRef = new AtomicReference<>();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(NODES_CNT);

        startClientGrid(NODES_CNT);

        failRef.set(null);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** */
    @Test
    public void testSkipClientResultsByDefault() throws Exception {
        Set<UUID> nodeIdsRes = new HashSet<>();

        for (int i = 0; i < NODES_CNT; i++)
            nodeIdsRes.add(grid(i).localNode().id());

        checkExpectedResults(nodeIdsRes, (id, req) -> new InitMessage<>(id, TEST_PROCESS, req));
    }

    /** */
    @Test
    public void testAwaitClientResults() throws Exception {
        Set<UUID> nodeIdsRes = new HashSet<>();

        for (int i = 0; i < NODES_CNT + 1; i++)
            nodeIdsRes.add(grid(i).localNode().id());

        checkExpectedResults(nodeIdsRes, AwaitClientInitMessage::new);
    }

    /** */
    @Test
    public void testSkipWaitingFailedClient() throws Exception {
        finishLatchRef.set(new CountDownLatch(NODES_CNT));

        List<DistributedProcess<Integer, Integer>> processes = new ArrayList<>(NODES_CNT + 1);

        TestRecordingCommunicationSpi clnCommSpi = TestRecordingCommunicationSpi.spi(grid(NODES_CNT));
        clnCommSpi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        Set<UUID> nodeIdsRes = new HashSet<>();

        for (int i = 0; i < NODES_CNT; i++)
            nodeIdsRes.add(grid(i).localNode().id());

        for (int n = 0; n < NODES_CNT + 1; n++) {
            DistributedProcess<Integer, Integer> dp = new TestDistributedProcess(
                nodeIdsRes, grid(n).context(), AwaitClientInitMessage::new);

            processes.add(dp);
        }

        processes.get(0).start(UUID.randomUUID(), 0);

        clnCommSpi.waitForBlocked();

        stopGrid(NODES_CNT);

        finishLatchRef.get().await(getTestTimeout(), MILLISECONDS);

        assertNull(failRef.get());
    }

    /** */
    @Test
    public void testChangedCoordinatorAwaitsClientResult() throws Exception {
        finishLatchRef.set(new CountDownLatch(NODES_CNT));

        List<DistributedProcess<Integer, Integer>> processes = new ArrayList<>(NODES_CNT + 1);

        TestRecordingCommunicationSpi clnCommSpi = TestRecordingCommunicationSpi.spi(grid(NODES_CNT));
        clnCommSpi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage);

        Set<UUID> nodeIdsRes = new HashSet<>();

        for (int i = 1; i < NODES_CNT + 1; i++)
            nodeIdsRes.add(grid(i).localNode().id());

        for (int n = 0; n < NODES_CNT + 1; n++) {
            DistributedProcess<Integer, Integer> dp = new TestDistributedProcess(
                nodeIdsRes, grid(n).context(), AwaitClientInitMessage::new);

            processes.add(dp);
        }

        processes.get(0).start(UUID.randomUUID(), 0);

        clnCommSpi.waitForBlocked();

        assertTrue(U.isLocalNodeCoordinator((grid(0).context().discovery())));

        stopGrid(0);

        clnCommSpi.stopBlock();

        finishLatchRef.get().await(getTestTimeout(), MILLISECONDS);

        assertNull(failRef.get());
    }

    /** */
    private void checkExpectedResults(
        Set<UUID> expNodeIdRes,
        BiFunction<UUID, Integer, ? extends InitMessage<Integer>> initMsgFactory
    ) throws Exception {
        List<DistributedProcess<Integer, Integer>> processes = new ArrayList<>(NODES_CNT + 1);

        for (int n = 0; n < NODES_CNT + 1; n++) {
            DistributedProcess<Integer, Integer> dp = new TestDistributedProcess(
                expNodeIdRes, grid(n).context(), initMsgFactory);

            processes.add(dp);
        }

        for (int n = 0; n < NODES_CNT + 1; n++) {
            failRef.set(null);
            finishLatchRef.set(new CountDownLatch(NODES_CNT + 1));

            processes.get(n).start(UUID.randomUUID(), 0);

            finishLatchRef.get().await(getTestTimeout(), MILLISECONDS);

            assertNull(failRef.get());
        }
    }

    /** */
    private static class TestDistributedProcess extends DistributedProcess<Integer, Integer> {
        /** */
        public TestDistributedProcess(
            Set<UUID> expNodeIdsRes,
            GridKernalContext ctx,
            BiFunction<UUID, Integer, ? extends InitMessage<Integer>> initMsgFactory
        ) {
            super(
                ctx,
                TEST_PROCESS,
                (req) -> new GridFinishedFuture<>(),
                (uuid, res, err) -> {
                    try {
                        assertEquals(expNodeIdsRes, res.keySet());
                    }
                    catch (AssertionError e) {
                        failRef.set(e);
                    }

                    finishLatchRef.get().countDown();
                },
                initMsgFactory);
        }
    }

    /** */
    private static class AwaitClientInitMessage extends InitMessage<Integer> {
        /** */
        public AwaitClientInitMessage(UUID processId, Integer req) {
            super(processId, TEST_PROCESS, req);
        }

        /** {@inheritDoc} */
        @Override public boolean waitClientResults() {
            return true;
        }
    }
}
