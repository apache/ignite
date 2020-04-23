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
package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import com.google.common.collect.Lists;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch.ExchangeLatchManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch.Latch;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch.LatchAckMessage;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link ExchangeLatchManager} functionality when latch coordinator is failed.
 */
public class IgniteExchangeLatchManagerCoordinatorFailTest extends GridCommonAbstractTest {
    /** */
    private static final String LATCH_NAME = "test";

    /** */
    private static final String LATCH_DROP_NAME = "testDrop";

    /** 5 nodes. */
    private final AffinityTopologyVersion latchTopVer = new AffinityTopologyVersion(5, 1);

    /** Latch coordinator index. */
    private static final int LATCH_CRD_INDEX = 0;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        if (getTestIgniteInstanceName(0).equals(igniteInstanceName)) {
            commSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    if (msg instanceof LatchAckMessage && (node.id().getLeastSignificantBits() & 0xFFFF) == 4) {
                        LatchAckMessage ackMsg = (LatchAckMessage)msg;

                        if (ackMsg.topVer().equals(latchTopVer) && ackMsg.latchId().equals(LATCH_DROP_NAME)) {
                            info("Going to block message [node=" + node + ", msg=" + msg + ']');

                            return true;
                        }
                    }

                    return false;
                }
            });
        }

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /** Wait before latch creation. */
    private final IgniteBiClosure<ExchangeLatchManager, CountDownLatch, Boolean> beforeCreate = (mgr, syncLatch) -> {
        try {
            syncLatch.countDown();
            syncLatch.await();

            Latch distributedLatch = mgr.getOrCreate(LATCH_NAME, latchTopVer);

            distributedLatch.countDown();

            distributedLatch.await();
        }
        catch (Exception e) {
            log.error("Unexpected exception", e);

            return false;
        }

        return true;
    };

    /** Wait before latch count down. */
    private final IgniteBiClosure<ExchangeLatchManager, CountDownLatch, Boolean> beforeCountDown = (mgr, syncLatch) -> {
        try {
            Latch distributedLatch = mgr.getOrCreate(LATCH_NAME, latchTopVer);

            syncLatch.countDown();
            syncLatch.await();

            distributedLatch.countDown();

            distributedLatch.await();
        }
        catch (Exception e) {
            log.error("Unexpected exception ", e);

            return false;
        }

        return true;
    };

    /** Wait after all operations are successful. */
    private final IgniteBiClosure<ExchangeLatchManager, CountDownLatch, Boolean> all = (mgr, syncLatch) -> {
        try {
            Latch distributedLatch = mgr.getOrCreate(LATCH_NAME, latchTopVer);

            distributedLatch.countDown();

            syncLatch.countDown();

            distributedLatch.await();

            syncLatch.await();
        }
        catch (Exception e) {
            log.error("Unexpected exception ", e);

            return false;
        }

        return true;
    };

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Test scenarios description:
     *
     * We have existing coordinator and 4 other nodes.
     * Each node do following operations:
     * 1) Create latch
     * 2) Countdown latch
     * 3) Await latch
     *
     * While nodes do the operations we shutdown coordinator and next oldest node become new coordinator.
     * We should check that new coordinator properly restored latch and all nodes finished latch completion successfully after that.
     *
     * Each node before coordinator shutdown can be in 3 different states:
     *
     * State {@link #beforeCreate} - Node didn't create latch yet.
     * State {@link #beforeCountDown} - Node created latch but didn't count down it yet.
     * State {@link #all} - Node created latch and count downed it.
     *
     * We should check important cases when future coordinator is in one of these states, and other 3 nodes have 3 different states.
     */

    /**
     * Scenario 1:
     *
     * Node 1 state -> {@link #beforeCreate}
     * Node 2 state -> {@link #beforeCountDown}
     * Node 3 state -> {@link #all}
     * Node 4 state -> {@link #beforeCreate}
     */
    @Test
    public void testCoordinatorFail1() throws Exception {
        List<IgniteBiClosure<ExchangeLatchManager, CountDownLatch, Boolean>> nodeStates = Lists.newArrayList(
            beforeCreate,
            beforeCountDown,
            all,
            beforeCreate
        );

        doTestCoordinatorFail(nodeStates);
    }

    /**
     * Scenario 2:
     *
     * Node 1 state -> {@link #beforeCountDown}
     * Node 2 state -> {@link #beforeCountDown}
     * Node 3 state -> {@link #all}
     * Node 4 state -> {@link #beforeCreate}
     */
    @Test
    public void testCoordinatorFail2() throws Exception {
        List<IgniteBiClosure<ExchangeLatchManager, CountDownLatch, Boolean>> nodeStates = Lists.newArrayList(
            beforeCountDown,
            beforeCountDown,
            all,
            beforeCreate
        );

        doTestCoordinatorFail(nodeStates);
    }

    /**
     * Scenario 3:
     *
     * Node 1 state -> {@link #all}
     * Node 2 state -> {@link #beforeCountDown}
     * Node 3 state -> {@link #all}
     * Node 4 state -> {@link #beforeCreate}
     */
    @Test
    public void testCoordinatorFail3() throws Exception {
        List<IgniteBiClosure<ExchangeLatchManager, CountDownLatch, Boolean>> nodeStates = Lists.newArrayList(
            all,
            beforeCountDown,
            all,
            beforeCreate
        );

        doTestCoordinatorFail(nodeStates);
    }

    /**
     * Test latch coordinator fail with specified scenarios.
     *
     * @param nodeScenarios Node scenarios.
     * @throws Exception If failed.
     */
    private void doTestCoordinatorFail(List<IgniteBiClosure<ExchangeLatchManager, CountDownLatch, Boolean>> nodeScenarios) throws Exception {
        IgniteEx crd = (IgniteEx) startGridsMultiThreaded(5);
        crd.cluster().active(true);

        IgniteEx latchCrd = grid(LATCH_CRD_INDEX);

        // Latch to synchronize node states.
        CountDownLatch syncLatch = new CountDownLatch(5);

        GridCompoundFuture finishAllLatches = new GridCompoundFuture();

        AtomicBoolean hasErrors = new AtomicBoolean();

        int scenarioIdx = 0;

        for (int nodeId = 0; nodeId < 5; nodeId++) {
            if (nodeId == LATCH_CRD_INDEX)
                continue;

            IgniteEx grid = grid(nodeId);

            ExchangeLatchManager latchMgr = grid.context().cache().context().exchange().latch();

            IgniteBiClosure<ExchangeLatchManager, CountDownLatch, Boolean> scenario = nodeScenarios.get(scenarioIdx);

            IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(() -> {
                boolean success = scenario.apply(latchMgr, syncLatch);

                if (!success)
                    hasErrors.set(true);
            }, 1, "latch-runner-" + nodeId);

            finishAllLatches.add(fut);

            scenarioIdx++;
        }

        finishAllLatches.markInitialized();

        // Wait while all nodes reaches their states.
        while (syncLatch.getCount() != 1) {
            U.sleep(10);

            if (hasErrors.get())
                throw new Exception("All nodes should complete latches without errors");
        }

        latchCrd.close();

        // Resume progress for all nodes.
        syncLatch.countDown();

        // Wait for distributed latch completion.
        finishAllLatches.get(5000);

        Assert.assertFalse("All nodes should complete latches without errors", hasErrors.get());
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testCoordinatorFailoverAfterServerLatchCompleted() throws Exception {
        startGrids(5);

        ignite(0).cluster().active(true);

        assertTrue(GridTestUtils.waitForCondition(() -> {
            for (int i = 0; i < 5; i++) {
                if (!grid(0).context().cache().context().exchange().readyAffinityVersion().equals(latchTopVer))
                    return false;
            }

            return true;
        }, getTestTimeout()));

        Latch[] latches = new Latch[5];

        for (int i = 0; i < 5; i++) {
            ExchangeLatchManager latchMgr = grid(i).context().cache().context().exchange().latch();

            latches[i] = latchMgr.getOrCreate(LATCH_DROP_NAME, latchTopVer);

            info("Created latch: " + i);

            latches[i].countDown();
        }

        for (int i = 0; i < 4; i++) {
            info("Waiting for latch: " + i);

            latches[i].await(10_000, TimeUnit.MILLISECONDS);
        }

        stopGrid(0);

        for (int i = 1; i < 5; i++) {
            info("Waiting for latch after stop: " + i);

            latches[i].await(10_000, TimeUnit.MILLISECONDS);
        }
    }
}
