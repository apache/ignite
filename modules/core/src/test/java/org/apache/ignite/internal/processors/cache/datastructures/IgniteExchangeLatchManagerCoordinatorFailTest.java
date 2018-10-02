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
import java.util.concurrent.atomic.AtomicBoolean;
import com.google.common.collect.Lists;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch.ExchangeLatchManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch.Latch;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

/**
 * Tests for {@link ExchangeLatchManager} functionality when latch coordinator is failed.
 */
public class IgniteExchangeLatchManagerCoordinatorFailTest extends GridCommonAbstractTest {
    /** */
    private static final String LATCH_NAME = "test";

    /** 5 nodes. */
    private final AffinityTopologyVersion latchTopVer = new AffinityTopologyVersion(5, 1);

    /** Latch coordinator index. */
    private static final int LATCH_CRD_INDEX = 0;

    /** Wait before latch creation. */
    private final IgniteBiClosure<ExchangeLatchManager, CountDownLatch, Boolean> beforeCreate = (mgr, syncLatch) -> {
        try {
            syncLatch.countDown();
            syncLatch.await();

            Latch distributedLatch = mgr.getOrCreate(LATCH_NAME, latchTopVer);

            distributedLatch.countDown();

            distributedLatch.await();
        } catch (Exception e) {
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
        } catch (Exception e) {
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
        } catch (Exception e) {
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
}
