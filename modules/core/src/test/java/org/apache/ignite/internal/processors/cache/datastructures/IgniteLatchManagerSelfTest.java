package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.datastructures.latch.Latch;
import org.apache.ignite.internal.processors.cache.datastructures.latch.LatchManager;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

public class IgniteLatchManagerSelfTest extends GridCommonAbstractTest {

    private static final String LATCH_NAME = "test";

    private final IgniteBiClosure<LatchManager, CountDownLatch, Boolean> beforeCreate = (mgr, latch) -> {
        try {
            latch.await();

            Latch distributedLatch = mgr.getOrCreate(LATCH_NAME, AffinityTopologyVersion.NONE);

            distributedLatch.countDown();

            distributedLatch.await();
        } catch (Exception e) {
            log.warning("Unexpected exception", e);

            return false;
        }

        return true;
    };

    private final IgniteBiClosure<LatchManager, CountDownLatch, Boolean> beforeCountDown = (mgr, latch) -> {
        try {
            Latch distributedLatch = mgr.getOrCreate(LATCH_NAME, AffinityTopologyVersion.NONE);

            latch.await();

            distributedLatch.countDown();

            distributedLatch.await();
        } catch (Exception e) {
            log.warning("Unexpected exception ", e);

            return false;
        }

        return true;
    };

    private final IgniteBiClosure<LatchManager, CountDownLatch, Boolean> all = (mgr, latch) -> {
        try {
            Latch distributedLatch = mgr.getOrCreate(LATCH_NAME, AffinityTopologyVersion.NONE);

            distributedLatch.countDown();

            distributedLatch.await();

            latch.await();
        } catch (Exception e) {
            log.warning("Unexpected exception ", e);

            return false;
        }

        return true;
    };

    @Override
    protected void beforeTest() throws Exception {
        stopAllGrids();
    }

    @Override
    protected void afterTest() throws Exception {
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
    public void testCoordinatorFail1() {

    }

    /**
     * Scenario 2:
     *
     * Node 1 state -> {@link #beforeCountDown}
     * Node 2 state -> {@link #beforeCountDown}
     * Node 3 state -> {@link #all}
     * Node 4 state -> {@link #beforeCreate}
     */
    public void testCoordinatorFail2() {

    }

    /**
     * Scenario 3:
     *
     * Node 1 state -> {@link #all}
     * Node 2 state -> {@link #beforeCountDown}
     * Node 3 state -> {@link #all}
     * Node 4 state -> {@link #beforeCreate}
     */
    public void testCoordinatorFail3() {

    }

    private void doTestCoordinatorFail(List<IgniteBiClosure<LatchManager, CountDownLatch, Boolean>> nodeStates) throws Exception {
        IgniteEx crd = (IgniteEx) startGrids(5);
        crd.cluster().active(true);

        // Latch to synchronize node states.
        CountDownLatch latch = new CountDownLatch(1);

        GridCompoundFuture finishAllLatches = new GridCompoundFuture();

        AtomicBoolean hasErrors = new AtomicBoolean();

        for (int node = 1; node < 5; node++) {
            IgniteEx grid = grid(node);
            LatchManager latchMgr = grid.context().cache().context().exchange().latch();
            final int stateIdx = node - 1;

            IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(() -> {
                boolean success = nodeStates.get(stateIdx).apply(latchMgr, latch);
                if (!success)
                    hasErrors.set(true);
            }, 1, "latch-runner-" + node);

            finishAllLatches.add(fut);
        }

        finishAllLatches.markInitialized();

        crd.close();

        // Resume
        latch.countDown();

        finishAllLatches.get(5000);

        Assert.assertFalse("All nodes should finish latches without error", hasErrors.get());
    }
}
