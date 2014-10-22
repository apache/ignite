/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.kernal.GridTopic.*;

/**
 * Test for {@link GridCompute#cancelJob(GridUuid)}
 */
public class GridProjectionJobCancelSelfTest extends GridCommonAbstractTest {
    /** Number fo nodes to run in this test. */
    private static final int NODES_CNT = 3;

    /** Latch to hold jobs executing. */
    private static volatile CountDownLatch finishJobs;

    /** Exchange to pass job id. */
    private static volatile OneWayExchange<GridUuid> jobIdExc;

    /** */
    private static CountDownLatch jobFinishLatch;

    /** */
    private static CountDownLatch jobStartedLatch;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        finishJobs = new CountDownLatch(1);
        jobIdExc = new OneWayExchange<>();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new GridOptimizedMarshaller(false));

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalCancel() throws Exception {
        GridCompute comp = grid(0).compute().enableAsync();

        assertNull(comp.execute(TestTask.class, grid(0).localNode().id()));

        final GridComputeTaskFuture<Integer> fut = comp.future();

        grid(0).events().localListen(new GridPredicate<GridEvent>() {
            @Override public boolean apply(GridEvent evt) {
                finishJobs.countDown();

                return true;
            }
        }, GridEventType.EVT_JOB_CANCELLED);

        GridUuid jobId = jobIdExc.get();

        assert jobId != null;

        grid(0).compute().cancelJob(jobId);

        Integer res = fut.get();

        assertEquals(1, res.intValue());
    }

    /**
     * @throws Exception  If failed.
     */
    public void testRemoteCancel() throws Exception {
        GridCompute comp = grid(0).compute().enableAsync();

        assertNull(comp.execute(TestTask.class, grid(1).localNode().id()));

        final GridComputeTaskFuture<Integer> fut = comp.future();

        grid(1).events().localListen(new GridPredicate<GridEvent>() {
            @Override public boolean apply(GridEvent evt) {
                finishJobs.countDown();

                return true;
            }
        }, GridEventType.EVT_JOB_CANCELLED);

        GridUuid jobId = jobIdExc.get();

        assert jobId != null;

        grid(0).compute().cancelJob(jobId);

        Integer res = fut.get();

        assertEquals(1, res.intValue());
    }

    /**
     * @throws Exception  If failed.
     */
    public void testMissedCancel() throws Exception {
        GridCompute comp = grid(0).forLocal().compute().enableAsync();

        comp.execute(TestTask.class, grid(0).localNode().id());

        // Run task on single node.
        final GridComputeTaskFuture<Integer> fut = comp.future();

        GridUuid jobId = jobIdExc.get();

        assert jobId != null;

        // Cancel task on all other nodes.
        grid(0).forOthers(grid(0).localNode()).compute().cancelJob(jobId);

        Thread.sleep(100);

        finishJobs.countDown();

        Integer res = fut.get();

        // No jobs were interrupted.
        assertEquals(0, res.intValue());
    }

    /**
     * Tests that task cancel request is only sent to the nodes that are
     * in topology.
     *
     * @throws Exception If error occurs.
     */
    @SuppressWarnings("deprecation")
    public void testCancelInProjection() throws Exception {
        jobFinishLatch = new CountDownLatch(2);
        jobStartedLatch = new CountDownLatch(2);

        final Map<UUID, AtomicInteger> rcvNodes = new ConcurrentHashMap8<>();

        GridMessageListener msgLsnr1 = new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                UUID rcvNodeId = grid(0).localNode().id();

                info("Got message [sndNodeId=" + nodeId + ", rcvNodeId=" + rcvNodeId + ", msg=" + msg + "]");

                F.addIfAbsent(rcvNodes, rcvNodeId, F.newAtomicInt()).incrementAndGet();
            }
        };

        GridMessageListener msgLsnr2 = new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                UUID rcvNodeId = grid(1).localNode().id();

                info("Got message [sndNodeId=" + nodeId + ", rcvNodeId=" + rcvNodeId + ", msg=" + msg + "]");

                F.addIfAbsent(rcvNodes, rcvNodeId, F.newAtomicInt()).incrementAndGet();
            }
        };

        GridMessageListener msgLsnr3 = new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                UUID rcvNodeId = grid(2).localNode().id();

                info("Got message [sndNodeId=" + nodeId + ", rcvNodeId=" + rcvNodeId + ", msg=" + msg + "]");

                F.addIfAbsent(rcvNodes, rcvNodeId, F.newAtomicInt()).incrementAndGet();
            }
        };

        ((GridKernal)grid(0)).context().io().addMessageListener(TOPIC_JOB_CANCEL, msgLsnr1);
        ((GridKernal)grid(1)).context().io().addMessageListener(TOPIC_JOB_CANCEL, msgLsnr2);
        ((GridKernal)grid(2)).context().io().addMessageListener(TOPIC_JOB_CANCEL, msgLsnr3);

        GridCompute comp = grid(0).forOthers(grid(1).localNode()).compute().enableAsync();

        comp.execute(new GridComputeTaskAdapter<Object, Void>() {
            @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, @Nullable Object arg) {
                assert subgrid.size() == 2;

                Map<GridComputeJob, GridNode> ret = new HashMap<>(subgrid.size());

                for (GridNode node : subgrid) {
                    final UUID nodeId = node.id();

                    ret.put(
                        new GridComputeJobAdapter() {
                            @Override public Object execute() throws GridException {
                                try {
                                    jobStartedLatch.countDown();

                                    U.sleep(5000);

                                    return "Job for " + nodeId;
                                }
                                finally {
                                    jobFinishLatch.countDown();
                                }
                            }
                        },
                        node);
                }

                return ret;
            }

            @Nullable @Override public Void reduce(List<GridComputeJobResult> results) throws GridException {
                return null;
            }
        }, null);

        GridComputeTaskFuture<Void> fut = comp.future();

        assertTrue(jobStartedLatch.await(5, SECONDS));

        fut.cancel();

        assertTrue(jobFinishLatch.await(5, SECONDS));

        // Wait to make sure that we received only 1 cancel request.
        Thread.sleep(1000);

        assertFalse(rcvNodes.containsKey(grid(0).localNode().id())); // This is a local node.
        assertFalse(rcvNodes.containsKey(grid(1).localNode().id())); // This node is outside of topology.
        assertEquals(1, rcvNodes.get(grid(2).localNode().id()).get()); // This node should receive cancel request.
    }

    /**
     * Test task for this test.
     */
    private static class TestTask extends GridComputeTaskSplitAdapter<UUID, Integer> {
        /** Successful job result. */
        public static final String SUCCESS = "Success";

        /** Interrupted job result. */
        public static final String INTERRUPTED = "Interrupted";

        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, final UUID arg) throws GridException {
            Collection<GridComputeJob> jobs = new ArrayList<>(NODES_CNT);

            for (int i = 0; i < NODES_CNT; i++)
                jobs.add(new GridComputeJobAdapter() {
                    @GridLocalNodeIdResource
                    private UUID nodeId;

                    @GridJobContextResource
                    private GridComputeJobContext ctx;

                    @Override public Object execute() {
                        if (nodeId.equals(arg))
                            jobIdExc.put(ctx.getJobId());

                        try {
                            finishJobs.await();
                        }
                        catch (InterruptedException ignored) {
                            return INTERRUPTED;
                        }

                        return SUCCESS;
                    }
                });

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<GridComputeJobResult> results) throws GridException {
            assert results.size() == NODES_CNT;

            int interruptedCnt = 0;

            for (GridComputeJobResult r : results) {
                if (r.getData().equals(INTERRUPTED))
                    interruptedCnt++;
                else
                    assert SUCCESS.equals(r.getData());
            }

            return interruptedCnt;
        }
    }

    /**
     * Simple wrapper for single value, passed from one thread to another.
     * Could be used without {@link GridKernalContext} running.
     *
     * @param <R> Type of value to pass.
     */
    private static class OneWayExchange<R> {
        /** Value. */
        private AtomicReference<R> val = new AtomicReference<>(null);

        /** Latch, indicating value pass.. */
        private final CountDownLatch finished = new CountDownLatch(1);

        /**
         * Awaits for value and returns it.
         *
         * @return Incoming value.
         * @throws GridInterruptedException If waiting for value was interrupted.
         */
        public R get() throws GridInterruptedException {
            try {
                finished.await();
            }
            catch (InterruptedException e) {
                throw new GridInterruptedException(e);
            }

            return val.get();
        }

        /**
         * Tries to pass value to waiting thread(s).
         *
         * @param val Value to pass to another thread.
         * @return {@code true} if value were successfully passed, {@code false} if value were passed before.
         */
        public boolean put(R val) {
            assert val != null;

            if (this.val.compareAndSet(null, val)) {
                finished.countDown();

                return true;
            }
            else
                return false;
        }
    }
}
