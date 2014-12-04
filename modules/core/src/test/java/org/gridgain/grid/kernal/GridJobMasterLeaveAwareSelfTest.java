/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.communication.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Test behavior of jobs when master node has failed, but job class implements {@link GridComputeJobMasterLeaveAware}
 * interface.
 */
@GridCommonTest(group = "Task Session")
public class GridJobMasterLeaveAwareSelfTest extends GridCommonAbstractTest {
    /** Total grid count within the cloud. */
    private static final int GRID_CNT = 2;

    /** Default IP finder for single-JVM cloud grid. */
    private static final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** Counts how many times master-leave interface implementation was called. */
    private static volatile CountDownLatch invokeLatch;

    /** Latch which blocks job execution until main thread has sent node fail signal. */
    private static volatile CountDownLatch latch;

    /** Latch which blocks main thread until all jobs start their execution. */
    private static volatile CountDownLatch jobLatch;

    /** Should job wait for callback. */
    private static volatile boolean awaitMasterLeaveCallback = true;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        awaitMasterLeaveCallback = true;
        latch = new CountDownLatch(1);
        jobLatch = new CountDownLatch(GRID_CNT - 1);
        invokeLatch  = new CountDownLatch(GRID_CNT - 1);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();
        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCommunicationSpi(new CommunicationSpi());
        cfg.setMarshaller(new GridOptimizedMarshaller(false));

        GridCacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Get predicate which allows task execution on all nodes except the last one.
     *
     * @return Predicate.
     */
    private GridPredicate<ClusterNode> excludeLastPredicate() {
        return new GridPredicate<ClusterNode>() {
            @Override public boolean apply(ClusterNode e) {
                return !e.id().equals(grid(GRID_CNT - 1).localNode().id());
            }
        };
    }

    /**
     * Constructor.
     */
    public GridJobMasterLeaveAwareSelfTest() {
        super(/* don't start grid */ false);
    }

    /**
     * Ensure that {@link GridComputeJobMasterLeaveAware} callback is invoked on job which is initiated by
     * master and is currently running on it.
     *
     * @throws Exception If failed.
     */
    public void testLocalJobOnMaster() throws Exception {
        invokeLatch  = new CountDownLatch(1);
        jobLatch = new CountDownLatch(1);

        Ignite g = startGrid(0);

        g.compute().enableAsync().execute(new TestTask(1), null);

        jobLatch.await();

        // Count down the latch in a separate thread.
        new Thread(new Runnable() {
            @Override public void run() {
                try {
                    U.sleep(500);
                }
                catch (GridInterruptedException ignore) {
                    // No-op.
                }

                latch.countDown();
            }
        }).start();

        stopGrid(0, true);

        latch.countDown();

        assert invokeLatch.await(5000, MILLISECONDS);
    }

    /**
     * Ensure that {@link GridComputeJobMasterLeaveAware} callback is invoked when master node leaves topology normally.
     *
     * @throws Exception If failed.
     */
    public void testMasterStoppedNormally() throws Exception {
        // Start grids.
        for (int i = 0; i < GRID_CNT; i++)
            startGrid(i);

        int lastGridIdx = GRID_CNT - 1;

        compute(grid(lastGridIdx).forPredicate(excludeLastPredicate())).enableAsync().
            execute(new TestTask(GRID_CNT - 1), null);

        jobLatch.await();

        stopGrid(lastGridIdx, true);

        latch.countDown();

        assert invokeLatch.await(5000, MILLISECONDS);
    }

    /**
     * Ensure that {@link GridComputeJobMasterLeaveAware} callback is invoked when master node leaves topology
     * abruptly (e.g. due to a network failure or immediate node shutdown).
     *
     * @throws Exception If failed.
     */
    public void testMasterStoppedAbruptly() throws Exception {
        // Start grids.
        for (int i = 0; i < GRID_CNT; i++)
            startGrid(i);

        int lastGridIdx = GRID_CNT - 1;

        compute(grid(lastGridIdx).forPredicate(excludeLastPredicate())).enableAsync().
            execute(new TestTask(GRID_CNT - 1), null);

        jobLatch.await();

        ((CommunicationSpi)grid(lastGridIdx).configuration().getCommunicationSpi()).blockMessages();

        stopGrid(lastGridIdx, true);

        latch.countDown();

        assert invokeLatch.await(5000, MILLISECONDS);
    }

    /**
     * Ensure that {@link GridComputeJobMasterLeaveAware} callback is invoked when fails to send
     * {@link GridJobExecuteResponse} to master node.
     *
     * @throws Exception If failed.
     */
    public void testCannotSendJobExecuteResponse() throws Exception {
        awaitMasterLeaveCallback = false;

        // Start grids.
        for (int i = 0; i < GRID_CNT; i++)
            startGrid(i);

        int lastGridIdx = GRID_CNT - 1;

        compute(grid(lastGridIdx).forPredicate(excludeLastPredicate())).enableAsync().
            execute(new TestTask(GRID_CNT - 1), null);

        jobLatch.await();

        for (int i = 0; i < lastGridIdx; i++)
            ((CommunicationSpi)grid(i).configuration().getCommunicationSpi()).waitLatch();

        latch.countDown();

        // Ensure that all worker nodes has already started job response sending.
        for (int i = 0; i < lastGridIdx; i++)
            ((CommunicationSpi)grid(i).configuration().getCommunicationSpi()).awaitResponse();

        // Now we stop master grid.
        stopGrid(lastGridIdx, true);

        // Release communication SPI wait latches. As master node is stopped, job worker will receive and exception.
        for (int i = 0; i < lastGridIdx; i++)
            ((CommunicationSpi)grid(i).configuration().getCommunicationSpi()).releaseWaitLatch();

        assert invokeLatch.await(5000, MILLISECONDS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testApply1() throws Exception {
        testMasterLeaveAwareCallback(1, new CX1<ClusterGroup, GridFuture<?>>() {
            @Override public GridFuture<?> applyx(ClusterGroup grid) throws GridException {
                GridCompute comp = compute(grid).enableAsync();

                comp.apply(new TestClosure(), "arg");

                return comp.future();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testApply2() throws Exception {
        testMasterLeaveAwareCallback(2, new CX1<ClusterGroup, GridFuture<?>>() {
            @Override public GridFuture<?> applyx(ClusterGroup grid) throws GridException {
                GridCompute comp = compute(grid).enableAsync();

                comp.apply(new TestClosure(), Arrays.asList("arg1", "arg2"));

                return comp.future();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testApply3() throws Exception {
        testMasterLeaveAwareCallback(2, new CX1<ClusterGroup, GridFuture<?>>() {
            @Override public GridFuture<?> applyx(ClusterGroup grid) throws GridException {
                GridCompute comp = compute(grid).enableAsync();

                comp.apply(new TestClosure(),
                    Arrays.asList("arg1", "arg2"),
                    new GridReducer<Void, Object>() {
                        @Override public boolean collect(@Nullable Void aVoid) {
                            return true;
                        }

                        @Override public Object reduce() {
                            return null;
                        }
                    });

                return comp.future();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testRun1() throws Exception {
        testMasterLeaveAwareCallback(1, new CX1<ClusterGroup, GridFuture<?>>() {
            @Override public GridFuture<?> applyx(ClusterGroup prj) throws GridException {
                GridCompute comp = compute(prj).enableAsync();

                comp.run(new TestRunnable());

                return comp.future();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testRun2() throws Exception {
        testMasterLeaveAwareCallback(2, new CX1<ClusterGroup, GridFuture<?>>() {
            @Override public GridFuture<?> applyx(ClusterGroup prj) throws GridException {
                GridCompute comp = compute(prj).enableAsync();

                comp.run(Arrays.asList(new TestRunnable(), new TestRunnable()));

                return comp.future();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCall1() throws Exception {
        testMasterLeaveAwareCallback(1, new CX1<ClusterGroup, GridFuture<?>>() {
            @Override public GridFuture<?> applyx(ClusterGroup prj) throws GridException {
                GridCompute comp = compute(prj).enableAsync();

                comp.call(new TestCallable());

                return comp.future();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCall2() throws Exception {
        testMasterLeaveAwareCallback(2, new CX1<ClusterGroup, GridFuture<?>>() {
            @Override public GridFuture<?> applyx(ClusterGroup prj) throws GridException {
                GridCompute comp = compute(prj).enableAsync();

                comp.call(Arrays.asList(new TestCallable(), new TestCallable()));

                return comp.future();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCall3() throws Exception {
        testMasterLeaveAwareCallback(2, new CX1<ClusterGroup, GridFuture<?>>() {
            @Override public GridFuture<?> applyx(ClusterGroup prj) throws GridException {
                GridCompute comp = compute(prj).enableAsync();

                comp.call(
                    Arrays.asList(new TestCallable(), new TestCallable()),
                    new GridReducer<Void, Object>() {
                        @Override public boolean collect(@Nullable Void aVoid) {
                            return true;
                        }

                        @Override public Object reduce() {
                            return null;
                        }
                    });

                return comp.future();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testBroadcast1() throws Exception {
        testMasterLeaveAwareCallback(1, new CX1<ClusterGroup, GridFuture<?>>() {
            @Override public GridFuture<?> applyx(ClusterGroup prj) throws GridException {
                GridCompute comp = compute(prj).enableAsync();

                comp.broadcast(new TestRunnable());

                return comp.future();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testBroadcast2() throws Exception {
        testMasterLeaveAwareCallback(1, new CX1<ClusterGroup, GridFuture<?>>() {
            @Override public GridFuture<?> applyx(ClusterGroup prj) throws GridException {
                GridCompute comp = compute(prj).enableAsync();

                comp.broadcast(new TestCallable());

                return comp.future();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testBroadcast3() throws Exception {
        testMasterLeaveAwareCallback(1, new CX1<ClusterGroup, GridFuture<?>>() {
            @Override public GridFuture<?> applyx(ClusterGroup prj) throws GridException {
                GridCompute comp = compute(prj).enableAsync();

                comp.broadcast(new TestClosure(), "arg");

                return comp.future();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityRun() throws Exception {
        testMasterLeaveAwareCallback(1, new CX1<ClusterGroup, GridFuture<?>>() {
            @Override public GridFuture<?> applyx(ClusterGroup prj) throws GridException {
                GridCompute comp = compute(prj).enableAsync();

                GridCacheAffinity<Object> aff = prj.grid().cache(null).affinity();

                ClusterNode node = F.first(prj.nodes());

                comp.affinityRun(null, keyForNode(aff, node), new TestRunnable());

                return comp.future();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityCall() throws Exception {
        testMasterLeaveAwareCallback(1, new CX1<ClusterGroup, GridFuture<?>>() {
            @Override public GridFuture<?> applyx(ClusterGroup prj) throws GridException {
                GridCompute comp = compute(prj).enableAsync();

                GridCacheAffinity<Object> aff = prj.grid().cache(null).affinity();

                ClusterNode node = F.first(prj.nodes());

                comp.affinityCall(null, keyForNode(aff, node), new TestCallable());

                return comp.future();
            }
        });
    }

    /**
     * @param aff Cache affinity.
     * @param node Node.
     * @return Finds some cache key for which given node is primary.
     */
    private Object keyForNode(GridCacheAffinity<Object> aff, ClusterNode node) {
        assertNotNull(node);

        Object key = null;

        for (int i = 0; i < 1000; i++) {
            if (aff.isPrimary(node, i)) {
                key = i;

                break;
            }
        }

        assertNotNull(key);

        return key;
    }

    /**
     * @param expJobs Expected jobs number.
     * @param taskStarter Task started.
     * @throws Exception If failed.
     */
    private void testMasterLeaveAwareCallback(int expJobs, GridClosure<ClusterGroup, GridFuture<?>> taskStarter)
        throws Exception {
        jobLatch = new CountDownLatch(expJobs);
        invokeLatch  = new CountDownLatch(expJobs);

        for (int i = 0; i < GRID_CNT; i++)
            startGrid(i);

        int lastGridIdx = GRID_CNT - 1;

        GridFuture<?> fut = taskStarter.apply(grid(lastGridIdx).forPredicate(excludeLastPredicate()));

        jobLatch.await();

        stopGrid(lastGridIdx, true);

        latch.countDown();

        assert invokeLatch.await(5000, MILLISECONDS);

        try {
            fut.get();
        }
        catch (GridException e) {
            log.debug("Task failed: " + e);
        }
    }

    /**
     */
    private static class TestMasterLeaveAware {
        /** */
        private final CountDownLatch latch0 = new CountDownLatch(1);

        /**
         * @param log Logger.
         */
        private void execute(GridLogger log) {
            try {
                log.info("Started execute.");

                // Countdown shared job latch so that the main thread know that all jobs are
                // inside the "execute" routine.
                jobLatch.countDown();

                log.info("After job latch.");

                // Await for the main thread to allow jobs to proceed.
                latch.await();

                log.info("After latch.");

                if (awaitMasterLeaveCallback) {
                    latch0.await();

                    log.info("After latch0.");
                }
                else
                    log.info("Latch 0 skipped.");
            }
            catch (InterruptedException e) {
                // We do not expect any interruptions here, hence this statement.
                fail("Unexpected exception: " + e);
            }
        }

        /**
         * @param log Logger.
         * @param job Actual job.
         */
        private void onMasterLeave(GridLogger log, Object job) {
            log.info("Callback executed: " + job);

            latch0.countDown();

            invokeLatch.countDown();
        }
    }

    /**
     * Master leave aware callable.
     */
    private static class TestCallable implements Callable<Void>, GridComputeJobMasterLeaveAware {
        /** Task session. */
        @GridLoggerResource
        private GridLogger log;

        /** */
        private TestMasterLeaveAware masterLeaveAware = new TestMasterLeaveAware();

        /** {@inheritDoc} */
        @Override public Void call() throws Exception {
            masterLeaveAware.execute(log);

            return null;
        }

        /** {@inheritDoc} */
        @Override public void onMasterNodeLeft(GridComputeTaskSession ses) throws GridException {
            masterLeaveAware.onMasterLeave(log, this);
        }
    }

    /**
     * Master leave aware runnable.
     */
    private static class TestRunnable implements Runnable, GridComputeJobMasterLeaveAware {
        /** Task session. */
        @GridLoggerResource
        private GridLogger log;

        /** */
        private TestMasterLeaveAware masterLeaveAware = new TestMasterLeaveAware();

        /** {@inheritDoc} */
        @Override public void run() {
            masterLeaveAware.execute(log);
        }

        /** {@inheritDoc} */
        @Override public void onMasterNodeLeft(GridComputeTaskSession ses) throws GridException {
            masterLeaveAware.onMasterLeave(log, this);
        }
    }

    /**
     * Master leave aware closure.
     */
    private static class TestClosure implements GridClosure<String, Void>, GridComputeJobMasterLeaveAware {
        /** Task session. */
        @GridLoggerResource
        private GridLogger log;

        /** */
        private TestMasterLeaveAware masterLeaveAware = new TestMasterLeaveAware();

        /** {@inheritDoc} */
        @Override public Void apply(String arg) {
            masterLeaveAware.execute(log);

            return null;
        }

        /** {@inheritDoc} */
        @Override public void onMasterNodeLeft(GridComputeTaskSession ses) throws GridException {
            masterLeaveAware.onMasterLeave(log, this);
        }
    }

    /**
     * Base implementation of dummy task which produces predefined amount of test jobs on split stage.
     */
    private static class TestTask extends GridComputeTaskSplitAdapter<String, Integer> {
        /** How many jobs to produce. */
        private int jobCnt;

        /** */
        @GridTaskSessionResource
        private GridComputeTaskSession taskSes;

        /**
         * Constructor.
         *
         * @param jobCnt How many jobs to produce on split stage.
         */
        private TestTask(int jobCnt) {
            this.jobCnt = jobCnt;
        }

        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, String arg) throws GridException {
            Collection<GridComputeJobAdapter> jobs = new ArrayList<>(jobCnt);

            for (int i = 0; i < jobCnt; i++)
                jobs.add(new TestJob());

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<GridComputeJobResult> results) throws GridException {
            return null;
        }
    }

    /**
     * Base implementation of dummy test job.
     */
    private static class TestJob extends GridComputeJobAdapter implements GridComputeJobMasterLeaveAware {
        /** Task session. */
        @GridLoggerResource
        private GridLogger log;

        /** */
        private TestMasterLeaveAware masterLeaveAware = new TestMasterLeaveAware();

        /**
         * Constructor
         */
        private TestJob() {
            super(new Object());
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws GridException {
            masterLeaveAware.execute(log);

            return null;
        }

        /** {@inheritDoc} */
        @Override public void onMasterNodeLeft(GridComputeTaskSession ses) throws GridException {
            masterLeaveAware.onMasterLeave(log, this);
        }
    }

    /**
     * Communication SPI which could optionally block outgoing messages.
     */
    private static class CommunicationSpi extends GridTcpCommunicationSpi {
        /** Marshaller. */
        @GridMarshallerResource
        private GridMarshaller marsh;

        /** Whether to block all outgoing messages. */
        private volatile boolean block;

        /** Job execution response latch. */
        private CountDownLatch respLatch = new CountDownLatch(1);

        /** Whether to wait for a wait latch before returning. */
        private volatile boolean wait;

        /** Wait latch. */
        private CountDownLatch waitLatch = new CountDownLatch(1);

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, GridTcpCommunicationMessageAdapter msg)
            throws GridSpiException {
            sendMessage0(node, msg);
        }

        /**
         * Send message optionally either blocking it or throwing an exception if it is of
         * {@link GridJobExecuteResponse} type.
         *
         * @param node Destination node.
         * @param msg Message to be sent.
         * @throws GridSpiException If failed.
         */
        private void sendMessage0(ClusterNode node, GridTcpCommunicationMessageAdapter msg) throws GridSpiException {
            if (msg instanceof GridIoMessage) {
                GridIoMessage msg0 = (GridIoMessage)msg;

                if (msg0.message() instanceof GridJobExecuteResponse) {
                    respLatch.countDown();

                    if (wait) {
                        try {
                            U.await(waitLatch);
                        }
                        catch (GridInterruptedException ignore) {
                            // No-op.
                        }
                    }
                }
            }

            if (!block)
                super.sendMessage(node, msg);
        }

        /**
         * Block all outgoing message.
         */
        void blockMessages() {
            block = true;
        }

        /**
         * Whether to block on a wait latch.
         */
        private void waitLatch() {
            wait = true;
        }

        /**
         * Count down wait latch.
         */
        private void releaseWaitLatch() {
            waitLatch.countDown();
        }

        /**
         * Await for job execution response to come.
         *
         * @throws GridInterruptedException If interrupted.
         */
        private void awaitResponse() throws GridInterruptedException{
            U.await(respLatch);
        }
    }
}
