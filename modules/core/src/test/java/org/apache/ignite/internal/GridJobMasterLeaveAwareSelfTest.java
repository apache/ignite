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

package org.apache.ignite.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobMasterLeaveAware;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test behavior of jobs when master node has failed, but job class implements {@link org.apache.ignite.compute.ComputeJobMasterLeaveAware}
 * interface.
 */
@GridCommonTest(group = "Task Session")
public class GridJobMasterLeaveAwareSelfTest extends GridCommonAbstractTest {
    /** Total grid count within the cloud. */
    private static final int GRID_CNT = 2;

    /** Default IP finder for single-JVM cloud grid. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

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

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCommunicationSpi(new CommunicationSpi());

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Get predicate which allows task execution on all nodes except the last one.
     *
     * @return Predicate.
     */
    private IgnitePredicate<ClusterNode> excludeLastPredicate() {
        return new IgnitePredicate<ClusterNode>() {
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
     * Ensure that {@link org.apache.ignite.compute.ComputeJobMasterLeaveAware} callback is invoked on job which is initiated by
     * master and is currently running on it.
     *
     * @throws Exception If failed.
     */
    public void testLocalJobOnMaster() throws Exception {
        invokeLatch  = new CountDownLatch(1);
        jobLatch = new CountDownLatch(1);

        Ignite g = startGrid(0);

        g.compute().withAsync().execute(new TestTask(1), null);

        jobLatch.await();

        // Count down the latch in a separate thread.
        new Thread(new Runnable() {
            @Override public void run() {
                try {
                    U.sleep(500);
                }
                catch (IgniteInterruptedCheckedException ignore) {
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
     * Ensure that {@link org.apache.ignite.compute.ComputeJobMasterLeaveAware} callback is invoked when master node leaves topology normally.
     *
     * @throws Exception If failed.
     */
    public void testMasterStoppedNormally() throws Exception {
        // Start grids.
        for (int i = 0; i < GRID_CNT; i++)
            startGrid(i);

        int lastGridIdx = GRID_CNT - 1;

        compute(grid(lastGridIdx).cluster().forPredicate(excludeLastPredicate())).withAsync().
            execute(new TestTask(GRID_CNT - 1), null);

        jobLatch.await();

        stopGrid(lastGridIdx, true);

        latch.countDown();

        assert invokeLatch.await(5000, MILLISECONDS);
    }

    /**
     * Ensure that {@link org.apache.ignite.compute.ComputeJobMasterLeaveAware} callback is invoked when master node leaves topology
     * abruptly (e.g. due to a network failure or immediate node shutdown).
     *
     * @throws Exception If failed.
     */
    public void testMasterStoppedAbruptly() throws Exception {
        // Start grids.
        for (int i = 0; i < GRID_CNT; i++)
            startGrid(i);

        int lastGridIdx = GRID_CNT - 1;

        compute(grid(lastGridIdx).cluster().forPredicate(excludeLastPredicate())).withAsync().
            execute(new TestTask(GRID_CNT - 1), null);

        jobLatch.await();

        ((CommunicationSpi)grid(lastGridIdx).configuration().getCommunicationSpi()).blockMessages();

        stopGrid(lastGridIdx, true);

        latch.countDown();

        assert invokeLatch.await(5000, MILLISECONDS);
    }

    /**
     * Ensure that {@link org.apache.ignite.compute.ComputeJobMasterLeaveAware} callback is invoked when fails to send
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

        compute(grid(lastGridIdx).cluster().forPredicate(excludeLastPredicate())).withAsync().
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
        testMasterLeaveAwareCallback(1, new CX1<ClusterGroup, IgniteFuture<?>>() {
            @Override public IgniteFuture<?> applyx(ClusterGroup grid) {
                IgniteCompute comp = compute(grid).withAsync();

                comp.apply(new TestClosure(), "arg");

                return comp.future();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testApply2() throws Exception {
        testMasterLeaveAwareCallback(2, new CX1<ClusterGroup, IgniteFuture<?>>() {
            @Override public IgniteFuture<?> applyx(ClusterGroup grid) {
                IgniteCompute comp = compute(grid).withAsync();

                comp.apply(new TestClosure(), Arrays.asList("arg1", "arg2"));

                return comp.future();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testApply3() throws Exception {
        testMasterLeaveAwareCallback(2, new CX1<ClusterGroup, IgniteFuture<?>>() {
            @Override public IgniteFuture<?> applyx(ClusterGroup grid) {
                IgniteCompute comp = compute(grid).withAsync();

                comp.apply(new TestClosure(),
                    Arrays.asList("arg1", "arg2"),
                    new IgniteReducer<Void, Object>() {
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
        testMasterLeaveAwareCallback(1, new CX1<ClusterGroup, IgniteFuture<?>>() {
            @Override public IgniteFuture<?> applyx(ClusterGroup prj) {
                IgniteCompute comp = compute(prj).withAsync();

                comp.run(new TestRunnable());

                return comp.future();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testRun2() throws Exception {
        testMasterLeaveAwareCallback(2, new CX1<ClusterGroup, IgniteFuture<?>>() {
            @Override public IgniteFuture<?> applyx(ClusterGroup prj) {
                IgniteCompute comp = compute(prj).withAsync();

                comp.run(Arrays.asList(new TestRunnable(), new TestRunnable()));

                return comp.future();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCall1() throws Exception {
        testMasterLeaveAwareCallback(1, new CX1<ClusterGroup, IgniteFuture<?>>() {
            @Override public IgniteFuture<?> applyx(ClusterGroup prj) {
                IgniteCompute comp = compute(prj).withAsync();

                comp.call(new TestCallable());

                return comp.future();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCall2() throws Exception {
        testMasterLeaveAwareCallback(2, new CX1<ClusterGroup, IgniteFuture<?>>() {
            @Override public IgniteFuture<?> applyx(ClusterGroup prj) {
                IgniteCompute comp = compute(prj).withAsync();

                comp.call(Arrays.asList(new TestCallable(), new TestCallable()));

                return comp.future();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCall3() throws Exception {
        testMasterLeaveAwareCallback(2, new CX1<ClusterGroup, IgniteFuture<?>>() {
            @Override public IgniteFuture<?> applyx(ClusterGroup prj) {
                IgniteCompute comp = compute(prj).withAsync();

                comp.call(
                    Arrays.asList(new TestCallable(), new TestCallable()),
                    new IgniteReducer<Void, Object>() {
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
        testMasterLeaveAwareCallback(1, new CX1<ClusterGroup, IgniteFuture<?>>() {
            @Override public IgniteFuture<?> applyx(ClusterGroup prj) {
                IgniteCompute comp = compute(prj).withAsync();

                comp.broadcast(new TestRunnable());

                return comp.future();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testBroadcast2() throws Exception {
        testMasterLeaveAwareCallback(1, new CX1<ClusterGroup, IgniteFuture<?>>() {
            @Override public IgniteFuture<?> applyx(ClusterGroup prj) {
                IgniteCompute comp = compute(prj).withAsync();

                comp.broadcast(new TestCallable());

                return comp.future();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testBroadcast3() throws Exception {
        testMasterLeaveAwareCallback(1, new CX1<ClusterGroup, IgniteFuture<?>>() {
            @Override public IgniteFuture<?> applyx(ClusterGroup prj) {
                IgniteCompute comp = compute(prj).withAsync();

                comp.broadcast(new TestClosure(), "arg");

                return comp.future();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityRun() throws Exception {
        testMasterLeaveAwareCallback(1, new CX1<ClusterGroup, IgniteFuture<?>>() {
            @Override public IgniteFuture<?> applyx(ClusterGroup prj) {
                IgniteCompute comp = compute(prj).withAsync();

                Affinity<Object> aff = prj.ignite().affinity(null);

                ClusterNode node = F.first(prj.nodes());

                comp.affinityRun((String)null, keyForNode(aff, node), new TestRunnable());

                return comp.future();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityCall() throws Exception {
        testMasterLeaveAwareCallback(1, new CX1<ClusterGroup, IgniteFuture<?>>() {
            @Override public IgniteFuture<?> applyx(ClusterGroup prj) {
                IgniteCompute comp = compute(prj).withAsync();

                Affinity<Object> aff = prj.ignite().affinity(null);

                ClusterNode node = F.first(prj.nodes());

                comp.affinityCall((String)null, keyForNode(aff, node), new TestCallable());

                return comp.future();
            }
        });
    }

    /**
     * @param aff Cache affinity.
     * @param node Node.
     * @return Finds some cache key for which given node is primary.
     */
    private Object keyForNode(Affinity<Object> aff, ClusterNode node) {
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
    private void testMasterLeaveAwareCallback(int expJobs, IgniteClosure<ClusterGroup, IgniteFuture<?>> taskStarter)
        throws Exception {
        jobLatch = new CountDownLatch(expJobs);
        invokeLatch  = new CountDownLatch(expJobs);

        for (int i = 0; i < GRID_CNT; i++)
            startGrid(i);

        int lastGridIdx = GRID_CNT - 1;

        IgniteFuture<?> fut = taskStarter.apply(grid(lastGridIdx).cluster().forPredicate(excludeLastPredicate()));

        jobLatch.await();

        stopGrid(lastGridIdx, true);

        latch.countDown();

        assert invokeLatch.await(5000, MILLISECONDS);

        try {
            fut.get();
        }
        catch (IgniteException e) {
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
        private void execute(IgniteLogger log) {
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
        private void onMasterLeave(IgniteLogger log, Object job) {
            log.info("Callback executed: " + job);

            latch0.countDown();

            invokeLatch.countDown();
        }
    }

    /**
     * Master leave aware callable.
     */
    private static class TestCallable implements IgniteCallable<Void>, ComputeJobMasterLeaveAware {
        /** Task session. */
        @LoggerResource
        private IgniteLogger log;

        /** */
        private TestMasterLeaveAware masterLeaveAware = new TestMasterLeaveAware();

        /** {@inheritDoc} */
        @Override public Void call() throws Exception {
            masterLeaveAware.execute(log);

            return null;
        }

        /** {@inheritDoc} */
        @Override public void onMasterNodeLeft(ComputeTaskSession ses) {
            masterLeaveAware.onMasterLeave(log, this);
        }
    }

    /**
     * Master leave aware runnable.
     */
    private static class TestRunnable implements IgniteRunnable, ComputeJobMasterLeaveAware {
        /** Task session. */
        @LoggerResource
        private IgniteLogger log;

        /** */
        private TestMasterLeaveAware masterLeaveAware = new TestMasterLeaveAware();

        /** {@inheritDoc} */
        @Override public void run() {
            masterLeaveAware.execute(log);
        }

        /** {@inheritDoc} */
        @Override public void onMasterNodeLeft(ComputeTaskSession ses) {
            masterLeaveAware.onMasterLeave(log, this);
        }
    }

    /**
     * Master leave aware closure.
     */
    private static class TestClosure implements IgniteClosure<String, Void>, ComputeJobMasterLeaveAware {
        /** Task session. */
        @LoggerResource
        private IgniteLogger log;

        /** */
        private TestMasterLeaveAware masterLeaveAware = new TestMasterLeaveAware();

        /** {@inheritDoc} */
        @Override public Void apply(String arg) {
            masterLeaveAware.execute(log);

            return null;
        }

        /** {@inheritDoc} */
        @Override public void onMasterNodeLeft(ComputeTaskSession ses) {
            masterLeaveAware.onMasterLeave(log, this);
        }
    }

    /**
     * Base implementation of dummy task which produces predefined amount of test jobs on split stage.
     */
    private static class TestTask extends ComputeTaskSplitAdapter<String, Integer> {
        /** How many jobs to produce. */
        private int jobCnt;

        /** */
        @TaskSessionResource
        private ComputeTaskSession taskSes;

        /**
         * Constructor.
         *
         * @param jobCnt How many jobs to produce on split stage.
         */
        private TestTask(int jobCnt) {
            this.jobCnt = jobCnt;
        }

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, String arg) {
            Collection<ComputeJobAdapter> jobs = new ArrayList<>(jobCnt);

            for (int i = 0; i < jobCnt; i++)
                jobs.add(new TestJob());

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) {
            return null;
        }
    }

    /**
     * Base implementation of dummy test job.
     */
    private static class TestJob extends ComputeJobAdapter implements ComputeJobMasterLeaveAware {
        /** Task session. */
        @LoggerResource
        private IgniteLogger log;

        /** */
        private TestMasterLeaveAware masterLeaveAware = new TestMasterLeaveAware();

        /**
         * Constructor
         */
        private TestJob() {
            super(new Object());
        }

        /** {@inheritDoc} */
        @Override public Object execute() {
            masterLeaveAware.execute(log);

            return null;
        }

        /** {@inheritDoc} */
        @Override public void onMasterNodeLeft(ComputeTaskSession ses) {
            masterLeaveAware.onMasterLeave(log, this);
        }
    }

    /**
     * Communication SPI which could optionally block outgoing messages.
     */
    private static class CommunicationSpi extends TcpCommunicationSpi {
        /** Whether to block all outgoing messages. */
        private volatile boolean block;

        /** Job execution response latch. */
        private CountDownLatch respLatch = new CountDownLatch(1);

        /** Whether to wait for a wait latch before returning. */
        private volatile boolean wait;

        /** Wait latch. */
        private CountDownLatch waitLatch = new CountDownLatch(1);

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            sendMessage0(node, msg, ackClosure);
        }

        /**
         * Send message optionally either blocking it or throwing an exception if it is of
         * {@link GridJobExecuteResponse} type.
         *
         * @param node Destination node.
         * @param msg Message to be sent.
         * @param ackClosure Ack closure.
         * @throws org.apache.ignite.spi.IgniteSpiException If failed.
         */
        private void sendMessage0(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                GridIoMessage msg0 = (GridIoMessage)msg;

                if (msg0.message() instanceof GridJobExecuteResponse) {
                    respLatch.countDown();

                    if (wait) {
                        try {
                            U.await(waitLatch);
                        }
                        catch (IgniteInterruptedCheckedException ignore) {
                            // No-op.
                        }
                    }
                }
            }

            if (!block)
                super.sendMessage(node, msg, ackClosure);
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
         * @throws IgniteInterruptedCheckedException If interrupted.
         */
        private void awaitResponse() throws IgniteInterruptedCheckedException {
            U.await(respLatch);
        }
    }
}