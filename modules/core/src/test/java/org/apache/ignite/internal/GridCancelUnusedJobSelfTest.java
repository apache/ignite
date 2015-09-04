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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Cancel unused job test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridCancelUnusedJobSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int WAIT_TIME = 100000;

    /** */
    public static final int SPLIT_COUNT = 10;

    /** */
    private static volatile int cancelCnt;

    /** */
    private static volatile int processedCnt;

    /** */
    private static CountDownLatch startSignal = new CountDownLatch(SPLIT_COUNT);

    /** */
    private static CountDownLatch stopSignal = new CountDownLatch(SPLIT_COUNT);

    /** */
    public GridCancelUnusedJobSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(discoSpi);

        c.setPublicThreadPoolSize(SPLIT_COUNT);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testCancel() throws Exception {
        Ignite ignite = G.ignite(getTestGridName());

        ignite.compute().localDeployTask(GridCancelTestTask.class, U.detectClassLoader(GridCancelTestTask.class));

        ComputeTaskFuture<?> fut = executeAsync(ignite.compute(), GridCancelTestTask.class.getName(), null);

        // Wait until jobs begin execution.
        boolean await = startSignal.await(WAIT_TIME, TimeUnit.MILLISECONDS);

        assert await : "Jobs did not start.";

        info("Test task result: " + fut);

        assert fut != null;

        // Only first job should successfully complete.
        Object res = fut.get();
        assert (Integer)res == 1;

        // Wait for all jobs to finish.
        await = stopSignal.await(WAIT_TIME, TimeUnit.MILLISECONDS);
        assert await : "Jobs did not stop.";

        // One is definitely processed. But there might be some more processed or cancelled or processed and cancelled.
        // Thus total number should be at least SPLIT_COUNT and at most (SPLIT_COUNT - 1) *2 +1
        assert (cancelCnt + processedCnt) >= SPLIT_COUNT && (cancelCnt + processedCnt) <= (SPLIT_COUNT - 1) * 2 +1 :
            "Invalid cancel count value: " + cancelCnt;
    }

    /**
     *
     */
    private static class GridCancelTestTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            if (log.isInfoEnabled())
                log.info("Splitting job [job=" + this + ", gridSize=" + gridSize + ", arg=" + arg + ']');

            Collection<ComputeJob> jobs = new ArrayList<>(SPLIT_COUNT);

            for (int i = 1; i <= SPLIT_COUNT; i++)
                jobs.add(new GridCancelTestJob(i));

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> received) {
            return ComputeJobResultPolicy.REDUCE;
        }

        /** {@inheritDoc} */
        @Override public Serializable reduce(List<ComputeJobResult> results) {
            if (log.isInfoEnabled())
                log.info("Reducing job [job=" + this + ", results=" + results + ']');

            if (results.size() > 1)
                fail();

            return results.get(0).getData();
        }
    }

    /**
     * Cancel test job.
     */
    private static class GridCancelTestJob extends ComputeJobAdapter {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        @TaskSessionResource
        private ComputeTaskSession ses;

        /**
         * @param arg Argument.
         */
        private GridCancelTestJob(Integer arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() {
            int arg = this.<Integer>argument(0);

            try {
                if (log.isInfoEnabled())
                    log.info("Executing job [job=" + this + ", arg=" + arg + ']');

                startSignal.countDown();

                try {
                    if (!startSignal.await(WAIT_TIME, TimeUnit.MILLISECONDS))
                        fail();

                    if (arg == 1) {
                        if (log.isInfoEnabled())
                            log.info("Job one is proceeding.");
                    }
                    else
                        Thread.sleep(WAIT_TIME);
                }
                catch (InterruptedException e) {
                    if (log.isInfoEnabled())
                        log.info("Job got cancelled [arg=" + arg + ", ses=" + ses + ", e=" + e + ']');

                    return 0;
                }

                if (log.isInfoEnabled())
                    log.info("Completing job: " + ses);

                return argument(0);
            }
            finally {
                stopSignal.countDown();

                processedCnt++;
            }
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            cancelCnt++;
        }
    }
}