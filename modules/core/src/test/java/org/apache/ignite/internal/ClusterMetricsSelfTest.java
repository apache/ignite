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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.GridTestTask;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterGroupEmptyException;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

import static org.apache.ignite.events.EventType.EVT_JOB_FINISHED;
import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;

/**
 * Tests for projection metrics.
 */
@GridCommonTest(group = "Kernal Self")
public class ClusterMetricsSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_CNT = 4;

    /** */
    private static final int ITER_CNT = 30;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < NODES_CNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration();
        cfg.setIncludeProperties();
        cfg.setMetricsUpdateFrequency(0);

        return cfg;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testEmptyProjection() throws Exception {
        try {
            grid(0).cluster().forPredicate(F.<ClusterNode>alwaysFalse()).metrics();

            assert false;
        }
        catch (ClusterGroupEmptyException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     *
     */
    public void testTaskExecution() {
        for (int i = 0; i < ITER_CNT; i++) {
            info("Starting new iteration: " + i);

            try {
                performTaskExecutionTest();
            }
            catch (Throwable t) {
                error("Iteration failed: " + i, t);

                fail("Test failed (see logs for details).");
            }
        }
    }

    /**
     * @throws Exception In case of error.
     */
    private void performTaskExecutionTest() throws Exception {
        Ignite g = grid(0);

        JobFinishLock jobFinishLock = new JobFinishLock();

        MetricsUpdateLock metricsUpdLock = new MetricsUpdateLock();

        try {
            for (Ignite g0 : G.allGrids())
                g0.events().localListen(jobFinishLock, EVT_JOB_FINISHED);

            g.compute().execute(new GridTestTask(), "testArg");

            // Wait until all nodes fire JOB FINISH event.
            jobFinishLock.await();

            g.events().localListen(metricsUpdLock, EVT_NODE_METRICS_UPDATED);

            // Wait until local node will have updated metrics.
            metricsUpdLock.await();

            ClusterMetrics m = g.cluster().metrics();

            checkMetrics(m);
        }
        finally {
            for (Ignite g0 : G.allGrids())
                g0.events().stopLocalListen(jobFinishLock);

            g.events().stopLocalListen(metricsUpdLock);
        }
    }

    /**
     * @param m Metrics.
     */
    @SuppressWarnings({"FloatingPointEquality"})
    private void checkMetrics(ClusterMetrics m) {
        assert m.getTotalNodes() == NODES_CNT;

        assert m.getMaximumActiveJobs() == 0;
        assert m.getAverageActiveJobs() == 0;

        assert m.getMaximumCancelledJobs() == 0;
        assert m.getAverageCancelledJobs() == 0;

        assert m.getMaximumRejectedJobs() == 0;
        assert m.getAverageRejectedJobs() == 0;

        assert m.getMaximumWaitingJobs() == 0;
        assert m.getAverageWaitingJobs() == 0;

        assert m.getMaximumJobExecuteTime() >= 0;
        assert m.getAverageJobExecuteTime() >= 0;

        assert m.getAverageJobExecuteTime() <= m.getMaximumJobExecuteTime();

        assert m.getMaximumJobWaitTime() >= 0;
        assert m.getAverageJobWaitTime() >= 0;

        assert m.getAverageJobWaitTime() <= m.getMaximumJobWaitTime();

        assert m.getMaximumThreadCount() > 0;
        assert m.getIdleTimePercentage() >= 0;
        assert m.getIdleTimePercentage() <= 1;

        assert m.getAverageCpuLoad() >= 0 || m.getAverageCpuLoad() == -1.0;

        assert m.getTotalCpus() > 0;
    }

    /**
     *
     */
    private static class JobFinishLock implements IgnitePredicate<Event> {
        /** Latch. */
        private final CountDownLatch latch = new CountDownLatch(NODES_CNT);

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            assert evt.type() == EVT_JOB_FINISHED;

            latch.countDown();

            return true;
        }

        /**
         * Waits until all nodes fire EVT_JOB_FINISHED.
         *
         * @throws InterruptedException If interrupted.
         */
        public void await() throws InterruptedException {
            latch.await();
        }
    }

    /**
     *
     */
    private static class MetricsUpdateLock implements IgnitePredicate<Event> {
        /** Latch. */
        private final CountDownLatch latch = new CountDownLatch(NODES_CNT * 2);

        /** */
        private final Map<UUID, Integer> metricsRcvdCnt = new HashMap<>();

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

            Integer cnt = F.addIfAbsent(metricsRcvdCnt, discoEvt.eventNode().id(), 0);

            assert cnt != null;

            if (cnt < 2) {
                latch.countDown();

                metricsRcvdCnt.put(discoEvt.eventNode().id(), ++cnt);
            }

            return true;
        }

        /**
         * Waits until all metrics will be received twice from all nodes in
         * topology.
         *
         * @throws InterruptedException If interrupted.
         */
        public void await() throws InterruptedException {
            latch.await();
        }
    }
}