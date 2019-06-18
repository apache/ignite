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

package org.apache.ignite.internal.processors.jobmetrics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.resource.GridResourceProcessor;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.job.GridJobProcessor.GridJobMetrics.ACTIVE;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.GridJobMetrics.CANCELED;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.GridJobMetrics.EXEC_TIME;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.GridJobMetrics.FINISHED;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.GridJobMetrics.JOBS;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.GridJobMetrics.LAST_UPDATE_TIME;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.GridJobMetrics.MAX_EXEC_TIME;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.GridJobMetrics.MAX_WAIT_TIME;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.GridJobMetrics.REJECTED;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.GridJobMetrics.STARTED;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.GridJobMetrics.WAITING;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.GridJobMetrics.WAIT_TIME;

/**
 * Grid job metrics processor load test.
 */
public class GridJobMetricsSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int THREADS_CNT = 10;

    /** */
    private GridTestKernalContext ctx;

    /** */
    public GridJobMetricsSelfTest() {
        super(/*start grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ctx = newContext();

        ctx.add(new GridResourceProcessor(ctx));
        ctx.add(new GridJobMetricsProcessor(ctx));

        ctx.start();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ctx.stop(true);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testJobMetricsMultiThreaded() throws Exception {
        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                try {
                    int i = 0;
                    while (i++ < 1000)
                        ctx.jobMetric().addSnapshot(new GridJobMetricsSnapshot());
                }
                catch (Exception e) {
                    fail(e.getMessage());
                }
            }
        }, THREADS_CNT, "grid-job-metrics-test");

        ctx.jobMetric().getJobMetrics();

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                try {
                    int i = 0;
                    while (i++ < 100000)
                        ctx.jobMetric().addSnapshot(new GridJobMetricsSnapshot());
                }
                catch (Exception e) {
                    fail(e.getMessage());
                }
            }
        }, THREADS_CNT, "grid-job-metrics-test");

        ctx.jobMetric().getJobMetrics();
    }

    @Test
    public void testJobMetrics() throws Exception {
        ctx.jobMetric().reset();

        final int waitTime = 1;
        final int activeJobs = 2;
        final int passiveJobs = 3;
        final int cancelJobs = 4;
        final int rejectJobs = 5;
        final int execTime = 6;
        final int startedJobs = 7;
        final int maxExecTime = 8;
        final int maxWaitTime = 9;
        final int finishedJobs = 10;
        final double cpuLoad = 11.0;

        GridJobMetricsSnapshot s = new GridJobMetricsSnapshot();

        s.setWaitTime(waitTime);
        s.setStartedJobs(startedJobs);

        s.setExecutionTime(execTime);
        s.setFinishedJobs(finishedJobs);

        s.setActiveJobs(activeJobs);
        s.setPassiveJobs(passiveJobs);
        s.setCancelJobs(cancelJobs);
        s.setRejectJobs(rejectJobs);
        s.setMaximumExecutionTime(maxExecTime);
        s.setMaximumWaitTime(maxWaitTime);
        s.setCpuLoad(cpuLoad);

        int cnt = 3;

        for(int i=0; i<cnt; i++)
            ctx.jobMetric().addSnapshot(s);

        GridJobMetrics m = ctx.jobMetric().getJobMetrics();

        assertEquals(activeJobs, m.getMaximumActiveJobs());
        assertEquals(activeJobs, m.getCurrentActiveJobs());
        assertEquals(activeJobs, m.getAverageActiveJobs());

        assertEquals(passiveJobs, m.getMaximumWaitingJobs());
        assertEquals(passiveJobs, m.getCurrentWaitingJobs());
        assertEquals(passiveJobs, m.getAverageWaitingJobs());

        assertEquals(cancelJobs, m.getMaximumCancelledJobs());
        assertEquals(cancelJobs, m.getCurrentCancelledJobs());
        assertEquals(cancelJobs, m.getAverageCancelledJobs());

        assertEquals(rejectJobs, m.getMaximumRejectedJobs());
        assertEquals(rejectJobs, m.getCurrentRejectedJobs());
        assertEquals(rejectJobs, m.getAverageRejectedJobs());

        assertEquals(maxExecTime, m.getMaximumJobExecuteTime());
        assertEquals(maxExecTime, m.getCurrentJobExecuteTime());
        assertEquals(1.0*execTime/ finishedJobs, m.getAverageJobExecuteTime());

        assertEquals(maxWaitTime, m.getMaximumJobWaitTime());
        assertEquals(maxWaitTime, m.getCurrentJobWaitTime());
        assertEquals(1.0*waitTime/ startedJobs, m.getAverageJobWaitTime());

        assertEquals(cnt* finishedJobs, m.getTotalExecutedJobs());
        assertEquals(cnt* cancelJobs, m.getTotalCancelledJobs());
        assertEquals(cnt* rejectJobs, m.getTotalRejectedJobs());

        assertEquals(cpuLoad, m.getAverageCpuLoad());

        assertEquals(cnt* execTime, m.getTotalJobsExecutionTime());
        assertEquals(0, m.getCurrentIdleTime());
    }

    @Test
    public void testGridJobMetrics() throws Exception {
        try(IgniteEx g = startGrid(0)) {

            g.compute().execute(new SimplestTask(), 1);

            Thread.sleep(IgniteConfiguration.DFLT_METRICS_UPDATE_FREQ*2);

            ReadOnlyMetricRegistry mreg = g.context().metric().registry().withPrefix(JOBS);

            assertNotNull(mreg.findMetric(STARTED));
            assertNotNull(mreg.findMetric(ACTIVE));
            assertNotNull(mreg.findMetric(WAITING));
            assertNotNull(mreg.findMetric(CANCELED));
            assertNotNull(mreg.findMetric(REJECTED));
            assertNotNull(mreg.findMetric(FINISHED));
            assertNotNull(mreg.findMetric(EXEC_TIME));
            assertNotNull(mreg.findMetric(WAIT_TIME));
            assertNotNull(mreg.findMetric(MAX_EXEC_TIME));
            assertNotNull(mreg.findMetric(MAX_WAIT_TIME));
            assertNotNull(mreg.findMetric(LAST_UPDATE_TIME));

            assertEquals(0, ((IntMetric)mreg.findMetric(STARTED)).value());
            assertEquals(0, ((IntMetric)mreg.findMetric(ACTIVE)).value());
            assertEquals(0, ((IntMetric)mreg.findMetric(WAITING)).value());
            assertEquals(0, ((IntMetric)mreg.findMetric(CANCELED)).value());
            assertEquals(0, ((IntMetric)mreg.findMetric(REJECTED)).value());
            assertEquals(0, ((IntMetric)mreg.findMetric(FINISHED)).value());
            assertEquals(0, ((LongMetric)mreg.findMetric(EXEC_TIME)).value());
            assertEquals(0, ((LongMetric)mreg.findMetric(WAIT_TIME)).value());
            assertEquals(0, ((LongMetric)mreg.findMetric(MAX_EXEC_TIME)).value());
            assertEquals(0, ((LongMetric)mreg.findMetric(MAX_WAIT_TIME)).value());
        }
    }

    /** */
    private static class SimplestJob implements ComputeJob {
        /** */
        @Override public void cancel() {
            // No-op.
        }

        /** */
        @Override public Object execute() throws IgniteException {
            return "1";
        }
    }

    /** */
    private static class SimplestTask extends ComputeTaskAdapter<Object, Object> {
        /** */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Object arg) throws IgniteException {
            Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

            for (ClusterNode node : subgrid)
                jobs.put(new SimplestJob(), node);

            return jobs;
        }

        /** */
        @Nullable @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
            return "1";
        }
    }
}
