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

package org.apache.ignite.internal.processors.cache.checker.processor;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.checker.tasks.ReconciliationResourceLimitedJob;
import org.apache.ignite.internal.processors.cache.verify.checker.tasks.PartitionReconciliationProcessorTask;
import org.apache.ignite.internal.util.GridAtomicInteger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Test that covers thread number control mechanism for reconciliation jobs.
 */
public class ReconciliationResourceLimitedJobTest extends PartitionReconciliationAbstractTest {
    /** Nodes. */
    private static final int NODES_CNT = 4;

    /** Parallelism. */
    private static final int PARALLELISM = 2;

    /** Task count. */
    private static final int TASK_CNT = 50;

    /** Session id. */
    private static final long SESSION_ID = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);

    /** Max sleep millis. */
    private static final int MAX_SLEEP_MILLIS = 200;

    /** Running jobs count per node. */
    private static Map<UUID, Integer> runningJobsCnt;

    /** Executed jobs count. */
    private static AtomicInteger executedJobsCnt;

    /** Max running jobs count. */
    private static GridAtomicInteger maxRunningJobsCnt;

    /** Crd server node. */
    private IgniteEx ig;

    /** Client node. */
    private IgniteEx client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(false)
                .setMaxSize(300L * 1024 * 1024))
        );

        cfg.setConsistentId(name);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        runningJobsCnt = new ConcurrentHashMap<>();

        executedJobsCnt = new AtomicInteger();

        maxRunningJobsCnt = new GridAtomicInteger();

        stopAllGrids();

        cleanPersistenceDir();

        ig = startGrids(NODES_CNT);

        client = startClientGrid(NODES_CNT);

        ig.cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        runningJobsCnt = null;

        executedJobsCnt = null;

        maxRunningJobsCnt = null;

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Checks that resource limitation works: maximum thread number is not exceeded and every suspended job is executed.
     */
    @Test
    public void testJobParallelism() throws Exception {
        client.compute().broadcastAsync(
            new PartitionReconciliationProcessorTask.ReconciliationSessionId(SESSION_ID, PARALLELISM)).get();

        List<ComputeTaskFuture<Void>> taskFuts = new ArrayList<>();

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < TASK_CNT; i++)
            taskFuts.add(client.compute().executeAsync(new TestReconciliationTask(), null));

        long maxRunTimeMillis = TASK_CNT * MAX_SLEEP_MILLIS * 2;

        boolean maxRunningJobsReached = GridTestUtils.waitForCondition(
            () -> maxRunningJobsCnt.get() >= PARALLELISM, maxRunTimeMillis);

        boolean allJobsAreExecuted = GridTestUtils.waitForCondition(
            () -> executedJobsCnt.get() >= NODES_CNT * TASK_CNT, maxRunTimeMillis);

        for (ComputeTaskFuture fut : taskFuts)
            fut.get();

        log.info(">>>> Finished in " + (System.currentTimeMillis() - startTime) + "ms");

        assertTrue(maxRunningJobsReached);
        assertEquals(PARALLELISM, maxRunningJobsCnt.get());

        assertTrue(allJobsAreExecuted);
        assertEquals(NODES_CNT * TASK_CNT, executedJobsCnt.get());
    }

    /** */
    private static class TestReconciliationTask extends ComputeTaskAdapter<Void, Void> {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Void arg) throws IgniteException {
            Map<ComputeJob, ClusterNode> res = new HashMap<>();

            for (ClusterNode n : subgrid)
                res.put(new TestReconciliationJob(), n);

            return res;
        }

        /** {@inheritDoc} */
        @Override public Void reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }

        /** */
        private static class TestReconciliationJob extends ReconciliationResourceLimitedJob {
            /** {@inheritDoc} */
            @Override protected long sessionId() {
                return SESSION_ID;
            }

            /** {@inheritDoc} */
            @Override protected Object execute0() {
                try {
                    int runningCnt = runningJobsCnt.compute(ignite.localNode().id(), (k, v) -> {
                        if (v == null)
                            return 1;

                        return v + 1;
                    });

                    maxRunningJobsCnt.setIfGreater(runningCnt);

                    U.sleep(ThreadLocalRandom.current().nextInt(MAX_SLEEP_MILLIS));

                    runningJobsCnt.compute(ignite.localNode().id(), (k, v) -> v - 1);

                    executedJobsCnt.incrementAndGet();
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw new IgniteException(e);
                }

                return null;
            }
        }
    }
}
