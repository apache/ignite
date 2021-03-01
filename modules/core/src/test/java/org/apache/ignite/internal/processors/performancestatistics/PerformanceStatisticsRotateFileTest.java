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

package org.apache.ignite.internal.processors.performancestatistics;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.ClientType.CLIENT;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.ClientType.SERVER;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests performance statistics.
 */
@RunWith(Parameterized.class)
public class PerformanceStatisticsRotateFileTest extends AbstractPerformanceStatisticsTest {
    /** Nodes count. */
    private static final int NODES_CNT = 2;

    /** Client type to run operations from. */
    @Parameterized.Parameter
    public ClientType clientType;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "clientType={0}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {{SERVER}, {CLIENT}});
    }

    /** Ignite. */
    private static IgniteEx srv;

    /** Ignite node to run load from. */
    private static IgniteEx node;

    /** Listener test logger. */
    private static final ListeningTestLogger listeningTestLog = new ListeningTestLogger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(defaultCacheConfiguration());
        cfg.setGridLogger(listeningTestLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        srv = startGrid(NODES_CNT - 1);

        IgniteEx client = startClientGrid(NODES_CNT);

        node = clientType == SERVER ? srv : client;
    }

    /** @throws Exception If failed. */
    @Test
    public void testRotateFile() throws Exception {
        String testTaskName = "testTask";
        int executions = 5;
        long startTime = U.currentTimeMillis();

        startCollectStatistics();

        IgniteRunnable task = new IgniteRunnable() {
            @Override public void run() {
                // No-op.
            }
        };

        for (int i = 0; i < executions; i++)
            node.compute().withName(testTaskName).run(task);

        HashMap<IgniteUuid, Integer> sessions = new HashMap<>();
        AtomicInteger tasks = new AtomicInteger();
        AtomicInteger jobs = new AtomicInteger();

        LogListener logLsnr = LogListener.matches("Performance statistics writer updated.").build();

        listeningTestLog.registerListener(logLsnr);

        rotateCollectStatistics();

        assertTrue(waitForCondition(logLsnr::check, TIMEOUT));

        List<File> files = statisticsFiles().stream()
            .filter(f -> !f.getName().endsWith("-1.prf"))
            .collect(Collectors.toList());

        TestHandler hnd = new TestHandler() {
            @Override public void task(UUID nodeId, IgniteUuid sesId, String taskName, long taskStartTime,
                long duration, int affPartId) {
                sessions.compute(sesId, (uuid, cnt) -> cnt == null ? 1 : ++cnt);

                tasks.incrementAndGet();

                assertEquals(node.context().localNodeId(), nodeId);
                assertEquals(testTaskName, taskName);
                assertTrue(taskStartTime >= startTime);
                assertTrue(duration >= 0);
                assertEquals(-1, affPartId);
            }

            @Override public void job(UUID nodeId, IgniteUuid sesId, long queuedTime, long jobStartTime, long duration,
                boolean timedOut) {
                sessions.compute(sesId, (uuid, cnt) -> cnt == null ? 1 : ++cnt);

                jobs.incrementAndGet();

                assertEquals(srv.context().localNodeId(), nodeId);
                assertTrue(queuedTime >= 0);
                assertTrue(jobStartTime >= startTime);
                assertTrue(duration >= 0);
                assertFalse(timedOut);
            }
        };

        readFiles(files, hnd);

        check(executions, tasks.get(), jobs.get(), sessions.values());

        tasks.set(0);
        jobs.set(0);
        sessions.clear();

        for (int i = 0; i < executions; i++)
            node.compute().withName(testTaskName).run(task);

        stopCollectStatistics();

        files = statisticsFiles().stream()
            .filter(f -> f.getName().endsWith("-1.prf"))
            .collect(Collectors.toList());

        readFiles(files, hnd);

        check(executions, tasks.get(), jobs.get(), sessions.values());
    }

    /**
     * @param executions Executions.
     * @param task Task.
     * @param jobs Jobs.
     * @param vals Vals.
     */
    private void check(int executions, int task, int jobs, Collection<Integer> vals) {
        assertEquals(executions, task);
        assertEquals(executions, jobs);

        assertEquals(executions, vals.size());
        assertTrue("Invalid sessions: ", vals.stream().allMatch(cnt -> cnt == NODES_CNT));
    }
}
