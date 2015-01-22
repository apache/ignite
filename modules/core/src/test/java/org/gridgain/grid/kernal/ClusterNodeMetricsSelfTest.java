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

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.messaging.*;
import org.gridgain.grid.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.events.IgniteEventType.*;

/**
 * Grid node metrics self test.
 */
@GridCommonTest(group = "Kernal Self")
public class ClusterNodeMetricsSelfTest extends GridCommonAbstractTest {
    /** Test message size. */
    private static final int MSG_SIZE = 1024;

    /** Number of messages. */
    private static final int MSG_CNT = 3;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration();
        cfg.setMetricsUpdateFrequency(0);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleTaskMetrics() throws Exception {
        Ignite ignite = grid();

        ignite.compute().execute(new GridTestTask(), "testArg");

        // Let metrics update twice.
        final CountDownLatch latch = new CountDownLatch(2);

        ignite.events().localListen(new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent evt) {
                assert evt.type() == EVT_NODE_METRICS_UPDATED;

                latch.countDown();

                return true;
            }
        }, EVT_NODE_METRICS_UPDATED);

        // Wait for metrics update.
        latch.await();

        ClusterNodeMetrics metrics = ignite.cluster().localNode().metrics();

        info("Node metrics: " + metrics);

        assert metrics.getAverageActiveJobs() > 0;
        assert metrics.getAverageCancelledJobs() == 0;
        assert metrics.getAverageJobExecuteTime() >= 0;
        assert metrics.getAverageJobWaitTime() >= 0;
        assert metrics.getAverageRejectedJobs() == 0;
        assert metrics.getAverageWaitingJobs() == 0;
        assert metrics.getCurrentActiveJobs() == 0;
        assert metrics.getCurrentCancelledJobs() == 0;
        assert metrics.getCurrentJobExecuteTime() == 0;
        assert metrics.getCurrentJobWaitTime() == 0;
        assert metrics.getCurrentWaitingJobs() == 0;
        assert metrics.getMaximumActiveJobs() == 1;
        assert metrics.getMaximumCancelledJobs() == 0;
        assert metrics.getMaximumJobExecuteTime() >= 0;
        assert metrics.getMaximumJobWaitTime() >= 0;
        assert metrics.getMaximumRejectedJobs() == 0;
        assert metrics.getMaximumWaitingJobs() == 0;
        assert metrics.getTotalCancelledJobs() == 0;
        assert metrics.getTotalExecutedJobs() == 1;
        assert metrics.getTotalRejectedJobs() == 0;
        assert metrics.getTotalExecutedTasks() == 1;

        assertTrue("MaximumJobExecuteTime=" + metrics.getMaximumJobExecuteTime() +
            " is less than AverageJobExecuteTime=" + metrics.getAverageJobExecuteTime(),
            metrics.getMaximumJobExecuteTime() >= metrics.getAverageJobExecuteTime());
    }

    /**
     * @throws Exception If failed.
     */
    public void testInternalTaskMetrics() throws Exception {
        Ignite ignite = grid();

        // Visor task is internal and should not affect metrics.
        ignite.compute().withName("visor-test-task").execute(new TestInternalTask(), "testArg");

        // Let metrics update twice.
        final CountDownLatch latch = new CountDownLatch(2);

        ignite.events().localListen(new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent evt) {
                assert evt.type() == EVT_NODE_METRICS_UPDATED;

                latch.countDown();

                return true;
            }
        }, EVT_NODE_METRICS_UPDATED);

        // Wait for metrics update.
        latch.await();

        ClusterNodeMetrics metrics = ignite.cluster().localNode().metrics();

        info("Node metrics: " + metrics);

        assert metrics.getAverageActiveJobs() == 0;
        assert metrics.getAverageCancelledJobs() == 0;
        assert metrics.getAverageJobExecuteTime() == 0;
        assert metrics.getAverageJobWaitTime() == 0;
        assert metrics.getAverageRejectedJobs() == 0;
        assert metrics.getAverageWaitingJobs() == 0;
        assert metrics.getCurrentActiveJobs() == 0;
        assert metrics.getCurrentCancelledJobs() == 0;
        assert metrics.getCurrentJobExecuteTime() == 0;
        assert metrics.getCurrentJobWaitTime() == 0;
        assert metrics.getCurrentWaitingJobs() == 0;
        assert metrics.getMaximumActiveJobs() == 0;
        assert metrics.getMaximumCancelledJobs() == 0;
        assert metrics.getMaximumJobExecuteTime() == 0;
        assert metrics.getMaximumJobWaitTime() == 0;
        assert metrics.getMaximumRejectedJobs() == 0;
        assert metrics.getMaximumWaitingJobs() == 0;
        assert metrics.getTotalCancelledJobs() == 0;
        assert metrics.getTotalExecutedJobs() == 0;
        assert metrics.getTotalRejectedJobs() == 0;
        assert metrics.getTotalExecutedTasks() == 0;

        assertTrue("MaximumJobExecuteTime=" + metrics.getMaximumJobExecuteTime() +
            " is less than AverageJobExecuteTime=" + metrics.getAverageJobExecuteTime(),
            metrics.getMaximumJobExecuteTime() >= metrics.getAverageJobExecuteTime());
    }

    /**
     * @throws Exception If failed.
     */
    public void testIoMetrics() throws Exception {
        Ignite ignite0 = grid();
        Ignite ignite1 = startGrid(1);

        Object msg = new TestMessage();

        int size = ignite0.configuration().getMarshaller().marshal(msg).length;

        assert size > MSG_SIZE;

        final CountDownLatch latch = new CountDownLatch(MSG_CNT);

        ignite0.message().localListen(null, new MessagingListenActor<TestMessage>() {
            @Override protected void receive(UUID nodeId, TestMessage rcvMsg) throws Throwable {
                latch.countDown();
            }
        });

        ignite1.message().localListen(null, new MessagingListenActor<TestMessage>() {
            @Override protected void receive(UUID nodeId, TestMessage rcvMsg) throws Throwable {
                respond(rcvMsg);
            }
        });

        for (int i = 0; i < MSG_CNT; i++)
            message(ignite0.cluster().forRemotes()).send(null, msg);

        latch.await();

        ClusterNodeMetrics metrics = ignite0.cluster().localNode().metrics();

        info("Node 0 metrics: " + metrics);

        // Time sync messages are being sent.
        assert metrics.getSentMessagesCount() >= MSG_CNT;
        assert metrics.getSentBytesCount() > size * MSG_CNT;
        assert metrics.getReceivedMessagesCount() >= MSG_CNT;
        assert metrics.getReceivedBytesCount() > size * MSG_CNT;

        metrics = ignite1.cluster().localNode().metrics();

        info("Node 1 metrics: " + metrics);

        // Time sync messages are being sent.
        assert metrics.getSentMessagesCount() >= MSG_CNT;
        assert metrics.getSentBytesCount() > size * MSG_CNT;
        assert metrics.getReceivedMessagesCount() >= MSG_CNT;
        assert metrics.getReceivedBytesCount() > size * MSG_CNT;
    }

    /**
     * Test message.
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class TestMessage implements Serializable {
        /** */
        private final byte[] arr = new byte[MSG_SIZE];
    }

    /**
     * Test internal task.
     */
    @GridInternal
    private static class TestInternalTask extends GridTestTask {
        // No-op.
    }
}
