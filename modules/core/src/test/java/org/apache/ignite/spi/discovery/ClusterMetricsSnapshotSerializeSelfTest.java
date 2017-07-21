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

package org.apache.ignite.spi.discovery;

import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Grid discovery metrics test.
 */
@GridCommonTest(group = "Utils")
public class ClusterMetricsSnapshotSerializeSelfTest extends GridCommonAbstractTest {
    /** Metrics serialized by Ignite 1.0 */
    private static final byte[] METRICS_V1 = {0, 0, 0, 22, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, 27, 0, 0, 0, 15, 64,
        (byte)-32, 0, 0, 0, 0, 0, 26, 0, 0, 0, 14, 64, (byte)-64, 0, 0, 0, 0, 0, 23, 0, 0, 0, 9, 64, 64, 0, 0, 0, 0, 0,
        39, 0, 0, 0, 36, 0, 0, 0, 37, 0, 0, 0, 0, 0, 0, 0, 25, 0, 0, 0, 0, 0, 0, 0, 13, 64, 20, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 12, 64, 16, 0, 0, 0, 0, 0, 0, (byte)-1, (byte)-1, (byte)-1, (byte)-1, 0,
        0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 38, 0, 0, 0, 1, 64, 65, 0, 0, 0, 0, 0, 0, (byte)-65, (byte)-16, 0, 0,
        0, 0, 0, 0, (byte)-65, (byte)-16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 18, 0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0,
        0, 0, 0, 0, 17, 0, 0, 0, 0, 0, 0, 0, 19, 0, 0, 0, 0, 0, 0, 0, 48, 0, 0, 0, 0, 0, 0, 0, 29, 0, 0, 0, 0, 0, 0, 0,
        31, 0, 0, 0, 0, 0, 0, 0, 28, 0, 0, 0, 0, 0, 0, 0, 30, 0, 0, 0, 0, 0, 0, 0, 47, 0, 0, 0, 0, 0, 0, 0, 33,
        (byte)-1, (byte)-1, (byte)-1, (byte)-1, (byte)-1, (byte)-1, (byte)-1, (byte)-1, 0, 0, 0, 0, 0, 0, 0, 41, 0, 0,
        0, 35, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 40, 0, 0, 0, 16, (byte)-1, (byte)-1, (byte)-1, (byte)-1, (byte)-1,
        (byte)-1, (byte)-1, (byte)-1, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0, 43, 0, 0, 0, 44, 0, 0, 0, 0, 0, 0, 0, 45, 0, 0,
        0, 46, (byte)-1, (byte)-1, (byte)-1, (byte)-1};

    /** */
    public ClusterMetricsSnapshotSerializeSelfTest() {
        super(false /*don't start grid*/);
    }

    /** */
    public void testMetricsSize() {
        byte[] data = new byte[ClusterMetricsSnapshot.METRICS_SIZE];

        // Test serialization.
        int off = ClusterMetricsSnapshot.serialize(data, 0, createMetrics());

        assert off == ClusterMetricsSnapshot.METRICS_SIZE;

        // Test deserialization.
        ClusterMetrics res = ClusterMetricsSnapshot.deserialize(data, 0);

        assert res != null;
    }

    /** */
    public void testSerialization() {
        byte[] data = new byte[ClusterMetricsSnapshot.METRICS_SIZE];

        ClusterMetrics metrics1 = createMetrics();

        // Test serialization.
        int off = ClusterMetricsSnapshot.serialize(data, 0, metrics1);

        assert off == ClusterMetricsSnapshot.METRICS_SIZE;

        // Test deserialization.
        ClusterMetrics metrics2 = ClusterMetricsSnapshot.deserialize(data, 0);

        assert metrics2 != null;

        assert isMetricsEquals(metrics1, metrics2);
    }

    /**
     * Checks compatibility with old serialized metrics.
     */
    public void testMetricsCompatibility() {
        ClusterMetrics metrics = ClusterMetricsSnapshot.deserialize(METRICS_V1, 0);

        assert metrics != null;
    }

    /**
     * @return Test metrics.
     */
    private ClusterMetrics createMetrics() {
        ClusterMetricsSnapshot metrics = new ClusterMetricsSnapshot();

        metrics.setAvailableProcessors(1);
        metrics.setAverageActiveJobs(2);
        metrics.setAverageCancelledJobs(3);
        metrics.setAverageJobExecuteTime(4);
        metrics.setAverageJobWaitTime(5);
        metrics.setAverageRejectedJobs(6);
        metrics.setAverageWaitingJobs(7);
        metrics.setCurrentActiveJobs(8);
        metrics.setCurrentCancelledJobs(9);
        metrics.setCurrentIdleTime(10);
        metrics.setCurrentIdleTime(11);
        metrics.setCurrentJobExecuteTime(12);
        metrics.setCurrentJobWaitTime(13);
        metrics.setCurrentRejectedJobs(14);
        metrics.setCurrentWaitingJobs(15);
        metrics.setCurrentDaemonThreadCount(16);
        metrics.setHeapMemoryCommitted(17);
        metrics.setHeapMemoryInitialized(18);
        metrics.setHeapMemoryMaximum(19);
        metrics.setHeapMemoryUsed(20);
        metrics.setLastUpdateTime(21);
        metrics.setMaximumActiveJobs(22);
        metrics.setMaximumCancelledJobs(23);
        metrics.setMaximumJobExecuteTime(24);
        metrics.setMaximumJobWaitTime(25);
        metrics.setMaximumRejectedJobs(26);
        metrics.setMaximumWaitingJobs(27);
        metrics.setNonHeapMemoryCommitted(28);
        metrics.setNonHeapMemoryInitialized(29);
        metrics.setNonHeapMemoryMaximum(30);
        metrics.setNonHeapMemoryUsed(31);
        metrics.setMaximumThreadCount(32);
        metrics.setStartTime(33);
        metrics.setCurrentCpuLoad(34);
        metrics.setCurrentThreadCount(35);
        metrics.setTotalCancelledJobs(36);
        metrics.setTotalExecutedJobs(37);
        metrics.setTotalIdleTime(38);
        metrics.setTotalRejectedJobs(39);
        metrics.setTotalStartedThreadCount(40);
        metrics.setUpTime(41);
        metrics.setSentMessagesCount(42);
        metrics.setSentBytesCount(43);
        metrics.setReceivedMessagesCount(44);
        metrics.setReceivedBytesCount(45);
        metrics.setOutboundMessagesQueueSize(46);
        metrics.setNonHeapMemoryTotal(47);
        metrics.setHeapMemoryTotal(48);

        return metrics;
    }

    /**
     * @param obj Object.
     * @param obj1 Object 1.
     */
    @SuppressWarnings("FloatingPointEquality")
    private boolean isMetricsEquals(ClusterMetrics obj, ClusterMetrics obj1) {
        return
            obj.getAverageActiveJobs() == obj1.getAverageActiveJobs() &&
            obj.getAverageCancelledJobs() == obj1.getAverageCancelledJobs() &&
            obj.getAverageJobExecuteTime() == obj1.getAverageJobExecuteTime() &&
            obj.getAverageJobWaitTime() == obj1.getAverageJobWaitTime() &&
            obj.getAverageRejectedJobs() == obj1.getAverageRejectedJobs() &&
            obj.getAverageWaitingJobs() == obj1.getAverageWaitingJobs() &&
            obj.getCurrentActiveJobs() == obj1.getCurrentActiveJobs() &&
            obj.getCurrentCancelledJobs() == obj1.getCurrentCancelledJobs() &&
            obj.getCurrentIdleTime() == obj1.getCurrentIdleTime() &&
            obj.getCurrentJobExecuteTime() == obj1.getCurrentJobExecuteTime() &&
            obj.getCurrentJobWaitTime() == obj1.getCurrentJobWaitTime() &&
            obj.getCurrentRejectedJobs() == obj1.getCurrentRejectedJobs() &&
            obj.getCurrentWaitingJobs() == obj1.getCurrentWaitingJobs() &&
            obj.getCurrentDaemonThreadCount() == obj1.getCurrentDaemonThreadCount() &&
            obj.getHeapMemoryCommitted() == obj1.getHeapMemoryCommitted() &&
            obj.getHeapMemoryInitialized() == obj1.getHeapMemoryInitialized() &&
            obj.getHeapMemoryMaximum() == obj1.getHeapMemoryMaximum() &&
            obj.getHeapMemoryUsed() == obj1.getHeapMemoryUsed() &&
            obj.getMaximumActiveJobs() == obj1.getMaximumActiveJobs() &&
            obj.getMaximumCancelledJobs() == obj1.getMaximumCancelledJobs() &&
            obj.getMaximumJobExecuteTime() == obj1.getMaximumJobExecuteTime() &&
            obj.getMaximumJobWaitTime() == obj1.getMaximumJobWaitTime() &&
            obj.getMaximumRejectedJobs() == obj1.getMaximumRejectedJobs() &&
            obj.getMaximumWaitingJobs() == obj1.getMaximumWaitingJobs() &&
            obj.getNonHeapMemoryCommitted() == obj1.getNonHeapMemoryCommitted() &&
            obj.getNonHeapMemoryInitialized() == obj1.getNonHeapMemoryInitialized() &&
            obj.getNonHeapMemoryMaximum() == obj1.getNonHeapMemoryMaximum() &&
            obj.getNonHeapMemoryUsed() == obj1.getNonHeapMemoryUsed() &&
            obj.getMaximumThreadCount() == obj1.getMaximumThreadCount() &&
            obj.getStartTime() == obj1.getStartTime() &&
            obj.getCurrentCpuLoad() == obj1.getCurrentCpuLoad() &&
            obj.getCurrentThreadCount() == obj1.getCurrentThreadCount() &&
            obj.getTotalCancelledJobs() == obj1.getTotalCancelledJobs() &&
            obj.getTotalExecutedJobs() == obj1.getTotalExecutedJobs() &&
            obj.getTotalIdleTime() == obj1.getTotalIdleTime() &&
            obj.getTotalRejectedJobs() == obj1.getTotalRejectedJobs() &&
            obj.getTotalStartedThreadCount() == obj1.getTotalStartedThreadCount() &&
            obj.getUpTime() == obj1.getUpTime() &&
            obj.getSentMessagesCount() == obj1.getSentMessagesCount() &&
            obj.getSentBytesCount() == obj1.getSentBytesCount() &&
            obj.getReceivedMessagesCount() == obj1.getReceivedMessagesCount() &&
            obj.getReceivedBytesCount() == obj1.getReceivedBytesCount() &&
            obj.getOutboundMessagesQueueSize() == obj1.getOutboundMessagesQueueSize() &&
            obj.getNonHeapMemoryTotal() == obj1.getNonHeapMemoryTotal() &&
            obj.getHeapMemoryTotal() == obj1.getHeapMemoryTotal();
    }
}