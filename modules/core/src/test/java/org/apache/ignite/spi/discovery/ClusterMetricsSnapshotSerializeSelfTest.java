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

import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 * Grid discovery metrics test.
 */
@GridCommonTest(group = "Utils")
public class ClusterMetricsSnapshotSerializeSelfTest extends GridCommonAbstractTest {
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

        assert metrics1.equals(metrics2);
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

        return metrics;
    }
}
