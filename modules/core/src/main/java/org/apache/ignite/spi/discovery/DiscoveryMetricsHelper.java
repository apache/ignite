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
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Helper class to serialize and deserialize node metrics.
 */
public final class DiscoveryMetricsHelper {
    /** Size of serialized node metrics. */
    public static final int METRICS_SIZE =
        4/*max active jobs*/ +
        4/*current active jobs*/ +
        4/*average active jobs*/ +
        4/*max waiting jobs*/ +
        4/*current waiting jobs*/ +
        4/*average waiting jobs*/ +
        4/*max cancelled jobs*/ +
        4/*current cancelled jobs*/ +
        4/*average cancelled jobs*/ +
        4/*max rejected jobs*/ +
        4/*current rejected jobs*/ +
        4/*average rejected jobs*/ +
        4/*total executed jobs*/ +
        4/*total rejected jobs*/ +
        4/*total cancelled jobs*/ +
        8/*max job wait time*/ +
        8/*current job wait time*/ +
        8/*average job wait time*/ +
        8/*max job execute time*/ +
        8/*current job execute time*/ +
        8/*average job execute time*/ +
        4/*total executed tasks*/ +
        8/*current idle time*/ +
        8/*total idle time*/ +
        4/*available processors*/ +
        8/*current CPU load*/ +
        8/*average CPU load*/ +
        8/*current GC CPU load*/ +
        8/*heap memory init*/ +
        8/*heap memory used*/ +
        8/*heap memory committed*/ +
        8/*heap memory max*/ +
        8/*non-heap memory init*/ +
        8/*non-heap memory used*/ +
        8/*non-heap memory committed*/ +
        8/*non-heap memory max*/ +
        8/*uptime*/ +
        8/*start time*/ +
        8/*node start time*/ +
        4/*thread count*/ +
        4/*peak thread count*/ +
        8/*total started thread count*/ +
        4/*daemon thread count*/ +
        8/*last data version.*/ +
        4/*sent messages count*/ +
        8/*sent bytes count*/ +
        4/*received messages count*/ +
        8/*received bytes count*/ +
        4/*outbound messages queue size*/;

    /**
     * Enforces singleton.
     */
    private DiscoveryMetricsHelper() {
        // No-op.
    }

    /**
     * Serializes node metrics into byte array.
     *
     * @param data Byte array.
     * @param off Offset into byte array.
     * @param metrics Node metrics to serialize.
     * @return New offset.
     */
    public static int serialize(byte[] data, int off, ClusterNodeMetrics metrics) {
        int start = off;

        off = U.intToBytes(metrics.getMaximumActiveJobs(), data, off);
        off = U.intToBytes(metrics.getCurrentActiveJobs(), data, off);
        off = U.floatToBytes(metrics.getAverageActiveJobs(), data, off);
        off = U.intToBytes(metrics.getMaximumWaitingJobs(), data, off);
        off = U.intToBytes(metrics.getCurrentWaitingJobs(), data, off);
        off = U.floatToBytes(metrics.getAverageWaitingJobs(), data, off);
        off = U.intToBytes(metrics.getMaximumRejectedJobs(), data, off);
        off = U.intToBytes(metrics.getCurrentRejectedJobs(), data, off);
        off = U.floatToBytes(metrics.getAverageRejectedJobs(), data, off);
        off = U.intToBytes(metrics.getMaximumCancelledJobs(), data, off);
        off = U.intToBytes(metrics.getCurrentCancelledJobs(), data, off);
        off = U.floatToBytes(metrics.getAverageCancelledJobs(), data, off);
        off = U.intToBytes(metrics.getTotalRejectedJobs(), data , off);
        off = U.intToBytes(metrics.getTotalCancelledJobs(), data , off);
        off = U.intToBytes(metrics.getTotalExecutedJobs(), data , off);
        off = U.longToBytes(metrics.getMaximumJobWaitTime(), data, off);
        off = U.longToBytes(metrics.getCurrentJobWaitTime(), data, off);
        off = U.doubleToBytes(metrics.getAverageJobWaitTime(), data, off);
        off = U.longToBytes(metrics.getMaximumJobExecuteTime(), data, off);
        off = U.longToBytes(metrics.getCurrentJobExecuteTime(), data, off);
        off = U.doubleToBytes(metrics.getAverageJobExecuteTime(), data, off);
        off = U.intToBytes(metrics.getTotalExecutedTasks(), data, off);
        off = U.longToBytes(metrics.getCurrentIdleTime(), data, off);
        off = U.longToBytes(metrics.getTotalIdleTime(), data , off);
        off = U.intToBytes(metrics.getTotalCpus(), data, off);
        off = U.doubleToBytes(metrics.getCurrentCpuLoad(), data, off);
        off = U.doubleToBytes(metrics.getAverageCpuLoad(), data, off);
        off = U.doubleToBytes(metrics.getCurrentGcCpuLoad(), data, off);
        off = U.longToBytes(metrics.getHeapMemoryInitialized(), data, off);
        off = U.longToBytes(metrics.getHeapMemoryUsed(), data, off);
        off = U.longToBytes(metrics.getHeapMemoryCommitted(), data, off);
        off = U.longToBytes(metrics.getHeapMemoryMaximum(), data, off);
        off = U.longToBytes(metrics.getNonHeapMemoryInitialized(), data, off);
        off = U.longToBytes(metrics.getNonHeapMemoryUsed(), data, off);
        off = U.longToBytes(metrics.getNonHeapMemoryCommitted(), data, off);
        off = U.longToBytes(metrics.getNonHeapMemoryMaximum(), data, off);
        off = U.longToBytes(metrics.getStartTime(), data, off);
        off = U.longToBytes(metrics.getNodeStartTime(), data, off);
        off = U.longToBytes(metrics.getUpTime(), data, off);
        off = U.intToBytes(metrics.getCurrentThreadCount(), data, off);
        off = U.intToBytes(metrics.getMaximumThreadCount(), data, off);
        off = U.longToBytes(metrics.getTotalStartedThreadCount(), data, off);
        off = U.intToBytes(metrics.getCurrentDaemonThreadCount(), data, off);
        off = U.longToBytes(metrics.getLastDataVersion(), data, off);
        off = U.intToBytes(metrics.getSentMessagesCount(), data, off);
        off = U.longToBytes(metrics.getSentBytesCount(), data, off);
        off = U.intToBytes(metrics.getReceivedMessagesCount(), data, off);
        off = U.longToBytes(metrics.getReceivedBytesCount(), data, off);
        off = U.intToBytes(metrics.getOutboundMessagesQueueSize(), data, off);

        assert off - start == METRICS_SIZE : "Invalid metrics size [expected=" + METRICS_SIZE + ", actual=" +
            (off - start) + ']';

        return off;
    }

    /**
     * De-serializes node metrics.
     *
     * @param data Byte array.
     * @param off Offset into byte array.
     * @return Deserialized node metrics.
     */
    public static ClusterNodeMetrics deserialize(byte[] data, int off) {
        int start = off;

        DiscoveryNodeMetricsAdapter metrics = new DiscoveryNodeMetricsAdapter();

        metrics.setLastUpdateTime(U.currentTimeMillis());

        metrics.setMaximumActiveJobs(U.bytesToInt(data, off));

        off += 4;

        metrics.setCurrentActiveJobs(U.bytesToInt(data, off));

        off += 4;

        metrics.setAverageActiveJobs(U.bytesToFloat(data, off));

        off += 4;

        metrics.setMaximumWaitingJobs(U.bytesToInt(data, off));

        off += 4;

        metrics.setCurrentWaitingJobs(U.bytesToInt(data, off));

        off += 4;

        metrics.setAverageWaitingJobs(U.bytesToFloat(data, off));

        off += 4;

        metrics.setMaximumRejectedJobs(U.bytesToInt(data, off));

        off += 4;

        metrics.setCurrentRejectedJobs(U.bytesToInt(data, off));

        off += 4;

        metrics.setAverageRejectedJobs(U.bytesToFloat(data, off));

        off += 4;

        metrics.setMaximumCancelledJobs(U.bytesToInt(data, off));

        off += 4;

        metrics.setCurrentCancelledJobs(U.bytesToInt(data, off));

        off += 4;

        metrics.setAverageCancelledJobs(U.bytesToFloat(data, off));

        off += 4;

        metrics.setTotalRejectedJobs(U.bytesToInt(data, off));

        off += 4;

        metrics.setTotalCancelledJobs(U.bytesToInt(data, off));

        off += 4;

        metrics.setTotalExecutedJobs(U.bytesToInt(data, off));

        off += 4;

        metrics.setMaximumJobWaitTime(U.bytesToLong(data, off));

        off += 8;

        metrics.setCurrentJobWaitTime(U.bytesToLong(data, off));

        off += 8;

        metrics.setAverageJobWaitTime(U.bytesToDouble(data, off));

        off += 8;

        metrics.setMaximumJobExecuteTime(U.bytesToLong(data, off));

        off += 8;

        metrics.setCurrentJobExecuteTime(U.bytesToLong(data, off));

        off += 8;

        metrics.setAverageJobExecuteTime(U.bytesToDouble(data, off));

        off += 8;

        metrics.setTotalExecutedTasks(U.bytesToInt(data, off));

        off += 4;

        metrics.setCurrentIdleTime(U.bytesToLong(data, off));

        off += 8;

        metrics.setTotalIdleTime(U.bytesToLong(data, off));

        off += 8;

        metrics.setAvailableProcessors(U.bytesToInt(data, off));

        off += 4;

        metrics.setCurrentCpuLoad(U.bytesToDouble(data, off));

        off += 8;

        metrics.setAverageCpuLoad(U.bytesToDouble(data, off));

        off += 8;

        metrics.setCurrentGcCpuLoad(U.bytesToDouble(data, off));

        off += 8;

        metrics.setHeapMemoryInitialized(U.bytesToLong(data, off));

        off += 8;

        metrics.setHeapMemoryUsed(U.bytesToLong(data, off));

        off += 8;

        metrics.setHeapMemoryCommitted(U.bytesToLong(data, off));

        off += 8;

        metrics.setHeapMemoryMaximum(U.bytesToLong(data, off));

        off += 8;

        metrics.setNonHeapMemoryInitialized(U.bytesToLong(data, off));

        off += 8;

        metrics.setNonHeapMemoryUsed(U.bytesToLong(data, off));

        off += 8;

        metrics.setNonHeapMemoryCommitted(U.bytesToLong(data, off));

        off += 8;

        metrics.setNonHeapMemoryMaximum(U.bytesToLong(data, off));

        off += 8;

        metrics.setStartTime(U.bytesToLong(data, off));

        off += 8;

        metrics.setNodeStartTime(U.bytesToLong(data, off));

        off += 8;

        metrics.setUpTime(U.bytesToLong(data, off));

        off += 8;

        metrics.setCurrentThreadCount(U.bytesToInt(data, off));

        off += 4;

        metrics.setMaximumThreadCount(U.bytesToInt(data, off));

        off += 4;

        metrics.setTotalStartedThreadCount(U.bytesToLong(data, off));

        off += 8;

        metrics.setCurrentDaemonThreadCount(U.bytesToInt(data, off));

        off += 4;

        metrics.setLastDataVersion(U.bytesToLong(data, off));

        off += 8;

        metrics.setSentMessagesCount(U.bytesToInt(data, off));

        off += 4;

        metrics.setSentBytesCount(U.bytesToLong(data, off));

        off += 8;

        metrics.setReceivedMessagesCount(U.bytesToInt(data, off));

        off += 4;

        metrics.setReceivedBytesCount(U.bytesToLong(data, off));

        off += 8;

        metrics.setOutboundMessagesQueueSize(U.bytesToInt(data, off));

        off += 4;

        assert off - start == METRICS_SIZE : "Invalid metrics size [expected=" + METRICS_SIZE + ", actual=" +
            (off - start) + ']';

        return metrics;
    }
}
