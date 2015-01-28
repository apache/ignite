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

package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.mxbean.*;
import org.apache.ignite.spi.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Management bean for {@link TcpDiscoverySpi}.
 */
public interface TcpDiscoverySpiMBean extends IgniteSpiManagementMBean {
    /**
     * Gets delay between heartbeat messages sent by coordinator.
     *
     * @return Time period in milliseconds.
     */
    @IgniteMXBeanDescription("Heartbeat frequency.")
    public long getHeartbeatFrequency();

    /**
     * Gets current SPI state.
     *
     * @return Current SPI state.
     */
    @IgniteMXBeanDescription("SPI state.")
    public String getSpiState();

    /**
     * Gets {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder} (string representation).
     *
     * @return IPFinder (string representation).
     */
    @IgniteMXBeanDescription("IP Finder.")
    public String getIpFinderFormatted();

    /**
     * Gets number of connection attempts.
     *
     * @return Number of connection attempts.
     */
    @IgniteMXBeanDescription("Reconnect count.")
    public int getReconnectCount();

    /**
     * Gets network timeout.
     *
     * @return Network timeout.
     */
    @IgniteMXBeanDescription("Network timeout.")
    public long getNetworkTimeout();

    /**
     * Gets local TCP port SPI listens to.
     *
     * @return Local port range.
     */
    @IgniteMXBeanDescription("Local TCP port.")
    public int getLocalPort();

    /**
     * Gets local TCP port range.
     *
     * @return Local port range.
     */
    @IgniteMXBeanDescription("Local TCP port range.")
    public int getLocalPortRange();

    /**
     * Gets max heartbeats count node can miss without initiating status check.
     *
     * @return Max missed heartbeats.
     */
    @IgniteMXBeanDescription("Max missed heartbeats.")
    public int getMaxMissedHeartbeats();

    /**
     * Gets max heartbeats count node can miss without failing client node.
     *
     * @return Max missed client heartbeats.
     */
    @IgniteMXBeanDescription("Max missed client heartbeats.")
    public int getMaxMissedClientHeartbeats();

    /**
     * Gets thread priority. All threads within SPI will be started with it.
     *
     * @return Thread priority.
     */
    @IgniteMXBeanDescription("Threads priority.")
    public int getThreadPriority();

    /**
     * Gets IP finder clean frequency.
     *
     * @return IP finder clean frequency.
     */
    @IgniteMXBeanDescription("IP finder clean frequency.")
    public long getIpFinderCleanFrequency();

    /**
     * Gets statistics print frequency.
     *
     * @return Statistics print frequency in milliseconds.
     */
    @IgniteMXBeanDescription("Statistics print frequency.")
    public long getStatisticsPrintFrequency();

    /**
     * Gets message worker queue current size.
     *
     * @return Message worker queue current size.
     */
    @IgniteMXBeanDescription("Message worker queue current size.")
    public int getMessageWorkerQueueSize();

    /**
     * Gets joined nodes count.
     *
     * @return Nodes joined count.
     */
    @IgniteMXBeanDescription("Nodes joined count.")
    public long getNodesJoined();

    /**
     * Gets left nodes count.
     *
     * @return Left nodes count.
     */
    @IgniteMXBeanDescription("Nodes left count.")
    public long getNodesLeft();

    /**
     * Gets failed nodes count.
     *
     * @return Failed nodes count.
     */
    @IgniteMXBeanDescription("Nodes failed count.")
    public long getNodesFailed();

    /**
     * Gets pending messages registered count.
     *
     * @return Pending messages registered count.
     */
    @IgniteMXBeanDescription("Pending messages registered.")
    public long getPendingMessagesRegistered();

    /**
     * Gets pending messages discarded count.
     *
     * @return Pending messages registered count.
     */
    @IgniteMXBeanDescription("Pending messages discarded.")
    public long getPendingMessagesDiscarded();

    /**
     * Gets avg message processing time.
     *
     * @return Avg message processing time.
     */
    @IgniteMXBeanDescription("Avg message processing time.")
    public long getAvgMessageProcessingTime();

    /**
     * Gets max message processing time.
     *
     * @return Max message processing time.
     */
    @IgniteMXBeanDescription("Max message processing time.")
    public long getMaxMessageProcessingTime();

    /**
     * Gets total received messages count.
     *
     * @return Total received messages count.
     */
    @IgniteMXBeanDescription("Total received messages count.")
    public int getTotalReceivedMessages();

    /**
     * Gets received messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    @IgniteMXBeanDescription("Received messages by type.")
    public Map<String, Integer> getReceivedMessages();

    /**
     * Gets total processed messages count.
     *
     * @return Total processed messages count.
     */
    @IgniteMXBeanDescription("Total processed messages count.")
    public int getTotalProcessedMessages();

    /**
     * Gets processed messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    @IgniteMXBeanDescription("Received messages by type.")
    public Map<String, Integer> getProcessedMessages();

    /**
     * Gets time local node has been coordinator since.
     *
     * @return Time local node is coordinator since.
     */
    @IgniteMXBeanDescription("Local node is coordinator since.")
    public long getCoordinatorSinceTimestamp();

    /**
     * Gets current coordinator.
     *
     * @return Gets current coordinator.
     */
    @IgniteMXBeanDescription("Coordinator node ID.")
    @Nullable public UUID getCoordinator();

    /**
     * Gets message acknowledgement timeout.
     *
     * @return Message acknowledgement timeout.
     */
    @IgniteMXBeanDescription("Message acknowledgement timeout.")
    public long getAckTimeout();

    /**
     * Gets maximum message acknowledgement timeout.
     *
     * @return Maximum message acknowledgement timeout.
     */
    @IgniteMXBeanDescription("Maximum message acknowledgement timeout.")
    public long getMaxAckTimeout();

    /**
     * Gets socket timeout.
     *
     * @return Socket timeout.
     */
    @IgniteMXBeanDescription("Socket timeout.")
    public long getSocketTimeout();

    /**
     * Gets join timeout.
     *
     * @return Join timeout.
     */
    @IgniteMXBeanDescription("Join timeout.")
    public long getJoinTimeout();

    /**
     * Dumps debug info using configured logger.
     */
    @IgniteMXBeanDescription("Dump debug info.")
    public void dumpDebugInfo();
}
