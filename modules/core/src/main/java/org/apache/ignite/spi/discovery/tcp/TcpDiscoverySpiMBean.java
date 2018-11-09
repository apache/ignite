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

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.mxbean.MXBeanDescription;
import org.apache.ignite.mxbean.MXBeanParametersDescriptions;
import org.apache.ignite.mxbean.MXBeanParametersNames;
import org.apache.ignite.spi.IgniteSpiManagementMBean;
import org.apache.ignite.spi.discovery.DiscoverySpiMBean;
import org.jetbrains.annotations.Nullable;

/**
 * Management bean for {@link TcpDiscoverySpi}.
 */
@MXBeanDescription("MBean provide access to TCP-based discovery SPI.")
public interface TcpDiscoverySpiMBean extends IgniteSpiManagementMBean, DiscoverySpiMBean {
    /**
     * Gets current SPI state.
     *
     * @return Current SPI state.
     */
    @MXBeanDescription("SPI state.")
    @Override public String getSpiState();

    /**
     * Gets {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder} (string representation).
     *
     * @return IPFinder (string representation).
     */
    @MXBeanDescription("IP Finder.")
    public String getIpFinderFormatted();

    /**
     * Gets number of connection attempts.
     *
     * @return Number of connection attempts.
     */
    @MXBeanDescription("Reconnect count.")
    public int getReconnectCount();

    /**
     * Gets connection check interval in ms.
     *
     * @return Connection check interval.
     */
    @MXBeanDescription("Connection check interval.")
    public long getConnectionCheckInterval();

    /**
     * Gets network timeout.
     *
     * @return Network timeout.
     */
    @MXBeanDescription("Network timeout.")
    public long getNetworkTimeout();

    /**
     * Gets local TCP port SPI listens to.
     *
     * @return Local port range.
     */
    @MXBeanDescription("Local TCP port.")
    public int getLocalPort();

    /**
     * Gets local TCP port range.
     *
     * @return Local port range.
     */
    @MXBeanDescription("Local TCP port range.")
    public int getLocalPortRange();

    /**
     * Gets thread priority. All threads within SPI will be started with it.
     *
     * @return Thread priority.
     */
    @MXBeanDescription("Threads priority.")
    public int getThreadPriority();

    /**
     * Gets IP finder clean frequency.
     *
     * @return IP finder clean frequency.
     */
    @MXBeanDescription("IP finder clean frequency.")
    public long getIpFinderCleanFrequency();

    /**
     * Gets statistics print frequency.
     *
     * @return Statistics print frequency in milliseconds.
     */
    @MXBeanDescription("Statistics print frequency.")
    public long getStatisticsPrintFrequency();

    /**
     * Gets message worker queue current size.
     *
     * @return Message worker queue current size.
     */
    @MXBeanDescription("Message worker queue current size.")
    public int getMessageWorkerQueueSize();

    /**
     * Gets joined nodes count.
     *
     * @return Nodes joined count.
     */
    @MXBeanDescription("Nodes joined count.")
    @Override public long getNodesJoined();

    /**
     * Gets left nodes count.
     *
     * @return Left nodes count.
     */
    @MXBeanDescription("Nodes left count.")
    @Override public long getNodesLeft();

    /**
     * Gets failed nodes count.
     *
     * @return Failed nodes count.
     */
    @MXBeanDescription("Nodes failed count.")
    @Override public long getNodesFailed();

    /**
     * Gets pending messages registered count.
     *
     * @return Pending messages registered count.
     */
    @MXBeanDescription("Pending messages registered.")
    public long getPendingMessagesRegistered();

    /**
     * Gets pending messages discarded count.
     *
     * @return Pending messages registered count.
     */
    @MXBeanDescription("Pending messages discarded.")
    public long getPendingMessagesDiscarded();

    /**
     * Gets avg message processing time.
     *
     * @return Avg message processing time.
     */
    @MXBeanDescription("Avg message processing time.")
    public long getAvgMessageProcessingTime();

    /**
     * Gets max message processing time.
     *
     * @return Max message processing time.
     */
    @MXBeanDescription("Max message processing time.")
    public long getMaxMessageProcessingTime();

    /**
     * Gets total received messages count.
     *
     * @return Total received messages count.
     */
    @MXBeanDescription("Total received messages count.")
    public int getTotalReceivedMessages();

    /**
     * Gets received messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    @MXBeanDescription("Received messages by type.")
    public Map<String, Integer> getReceivedMessages();

    /**
     * Gets total processed messages count.
     *
     * @return Total processed messages count.
     */
    @MXBeanDescription("Total processed messages count.")
    public int getTotalProcessedMessages();

    /**
     * Gets processed messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    @MXBeanDescription("Processed messages by type.")
    public Map<String, Integer> getProcessedMessages();

    /**
     * Gets time local node has been coordinator since.
     *
     * @return Time local node is coordinator since.
     */
    @MXBeanDescription("Local node is coordinator since.")
    public long getCoordinatorSinceTimestamp();

    /**
     * Gets current coordinator.
     *
     * @return Gets current coordinator.
     */
    @MXBeanDescription("Coordinator node ID.")
    @Override @Nullable public UUID getCoordinator();

    /**
     * Gets message acknowledgement timeout.
     *
     * @return Message acknowledgement timeout.
     */
    @MXBeanDescription("Message acknowledgement timeout.")
    public long getAckTimeout();

    /**
     * Gets maximum message acknowledgement timeout.
     *
     * @return Maximum message acknowledgement timeout.
     */
    @MXBeanDescription("Maximum message acknowledgement timeout.")
    public long getMaxAckTimeout();

    /**
     * Gets socket timeout.
     *
     * @return Socket timeout.
     */
    @MXBeanDescription("Socket timeout.")
    public long getSocketTimeout();

    /**
     * Gets join timeout.
     *
     * @return Join timeout.
     */
    @MXBeanDescription("Join timeout.")
    public long getJoinTimeout();

    /**
     * Dumps debug info using configured logger.
     */
    @MXBeanDescription("Dump debug info.")
    public void dumpDebugInfo();

    /**
     * Whether or not discovery is started in client mode.
     *
     * @return {@code true} if node is in client mode.
     * @throws IllegalStateException If discovery SPI is not started.
     */
    @MXBeanDescription("Client mode.")
    public boolean isClientMode() throws IllegalStateException;

    /**
     * Diagnosis method for determining ring message latency.
     * On this method call special message will be sent across the ring
     * and stats about the message will appear in the logs of each node.
     *
     * @param maxHops Maximum hops for the message (3 * TOTAL_NODE_CNT is recommended).
     */
    @MXBeanDescription("Check ring latency.")
    @MXBeanParametersNames(
        {
            "maxHops"
        }
    )
    @MXBeanParametersDescriptions(
        {
            "Maximum hops for the message (3 * TOTAL_NODE_CNT is recommended)."
        }
    )
    public void checkRingLatency(int maxHops);

    /**
     * Current topology version.
     *
     * @return current topVer.
     */
    @MXBeanDescription("Get current topology version.")
    public long getCurrentTopologyVersion();

    /**
     * Dumps ring structure to log.
     */
    @MXBeanDescription("Dumps ring structure to log.")
    public void dumpRingStructure();
}
