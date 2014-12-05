/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp;

import org.apache.ignite.mbean.*;
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
    @IgniteMBeanDescription("Heartbeat frequency.")
    public long getHeartbeatFrequency();

    /**
     * Gets current SPI state.
     *
     * @return Current SPI state.
     */
    @IgniteMBeanDescription("SPI state.")
    public String getSpiState();

    /**
     * Gets {@link org.gridgain.grid.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder} (string representation).
     *
     * @return IPFinder (string representation).
     */
    @IgniteMBeanDescription("IP Finder.")
    public String getIpFinderFormatted();

    /**
     * Gets number of connection attempts.
     *
     * @return Number of connection attempts.
     */
    @IgniteMBeanDescription("Reconnect count.")
    public int getReconnectCount();

    /**
     * Gets network timeout.
     *
     * @return Network timeout.
     */
    @IgniteMBeanDescription("Network timeout.")
    public long getNetworkTimeout();

    /**
     * Gets local TCP port SPI listens to.
     *
     * @return Local port range.
     */
    @IgniteMBeanDescription("Local TCP port.")
    public int getLocalPort();

    /**
     * Gets local TCP port range.
     *
     * @return Local port range.
     */
    @IgniteMBeanDescription("Local TCP port range.")
    public int getLocalPortRange();

    /**
     * Gets max heartbeats count node can miss without initiating status check.
     *
     * @return Max missed heartbeats.
     */
    @IgniteMBeanDescription("Max missed heartbeats.")
    public int getMaxMissedHeartbeats();

    /**
     * Gets max heartbeats count node can miss without failing client node.
     *
     * @return Max missed client heartbeats.
     */
    @IgniteMBeanDescription("Max missed client heartbeats.")
    public int getMaxMissedClientHeartbeats();

    /**
     * Gets thread priority. All threads within SPI will be started with it.
     *
     * @return Thread priority.
     */
    @IgniteMBeanDescription("Threads priority.")
    public int getThreadPriority();

    /**
     * Gets IP finder clean frequency.
     *
     * @return IP finder clean frequency.
     */
    @IgniteMBeanDescription("IP finder clean frequency.")
    public long getIpFinderCleanFrequency();

    /**
     * Gets statistics print frequency.
     *
     * @return Statistics print frequency in milliseconds.
     */
    @IgniteMBeanDescription("Statistics print frequency.")
    public long getStatisticsPrintFrequency();

    /**
     * Gets message worker queue current size.
     *
     * @return Message worker queue current size.
     */
    @IgniteMBeanDescription("Message worker queue current size.")
    public int getMessageWorkerQueueSize();

    /**
     * Gets joined nodes count.
     *
     * @return Nodes joined count.
     */
    @IgniteMBeanDescription("Nodes joined count.")
    public long getNodesJoined();

    /**
     * Gets left nodes count.
     *
     * @return Left nodes count.
     */
    @IgniteMBeanDescription("Nodes left count.")
    public long getNodesLeft();

    /**
     * Gets failed nodes count.
     *
     * @return Failed nodes count.
     */
    @IgniteMBeanDescription("Nodes failed count.")
    public long getNodesFailed();

    /**
     * Gets pending messages registered count.
     *
     * @return Pending messages registered count.
     */
    @IgniteMBeanDescription("Pending messages registered.")
    public long getPendingMessagesRegistered();

    /**
     * Gets pending messages discarded count.
     *
     * @return Pending messages registered count.
     */
    @IgniteMBeanDescription("Pending messages discarded.")
    public long getPendingMessagesDiscarded();

    /**
     * Gets avg message processing time.
     *
     * @return Avg message processing time.
     */
    @IgniteMBeanDescription("Avg message processing time.")
    public long getAvgMessageProcessingTime();

    /**
     * Gets max message processing time.
     *
     * @return Max message processing time.
     */
    @IgniteMBeanDescription("Max message processing time.")
    public long getMaxMessageProcessingTime();

    /**
     * Gets total received messages count.
     *
     * @return Total received messages count.
     */
    @IgniteMBeanDescription("Total received messages count.")
    public int getTotalReceivedMessages();

    /**
     * Gets received messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    @IgniteMBeanDescription("Received messages by type.")
    public Map<String, Integer> getReceivedMessages();

    /**
     * Gets total processed messages count.
     *
     * @return Total processed messages count.
     */
    @IgniteMBeanDescription("Total processed messages count.")
    public int getTotalProcessedMessages();

    /**
     * Gets processed messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    @IgniteMBeanDescription("Received messages by type.")
    public Map<String, Integer> getProcessedMessages();

    /**
     * Gets time local node has been coordinator since.
     *
     * @return Time local node is coordinator since.
     */
    @IgniteMBeanDescription("Local node is coordinator since.")
    public long getCoordinatorSinceTimestamp();

    /**
     * Gets current coordinator.
     *
     * @return Gets current coordinator.
     */
    @IgniteMBeanDescription("Coordinator node ID.")
    @Nullable public UUID getCoordinator();

    /**
     * Gets message acknowledgement timeout.
     *
     * @return Message acknowledgement timeout.
     */
    @IgniteMBeanDescription("Message acknowledgement timeout.")
    public long getAckTimeout();

    /**
     * Gets maximum message acknowledgement timeout.
     *
     * @return Maximum message acknowledgement timeout.
     */
    @IgniteMBeanDescription("Maximum message acknowledgement timeout.")
    public long getMaxAckTimeout();

    /**
     * Gets socket timeout.
     *
     * @return Socket timeout.
     */
    @IgniteMBeanDescription("Socket timeout.")
    public long getSocketTimeout();

    /**
     * Gets join timeout.
     *
     * @return Join timeout.
     */
    @IgniteMBeanDescription("Join timeout.")
    public long getJoinTimeout();

    /**
     * Dumps debug info using configured logger.
     */
    @IgniteMBeanDescription("Dump debug info.")
    public void dumpDebugInfo();
}
