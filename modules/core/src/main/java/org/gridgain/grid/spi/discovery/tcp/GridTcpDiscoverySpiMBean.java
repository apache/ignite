/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp;

import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.util.mbean.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Management bean for {@link GridTcpDiscoverySpi}.
 */
public interface GridTcpDiscoverySpiMBean extends GridSpiManagementMBean {
    /**
     * Gets delay between heartbeat messages sent by coordinator.
     *
     * @return Time period in milliseconds.
     */
    @GridMBeanDescription("Heartbeat frequency.")
    public long getHeartbeatFrequency();

    /**
     * Gets current SPI state.
     *
     * @return Current SPI state.
     */
    @GridMBeanDescription("SPI state.")
    public String getSpiState();

    /**
     * Gets {@link GridTcpDiscoveryIpFinder} (string representation).
     *
     * @return IPFinder (string representation).
     */
    @GridMBeanDescription("IP Finder.")
    public String getIpFinderFormatted();

    /**
     * Gets number of connection attempts.
     *
     * @return Number of connection attempts.
     */
    @GridMBeanDescription("Reconnect count.")
    public int getReconnectCount();

    /**
     * Gets network timeout.
     *
     * @return Network timeout.
     */
    @GridMBeanDescription("Network timeout.")
    public long getNetworkTimeout();

    /**
     * Gets local TCP port SPI listens to.
     *
     * @return Local port range.
     */
    @GridMBeanDescription("Local TCP port.")
    public int getLocalPort();

    /**
     * Gets local TCP port range.
     *
     * @return Local port range.
     */
    @GridMBeanDescription("Local TCP port range.")
    public int getLocalPortRange();

    /**
     * Gets max heartbeats count node can miss without initiating status check.
     *
     * @return Max missed heartbeats.
     */
    @GridMBeanDescription("Max missed heartbeats.")
    public int getMaxMissedHeartbeats();

    /**
     * Gets max heartbeats count node can miss without failing client node.
     *
     * @return Max missed client heartbeats.
     */
    @GridMBeanDescription("Max missed client heartbeats.")
    public int getMaxMissedClientHeartbeats();

    /**
     * Gets thread priority. All threads within SPI will be started with it.
     *
     * @return Thread priority.
     */
    @GridMBeanDescription("Threads priority.")
    public int getThreadPriority();

    /**
     * Gets IP finder clean frequency.
     *
     * @return IP finder clean frequency.
     */
    @GridMBeanDescription("IP finder clean frequency.")
    public long getIpFinderCleanFrequency();

    /**
     * Gets statistics print frequency.
     *
     * @return Statistics print frequency in milliseconds.
     */
    @GridMBeanDescription("Statistics print frequency.")
    public long getStatisticsPrintFrequency();

    /**
     * Gets message worker queue current size.
     *
     * @return Message worker queue current size.
     */
    @GridMBeanDescription("Message worker queue current size.")
    public int getMessageWorkerQueueSize();

    /**
     * Gets joined nodes count.
     *
     * @return Nodes joined count.
     */
    @GridMBeanDescription("Nodes joined count.")
    public long getNodesJoined();

    /**
     * Gets left nodes count.
     *
     * @return Left nodes count.
     */
    @GridMBeanDescription("Nodes left count.")
    public long getNodesLeft();

    /**
     * Gets failed nodes count.
     *
     * @return Failed nodes count.
     */
    @GridMBeanDescription("Nodes failed count.")
    public long getNodesFailed();

    /**
     * Gets pending messages registered count.
     *
     * @return Pending messages registered count.
     */
    @GridMBeanDescription("Pending messages registered.")
    public long getPendingMessagesRegistered();

    /**
     * Gets pending messages discarded count.
     *
     * @return Pending messages registered count.
     */
    @GridMBeanDescription("Pending messages discarded.")
    public long getPendingMessagesDiscarded();

    /**
     * Gets avg message processing time.
     *
     * @return Avg message processing time.
     */
    @GridMBeanDescription("Avg message processing time.")
    public long getAvgMessageProcessingTime();

    /**
     * Gets max message processing time.
     *
     * @return Max message processing time.
     */
    @GridMBeanDescription("Max message processing time.")
    public long getMaxMessageProcessingTime();

    /**
     * Gets total received messages count.
     *
     * @return Total received messages count.
     */
    @GridMBeanDescription("Total received messages count.")
    public int getTotalReceivedMessages();

    /**
     * Gets received messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    @GridMBeanDescription("Received messages by type.")
    public Map<String, Integer> getReceivedMessages();

    /**
     * Gets total processed messages count.
     *
     * @return Total processed messages count.
     */
    @GridMBeanDescription("Total processed messages count.")
    public int getTotalProcessedMessages();

    /**
     * Gets processed messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    @GridMBeanDescription("Received messages by type.")
    public Map<String, Integer> getProcessedMessages();

    /**
     * Gets time local node has been coordinator since.
     *
     * @return Time local node is coordinator since.
     */
    @GridMBeanDescription("Local node is coordinator since.")
    public long getCoordinatorSinceTimestamp();

    /**
     * Gets current coordinator.
     *
     * @return Gets current coordinator.
     */
    @GridMBeanDescription("Coordinator node ID.")
    @Nullable public UUID getCoordinator();

    /**
     * Gets message acknowledgement timeout.
     *
     * @return Message acknowledgement timeout.
     */
    @GridMBeanDescription("Message acknowledgement timeout.")
    public long getAckTimeout();

    /**
     * Gets maximum message acknowledgement timeout.
     *
     * @return Maximum message acknowledgement timeout.
     */
    @GridMBeanDescription("Maximum message acknowledgement timeout.")
    public long getMaxAckTimeout();

    /**
     * Gets socket timeout.
     *
     * @return Socket timeout.
     */
    @GridMBeanDescription("Socket timeout.")
    public long getSocketTimeout();

    /**
     * Gets join timeout.
     *
     * @return Join timeout.
     */
    @GridMBeanDescription("Join timeout.")
    public long getJoinTimeout();

    /**
     * Dumps debug info using configured logger.
     */
    @GridMBeanDescription("Dump debug info.")
    public void dumpDebugInfo();
}
