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
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;

import java.util.*;

/**
 * Management bean for {@link TcpClientDiscoverySpi}.
 */
public interface TcpClientDiscoverySpiMBean extends IgniteSpiManagementMBean {
    /**
     * Gets disconnect check interval.
     *
     * @return Disconnect check interval.
     */
    @IgniteMBeanDescription("Disconnect check interval.")
    public long getDisconnectCheckInterval();

    /**
     * Gets socket timeout.
     *
     * @return Socket timeout.
     */
    @IgniteMBeanDescription("Socket timeout.")
    public long getSocketTimeout();

    /**
     * Gets message acknowledgement timeout.
     *
     * @return Message acknowledgement timeout.
     */
    @IgniteMBeanDescription("Message acknowledgement timeout.")
    public long getAckTimeout();

    /**
     * Gets network timeout.
     *
     * @return Network timeout.
     */
    @IgniteMBeanDescription("Network timeout.")
    public long getNetworkTimeout();

    /**
     * Gets thread priority. All threads within SPI will be started with it.
     *
     * @return Thread priority.
     */
    @IgniteMBeanDescription("Threads priority.")
    public int getThreadPriority();

    /**
     * Gets delay between heartbeat messages sent by coordinator.
     *
     * @return Time period in milliseconds.
     */
    @IgniteMBeanDescription("Heartbeat frequency.")
    public long getHeartbeatFrequency();

    /**
     * Gets {@link GridTcpDiscoveryIpFinder} (string representation).
     *
     * @return IPFinder (string representation).
     */
    @IgniteMBeanDescription("IP Finder.")
    public String getIpFinderFormatted();

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
}
