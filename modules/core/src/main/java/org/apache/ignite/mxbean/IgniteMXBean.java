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

package org.apache.ignite.mxbean;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.management.JMException;

/**
 * This interface defines JMX view on kernal.
 */
@MXBeanDescription("MBean that provides access to Kernal information.")
public interface IgniteMXBean {
    /** */
    public String ATTR_DESC_FULL_VER = "String presentation of the Ignite version.";

    /** */
    public String ATTR_DESC_COPYRIGHT = "Copyright statement for Ignite product.";

    /** */
    public String ATTR_DESC_START_TIMESTAMP_FORMATTED = "String presentation of the kernal start timestamp.";

    /** */
    public String ATTR_DESC_IS_REBALANCE_ENABLED = "Rebalance enabled flag.";

    /** */
    public String ATTR_DESC_UPTIME_FORMATTED = "String presentation of up-time for the kernal.";

    /** */
    public String ATTR_DESC_START_TIMESTAMP = "Start timestamp of the kernal.";

    /** */
    public String ATTR_DESC_UPTIME = "Up-time of the kernal.";

    /** */
    public String ATTR_DESC_LONG_JVM_PAUSES_CNT = "Long JVM pauses count.";

    /** */
    public String ATTR_DESC_LONG_JVM_PAUSES_TOTAL_DURATION = "Long JVM pauses total duration.";

    /** */
    public String ATTR_DESC_LONG_JVM_PAUSE_LAST_EVENTS = "Long JVM pause last events.";

    /** */
    public String ATTR_DESC_USER_ATTRS_FORMATTED = "Collection of formatted user-defined attributes added to this node.";

    /** */
    public String ATTR_DESC_GRID_LOG_FORMATTED ="Formatted instance of logger that is in grid.";

    /** */
    public String ATTR_DESC_EXECUTOR_SRVC_FORMATTED = "Formatted instance of fully configured thread pool that is used in grid.";

    /** */
    public String ATTR_DESC_IGNITE_HOME = "Ignite installation home folder.";

    /** */
    public String ATTR_DESC_MBEAN_SERVER_FORMATTED = "Formatted instance of MBean server instance.";

    /** */
    public String ATTR_DESC_LOC_NODE_ID = "Unique identifier for this node within grid.";

    /** */
    public String ATTR_DESC_IS_PEER_CLS_LOADING_ENABLED = "Whether or not peer class loading (a.k.a. P2P class loading) is enabled.";

    /** */
    public String ATTR_DESC_LIFECYCLE_BEANS_FORMATTED = "String representation of lifecycle beans.";

    /** */
    public String ATTR_DESC_ACTIVE = "Checks Ignite grid is active or is not active.";

    /** */
    public String ATTR_DESC_DISCOVERY_SPI_FORMATTED = "Formatted instance of configured discovery SPI implementation.";

    /** */
    public String ATTR_DESC_COMMUNICATION_SPI_FORMATTED = "Formatted instance of fully configured SPI communication implementation.";

    /** */
    public String ATTR_DESC_DEPLOYMENT_SPI_FORMATTED = "Formatted instance of fully configured deployment SPI implementation.";

    /** */
    public String ATTR_DESC_CHECKPOINT_SPI_FORMATTED = "Formatted instance of configured checkpoint SPI implementation.";

    /** */
    public String ATTR_DESC_COLLISION_SPI_FORMATTED = "Formatted instance of configured collision SPI implementations.";

    /** */
    public String ATTR_DESC_EVT_STORAGE_SPI_FORMATTED = "Formatted instance of fully configured event SPI implementation.";

    /** */
    public String ATTR_DESC_FAILOVER_SPI_FORMATTED = "Formatted instance of fully configured failover SPI implementations.";

    /** */
    public String ATTR_DESC_LOAD_BALANCING_SPI_FORMATTED = "Formatted instance of fully configured load balancing SPI implementations.";

    /** */
    public String ATTR_DESC_OS_INFO = "OS information.";

    /** */
    public String ATTR_DESC_JDK_INFO = "JDK information.";

    /** */
    public String ATTR_DESC_OS_USER = "OS user name.";

    /** */
    public String ATTR_DESC_VM_NAME = "VM name.";

    /** */
    public String ATTR_DESC_INSTANCE_NAME = "Optional kernal instance name.";

    /** */
    public String ATTR_DESC_CUR_COORDINATOR_FORMATTED = "Formatted properties of current coordinator.";

    /** */
    public String ATTR_DESC_IS_NODE_BASELINE = "Baseline node flag.";

    /** */
    public String ATTR_DESC_READ_ONLY_MODE = "Cluster read-only mode status.";

    /** */
    public String ATTR_DESC_READ_ONLY_MODE_DURATION = "Duration of read-only mode enabled on cluster.";

    /**
     * Gets string presentation of the version.
     *
     * @return String presentation of the version.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_FULL_VER)
    public String getFullVersion();

    /**
     * Gets copyright statement for Ignite product.
     *
     * @return Copyright statement for Ignite product.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_COPYRIGHT)
    public String getCopyright();

    /**
     * Gets string presentation of the kernal start timestamp.
     *
     * @return String presentation of the kernal start timestamp.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_START_TIMESTAMP)
    public String getStartTimestampFormatted();

    /**
     * Gets rebalance enabled flag.
     *
     * @return Rebalance enabled flag.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_IS_REBALANCE_ENABLED)
    public boolean isRebalanceEnabled();

    /**
     * Enable or disable cache partition rebalance per node.
     *
     * @param rebalanceEnabled If {@code true} then set rebalance to enabled state.
     */
    @MXBeanParametersDescriptions(
        {
            "Enable cache partitions rebalance on node.",
            "Disable cache partitions rebalance on node."
        }
    )
    public void rebalanceEnabled(boolean rebalanceEnabled);

    /**
     * Gets string presentation of up-time for the kernal.
     *
     * @return String presentation of up-time for the kernal.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_UPTIME_FORMATTED)
    public String getUpTimeFormatted();

    /**
     * Get start timestamp of the kernal.
     *
     * @return Start timestamp of the kernal.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_START_TIMESTAMP)
    public long getStartTimestamp();

    /**
     * Gets up-time of the kernal.
     *
     * @return Up-time of the kernal.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_UPTIME)
    public long getUpTime();

    /**
     * Gets long JVM pauses count.
     *
     * @return Long JVM pauses count.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_LONG_JVM_PAUSES_CNT)
    public long getLongJVMPausesCount();

    /**
     * Gets long JVM pauses total duration.
     *
     * @return Long JVM pauses total duration.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_LONG_JVM_PAUSES_TOTAL_DURATION)
    public long getLongJVMPausesTotalDuration();

    /**
     * Gets long JVM pause last events.
     *
     * @return Long JVM pause last events.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_LONG_JVM_PAUSE_LAST_EVENTS)
    public Map<Long, Long> getLongJVMPauseLastEvents();

    /**
     * Gets a list of formatted user-defined attributes added to this node.
     * <p>
     * Note that grid will add all System properties and environment properties
     * to grid node attributes also. SPIs may also add node attributes that are
     * used for SPI implementation.
     *
     * @return User defined attributes for this node.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_USER_ATTRS_FORMATTED)
    public List<String> getUserAttributesFormatted();

    /**
     * Gets a formatted instance of logger that is in grid.
     *
     * @return Logger that is used in grid.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_GRID_LOG_FORMATTED)
    public String getGridLoggerFormatted();

    /**
     * Gets a formatted instance of fully configured thread pool that is used in grid.
     *
     * @return Thread pool implementation that is used in grid.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_EXECUTOR_SRVC_FORMATTED)
    public String getExecutorServiceFormatted();

    /**
     * Gets Ignite installation home folder.
     *
     * @return Ignite installation home.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_IGNITE_HOME)
    public String getIgniteHome();

    /**
     * Gets a formatted instance of MBean server instance.
     *
     * @return MBean server instance.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_MBEAN_SERVER_FORMATTED)
    public String getMBeanServerFormatted();

    /**
     * Unique identifier for this node within grid.
     *
     * @return Unique identifier for this node within grid.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_LOC_NODE_ID)
    public UUID getLocalNodeId();

    /**
     * Returns {@code true} if peer class loading is enabled, {@code false}
     * otherwise. Default value is {@code true}.
     * <p>
     * When peer class loading is enabled and task is not deployed on local node,
     * local node will try to load classes from the node that initiated task
     * execution. This way, a task can be physically deployed only on one node
     * and then internally penetrate to all other nodes.
     *
     * @return {@code true} if peer class loading is enabled, {@code false}
     *      otherwise.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_IS_PEER_CLS_LOADING_ENABLED)
    public boolean isPeerClassLoadingEnabled();

    /**
     * Gets {@code toString()} representation of of lifecycle beans configured
     * with Ignite.
     *
     * @return {@code toString()} representation of all lifecycle beans configured
     *      with Ignite.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_LIFECYCLE_BEANS_FORMATTED)
    public List<String> getLifecycleBeansFormatted();

    /**
     * This method allows manually remove the checkpoint with given {@code key}.
     *
     * @param key Checkpoint key.
     * @return {@code true} if specified checkpoint was indeed removed, {@code false}
     *      otherwise.
     */
    @MXBeanDescription("This method allows manually remove the checkpoint with given key. Return true " +
        "if specified checkpoint was indeed removed, false otherwise.")
    @MXBeanParametersNames(
        "key"
    )
    @MXBeanParametersDescriptions(
        "Checkpoint key to remove."
    )
    public boolean removeCheckpoint(String key);

    /**
     * Pings node with given node ID to see whether it is alive.
     *
     * @param nodeId String presentation of node ID. See {@link UUID#fromString(String)} for
     *      details on string formatting.
     * @return Whether or not node is alive.
     */
    @MXBeanDescription("Pings node with given node ID to see whether it is alive. " +
        "Returns whether or not node is alive.")
    @MXBeanParametersNames(
        "nodeId"
    )
    @MXBeanParametersDescriptions(
        "String presentation of node ID. See java.util.UUID class for details."
    )
    public boolean pingNode(String nodeId);

    /**
     * @param active Activate/DeActivate flag.
     */
    @MXBeanDescription(
        "Execute activate or deactivate process."
    )
    @MXBeanParametersNames(
        "active"
    )
    public void active(boolean active);

    /**
     * Checks if Ignite grid is active. If Ignite grid is not active return {@code False}.
     *
     * @return {@code True} if grid is active. {@code False} If grid is not active.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_ACTIVE)
    public boolean active();

    /**
     * Makes the best attempt to undeploy a task from the whole grid. Note that this
     * method returns immediately and does not wait until the task will actually be
     * undeployed on every node.
     * <p>
     * Note that Ignite maintains internal versions for grid tasks in case of redeployment.
     * This method will attempt to undeploy all versions on the grid task with
     * given name.
     *
     * @param taskName Name of the task to undeploy. If task class has {@link org.apache.ignite.compute.ComputeTaskName} annotation,
     *      then task was deployed under a name specified within annotation. Otherwise, full
     *      class name should be used as task's name.
     * @throws JMException Thrown if undeploy failed.
     */
    @MXBeanDescription("Makes the best attempt to undeploy a task from the whole grid.")
    @MXBeanParametersNames(
        "taskName"
    )
    @MXBeanParametersDescriptions(
        "Name of the task to undeploy."
    )
    public void undeployTaskFromGrid(String taskName) throws JMException;

    /**
     * A shortcut method that executes given task assuming single {@code java.lang.String} argument
     * and {@code java.lang.String} return type.
     *
     * @param taskName Name of the task to execute.
     * @param arg Single task execution argument (can be {@code null}).
     * @return Task return value (assumed of {@code java.lang.String} type).
     * @throws JMException Thrown in case when execution failed.
     */
    @MXBeanDescription("A shortcut method that executes given task assuming single " +
        "String argument and String return type. Returns Task return value (assumed of String type).")
    @MXBeanParametersNames(
        {
            "taskName",
            "arg"
        }
    )
    @MXBeanParametersDescriptions(
        {
            "Name of the task to execute.",
            "Single task execution argument (can be null)."
        }
    )
    public String executeTask(String taskName, String arg) throws JMException;

    /**
     * Pings node with given host name to see if it is alive.
     *
     * @param host Host name or IP address of the node to ping.
     * @return Whether or not node is alive.
     */
    @MXBeanDescription("Pings node with given host name to see if it is alive. " +
        "Returns whether or not node is alive.")
    @MXBeanParametersNames(
        "host"
    )
    @MXBeanParametersDescriptions(
        "Host name or IP address of the node to ping."
    )
    public boolean pingNodeByAddress(String host);

    /**
     * Gets a formatted instance of configured discovery SPI implementation.
     *
     * @return Grid discovery SPI implementation.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_DISCOVERY_SPI_FORMATTED)
    public String getDiscoverySpiFormatted();

    /**
     * Gets a formatted instance of fully configured SPI communication implementation.
     *
     * @return Grid communication SPI implementation.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_COMMUNICATION_SPI_FORMATTED)
    public String getCommunicationSpiFormatted();

    /**
     * Gets a formatted instance of fully configured deployment SPI implementation.
     *
     * @return Grid deployment SPI implementation.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_DEPLOYMENT_SPI_FORMATTED)
    public String getDeploymentSpiFormatted();

    /**
     * Gets a formatted instance of configured checkpoint SPI implementation.
     *
     * @return Grid checkpoint SPI implementation.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_CHECKPOINT_SPI_FORMATTED)
    public String getCheckpointSpiFormatted();

    /**
     * Gets a formatted instance of configured collision SPI implementations.
     *
     * @return Grid collision SPI implementations.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_COLLISION_SPI_FORMATTED)
    public String getCollisionSpiFormatted();

    /**
     * Gets a formatted instance of fully configured event SPI implementation.
     *
     * @return Grid event SPI implementation.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_EVT_STORAGE_SPI_FORMATTED)
    public String getEventStorageSpiFormatted();

    /**
     * Gets a formatted instance of fully configured failover SPI implementations.
     *
     * @return Grid failover SPI implementations.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_FAILOVER_SPI_FORMATTED)
    public String getFailoverSpiFormatted();

    /**
     * Gets a formatted instance of fully configured load balancing SPI implementations.
     *
     * @return Grid load balancing SPI implementations.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_LOAD_BALANCING_SPI_FORMATTED)
    public String getLoadBalancingSpiFormatted();

    /**
     * Gets OS information.
     *
     * @return OS information.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_OS_INFO)
    public String getOsInformation();

    /**
     * Gets JDK information.
     *
     * @return JDK information.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_JDK_INFO)
    public String getJdkInformation();

    /**
     * Gets OS user.
     *
     * @return OS user name.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_OS_USER)
    public String getOsUser();

    /**
     * Gets VM name.
     *
     * @return VM name.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_VM_NAME)
    public String getVmName();

    /**
     * Gets optional kernal instance name. It can be {@code null}.
     *
     * @return Optional kernal instance name.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_INSTANCE_NAME)
    public String getInstanceName();

    /**
     * Prints errors.
     */
    @MXBeanDescription("Prints last suppressed errors.")
    public void printLastErrors();

    /**
     * Dumps debug information for the current node.
     */
    @MXBeanDescription("Dumps debug information for the current node.")
    public void dumpDebugInfo();

    /**
     * Gets a formatted properties of current coordinator.
     * @return String representation of current coordinator node.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_CUR_COORDINATOR_FORMATTED)
    public String getCurrentCoordinatorFormatted();

    /**
     * Gets a flag whether local node is in baseline. Returns false if baseline topology is not established.
     *
     * @return Return a baseline flag.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_IS_NODE_BASELINE)
    public boolean isNodeInBaseline();

    /**
     * Runs IO latency test against all remote server nodes in cluster.
     *
     * @param warmup Warmup duration in milliseconds.
     * @param duration Test duration in milliseconds.
     * @param threads Thread count.
     * @param maxLatency Max latency in nanoseconds.
     * @param rangesCnt Ranges count in resulting histogram.
     * @param payLoadSize Payload size in bytes.
     * @param procFromNioThread {@code True} to process requests in NIO threads.
     */
    @MXBeanDescription("Runs IO latency test against all remote server nodes in cluster.")
    @MXBeanParametersNames(
        {
            "warmup",
            "duration",
            "threads",
            "maxLatency",
            "rangesCnt",
            "payLoadSize",
            "procFromNioThread"
        }
    )
    @MXBeanParametersDescriptions(
        {
            "Warmup duration (millis).",
            "Test duration (millis).",
            "Threads count.",
            "Maximum latency expected (nanos).",
            "Ranges count for histogram.",
            "Payload size (bytes).",
            "Process requests in NIO-threads flag."
        }
    )
    void runIoTest(
        long warmup,
        long duration,
        int threads,
        long maxLatency,
        int rangesCnt,
        int payLoadSize,
        boolean procFromNioThread
    );

    /**
     * Clears node local map.
     */
    @MXBeanDescription("Clears local node map.")
    void clearNodeLocalMap();

    /**
     * Resets metrics for of a given registry.
     *
     * @param registry Metrics registry name.
     */
    @MXBeanDescription("Resets metrics of a given registry.")
    @MXBeanParametersNames("registry")
    @MXBeanParametersDescriptions("Metrics registry.")
    public void resetMetrics(String registry);

    /**
     * Gets cluster read-only mode status.
     *
     * @return {@code true} if cluster active and read-only mode enabled, and {@code false} otherwise.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_READ_ONLY_MODE)
    boolean readOnlyMode();

    /**
     * Enable or disable cluster read-only mode. If {@code readOnly} flag is {@code true} read-only mode will be
     * enabled. If {@code readOnly} flag is {@code false} read-only mode will be disabled.
     *
     * @param readOnly enable/disable cluster read-only mode flag.
     */
    @MXBeanDescription("Enable or disable cluster read-only mode.")
    @MXBeanParametersNames("readOnly")
    @MXBeanParametersDescriptions("True - enable read-only mode, false - disable read-only mode.")
    void readOnlyMode(boolean readOnly);

    /**
     * Gets duration of read-only mode enabled on cluster.
     *
     * @return {@code 0} if cluster read-only mode disabled, and time in milliseconds since enabling cluster read-only
     * mode.
     * @deprecated Use metric API.
     */
    @Deprecated
    @MXBeanDescription(ATTR_DESC_READ_ONLY_MODE_DURATION)
    long getReadOnlyModeDuration();
}
