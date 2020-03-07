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
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.processors.metric.GridMetricManager;

/**
 * This interface defines JMX view on kernal.
 */
@MXBeanDescription("MBean that provides access to Kernal information.")
public interface IgniteMXBean {
    /** */
    public String FULL_VER_DESC = "String presentation of the Ignite version.";

    /** */
    public String COPYRIGHT_DESC = "Copyright statement for Ignite product.";

    /** */
    public String START_TIMESTAMP_FORMATTED_DESC = "String presentation of the kernal start timestamp.";

    /** */
    public String IS_REBALANCE_ENABLED_DESC = "Rebalance enabled flag.";

    /** */
    public String UPTIME_FORMATTED_DESC = "String presentation of up-time for the kernal.";

    /** */
    public String START_TIMESTAMP_DESC = "Start timestamp of the kernal.";

    /** */
    public String UPTIME_DESC = "Up-time of the kernal.";

    /** */
    public String LONG_JVM_PAUSES_CNT_DESC = "Long JVM pauses count.";

    /** */
    public String LONG_JVM_PAUSES_TOTAL_DURATION_DESC = "Long JVM pauses total duration.";

    /** */
    public String LONG_JVM_PAUSE_LAST_EVENTS_DESC = "Long JVM pause last events.";

    /** */
    public String USER_ATTRS_FORMATTED_DESC = "Collection of formatted user-defined attributes added to this node.";

    /** */
    public String GRID_LOG_FORMATTED_DESC ="Formatted instance of logger that is in grid.";

    /** */
    public String EXECUTOR_SRVC_FORMATTED_DESC = "Formatted instance of fully configured thread pool" +
        " that is used in grid.";

    /** */
    public String IGNITE_HOME_DESC = "Ignite installation home folder.";

    /** */
    public String MBEAN_SERVER_FORMATTED_DESC = "Formatted instance of MBean server instance.";

    /** */
    public String LOC_NODE_ID_DESC = "Unique identifier for this node within grid.";

    /** */
    public String IS_PEER_CLS_LOADING_ENABLED_DESC = "Whether or not peer class loading" +
        " (a.k.a. P2P class loading) is enabled.";

    /** */
    public String LIFECYCLE_BEANS_FORMATTED_DESC = "String representation of lifecycle beans.";

    /** */
    public String ACTIVE_DESC = "Checks Ignite grid is active or is not active.";

    /** */
    public String DISCOVERY_SPI_FORMATTED_DESC = "Formatted instance of configured discovery SPI implementation.";

    /** */
    public String COMMUNICATION_SPI_FORMATTED_DESC = "Formatted instance of fully configured SPI communication" +
        " implementation.";

    /** */
    public String DEPLOYMENT_SPI_FORMATTED_DESC = "Formatted instance of fully configured deployment SPI" +
        " implementation.";

    /** */
    public String CHECKPOINT_SPI_FORMATTED_DESC = "Formatted instance of configured checkpoint SPI implementation.";

    /** */
    public String COLLISION_SPI_FORMATTED_DESC = "Formatted instance of configured collision SPI implementations.";

    /** */
    public String EVT_STORAGE_SPI_FORMATTED_DESC = "Formatted instance of fully configured event SPI implementation.";

    /** */
    public String FAILOVER_SPI_FORMATTED_DESC = "Formatted instance of fully configured failover SPI implementations.";

    /** */
    public String LOAD_BALANCING_SPI_FORMATTED_DESC = "Formatted instance of fully configured load balancing SPI" +
        " implementations.";

    /** */
    public String OS_INFO_DESC = "OS information.";

    /** */
    public String JDK_INFO_DESC = "JDK information.";

    /** */
    public String OS_USER_DESC = "OS user name.";

    /** */
    public String VM_NAME_DESC = "VM name.";

    /** */
    public String INSTANCE_NAME_DESC = "Optional kernal instance name.";

    /** */
    public String CUR_COORDINATOR_FORMATTED_DESC = "Formatted properties of current coordinator.";

    /** */
    public String IS_NODE_BASELINE_DESC = "Baseline node flag.";

    /** */
    public static final String LAST_CLUSTER_STATE_CHANGE_TIME_DESC = "Unix time of last cluster state change operation.";

    /** */
    public static final String CLUSTER_STATE_DESC = "Checks cluster state.";

    /** */
    public String READ_ONLY_MODE_DESC = "Cluster read-only mode status.";

    /** */
    public String READ_ONLY_MODE_DURATION_DESC = "Duration of read-only mode enabled on cluster.";

    /**
     * Gets string presentation of the version.
     *
     * @return String presentation of the version.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(FULL_VER_DESC)
    public String getFullVersion();

    /**
     * Gets copyright statement for Ignite product.
     *
     * @return Copyright statement for Ignite product.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(COPYRIGHT_DESC)
    public String getCopyright();

    /**
     * Gets string presentation of the kernal start timestamp.
     *
     * @return String presentation of the kernal start timestamp.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(START_TIMESTAMP_DESC)
    public String getStartTimestampFormatted();

    /**
     * Gets rebalance enabled flag.
     *
     * @return Rebalance enabled flag.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(IS_REBALANCE_ENABLED_DESC)
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
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(UPTIME_FORMATTED_DESC)
    public String getUpTimeFormatted();

    /**
     * Get start timestamp of the kernal.
     *
     * @return Start timestamp of the kernal.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(START_TIMESTAMP_DESC)
    public long getStartTimestamp();

    /**
     * Gets up-time of the kernal.
     *
     * @return Up-time of the kernal.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(UPTIME_DESC)
    public long getUpTime();

    /**
     * Gets long JVM pauses count.
     *
     * @return Long JVM pauses count.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(LONG_JVM_PAUSES_CNT_DESC)
    public long getLongJVMPausesCount();

    /**
     * Gets long JVM pauses total duration.
     *
     * @return Long JVM pauses total duration.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(LONG_JVM_PAUSES_TOTAL_DURATION_DESC)
    public long getLongJVMPausesTotalDuration();

    /**
     * Gets long JVM pause last events.
     *
     * @return Long JVM pause last events.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(LONG_JVM_PAUSE_LAST_EVENTS_DESC)
    public Map<Long, Long> getLongJVMPauseLastEvents();

    /**
     * Gets a list of formatted user-defined attributes added to this node.
     * <p>
     * Note that grid will add all System properties and environment properties
     * to grid node attributes also. SPIs may also add node attributes that are
     * used for SPI implementation.
     *
     * @return User defined attributes for this node.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(USER_ATTRS_FORMATTED_DESC)
    public List<String> getUserAttributesFormatted();

    /**
     * Gets a formatted instance of logger that is in grid.
     *
     * @return Logger that is used in grid.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(GRID_LOG_FORMATTED_DESC)
    public String getGridLoggerFormatted();

    /**
     * Gets a formatted instance of fully configured thread pool that is used in grid.
     *
     * @return Thread pool implementation that is used in grid.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(EXECUTOR_SRVC_FORMATTED_DESC)
    public String getExecutorServiceFormatted();

    /**
     * Gets Ignite installation home folder.
     *
     * @return Ignite installation home.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(IGNITE_HOME_DESC)
    public String getIgniteHome();

    /**
     * Gets a formatted instance of MBean server instance.
     *
     * @return MBean server instance.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(MBEAN_SERVER_FORMATTED_DESC)
    public String getMBeanServerFormatted();

    /**
     * Unique identifier for this node within grid.
     *
     * @return Unique identifier for this node within grid.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(LOC_NODE_ID_DESC)
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
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(IS_PEER_CLS_LOADING_ENABLED_DESC)
    public boolean isPeerClassLoadingEnabled();

    /**
     * Gets {@code toString()} representation of of lifecycle beans configured
     * with Ignite.
     *
     * @return {@code toString()} representation of all lifecycle beans configured
     *      with Ignite.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(LIFECYCLE_BEANS_FORMATTED_DESC)
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
    public boolean removeCheckpoint(
        @MXBeanParameter(name = "key", description = "Checkpoint key to remove.") String key
    );

    /**
     * Pings node with given node ID to see whether it is alive.
     *
     * @param nodeId String presentation of node ID. See {@link UUID#fromString(String)} for
     *      details on string formatting.
     * @return Whether or not node is alive.
     */
    @MXBeanDescription("Pings node with given node ID to see whether it is alive. " +
        "Returns whether or not node is alive.")
    public boolean pingNode(
        @MXBeanParameter(name = "nodeId",
            description = "String presentation of node ID. See java.util.UUID class for details.") String nodeId
    );

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
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(ACTIVE_DESC)
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
    public void undeployTaskFromGrid(
        @MXBeanParameter(name = "taskName", description = "Name of the task to undeploy.") String taskName
    ) throws JMException;

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
    public String executeTask(
        @MXBeanParameter(name = "taskName", description = "Name of the task to execute.") String taskName,
        @MXBeanParameter(name = "arg", description = "Single task execution argument (can be null).") String arg
    ) throws JMException;

    /**
     * Pings node with given host name to see if it is alive.
     *
     * @param host Host name or IP address of the node to ping.
     * @return Whether or not node is alive.
     */
    @MXBeanDescription("Pings node with given host name to see if it is alive. " +
        "Returns whether or not node is alive.")
    public boolean pingNodeByAddress(
        @MXBeanParameter(name = "host", description = "Host name or IP address of the node to ping.") String host
    );

    /**
     * Gets a formatted instance of configured discovery SPI implementation.
     *
     * @return Grid discovery SPI implementation.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(DISCOVERY_SPI_FORMATTED_DESC)
    public String getDiscoverySpiFormatted();

    /**
     * Gets a formatted instance of fully configured SPI communication implementation.
     *
     * @return Grid communication SPI implementation.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(COMMUNICATION_SPI_FORMATTED_DESC)
    public String getCommunicationSpiFormatted();

    /**
     * Gets a formatted instance of fully configured deployment SPI implementation.
     *
     * @return Grid deployment SPI implementation.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(DEPLOYMENT_SPI_FORMATTED_DESC)
    public String getDeploymentSpiFormatted();

    /**
     * Gets a formatted instance of configured checkpoint SPI implementation.
     *
     * @return Grid checkpoint SPI implementation.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(CHECKPOINT_SPI_FORMATTED_DESC)
    public String getCheckpointSpiFormatted();

    /**
     * Gets a formatted instance of configured collision SPI implementations.
     *
     * @return Grid collision SPI implementations.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(COLLISION_SPI_FORMATTED_DESC)
    public String getCollisionSpiFormatted();

    /**
     * Gets a formatted instance of fully configured event SPI implementation.
     *
     * @return Grid event SPI implementation.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(EVT_STORAGE_SPI_FORMATTED_DESC)
    public String getEventStorageSpiFormatted();

    /**
     * Gets a formatted instance of fully configured failover SPI implementations.
     *
     * @return Grid failover SPI implementations.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(FAILOVER_SPI_FORMATTED_DESC)
    public String getFailoverSpiFormatted();

    /**
     * Gets a formatted instance of fully configured load balancing SPI implementations.
     *
     * @return Grid load balancing SPI implementations.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(LOAD_BALANCING_SPI_FORMATTED_DESC)
    public String getLoadBalancingSpiFormatted();

    /**
     * Gets OS information.
     *
     * @return OS information.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(OS_INFO_DESC)
    public String getOsInformation();

    /**
     * Gets JDK information.
     *
     * @return JDK information.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(JDK_INFO_DESC)
    public String getJdkInformation();

    /**
     * Gets OS user.
     *
     * @return OS user name.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(OS_USER_DESC)
    public String getOsUser();

    /**
     * Gets VM name.
     *
     * @return VM name.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(VM_NAME_DESC)
    public String getVmName();

    /**
     * Gets optional kernal instance name. It can be {@code null}.
     *
     * @return Optional kernal instance name.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(INSTANCE_NAME_DESC)
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
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(CUR_COORDINATOR_FORMATTED_DESC)
    public String getCurrentCoordinatorFormatted();

    /**
     * Gets a flag whether local node is in baseline. Returns false if baseline topology is not established.
     *
     * @return Return a baseline flag.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(IS_NODE_BASELINE_DESC)
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
    void runIoTest(
        @MXBeanParameter(name = "warmup", description = "Warmup duration (millis).")
            long warmup,
        @MXBeanParameter(name = "duration", description = "Test duration (millis).")
            long duration,
        @MXBeanParameter(name = "threads", description = "Threads count.")
            int threads,
        @MXBeanParameter(name = "maxLatency", description = "Maximum latency expected (nanos).")
            long maxLatency,
        @MXBeanParameter(name = "rangesCnt", description = "Ranges count for histogram.")
            int rangesCnt,
        @MXBeanParameter(name = "payLoadSize", description = "Payload size (bytes).")
            int payLoadSize,
        @MXBeanParameter(name = "procFromNioThread", description = "Process requests in NIO-threads flag.")
            boolean procFromNioThread
    );

    /**
     * Clears node local map.
     */
    @MXBeanDescription("Clears local node map.")
    void clearNodeLocalMap();

    /**
     * Checks cluster state.
     *
     * @return String representation of current cluster state.
     * See {@link ClusterState}.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(CLUSTER_STATE_DESC)
    public String clusterState();

    /**
     * Changes current cluster state.
     *
     * @param state String representation of new cluster state.
     * See {@link ClusterState}
     */
    @MXBeanDescription("Changes current cluster state.")
    public void clusterState(
        @MXBeanParameter(name = "state", description = "New cluster state.") String state
    );

    /**
     * Gets last cluster state change operation.
     *
     * @return Unix time of last cluster state change operation.
     * @deprecated Use {@link GridMetricManager} instead.
     */
    @Deprecated
    @MXBeanDescription(LAST_CLUSTER_STATE_CHANGE_TIME_DESC)
    public long lastClusterStateChangeTime();
}
