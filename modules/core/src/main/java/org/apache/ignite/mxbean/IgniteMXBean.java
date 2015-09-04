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

import java.util.Collection;
import java.util.UUID;
import javax.management.JMException;

/**
 * This interface defines JMX view on kernal.
 */
@MXBeanDescription("MBean that provides access to Kernal information.")
public interface IgniteMXBean {
    /**
     * Gets string presentation of the version.
     *
     * @return String presentation of the version.
     */
    @MXBeanDescription("String presentation of the Ignite version.")
    public String getFullVersion();

    /**
     * Gets copyright statement for Ignite product.
     *
     * @return Copyright statement for Ignite product.
     */
    @MXBeanDescription("Copyright statement for Ignite product.")
    public String getCopyright();

    /**
     * Gets string presentation of the kernal start timestamp.
     *
     * @return String presentation of the kernal start timestamp.
     */
    @MXBeanDescription("String presentation of the kernal start timestamp.")
    public String getStartTimestampFormatted();

    /**
     * Gets string presentation of up-time for the kernal.
     *
     * @return String presentation of up-time for the kernal.
     */
    @MXBeanDescription("String presentation of up-time for the kernal.")
    public String getUpTimeFormatted();

    /**
     * Get start timestamp of the kernal.
     *
     * @return Start timestamp of the kernal.
     */
    @MXBeanDescription("Start timestamp of the kernal.")
    public long getStartTimestamp();

    /**
     * Gets up-time of the kernal.
     *
     * @return Up-time of the kernal.
     */
    @MXBeanDescription("Up-time of the kernal.")
    public long getUpTime();

    /**
     * Gets a collection of formatted user-defined attributes added to this node.
     * <p>
     * Note that grid will add all System properties and environment properties
     * to grid node attributes also. SPIs may also add node attributes that are
     * used for SPI implementation.
     *
     * @return User defined attributes for this node.
     */
    @MXBeanDescription("Collection of formatted user-defined attributes added to this node.")
    public Collection<String> getUserAttributesFormatted();

    /**
     * Gets a formatted instance of logger that is in grid.
     *
     * @return Logger that is used in grid.
     */
    @MXBeanDescription("Formatted instance of logger that is in grid.")
    public String getGridLoggerFormatted();

    /**
     * Gets a formatted instance of fully configured thread pool that is used in grid.
     *
     * @return Thread pool implementation that is used in grid.
     */
    @MXBeanDescription("Formatted instance of fully configured thread pool that is used in grid.")
    public String getExecutorServiceFormatted();

    /**
     * Gets Ignite installation home folder.
     *
     * @return Ignite installation home.
     */
    @MXBeanDescription("Ignite installation home folder.")
    public String getIgniteHome();

    /**
     * Gets a formatted instance of MBean server instance.
     *
     * @return MBean server instance.
     */
    @MXBeanDescription("Formatted instance of MBean server instance.")
    public String getMBeanServerFormatted();

    /**
     * Unique identifier for this node within grid.
     *
     * @return Unique identifier for this node within grid.
     */
    @MXBeanDescription("Unique identifier for this node within grid.")
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
     */
    @MXBeanDescription("Whether or not peer class loading (a.k.a. P2P class loading) is enabled.")
    public boolean isPeerClassLoadingEnabled();

    /**
     * Gets {@code toString()} representation of of lifecycle beans configured
     * with Ignite.
     *
     * @return {@code toString()} representation of all lifecycle beans configured
     *      with Ignite.
     */
    @MXBeanDescription("String representation of lifecycle beans.")
    public Collection<String> getLifecycleBeansFormatted();

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
     */
    @MXBeanDescription("Formatted instance of configured discovery SPI implementation.")
    public String getDiscoverySpiFormatted();

    /**
     * Gets a formatted instance of fully configured SPI communication implementation.
     *
     * @return Grid communication SPI implementation.
     */
    @MXBeanDescription("Formatted instance of fully configured SPI communication implementation.")
    public String getCommunicationSpiFormatted();

    /**
     * Gets a formatted instance of fully configured deployment SPI implementation.
     *
     * @return Grid deployment SPI implementation.
     */
    @MXBeanDescription("Formatted instance of fully configured deployment SPI implementation.")
    public String getDeploymentSpiFormatted();

    /**
     * Gets a formatted instance of configured checkpoint SPI implementation.
     *
     * @return Grid checkpoint SPI implementation.
     */
    @MXBeanDescription("Formatted instance of configured checkpoint SPI implementation.")
    public String getCheckpointSpiFormatted();

    /**
     * Gets a formatted instance of configured collision SPI implementations.
     *
     * @return Grid collision SPI implementations.
     */
    @MXBeanDescription("Formatted instance of configured collision SPI implementations.")
    public String getCollisionSpiFormatted();

    /**
     * Gets a formatted instance of configured swapspace SPI implementations.
     *
     * @return Grid swapspace SPI implementations.
     */
    @MXBeanDescription("Formatted instance of configured swapspace SPI implementations.")
    public String getSwapSpaceSpiFormatted();

    /**
     * Gets a formatted instance of fully configured event SPI implementation.
     *
     * @return Grid event SPI implementation.
     */
    @MXBeanDescription("Formatted instance of fully configured event SPI implementation.")
    public String getEventStorageSpiFormatted();

    /**
     * Gets a formatted instance of fully configured failover SPI implementations.
     *
     * @return Grid failover SPI implementations.
     */
    @MXBeanDescription("Formatted instance of fully configured failover SPI implementations.")
    public String getFailoverSpiFormatted();

    /**
     * Gets a formatted instance of fully configured load balancing SPI implementations.
     *
     * @return Grid load balancing SPI implementations.
     */
    @MXBeanDescription("Formatted instance of fully configured load balancing SPI implementations.")
    public String getLoadBalancingSpiFormatted();

    /**
     * Gets OS information.
     *
     * @return OS information.
     */
    @MXBeanDescription("OS information.")
    public String getOsInformation();

    /**
     * Gets JDK information.
     *
     * @return JDK information.
     */
    @MXBeanDescription("JDK information.")
    public String getJdkInformation();

    /**
     * Gets OS user.
     *
     * @return OS user name.
     */
    @MXBeanDescription("OS user name.")
    public String getOsUser();

    /**
     * Gets VM name.
     *
     * @return VM name.
     */
    @MXBeanDescription("VM name.")
    public String getVmName();

    /**
     * Gets optional kernal instance name. It can be {@code null}.
     *
     * @return Optional kernal instance name.
     */
    @MXBeanDescription("Optional kernal instance name.")
    public String getInstanceName();

    /**
     * Prints errors.
     */
    @MXBeanDescription("Prints last suppressed errors.")
    public void printLastErrors();
}