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

/**
 * This interface defines JMX view on {@link org.apache.ignite.Ignition}.
 */
@MXBeanDescription("MBean that provides access to grid life-cycle operations.")
public interface IgnitionMXBean {
    /**
     * Gets state of default grid instance.
     *
     * @return State of default grid instance.
     * @see org.apache.ignite.Ignition#state()
     */
    @MXBeanDescription("State of default grid instance.")
    public String getState();

    /**
     * Gets state for a given grid instance.
     *
     * @param name Name of grid instance.
     * @return State of grid instance with given name.
     * @see org.apache.ignite.Ignition#state(String)
     */
    @MXBeanDescription("Gets state for a given grid instance. Returns state of grid instance with given name.")
    @MXBeanParametersNames(
        "name"
    )
    @MXBeanParametersDescriptions(
        "Name of grid instance."
    )
    public String getState(String name);

    /**
     * Stops default grid instance.
     *
     * @param cancel If {@code true} then all jobs currently executing on
     *      default grid will be cancelled by calling {@link org.apache.ignite.compute.ComputeJob#cancel()}
     *      method. Note that just like with {@link Thread#interrupt()}, it is
     *      up to the actual job to exit from execution.
     * @return {@code true} if default grid instance was indeed stopped,
     *      {@code false} otherwise (if it was not started).
     * @see org.apache.ignite.Ignition#stop(boolean)
     */
    @MXBeanDescription("Stops default grid instance. Return true if default grid instance was " +
        "indeed stopped, false otherwise (if it was not started).")
    @MXBeanParametersNames(
        "cancel"
    )
    @MXBeanParametersDescriptions(
        "If true then all jobs currently executing on default grid will be cancelled."
    )
    public boolean stop(boolean cancel);

    /**
     * Stops named grid. If {@code cancel} flag is set to {@code true} then
     * all jobs currently executing on local node will be interrupted. If
     * grid name is {@code null}, then default no-name grid will be stopped.
     * It does not wait for the tasks to finish their execution.
     *
     * @param name Grid name. If {@code null}, then default no-name grid will
     *      be stopped.
     * @param cancel If {@code true} then all jobs currently will be cancelled
     *      by calling {@link org.apache.ignite.compute.ComputeJob#cancel()} method. Note that just like with
     *      {@link Thread#interrupt()}, it is up to the actual job to exit from
     *      execution. If {@code false}, then jobs currently running will not be
     *      canceled. In either case, grid node will wait for completion of all
     *      jobs running on it before stopping.
     * @return {@code true} if named grid instance was indeed found and stopped,
     *      {@code false} otherwise (the instance with given {@code name} was
     *      not found).
     * @see org.apache.ignite.Ignition#stop(String, boolean)
     */
    @MXBeanDescription("Stops grid by name. Cancels running jobs if cancel is true. Returns true if named " +
        "grid instance was indeed found and stopped, false otherwise.")
    @MXBeanParametersNames(
        {
            "name",
            "cancel"
        })
    @MXBeanParametersDescriptions(
        {
            "Grid instance name to stop.",
            "Whether or not running jobs should be cancelled."
        }
    )
    public boolean stop(String name, boolean cancel);

    /**
     * Stops <b>all</b> started grids. If {@code cancel} flag is set to {@code true} then
     * all jobs currently executing on local node will be interrupted.
     * It does not wait for the tasks to finish their execution.
     * <p>
     * <b>Note:</b> it is usually safer and more appropriate to stop grid instances individually
     * instead of blanket operation. In most cases, the party that started the grid instance
     * should be responsible for stopping it.
     *
     * @param cancel If {@code true} then all jobs currently executing on
     *      all grids will be cancelled by calling {@link org.apache.ignite.compute.ComputeJob#cancel()}
     *      method. Note that just like with {@link Thread#interrupt()}, it is
     *      up to the actual job to exit from execution
     * @see org.apache.ignite.Ignition#stopAll(boolean)
     */
    @MXBeanDescription("Stops all started grids.")
    @MXBeanParametersNames(
        "cancel"
    )
    @MXBeanParametersDescriptions(
        "If true then all jobs currently executing on all grids will be cancelled."
    )
    public void stopAll(boolean cancel);

    /**
     * Restart JVM.
     *
     * @param cancel If {@code true} then all jobs currently executing on
     *      all grids will be cancelled by calling {@link org.apache.ignite.compute.ComputeJob#cancel()}
     *      method. Note that just like with {@link Thread#interrupt()}, it is
     *      up to the actual job to exit from execution
     * @see org.apache.ignite.Ignition#stopAll(boolean)
     */
    @MXBeanDescription("Restart JVM.")
    @MXBeanParametersNames(
        {
            "cancel",
            "wait"
        })
    @MXBeanParametersDescriptions(
        {
            "If true then all jobs currently executing on default grid will be cancelled.",
            "If true then method will wait for all task being executed until they finish their execution."
        }
    )
    public void restart(boolean cancel);
}