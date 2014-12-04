/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.mbean;

import org.gridgain.grid.compute.*;
import org.gridgain.grid.util.mbean.*;

/**
 * This interface defines JMX view on {@link org.gridgain.grid.GridGain}.
 */
@IgniteMBeanDescription("MBean that provides access to grid life-cycle operations.")
public interface IgnitionMBean {
    /**
     * Gets state of default grid instance.
     *
     * @return State of default grid instance.
     * @see org.gridgain.grid.GridGain#state()
     */
    @IgniteMBeanDescription("State of default grid instance.")
    public String getState();

    /**
     * Gets state for a given grid instance.
     *
     * @param name Name of grid instance.
     * @return State of grid instance with given name.
     * @see org.gridgain.grid.GridGain#state(String)
     */
    @IgniteMBeanDescription("Gets state for a given grid instance. Returns state of grid instance with given name.")
    @IgniteMBeanParametersNames(
        "name"
    )
    @IgniteMBeanParametersDescriptions(
        "Name of grid instance."
    )
    public String getState(String name);

    /**
     * Stops default grid instance.
     *
     * @param cancel If {@code true} then all jobs currently executing on
     *      default grid will be cancelled by calling {@link GridComputeJob#cancel()}
     *      method. Note that just like with {@link Thread#interrupt()}, it is
     *      up to the actual job to exit from execution.
     * @return {@code true} if default grid instance was indeed stopped,
     *      {@code false} otherwise (if it was not started).
     * @see org.gridgain.grid.GridGain#stop(boolean)
     */
    @IgniteMBeanDescription("Stops default grid instance. Return true if default grid instance was " +
        "indeed stopped, false otherwise (if it was not started).")
    @IgniteMBeanParametersNames(
        "cancel"
    )
    @IgniteMBeanParametersDescriptions(
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
     *      by calling {@link GridComputeJob#cancel()} method. Note that just like with
     *      {@link Thread#interrupt()}, it is up to the actual job to exit from
     *      execution. If {@code false}, then jobs currently running will not be
     *      canceled. In either case, grid node will wait for completion of all
     *      jobs running on it before stopping.
     * @return {@code true} if named grid instance was indeed found and stopped,
     *      {@code false} otherwise (the instance with given {@code name} was
     *      not found).
     * @see org.gridgain.grid.GridGain#stop(String, boolean)
     */
    @IgniteMBeanDescription("Stops grid by name. Cancels running jobs if cancel is true. Returns true if named " +
        "grid instance was indeed found and stopped, false otherwise.")
    @IgniteMBeanParametersNames(
        {
            "name",
            "cancel"
        })
    @IgniteMBeanParametersDescriptions(
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
     *      all grids will be cancelled by calling {@link GridComputeJob#cancel()}
     *      method. Note that just like with {@link Thread#interrupt()}, it is
     *      up to the actual job to exit from execution
     * @see org.gridgain.grid.GridGain#stopAll(boolean)
     */
    @IgniteMBeanDescription("Stops all started grids.")
    @IgniteMBeanParametersNames(
        "cancel"
    )
    @IgniteMBeanParametersDescriptions(
        "If true then all jobs currently executing on all grids will be cancelled."
    )
    public void stopAll(boolean cancel);

    /**
     * Restart JVM.
     *
     * @param cancel If {@code true} then all jobs currently executing on
     *      all grids will be cancelled by calling {@link GridComputeJob#cancel()}
     *      method. Note that just like with {@link Thread#interrupt()}, it is
     *      up to the actual job to exit from execution
     * @see org.gridgain.grid.GridGain#stopAll(boolean)
     */
    @IgniteMBeanDescription("Restart JVM.")
    @IgniteMBeanParametersNames(
        {
            "cancel",
            "wait"
        })
    @IgniteMBeanParametersDescriptions(
        {
            "If true then all jobs currently executing on default grid will be cancelled.",
            "If true then method will wait for all task being executed until they finish their execution."
        }
    )
    public void restart(boolean cancel);
}
