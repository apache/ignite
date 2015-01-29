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

package org.apache.ignite;

import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.lifecycle.*;
import org.jetbrains.annotations.*;

import java.net.*;
import java.util.*;

/**
 * This class defines a factory for the main GridGain API. It controls Grid life cycle
 * and allows listening for grid events.
 * <h1 class="header">Grid Loaders</h1>
 * Although user can apply grid factory directly to start and stop grid, grid is
 * often started and stopped by grid loaders. Grid loaders can be found in
 * {@link org.apache.ignite.startup} package, for example:
 * <ul>
 * <li>{@link org.apache.ignite.startup.cmdline.CommandLineStartup}</li>
 * <li>{@ignitelink org.apache.ignite.startup.servlet.GridServletStartup}</li>
 * </ul>
 * <h1 class="header">Examples</h1>
 * Use {@link #start()} method to start grid with default configuration. You can also use
 * {@link org.apache.ignite.configuration.IgniteConfiguration} to override some default configuration. Below is an
 * example on how to start grid with custom configuration for <strong>URI deployment</strong>.
 * <pre name="code" class="java">
 * GridConfiguration cfg = new GridConfiguration();
 *
 * GridUriDeployment deploySpi = new GridUriDeployment();
 *
 * deploySpi.setUriList(Collections.singletonList("classes://tmp/output/classes"));
 *
 * cfg.setDeploymentSpi(deploySpi);
 *
 * GridGain.start(cfg);
 * </pre>
 * Here is how a grid instance can be configured from Spring XML configuration file. The
 * example below configures a grid instance with additional user attributes
 * (see {@link org.apache.ignite.cluster.ClusterNode#attributes()}) and specifies a grid name:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.cfg" class="org.gridgain.grid.GridConfiguration"&gt;
 *     ...
 *     &lt;property name="gridName" value="grid"/&gt;
 *     &lt;property name="userAttributes"&gt;
 *         &lt;map&gt;
 *             &lt;entry key="group" value="worker"/&gt;
 *         &lt;/map&gt;
 *     &lt;/property&gt;
 *     ...
 * &lt;/bean&gt;
 * </pre>
 * A grid instance with Spring configuration above can be started as following. Note that
 * you do not need to pass path to Spring XML file if you are using
 * {@code GRIDGAIN_HOME/config/default-config.xml}. Also note, that the path can be
 * absolute or relative to GRIDGAIN_HOME.
 * <pre name="code" class="java">
 * ...
 * GridGain.start("/path/to/spring/xml/file.xml");
 * ...
 * </pre>
 * You can also instantiate grid directly from Spring without using {@code GridGain}.
 * For more information refer to {@ignitelink org.apache.ignite.IgniteSpringBean} documentation.
 */
public class Ignition {
    /**
     * This is restart code that can be used by external tools, like Shell scripts,
     * to auto-restart the GridGain JVM process. Note that there is no standard way
     * for a JVM to restart itself from Java application and therefore we rely on
     * external tools to provide that capability.
     * <p>
     * Note that standard <tt>ggstart.{sh|bat}</tt> scripts support restarting when
     * JVM process exits with this code.
     */
    public static final int RESTART_EXIT_CODE = 250;

    /**
     * This is kill code that can be used by external tools, like Shell scripts,
     * to auto-stop the GridGain JVM process without restarting.
     */
    public static final int KILL_EXIT_CODE = 130;

    /**
     * Default License file name. This file will be tried if no
     * license provided in configuration.
     */
    public static final String DFLT_LIC_FILE_NAME = "gridgain-license.xml";

    /**
     * Enforces singleton.
     */
    protected Ignition() {
        // No-op.
    }

    /**
     * Sets daemon flag.
     * <p>
     * If daemon flag is set then all grid instances created by the factory will be
     * daemon, i.e. the local node for these instances will be a daemon node. Note that
     * if daemon flag is set - it will override the same settings in {@link org.apache.ignite.configuration.IgniteConfiguration#isDaemon()}.
     * Note that you can set on and off daemon flag at will.
     *
     * @param daemon Daemon flag to set.
     */
    public static void setDaemon(boolean daemon) {
        GridGainEx.setDaemon(daemon);
    }

    /**
     * Gets daemon flag.
     * <p>
     * If daemon flag it set then all grid instances created by the factory will be
     * daemon, i.e. the local node for these instances will be a daemon node. Note that
     * if daemon flag is set - it will override the same settings in {@link org.apache.ignite.configuration.IgniteConfiguration#isDaemon()}.
     * Note that you can set on and off daemon flag at will.
     *
     * @return Daemon flag.
     */
    public static boolean isDaemon() {
        return GridGainEx.isDaemon();
    }

    /**
     * Gets state of grid default grid.
     *
     * @return Default grid state.
     */
    public static IgniteState state() {
        return GridGainEx.state();
    }

    /**
     * Gets states of named grid. If name is {@code null}, then state of
     * default no-name grid is returned.
     *
     * @param name Grid name. If name is {@code null}, then state of
     *      default no-name grid is returned.
     * @return Grid state.
     */
    public static IgniteState state(@Nullable String name) {
        return GridGainEx.state(name);
    }

    /**
     * Stops default grid. This method is identical to {@code G.stop(null, cancel)} apply.
     * Note that method does not wait for all tasks to be completed.
     *
     * @param cancel If {@code true} then all jobs currently executing on
     *      default grid will be cancelled by calling {@link org.apache.ignite.compute.ComputeJob#cancel()}
     *      method. Note that just like with {@link Thread#interrupt()}, it is
     *      up to the actual job to exit from execution
     * @return {@code true} if default grid instance was indeed stopped,
     *      {@code false} otherwise (if it was not started).
     */
    public static boolean stop(boolean cancel) {
        return GridGainEx.stop(cancel);
    }

    /**
     * Stops named grid. If {@code cancel} flag is set to {@code true} then
     * all jobs currently executing on local node will be interrupted. If
     * grid name is {@code null}, then default no-name grid will be stopped.
     * If wait parameter is set to {@code true} then grid will wait for all
     * tasks to be finished.
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
     */
    public static boolean stop(@Nullable String name, boolean cancel) {
        return GridGainEx.stop(name, cancel);
    }

    /**
     * Stops <b>all</b> started grids in current JVM. If {@code cancel} flag is set to {@code true} then
     * all jobs currently executing on local node will be interrupted.
     * If wait parameter is set to {@code true} then grid will wait for all
     * tasks to be finished.
     * <p>
     * <b>Note:</b> it is usually safer and more appropriate to stop grid instances individually
     * instead of blanket operation. In most cases, the party that started the grid instance
     * should be responsible for stopping it.
     *
     * @param cancel If {@code true} then all jobs currently executing on
     *      all grids will be cancelled by calling {@link org.apache.ignite.compute.ComputeJob#cancel()}
     *      method. Note that just like with {@link Thread#interrupt()}, it is
     *      up to the actual job to exit from execution
     */
    public static void stopAll(boolean cancel) {
        GridGainEx.stopAll(cancel);
    }

    /**
     * Restarts <b>all</b> started grids. If {@code cancel} flag is set to {@code true} then
     * all jobs currently executing on the local node will be interrupted.
     * If {@code wait} parameter is set to {@code true} then grid will wait for all
     * tasks to be finished.
     * <p>
     * <b>Note:</b> it is usually safer and more appropriate to stop grid instances individually
     * instead of blanket operation. In most cases, the party that started the grid instance
     * should be responsible for stopping it.
     * <p>
     * Note also that restarting functionality only works with the tools that specifically
     * support GridGain's protocol for restarting. Currently only standard <tt>ggstart.{sh|bat}</tt>
     * scripts support restarting of JVM GridGain's process.
     *
     * @param cancel If {@code true} then all jobs currently executing on
     *      all grids will be cancelled by calling {@link org.apache.ignite.compute.ComputeJob#cancel()}
     *      method. Note that just like with {@link Thread#interrupt()}, it is
     *      up to the actual job to exit from execution.
     * @see #RESTART_EXIT_CODE
     */
    public static void restart(boolean cancel) {
        GridGainEx.restart(cancel);
    }

    /**
     * Stops <b>all</b> started grids. If {@code cancel} flag is set to {@code true} then
     * all jobs currently executing on the local node will be interrupted.
     * If {@code wait} parameter is set to {@code true} then grid will wait for all
     * tasks to be finished.
     * <p>
     * <b>Note:</b> it is usually safer and more appropriate to stop grid instances individually
     * instead of blanket operation. In most cases, the party that started the grid instance
     * should be responsible for stopping it.
     * <p>
     * Note that upon completion of this method, the JVM with forcefully exist with
     * exit code {@link #KILL_EXIT_CODE}.
     *
     * @param cancel If {@code true} then all jobs currently executing on
     *      all grids will be cancelled by calling {@link org.apache.ignite.compute.ComputeJob#cancel()}
     *      method. Note that just like with {@link Thread#interrupt()}, it is
     *      up to the actual job to exit from execution.
     * @see #KILL_EXIT_CODE
     */
    public static void kill(boolean cancel) {
        GridGainEx.kill(cancel);
    }

    /**
     * Starts grid with default configuration. By default this method will
     * use grid configuration defined in {@code GRIDGAIN_HOME/config/default-config.xml}
     * configuration file. If such file is not found, then all system defaults will be used.
     *
     * @return Started grid.
     * @throws IgniteCheckedException If default grid could not be started. This exception will be thrown
     *      also if default grid has already been started.
     */
    public static Ignite start() throws IgniteCheckedException {
        return GridGainEx.start();
    }

    /**
     * Starts grid with given configuration. Note that this method is no-op if grid with the name
     * provided in given configuration is already started.
     *
     * @param cfg Grid configuration. This cannot be {@code null}.
     * @return Started grid.
     * @throws IgniteCheckedException If grid could not be started. This exception will be thrown
     *      also if named grid has already been started.
     */
    public static Ignite start(IgniteConfiguration cfg) throws IgniteCheckedException {
        return GridGainEx.start(cfg);
    }

    /**
     * Starts all grids specified within given Spring XML configuration file. If grid with given name
     * is already started, then exception is thrown. In this case all instances that may
     * have been started so far will be stopped too.
     * <p>
     * Usually Spring XML configuration file will contain only one Grid definition. Note that
     * Grid configuration bean(s) is retrieved form configuration file by type, so the name of
     * the Grid configuration bean is ignored.
     *
     * @param springCfgPath Spring XML configuration file path or URL.
     * @return Started grid. If Spring configuration contains multiple grid instances,
     *      then the 1st found instance is returned.
     * @throws IgniteCheckedException If grid could not be started or configuration
     *      read. This exception will be thrown also if grid with given name has already
     *      been started or Spring XML configuration file is invalid.
     */
    public static Ignite start(@Nullable String springCfgPath) throws IgniteCheckedException {
        return GridGainEx.start(springCfgPath);
    }

    /**
     * Starts all grids specified within given Spring XML configuration file URL. If grid with given name
     * is already started, then exception is thrown. In this case all instances that may
     * have been started so far will be stopped too.
     * <p>
     * Usually Spring XML configuration file will contain only one Grid definition. Note that
     * Grid configuration bean(s) is retrieved form configuration file by type, so the name of
     * the Grid configuration bean is ignored.
     *
     * @param springCfgUrl Spring XML configuration file URL. This cannot be {@code null}.
     * @return Started grid. If Spring configuration contains multiple grid instances,
     *      then the 1st found instance is returned.
     * @throws IgniteCheckedException If grid could not be started or configuration
     *      read. This exception will be thrown also if grid with given name has already
     *      been started or Spring XML configuration file is invalid.
     */
    public static Ignite start(URL springCfgUrl) throws IgniteCheckedException {
        return GridGainEx.start(springCfgUrl);
    }

    /**
     * Gets an instance of default no-name grid. Note that
     * caller of this method should not assume that it will return the same
     * instance every time.
     * <p>
     * This method is identical to {@code G.grid(null)} apply.
     *
     * @return An instance of default no-name grid. This method never returns
     *      {@code null}.
     * @throws IgniteIllegalStateException Thrown if default grid was not properly
     *      initialized or grid instance was stopped or was not started.
     */
    public static Ignite ignite() throws IgniteIllegalStateException {
        return GridGainEx.grid();
    }

    /**
     * Gets a list of all grids started so far.
     *
     * @return List of all grids started so far.
     */
    public static List<Ignite> allGrids() {
        return GridGainEx.allGrids();
    }

    /**
     * Gets a grid instance for given local node ID. Note that grid instance and local node have
     * one-to-one relationship where node has ID and instance has name of the grid to which
     * both grid instance and its node belong. Note also that caller of this method
     * should not assume that it will return the same instance every time.
     *
     * @param locNodeId ID of local node the requested grid instance is managing.
     * @return An instance of named grid. This method never returns
     *      {@code null}.
     * @throws IgniteIllegalStateException Thrown if grid was not properly
     *      initialized or grid instance was stopped or was not started.
     */
    public static Ignite ignite(UUID locNodeId) throws IgniteIllegalStateException {
        return GridGainEx.grid(locNodeId);
    }

    /**
     * Gets an named grid instance. If grid name is {@code null} or empty string,
     * then default no-name grid will be returned. Note that caller of this method
     * should not assume that it will return the same instance every time.
     * <p>
     * Note that Java VM can run multiple grid instances and every grid instance (and its
     * node) can belong to a different grid. Grid name defines what grid a particular grid
     * instance (and correspondingly its node) belongs to.
     *
     * @param name Grid name to which requested grid instance belongs to. If {@code null},
     *      then grid instance belonging to a default no-name grid will be returned.
     * @return An instance of named grid. This method never returns
     *      {@code null}.
     * @throws IgniteIllegalStateException Thrown if default grid was not properly
     *      initialized or grid instance was stopped or was not started.
     */
    public static Ignite ignite(@Nullable String name) throws IgniteIllegalStateException {
        return GridGainEx.grid(name);
    }

    /**
     * Adds a lsnr for grid life cycle events.
     * <p>
     * Note that unlike other listeners in GridGain this listener will be
     * notified from the same thread that triggers the state change. Because of
     * that it is the responsibility of the user to make sure that listener logic
     * is light-weight and properly handles (catches) any runtime exceptions, if any
     * are expected.
     *
     * @param lsnr Listener for grid life cycle events. If this listener was already added
     *      this method is no-op.
     */
    public static void addListener(IgniteListener lsnr) {
        GridGainEx.addListener(lsnr);
    }

    /**
     * Removes lsnr added by {@link #addListener(org.apache.ignite.lifecycle.IgniteListener)} method.
     *
     * @param lsnr Listener to remove.
     * @return {@code true} if lsnr was added before, {@code false} otherwise.
     */
    public static boolean removeListener(IgniteListener lsnr) {
        return GridGainEx.removeListener(lsnr);
    }
}
