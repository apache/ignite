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

package org.apache.ignite.internal;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Handler;
import java.util.stream.Collectors;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteState;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.IgnitionListener;
import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.ExecutorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.SystemDataRegionConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.binary.BinaryArray;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl;
import org.apache.ignite.internal.processors.resource.DependencyResolver;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.TimeBag;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.internal.util.typedef.CA;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.mxbean.IgnitionMXBean;
import org.apache.ignite.plugin.segmentation.SegmentationPolicy;
import org.apache.ignite.resources.SpringApplicationContextResource;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.checkpoint.noop.NoopCheckpointSpi;
import org.apache.ignite.spi.collision.noop.NoopCollisionSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.deployment.local.LocalDeploymentSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.encryption.noop.NoopEncryptionSpi;
import org.apache.ignite.spi.eventstorage.NoopEventStorageSpi;
import org.apache.ignite.spi.failover.always.AlwaysFailoverSpi;
import org.apache.ignite.spi.indexing.noop.NoopIndexingSpi;
import org.apache.ignite.spi.loadbalancing.LoadBalancingSpi;
import org.apache.ignite.spi.loadbalancing.roundrobin.RoundRobinLoadBalancingSpi;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.spi.metric.noop.NoopMetricExporterSpi;
import org.apache.ignite.spi.tracing.NoopTracingSpi;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.IgniteState.STARTED;
import static org.apache.ignite.IgniteState.STOPPED;
import static org.apache.ignite.IgniteState.STOPPED_ON_FAILURE;
import static org.apache.ignite.IgniteState.STOPPED_ON_SEGMENTATION;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_CACHE_CLIENT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_CONFIG_URL;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DEP_MODE_OVERRIDE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_LOCAL_HOST;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_NO_SHUTDOWN_HOOK;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_OVERRIDE_CONSISTENT_ID;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_RESTART_CODE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SYSTEM_WORKER_BLOCKED_TIMEOUT;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.MemoryConfiguration.DFLT_MEMORY_POLICY_MAX_SIZE;
import static org.apache.ignite.configuration.MemoryConfiguration.DFLT_MEM_PLC_DEFAULT_NAME;
import static org.apache.ignite.internal.IgniteComponentType.SPRING;
import static org.apache.ignite.internal.processors.task.TaskExecutionOptions.options;
import static org.apache.ignite.internal.util.IgniteUtils.EMPTY_STRS;
import static org.apache.ignite.internal.util.IgniteUtils.IGNITE_MBEANS_DISABLED;
import static org.apache.ignite.plugin.segmentation.SegmentationPolicy.RESTART_JVM;

/**
 * This class is part of an internal API and can be modified at any time without backward compatibility.
 *
 * This class defines a factory for the main Ignite API. It controls Grid life cycle
 * and allows listening for grid events.
 * <h1 class="header">Grid Loaders</h1>
 * Although user can apply grid factory directly to start and stop grid, grid is
 * often started and stopped by grid loaders. Grid loaders can be found in
 * {@link org.apache.ignite.startup} package, for example:
 * <ul>
 * <li>{@code CommandLineStartup}</li>
 * <li>{@code ServletStartup}</li>
 * </ul>
 * <h1 class="header">Examples</h1>
 * Use {@link #start()} method to start grid with default configuration. You can also use
 * {@link IgniteConfiguration} to override some default configuration. Below is an
 * example on how to start grid with <strong>URI deployment</strong>.
 * <pre name="code" class="java">
 * GridConfiguration cfg = new GridConfiguration();
 */
public class IgnitionEx {
    /** Default configuration path relative to Ignite home. */
    public static final String DFLT_CFG = "config/default-config.xml";

    /** */
    private static final int WAIT_FOR_BACKUPS_CHECK_INTERVAL = 1000;

    /** Key to store list of gracefully stopping nodes within metastore. */
    private static final String GRACEFUL_SHUTDOWN_METASTORE_KEY =
        DistributedMetaStorageImpl.IGNITE_INTERNAL_KEY_PREFIX + "graceful.shutdown";

    /** Map of named Ignite instances. */
    private static final ConcurrentMap<Object, IgniteNamedInstance> grids = new ConcurrentHashMap<>();

    /** Map of grid states ever started in this JVM. */
    private static final Map<Object, IgniteState> gridStates = new ConcurrentHashMap<>();

    /** Mutex to synchronize updates of default grid reference. */
    private static final Object dfltGridMux = new Object();

    /** Default grid. */
    private static volatile IgniteNamedInstance dfltGrid;

    /** Default grid state. */
    private static volatile IgniteState dfltGridState;

    /** List of state listeners. */
    private static final Collection<IgnitionListener> lsnrs = new GridConcurrentHashSet<>(4);

    /** */
    private static ThreadLocal<Boolean> clientMode = new ThreadLocal<>();

    /** Dependency container. */
    private static ThreadLocal<DependencyResolver> dependencyResolver = new ThreadLocal<>();

    /**
     * Enforces singleton.
     */
    private IgnitionEx() {
        // No-op.
    }

    /**
     * Sets client mode flag.
     *
     * @param clientMode Client mode flag.
     */
    public static void setClientMode(boolean clientMode) {
        IgnitionEx.clientMode.set(clientMode);
    }

    /**
     * Gets client mode flag.
     *
     * @return Client mode flag.
     */
    public static boolean isClientMode() {
        return clientMode.get() == null ? false : clientMode.get();
    }

    /**
     * Gets state of grid default grid.
     *
     * @return Default grid state.
     */
    public static IgniteState state() {
        return state(null);
    }

    /**
     * Gets states of named Ignite instance. If name is {@code null}, then state of
     * default no-name Ignite instance is returned.
     *
     * @param name Ignite instance name. If name is {@code null}, then state of
     *      default no-name Ignite instance is returned.
     * @return Grid state.
     */
    public static IgniteState state(@Nullable String name) {
        IgniteNamedInstance grid = name != null ? grids.get(name) : dfltGrid;

        if (grid == null) {
            IgniteState state = name != null ? gridStates.get(name) : dfltGridState;

            return state != null ? state : STOPPED;
        }

        return grid.state();
    }

    /**
     * Stops default grid. This method is identical to {@code G.stop(null, cancel)} apply.
     * Note that method does not wait for all tasks to be completed.
     *
     * @param cancel If {@code true} then all jobs currently executing on
     *      default grid will be cancelled by calling {@link ComputeJob#cancel()}
     *      method. Note that just like with {@link Thread#interrupt()}, it is
     *      up to the actual job to exit from execution
     * @param shutdown If this parameter is set explicitly this policy will use for stopping node.
     *       If this parameter is {@code null} common cluster policy will be use.
     * @return {@code true} if default grid instance was indeed stopped,
     *      {@code false} otherwise (if it was not started).
     */
    public static boolean stop(boolean cancel, @Nullable ShutdownPolicy shutdown) {
        return stop(null, cancel, shutdown, false);
    }

    /**
     * Stops named Ignite instance. If {@code cancel} flag is set to {@code true} then
     * all jobs currently executing on local node will be interrupted. If
     * Ignite instance name is {@code null}, then default no-name Ignite instance will be stopped.
     * If wait parameter is set to {@code true} then Ignite instance will wait for all
     * tasks to be finished.
     *
     * @param name Ignite instance name. If {@code null}, then default no-name
     *      Ignite instance will be stopped.
     * @param cancel If {@code true} then all jobs currently will be cancelled
     *      by calling {@link ComputeJob#cancel()} method. Note that just like with
     *      {@link Thread#interrupt()}, it is up to the actual job to exit from
     *      execution. If {@code false}, then jobs currently running will not be
     *      canceled. In either case, grid node will wait for completion of all
     *      jobs running on it before stopping.
     * @param shutdown If this parameter is set explicitly this policy will use for stopping node.
     *      If this parameter is {@code null} common cluster policy will be use.
     * @param stopNotStarted If {@code true} and node start did not finish then interrupts starting thread.
     * @return {@code true} if named Ignite instance was indeed found and stopped,
     *      {@code false} otherwise (the instance with given {@code name} was
     *      not found).
     */
    public static boolean stop(@Nullable String name, boolean cancel,
        @Nullable ShutdownPolicy shutdown, boolean stopNotStarted) {
        IgniteNamedInstance grid = name != null ? grids.get(name) : dfltGrid;

        if (grid != null && stopNotStarted && grid.startLatch.getCount() != 0) {
            grid.starterThreadInterrupted = true;

            grid.starterThread.interrupt();
        }

        if (grid != null) {
            if (grid.state() == STARTED)
                grid.stop(cancel, shutdown);

            boolean fireEvt;

            if (name != null)
                fireEvt = grids.remove(name, grid);
            else {
                synchronized (dfltGridMux) {
                    fireEvt = dfltGrid == grid;

                    if (fireEvt)
                        dfltGrid = null;
                }
            }

            if (fireEvt)
                notifyStateChange(grid.getName(), grid.state());

            return true;
        }

        // We don't have log at this point...
        U.warn(null, "Ignoring stopping Ignite instance that was already stopped or never started: " + name);

        return false;
    }

    /**
     * @deprecated
     *
     * Behavior of the method is the almost same as {@link IgnitionEx#stop(boolean, ShutdownPolicy)}.
     * If node stopping process will not be finished within {@code timeoutMs} whole JVM will be killed.
     *
     * @param timeoutMs Timeout to wait graceful stopping.
     */
    @Deprecated
    public static boolean stop(@Nullable String name, boolean cancel, boolean stopNotStarted, long timeoutMs) {
        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        // Schedule delayed node killing if graceful stopping will be not finished within timeout.
        executor.schedule(new Runnable() {
            @Override public void run() {
                if (state(name) == IgniteState.STARTED) {
                    U.error(null, "Unable to gracefully stop node within timeout " + timeoutMs +
                        " milliseconds. Killing node...");

                    // We are not able to kill only one grid so whole JVM will be stopped.
                    Runtime.getRuntime().halt(Ignition.KILL_EXIT_CODE);
                }
            }
        }, timeoutMs, TimeUnit.MILLISECONDS);

        boolean success = stop(name, cancel, null, stopNotStarted);

        executor.shutdownNow();

        return success;
    }

    /**
     * Stops <b>all</b> started grids. If {@code cancel} flag is set to {@code true} then
     * all jobs currently executing on local node will be interrupted.
     * If wait parameter is set to {@code true} then grid will wait for all
     * tasks to be finished.
     * <p>
     * <b>Note:</b> it is usually safer and more appropriate to stop grid instances individually
     * instead of blanket operation. In most cases, the party that started the grid instance
     * should be responsible for stopping it.
     *
     * @param cancel If {@code true} then all jobs currently executing on
     *      all grids will be cancelled by calling {@link ComputeJob#cancel()}
     *      method. Note that just like with {@link Thread#interrupt()}, it is
     *      up to the actual job to exit from execution
     * @param shutdown If this parameter is set explicitly this policy will use for stopping node.
     *      If this parameter is {@code null} common cluster policy will be use.
     */
    public static void stopAll(boolean cancel, @Nullable ShutdownPolicy shutdown) {
        IgniteNamedInstance dfltGrid0 = dfltGrid;

        if (dfltGrid0 != null) {
            dfltGrid0.stop(cancel, shutdown);

            boolean fireEvt;

            synchronized (dfltGridMux) {
                fireEvt = dfltGrid == dfltGrid0;

                if (fireEvt)
                    dfltGrid = null;
            }

            if (fireEvt)
                notifyStateChange(dfltGrid0.getName(), dfltGrid0.state());
        }

        // Stop the rest and clear grids map.
        for (IgniteNamedInstance grid : grids.values()) {
            grid.stop(cancel, shutdown);

            boolean fireEvt = grids.remove(grid.getName(), grid);

            if (fireEvt)
                notifyStateChange(grid.getName(), grid.state());
        }
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
     * support Ignite's protocol for restarting. Currently only standard <tt>ignite.{sh|bat}</tt>
     * scripts support restarting of JVM Ignite's process.
     *
     * @param cancel If {@code true} then all jobs currently executing on
     *      all grids will be cancelled by calling {@link ComputeJob#cancel()}
     *      method. Note that just like with {@link Thread#interrupt()}, it is
     *      up to the actual job to exit from execution.
     * @see Ignition#RESTART_EXIT_CODE
     */
    public static void restart(boolean cancel) {
        String file = U.IGNITE_SUCCESS_FILE_PROPERTY;

        if (file == null)
            U.warn(null, "Cannot restart node when restart not enabled.");
        else {
            try {
                new File(file).createNewFile();
            }
            catch (IOException e) {
                U.error(null, "Failed to create restart marker file (restart aborted): " + e.getMessage());

                return;
            }

            U.log(null, "Restarting node. Will exit (" + Ignition.RESTART_EXIT_CODE + ").");

            // Set the exit code so that shell process can recognize it and loop
            // the start up sequence again.
            System.setProperty(IGNITE_RESTART_CODE, Integer.toString(Ignition.RESTART_EXIT_CODE));

            stopAll(cancel, null);

            // This basically leaves loaders hang - we accept it.
            System.exit(Ignition.RESTART_EXIT_CODE);
        }
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
     * exit code {@link Ignition#KILL_EXIT_CODE}.
     *
     * @param cancel If {@code true} then all jobs currently executing on
     *      all grids will be cancelled by calling {@link ComputeJob#cancel()}
     *      method. Note that just like with {@link Thread#interrupt()}, it is
     *      up to the actual job to exit from execution.
     * @see Ignition#KILL_EXIT_CODE
     */
    public static void kill(boolean cancel) {
        stopAll(cancel, null);

        // This basically leaves loaders hang - we accept it.
        System.exit(Ignition.KILL_EXIT_CODE);
    }

    /**
     * Starts grid with default configuration. By default this method will
     * use grid configuration defined in {@code IGNITE_HOME/config/default-config.xml}
     * configuration file. If such file is not found, then all system defaults will be used.
     *
     * @return Started grid.
     * @throws IgniteCheckedException If default grid could not be started. This exception will be thrown
     *      also if default grid has already been started.
     */
    public static Ignite start() throws IgniteCheckedException {
        return start((GridSpringResourceContext)null);
    }

    /**
     * Starts grid with default configuration. By default this method will
     * use grid configuration defined in {@code IGNITE_HOME/config/default-config.xml}
     * configuration file. If such file is not found, then all system defaults will be used.
     *
     * @param springCtx Optional Spring application context, possibly {@code null}.
     *      Spring bean definitions for bean injection are taken from this context.
     *      If provided, this context can be injected into grid tasks and grid jobs using
     *      {@link SpringApplicationContextResource @SpringApplicationContextResource} annotation.
     * @return Started grid.
     * @throws IgniteCheckedException If default grid could not be started. This exception will be thrown
     *      also if default grid has already been started.
     */
    public static Ignite start(@Nullable GridSpringResourceContext springCtx) throws IgniteCheckedException {
        URL url = U.resolveIgniteUrl(DFLT_CFG);

        if (url != null)
            return start(DFLT_CFG, null, springCtx, null);

        U.warn(null, "Default Spring XML file not found (is IGNITE_HOME set?): " + DFLT_CFG);

        return start0(new GridStartContext(new IgniteConfiguration(), null, springCtx), true)
            .get1().grid();
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
        return start(cfg, null, true).get1();
    }

    /**
     * Starts a grid with given configuration. If the grid is already started and failIfStarted set to TRUE
     * an exception will be thrown.
     *
     * @param cfg Grid configuration. This cannot be {@code null}.
     * @param failIfStarted When flag is {@code true} and grid with specified name has been already started
     *      the exception is thrown. Otherwise the existing instance of the grid is returned.
     * @return Started grid or existing grid.
     * @throws IgniteCheckedException If grid could not be started. This exception will be thrown
     *      also if named grid has already been started.
     */
    public static Ignite start(IgniteConfiguration cfg, boolean failIfStarted) throws IgniteCheckedException {
        return start(cfg, null, failIfStarted).get1();
    }

    /**
     * Gets or starts new grid instance if it hasn't been started yet.
     *
     * @param cfg Grid configuration. This cannot be {@code null}.
     * @return Tuple with: grid instance and flag to indicate the instance is started by this call.
     *      So, when the new ignite instance is started the flag is {@code true}. If an existing instance is returned
     *      the flag is {@code false}.
     * @throws IgniteException If grid could not be started.
     */
    public static T2<Ignite, Boolean> getOrStart(IgniteConfiguration cfg) throws IgniteException {
        try {
            return start(cfg, null, false);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * Starts grid with given configuration. Note that this method will throw and exception if grid with the name
     * provided in given configuration is already started.
     *
     * @param cfg Grid configuration. This cannot be {@code null}.
     * @param springCtx Optional Spring application context, possibly {@code null}.
     *      Spring bean definitions for bean injection are taken from this context.
     *      If provided, this context can be injected into grid tasks and grid jobs using
     *      {@link SpringApplicationContextResource @SpringApplicationContextResource} annotation.
     * @return Started grid.
     * @throws IgniteCheckedException If grid could not be started. This exception will be thrown
     *      also if named grid has already been started.
     */
    public static Ignite start(IgniteConfiguration cfg, @Nullable GridSpringResourceContext springCtx) throws IgniteCheckedException {
        A.notNull(cfg, "cfg");

        return start0(new GridStartContext(cfg, null, springCtx), true).get1().grid();
    }

    /**
     * Starts grid with given configuration. If the grid is already started and failIfStarted set to TRUE
     * an exception will be thrown.
     *
     * @param cfg Grid configuration. This cannot be {@code null}.
     * @param springCtx Optional Spring application context, possibly {@code null}.
     *      Spring bean definitions for bean injection are taken from this context.
     *      If provided, this context can be injected into grid tasks and grid jobs using
     *      {@link SpringApplicationContextResource @SpringApplicationContextResource} annotation.
     * @param failIfStarted Throw or not an exception if grid is already started.
     * @return Tuple with: grid instance and flag to indicate the instance is started by this call.
     *      So, when the new ignite instance is started the flag is {@code true}. If an existing instance is returned
     *      the flag is {@code false}.
     * @throws IgniteCheckedException If grid could not be started. This exception will be thrown
     *      also if named grid has already been started.
     */
    public static T2<Ignite, Boolean> start(
        IgniteConfiguration cfg,
        @Nullable GridSpringResourceContext springCtx,
        boolean failIfStarted
    ) throws IgniteCheckedException {
        A.notNull(cfg, "cfg");

        T2<IgniteNamedInstance, Boolean> res = start0(new GridStartContext(cfg, null, springCtx), failIfStarted);

        return new T2<>((Ignite)res.get1().grid(), res.get2());
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
        return springCfgPath == null ? start() : start(springCfgPath, null);
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
     * @param igniteInstanceName Ignite instance name that will override default.
     * @return Started grid. If Spring configuration contains multiple grid instances,
     *      then the 1st found instance is returned.
     * @throws IgniteCheckedException If grid could not be started or configuration
     *      read. This exception will be thrown also if grid with given name has already
     *      been started or Spring XML configuration file is invalid.
     */
    public static Ignite start(@Nullable String springCfgPath, @Nullable String igniteInstanceName)
        throws IgniteCheckedException {
        if (springCfgPath == null) {
            IgniteConfiguration cfg = new IgniteConfiguration();

            if (cfg.getIgniteInstanceName() == null && !F.isEmpty(igniteInstanceName))
                cfg.setIgniteInstanceName(igniteInstanceName);

            return start(cfg);
        }
        else
            return start(springCfgPath, igniteInstanceName, null, null);
    }

    /**
     * Loads all grid configurations specified within given Spring XML configuration file.
     * <p>
     * Usually Spring XML configuration file will contain only one Grid definition. Note that
     * Grid configuration bean(s) is retrieved form configuration file by type, so the name of
     * the Grid configuration bean is ignored.
     *
     * @param springCfgUrl Spring XML configuration file path or URL. This cannot be {@code null}.
     * @return Tuple containing all loaded configurations and Spring context used to load them.
     * @throws IgniteCheckedException If grid could not be started or configuration
     *      read. This exception will be thrown also if grid with given name has already
     *      been started or Spring XML configuration file is invalid.
     */
    public static IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext>
        loadConfigurations(URL springCfgUrl) throws IgniteCheckedException {
        IgniteSpringHelper spring = SPRING.create(false);

        return spring.loadConfigurations(springCfgUrl);
    }

    /**
     * Loads all grid configurations specified within given input stream.
     * <p>
     * Usually Spring XML input stream will contain only one Grid definition. Note that
     * Grid configuration bean(s) is retrieved form configuration input stream by type, so the name of
     * the Grid configuration bean is ignored.
     *
     * @param springCfgStream Input stream contained Spring XML configuration. This cannot be {@code null}.
     * @return Tuple containing all loaded configurations and Spring context used to load them.
     * @throws IgniteCheckedException If grid could not be started or configuration
     *      read. This exception will be thrown also if grid with given name has already
     *      been started or Spring XML configuration file is invalid.
     */
    public static IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext>
        loadConfigurations(InputStream springCfgStream) throws IgniteCheckedException {
        IgniteSpringHelper spring = SPRING.create(false);

        return spring.loadConfigurations(springCfgStream);
    }

    /**
     * Loads all grid configurations specified within given Spring XML configuration file.
     * <p>
     * Usually Spring XML configuration file will contain only one Grid definition. Note that
     * Grid configuration bean(s) is retrieved form configuration file by type, so the name of
     * the Grid configuration bean is ignored.
     *
     * @param springCfgPath Spring XML configuration file path. This cannot be {@code null}.
     * @return Tuple containing all loaded configurations and Spring context used to load them.
     * @throws IgniteCheckedException If grid could not be started or configuration
     *      read. This exception will be thrown also if grid with given name has already
     *      been started or Spring XML configuration file is invalid.
     */
    public static IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext>
        loadConfigurations(String springCfgPath) throws IgniteCheckedException {
        A.notNull(springCfgPath, "springCfgPath");
        return loadConfigurations(IgniteUtils.resolveSpringUrl(springCfgPath));
    }

    /**
     * Loads first found grid configuration specified within given Spring XML configuration file.
     * <p>
     * Usually Spring XML configuration file will contain only one Grid definition. Note that
     * Grid configuration bean(s) is retrieved form configuration file by type, so the name of
     * the Grid configuration bean is ignored.
     *
     * @param springCfgUrl Spring XML configuration file path or URL. This cannot be {@code null}.
     * @return First found configuration and Spring context used to load it.
     * @throws IgniteCheckedException If grid could not be started or configuration
     *      read. This exception will be thrown also if grid with given name has already
     *      been started or Spring XML configuration file is invalid.
     */
    public static IgniteBiTuple<IgniteConfiguration, GridSpringResourceContext> loadConfiguration(URL springCfgUrl)
        throws IgniteCheckedException {
        IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> t =
            loadConfigurations(springCfgUrl);

        return F.t(F.first(t.get1()), t.get2());
    }

    /**
     * Loads first found grid configuration specified within given Spring XML configuration file.
     * <p>
     * Usually Spring XML configuration file will contain only one Grid definition. Note that
     * Grid configuration bean(s) is retrieved form configuration file by type, so the name of
     * the Grid configuration bean is ignored.
     *
     * @param springCfgPath Spring XML configuration file path. This cannot be {@code null}.
     * @return First found configuration and Spring context used to load it.
     * @throws IgniteCheckedException If grid could not be started or configuration
     *      read. This exception will be thrown also if grid with given name has already
     *      been started or Spring XML configuration file is invalid.
     */
    public static IgniteBiTuple<IgniteConfiguration, GridSpringResourceContext> loadConfiguration(String springCfgPath)
        throws IgniteCheckedException {
        IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> t =
            loadConfigurations(springCfgPath);

        return F.t(F.first(t.get1()), t.get2());
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
     * @param springCfgPath Spring XML configuration file path or URL. This cannot be {@code null}.
     * @param igniteInstanceName Ignite instance name that will override default.
     * @param springCtx Optional Spring application context, possibly {@code null}.
     * @param ldr Optional class loader that will be used by default.
     *      Spring bean definitions for bean injection are taken from this context.
     *      If provided, this context can be injected into grid tasks and grid jobs using
     *      {@link SpringApplicationContextResource @SpringApplicationContextResource} annotation.
     * @return Started grid. If Spring configuration contains multiple grid instances,
     *      then the 1st found instance is returned.
     * @throws IgniteCheckedException If grid could not be started or configuration
     *      read. This exception will be thrown also if grid with given name has already
     *      been started or Spring XML configuration file is invalid.
     */
    public static Ignite start(String springCfgPath, @Nullable String igniteInstanceName,
        @Nullable GridSpringResourceContext springCtx, @Nullable ClassLoader ldr) throws IgniteCheckedException {
        URL url = U.resolveSpringUrl(springCfgPath);

        return start(url, igniteInstanceName, springCtx, ldr);
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
        return start(springCfgUrl, null, null, null);
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
     * @param ldr Optional class loader that will be used by default.
     * @return Started grid. If Spring configuration contains multiple grid instances,
     *      then the 1st found instance is returned.
     * @throws IgniteCheckedException If grid could not be started or configuration
     *      read. This exception will be thrown also if grid with given name has already
     *      been started or Spring XML configuration file is invalid.
     */
    public static Ignite start(URL springCfgUrl, @Nullable ClassLoader ldr) throws IgniteCheckedException {
        return start(springCfgUrl, null, null, ldr);
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
     * @param igniteInstanceName Ignite instance name that will override default.
     * @param springCtx Optional Spring application context, possibly {@code null}.
     * @param ldr Optional class loader that will be used by default.
     *      Spring bean definitions for bean injection are taken from this context.
     *      If provided, this context can be injected into grid tasks and grid jobs using
     *      {@link SpringApplicationContextResource @SpringApplicationContextResource} annotation.
     * @return Started grid. If Spring configuration contains multiple grid instances,
     *      then the 1st found instance is returned.
     * @throws IgniteCheckedException If grid could not be started or configuration
     *      read. This exception will be thrown also if grid with given name has already
     *      been started or Spring XML configuration file is invalid.
     */
    public static Ignite start(URL springCfgUrl, @Nullable String igniteInstanceName,
        @Nullable GridSpringResourceContext springCtx, @Nullable ClassLoader ldr) throws IgniteCheckedException {
        A.notNull(springCfgUrl, "springCfgUrl");

        Collection<Handler> savedHnds = U.addJavaNoOpLogger();

        IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> cfgMap;

        try {
            cfgMap = loadConfigurations(springCfgUrl);
        }
        finally {
            U.removeJavaNoOpLogger(savedHnds);
        }

        return startConfigurations(cfgMap, springCfgUrl, igniteInstanceName, springCtx, ldr);
    }

    /**
     * Starts all grids specified within given Spring XML configuration input stream. If grid with given name
     * is already started, then exception is thrown. In this case all instances that may
     * have been started so far will be stopped too.
     * <p>
     * Usually Spring XML configuration input stream will contain only one Grid definition. Note that
     * Grid configuration bean(s) is retrieved form configuration input stream by type, so the name of
     * the Grid configuration bean is ignored.
     *
     * @param springCfgStream Input stream containing Spring XML configuration. This cannot be {@code null}.
     * @return Started grid. If Spring configuration contains multiple grid instances,
     *      then the 1st found instance is returned.
     * @throws IgniteCheckedException If grid could not be started or configuration
     *      read. This exception will be thrown also if grid with given name has already
     *      been started or Spring XML configuration file is invalid.
     */
    public static Ignite start(InputStream springCfgStream) throws IgniteCheckedException {
        return start(springCfgStream, null, null, null);
    }

    /**
     * Starts all grids specified within given Spring XML configuration input stream. If grid with given name
     * is already started, then exception is thrown. In this case all instances that may
     * have been started so far will be stopped too.
     * <p>
     * Usually Spring XML configuration input stream will contain only one Grid definition. Note that
     * Grid configuration bean(s) is retrieved form configuration input stream by type, so the name of
     * the Grid configuration bean is ignored.
     *
     * @param springCfgStream Input stream containing Spring XML configuration. This cannot be {@code null}.
     * @param igniteInstanceName Ignite instance name that will override default.
     * @param springCtx Optional Spring application context, possibly {@code null}.
     * @param ldr Optional class loader that will be used by default.
     *      Spring bean definitions for bean injection are taken from this context.
     *      If provided, this context can be injected into grid tasks and grid jobs using
     *      {@link SpringApplicationContextResource @SpringApplicationContextResource} annotation.
     * @return Started grid. If Spring configuration contains multiple grid instances,
     *      then the 1st found instance is returned.
     * @throws IgniteCheckedException If grid could not be started or configuration
     *      read. This exception will be thrown also if grid with given name has already
     *      been started or Spring XML configuration file is invalid.
     */
    public static Ignite start(InputStream springCfgStream, @Nullable String igniteInstanceName,
        @Nullable GridSpringResourceContext springCtx, @Nullable ClassLoader ldr) throws IgniteCheckedException {
        A.notNull(springCfgStream, "springCfgUrl");

        Collection<Handler> savedHnds = U.addJavaNoOpLogger();

        IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> cfgMap;

        try {
            cfgMap = loadConfigurations(springCfgStream);
        }
        finally {
            U.removeJavaNoOpLogger(savedHnds);
        }

        return startConfigurations(cfgMap, null, igniteInstanceName, springCtx, ldr);
    }

    /**
     * Internal Spring-based start routine. Starts loaded configurations.
     *
     * @param cfgMap Configuration map.
     * @param springCfgUrl Spring XML configuration file URL.
     * @param igniteInstanceName Ignite instance name that will override default.
     * @param springCtx Optional Spring application context.
     * @param ldr Optional class loader that will be used by default.
     * @return Started grid.
     * @throws IgniteCheckedException If failed.
     */
    private static Ignite startConfigurations(
        IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> cfgMap,
        URL springCfgUrl,
        @Nullable String igniteInstanceName,
        @Nullable GridSpringResourceContext springCtx,
        @Nullable ClassLoader ldr)
        throws IgniteCheckedException {
        List<IgniteNamedInstance> grids = new ArrayList<>(cfgMap.size());

        try {
            for (IgniteConfiguration cfg : cfgMap.get1()) {
                assert cfg != null;

                if (cfg.getIgniteInstanceName() == null && !F.isEmpty(igniteInstanceName))
                    cfg.setIgniteInstanceName(igniteInstanceName);

                if (ldr != null && cfg.getClassLoader() == null)
                    cfg.setClassLoader(ldr);

                // Use either user defined context or our one.
                IgniteNamedInstance grid = start0(
                    new GridStartContext(cfg, springCfgUrl, springCtx == null
                        ? cfgMap.get2() : springCtx), true).get1();

                // Add it if it was not stopped during startup.
                if (grid != null)
                    grids.add(grid);
            }
        }
        catch (IgniteCheckedException e) {
            // Stop all instances started so far.
            for (IgniteNamedInstance grid : grids) {
                try {
                    grid.stop(true, ShutdownPolicy.IMMEDIATE);
                }
                catch (Exception e1) {
                    U.error(grid.log, "Error when stopping grid: " + grid, e1);
                }
            }

            throw e;
        }

        // Return the first grid started.
        IgniteNamedInstance res = !grids.isEmpty() ? grids.get(0) : null;

        return res != null ? res.grid() : null;
    }

    /**
     * Starts grid with given configuration.
     *
     * @param startCtx Start context.
     * @param failIfStarted Throw or not an exception if grid is already started.
     * @return Tuple with: grid instance and flag to indicate the instance is started by this call.
     *      So, when the new ignite instance is started the flag is {@code true}. If an existing instance is returned
     *      the flag is {@code false}.
     * @throws IgniteCheckedException If grid could not be started.
     */
    private static T2<IgniteNamedInstance, Boolean> start0(
        GridStartContext startCtx,
        boolean failIfStarted
    ) throws IgniteCheckedException {
        assert startCtx != null;

        String name = startCtx.config().getIgniteInstanceName();

        if (name != null && name.isEmpty())
            throw new IgniteCheckedException("Non default Ignite instances cannot have empty string name.");

        IgniteNamedInstance grid = new IgniteNamedInstance(name);

        IgniteNamedInstance old;

        if (name != null)
            old = grids.putIfAbsent(name, grid);
        else {
            synchronized (dfltGridMux) {
                old = dfltGrid;

                if (old == null)
                    dfltGrid = grid;
            }
        }

        if (old != null)
            if (old.grid() == null) { // Stopped but not removed from map yet.
                boolean replaced;

                if (name != null)
                    replaced = grids.replace(name, old, grid);
                else {
                    synchronized (dfltGridMux) {
                        replaced = old == dfltGrid;

                        if (replaced)
                            dfltGrid = grid;
                    }
                }

                if (!replaced) {
                    throw new IgniteCheckedException("Ignite instance with this name has been concurrently started: " +
                        name);
                }
                else
                    notifyStateChange(old.getName(), old.state());
            }
            else if (failIfStarted) {
                if (name == null)
                    throw new IgniteCheckedException("Default Ignite instance has already been started.");
                else
                    throw new IgniteCheckedException("Ignite instance with this name has already been started: " +
                        name);
            }
            else
                return new T2<>(old, false);

        if (startCtx.config().getWarmupClosure() != null)
            startCtx.config().getWarmupClosure().apply(startCtx.config());

        startCtx.single(grids.size() == 1);

        boolean success = false;

        try {
            try {
                grid.start(startCtx);
            }
            catch (Exception e) {
                if (X.hasCause(e, IgniteInterruptedCheckedException.class, InterruptedException.class)) {
                    if (grid.starterThreadInterrupted)
                        Thread.interrupted();
                }

                throw e;
            }

            notifyStateChange(name, STARTED);

            success = true;
        }
        finally {
            if (!success) {
                if (name != null)
                    grids.remove(name, grid);
                else {
                    synchronized (dfltGridMux) {
                        if (dfltGrid == grid)
                            dfltGrid = null;
                    }
                }

                grid = null;
            }
        }

        if (grid == null)
            throw new IgniteCheckedException("Failed to start grid with provided configuration.");

        return new T2<>(grid, true);
    }

    /**
     * Loads spring bean by name.
     *
     * @param springXmlPath Spring XML file path.
     * @param beanName Bean name.
     * @return Bean instance.
     * @throws IgniteCheckedException In case of error.
     */
    public static <T> T loadSpringBean(String springXmlPath, String beanName) throws IgniteCheckedException {
        A.notNull(springXmlPath, "springXmlPath");
        A.notNull(beanName, "beanName");

        URL url = U.resolveSpringUrl(springXmlPath);

        assert url != null;

        return loadSpringBean(url, beanName);
    }

    /**
     * Loads spring bean by name.
     *
     * @param springXmlUrl Spring XML file URL.
     * @param beanName Bean name.
     * @return Bean instance.
     * @throws IgniteCheckedException In case of error.
     */
    public static <T> T loadSpringBean(URL springXmlUrl, String beanName) throws IgniteCheckedException {
        A.notNull(springXmlUrl, "springXmlUrl");
        A.notNull(beanName, "beanName");

        IgniteSpringHelper spring = SPRING.create(false);

        return spring.loadBean(springXmlUrl, beanName);
    }

    /**
     * Loads spring bean by name.
     *
     * @param springXmlStream Input stream containing Spring XML configuration.
     * @param beanName Bean name.
     * @return Bean instance.
     * @throws IgniteCheckedException In case of error.
     */
    public static <T> T loadSpringBean(InputStream springXmlStream, String beanName) throws IgniteCheckedException {
        A.notNull(springXmlStream, "springXmlPath");
        A.notNull(beanName, "beanName");

        IgniteSpringHelper spring = SPRING.create(false);

        return spring.loadBean(springXmlStream, beanName);
    }

    /**
     * Gets an instance of default no-name grid. Note that
     * caller of this method should not assume that it will return the same
     * instance every time.
     *
     * @return An instance of default no-name grid. This method never returns
     *      {@code null}.
     * @throws IgniteIllegalStateException Thrown if default grid was not properly
     *      initialized or grid instance was stopped or was not started.
     */
    public static Ignite grid() throws IgniteIllegalStateException {
        return grid((String)null);
    }

    /**
     * Gets a list of all grids started so far.
     *
     * @return List of all grids started so far.
     */
    public static List<Ignite> allGrids() {
        return allGrids(true);
    }

    /**
     * Gets a list of all grids started so far.
     *
     * @return List of all grids started so far.
     */
    public static List<Ignite> allGridsx() {
        return allGrids(false);
    }

    /**
     * Gets a list of all grids started so far.
     *
     * @param wait If {@code true} wait for node start finish.
     * @return List of all grids started so far.
     */
    private static List<Ignite> allGrids(boolean wait) {
        List<Ignite> allIgnites = new ArrayList<>(grids.size() + 1);

        for (IgniteNamedInstance grid : grids.values()) {
            Ignite g = wait ? grid.grid() : grid.gridx();

            if (g != null)
                allIgnites.add(g);
        }

        IgniteNamedInstance dfltGrid0 = dfltGrid;

        if (dfltGrid0 != null) {
            IgniteKernal g = wait ? dfltGrid0.grid() : dfltGrid0.gridx();

            if (g != null)
                allIgnites.add(g);
        }

        return allIgnites;
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
    public static Ignite grid(UUID locNodeId) throws IgniteIllegalStateException {
        A.notNull(locNodeId, "locNodeId");

        IgniteNamedInstance dfltGrid0 = dfltGrid;

        if (dfltGrid0 != null) {
            IgniteKernal g = dfltGrid0.grid();

            if (g != null && g.localNodeId().equals(locNodeId))
                return g;
        }

        for (IgniteNamedInstance grid : grids.values()) {
            IgniteKernal g = grid.grid();

            if (g != null && g.localNodeId().equals(locNodeId))
                return g;
        }

        throw new IgniteIllegalStateException("Grid instance with given local node ID was not properly " +
            "started or was stopped: " + locNodeId);
    }

    /**
     * Gets grid instance without waiting its initialization and not throwing any exception.
     *
     * @param locNodeId ID of local node the requested grid instance is managing.
     * @return Grid instance or {@code null}.
     */
    public static IgniteKernal gridxx(UUID locNodeId) {
        IgniteNamedInstance dfltGrid0 = dfltGrid;

        if (dfltGrid0 != null) {
            IgniteKernal g = dfltGrid0.grid();

            if (g != null && g.localNodeId().equals(locNodeId))
                return g;
        }

        for (IgniteNamedInstance grid : grids.values()) {
            IgniteKernal g = grid.grid();

            if (g != null && g.localNodeId().equals(locNodeId))
                return g;
        }

        return null;
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
    public static Ignite grid(@Nullable String name) throws IgniteIllegalStateException {
        IgniteNamedInstance grid = name != null ? grids.get(name) : dfltGrid;

        Ignite res;

        if (grid == null || (res = grid.grid()) == null)
            throw new IgniteIllegalStateException("Ignite instance with provided name doesn't exist. " +
                "Did you call Ignition.start(..) to start an Ignite instance? [name=" + name + ']');

        return res;
    }

    /**
     * Gets a name of the grid from thread local config, which is owner of current thread.
     *
     * @return Grid instance related to current thread
     * @throws IllegalArgumentException Thrown to indicate, that current thread is not an {@link IgniteThread}.
     */
    public static IgniteKernal localIgnite() throws IllegalArgumentException {
        String name = U.getCurrentIgniteName();

        if (U.isCurrentIgniteNameSet(name))
            return gridx(name);
        else if (Thread.currentThread() instanceof IgniteThread)
            return gridx(((IgniteThread)Thread.currentThread()).getIgniteInstanceName());
        else
            throw new IllegalArgumentException("Ignite instance name thread local must be set or" +
                " this method should be accessed under " + IgniteThread.class.getName());
    }

    /**
     * Gets grid instance without waiting its initialization.
     *
     * @param name Grid name.
     * @return Grid instance.
     */
    public static IgniteKernal gridx(@Nullable String name) {
        IgniteNamedInstance grid = name != null ? grids.get(name) : dfltGrid;

        IgniteKernal res;

        if (grid == null || (res = grid.gridx()) == null)
            throw new IgniteIllegalStateException("Ignite instance with provided name doesn't exist. " +
                "Did you call Ignition.start(..) to start an Ignite instance? [name=" + name + ']');

        return res;
    }

    /**
     * Adds a lsnr for grid life cycle events.
     * <p>
     * Note that unlike other listeners in Ignite this listener will be
     * notified from the same thread that triggers the state change. Because of
     * that it is the responsibility of the user to make sure that listener logic
     * is light-weight and properly handles (catches) any runtime exceptions, if any
     * are expected.
     *
     * @param lsnr Listener for grid life cycle events. If this listener was already added
     *      this method is no-op.
     */
    public static void addListener(IgnitionListener lsnr) {
        A.notNull(lsnr, "lsnr");

        lsnrs.add(lsnr);
    }

    /**
     * Removes lsnr added by {@link #addListener(IgnitionListener)} method.
     *
     * @param lsnr Listener to remove.
     * @return {@code true} if lsnr was added before, {@code false} otherwise.
     */
    public static boolean removeListener(IgnitionListener lsnr) {
        A.notNull(lsnr, "lsnr");

        return lsnrs.remove(lsnr);
    }

    /**
     * @param igniteInstanceName Ignite instance name.
     * @param state Factory state.
     */
    private static void notifyStateChange(@Nullable String igniteInstanceName, IgniteState state) {
        if (igniteInstanceName != null)
            gridStates.put(igniteInstanceName, state);
        else
            dfltGridState = state;

        for (IgnitionListener lsnr : lsnrs)
            lsnr.onStateChange(igniteInstanceName, state);
    }

    /**
     * Sets custom dependency resolver which provides overridden dependencies
     *
     * @param rslvr Dependency resolver.
     */
    public static void dependencyResolver(DependencyResolver rslvr) {
        dependencyResolver.set(rslvr);
    }

    /**
     * Custom dependency resolver.
     *
     * @return Returns {@code null} if resolver wasn't added.
     */
    public static DependencyResolver dependencyResolver() {
        return dependencyResolver.get();
    }

    /**
     * @param name Grid name (possibly {@code null} for default grid).
     * @return true when all managers, processors, and plugins have started and ignite kernal start method has fully
     * completed.
     */
    public static boolean hasKernalStarted(String name) {
        IgniteNamedInstance grid = name != null ? grids.get(name) : dfltGrid;
        return grid != null && grid.hasStartLatchCompleted();
    }

    /**
     * Start context encapsulates all starting parameters.
     */
    private static final class GridStartContext {
        /** User-defined configuration. */
        private IgniteConfiguration cfg;

        /** Optional configuration path. */
        private URL cfgUrl;

        /** Optional Spring application context. */
        private GridSpringResourceContext springCtx;

        /** Whether or not this is a single grid instance in current VM. */
        private boolean single;

        /**
         *
         * @param cfg User-defined configuration.
         * @param cfgUrl Optional configuration path.
         * @param springCtx Optional Spring application context.
         */
        GridStartContext(IgniteConfiguration cfg, @Nullable URL cfgUrl, @Nullable GridSpringResourceContext springCtx) {
            assert (cfg != null);

            this.cfg = cfg;
            this.cfgUrl = cfgUrl;
            this.springCtx = springCtx;
        }

        /**
         * @return Whether or not this is a single grid instance in current VM.
         */
        public boolean single() {
            return single;
        }

        /**
         * @param single Whether or not this is a single grid instance in current VM.
         */
        public void single(boolean single) {
            this.single = single;
        }

        /**
         * @return User-defined configuration.
         */
        IgniteConfiguration config() {
            return cfg;
        }

        /**
         * @param cfg User-defined configuration.
         */
        void config(IgniteConfiguration cfg) {
            this.cfg = cfg;
        }

        /**
         * @return Optional configuration path.
         */
        URL configUrl() {
            return cfgUrl;
        }

        /**
         * @param cfgUrl Optional configuration path.
         */
        void configUrl(URL cfgUrl) {
            this.cfgUrl = cfgUrl;
        }

        /**
         * @return Optional Spring application context.
         */
        public GridSpringResourceContext springContext() {
            return springCtx;
        }
    }

    /**
     * Grid data container.
     */
    private static final class IgniteNamedInstance {
        /** Map of registered MBeans. */
        private static final Map<MBeanServer, GridMBeanServerData> mbeans =
            new HashMap<>();

        /** Grid name. */
        private final String name;

        /** Grid instance. */
        private volatile IgniteKernal grid;

        /** Grid state. */
        private volatile IgniteState state = STOPPED;

        /** Shutdown hook. */
        private Thread shutdownHook;

        /** Grid log. */
        private IgniteLogger log;

        /** Start guard. */
        private final AtomicBoolean startGuard = new AtomicBoolean();

        /** Start latch. */
        private final CountDownLatch startLatch = new CountDownLatch(1);

        /** Raised if node is waiting graceful shutdown. Set to false to end wait. */
        private volatile boolean delayedShutdown = false;

        /**
         * Thread that starts this named instance. This field can be non-volatile since
         * it makes sense only for thread where it was originally initialized.
         */
        @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
        private Thread starterThread;

        /** */
        private boolean starterThreadInterrupted;

        /**
         * Creates un-started named instance.
         *
         * @param name Grid name (possibly {@code null} for default grid).
         */
        IgniteNamedInstance(@Nullable String name) {
            this.name = name;
        }

        /**
         * Gets grid name.
         *
         * @return Grid name.
         */
        String getName() {
            return name;
        }

        /**
         * Gets grid instance.
         *
         * @return Grid instance.
         */
        IgniteKernal grid() {
            if (starterThread != Thread.currentThread())
                U.awaitQuiet(startLatch);

            return grid;
        }

        /**
         * Gets grid instance without waiting for its initialization.
         *
         * @return Grid instance.
         */
        public IgniteKernal gridx() {
            return grid;
        }

        /**
         * Gets grid state.
         *
         * @return Grid state.
         */
        IgniteState state() {
            if (starterThread != Thread.currentThread())
                U.awaitQuiet(startLatch);

            return state;
        }

        /**
         * @param spi SPI implementation.
         * @throws IgniteCheckedException Thrown in case if multi-instance is not supported.
         */
        private void ensureMultiInstanceSupport(IgniteSpi spi) throws IgniteCheckedException {
            IgniteSpiMultipleInstancesSupport ann = U.getAnnotation(spi.getClass(),
                IgniteSpiMultipleInstancesSupport.class);

            if (ann == null || !ann.value())
                throw new IgniteCheckedException("SPI implementation doesn't support multiple grid instances in " +
                    "the same VM: " + spi);
        }

        /**
         * @param spis SPI implementations.
         * @throws IgniteCheckedException Thrown in case if multi-instance is not supported.
         */
        private void ensureMultiInstanceSupport(IgniteSpi[] spis) throws IgniteCheckedException {
            for (IgniteSpi spi : spis)
                ensureMultiInstanceSupport(spi);
        }

        /**
         * Starts grid with given configuration.
         *
         * @param startCtx Starting context.
         * @throws IgniteCheckedException If start failed.
         */
        synchronized void start(GridStartContext startCtx) throws IgniteCheckedException {
            if (startGuard.compareAndSet(false, true)) {
                try {
                    starterThread = Thread.currentThread();

                    IgniteConfiguration myCfg = initializeConfiguration(
                        startCtx.config() != null ? startCtx.config() : new IgniteConfiguration()
                    );

                    TimeBag startNodeTimer = new TimeBag(TimeUnit.MILLISECONDS, log.isInfoEnabled());

                    start0(startCtx, myCfg, startNodeTimer);

                    if (log.isInfoEnabled())
                        log.info("Node started : "
                            + startNodeTimer.stagesTimings().stream().collect(joining(",", "[", "]")));
                }
                finally {
                    startLatch.countDown();
                }
            }
            else
                U.awaitQuiet(startLatch);
        }

        /**
         * @param startCtx Starting context.
         * @throws IgniteCheckedException If start failed.
         */
        private void start0(GridStartContext startCtx, IgniteConfiguration cfg, TimeBag startTimer)
            throws IgniteCheckedException {
            assert grid == null : "Grid is already started: " + name;

            // Set configuration URL, if any, into system property.
            if (startCtx.configUrl() != null)
                System.setProperty(IGNITE_CONFIG_URL, startCtx.configUrl().toString());

            // Ensure that SPIs support multiple grid instances, if required.
            if (!startCtx.single()) {
                ensureMultiInstanceSupport(cfg.getDeploymentSpi());
                ensureMultiInstanceSupport(cfg.getCommunicationSpi());
                ensureMultiInstanceSupport(cfg.getDiscoverySpi());
                ensureMultiInstanceSupport(cfg.getCheckpointSpi());
                ensureMultiInstanceSupport(cfg.getEventStorageSpi());
                ensureMultiInstanceSupport(cfg.getCollisionSpi());
                ensureMultiInstanceSupport(cfg.getFailoverSpi());
                ensureMultiInstanceSupport(cfg.getLoadBalancingSpi());
            }

            UncaughtExceptionHandler oomeHnd = new UncaughtExceptionHandler() {
                @Override public void uncaughtException(Thread t, Throwable e) {
                    if (grid != null && X.hasCause(e, OutOfMemoryError.class))
                        grid.context().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
                }
            };

            WorkersRegistry workerRegistry = new WorkersRegistry(
                new IgniteBiInClosure<GridWorker, FailureType>() {
                    @Override public void apply(GridWorker worker, FailureType failureType) {
                        IgniteException ex = new IgniteException(S.toString(GridWorker.class, worker));

                        Thread runner = worker.runner();

                        if (runner != null && runner != Thread.currentThread())
                            ex.setStackTrace(runner.getStackTrace());

                        if (grid != null)
                            grid.context().failure().process(new FailureContext(failureType, ex));
                    }
                },
                IgniteSystemProperties.getLong(IGNITE_SYSTEM_WORKER_BLOCKED_TIMEOUT,
                    cfg.getSystemWorkerBlockedTimeout() != null
                    ? cfg.getSystemWorkerBlockedTimeout()
                    : cfg.getFailureDetectionTimeout()),
                log);

            // Register Ignite MBean for current grid instance.
            registerFactoryMbean(cfg.getMBeanServer());

            boolean started = false;

            try {
                IgniteKernal grid0 = new IgniteKernal(startCtx.springContext());

                // Init here to make grid available to lifecycle listeners.
                grid = grid0;

                startTimer.finishGlobalStage("Configure system pool");

                grid0.start(
                    cfg,
                    new CA() {
                        @Override public void apply() {
                            startLatch.countDown();
                        }
                    },
                    workerRegistry,
                    oomeHnd,
                    startTimer
                );

                state = STARTED;

                if (log.isDebugEnabled())
                    log.debug("Grid factory started ok: " + name);

                started = true;
            }
            catch (IgniteCheckedException e) {
                unregisterFactoryMBean();

                throw e;
            }
            // Catch Throwable to protect against any possible failure.
            catch (Throwable e) {
                unregisterFactoryMBean();

                if (e instanceof Error)
                    throw e;

                throw new IgniteCheckedException("Unexpected exception when starting grid.", e);
            }
            finally {
                if (!started)
                    // Grid was not started.
                    grid = null;
            }

            // Do NOT set it up only if IGNITE_NO_SHUTDOWN_HOOK=TRUE is provided.
            if (!IgniteSystemProperties.getBoolean(IGNITE_NO_SHUTDOWN_HOOK, false)) {
                try {
                    Runtime.getRuntime().addShutdownHook(shutdownHook = new Thread("shutdown-hook") {
                        @Override public void run() {
                            if (log.isInfoEnabled())
                                log.info("Invoking shutdown hook...");

                            IgniteNamedInstance ignite = IgniteNamedInstance.this;

                            ignite.stop(true, null);
                        }
                    });

                    if (log.isDebugEnabled())
                        log.debug("Shutdown hook is installed.");
                }
                catch (IllegalStateException e) {
                    stop(true, ShutdownPolicy.IMMEDIATE);

                    throw new IgniteCheckedException("Failed to install shutdown hook.", e);
                }
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Shutdown hook has not been installed because environment " +
                        "or system property " + IGNITE_NO_SHUTDOWN_HOOK + " is set.");
            }
        }

        /**
         * @param cfg Ignite configuration copy to.
         * @return New ignite configuration.
         * @throws IgniteCheckedException If failed.
         */
        private IgniteConfiguration initializeConfiguration(IgniteConfiguration cfg)
            throws IgniteCheckedException {
            BinaryArray.initUseBinaryArrays();

            IgniteConfiguration myCfg = new IgniteConfiguration(cfg);

            String ggHome = cfg.getIgniteHome();

            // Set Ignite home.
            if (ggHome == null)
                ggHome = U.getIgniteHome();
            else
                // If user provided IGNITE_HOME - set it as a system property.
                U.setIgniteHome(ggHome);

            String userProvidedWorkDir = cfg.getWorkDirectory();

            // Correctly resolve work directory and set it back to configuration.
            String workDir = U.workDirectory(userProvidedWorkDir, ggHome);

            myCfg.setWorkDirectory(workDir);

            // Ensure invariant.
            // It's a bit dirty - but this is a result of late refactoring
            // and I don't want to reshuffle a lot of code.
            assert F.eq(name, cfg.getIgniteInstanceName());

            UUID nodeId = cfg.getNodeId() != null ? cfg.getNodeId() : UUID.randomUUID();

            myCfg.setNodeId(nodeId);

            String predefineConsistentId = IgniteSystemProperties.getString(IGNITE_OVERRIDE_CONSISTENT_ID);

            if (!F.isEmpty(predefineConsistentId))
                myCfg.setConsistentId(predefineConsistentId);

            IgniteLogger cfgLog = U.initLogger(cfg.getGridLogger(), null, nodeId, workDir);

            assert cfgLog != null;

            cfgLog = new GridLoggerProxy(cfgLog, null, name, U.id8(nodeId));

            // Initialize factory's log.
            log = cfgLog.getLogger(G.class);

            myCfg.setGridLogger(cfgLog);

            if (F.isEmpty(userProvidedWorkDir) && F.isEmpty(U.IGNITE_WORK_DIR))
                log.warning("Ignite work directory is not provided, automatically resolved to: " + workDir);

            // Check Ignite home folder (after log is available).
            if (ggHome != null) {
                File ggHomeFile = new File(ggHome);

                if (!ggHomeFile.exists() || !ggHomeFile.isDirectory())
                    throw new IgniteCheckedException("Invalid Ignite installation home folder: " + ggHome);
            }

            myCfg.setIgniteHome(ggHome);

            // Validate segmentation configuration.
            SegmentationPolicy segPlc = cfg.getSegmentationPolicy();

            // 1. Warn on potential configuration problem: grid is not configured to wait
            // for correct segment after segmentation happens.
            if (!F.isEmpty(cfg.getSegmentationResolvers()) && segPlc == RESTART_JVM && !cfg.isWaitForSegmentOnStart()) {
                U.warn(log, "Found potential configuration problem (forgot to enable waiting for segment" +
                    "on start?) [segPlc=" + segPlc + ", wait=false]");
            }

            if (CU.isPersistenceEnabled(cfg) && myCfg.getConsistentId() == null)
                U.warn(log, "Consistent ID is not set, it is recommended to set consistent ID for production " +
                    "clusters (use IgniteConfiguration.setConsistentId property)");

            myCfg.setTransactionConfiguration(myCfg.getTransactionConfiguration() != null ?
                new TransactionConfiguration(myCfg.getTransactionConfiguration()) : null);

            myCfg.setConnectorConfiguration(myCfg.getConnectorConfiguration() != null ?
                new ConnectorConfiguration(myCfg.getConnectorConfiguration()) : null);

            // Local host.
            String locHost = IgniteSystemProperties.getString(IGNITE_LOCAL_HOST);

            myCfg.setLocalHost(F.isEmpty(locHost) ? myCfg.getLocalHost() : locHost);

            if (myCfg.isClientMode() == null) {
                Boolean threadClient = clientMode.get();

                if (threadClient == null)
                    myCfg.setClientMode(IgniteSystemProperties.getBoolean(IGNITE_CACHE_CLIENT, false));
                else
                    myCfg.setClientMode(threadClient);
            }

            // Check for deployment mode override.
            String depModeName = IgniteSystemProperties.getString(IGNITE_DEP_MODE_OVERRIDE);

            if (!F.isEmpty(depModeName)) {
                if (!F.isEmpty(myCfg.getCacheConfiguration())) {
                    U.quietAndInfo(log, "Skipping deployment mode override for caches (custom closure " +
                        "execution may not work for console Visor)");
                }
                else {
                    try {
                        DeploymentMode depMode = DeploymentMode.valueOf(depModeName);

                        if (myCfg.getDeploymentMode() != depMode)
                            myCfg.setDeploymentMode(depMode);
                    }
                    catch (IllegalArgumentException e) {
                        throw new IgniteCheckedException("Failed to override deployment mode using system property " +
                            "(are there any misspellings?)" +
                            "[name=" + IGNITE_DEP_MODE_OVERRIDE + ", value=" + depModeName + ']', e);
                    }
                }
            }

            if (myCfg.getUserAttributes() == null)
                myCfg.setUserAttributes(Collections.<String, Object>emptyMap());

            initializeDefaultMBeanServer(myCfg);

            Marshaller marsh = myCfg.getMarshaller();

            if (marsh == null) {
                if (!BinaryMarshaller.available()) {
                    U.warn(log, "Standard BinaryMarshaller can't be used on this JVM. " +
                        "Switch to HotSpot JVM or reach out Apache Ignite community for recommendations.");

                    marsh = new JdkMarshaller();
                }
                else
                    marsh = new BinaryMarshaller();
            }

            MarshallerUtils.setNodeName(marsh, cfg.getIgniteInstanceName());

            myCfg.setMarshaller(marsh);

            if (myCfg.getPeerClassLoadingLocalClassPathExclude() == null)
                myCfg.setPeerClassLoadingLocalClassPathExclude(EMPTY_STRS);

            initializeDefaultSpi(myCfg);

            GridDiscoveryManager.initCommunicationErrorResolveConfiguration(myCfg);

            initializeDefaultCacheConfiguration(myCfg);

            ExecutorConfiguration[] execCfgs = myCfg.getExecutorConfiguration();

            if (execCfgs != null) {
                ExecutorConfiguration[] clone = execCfgs.clone();

                for (int i = 0; i < execCfgs.length; i++)
                    clone[i] = new ExecutorConfiguration(execCfgs[i]);

                myCfg.setExecutorConfiguration(clone);
            }

            initializeDataStorageConfiguration(myCfg);

            return myCfg;
        }

        /**
         * @param cfg Ignite configuration.
         */
        private void initializeDataStorageConfiguration(IgniteConfiguration cfg) throws IgniteCheckedException {
            if (cfg.getDataStorageConfiguration() != null &&
                (cfg.getMemoryConfiguration() != null || cfg.getPersistentStoreConfiguration() != null)) {
                throw new IgniteCheckedException("Data storage can be configured with either legacy " +
                    "(MemoryConfiguration, PersistentStoreConfiguration) or new (DataStorageConfiguration) classes, " +
                    "but not both.");
            }

            if (cfg.getMemoryConfiguration() != null || cfg.getPersistentStoreConfiguration() != null)
                convertLegacyDataStorageConfigurationToNew(cfg);

            if (!cfg.isClientMode() && cfg.getDataStorageConfiguration() == null)
                cfg.setDataStorageConfiguration(new DataStorageConfiguration());
        }

        /**
         * Initialize default cache configuration.
         *
         * @param cfg Ignite configuration.
         * @throws IgniteCheckedException If failed.
         */
        public void initializeDefaultCacheConfiguration(IgniteConfiguration cfg) throws IgniteCheckedException {
            List<CacheConfiguration> cacheCfgs = new ArrayList<>();

            cacheCfgs.add(utilitySystemCache());

            CacheConfiguration[] userCaches = cfg.getCacheConfiguration();

            if (userCaches != null && userCaches.length > 0) {
                if (!U.discoOrdered(cfg.getDiscoverySpi()) && !U.relaxDiscoveryOrdered())
                    throw new IgniteCheckedException("Discovery SPI implementation does not support node ordering and " +
                        "cannot be used with cache (use SPI with @DiscoverySpiOrderSupport annotation, " +
                        "like TcpDiscoverySpi)");

                for (CacheConfiguration ccfg : userCaches) {
                    if (CU.isReservedCacheName(ccfg.getName()))
                        throw new IgniteCheckedException("Cache name cannot be \"" + ccfg.getName() +
                            "\" because it is reserved for internal purposes.");

                    if (DataStructuresProcessor.isDataStructureCache(ccfg.getName()))
                        throw new IgniteCheckedException("Cache name cannot be \"" + ccfg.getName() +
                            "\" because it is reserved for data structures.");

                    cacheCfgs.add(ccfg);
                }
            }

            cfg.setCacheConfiguration(cacheCfgs.toArray(new CacheConfiguration[cacheCfgs.size()]));

            assert cfg.getCacheConfiguration() != null;
        }

        /**
         * Initialize default SPI implementations.
         *
         * @param cfg Ignite configuration.
         */
        private void initializeDefaultSpi(IgniteConfiguration cfg) {
            if (cfg.getDiscoverySpi() == null)
                cfg.setDiscoverySpi(new TcpDiscoverySpi());

            if (cfg.getDiscoverySpi() instanceof TcpDiscoverySpi) {
                TcpDiscoverySpi tcpDisco = (TcpDiscoverySpi)cfg.getDiscoverySpi();

                if (tcpDisco.getIpFinder() == null)
                    tcpDisco.setIpFinder(new TcpDiscoveryMulticastIpFinder());
            }

            if (cfg.getCommunicationSpi() == null)
                cfg.setCommunicationSpi(new TcpCommunicationSpi());

            if (cfg.getDeploymentSpi() == null)
                cfg.setDeploymentSpi(new LocalDeploymentSpi());

            if (cfg.getEventStorageSpi() == null)
                cfg.setEventStorageSpi(new NoopEventStorageSpi());

            if (cfg.getCheckpointSpi() == null)
                cfg.setCheckpointSpi(new NoopCheckpointSpi());

            if (cfg.getCollisionSpi() == null)
                cfg.setCollisionSpi(new NoopCollisionSpi());

            if (cfg.getFailoverSpi() == null)
                cfg.setFailoverSpi(new AlwaysFailoverSpi());

            if (cfg.getLoadBalancingSpi() == null)
                cfg.setLoadBalancingSpi(new RoundRobinLoadBalancingSpi());
            else {
                Collection<LoadBalancingSpi> spis = new ArrayList<>();

                boolean dfltLoadBalancingSpi = false;

                for (LoadBalancingSpi spi : cfg.getLoadBalancingSpi()) {
                    spis.add(spi);

                    if (!dfltLoadBalancingSpi && spi instanceof RoundRobinLoadBalancingSpi)
                        dfltLoadBalancingSpi = true;
                }

                // Add default load balancing SPI for internal tasks.
                if (!dfltLoadBalancingSpi)
                    spis.add(new RoundRobinLoadBalancingSpi());

                cfg.setLoadBalancingSpi(spis.toArray(new LoadBalancingSpi[spis.size()]));
            }

            if (cfg.getIndexingSpi() == null)
                cfg.setIndexingSpi(new NoopIndexingSpi());

            if (cfg.getEncryptionSpi() == null)
                cfg.setEncryptionSpi(new NoopEncryptionSpi());

            if (F.isEmpty(cfg.getMetricExporterSpi())) {
                cfg.setMetricExporterSpi(IGNITE_MBEANS_DISABLED
                    ? new NoopMetricExporterSpi()
                    : new JmxMetricExporterSpi());
            }

            if (cfg.getTracingSpi() == null)
                cfg.setTracingSpi(new NoopTracingSpi());
        }

        /**
         * Creates utility system cache configuration.
         *
         * @return Utility system cache configuration.
         */
        private static CacheConfiguration utilitySystemCache() {
            CacheConfiguration cache = new CacheConfiguration();

            cache.setName(CU.UTILITY_CACHE_NAME);
            cache.setCacheMode(REPLICATED);
            cache.setAtomicityMode(TRANSACTIONAL);
            cache.setRebalanceMode(SYNC);
            cache.setWriteSynchronizationMode(FULL_SYNC);
            cache.setAffinity(new RendezvousAffinityFunction(false, 100));
            cache.setNodeFilter(CacheConfiguration.ALL_NODES);
            cache.setRebalanceOrder(-2); //Prior to user caches.
            cache.setCopyOnRead(false);

            return cache;
        }

        /**
         * Stops grid.
         *
         * @param cancel Flag indicating whether all currently running jobs
         *      should be cancelled.
         * @param shutdown This is a policy of shutdown which is applied forcibly.
         * If this property is {@code null}, present policy of the cluster will be used.
         */
        void stop(boolean cancel, ShutdownPolicy shutdown) {
            // Stop cannot be called prior to start from public API,
            // since it checks for STARTED state. So, we can assert here.
            assert startGuard.get();

            if (shutdown == null)
                shutdown = determineShutdownPolicy();

            // If waiting for backups due to earlier invocation of stop(), stop wait and proceed shutting down.
            if (shutdown == ShutdownPolicy.IMMEDIATE)
                delayedShutdown = false;

            stop0(cancel, shutdown);
        }

        /**
         * Reads a policy from distributed meta storage or returns a default value if it isn't present.
         *
         * @return Shutdown policy.
         */
        private ShutdownPolicy determineShutdownPolicy() {
            if (IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_WAIT_FOR_BACKUPS_ON_SHUTDOWN))
                return ShutdownPolicy.GRACEFUL;

            return grid.cluster().shutdownPolicy();
        }

        /**
         * Stops instance synchronously according to parameters.
         *
         * @param cancel Flag indicating whether all currently running job
         *      should be cancelled.
         * @param shutdown Policy according to which shutdown will be performed.
         */
        private synchronized void stop0(boolean cancel, ShutdownPolicy shutdown) {
            IgniteKernal grid0 = grid;

            // Double check.
            if (grid0 == null) {
                if (log != null)
                    U.warn(log, "Attempting to stop an already stopped Ignite instance (ignore): " + name);

                return;
            }

            if (shutdownHook != null) {
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);

                    shutdownHook = null;

                    if (log != null && log.isDebugEnabled())
                        log.debug("Shutdown hook is removed.");
                }
                catch (IllegalStateException e) {
                    // Shutdown is in progress...
                    if (log != null && log.isDebugEnabled())
                        log.debug("Shutdown is in progress (ignoring): " + e.getMessage());
                }
            }

            if (shutdown == ShutdownPolicy.GRACEFUL && !grid.context().clientNode() && grid.cluster().state().active()) {
                delayedShutdown = true;

                if (log.isInfoEnabled())
                    log.info("Ensuring that caches have sufficient backups and local rebalance completion...");

                DistributedMetaStorage metaStorage = grid.context().distributedMetastorage();

                while (delayedShutdown) {
                    boolean safeToStop = true;

                    long topVer = grid.cluster().topologyVersion();

                    HashSet<UUID> originalNodesToExclude;

                    HashSet<UUID> nodesToExclude;

                    try {
                        originalNodesToExclude = metaStorage.read(GRACEFUL_SHUTDOWN_METASTORE_KEY);

                        nodesToExclude = originalNodesToExclude != null ? new HashSet<>(originalNodesToExclude) :
                            new HashSet<>();
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Unable to read " + GRACEFUL_SHUTDOWN_METASTORE_KEY +
                            " value from metastore.", e);

                        continue;
                    }

                    Map<UUID, Map<Integer, Set<Integer>>> proposedSuppliers = new HashMap<>();

                    for (CacheGroupContext grpCtx : grid.context().cache().cacheGroups()) {
                        if (grpCtx.systemCache())
                            continue;

                        if (grpCtx.config().getCacheMode() == PARTITIONED && grpCtx.config().getBackups() == 0) {
                            LT.warn(log, "Ignoring potential data loss on cache without backups [name="
                                + grpCtx.cacheOrGroupName() + "]");

                            continue;
                        }

                        if (topVer != grpCtx.topology().readyTopologyVersion().topologyVersion()) {
                            // At the moment, there is an exchange.
                            safeToStop = false;

                            break;
                        }

                        GridDhtPartitionFullMap fullMap = grpCtx.topology().partitionMap(false);

                        if (fullMap == null) {
                            safeToStop = false;

                            break;
                        }

                        nodesToExclude.retainAll(fullMap.keySet());

                        if (!haveCopyLocalPartitions(grpCtx, nodesToExclude, proposedSuppliers)) {
                            safeToStop = false;

                            if (log.isInfoEnabled()) {
                                LT.info(log, "This node is waiting for backups of local partitions for group [id="
                                    + grpCtx.groupId() + ", name=" + grpCtx.cacheOrGroupName() + "]");
                            }

                            break;
                        }

                        if (!isRebalanceCompleted(grpCtx)) {
                            safeToStop = false;

                            if (log.isInfoEnabled()) {
                                LT.info(log, "This node is waiting for completion of rebalance for group [id="
                                    + grpCtx.groupId() + ", name=" + grpCtx.cacheOrGroupName() + "]");
                            }

                            break;
                        }
                    }

                    if (topVer != grid.cluster().topologyVersion())
                        safeToStop = false;

                    if (safeToStop && !proposedSuppliers.isEmpty()) {
                        Set<UUID> supportedPolicyNodes = proposedSuppliers.keySet().stream()
                            .filter(nodeId ->
                                IgniteFeatures.nodeSupports(grid0.cluster().node(nodeId), IgniteFeatures.SHUTDOWN_POLICY))
                            .collect(Collectors.toSet());

                        if (!supportedPolicyNodes.isEmpty()) {
                            try {
                                safeToStop = grid0.context().task().execute(
                                    CheckCpHistTask.class,
                                    proposedSuppliers,
                                    options(grid0.cluster().forNodeIds(supportedPolicyNodes).nodes())
                                ).get();
                            }
                            catch (IgniteCheckedException e) {
                                U.error(log, "Failed to check availability of historical rebalance", e);

                                safeToStop = false;
                            }
                        }
                    }

                    if (safeToStop) {
                        try {
                            HashSet<UUID> newNodesToExclude = new HashSet<>(nodesToExclude);
                            newNodesToExclude.add(grid.localNodeId());

                            if (metaStorage.compareAndSet(GRACEFUL_SHUTDOWN_METASTORE_KEY, originalNodesToExclude,
                                newNodesToExclude))
                                break;
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Unable to write " + GRACEFUL_SHUTDOWN_METASTORE_KEY +
                                " value from metastore.", e);

                            continue;
                        }
                    }

                    try {
                        IgniteUtils.sleep(WAIT_FOR_BACKUPS_CHECK_INTERVAL);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

            // Unregister Ignite MBean.
            unregisterFactoryMBean();

            try {
                grid0.stop(cancel);

                if (log != null && log.isDebugEnabled())
                    log.debug("Ignite instance stopped ok: " + name);
            }
            catch (Throwable e) {
                U.error(log, "Failed to properly stop grid instance due to undeclared exception.", e);

                if (e instanceof Error)
                    throw e;
            }
            finally {
                if (grid0.context().segmented())
                    state = STOPPED_ON_SEGMENTATION;
                else if (grid0.context().invalid())
                    state = STOPPED_ON_FAILURE;
                else
                    state = STOPPED;

                grid = null;

                log = null;
            }
        }

        /**
         * Checks, does the cluster have another copy of each local partition for specific group.
         * Also, the method collects all nodes with can supply a local partition into {@code proposedSuppliers}.
         *
         * @param grpCtx Cahce group.
         * @param nodesToExclude Nodes to exclude from check.
         * @param proposedSuppliers Map of proposed suppliers for groups.
         * @return True if all local partition of group specified have a copy in cluster, false otherwise.
         */
        private boolean haveCopyLocalPartitions(
            CacheGroupContext grpCtx,
            Set<UUID> nodesToExclude,
            Map<UUID, Map<Integer, Set<Integer>>> proposedSuppliers
        ) {
            GridDhtPartitionFullMap fullMap = grpCtx.topology().partitionMap(false);

            if (fullMap == null)
                return false;

            UUID localNodeId = grid.localNodeId();

            GridDhtPartitionMap localPartMap = fullMap.get(localNodeId);

            int parts = grpCtx.topology().partitions();

            List<List<ClusterNode>> idealAssignment = grpCtx.affinity().idealAssignmentRaw();

            for (int p = 0; p < parts; p++) {
                if (localPartMap.get(p) != GridDhtPartitionState.OWNING)
                    continue;

                boolean foundCopy = false;

                for (Map.Entry<UUID, GridDhtPartitionMap> entry : fullMap.entrySet()) {
                    if (localNodeId.equals(entry.getKey()) || nodesToExclude.contains(entry.getKey()))
                        continue;

                    //This remote node does not present in ideal assignment.
                    if (!idealAssignment.get(p).stream().anyMatch(node -> node.id().equals(entry.getKey())))
                        continue;

                    //Rebalance in this cache.
                    if (entry.getValue().hasMovingPartitions())
                        continue;

                    if (entry.getValue().get(p) == GridDhtPartitionState.OWNING) {
                        proposedSuppliers.computeIfAbsent(entry.getKey(), (nodeId) -> new HashMap<>())
                            .computeIfAbsent(grpCtx.groupId(), grpId -> new HashSet<>())
                            .add(p);

                        foundCopy = true;
                    }
                }

                if (!foundCopy)
                    return false;
            }

            return true;
        }

        /**
         * Check is rebalance completed for specific group on this node or not.
         * It checks Demander and Supplier contexts.
         *
         * @param grpCtx Group context.
         * @return True if rebalance completed, false otherwise.
         */
        private boolean isRebalanceCompleted(CacheGroupContext grpCtx) {
            if (!grpCtx.preloader().rebalanceFuture().isDone())
                return false;

            grpCtx.preloader().pause();

            try {
                return !((GridDhtPreloader)grpCtx.preloader()).supplier().isSupply();
            }
            finally {
                grpCtx.preloader().resume();
            }
        }

        /**
         * Registers delegate Mbean instance for {@link Ignition}.
         *
         * @param srv MBeanServer where mbean should be registered.
         * @throws IgniteCheckedException If registration failed.
         */
        private void registerFactoryMbean(MBeanServer srv) throws IgniteCheckedException {
            if (IGNITE_MBEANS_DISABLED)
                return;

            assert srv != null;

            synchronized (mbeans) {
                GridMBeanServerData data = mbeans.get(srv);

                if (data == null) {
                    try {
                        IgnitionMXBean mbean = new IgnitionMXBeanAdapter();

                        ObjectName objName = U.makeMBeanName(
                            null,
                            "Kernal",
                            Ignition.class.getSimpleName()
                        );

                        // Make check if MBean was already registered.
                        if (!srv.queryMBeans(objName, null).isEmpty())
                            throw new IgniteCheckedException("MBean was already registered: " + objName);
                        else {
                            objName = U.registerMBean(
                                srv,
                                null,
                                "Kernal",
                                Ignition.class.getSimpleName(),
                                mbean,
                                IgnitionMXBean.class
                            );

                            data = new GridMBeanServerData(objName);

                            mbeans.put(srv, data);

                            if (log.isDebugEnabled())
                                log.debug("Registered MBean: " + objName);
                        }
                    }
                    catch (JMException e) {
                        throw new IgniteCheckedException("Failed to register MBean.", e);
                    }
                }

                assert data != null;

                data.addIgniteInstance(name);
                data.setCounter(data.getCounter() + 1);
            }
        }

        /**
         * Unregister delegate Mbean instance for {@link Ignition}.
         */
        private void unregisterFactoryMBean() {
            if (IGNITE_MBEANS_DISABLED)
                return;

            synchronized (mbeans) {
                Iterator<Entry<MBeanServer, GridMBeanServerData>> iter = mbeans.entrySet().iterator();

                while (iter.hasNext()) {
                    Entry<MBeanServer, GridMBeanServerData> entry = iter.next();

                    if (entry.getValue().containsIgniteInstance(name)) {
                        GridMBeanServerData data = entry.getValue();

                        assert data != null;

                        // Unregister MBean if no grid instances started for current MBeanServer.
                        if (data.getCounter() == 1) {
                            try {
                                entry.getKey().unregisterMBean(data.getMbean());

                                if (log.isDebugEnabled())
                                    log.debug("Unregistered MBean: " + data.getMbean());
                            }
                            catch (JMException e) {
                                U.error(log, "Failed to unregister MBean.", e);
                            }

                            iter.remove();
                        }
                        else {
                            // Decrement counter.
                            data.setCounter(data.getCounter() - 1);
                            data.removeIgniteInstance(name);
                        }
                    }
                }
            }
        }

        /**
         * Grid factory MBean data container.
         * Contains necessary data for selected MBeanServer.
         */
        private static class GridMBeanServerData {
            /** Set of grid names for selected MBeanServer. */
            private Collection<String> igniteInstanceNames = new HashSet<>();

            /** */
            private ObjectName mbean;

            /** Count of grid instances. */
            private int cnt;

            /**
             * Create data container.
             *
             * @param mbean Object name of MBean.
             */
            GridMBeanServerData(ObjectName mbean) {
                assert mbean != null;

                this.mbean = mbean;
            }

            /**
             * Add Ignite instance name.
             *
             * @param igniteInstanceName Ignite instance name.
             */
            public void addIgniteInstance(String igniteInstanceName) {
                igniteInstanceNames.add(igniteInstanceName);
            }

            /**
             * Remove Ignite instance name.
             *
             * @param igniteInstanceName Ignite instance name.
             */
            public void removeIgniteInstance(String igniteInstanceName) {
                igniteInstanceNames.remove(igniteInstanceName);
            }

            /**
             * Returns {@code true} if data contains the specified
             * Ignite instance name.
             *
             * @param igniteInstanceName Ignite instance name.
             * @return {@code true} if data contains the specified Ignite instance name.
             */
            public boolean containsIgniteInstance(String igniteInstanceName) {
                return igniteInstanceNames.contains(igniteInstanceName);
            }

            /**
             * Gets name used in MBean server.
             *
             * @return Object name of MBean.
             */
            public ObjectName getMbean() {
                return mbean;
            }

            /**
             * Gets number of grid instances working with MBeanServer.
             *
             * @return Number of grid instances.
             */
            public int getCounter() {
                return cnt;
            }

            /**
             * Sets number of grid instances working with MBeanServer.
             *
             * @param cnt Number of grid instances.
             */
            public void setCounter(int cnt) {
                this.cnt = cnt;
            }
        }

        /**
         * @return whether the startLatch has been counted down, thereby indicating that the kernal has full started.
         */
        public boolean hasStartLatchCompleted() {
            return startLatch.getCount() == 0;
        }
    }

    /** Initialize default mbean server. */
    public static void initializeDefaultMBeanServer(IgniteConfiguration myCfg) {
        if (myCfg.getMBeanServer() == null && !IGNITE_MBEANS_DISABLED)
            myCfg.setMBeanServer(ManagementFactory.getPlatformMBeanServer());
    }

    /**
     * @param cfg Ignite Configuration with legacy data storage configuration.
     */
    private static void convertLegacyDataStorageConfigurationToNew(
        IgniteConfiguration cfg) throws IgniteCheckedException {
        PersistentStoreConfiguration psCfg = cfg.getPersistentStoreConfiguration();

        boolean persistenceEnabled = psCfg != null;

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        MemoryConfiguration memCfg = cfg.getMemoryConfiguration() != null ?
            cfg.getMemoryConfiguration() : new MemoryConfiguration();

        dsCfg.setConcurrencyLevel(memCfg.getConcurrencyLevel());
        dsCfg.setPageSize(memCfg.getPageSize());

        dsCfg.setSystemDataRegionConfiguration(
                new SystemDataRegionConfiguration()
                        .setInitialSize(memCfg.getSystemCacheInitialSize())
                        .setMaxSize(memCfg.getSystemCacheMaxSize())
        );

        List<DataRegionConfiguration> optionalDataRegions = new ArrayList<>();

        boolean customDfltPlc = false;

        if (memCfg.getMemoryPolicies() != null) {
            for (MemoryPolicyConfiguration mpc : memCfg.getMemoryPolicies()) {
                DataRegionConfiguration region = new DataRegionConfiguration();

                region.setPersistenceEnabled(persistenceEnabled);

                if (mpc.getInitialSize() != 0L)
                    region.setInitialSize(mpc.getInitialSize());

                region.setEmptyPagesPoolSize(mpc.getEmptyPagesPoolSize());
                region.setEvictionThreshold(mpc.getEvictionThreshold());
                region.setMaxSize(mpc.getMaxSize());
                region.setName(mpc.getName());
                region.setPageEvictionMode(mpc.getPageEvictionMode());
                region.setMetricsRateTimeInterval(mpc.getRateTimeInterval());
                region.setMetricsSubIntervalCount(mpc.getSubIntervals());
                region.setSwapPath(mpc.getSwapFilePath());
                region.setMetricsEnabled(mpc.isMetricsEnabled());

                if (persistenceEnabled)
                    region.setCheckpointPageBufferSize(psCfg.getCheckpointingPageBufferSize());

                if (mpc.getName() == null) {
                    throw new IgniteCheckedException(new IllegalArgumentException(
                        "User-defined MemoryPolicyConfiguration must have non-null and non-empty name."));
                }

                if (mpc.getName().equals(memCfg.getDefaultMemoryPolicyName())) {
                    customDfltPlc = true;

                    dsCfg.setDefaultDataRegionConfiguration(region);
                }
                else
                    optionalDataRegions.add(region);
            }
        }

        if (!optionalDataRegions.isEmpty())
            dsCfg.setDataRegionConfigurations(optionalDataRegions.toArray(
                new DataRegionConfiguration[optionalDataRegions.size()]));

        if (!customDfltPlc) {
            if (!DFLT_MEM_PLC_DEFAULT_NAME.equals(memCfg.getDefaultMemoryPolicyName())) {
                throw new IgniteCheckedException(new IllegalArgumentException("User-defined default MemoryPolicy " +
                    "name must be presented among configured MemoryPolices: " + memCfg.getDefaultMemoryPolicyName()));
            }

            dsCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(memCfg.getDefaultMemoryPolicySize())
                .setName(memCfg.getDefaultMemoryPolicyName())
                .setPersistenceEnabled(persistenceEnabled));
        }
        else {
            if (memCfg.getDefaultMemoryPolicySize() != DFLT_MEMORY_POLICY_MAX_SIZE)
                throw new IgniteCheckedException(new IllegalArgumentException("User-defined MemoryPolicy " +
                    "configuration and defaultMemoryPolicySize properties are set at the same time."));
        }

        if (persistenceEnabled) {
            dsCfg.setCheckpointFrequency(psCfg.getCheckpointingFrequency());
            dsCfg.setCheckpointThreads(psCfg.getCheckpointingThreads());
            dsCfg.setCheckpointWriteOrder(psCfg.getCheckpointWriteOrder());
            dsCfg.setFileIOFactory(psCfg.getFileIOFactory());
            dsCfg.setLockWaitTime(psCfg.getLockWaitTime());
            dsCfg.setStoragePath(psCfg.getPersistentStorePath());
            dsCfg.setMetricsRateTimeInterval(psCfg.getRateTimeInterval());
            dsCfg.setMetricsSubIntervalCount(psCfg.getSubIntervals());
            dsCfg.setWalThreadLocalBufferSize(psCfg.getTlbSize());
            dsCfg.setWalArchivePath(psCfg.getWalArchivePath());
            dsCfg.setWalAutoArchiveAfterInactivity(psCfg.getWalAutoArchiveAfterInactivity());
            dsCfg.setWalFlushFrequency(psCfg.getWalFlushFrequency());
            dsCfg.setWalFsyncDelayNanos(psCfg.getWalFsyncDelayNanos());
            dsCfg.setWalHistorySize(psCfg.getWalHistorySize());
            dsCfg.setWalMode(psCfg.getWalMode());
            dsCfg.setWalRecordIteratorBufferSize(psCfg.getWalRecordIteratorBufferSize());
            dsCfg.setWalSegments(psCfg.getWalSegments());
            dsCfg.setWalSegmentSize(psCfg.getWalSegmentSize());
            dsCfg.setWalPath(psCfg.getWalStorePath());
            dsCfg.setAlwaysWriteFullPages(psCfg.isAlwaysWriteFullPages());
            dsCfg.setMetricsEnabled(psCfg.isMetricsEnabled());
            dsCfg.setWriteThrottlingEnabled(psCfg.isWriteThrottlingEnabled());
        }

        cfg.setDataStorageConfiguration(dsCfg);
    }
}
