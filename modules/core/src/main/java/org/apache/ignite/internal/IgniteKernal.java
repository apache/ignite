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

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Constructor;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.management.JMException;
import javax.management.ObjectName;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteScheduler;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.binary.BinaryEnumCache;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.cluster.ClusterGroupAdapter;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.managers.GridManager;
import org.apache.ignite.internal.managers.checkpoint.GridCheckpointManager;
import org.apache.ignite.internal.managers.collision.GridCollisionManager;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.deployment.GridDeploymentManager;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.managers.failover.GridFailoverManager;
import org.apache.ignite.internal.managers.indexing.GridIndexingManager;
import org.apache.ignite.internal.managers.loadbalancer.GridLoadBalancerManager;
import org.apache.ignite.internal.managers.swapspace.GridSwapSpaceManager;
import org.apache.ignite.internal.processors.GridProcessor;
import org.apache.ignite.internal.processors.affinity.GridAffinityProcessor;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.GridCacheUtilityKey;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.processors.clock.GridClockSyncProcessor;
import org.apache.ignite.internal.processors.closure.GridClosureProcessor;
import org.apache.ignite.internal.processors.cluster.ClusterProcessor;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor;
import org.apache.ignite.internal.processors.datastreamer.DataStreamProcessor;
import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.apache.ignite.internal.processors.hadoop.Hadoop;
import org.apache.ignite.internal.processors.hadoop.HadoopProcessorAdapter;
import org.apache.ignite.internal.processors.job.GridJobProcessor;
import org.apache.ignite.internal.processors.jobmetrics.GridJobMetricsProcessor;
import org.apache.ignite.internal.processors.nodevalidation.DiscoveryNodeValidationProcessor;
import org.apache.ignite.internal.processors.nodevalidation.OsDiscoveryNodeValidationProcessor;
import org.apache.ignite.internal.processors.odbc.OdbcProcessor;
import org.apache.ignite.internal.processors.offheap.GridOffHeapProcessor;
import org.apache.ignite.internal.processors.platform.PlatformNoopProcessor;
import org.apache.ignite.internal.processors.platform.PlatformProcessor;
import org.apache.ignite.internal.processors.plugin.IgnitePluginProcessor;
import org.apache.ignite.internal.processors.pool.PoolProcessor;
import org.apache.ignite.internal.processors.port.GridPortProcessor;
import org.apache.ignite.internal.processors.port.GridPortRecord;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.resource.GridResourceProcessor;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.processors.rest.GridRestProcessor;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.segmentation.GridSegmentationProcessor;
import org.apache.ignite.internal.processors.service.GridServiceProcessor;
import org.apache.ignite.internal.processors.session.GridTaskSessionProcessor;
import org.apache.ignite.internal.processors.task.GridTaskProcessor;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.suggestions.GridPerformanceSuggestions;
import org.apache.ignite.internal.suggestions.JvmConfigurationSuggestions;
import org.apache.ignite.internal.suggestions.OsConfigurationSuggestions;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.lang.GridAbsClosure;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.marshaller.MarshallerExclusions;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.mxbean.ClusterLocalNodeMetricsMXBean;
import org.apache.ignite.mxbean.IgniteMXBean;
import org.apache.ignite.mxbean.StripedExecutorMXBean;
import org.apache.ignite.mxbean.ThreadPoolMXBean;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginNotFoundException;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.IgniteSpiVersionCheckException;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.thread.IgniteStripedThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_CONFIG_URL;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DAEMON;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_NO_ASCII;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_OPTIMIZED_MARSHALLER_USE_DEFAULT_SUID;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_REST_START_ON_CLIENT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_STARVATION_CHECK_INTERVAL;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SUCCESS_FILE;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.IgniteSystemProperties.snapshot;
import static org.apache.ignite.internal.GridKernalState.DISCONNECTED;
import static org.apache.ignite.internal.GridKernalState.STARTED;
import static org.apache.ignite.internal.GridKernalState.STARTING;
import static org.apache.ignite.internal.GridKernalState.STOPPED;
import static org.apache.ignite.internal.GridKernalState.STOPPING;
import static org.apache.ignite.internal.IgniteComponentType.HADOOP_HELPER;
import static org.apache.ignite.internal.IgniteComponentType.IGFS;
import static org.apache.ignite.internal.IgniteComponentType.IGFS_HELPER;
import static org.apache.ignite.internal.IgniteComponentType.SCHEDULE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_DATE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_CLIENT_MODE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_CONSISTENCY_CHECK_SKIPPED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_DAEMON;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_DEPLOYMENT_MODE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_GRID_NAME;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IPS;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_JIT_NAME;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_JMX_PORT;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_JVM_ARGS;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_JVM_PID;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_LANG_RUNTIME;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_LATE_AFFINITY_ASSIGNMENT;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MARSHALLER;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MARSHALLER_COMPACT_FOOTER;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MARSHALLER_USE_BINARY_STRING_SER_VER_2;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MARSHALLER_USE_DFLT_SUID;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_NODE_CONSISTENT_ID;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_PEER_CLASSLOADING;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_PHY_RAM;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_PREFIX;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_RESTART_ENABLED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_PORT_RANGE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SPI_CLASS;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_USER_NAME;
import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.BUILD_TSTAMP_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;
import static org.apache.ignite.internal.IgniteVersionUtils.REV_HASH_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.VER;
import static org.apache.ignite.internal.IgniteVersionUtils.VER_STR;
import static org.apache.ignite.lifecycle.LifecycleEventType.AFTER_NODE_START;
import static org.apache.ignite.lifecycle.LifecycleEventType.BEFORE_NODE_START;

/**
 * Ignite kernal.
 * <p/>
 * See <a href="http://en.wikipedia.org/wiki/Kernal">http://en.wikipedia.org/wiki/Kernal</a> for information on the
 * misspelling.
 */
public class IgniteKernal implements IgniteEx, IgniteMXBean, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Ignite site that is shown in log messages. */
    public static final String SITE = "ignite.apache.org";

    /** System line separator. */
    private static final String NL = U.nl();

    /** Periodic starvation check interval. */
    private static final long PERIODIC_STARVATION_CHECK_FREQ = 1000 * 30;

    /** */
    @GridToStringExclude
    private GridKernalContextImpl ctx;

    /** Configuration. */
    private IgniteConfiguration cfg;

    /** */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    @GridToStringExclude
    private GridLoggerProxy log;

    /** */
    private String gridName;

    /** */
    @GridToStringExclude
    private ObjectName kernalMBean;

    /** */
    @GridToStringExclude
    private ObjectName locNodeMBean;

    /** */
    @GridToStringExclude
    private ObjectName pubExecSvcMBean;

    /** */
    @GridToStringExclude
    private ObjectName sysExecSvcMBean;

    /** */
    @GridToStringExclude
    private ObjectName mgmtExecSvcMBean;

    /** */
    @GridToStringExclude
    private ObjectName p2PExecSvcMBean;

    /** */
    @GridToStringExclude
    private ObjectName restExecSvcMBean;

    /** */
    @GridToStringExclude
    private ObjectName stripedExecSvcMBean;

    /** Kernal start timestamp. */
    private long startTime = U.currentTimeMillis();

    /** Spring context, potentially {@code null}. */
    private GridSpringResourceContext rsrcCtx;

    /** */
    @GridToStringExclude
    private GridTimeoutProcessor.CancelableTask starveTask;

    /** */
    @GridToStringExclude
    private GridTimeoutProcessor.CancelableTask metricsLogTask;

    /** */
    @GridToStringExclude
    private GridTimeoutProcessor.CancelableTask longOpDumpTask;

    /** Indicate error on grid stop. */
    @GridToStringExclude
    private boolean errOnStop;

    /** Scheduler. */
    @GridToStringExclude
    private IgniteScheduler scheduler;

    /** Kernal gateway. */
    @GridToStringExclude
    private final AtomicReference<GridKernalGateway> gw = new AtomicReference<>();

    /** Stop guard. */
    @GridToStringExclude
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /**
     * No-arg constructor is required by externalization.
     */
    public IgniteKernal() {
        this(null);
    }

    /**
     * @param rsrcCtx Optional Spring application context.
     */
    public IgniteKernal(@Nullable GridSpringResourceContext rsrcCtx) {
        this.rsrcCtx = rsrcCtx;
    }

    /** {@inheritDoc} */
    @Override public IgniteClusterEx cluster() {
        return ctx.cluster().get();
    }

    /** {@inheritDoc} */
    @Override public ClusterNode localNode() {
        return ctx.cluster().get().localNode();
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute compute() {
        return ((ClusterGroupAdapter)ctx.cluster().get().forServers()).compute();
    }

    /** {@inheritDoc} */
    @Override public IgniteMessaging message() {
        return ctx.cluster().get().message();
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents events() {
        return ctx.cluster().get().events();
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services() {
        return ((ClusterGroupAdapter)ctx.cluster().get().forServers()).services();
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService() {
        return ctx.cluster().get().executorService();
    }

    /** {@inheritDoc} */
    @Override public final IgniteCompute compute(ClusterGroup grp) {
        return ((ClusterGroupAdapter)grp).compute();
    }

    /** {@inheritDoc} */
    @Override public final IgniteMessaging message(ClusterGroup prj) {
        return ((ClusterGroupAdapter)prj).message();
    }

    /** {@inheritDoc} */
    @Override public final IgniteEvents events(ClusterGroup grp) {
        return ((ClusterGroupAdapter) grp).events();
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services(ClusterGroup grp) {
        return ((ClusterGroupAdapter)grp).services();
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService(ClusterGroup grp) {
        return ((ClusterGroupAdapter)grp).executorService();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return gridName;
    }

    /** {@inheritDoc} */
    @Override public String getCopyright() {
        return COPYRIGHT;
    }

    /** {@inheritDoc} */
    @Override public long getStartTimestamp() {
        return startTime;
    }

    /** {@inheritDoc} */
    @Override public String getStartTimestampFormatted() {
        return DateFormat.getDateTimeInstance().format(new Date(startTime));
    }

    /** {@inheritDoc} */
    @Override public long getUpTime() {
        return U.currentTimeMillis() - startTime;
    }

    /** {@inheritDoc} */
    @Override public String getUpTimeFormatted() {
        return X.timeSpan2HMSM(U.currentTimeMillis() - startTime);
    }

    /** {@inheritDoc} */
    @Override public String getFullVersion() {
        return VER_STR + '-' + BUILD_TSTAMP_STR;
    }

    /** {@inheritDoc} */
    @Override public String getCheckpointSpiFormatted() {
        assert cfg != null;

        return Arrays.toString(cfg.getCheckpointSpi());
    }

    /** {@inheritDoc} */
    @Override public String getSwapSpaceSpiFormatted() {
        assert cfg != null;

        return cfg.getSwapSpaceSpi().toString();
    }

    /** {@inheritDoc} */
    @Override public String getCommunicationSpiFormatted() {
        assert cfg != null;

        return cfg.getCommunicationSpi().toString();
    }

    /** {@inheritDoc} */
    @Override public String getDeploymentSpiFormatted() {
        assert cfg != null;

        return cfg.getDeploymentSpi().toString();
    }

    /** {@inheritDoc} */
    @Override public String getDiscoverySpiFormatted() {
        assert cfg != null;

        return cfg.getDiscoverySpi().toString();
    }

    /** {@inheritDoc} */
    @Override public String getEventStorageSpiFormatted() {
        assert cfg != null;

        return cfg.getEventStorageSpi().toString();
    }

    /** {@inheritDoc} */
    @Override public String getCollisionSpiFormatted() {
        assert cfg != null;

        return cfg.getCollisionSpi().toString();
    }

    /** {@inheritDoc} */
    @Override public String getFailoverSpiFormatted() {
        assert cfg != null;

        return Arrays.toString(cfg.getFailoverSpi());
    }

    /** {@inheritDoc} */
    @Override public String getLoadBalancingSpiFormatted() {
        assert cfg != null;

        return Arrays.toString(cfg.getLoadBalancingSpi());
    }

    /** {@inheritDoc} */
    @Override public String getOsInformation() {
        return U.osString();
    }

    /** {@inheritDoc} */
    @Override public String getJdkInformation() {
        return U.jdkString();
    }

    /** {@inheritDoc} */
    @Override public String getOsUser() {
        return System.getProperty("user.name");
    }

    /** {@inheritDoc} */
    @Override public void printLastErrors() {
        ctx.exceptionRegistry().printErrors(log);
    }

    /** {@inheritDoc} */
    @Override public String getVmName() {
        return ManagementFactory.getRuntimeMXBean().getName();
    }

    /** {@inheritDoc} */
    @Override public String getInstanceName() {
        return gridName;
    }

    /** {@inheritDoc} */
    @Override public String getExecutorServiceFormatted() {
        assert cfg != null;

        return String.valueOf(cfg.getPublicThreadPoolSize());
    }

    /** {@inheritDoc} */
    @Override public String getIgniteHome() {
        assert cfg != null;

        return cfg.getIgniteHome();
    }

    /** {@inheritDoc} */
    @Override public String getGridLoggerFormatted() {
        assert cfg != null;

        return cfg.getGridLogger().toString();
    }

    /** {@inheritDoc} */
    @Override public String getMBeanServerFormatted() {
        assert cfg != null;

        return cfg.getMBeanServer().toString();
    }

    /** {@inheritDoc} */
    @Override public UUID getLocalNodeId() {
        assert cfg != null;

        return cfg.getNodeId();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public List<String> getUserAttributesFormatted() {
        assert cfg != null;

        return (List<String>)F.transform(cfg.getUserAttributes().entrySet(), new C1<Map.Entry<String, ?>, String>() {
            @Override public String apply(Map.Entry<String, ?> e) {
                return e.getKey() + ", " + e.getValue().toString();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean isPeerClassLoadingEnabled() {
        assert cfg != null;

        return cfg.isPeerClassLoadingEnabled();
    }

    /** {@inheritDoc} */
    @Override public List<String> getLifecycleBeansFormatted() {
        LifecycleBean[] beans = cfg.getLifecycleBeans();

        if (F.isEmpty(beans))
            return Collections.emptyList();
        else {
            List<String> res = new ArrayList<>(beans.length);

            for (LifecycleBean bean : beans)
                res.add(String.valueOf(bean));

            return res;
        }
    }

    /**
     * @param name  New attribute name.
     * @param val New attribute value.
     * @throws IgniteCheckedException If duplicated SPI name found.
     */
    private void add(String name, @Nullable Serializable val) throws IgniteCheckedException {
        assert name != null;

        if (ctx.addNodeAttribute(name, val) != null) {
            if (name.endsWith(ATTR_SPI_CLASS))
                // User defined duplicated names for the different SPIs.
                throw new IgniteCheckedException("Failed to set SPI attribute. Duplicated SPI name found: " +
                    name.substring(0, name.length() - ATTR_SPI_CLASS.length()));

            // Otherwise it's a mistake of setting up duplicated attribute.
            assert false : "Duplicate attribute: " + name;
        }
    }

    /**
     * Notifies life-cycle beans of grid event.
     *
     * @param evt Grid event.
     * @throws IgniteCheckedException If user threw exception during start.
     */
    @SuppressWarnings({"CatchGenericClass"})
    private void notifyLifecycleBeans(LifecycleEventType evt) throws IgniteCheckedException {
        if (!cfg.isDaemon() && cfg.getLifecycleBeans() != null) {
            for (LifecycleBean bean : cfg.getLifecycleBeans())
                if (bean != null) {
                    try {
                        bean.onLifecycleEvent(evt);
                    }
                    catch (Exception e) {
                        throw new IgniteCheckedException(e);
                    }
                }
        }
    }

    /**
     * Notifies life-cycle beans of grid event.
     *
     * @param evt Grid event.
     */
    @SuppressWarnings({"CatchGenericClass"})
    private void notifyLifecycleBeansEx(LifecycleEventType evt) {
        try {
            notifyLifecycleBeans(evt);
        }
        // Catch generic throwable to secure against user assertions.
        catch (Throwable e) {
            U.error(log, "Failed to notify lifecycle bean (safely ignored) [evt=" + evt +
                (gridName == null ? "" : ", gridName=" + gridName) + ']', e);

            if (e instanceof Error)
                throw (Error)e;
        }
    }

    /**
     * @param cfg Configuration to use.
     * @param utilityCachePool Utility cache pool.
     * @param execSvc Executor service.
     * @param sysExecSvc System executor service.
     * @param stripedExecSvc Striped executor.
     * @param p2pExecSvc P2P executor service.
     * @param mgmtExecSvc Management executor service.
     * @param igfsExecSvc IGFS executor service.
     * @param restExecSvc Reset executor service.
     * @param affExecSvc Affinity executor service.
     * @param idxExecSvc Indexing executor service.
     * @param errHnd Error handler to use for notification about startup problems.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    @SuppressWarnings({"CatchGenericClass", "unchecked"})
    public void start(
        final IgniteConfiguration cfg,
        ExecutorService utilityCachePool,
        ExecutorService marshCachePool,
        final ExecutorService execSvc,
        final ExecutorService sysExecSvc,
        final StripedExecutor stripedExecSvc,
        ExecutorService p2pExecSvc,
        ExecutorService mgmtExecSvc,
        ExecutorService igfsExecSvc,
        ExecutorService restExecSvc,
        ExecutorService affExecSvc,
        @Nullable ExecutorService idxExecSvc,
        IgniteStripedThreadPoolExecutor callbackExecSvc,
        GridAbsClosure errHnd
    )
        throws IgniteCheckedException
    {
        gw.compareAndSet(null, new GridKernalGatewayImpl(cfg.getGridName()));

        GridKernalGateway gw = this.gw.get();

        gw.writeLock();

        try {
            switch (gw.getState()) {
                case STARTED: {
                    U.warn(log, "Grid has already been started (ignored).");

                    return;
                }

                case STARTING: {
                    U.warn(log, "Grid is already in process of being started (ignored).");

                    return;
                }

                case STOPPING: {
                    throw new IgniteCheckedException("Grid is in process of being stopped");
                }

                case STOPPED: {
                    break;
                }
            }

            gw.setState(STARTING);
        }
        finally {
            gw.writeUnlock();
        }

        assert cfg != null;

        // Make sure we got proper configuration.
        validateCommon(cfg);

        gridName = cfg.getGridName();

        this.cfg = cfg;

        log = (GridLoggerProxy)cfg.getGridLogger().getLogger(
            getClass().getName() + (gridName != null ? '%' + gridName : ""));

        RuntimeMXBean rtBean = ManagementFactory.getRuntimeMXBean();

        // Ack various information.
        ackAsciiLogo();
        ackConfigUrl();
        ackDaemon();
        ackOsInfo();
        ackLanguageRuntime();
        ackRemoteManagement();
        ackVmArguments(rtBean);
        ackClassPaths(rtBean);
        ackSystemProperties();
        ackEnvironmentVariables();
        ackCacheConfiguration();
        ackP2pConfiguration();
        ackRebalanceConfiguration();

        // Run background network diagnostics.
        GridDiagnostic.runBackgroundCheck(gridName, execSvc, log);

        // Ack 3-rd party licenses location.
        if (log.isInfoEnabled() && cfg.getIgniteHome() != null)
            log.info("3-rd party licenses can be found at: " + cfg.getIgniteHome() + File.separatorChar + "libs" +
                File.separatorChar + "licenses");

        // Check that user attributes are not conflicting
        // with internally reserved names.
        for (String name : cfg.getUserAttributes().keySet())
            if (name.startsWith(ATTR_PREFIX))
                throw new IgniteCheckedException("User attribute has illegal name: '" + name + "'. Note that all names " +
                    "starting with '" + ATTR_PREFIX + "' are reserved for internal use.");

        // Ack local node user attributes.
        logNodeUserAttributes();

        // Ack configuration.
        ackSpis();

        List<PluginProvider> plugins = U.allPluginProviders();

        // Spin out SPIs & managers.
        try {
            ctx = new GridKernalContextImpl(log,
                this,
                cfg,
                gw,
                utilityCachePool,
                marshCachePool,
                execSvc,
                sysExecSvc,
                stripedExecSvc,
                p2pExecSvc,
                mgmtExecSvc,
                igfsExecSvc,
                restExecSvc,
                affExecSvc,
                idxExecSvc,
                callbackExecSvc,
                plugins
            );

            cfg.getMarshaller().setContext(ctx.marshallerContext());

            ClusterProcessor clusterProc = new ClusterProcessor(ctx);

            startProcessor(clusterProc);

            U.onGridStart();

            // Start and configure resource processor first as it contains resources used
            // by all other managers and processors.
            GridResourceProcessor rsrcProc = new GridResourceProcessor(ctx);

            rsrcProc.setSpringContext(rsrcCtx);

            scheduler = new IgniteSchedulerImpl(ctx);

            startProcessor(rsrcProc);

            // Inject resources into lifecycle beans.
            if (!cfg.isDaemon() && cfg.getLifecycleBeans() != null) {
                for (LifecycleBean bean : cfg.getLifecycleBeans()) {
                    if (bean != null)
                        rsrcProc.inject(bean);
                }
            }

            // Lifecycle notification.
            notifyLifecycleBeans(BEFORE_NODE_START);

            // Starts lifecycle aware components.
            U.startLifecycleAware(lifecycleAwares(cfg));

            addHelper(IGFS_HELPER.create(F.isEmpty(cfg.getFileSystemConfiguration())));

            addHelper(HADOOP_HELPER.createIfInClassPath(ctx, false));

            startProcessor(new IgnitePluginProcessor(ctx, cfg, plugins));

            startProcessor(new PoolProcessor(ctx));

            // Off-heap processor has no dependencies.
            startProcessor(new GridOffHeapProcessor(ctx));

            // Closure processor should be started before all others
            // (except for resource processor), as many components can depend on it.
            startProcessor(new GridClosureProcessor(ctx));

            // Start some other processors (order & place is important).
            startProcessor(new GridPortProcessor(ctx));
            startProcessor(new GridJobMetricsProcessor(ctx));

            // Timeout processor needs to be started before managers,
            // as managers may depend on it.
            startProcessor(new GridTimeoutProcessor(ctx));

            // Start security processors.
            startProcessor(createComponent(GridSecurityProcessor.class, ctx));

            // Start SPI managers.
            // NOTE: that order matters as there are dependencies between managers.
            startManager(new GridIoManager(ctx));
            startManager(new GridCheckpointManager(ctx));

            startManager(new GridEventStorageManager(ctx));
            startManager(new GridDeploymentManager(ctx));
            startManager(new GridLoadBalancerManager(ctx));
            startManager(new GridFailoverManager(ctx));
            startManager(new GridCollisionManager(ctx));
            startManager(new GridSwapSpaceManager(ctx));
            startManager(new GridIndexingManager(ctx));

            ackSecurity();

            // Assign discovery manager to context before other processors start so they
            // are able to register custom event listener.
            GridManager discoMgr = new GridDiscoveryManager(ctx);

            ctx.add(discoMgr, false);

            // Start processors before discovery manager, so they will
            // be able to start receiving messages once discovery completes.
            startProcessor(createComponent(DiscoveryNodeValidationProcessor.class, ctx));
            startProcessor(new GridClockSyncProcessor(ctx));
            startProcessor(new GridAffinityProcessor(ctx));
            startProcessor(createComponent(GridSegmentationProcessor.class, ctx));
            startProcessor(createComponent(IgniteCacheObjectProcessor.class, ctx));
            startProcessor(new GridCacheProcessor(ctx));
            startProcessor(new GridQueryProcessor(ctx));
            startProcessor(new OdbcProcessor(ctx));
            startProcessor(new GridServiceProcessor(ctx));
            startProcessor(new GridTaskSessionProcessor(ctx));
            startProcessor(new GridJobProcessor(ctx));
            startProcessor(new GridTaskProcessor(ctx));
            startProcessor((GridProcessor)SCHEDULE.createOptional(ctx));
            startProcessor(new GridRestProcessor(ctx));
            startProcessor(new DataStreamProcessor(ctx));
            startProcessor((GridProcessor)IGFS.create(ctx, F.isEmpty(cfg.getFileSystemConfiguration())));
            startProcessor(new GridContinuousProcessor(ctx));
            startProcessor(createHadoopComponent());
            startProcessor(new DataStructuresProcessor(ctx));
            startProcessor(createComponent(PlatformProcessor.class, ctx));

            // Start plugins.
            for (PluginProvider provider : ctx.plugins().allProviders()) {
                ctx.add(new GridPluginComponent(provider));

                provider.start(ctx.plugins().pluginContextForProvider(provider));
            }

            fillNodeAttributes(clusterProc.updateNotifierEnabled());

            gw.writeLock();

            try {
                gw.setState(STARTED);

                // Start discovery manager last to make sure that grid is fully initialized.
                startManager(discoMgr);
            }
            finally {
                gw.writeUnlock();
            }

            // Check whether physical RAM is not exceeded.
            checkPhysicalRam();

            // Suggest configuration optimizations.
            suggestOptimizations(cfg);

            // Suggest JVM optimizations.
            ctx.performance().addAll(JvmConfigurationSuggestions.getSuggestions());

            // Suggest Operation System optimizations.
            ctx.performance().addAll(OsConfigurationSuggestions.getSuggestions());

            // Notify discovery manager the first to make sure that topology is discovered.
            ctx.discovery().onKernalStart();

            // Notify IO manager the second so further components can send and receive messages.
            ctx.io().onKernalStart();

            // Callbacks.
            for (GridComponent comp : ctx) {
                // Skip discovery manager.
                if (comp instanceof GridDiscoveryManager)
                    continue;

                // Skip IO manager.
                if (comp instanceof GridIoManager)
                    continue;

                if (!skipDaemon(comp))
                    comp.onKernalStart();
            }

            // Register MBeans.
            registerKernalMBean();
            registerLocalNodeMBean();
            registerExecutorMBeans(execSvc, sysExecSvc, p2pExecSvc, mgmtExecSvc, restExecSvc);
            registerStripedExecutorMBean(stripedExecSvc);

            // Lifecycle bean notifications.
            notifyLifecycleBeans(AFTER_NODE_START);
        }
        catch (Throwable e) {
            IgniteSpiVersionCheckException verCheckErr = X.cause(e, IgniteSpiVersionCheckException.class);

            if (verCheckErr != null)
                U.error(log, verCheckErr.getMessage());
            else if (X.hasCause(e, InterruptedException.class, IgniteInterruptedCheckedException.class))
                U.warn(log, "Grid startup routine has been interrupted (will rollback).");
            else
                U.error(log, "Got exception while starting (will rollback startup routine).", e);

            errHnd.apply();

            stop(true);

            if (e instanceof Error)
                throw e;
            else if (e instanceof IgniteCheckedException)
                throw (IgniteCheckedException)e;
            else
                throw new IgniteCheckedException(e);
        }

        // Mark start timestamp.
        startTime = U.currentTimeMillis();

        String intervalStr = IgniteSystemProperties.getString(IGNITE_STARVATION_CHECK_INTERVAL);

        // Start starvation checker if enabled.
        boolean starveCheck = !isDaemon() && !"0".equals(intervalStr);

        if (starveCheck) {
            final long interval = F.isEmpty(intervalStr) ? PERIODIC_STARVATION_CHECK_FREQ : Long.parseLong(intervalStr);

            starveTask = ctx.timeout().schedule(new Runnable() {
                /** Last completed task count. */
                private long lastCompletedCntPub;

                /** Last completed task count. */
                private long lastCompletedCntSys;

                @Override public void run() {
                    if (execSvc instanceof ThreadPoolExecutor) {
                        ThreadPoolExecutor exec = (ThreadPoolExecutor)execSvc;

                        lastCompletedCntPub = checkPoolStarvation(exec, lastCompletedCntPub, "public");
                    }

                    if (sysExecSvc instanceof ThreadPoolExecutor) {
                        ThreadPoolExecutor exec = (ThreadPoolExecutor)sysExecSvc;

                        lastCompletedCntSys = checkPoolStarvation(exec, lastCompletedCntSys, "system");
                    }

                    if (stripedExecSvc != null)
                        stripedExecSvc.checkStarvation();
                }

                /**
                 * @param exec Thread pool executor to check.
                 * @param lastCompletedCnt Last completed tasks count.
                 * @param pool Pool name for message.
                 * @return Current completed tasks count.
                 */
                private long checkPoolStarvation(
                    ThreadPoolExecutor exec,
                    long lastCompletedCnt,
                    String pool
                ) {
                    long completedCnt = exec.getCompletedTaskCount();

                    // If all threads are active and no task has completed since last time and there is
                    // at least one waiting request, then it is possible starvation.
                    if (exec.getPoolSize() == exec.getActiveCount() && completedCnt == lastCompletedCnt &&
                        !exec.getQueue().isEmpty())
                        LT.warn(
                            log,
                            "Possible thread pool starvation detected (no task completed in last " +
                                interval + "ms, is " + pool + " thread pool size large enough?)");

                    return completedCnt;
                }
            }, interval, interval);
        }

        long metricsLogFreq = cfg.getMetricsLogFrequency();

        if (metricsLogFreq > 0) {
            metricsLogTask = ctx.timeout().schedule(new Runnable() {
                private final DecimalFormat dblFmt = new DecimalFormat("#.##");

                @Override public void run() {
                    if (log.isInfoEnabled()) {
                        try {
                            ClusterMetrics m = cluster().localNode().metrics();

                            double cpuLoadPct = m.getCurrentCpuLoad() * 100;
                            double avgCpuLoadPct = m.getAverageCpuLoad() * 100;
                            double gcPct = m.getCurrentGcCpuLoad() * 100;

                            //Heap params
                            long heapUsed = m.getHeapMemoryUsed();
                            long heapMax = m.getHeapMemoryMaximum();

                            long heapUsedInMBytes = heapUsed / 1024 / 1024;
                            long heapCommInMBytes = m.getHeapMemoryCommitted() / 1024 / 1024;

                            double freeHeapPct = heapMax > 0 ? ((double)((heapMax - heapUsed) * 100)) / heapMax : -1;

                            //Non heap params
                            long nonHeapUsed = m.getNonHeapMemoryUsed();
                            long nonHeapMax = m.getNonHeapMemoryMaximum();

                            long nonHeapUsedInMBytes = nonHeapUsed / 1024 / 1024;
                            long nonHeapCommInMBytes = m.getNonHeapMemoryCommitted() / 1024 / 1024;

                            double freeNonHeapPct = nonHeapMax > 0 ? ((double)((nonHeapMax - nonHeapUsed) * 100)) / nonHeapMax : -1;

                            int hosts = 0;
                            int nodes = 0;
                            int cpus = 0;

                            try {
                                ClusterMetrics metrics = cluster().metrics();

                                Collection<ClusterNode> nodes0 = cluster().nodes();

                                hosts = U.neighborhood(nodes0).size();
                                nodes = metrics.getTotalNodes();
                                cpus = metrics.getTotalCpus();
                            }
                            catch (IgniteException ignore) {
                                // No-op.
                            }

                            int pubPoolActiveThreads = 0;
                            int pubPoolIdleThreads = 0;
                            int pubPoolQSize = 0;

                            if (execSvc instanceof ThreadPoolExecutor) {
                                ThreadPoolExecutor exec = (ThreadPoolExecutor)execSvc;

                                int poolSize = exec.getPoolSize();

                                pubPoolActiveThreads = Math.min(poolSize, exec.getActiveCount());
                                pubPoolIdleThreads = poolSize - pubPoolActiveThreads;
                                pubPoolQSize = exec.getQueue().size();
                            }

                            int sysPoolActiveThreads = 0;
                            int sysPoolIdleThreads = 0;
                            int sysPoolQSize = 0;

                            if (sysExecSvc instanceof ThreadPoolExecutor) {
                                ThreadPoolExecutor exec = (ThreadPoolExecutor)sysExecSvc;

                                int poolSize = exec.getPoolSize();

                                sysPoolActiveThreads = Math.min(poolSize, exec.getActiveCount());
                                sysPoolIdleThreads = poolSize - sysPoolActiveThreads;
                                sysPoolQSize = exec.getQueue().size();
                            }

                            String id = U.id8(localNode().id());

                            String msg = NL +
                                "Metrics for local node (to disable set 'metricsLogFrequency' to 0)" + NL +
                                "    ^-- Node [id=" + id + ", name=" + name() + ", uptime=" + getUpTimeFormatted() + "]" + NL +
                                "    ^-- H/N/C [hosts=" + hosts + ", nodes=" + nodes + ", CPUs=" + cpus + "]" + NL +
                                "    ^-- CPU [cur=" + dblFmt.format(cpuLoadPct) + "%, avg=" +
                                dblFmt.format(avgCpuLoadPct) + "%, GC=" + dblFmt.format(gcPct) + "%]" + NL +
                                "    ^-- Heap [used=" + dblFmt.format(heapUsedInMBytes) + "MB, free=" +
                                dblFmt.format(freeHeapPct) + "%, comm=" + dblFmt.format(heapCommInMBytes) + "MB]" + NL +
                                "    ^-- Non heap [used=" + dblFmt.format(nonHeapUsedInMBytes) + "MB, free=" +
                                dblFmt.format(freeNonHeapPct) + "%, comm=" + dblFmt.format(nonHeapCommInMBytes) + "MB]" + NL +
                                "    ^-- Public thread pool [active=" + pubPoolActiveThreads + ", idle=" +
                                pubPoolIdleThreads + ", qSize=" + pubPoolQSize + "]" + NL +
                                "    ^-- System thread pool [active=" + sysPoolActiveThreads + ", idle=" +
                                sysPoolIdleThreads + ", qSize=" + sysPoolQSize + "]" + NL +
                                "    ^-- Outbound messages queue [size=" + m.getOutboundMessagesQueueSize() + "]";

                            log.info(msg);
                        }
                        catch (IgniteClientDisconnectedException ignore) {
                            // No-op.
                        }
                    }
                }
            }, metricsLogFreq, metricsLogFreq);
        }

        final long longOpDumpTimeout =
            IgniteSystemProperties.getLong(IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, 60_000);

        if (longOpDumpTimeout > 0) {
            longOpDumpTask = ctx.timeout().schedule(new Runnable() {
                @Override public void run() {
                    GridKernalContext ctx = IgniteKernal.this.ctx;

                    if (ctx != null)
                        ctx.cache().context().exchange().dumpLongRunningOperations(longOpDumpTimeout);
                }
            }, longOpDumpTimeout, longOpDumpTimeout);
        }

        ctx.performance().add("Disable assertions (remove '-ea' from JVM options)", !U.assertionsEnabled());

        ctx.performance().logSuggestions(log, gridName);

        U.quietAndInfo(log, "To start Console Management & Monitoring run ignitevisorcmd.{sh|bat}");

        ackStart(rtBean);

        if (!isDaemon())
            ctx.discovery().ackTopology(localNode().order());
    }

    /**
     * Create Hadoop component.
     *
     * @return Non-null Hadoop component: workable or no-op.
     * @throws IgniteCheckedException If the component is mandatory and cannot be initialized.
     */
    private HadoopProcessorAdapter createHadoopComponent() throws IgniteCheckedException {
        boolean mandatory = cfg.getHadoopConfiguration() != null;

        if (mandatory) {
            if (cfg.isPeerClassLoadingEnabled())
                throw new IgniteCheckedException("Hadoop module cannot be used with peer class loading enabled " +
                    "(set IgniteConfiguration.peerClassLoadingEnabled to \"false\").");

            HadoopProcessorAdapter res = IgniteComponentType.HADOOP.createIfInClassPath(ctx, true);

            res.validateEnvironment();

            return res;
        }
        else {
            HadoopProcessorAdapter cmp = null;

            if (!ctx.hadoopHelper().isNoOp() && cfg.isPeerClassLoadingEnabled()) {
                U.warn(log, "Hadoop module is found in classpath, but will not be started because peer class " +
                    "loading is enabled (set IgniteConfiguration.peerClassLoadingEnabled to \"false\" if you want " +
                    "to use Hadoop module).");
            }
            else {
                cmp = IgniteComponentType.HADOOP.createIfInClassPath(ctx, false);

                try {
                    cmp.validateEnvironment();
                }
                catch (IgniteException | IgniteCheckedException e) {
                    U.quietAndWarn(log, "Hadoop module will not start due to exception: " + e.getMessage());

                    cmp = null;
                }
            }

            if (cmp == null)
                cmp = IgniteComponentType.HADOOP.create(ctx, true);

            return cmp;
        }
    }

    /**
     * Validates common configuration parameters.
     *
     * @param cfg Configuration.
     */
    private void validateCommon(IgniteConfiguration cfg) {
        A.notNull(cfg.getNodeId(), "cfg.getNodeId()");

        A.notNull(cfg.getMBeanServer(), "cfg.getMBeanServer()");
        A.notNull(cfg.getGridLogger(), "cfg.getGridLogger()");
        A.notNull(cfg.getMarshaller(), "cfg.getMarshaller()");
        A.notNull(cfg.getUserAttributes(), "cfg.getUserAttributes()");

        // All SPIs should be non-null.
        A.notNull(cfg.getSwapSpaceSpi(), "cfg.getSwapSpaceSpi()");
        A.notNull(cfg.getCheckpointSpi(), "cfg.getCheckpointSpi()");
        A.notNull(cfg.getCommunicationSpi(), "cfg.getCommunicationSpi()");
        A.notNull(cfg.getDeploymentSpi(), "cfg.getDeploymentSpi()");
        A.notNull(cfg.getDiscoverySpi(), "cfg.getDiscoverySpi()");
        A.notNull(cfg.getEventStorageSpi(), "cfg.getEventStorageSpi()");
        A.notNull(cfg.getCollisionSpi(), "cfg.getCollisionSpi()");
        A.notNull(cfg.getFailoverSpi(), "cfg.getFailoverSpi()");
        A.notNull(cfg.getLoadBalancingSpi(), "cfg.getLoadBalancingSpi()");
        A.notNull(cfg.getIndexingSpi(), "cfg.getIndexingSpi()");

        A.ensure(cfg.getNetworkTimeout() > 0, "cfg.getNetworkTimeout() > 0");
        A.ensure(cfg.getNetworkSendRetryDelay() > 0, "cfg.getNetworkSendRetryDelay() > 0");
        A.ensure(cfg.getNetworkSendRetryCount() > 0, "cfg.getNetworkSendRetryCount() > 0");
    }

    /**
     * Checks whether physical RAM is not exceeded.
     */
    @SuppressWarnings("ConstantConditions")
    private void checkPhysicalRam() {
        long ram = ctx.discovery().localNode().attribute(ATTR_PHY_RAM);

        if (ram != -1) {
            String macs = ctx.discovery().localNode().attribute(ATTR_MACS);

            long totalHeap = 0;

            for (ClusterNode node : ctx.discovery().allNodes()) {
                if (macs.equals(node.attribute(ATTR_MACS))) {
                    long heap = node.metrics().getHeapMemoryMaximum();

                    if (heap != -1)
                        totalHeap += heap;
                }
            }

            if (totalHeap > ram) {
                U.quietAndWarn(log, "Attempting to start more nodes than physical RAM " +
                    "available on current host (this can cause significant slowdown)");
            }
        }
    }

    /**
     * @param cfg Configuration to check for possible performance issues.
     */
    private void suggestOptimizations(IgniteConfiguration cfg) {
        GridPerformanceSuggestions perf = ctx.performance();

        if (ctx.collision().enabled())
            perf.add("Disable collision resolution (remove 'collisionSpi' from configuration)");

        if (ctx.checkpoint().enabled())
            perf.add("Disable checkpoints (remove 'checkpointSpi' from configuration)");

        if (cfg.isMarshalLocalJobs())
            perf.add("Disable local jobs marshalling (set 'marshalLocalJobs' to false)");

        if (cfg.getIncludeEventTypes() != null && cfg.getIncludeEventTypes().length != 0)
            perf.add("Disable grid events (remove 'includeEventTypes' from configuration)");

        if (BinaryMarshaller.available() && (cfg.getMarshaller() != null && !(cfg.getMarshaller() instanceof BinaryMarshaller)))
            perf.add("Use default binary marshaller (do not set 'marshaller' explicitly)");
    }

    /**
     * Creates attributes map and fills it in.
     *
     * @param notifyEnabled Update notifier flag.
     * @throws IgniteCheckedException thrown if was unable to set up attribute.
     */
    @SuppressWarnings({"SuspiciousMethodCalls", "unchecked", "TypeMayBeWeakened"})
    private void fillNodeAttributes(boolean notifyEnabled) throws IgniteCheckedException {
        final String[] incProps = cfg.getIncludeProperties();

        try {
            // Stick all environment settings into node attributes.
            for (Map.Entry<String, String> sysEntry : System.getenv().entrySet()) {
                String name = sysEntry.getKey();

                if (incProps == null || U.containsStringArray(incProps, name, true) ||
                    U.isVisorNodeStartProperty(name) || U.isVisorRequiredProperty(name))
                    ctx.addNodeAttribute(name, sysEntry.getValue());
            }

            if (log.isDebugEnabled())
                log.debug("Added environment properties to node attributes.");
        }
        catch (SecurityException e) {
            throw new IgniteCheckedException("Failed to add environment properties to node attributes due to " +
                "security violation: " + e.getMessage());
        }

        try {
            // Stick all system properties into node's attributes overwriting any
            // identical names from environment properties.
            for (Map.Entry<Object, Object> e : snapshot().entrySet()) {
                String key = (String)e.getKey();

                if (incProps == null || U.containsStringArray(incProps, key, true) ||
                    U.isVisorRequiredProperty(key)) {
                    Object val = ctx.nodeAttribute(key);

                    if (val != null && !val.equals(e.getValue()))
                        U.warn(log, "System property will override environment variable with the same name: " + key);

                    ctx.addNodeAttribute(key, e.getValue());
                }
            }

            ctx.addNodeAttribute(IgniteNodeAttributes.ATTR_UPDATE_NOTIFIER_ENABLED, notifyEnabled);

            if (log.isDebugEnabled())
                log.debug("Added system properties to node attributes.");
        }
        catch (SecurityException e) {
            throw new IgniteCheckedException("Failed to add system properties to node attributes due to security " +
                "violation: " + e.getMessage());
        }

        // Add local network IPs and MACs.
        String ips = F.concat(U.allLocalIps(), ", "); // Exclude loopbacks.
        String macs = F.concat(U.allLocalMACs(), ", "); // Only enabled network interfaces.

        // Ack network context.
        if (log.isInfoEnabled()) {
            log.info("Non-loopback local IPs: " + (F.isEmpty(ips) ? "N/A" : ips));
            log.info("Enabled local MACs: " + (F.isEmpty(macs) ? "N/A" : macs));
        }

        // Warn about loopback.
        if (ips.isEmpty() && macs.isEmpty())
            U.warn(log, "Ignite is starting on loopback address... Only nodes on the same physical " +
                    "computer can participate in topology.",
                "Ignite is starting on loopback address...");

        // Stick in network context into attributes.
        add(ATTR_IPS, (ips.isEmpty() ? "" : ips));
        add(ATTR_MACS, (macs.isEmpty() ? "" : macs));

        // Stick in some system level attributes
        add(ATTR_JIT_NAME, U.getCompilerMx() == null ? "" : U.getCompilerMx().getName());
        add(ATTR_BUILD_VER, VER_STR);
        add(ATTR_BUILD_DATE, BUILD_TSTAMP_STR);
        add(ATTR_MARSHALLER, cfg.getMarshaller().getClass().getName());
        add(ATTR_MARSHALLER_USE_DFLT_SUID,
            getBoolean(IGNITE_OPTIMIZED_MARSHALLER_USE_DEFAULT_SUID, OptimizedMarshaller.USE_DFLT_SUID));
        add(ATTR_LATE_AFFINITY_ASSIGNMENT, cfg.isLateAffinityAssignment());

        if (cfg.getMarshaller() instanceof BinaryMarshaller) {
            add(ATTR_MARSHALLER_COMPACT_FOOTER, cfg.getBinaryConfiguration() == null ?
                BinaryConfiguration.DFLT_COMPACT_FOOTER :
                cfg.getBinaryConfiguration().isCompactFooter());

            add(ATTR_MARSHALLER_USE_BINARY_STRING_SER_VER_2,
                getBoolean(IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2,
                    BinaryUtils.USE_STR_SERIALIZATION_VER_2));
        }

        add(ATTR_USER_NAME, System.getProperty("user.name"));
        add(ATTR_GRID_NAME, gridName);

        add(ATTR_PEER_CLASSLOADING, cfg.isPeerClassLoadingEnabled());
        add(ATTR_DEPLOYMENT_MODE, cfg.getDeploymentMode());
        add(ATTR_LANG_RUNTIME, getLanguage());

        add(ATTR_JVM_PID, U.jvmPid());

        add(ATTR_CLIENT_MODE, cfg.isClientMode());

        add(ATTR_CONSISTENCY_CHECK_SKIPPED, getBoolean(IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK));

        if (cfg.getConsistentId() != null)
            add(ATTR_NODE_CONSISTENT_ID, cfg.getConsistentId());

        // Build a string from JVM arguments, because parameters with spaces are split.
        SB jvmArgs = new SB(512);

        for (String arg : U.jvmArgs()) {
            if (arg.startsWith("-"))
                jvmArgs.a("@@@");
            else
                jvmArgs.a(' ');

            jvmArgs.a(arg);
        }
        // Add it to attributes.
        add(ATTR_JVM_ARGS, jvmArgs.toString());

        // Check daemon system property and override configuration if it's set.
        if (isDaemon())
            add(ATTR_DAEMON, "true");

        // In case of the parsing error, JMX remote disabled or port not being set
        // node attribute won't be set.
        if (isJmxRemoteEnabled()) {
            String portStr = System.getProperty("com.sun.management.jmxremote.port");

            if (portStr != null)
                try {
                    add(ATTR_JMX_PORT, Integer.parseInt(portStr));
                }
                catch (NumberFormatException ignore) {
                    // No-op.
                }
        }

        // Whether restart is enabled and stick the attribute.
        add(ATTR_RESTART_ENABLED, Boolean.toString(isRestartEnabled()));

        // Save port range, port numbers will be stored by rest processor at runtime.
        if (cfg.getConnectorConfiguration() != null)
            add(ATTR_REST_PORT_RANGE, cfg.getConnectorConfiguration().getPortRange());

        // Stick in SPI versions and classes attributes.
        addSpiAttributes(cfg.getCollisionSpi());
        addSpiAttributes(cfg.getSwapSpaceSpi());
        addSpiAttributes(cfg.getDiscoverySpi());
        addSpiAttributes(cfg.getFailoverSpi());
        addSpiAttributes(cfg.getCommunicationSpi());
        addSpiAttributes(cfg.getEventStorageSpi());
        addSpiAttributes(cfg.getCheckpointSpi());
        addSpiAttributes(cfg.getLoadBalancingSpi());
        addSpiAttributes(cfg.getDeploymentSpi());

        // Set user attributes for this node.
        if (cfg.getUserAttributes() != null) {
            for (Map.Entry<String, ?> e : cfg.getUserAttributes().entrySet()) {
                if (ctx.hasNodeAttribute(e.getKey()))
                    U.warn(log, "User or internal attribute has the same name as environment or system " +
                        "property and will take precedence: " + e.getKey());

                ctx.addNodeAttribute(e.getKey(), e.getValue());
            }
        }
    }

    /**
     * Add SPI version and class attributes into node attributes.
     *
     * @param spiList Collection of SPIs to get attributes from.
     * @throws IgniteCheckedException Thrown if was unable to set up attribute.
     */
    private void addSpiAttributes(IgniteSpi... spiList) throws IgniteCheckedException {
        for (IgniteSpi spi : spiList) {
            Class<? extends IgniteSpi> spiCls = spi.getClass();

            add(U.spiAttribute(spi, ATTR_SPI_CLASS), spiCls.getName());
        }
    }

    /** @throws IgniteCheckedException If registration failed. */
    private void registerKernalMBean() throws IgniteCheckedException {
        try {
            kernalMBean = U.registerMBean(
                cfg.getMBeanServer(),
                cfg.getGridName(),
                "Kernal",
                getClass().getSimpleName(),
                this,
                IgniteMXBean.class);

            if (log.isDebugEnabled())
                log.debug("Registered kernal MBean: " + kernalMBean);
        }
        catch (JMException e) {
            kernalMBean = null;

            throw new IgniteCheckedException("Failed to register kernal MBean.", e);
        }
    }

    /** @throws IgniteCheckedException If registration failed. */
    private void registerLocalNodeMBean() throws IgniteCheckedException {
        ClusterLocalNodeMetricsMXBean mbean = new ClusterLocalNodeMetricsMXBeanImpl(ctx.discovery().localNode());

        try {
            locNodeMBean = U.registerMBean(
                cfg.getMBeanServer(),
                cfg.getGridName(),
                "Kernal",
                mbean.getClass().getSimpleName(),
                mbean,
                ClusterLocalNodeMetricsMXBean.class);

            if (log.isDebugEnabled())
                log.debug("Registered local node MBean: " + locNodeMBean);
        }
        catch (JMException e) {
            locNodeMBean = null;

            throw new IgniteCheckedException("Failed to register local node MBean.", e);
        }
    }

    /**
     * @param execSvc
     * @param sysExecSvc
     * @param p2pExecSvc
     * @param mgmtExecSvc
     * @param restExecSvc
     * @throws IgniteCheckedException If failed.
     */
    private void registerExecutorMBeans(ExecutorService execSvc,
        ExecutorService sysExecSvc,
        ExecutorService p2pExecSvc,
        ExecutorService mgmtExecSvc,
        ExecutorService restExecSvc) throws IgniteCheckedException {
        pubExecSvcMBean = registerExecutorMBean(execSvc, "GridExecutionExecutor");
        sysExecSvcMBean = registerExecutorMBean(sysExecSvc, "GridSystemExecutor");
        mgmtExecSvcMBean = registerExecutorMBean(mgmtExecSvc, "GridManagementExecutor");
        p2PExecSvcMBean = registerExecutorMBean(p2pExecSvc, "GridClassLoadingExecutor");

        ConnectorConfiguration clientCfg = cfg.getConnectorConfiguration();

        if (clientCfg != null)
            restExecSvcMBean = registerExecutorMBean(restExecSvc, "GridRestExecutor");
    }

    /**
     * @param exec Executor service to register.
     * @param name Property name for executor.
     * @return Name for created MBean.
     * @throws IgniteCheckedException If registration failed.
     */
    private ObjectName registerExecutorMBean(ExecutorService exec, String name) throws IgniteCheckedException {
        assert exec != null;

        try {
            ObjectName res = U.registerMBean(
                cfg.getMBeanServer(),
                cfg.getGridName(),
                "Thread Pools",
                name,
                new ThreadPoolMXBeanAdapter(exec),
                ThreadPoolMXBean.class);

            if (log.isDebugEnabled())
                log.debug("Registered executor service MBean: " + res);

            return res;
        }
        catch (JMException e) {
            throw new IgniteCheckedException("Failed to register executor service MBean [name=" + name +
                ", exec=" + exec + ']', e);
        }
    }

    /**
     * @param stripedExecSvc Executor service.
     * @throws IgniteCheckedException If registration failed.
     */
    private void registerStripedExecutorMBean(StripedExecutor stripedExecSvc) throws IgniteCheckedException {
        if (stripedExecSvc != null) {
            String name = "StripedExecutor";

            try {
                stripedExecSvcMBean = U.registerMBean(
                    cfg.getMBeanServer(),
                    cfg.getGridName(),
                    "Thread Pools",
                    name,
                    new StripedExecutorMXBeanAdapter(stripedExecSvc),
                    StripedExecutorMXBean.class);

                if (log.isDebugEnabled())
                    log.debug("Registered executor service MBean: " + stripedExecSvcMBean);
            } catch (JMException e) {
                throw new IgniteCheckedException("Failed to register executor service MBean [name="
                    + name + ", exec=" + stripedExecSvc + ']', e);
            }
        }
    }

    /**
     * Unregisters given mbean.
     *
     * @param mbean MBean to unregister.
     * @return {@code True} if successfully unregistered, {@code false} otherwise.
     */
    private boolean unregisterMBean(@Nullable ObjectName mbean) {
        if (mbean != null)
            try {
                cfg.getMBeanServer().unregisterMBean(mbean);

                if (log.isDebugEnabled())
                    log.debug("Unregistered MBean: " + mbean);

                return true;
            }
            catch (JMException e) {
                U.error(log, "Failed to unregister MBean.", e);

                return false;
            }

        return true;
    }

    /**
     * @param mgr Manager to start.
     * @throws IgniteCheckedException Throw in case of any errors.
     */
    private void startManager(GridManager mgr) throws IgniteCheckedException {
        // Add manager to registry before it starts to avoid cases when manager is started
        // but registry does not have it yet.
        ctx.add(mgr);

        try {
            if (!skipDaemon(mgr))
                mgr.start();
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to start manager: " + mgr , e);

            throw new IgniteCheckedException("Failed to start manager: " + mgr, e);
        }
    }

    /**
     * @param proc Processor to start.
     * @throws IgniteCheckedException Thrown in case of any error.
     */
    private void startProcessor(GridProcessor proc) throws IgniteCheckedException {
        ctx.add(proc);

        try {
            if (!skipDaemon(proc))
                proc.start();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Failed to start processor: " + proc, e);
        }
    }

    /**
     * Add helper.
     *
     * @param helper Helper.
     */
    private void addHelper(Object helper) {
        ctx.addHelper(helper);
    }

    /**
     * Gets "on" or "off" string for given boolean value.
     *
     * @param b Boolean value to convert.
     * @return Result string.
     */
    private String onOff(boolean b) {
        return b ? "on" : "off";
    }

    /**
     *
     * @return Whether or not REST is enabled.
     */
    private boolean isRestEnabled() {
        assert cfg != null;

        return cfg.getConnectorConfiguration() != null &&
            // By default rest processor doesn't start on client nodes.
            (!isClientNode() || (isClientNode() && IgniteSystemProperties.getBoolean(IGNITE_REST_START_ON_CLIENT)));
    }

    /**
     * @return {@code True} if node client or daemon otherwise {@code false}.
     */
    private boolean isClientNode() {
        return cfg.isClientMode() || cfg.isDaemon();
    }

    /**
     * Acks remote management.
     */
    private void ackRemoteManagement() {
        assert log != null;

        if (!log.isInfoEnabled())
            return;

        SB sb = new SB();

        sb.a("Remote Management [");

        boolean on = isJmxRemoteEnabled();

        sb.a("restart: ").a(onOff(isRestartEnabled())).a(", ");
        sb.a("REST: ").a(onOff(isRestEnabled())).a(", ");
        sb.a("JMX (");
        sb.a("remote: ").a(onOff(on));

        if (on) {
            sb.a(", ");

            sb.a("port: ").a(System.getProperty("com.sun.management.jmxremote.port", "<n/a>")).a(", ");
            sb.a("auth: ").a(onOff(Boolean.getBoolean("com.sun.management.jmxremote.authenticate"))).a(", ");

            // By default SSL is enabled, that's why additional check for null is needed.
            // See http://docs.oracle.com/javase/6/docs/technotes/guides/management/agent.html
            sb.a("ssl: ").a(onOff(Boolean.getBoolean("com.sun.management.jmxremote.ssl") ||
                System.getProperty("com.sun.management.jmxremote.ssl") == null));
        }

        sb.a(")");

        sb.a(']');

        log.info(sb.toString());
    }

    /**
     * Acks configuration URL.
     */
    private void ackConfigUrl() {
        assert log != null;

        if (log.isInfoEnabled())
            log.info("Config URL: " + System.getProperty(IGNITE_CONFIG_URL, "n/a"));
    }

    /**
     * Acks ASCII-logo. Thanks to http://patorjk.com/software/taag
     */
    private void ackAsciiLogo() {
        assert log != null;

        if (System.getProperty(IGNITE_NO_ASCII) == null) {
            String ver = "ver. " + ACK_VER_STR;

            // Big thanks to: http://patorjk.com/software/taag
            // Font name "Small Slant"
            if (log.isInfoEnabled()) {
                log.info(NL + NL +
                        ">>>    __________  ________________  " + NL +
                        ">>>   /  _/ ___/ |/ /  _/_  __/ __/  " + NL +
                        ">>>  _/ // (7 7    // /  / / / _/    " + NL +
                        ">>> /___/\\___/_/|_/___/ /_/ /___/   " + NL +
                        ">>> " + NL +
                        ">>> " + ver + NL +
                        ">>> " + COPYRIGHT + NL +
                        ">>> " + NL +
                        ">>> Ignite documentation: " + "http://" + SITE + NL
                );
            }

            if (log.isQuiet()) {
                U.quiet(false,
                    "   __________  ________________ ",
                    "  /  _/ ___/ |/ /  _/_  __/ __/ ",
                    " _/ // (7 7    // /  / / / _/   ",
                    "/___/\\___/_/|_/___/ /_/ /___/  ",
                    "",
                    ver,
                    COPYRIGHT,
                    "",
                    "Ignite documentation: " + "http://" + SITE,
                    "",
                    "Quiet mode.");

                String fileName = log.fileName();

                if (fileName != null)
                    U.quiet(false, "  ^-- Logging to file '" + fileName + '\'');

                U.quiet(false,
                    "  ^-- To see **FULL** console log here add -DIGNITE_QUIET=false or \"-v\" to ignite.{sh|bat}",
                    "");
            }
        }
    }

    /**
     * Prints start info.
     *
     * @param rtBean Java runtime bean.
     */
    private void ackStart(RuntimeMXBean rtBean) {
        ClusterNode locNode = localNode();

        if (log.isQuiet()) {
            U.quiet(false, "");
            U.quiet(false, "Ignite node started OK (id=" + U.id8(locNode.id()) +
                (F.isEmpty(gridName) ? "" : ", grid=" + gridName) + ')');
        }

        if (log.isInfoEnabled()) {
            log.info("");

            String ack = "Ignite ver. " + VER_STR + '#' + BUILD_TSTAMP_STR + "-sha1:" + REV_HASH_STR;

            String dash = U.dash(ack.length());

            SB sb = new SB();

            for (GridPortRecord rec : ctx.ports().records())
                sb.a(rec.protocol()).a(":").a(rec.port()).a(" ");

            String str =
                NL + NL +
                    ">>> " + dash + NL +
                    ">>> " + ack + NL +
                    ">>> " + dash + NL +
                    ">>> OS name: " + U.osString() + NL +
                    ">>> CPU(s): " + locNode.metrics().getTotalCpus() + NL +
                    ">>> Heap: " + U.heapSize(locNode, 2) + "GB" + NL +
                    ">>> VM name: " + rtBean.getName() + NL +
                    (gridName == null ? "" : ">>> Grid name: " + gridName + NL) +
                    ">>> Local node [" +
                    "ID=" + locNode.id().toString().toUpperCase() +
                    ", order=" + locNode.order() + ", clientMode=" + ctx.clientNode() +
                    "]" + NL +
                    ">>> Local node addresses: " + U.addressesAsString(locNode) + NL +
                    ">>> Local ports: " + sb + NL;

            log.info(str);
        }
    }

    /**
     * Logs out OS information.
     */
    private void ackOsInfo() {
        assert log != null;

        if (log.isQuiet())
            U.quiet(false, "OS: " + U.osString());

        if (log.isInfoEnabled()) {
            log.info("OS: " + U.osString());
            log.info("OS user: " + System.getProperty("user.name"));

            int jvmPid = U.jvmPid();

            log.info("PID: " + (jvmPid == -1 ? "N/A" : jvmPid));
        }
    }

    /**
     * Logs out language runtime.
     */
    private void ackLanguageRuntime() {
        assert log != null;

        if (log.isQuiet())
            U.quiet(false, "VM information: " + U.jdkString());

        if (log.isInfoEnabled()) {
            log.info("Language runtime: " + getLanguage());
            log.info("VM information: " + U.jdkString());
            log.info("VM total memory: " + U.heapSize(2) + "GB");
        }
    }

    /**
     * @return Language runtime.
     */
    @SuppressWarnings("ThrowableInstanceNeverThrown")
    private String getLanguage() {
        boolean scala = false;
        boolean groovy = false;
        boolean clojure = false;

        for (StackTraceElement elem : Thread.currentThread().getStackTrace()) {
            String s = elem.getClassName().toLowerCase();

            if (s.contains("scala")) {
                scala = true;

                break;
            }
            else if (s.contains("groovy")) {
                groovy = true;

                break;
            }
            else if (s.contains("clojure")) {
                clojure = true;

                break;
            }
        }

        if (scala) {
            try (InputStream in = getClass().getResourceAsStream("/library.properties")) {
                Properties props = new Properties();

                if (in != null)
                    props.load(in);

                return "Scala ver. " + props.getProperty("version.number", "<unknown>");
            }
            catch (Exception ignore) {
                return "Scala ver. <unknown>";
            }
        }

        // How to get Groovy and Clojure version at runtime?!?
        return groovy ? "Groovy" : clojure ? "Clojure" : U.jdkName() + " ver. " + U.jdkVersion();
    }

    /**
     * Stops grid instance.
     *
     * @param cancel Whether or not to cancel running jobs.
     */
    public void stop(boolean cancel) {
        // Make sure that thread stopping grid is not interrupted.
        boolean interrupted = Thread.interrupted();

        try {
            stop0(cancel);
        }
        finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    /**
     * @return {@code True} if node started shutdown sequence.
     */
    public boolean isStopping() {
        return stopGuard.get();
    }

    /**
     * @param cancel Whether or not to cancel running jobs.
     */
    private void stop0(boolean cancel) {
        gw.compareAndSet(null, new GridKernalGatewayImpl(gridName));

        GridKernalGateway gw = this.gw.get();

        if (stopGuard.compareAndSet(false, true)) {
            // Only one thread is allowed to perform stop sequence.
            boolean firstStop = false;

            GridKernalState state = gw.getState();

            if (state == STARTED || state == DISCONNECTED)
                firstStop = true;
            else if (state == STARTING)
                U.warn(log, "Attempt to stop starting grid. This operation " +
                    "cannot be guaranteed to be successful.");

            if (firstStop) {
                // Notify lifecycle beans.
                if (log.isDebugEnabled())
                    log.debug("Notifying lifecycle beans.");

                notifyLifecycleBeansEx(LifecycleEventType.BEFORE_NODE_STOP);
            }

            List<GridComponent> comps = ctx.components();

            ctx.marshallerContext().onKernalStop();

            // Callback component in reverse order while kernal is still functional
            // if called in the same thread, at least.
            for (ListIterator<GridComponent> it = comps.listIterator(comps.size()); it.hasPrevious();) {
                GridComponent comp = it.previous();

                try {
                    if (!skipDaemon(comp))
                        comp.onKernalStop(cancel);
                }
                catch (Throwable e) {
                    errOnStop = true;

                    U.error(log, "Failed to pre-stop processor: " + comp, e);

                    if (e instanceof Error)
                        throw e;
                }
            }

            if (starveTask != null)
                starveTask.close();

            if (metricsLogTask != null)
                metricsLogTask.close();

            if (longOpDumpTask != null)
                longOpDumpTask.close();

            boolean interrupted = false;

            while (true) {
                try {
                    if (gw.tryWriteLock(10))
                        break;
                }
                catch (InterruptedException ignored) {
                    // Preserve interrupt status & ignore.
                    // Note that interrupted flag is cleared.
                    interrupted = true;
                }
            }

            if (interrupted)
                Thread.currentThread().interrupt();

            try {
                assert gw.getState() == STARTED || gw.getState() == STARTING || gw.getState() == DISCONNECTED;

                // No more kernal calls from this point on.
                gw.setState(STOPPING);

                ctx.cluster().get().clearNodeMap();

                if (log.isDebugEnabled())
                    log.debug("Grid " + (gridName == null ? "" : '\'' + gridName + "' ") + "is stopping.");
            }
            finally {
                gw.writeUnlock();
            }

            // Stopping cache operations.
            GridCacheProcessor cache = ctx.cache();

            if (cache != null)
                cache.blockGateways();

            // Unregister MBeans.
            if (!(
                unregisterMBean(pubExecSvcMBean) &
                    unregisterMBean(sysExecSvcMBean) &
                    unregisterMBean(mgmtExecSvcMBean) &
                    unregisterMBean(p2PExecSvcMBean) &
                    unregisterMBean(kernalMBean) &
                    unregisterMBean(locNodeMBean) &
                    unregisterMBean(restExecSvcMBean) &
                    unregisterMBean(stripedExecSvcMBean)
            ))
                errOnStop = false;

            // Stop components in reverse order.
            for (ListIterator<GridComponent> it = comps.listIterator(comps.size()); it.hasPrevious();) {
                GridComponent comp = it.previous();

                try {
                    if (!skipDaemon(comp)) {
                        comp.stop(cancel);

                        if (log.isDebugEnabled())
                            log.debug("Component stopped: " + comp);
                    }
                }
                catch (Throwable e) {
                    errOnStop = true;

                    U.error(log, "Failed to stop component (ignoring): " + comp, e);

                    if (e instanceof Error)
                        throw (Error)e;
                }
            }

            // Stops lifecycle aware components.
            U.stopLifecycleAware(log, lifecycleAwares(cfg));

            // Lifecycle notification.
            notifyLifecycleBeansEx(LifecycleEventType.AFTER_NODE_STOP);

            // Clean internal class/classloader caches to avoid stopped contexts held in memory.
            U.clearClassCache();
            MarshallerExclusions.clearCache();
            BinaryEnumCache.clear();

            gw.writeLock();

            try {
                gw.setState(STOPPED);
            }
            finally {
                gw.writeUnlock();
            }

            // Ack stop.
            if (log.isQuiet()) {
                String nodeName = gridName == null ? "" : "name=" + gridName + ", ";

                if (!errOnStop)
                    U.quiet(false, "Ignite node stopped OK [" + nodeName + "uptime=" +
                        X.timeSpan2HMSM(U.currentTimeMillis() - startTime) + ']');
                else
                    U.quiet(true, "Ignite node stopped wih ERRORS [" + nodeName + "uptime=" +
                        X.timeSpan2HMSM(U.currentTimeMillis() - startTime) + ']');
            }

            if (log.isInfoEnabled())
                if (!errOnStop) {
                    String ack = "Ignite ver. " + VER_STR + '#' + BUILD_TSTAMP_STR + "-sha1:" + REV_HASH_STR +
                        " stopped OK";

                    String dash = U.dash(ack.length());

                    log.info(NL + NL +
                        ">>> " + dash + NL +
                        ">>> " + ack + NL +
                        ">>> " + dash + NL +
                        (gridName == null ? "" : ">>> Grid name: " + gridName + NL) +
                        ">>> Grid uptime: " + X.timeSpan2HMSM(U.currentTimeMillis() - startTime) +
                        NL +
                        NL);
                }
                else {
                    String ack = "Ignite ver. " + VER_STR + '#' + BUILD_TSTAMP_STR + "-sha1:" + REV_HASH_STR +
                        " stopped with ERRORS";

                    String dash = U.dash(ack.length());

                    log.info(NL + NL +
                        ">>> " + ack + NL +
                        ">>> " + dash + NL +
                        (gridName == null ? "" : ">>> Grid name: " + gridName + NL) +
                        ">>> Grid uptime: " + X.timeSpan2HMSM(U.currentTimeMillis() - startTime) +
                        NL +
                        ">>> See log above for detailed error message." + NL +
                        ">>> Note that some errors during stop can prevent grid from" + NL +
                        ">>> maintaining correct topology since this node may have" + NL +
                        ">>> not exited grid properly." + NL +
                        NL);
                }

            try {
                U.onGridStop();
            }
            catch (InterruptedException ignored) {
                // Preserve interrupt status.
                Thread.currentThread().interrupt();
            }
        }
        else {
            // Proper notification.
            if (log.isDebugEnabled()) {
                if (gw.getState() == STOPPED)
                    log.debug("Grid is already stopped. Nothing to do.");
                else
                    log.debug("Grid is being stopped by another thread. Aborting this stop sequence " +
                        "allowing other thread to finish.");
            }
        }
    }

    /**
     * USED ONLY FOR TESTING.
     *
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Internal cache instance.
     */
    /*@java.test.only*/
    public <K, V> GridCacheAdapter<K, V> internalCache() {
        return internalCache(null);
    }

    /**
     * USED ONLY FOR TESTING.
     *
     * @param name Cache name.
     * @param <K>  Key type.
     * @param <V>  Value type.
     * @return Internal cache instance.
     */
    /*@java.test.only*/
    public <K, V> GridCacheAdapter<K, V> internalCache(@Nullable String name) {
        return ctx.cache().internalCache(name);
    }

    /**
     * It's intended for use by internal marshalling implementation only.
     *
     * @return Kernal context.
     */
    @Override public GridKernalContext context() {
        return ctx;
    }

    /**
     * Prints all system properties in debug mode.
     */
    private void ackSystemProperties() {
        assert log != null;

        if (log.isDebugEnabled())
            for (Map.Entry<Object, Object> entry : snapshot().entrySet())
                log.debug("System property [" + entry.getKey() + '=' + entry.getValue() + ']');
    }

    /**
     * Prints all user attributes in info mode.
     */
    private void logNodeUserAttributes() {
        assert log != null;

        if (log.isInfoEnabled())
            for (Map.Entry<?, ?> attr : cfg.getUserAttributes().entrySet())
                log.info("Local node user attribute [" + attr.getKey() + '=' + attr.getValue() + ']');
    }

    /**
     * Prints all environment variables in debug mode.
     */
    private void ackEnvironmentVariables() {
        assert log != null;

        if (log.isDebugEnabled())
            for (Map.Entry<?, ?> envVar : System.getenv().entrySet())
                log.debug("Environment variable [" + envVar.getKey() + '=' + envVar.getValue() + ']');
    }

    /**
     * Acks daemon mode status.
     */
    private void ackDaemon() {
        assert log != null;

        if (log.isInfoEnabled())
            log.info("Daemon mode: " + (isDaemon() ? "on" : "off"));
    }

    /**
     *
     * @return {@code True} is this node is daemon.
     */
    private boolean isDaemon() {
        assert cfg != null;

        return cfg.isDaemon() || "true".equalsIgnoreCase(System.getProperty(IGNITE_DAEMON));
    }

    /**
     * Whether or not remote JMX management is enabled for this node. Remote JMX management is
     * enabled when the following system property is set:
     * <ul>
     *     <li>{@code com.sun.management.jmxremote}</li>
     * </ul>
     *
     * @return {@code True} if remote JMX management is enabled - {@code false} otherwise.
     */
    @Override public boolean isJmxRemoteEnabled() {
        return System.getProperty("com.sun.management.jmxremote") != null;
    }

    /**
     * Whether or not node restart is enabled. Node restart us supported when this node was started
     * with {@code bin/ignite.{sh|bat}} script using {@code -r} argument. Node can be
     * programmatically restarted using {@link Ignition#restart(boolean)}} method.
     *
     * @return {@code True} if restart mode is enabled, {@code false} otherwise.
     * @see Ignition#restart(boolean)
     */
    @Override public boolean isRestartEnabled() {
        return System.getProperty(IGNITE_SUCCESS_FILE) != null;
    }

    /**
     * Prints all configuration properties in info mode and SPIs in debug mode.
     */
    private void ackSpis() {
        assert log != null;

        if (log.isDebugEnabled()) {
            log.debug("+-------------+");
            log.debug("START SPI LIST:");
            log.debug("+-------------+");
            log.debug("Grid checkpoint SPI     : " + Arrays.toString(cfg.getCheckpointSpi()));
            log.debug("Grid collision SPI      : " + cfg.getCollisionSpi());
            log.debug("Grid communication SPI  : " + cfg.getCommunicationSpi());
            log.debug("Grid deployment SPI     : " + cfg.getDeploymentSpi());
            log.debug("Grid discovery SPI      : " + cfg.getDiscoverySpi());
            log.debug("Grid event storage SPI  : " + cfg.getEventStorageSpi());
            log.debug("Grid failover SPI       : " + Arrays.toString(cfg.getFailoverSpi()));
            log.debug("Grid load balancing SPI : " + Arrays.toString(cfg.getLoadBalancingSpi()));
            log.debug("Grid swap space SPI     : " + cfg.getSwapSpaceSpi());
        }
    }

    /**
     *
     */
    private void ackRebalanceConfiguration() throws IgniteCheckedException {
        if (cfg.getSystemThreadPoolSize() <= cfg.getRebalanceThreadPoolSize())
            throw new IgniteCheckedException("Rebalance thread pool size exceed or equals System thread pool size. " +
                "Change IgniteConfiguration.rebalanceThreadPoolSize property before next start.");

        if (cfg.getRebalanceThreadPoolSize() < 1)
            throw new IgniteCheckedException("Rebalance thread pool size minimal allowed value is 1. " +
                "Change IgniteConfiguration.rebalanceThreadPoolSize property before next start.");

        for (CacheConfiguration ccfg : cfg.getCacheConfiguration()){
            if (ccfg.getRebalanceBatchesPrefetchCount() < 1)
                throw new IgniteCheckedException("Rebalance batches prefetch count minimal allowed value is 1. " +
                    "Change CacheConfiguration.rebalanceBatchesPrefetchCount property before next start. " +
                    "[cache="+ccfg.getName()+"]");
        }
    }

    /**
     *
     */
    private void ackCacheConfiguration() {
        CacheConfiguration[] cacheCfgs = cfg.getCacheConfiguration();

        if (cacheCfgs == null || cacheCfgs.length == 0)
            U.warn(log, "Cache is not configured - in-memory data grid is off.");
        else {
            SB sb = new SB();

            for (CacheConfiguration c : cacheCfgs) {
                String name = U.maskName(c.getName());

                sb.a("'").a(name).a("', ");
            }

            String names = sb.toString();

            U.log(log, "Configured caches [" + names.substring(0, names.length() - 2) + ']');
        }
    }

    /**
     *
     */
    private void ackP2pConfiguration() {
        assert cfg != null;

        if (cfg.isPeerClassLoadingEnabled())
            U.warn(
                log,
                "Peer class loading is enabled (disable it in production for performance and " +
                    "deployment consistency reasons)",
                "Peer class loading is enabled (disable it for better performance)"
            );
    }

    /**
     * Prints security status.
     */
    private void ackSecurity() {
        assert log != null;

        U.quietAndInfo(log, "Security status [authentication=" + onOff(ctx.security().enabled())
            + ", tls/ssl=" + onOff(ctx.config().getSslContextFactory() != null) + ']');
    }

    /**
     * Prints out VM arguments and IGNITE_HOME in info mode.
     *
     * @param rtBean Java runtime bean.
     */
    private void ackVmArguments(RuntimeMXBean rtBean) {
        assert log != null;

        // Ack IGNITE_HOME and VM arguments.
        if (log.isInfoEnabled()) {
            log.info("IGNITE_HOME=" + cfg.getIgniteHome());
            log.info("VM arguments: " + rtBean.getInputArguments());
        }
    }

    /**
     * Prints out class paths in debug mode.
     *
     * @param rtBean Java runtime bean.
     */
    private void ackClassPaths(RuntimeMXBean rtBean) {
        assert log != null;

        // Ack all class paths.
        if (log.isDebugEnabled()) {
            log.debug("Boot class path: " + rtBean.getBootClassPath());
            log.debug("Class path: " + rtBean.getClassPath());
            log.debug("Library path: " + rtBean.getLibraryPath());
        }
    }

    /**
     * @param cfg Grid configuration.
     * @return Components provided in configuration which can implement {@link LifecycleAware} interface.
     */
    private Iterable<Object> lifecycleAwares(IgniteConfiguration cfg) {
        Collection<Object> objs = new ArrayList<>();

        if (cfg.getLifecycleBeans() != null)
            Collections.addAll(objs, cfg.getLifecycleBeans());

        if (cfg.getSegmentationResolvers() != null)
            Collections.addAll(objs, cfg.getSegmentationResolvers());

        if (cfg.getConnectorConfiguration() != null) {
            objs.add(cfg.getConnectorConfiguration().getMessageInterceptor());
            objs.add(cfg.getConnectorConfiguration().getSslContextFactory());
        }

        objs.add(cfg.getMarshaller());
        objs.add(cfg.getGridLogger());
        objs.add(cfg.getMBeanServer());

        return objs;
    }

    /** {@inheritDoc} */
    @Override public IgniteConfiguration configuration() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log() {
        return cfg.getGridLogger();
    }

    /** {@inheritDoc} */
    @Override public boolean removeCheckpoint(String key) {
        A.notNull(key, "key");

        guard();

        try {
            return ctx.checkpoint().removeCheckpoint(key);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean pingNode(String nodeId) {
        A.notNull(nodeId, "nodeId");

        return cluster().pingNode(UUID.fromString(nodeId));
    }

    /** {@inheritDoc} */
    @Override public void undeployTaskFromGrid(String taskName) throws JMException {
        A.notNull(taskName, "taskName");

        try {
            compute().undeployTask(taskName);
        }
        catch (IgniteException e) {
            throw U.jmException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public String executeTask(String taskName, String arg) throws JMException {
        try {
            return compute().execute(taskName, arg);
        }
        catch (IgniteException e) {
            throw U.jmException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean pingNodeByAddress(String host) {
        guard();

        try {
            for (ClusterNode n : cluster().nodes())
                if (n.addresses().contains(host))
                    return ctx.discovery().pingNode(n.id());

            return false;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean eventUserRecordable(int type) {
        guard();

        try {
            return ctx.event().isUserRecordable(type);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean allEventsUserRecordable(int[] types) {
        A.notNull(types, "types");

        guard();

        try {
            return ctx.event().isAllUserRecordable(types);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteTransactions transactions() {
        guard();

        try {
            return ctx.cache().transactions();
        }
        finally {
            unguard();
        }
    }

    /**
     * @param name Cache name.
     * @return Cache.
     */
    public <K, V> IgniteInternalCache<K, V> getCache(@Nullable String name) {
        guard();

        try {
            return ctx.cache().publicCache(name);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> cache(@Nullable String name) {
        guard();

        try {
            return ctx.cache().publicJCache(name, false, true);
        }
        catch (IgniteCheckedException e) {
            throw CU.convertToCacheException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> cacheCfg) {
        A.notNull(cacheCfg, "cacheCfg");

        guard();

        try {
            ctx.cache().dynamicStartCache(cacheCfg,
                cacheCfg.getName(),
                null,
                true,
                true,
                true).get();

            return ctx.cache().publicJCache(cacheCfg.getName());
        }
        catch (IgniteCheckedException e) {
            throw CU.convertToCacheException(e);
        }
        finally {
            unguard();
        }
    }


    /** {@inheritDoc} */
    @Override public Collection<IgniteCache> createCaches(Collection<CacheConfiguration> cacheCfgs) {
        A.notNull(cacheCfgs, "cacheCfgs");

        guard();

        try {
            ctx.cache().dynamicStartCaches(cacheCfgs,
                true,
                true).get();

            List<IgniteCache> createdCaches = new ArrayList<>(cacheCfgs.size());

            for (CacheConfiguration cacheCfg : cacheCfgs)
                createdCaches.add(ctx.cache().publicJCache(cacheCfg.getName()));

            return createdCaches;
        }
        catch (IgniteCheckedException e) {
            throw CU.convertToCacheException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(String cacheName) {
        guard();

        try {
            ctx.cache().createFromTemplate(cacheName).get();

            return ctx.cache().publicJCache(cacheName);
        }
        catch (IgniteCheckedException e) {
            throw CU.convertToCacheException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg) {
        A.notNull(cacheCfg, "cacheCfg");

        guard();

        try {
            if (ctx.cache().cache(cacheCfg.getName()) == null) {
                ctx.cache().dynamicStartCache(cacheCfg,
                    cacheCfg.getName(),
                    null,
                    false,
                    true,
                    true).get();
            }

            return ctx.cache().publicJCache(cacheCfg.getName());
        }
        catch (IgniteCheckedException e) {
            throw CU.convertToCacheException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteCache> getOrCreateCaches(Collection<CacheConfiguration> cacheCfgs) {
        A.notNull(cacheCfgs, "cacheCfgs");

        guard();

        try {
            ctx.cache().dynamicStartCaches(cacheCfgs,
                false,
                true).get();

            List<IgniteCache> createdCaches = new ArrayList<>(cacheCfgs.size());

            for (CacheConfiguration cacheCfg : cacheCfgs)
                createdCaches.add(ctx.cache().publicJCache(cacheCfg.getName()));

            return createdCaches;
        }
        catch (IgniteCheckedException e) {
            throw CU.convertToCacheException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(
        CacheConfiguration<K, V> cacheCfg,
        NearCacheConfiguration<K, V> nearCfg
    ) {
        A.notNull(cacheCfg, "cacheCfg");
        A.notNull(nearCfg, "nearCfg");

        guard();

        try {
            ctx.cache().dynamicStartCache(cacheCfg,
                cacheCfg.getName(),
                nearCfg,
                true,
                true,
                true).get();

            return ctx.cache().publicJCache(cacheCfg.getName());
        }
        catch (IgniteCheckedException e) {
            throw CU.convertToCacheException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg,
        NearCacheConfiguration<K, V> nearCfg) {
        A.notNull(cacheCfg, "cacheCfg");
        A.notNull(nearCfg, "nearCfg");

        guard();

        try {
            IgniteInternalCache<Object, Object> cache = ctx.cache().cache(cacheCfg.getName());

            if (cache == null) {
                ctx.cache().dynamicStartCache(cacheCfg,
                    cacheCfg.getName(),
                    nearCfg,
                    false,
                    true,
                    true).get();
            }
            else {
                if (cache.configuration().getNearConfiguration() == null) {
                    ctx.cache().dynamicStartCache(cacheCfg,
                        cacheCfg.getName(),
                        nearCfg,
                        false,
                        true,
                        true).get();
                }
            }

            return ctx.cache().publicJCache(cacheCfg.getName());
        }
        catch (IgniteCheckedException e) {
            throw CU.convertToCacheException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createNearCache(String cacheName, NearCacheConfiguration<K, V> nearCfg) {
        A.notNull(nearCfg, "nearCfg");

        guard();

        try {
            ctx.cache().dynamicStartCache(null,
                cacheName,
                nearCfg,
                true,
                true,
                true).get();

            IgniteCacheProxy<K, V> cache = ctx.cache().publicJCache(cacheName);

            checkNearCacheStarted(cache);

            return cache;
        }
        catch (IgniteCheckedException e) {
            throw CU.convertToCacheException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateNearCache(@Nullable String cacheName,
        NearCacheConfiguration<K, V> nearCfg) {
        A.notNull(nearCfg, "nearCfg");

        guard();

        try {
            IgniteInternalCache<Object, Object> internalCache = ctx.cache().cache(cacheName);

            if (internalCache == null) {
                ctx.cache().dynamicStartCache(null,
                    cacheName,
                    nearCfg,
                    false,
                    true,
                    true).get();
            }
            else {
                if (internalCache.configuration().getNearConfiguration() == null) {
                    ctx.cache().dynamicStartCache(null,
                        cacheName,
                        nearCfg,
                        false,
                        true,
                        true).get();
                }
            }

            IgniteCacheProxy<K, V> cache = ctx.cache().publicJCache(cacheName);

            checkNearCacheStarted(cache);

            return cache;
        }
        catch (IgniteCheckedException e) {
            throw CU.convertToCacheException(e);
        }
        finally {
            unguard();
        }
    }

    /**
     * @param cache Cache.
     * @throws IgniteCheckedException If cache without near cache was already started.
     */
    private void checkNearCacheStarted(IgniteCacheProxy<?, ?> cache) throws IgniteCheckedException {
        if (!cache.context().isNear())
            throw new IgniteCheckedException("Failed to start near cache " +
                "(a cache with the same name without near cache is already started)");
    }

    /** {@inheritDoc} */
    @Override public void destroyCache(String cacheName) {
        IgniteInternalFuture stopFut = destroyCacheAsync(cacheName, true);

        try {
            stopFut.get();
        }
        catch (IgniteCheckedException e) {
            throw CU.convertToCacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void destroyCaches(Collection<String> cacheNames) {
        IgniteInternalFuture stopFut = destroyCachesAsync(cacheNames, true);

        try {
            stopFut.get();
        }
        catch (IgniteCheckedException e) {
            throw CU.convertToCacheException(e);
        }
    }

    /**
     * @param cacheName Cache name.
     * @param checkThreadTx If {@code true} checks that current thread does not have active transactions.
     * @return Ignite future.
     */
    public IgniteInternalFuture<?> destroyCacheAsync(String cacheName, boolean checkThreadTx) {
        guard();

        try {
            return ctx.cache().dynamicDestroyCache(cacheName, checkThreadTx);
        }
        finally {
            unguard();
        }
    }

    /**
     * @param cacheNames Collection of cache names.
     * @param checkThreadTx If {@code true} checks that current thread does not have active transactions.
     * @return Ignite future.
     */
    public IgniteInternalFuture<?> destroyCachesAsync(Collection<String> cacheNames, boolean checkThreadTx) {
        guard();

        try {
            return ctx.cache().dynamicDestroyCaches(cacheNames, checkThreadTx);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(String cacheName) {
        guard();

        try {
            if (ctx.cache().cache(cacheName) == null)
                ctx.cache().getOrCreateFromTemplate(cacheName, true).get();

            return ctx.cache().publicJCache(cacheName);
        }
        catch (IgniteCheckedException e) {
            throw CU.convertToCacheException(e);
        }
        finally {
            unguard();
        }
    }

    /**
     * @param cacheName Cache name.
     * @param checkThreadTx If {@code true} checks that current thread does not have active transactions.
     * @return Future that will be completed when cache is deployed.
     */
    public IgniteInternalFuture<?> getOrCreateCacheAsync(String cacheName, boolean checkThreadTx) {
        guard();

        try {
            if (ctx.cache().cache(cacheName) == null)
                return ctx.cache().getOrCreateFromTemplate(cacheName, checkThreadTx);

            return new GridFinishedFuture<>();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> void addCacheConfiguration(CacheConfiguration<K, V> cacheCfg) {
        A.notNull(cacheCfg, "cacheCfg");

        guard();

        try {
            ctx.cache().addCacheConfiguration(cacheCfg);
        }
        catch (IgniteCheckedException e) {
            throw CU.convertToCacheException(e);
        }
        finally {
            unguard();
        }
    }

    /**
     * @return Public caches.
     */
    public Collection<IgniteCacheProxy<?, ?>> caches() {
        guard();

        try {
            return ctx.cache().publicCaches();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<String> cacheNames() {
        guard();

        try {
            return ctx.cache().publicCacheNames();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K extends GridCacheUtilityKey, V> IgniteInternalCache<K, V> utilityCache() {
        guard();

        try {
            return ctx.cache().utilityCache();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteInternalCache<K, V> cachex(@Nullable String name) {
        guard();

        try {
            return ctx.cache().cache(name);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteInternalCache<K, V> cachex() {
        guard();

        try {
            return ctx.cache().cache();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteInternalCache<?, ?>> cachesx(IgnitePredicate<? super IgniteInternalCache<?, ?>>[] p) {
        guard();

        try {
            return F.retain(ctx.cache().caches(), true, p);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteDataStreamer<K, V> dataStreamer(@Nullable String cacheName) {
        guard();

        try {
            return ctx.<K, V>dataStream().dataStreamer(cacheName);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFileSystem fileSystem(String name) {
        guard();

        try{
            IgniteFileSystem fs = ctx.igfs().igfs(name);

            if (fs == null)
                throw new IllegalArgumentException("IGFS is not configured: " + name);

            return fs;
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFileSystem igfsx(@Nullable String name) {
        guard();

        try {
            return ctx.igfs().igfs(name);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFileSystem> fileSystems() {
        guard();

        try {
            return ctx.igfs().igfss();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Hadoop hadoop() {
        guard();

        try {
            return ctx.hadoop().hadoop();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T extends IgnitePlugin> T plugin(String name) throws PluginNotFoundException {
        guard();

        try {
            return (T)ctx.pluginProvider(name).plugin();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteBinary binary() {
        IgniteCacheObjectProcessor objProc = ctx.cacheObjects();

        return objProc.binary();
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        return VER;
    }

    /** {@inheritDoc} */
    @Override public String latestVersion() {
        ctx.gateway().readLock();

        try {
            return ctx.cluster().latestVersion();
        }
        finally {
            ctx.gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteScheduler scheduler() {
        return scheduler;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteException {
        Ignition.stop(gridName, true);
    }

    /** {@inheritDoc} */
    @Override public <K> Affinity<K> affinity(String cacheName) {
        GridCacheAdapter<K, ?> cache = ctx.cache().internalCache(cacheName);

        if (cache != null)
            return cache.affinity();

        return ctx.affinity().affinityProxy(cacheName);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteAtomicSequence atomicSequence(String name, long initVal, boolean create) {
        guard();

        try {
            return ctx.dataStructures().sequence(name, initVal, create);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteAtomicLong atomicLong(String name, long initVal, boolean create) {
        guard();

        try {
            return ctx.dataStructures().atomicLong(name, initVal, create);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> IgniteAtomicReference<T> atomicReference(String name,
        @Nullable T initVal,
        boolean create)
    {
        guard();

        try {
            return ctx.dataStructures().atomicReference(name, initVal, create);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name,
        @Nullable T initVal,
        @Nullable S initStamp,
        boolean create) {
        guard();

        try {
            return ctx.dataStructures().atomicStamped(name, initVal, initStamp, create);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteCountDownLatch countDownLatch(String name,
        int cnt,
        boolean autoDel,
        boolean create) {
        guard();

        try {
            return ctx.dataStructures().countDownLatch(name, cnt, autoDel, create);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteSemaphore semaphore(
        String name,
        int cnt,
        boolean failoverSafe,
        boolean create
    ) {
        guard();

        try {
            return ctx.dataStructures().semaphore(name, cnt, failoverSafe, create);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteLock reentrantLock(
        String name,
        boolean failoverSafe,
        boolean fair,
        boolean create
    ) {
        guard();

        try {
            return ctx.dataStructures().reentrantLock(name, failoverSafe, fair, create);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> IgniteQueue<T> queue(String name,
        int cap,
        CollectionConfiguration cfg)
    {
        guard();

        try {
            return ctx.dataStructures().queue(name, cap, cfg);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> IgniteSet<T> set(String name,
        CollectionConfiguration cfg)
    {
        guard();

        try {
            return ctx.dataStructures().set(name, cfg);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /**
     * <tt>ctx.gateway().readLock()</tt>
     */
    private void guard() {
        assert ctx != null;

        ctx.gateway().readLock();
    }

    /**
     * <tt>ctx.gateway().readUnlock()</tt>
     */
    private void unguard() {
        assert ctx != null;

        ctx.gateway().readUnlock();
    }

    /**
     *
     */
    public void onDisconnected() {
        Throwable err = null;

        GridFutureAdapter<?> reconnectFut = ctx.gateway().onDisconnected();

        if (reconnectFut == null) {
            assert ctx.gateway().getState() != STARTED : ctx.gateway().getState();

            return;
        }

        IgniteFuture<?> userFut = new IgniteFutureImpl<>(reconnectFut);

        ctx.cluster().get().clientReconnectFuture(userFut);

        ctx.disconnected(true);

        List<GridComponent> comps = ctx.components();

        for (ListIterator<GridComponent> it = comps.listIterator(comps.size()); it.hasPrevious();) {
            GridComponent comp = it.previous();

            try {
                if (!skipDaemon(comp))
                    comp.onDisconnected(userFut);
            }
            catch (IgniteCheckedException e) {
                err = e;
            }
            catch (Throwable e) {
                err = e;

                if (e instanceof Error)
                    throw e;
            }
        }

        for (GridCacheContext cctx : ctx.cache().context().cacheContexts()) {
            cctx.gate().writeLock();

            cctx.gate().writeUnlock();
        }

        ctx.gateway().writeLock();

        ctx.gateway().writeUnlock();

        if (err != null) {
            reconnectFut.onDone(err);

            U.error(log, "Failed to reconnect, will stop node", err);

            close();
        }
    }

    /**
     * @param clusterRestarted {@code True} if all cluster nodes restarted while client was disconnected.
     */
    @SuppressWarnings("unchecked")
    public void onReconnected(final boolean clusterRestarted) {
        Throwable err = null;

        try {
            ctx.disconnected(false);

            GridCompoundFuture<?, ?> reconnectFut = new GridCompoundFuture<>();

            for (GridComponent comp : ctx.components()) {
                IgniteInternalFuture<?> fut = comp.onReconnected(clusterRestarted);

                if (fut != null)
                    reconnectFut.add((IgniteInternalFuture)fut);
            }

            reconnectFut.add((IgniteInternalFuture)ctx.cache().context().exchange().reconnectExchangeFuture());

            reconnectFut.markInitialized();

            reconnectFut.listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> fut) {
                    try {
                        fut.get();

                        ctx.gateway().onReconnected();
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to reconnect, will stop node", e);

                        close();
                    }
                }
            });
        }
        catch (IgniteCheckedException e) {
            err = e;
        }
        catch (Throwable e) {
            err = e;

            if (e instanceof Error)
                throw e;
        }

        if (err != null) {
            U.error(log, "Failed to reconnect, will stop node", err);

            close();
        }
    }

    /**
     * Creates optional component.
     *
     * @param cls Component interface.
     * @param ctx Kernal context.
     * @return Created component.
     * @throws IgniteCheckedException If failed to create component.
     */
    private static <T extends GridComponent> T createComponent(Class<T> cls, GridKernalContext ctx)
        throws IgniteCheckedException {
        assert cls.isInterface() : cls;

        T comp = ctx.plugins().createComponent(cls);

        if (comp != null)
            return comp;

        if (cls.equals(IgniteCacheObjectProcessor.class))
            return (T)new CacheObjectBinaryProcessorImpl(ctx);

        if (cls.equals(DiscoveryNodeValidationProcessor.class))
            return (T)new OsDiscoveryNodeValidationProcessor(ctx);

        Class<T> implCls = null;

        try {
            String clsName;

            // Handle special case for PlatformProcessor
            if (cls.equals(PlatformProcessor.class))
                clsName = ctx.config().getPlatformConfiguration() == null ?
                    PlatformNoopProcessor.class.getName() : cls.getName() + "Impl";
            else
                clsName = componentClassName(cls);

            implCls = (Class<T>)Class.forName(clsName);
        }
        catch (ClassNotFoundException ignore) {
            // No-op.
        }

        if (implCls == null)
            throw new IgniteCheckedException("Failed to find component implementation: " + cls.getName());

        if (!cls.isAssignableFrom(implCls))
            throw new IgniteCheckedException("Component implementation does not implement component interface " +
                "[component=" + cls.getName() + ", implementation=" + implCls.getName() + ']');

        Constructor<T> constructor;

        try {
            constructor = implCls.getConstructor(GridKernalContext.class);
        }
        catch (NoSuchMethodException e) {
            throw new IgniteCheckedException("Component does not have expected constructor: " + implCls.getName(), e);
        }

        try {
            return constructor.newInstance(ctx);
        }
        catch (ReflectiveOperationException e) {
            throw new IgniteCheckedException("Failed to create component [component=" + cls.getName() +
                ", implementation=" + implCls.getName() + ']', e);
        }
    }

    /**
     * @param cls Component interface.
     * @return Name of component implementation class for open source edition.
     */
    private static String componentClassName(Class<?> cls) {
        return cls.getPackage().getName() + ".os." + cls.getSimpleName().replace("Grid", "GridOs");
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        gridName = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, gridName);
    }

    /**
     * @return IgniteKernal instance.
     *
     * @throws ObjectStreamException If failed.
     */
    protected Object readResolve() throws ObjectStreamException {
        try {
            return IgnitionEx.localIgnite();
        }
        catch (IllegalStateException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
    }

    /**
     * @param comp Grid component.
     * @return {@code true} if node running in daemon mode and component marked by {@code SkipDaemon} annotation.
     */
    private boolean skipDaemon(GridComponent comp) {
        return ctx.isDaemon() && U.hasAnnotation(comp.getClass(), SkipDaemon.class);
    }

    /** {@inheritDoc} */
    public void dumpDebugInfo() {
        try {
            GridKernalContextImpl ctx = this.ctx;

            GridDiscoveryManager discoMrg = ctx != null ? ctx.discovery() : null;

            ClusterNode locNode = discoMrg != null ? discoMrg.localNode() : null;

            if (ctx != null && discoMrg != null && locNode != null) {
                boolean client = ctx.clientNode();

                UUID routerId = locNode instanceof TcpDiscoveryNode ? ((TcpDiscoveryNode)locNode).clientRouterNodeId() : null;

                U.warn(log, "Dumping debug info for node [id=" + locNode.id() +
                    ", name=" + ctx.gridName() +
                    ", order=" + locNode.order() +
                    ", topVer=" + discoMrg.topologyVersion() +
                    ", client=" + client +
                    (client && routerId != null ? ", routerId=" + routerId : "") + ']');

                ctx.cache().context().exchange().dumpDebugInfo();
            }
            else
                U.warn(log, "Dumping debug info for node, context is not initialized [name=" + gridName + ']');
        }
        catch (Exception e) {
            U.error(log, "Failed to dump debug info for node: " + e, e);
        }
    }

    /**
     * @param node Node.
     * @param payload Message payload.
     * @param procFromNioThread If {@code true} message is processed from NIO thread.
     * @return Response future.
     */
    public IgniteInternalFuture sendIoTest(ClusterNode node, byte[] payload, boolean procFromNioThread) {
        return ctx.io().sendIoTest(node, payload, procFromNioThread);
    }

    /**
     * @param nodes Nodes.
     * @param payload Message payload.
     * @param procFromNioThread If {@code true} message is processed from NIO thread.
     * @return Response future.
     */
    public IgniteInternalFuture sendIoTest(List<ClusterNode> nodes, byte[] payload, boolean procFromNioThread) {
        return ctx.io().sendIoTest(nodes, payload, procFromNioThread);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteKernal.class, this);
    }
}
