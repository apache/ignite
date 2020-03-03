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
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Constructor;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.CacheException;
import javax.management.JMException;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.DataRegionMetricsAdapter;
import org.apache.ignite.DataStorageMetrics;
import org.apache.ignite.DataStorageMetricsAdapter;
import org.apache.ignite.Ignite;
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
import org.apache.ignite.IgniteEncryption;
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
import org.apache.ignite.MemoryMetrics;
import org.apache.ignite.PersistenceMetrics;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.binary.BinaryEnumCache;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.cluster.ClusterGroupAdapter;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.managers.GridManager;
import org.apache.ignite.internal.managers.IgniteMBeansManager;
import org.apache.ignite.internal.managers.checkpoint.GridCheckpointManager;
import org.apache.ignite.internal.managers.collision.GridCollisionManager;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.deployment.GridDeploymentManager;
import org.apache.ignite.internal.managers.discovery.DiscoveryLocalJoinData;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.managers.failover.GridFailoverManager;
import org.apache.ignite.internal.managers.indexing.GridIndexingManager;
import org.apache.ignite.internal.managers.loadbalancer.GridLoadBalancerManager;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.internal.processors.GridProcessor;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityProcessor;
import org.apache.ignite.internal.processors.authentication.IgniteAuthenticationProcessor;
import org.apache.ignite.internal.processors.cache.CacheConfigurationOverride;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.GridCacheUtilityKey;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.mvcc.MvccProcessorImpl;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.processors.closure.GridClosureProcessor;
import org.apache.ignite.internal.processors.cluster.ClusterProcessor;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor;
import org.apache.ignite.internal.processors.cluster.IGridClusterStateProcessor;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationProcessor;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor;
import org.apache.ignite.internal.processors.datastreamer.DataStreamProcessor;
import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.hadoop.Hadoop;
import org.apache.ignite.internal.processors.hadoop.HadoopProcessorAdapter;
import org.apache.ignite.internal.processors.job.GridJobProcessor;
import org.apache.ignite.internal.processors.jobmetrics.GridJobMetricsProcessor;
import org.apache.ignite.internal.processors.localtask.DurableBackgroundTasksProcessor;
import org.apache.ignite.internal.processors.marshaller.GridMarshallerMappingProcessor;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.nodevalidation.DiscoveryNodeValidationProcessor;
import org.apache.ignite.internal.processors.nodevalidation.OsDiscoveryNodeValidationProcessor;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.processors.platform.PlatformNoopProcessor;
import org.apache.ignite.internal.processors.platform.PlatformProcessor;
import org.apache.ignite.internal.processors.platform.plugin.PlatformPluginProcessor;
import org.apache.ignite.internal.processors.plugin.IgnitePluginProcessor;
import org.apache.ignite.internal.processors.pool.PoolProcessor;
import org.apache.ignite.internal.processors.port.GridPortProcessor;
import org.apache.ignite.internal.processors.port.GridPortRecord;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.resource.GridResourceProcessor;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.processors.rest.GridRestProcessor;
import org.apache.ignite.internal.processors.rest.IgniteRestProcessor;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.processors.security.IgniteSecurityProcessor;
import org.apache.ignite.internal.processors.security.NoOpIgniteSecurityProcessor;
import org.apache.ignite.internal.processors.segmentation.GridSegmentationProcessor;
import org.apache.ignite.internal.processors.service.GridServiceProcessor;
import org.apache.ignite.internal.processors.service.IgniteServiceProcessor;
import org.apache.ignite.internal.processors.session.GridTaskSessionProcessor;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.processors.task.GridTaskProcessor;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.suggestions.GridPerformanceSuggestions;
import org.apache.ignite.internal.suggestions.JvmConfigurationSuggestions;
import org.apache.ignite.internal.suggestions.OsConfigurationSuggestions;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.TimeBag;
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
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.marshaller.MarshallerExclusions;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.mxbean.IgniteMXBean;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginNotFoundException;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.IgniteSpiVersionCheckException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.thread.IgniteStripedThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.nonNull;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_CONFIG_URL;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DAEMON;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_LOG_CLASSPATH_CONTENT_ON_STARTUP;
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
import static org.apache.ignite.internal.IgniteComponentType.COMPRESSION;
import static org.apache.ignite.internal.IgniteComponentType.HADOOP_HELPER;
import static org.apache.ignite.internal.IgniteComponentType.IGFS;
import static org.apache.ignite.internal.IgniteComponentType.IGFS_HELPER;
import static org.apache.ignite.internal.IgniteComponentType.SCHEDULE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_DATE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_CLIENT_MODE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_CONSISTENCY_CHECK_SKIPPED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_DAEMON;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_DATA_STORAGE_CONFIG;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_DATA_STREAMER_POOL_SIZE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_DEPLOYMENT_MODE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_DYNAMIC_CACHE_START_ROLLBACK_SUPPORTED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_FEATURES;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;
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
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MEMORY_CONFIG;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_NODE_CONSISTENT_ID;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_OFFHEAP_SIZE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_PEER_CLASSLOADING;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_PHY_RAM;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_PREFIX;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REBALANCE_POOL_SIZE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_RESTART_ENABLED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_PORT_RANGE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SPI_CLASS;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_TX_CONFIG;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_USER_NAME;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_VALIDATE_CACHE_REQUESTS;
import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.BUILD_TSTAMP_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;
import static org.apache.ignite.internal.IgniteVersionUtils.REV_HASH_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.VER;
import static org.apache.ignite.internal.IgniteVersionUtils.VER_STR;
import static org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager.INTERNAL_DATA_REGION_NAMES;
import static org.apache.ignite.lifecycle.LifecycleEventType.AFTER_NODE_START;
import static org.apache.ignite.lifecycle.LifecycleEventType.BEFORE_NODE_START;

/**
 * This class represents an implementation of the main Ignite API {@link Ignite} which is expanded by additional
 * methods of {@link IgniteEx} for the internal Ignite needs. It also controls the Ignite life cycle, checks
 * thread pools state for starvation, detects long JVM pauses and prints out the local node metrics.
 * <p>
 * Please, refer to the wiki <a href="http://en.wikipedia.org/wiki/Kernal">http://en.wikipedia.org/wiki/Kernal</a>
 * for the information on the misspelling.
 * <p>
 * <h3>Starting</h3>
 * The main entry point for all the Ignite instances creation is the method - {@link #start}.
 * <p>
 * It starts internal Ignite components (see {@link GridComponent}), for instance:
 * <ul>
 * <li>{@link GridManager} - a layer of indirection between kernal and SPI modules.</li>
 * <li>{@link GridProcessor} - an objects responsible for particular internal process implementation.</li>
 * <li>{@link IgnitePlugin} - an Ignite addition of user-provided functionality.</li>
 * </ul>
 * The {@code start} method also performs additional validation of the provided {@link IgniteConfiguration} and
 * prints some suggestions such as:
 * <ul>
 * <li>Ignite configuration optimizations (e.g. disabling {@link EventType} events).</li>
 * <li>{@link JvmConfigurationSuggestions} optimizations.</li>
 * <li>{@link OsConfigurationSuggestions} optimizations.</li>
 * </ul>
 * <h3>Stopping</h3>
 * To stop Ignite instance the {@link #stop(boolean)} method is used. The {@code cancel} argument of this method is used:
 * <ul>
 * <li>With {@code true} value. To interrupt all currently acitve {@link GridComponent}s related to the Ignite node.
 * For instance, {@link ComputeJob} will be interrupted by calling {@link ComputeJob#cancel()} method. Note that just
 * like with {@link Thread#interrupt()}, it is up to the actual job to exit from execution.</li>
 * <li>With {@code false} value. To stop the Ignite node gracefully. All jobs currently running will not be interrupted.
 * The Ignite node will wait for the completion of all {@link GridComponent}s running on it before stopping.
 * </li>
 * </ul>
 */
public class IgniteKernal implements IgniteEx, IgniteMXBean, Externalizable {
    /** Class serialization version number. */
    private static final long serialVersionUID = 0L;

    /** Ignite web-site that is shown in log messages. */
    public static final String SITE = "ignite.apache.org";

    /** System line separator. */
    private static final String NL = U.nl();

    /** System megabyte. */
    private static final int MEGABYTE = 1024 * 1024;

    /**
     * Default interval of checking thread pool state for the starvation. Will be used only if the
     * {@link IgniteSystemProperties#IGNITE_STARVATION_CHECK_INTERVAL} system property is not set.
     * <p>
     * Value is {@code 30 sec}.
     */
    private static final long PERIODIC_STARVATION_CHECK_FREQ = 1000 * 30;

    /** Object is used to force completion the previous reconnection attempt. See {@link ReconnectState} for details. */
    private static final Object STOP_RECONNECT = new Object();

    /** The separator is used for coordinator properties formatted as a string. */
    public static final String COORDINATOR_PROPERTIES_SEPARATOR = ",";

    /**
     * Default timeout in milliseconds for dumping long running operations. Will be used if the
     * {@link IgniteSystemProperties#IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT} is not set.
     * <p>
     * Value is {@code 60 sec}.
     */
    public static final long DFLT_LONG_OPERATIONS_DUMP_TIMEOUT = 60_000L;

    /** Currently used instance of JVM pause detector thread. See {@link LongJVMPauseDetector} for details. */
    private LongJVMPauseDetector longJVMPauseDetector;

    /** The main kernal context which holds all the {@link GridComponent}s. */
    @GridToStringExclude
    private GridKernalContextImpl ctx;

    /** Helper which registers and unregisters MBeans. */
    @GridToStringExclude
    private IgniteMBeansManager mBeansMgr;

    /** Ignite configuration instance. */
    private IgniteConfiguration cfg;

    /** Ignite logger instance which enriches log messages with the node instance name and the node id. */
    @GridToStringExclude
    private GridLoggerProxy log;

    /** Name of Ignite node. */
    private String igniteInstanceName;

    /** Kernal start timestamp. */
    private long startTime = U.currentTimeMillis();

    /** Spring context, potentially {@code null}. */
    private GridSpringResourceContext rsrcCtx;

    /**
     * The instance of scheduled thread pool starvation checker. {@code null} if starvation checks have been
     * disabled by the value of {@link IgniteSystemProperties#IGNITE_STARVATION_CHECK_INTERVAL} system property.
     */
    @GridToStringExclude
    private GridTimeoutProcessor.CancelableTask starveTask;

    /**
     * The instance of scheduled metrics logger. {@code null} means that the metrics loggin have been disabled
     * by configuration. See {@link IgniteConfiguration#getMetricsLogFrequency()} for details.
     */
    @GridToStringExclude
    private GridTimeoutProcessor.CancelableTask metricsLogTask;

    /**
     * The instance of scheduled long operation checker. {@code null} means that the operations checker is disabled
     * by the value of {@link IgniteSystemProperties#IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT} system property.
     */
    @GridToStringExclude
    private GridTimeoutProcessor.CancelableTask longOpDumpTask;

    /** {@code true} if an error occurs at Ignite instance stop. */
    @GridToStringExclude
    private boolean errOnStop;

    /** An instance of the scheduler which provides functionality for scheduling jobs locally. */
    @GridToStringExclude
    private IgniteScheduler scheduler;

    /** The kernal state guard. See {@link GridKernalGateway} for details. */
    @GridToStringExclude
    private final AtomicReference<GridKernalGateway> gw = new AtomicReference<>();

    /** Flag indicates that the ignite instance is scheduled to be stopped. */
    @GridToStringExclude
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** The state object is used when reconnection occurs. See {@link IgniteKernal#onReconnected(boolean)}. */
    private final ReconnectState reconnectState = new ReconnectState();

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
        checkClusterState();

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
        return ((ClusterGroupAdapter)grp).events();
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services(ClusterGroup grp) {
        checkClusterState();

        return ((ClusterGroupAdapter)grp).services();
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService(ClusterGroup grp) {
        return ((ClusterGroupAdapter)grp).executorService();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return igniteInstanceName;
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
    @Override public boolean isRebalanceEnabled() {
        return ctx.cache().context().isRebalanceEnabled();
    }

    /** {@inheritDoc} */
    @Override public void rebalanceEnabled(boolean rebalanceEnabled) {
        ctx.cache().context().rebalanceEnabled(rebalanceEnabled);
    }

    /** {@inheritDoc} */
    @Override public long getUpTime() {
        return U.currentTimeMillis() - startTime;
    }

    /** {@inheritDoc} */
    @Override public long getLongJVMPausesCount() {
        return longJVMPauseDetector != null ? longJVMPauseDetector.longPausesCount() : 0;
    }

    /** {@inheritDoc} */
    @Override public long getLongJVMPausesTotalDuration() {
        return longJVMPauseDetector != null ? longJVMPauseDetector.longPausesTotalDuration() : 0;
    }

    /** {@inheritDoc} */
    @Override public Map<Long, Long> getLongJVMPauseLastEvents() {
        return longJVMPauseDetector != null ? longJVMPauseDetector.longPauseEvents() : Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public String getUpTimeFormatted() {
        return X.timeSpan2DHMSM(U.currentTimeMillis() - startTime);
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
    @Override public String getCurrentCoordinatorFormatted() {
        ClusterNode node = ctx.discovery().oldestAliveServerNode(AffinityTopologyVersion.NONE);

        if (node == null)
            return "";

        return new StringBuilder()
            .append(node.addresses())
            .append(COORDINATOR_PROPERTIES_SEPARATOR)
            .append(node.id())
            .append(COORDINATOR_PROPERTIES_SEPARATOR)
            .append(node.order())
            .append(COORDINATOR_PROPERTIES_SEPARATOR)
            .append(node.hostNames())
            .toString();
    }

    /** {@inheritDoc} */
    @Override public boolean isNodeInBaseline() {
        ctx.gateway().readLockAnyway();

        try {
            if (ctx.gateway().getState() != STARTED)
                return false;

            ClusterNode locNode = localNode();

            if (locNode.isClient() || locNode.isDaemon())
                return false;

            DiscoveryDataClusterState clusterState = ctx.state().clusterState();

            return clusterState.hasBaselineTopology() && CU.baselineNode(locNode, clusterState);
        }
        finally {
            ctx.gateway().readUnlock();
        }
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
        return igniteInstanceName;
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

    /** {@inheritDoc} */
    @Override public String clusterState() {
        return ctx.state().clusterState().state().toString();
    }

    /** {@inheritDoc} */
    @Override public long lastClusterStateChangeTime() {
        return ctx.state().lastStateChangeTime();
    }

    /**
     * @param name New attribute name.
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
     * Notifies life-cycle beans of ignite event.
     *
     * @param evt Lifecycle event to notify beans with.
     * @throws IgniteCheckedException If user threw exception during start.
     */
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
     * Notifies life-cycle beans of ignite event.
     *
     * @param evt Lifecycle event to notify beans with.
     */
    private void notifyLifecycleBeansEx(LifecycleEventType evt) {
        try {
            notifyLifecycleBeans(evt);
        }
        // Catch generic throwable to secure against user assertions.
        catch (Throwable e) {
            U.error(log, "Failed to notify lifecycle bean (safely ignored) [evt=" + evt +
                (igniteInstanceName == null ? "" : ", igniteInstanceName=" + igniteInstanceName) + ']', e);

            if (e instanceof Error)
                throw (Error)e;
        }
    }

    /**
     * @param clsPathEntry Classpath file to process.
     * @param clsPathContent StringBuilder to attach path to.
     */
    private void ackClassPathElementRecursive(File clsPathEntry, SB clsPathContent) {
        if (clsPathEntry.isDirectory()) {
            String[] list = clsPathEntry.list();

            for (String listElement : list)
                ackClassPathElementRecursive(new File(clsPathEntry, listElement), clsPathContent);
        }
        else {
            String path = clsPathEntry.getAbsolutePath();

            if (path.endsWith(".class"))
                clsPathContent.a(path).a(";");
        }
    }

    /**
     * @param clsPathEntry Classpath string to process.
     * @param clsPathContent StringBuilder to attach path to.
     */
    private void ackClassPathEntry(String clsPathEntry, SB clsPathContent) {
        File clsPathElementFile = new File(clsPathEntry);

        if (clsPathElementFile.isDirectory())
            ackClassPathElementRecursive(clsPathElementFile, clsPathContent);
        else {
            String extension = clsPathEntry.length() >= 4
                ? clsPathEntry.substring(clsPathEntry.length() - 4).toLowerCase()
                : null;

            if (".jar".equals(extension) || ".zip".equals(extension))
                clsPathContent.a(clsPathEntry).a(";");
        }
    }

    /**
     * @param clsPathEntry Classpath string to process.
     * @param clsPathContent StringBuilder to attach path to.
     */
    private void ackClassPathWildCard(String clsPathEntry, SB clsPathContent) {
        final int lastSeparatorIdx = clsPathEntry.lastIndexOf(File.separator);

        final int asteriskIdx = clsPathEntry.indexOf('*');

        //just to log possibly incorrent entries to err
        if (asteriskIdx >= 0 && asteriskIdx < lastSeparatorIdx)
            throw new RuntimeException("Could not parse classpath entry");

        final int fileMaskFirstIdx = lastSeparatorIdx + 1;

        final String fileMask =
            (fileMaskFirstIdx >= clsPathEntry.length()) ? "*.jar" : clsPathEntry.substring(fileMaskFirstIdx);

        Path path = Paths.get(lastSeparatorIdx > 0 ? clsPathEntry.substring(0, lastSeparatorIdx) : ".")
            .toAbsolutePath()
            .normalize();

        if (lastSeparatorIdx == 0)
            path = path.getRoot();

        try {
            DirectoryStream<Path> files =
                Files.newDirectoryStream(path, fileMask);

            for (Path f : files) {
                String s = f.toString();

                if (s.toLowerCase().endsWith(".jar"))
                    clsPathContent.a(f.toString()).a(";");
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Prints the list of {@code *.jar} and {@code *.class} files containing in the classpath.
     */
    private void ackClassPathContent() {
        assert log != null;

        boolean enabled = IgniteSystemProperties.getBoolean(IGNITE_LOG_CLASSPATH_CONTENT_ON_STARTUP, true);

        if (enabled) {
            String clsPath = System.getProperty("java.class.path", ".");

            String[] clsPathElements = clsPath.split(File.pathSeparator);

            U.log(log, "Classpath value: " + clsPath);

            SB clsPathContent = new SB("List of files containing in classpath: ");

            for (String clsPathEntry : clsPathElements) {
                try {
                    if (clsPathEntry.contains("*"))
                        ackClassPathWildCard(clsPathEntry, clsPathContent);
                    else
                        ackClassPathEntry(clsPathEntry, clsPathContent);
                }
                catch (Exception e) {
                    U.warn(log, String.format("Could not log class path entry '%s': %s", clsPathEntry, e.getMessage()));
                }
            }

            U.log(log, clsPathContent.toString());
        }
    }

    /**
     * @param cfg Ignite configuration to use.
     * @param utilityCachePool Utility cache pool.
     * @param execSvc Executor service.
     * @param svcExecSvc Services executor service.
     * @param sysExecSvc System executor service.
     * @param stripedExecSvc Striped executor.
     * @param p2pExecSvc P2P executor service.
     * @param mgmtExecSvc Management executor service.
     * @param igfsExecSvc IGFS executor service.
     * @param dataStreamExecSvc Data streamer executor service.
     * @param restExecSvc Reset executor service.
     * @param affExecSvc Affinity executor service.
     * @param idxExecSvc Indexing executor service.
     * @param buildIdxExecSvc Create/rebuild indexes executor service.
     * @param callbackExecSvc Callback executor service.
     * @param qryExecSvc Query executor service.
     * @param schemaExecSvc Schema executor service.
     * @param rebalanceExecSvc Rebalance excutor service.
     * @param rebalanceStripedExecSvc Striped rebalance excutor service.
     * @param customExecSvcs Custom named executors.
     * @param errHnd Error handler to use for notification about startup problems.
     * @param workerRegistry Worker registry.
     * @param hnd Default uncaught exception handler used by thread pools.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void start(
        final IgniteConfiguration cfg,
        ExecutorService utilityCachePool,
        final ExecutorService execSvc,
        final ExecutorService svcExecSvc,
        final ExecutorService sysExecSvc,
        final StripedExecutor stripedExecSvc,
        ExecutorService p2pExecSvc,
        ExecutorService mgmtExecSvc,
        ExecutorService igfsExecSvc,
        StripedExecutor dataStreamExecSvc,
        ExecutorService restExecSvc,
        ExecutorService affExecSvc,
        @Nullable ExecutorService idxExecSvc,
        @Nullable ExecutorService buildIdxExecSvc,
        IgniteStripedThreadPoolExecutor callbackExecSvc,
        ExecutorService qryExecSvc,
        ExecutorService schemaExecSvc,
        ExecutorService rebalanceExecSvc,
        IgniteStripedThreadPoolExecutor rebalanceStripedExecSvc,
        @Nullable final Map<String, ? extends ExecutorService> customExecSvcs,
        GridAbsClosure errHnd,
        WorkersRegistry workerRegistry,
        Thread.UncaughtExceptionHandler hnd,
        TimeBag startTimer
    )
        throws IgniteCheckedException {
        gw.compareAndSet(null, new GridKernalGatewayImpl(cfg.getIgniteInstanceName()));

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

        igniteInstanceName = cfg.getIgniteInstanceName();

        this.cfg = cfg;

        log = (GridLoggerProxy)cfg.getGridLogger().getLogger(
            getClass().getName() + (igniteInstanceName != null ? '%' + igniteInstanceName : ""));

        longJVMPauseDetector = new LongJVMPauseDetector(log);

        longJVMPauseDetector.start();

        RuntimeMXBean rtBean = ManagementFactory.getRuntimeMXBean();

        // Ack various information.
        ackAsciiLogo();
        ackConfigUrl();
        ackConfiguration(cfg);
        ackDaemon();
        ackOsInfo();
        ackLanguageRuntime();
        ackRemoteManagement();
        ackLogger();
        ackVmArguments(rtBean);
        ackClassPaths(rtBean);
        ackSystemProperties();
        ackEnvironmentVariables();
        ackMemoryConfiguration();
        ackCacheConfiguration();
        ackP2pConfiguration();
        ackRebalanceConfiguration();
        ackIPv4StackFlagIsSet();

        // Run background network diagnostics.
        GridDiagnostic.runBackgroundCheck(igniteInstanceName, execSvc, log);

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

        List<PluginProvider> plugins = cfg.getPluginProviders() != null && cfg.getPluginProviders().length > 0 ?
           Arrays.asList(cfg.getPluginProviders()) : U.allPluginProviders();

        // Spin out SPIs & managers.
        try {
            ctx = new GridKernalContextImpl(log,
                this,
                cfg,
                gw,
                utilityCachePool,
                execSvc,
                svcExecSvc,
                sysExecSvc,
                stripedExecSvc,
                p2pExecSvc,
                mgmtExecSvc,
                igfsExecSvc,
                dataStreamExecSvc,
                restExecSvc,
                affExecSvc,
                idxExecSvc,
                buildIdxExecSvc,
                callbackExecSvc,
                qryExecSvc,
                schemaExecSvc,
                rebalanceExecSvc,
                rebalanceStripedExecSvc,
                customExecSvcs,
                plugins,
                MarshallerUtils.classNameFilter(this.getClass().getClassLoader()),
                workerRegistry,
                hnd,
                longJVMPauseDetector
            );

            startProcessor(new DiagnosticProcessor(ctx));

            mBeansMgr = new IgniteMBeansManager(this);

            cfg.getMarshaller().setContext(ctx.marshallerContext());

            GridInternalSubscriptionProcessor subscriptionProc = new GridInternalSubscriptionProcessor(ctx);

            startProcessor(subscriptionProc);

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

            startProcessor(new FailureProcessor(ctx));

            startProcessor(new PoolProcessor(ctx));

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
            startProcessor(securityProcessor());

            // Start SPI managers.
            // NOTE: that order matters as there are dependencies between managers.
            startManager(new GridMetricManager(ctx));
            startManager(new GridSystemViewManager(ctx));
            startManager(new GridIoManager(ctx));
            startManager(new GridCheckpointManager(ctx));

            startManager(new GridEventStorageManager(ctx));
            startManager(new GridDeploymentManager(ctx));
            startManager(new GridLoadBalancerManager(ctx));
            startManager(new GridFailoverManager(ctx));
            startManager(new GridCollisionManager(ctx));
            startManager(new GridIndexingManager(ctx));

            ackSecurity();

            // Assign discovery manager to context before other processors start so they
            // are able to register custom event listener.
            final GridManager discoMgr = new GridDiscoveryManager(ctx);

            ctx.add(discoMgr, false);

            // Start the encryption manager after assigning the discovery manager to context, so it will be
            // able to register custom event listener.
            startManager(new GridEncryptionManager(ctx));

            // Start processors before discovery manager, so they will
            // be able to start receiving messages once discovery completes.
            try {
                startProcessor(COMPRESSION.createOptional(ctx));
                startProcessor(new GridMarshallerMappingProcessor(ctx));
                startProcessor(new PdsConsistentIdProcessor(ctx));
                startProcessor(new MvccProcessorImpl(ctx));
                startProcessor(createComponent(DiscoveryNodeValidationProcessor.class, ctx));
                startProcessor(new GridAffinityProcessor(ctx));
                startProcessor(createComponent(GridSegmentationProcessor.class, ctx));

                startTimer.finishGlobalStage("Start managers");

                startProcessor(createComponent(IgniteCacheObjectProcessor.class, ctx));

                startTimer.finishGlobalStage("Configure binary metadata");

                startProcessor(createComponent(IGridClusterStateProcessor.class, ctx));
                startProcessor(new IgniteAuthenticationProcessor(ctx));
                startProcessor(new GridCacheProcessor(ctx));
                startProcessor(new GridQueryProcessor(ctx));
                startProcessor(new ClientListenerProcessor(ctx));
                startProcessor(createServiceProcessor());
                startProcessor(new GridTaskSessionProcessor(ctx));
                startProcessor(new GridJobProcessor(ctx));
                startProcessor(new GridTaskProcessor(ctx));
                startProcessor((GridProcessor)SCHEDULE.createOptional(ctx));
                startProcessor(createComponent(IgniteRestProcessor.class, ctx));
                startProcessor(new DataStreamProcessor(ctx));
                startProcessor((GridProcessor)IGFS.create(ctx, F.isEmpty(cfg.getFileSystemConfiguration())));
                startProcessor(new GridContinuousProcessor(ctx));
                startProcessor(createHadoopComponent());
                startProcessor(new DataStructuresProcessor(ctx));
                startProcessor(createComponent(PlatformProcessor.class, ctx));
                startProcessor(new DistributedMetaStorageImpl(ctx));
                startProcessor(new DistributedConfigurationProcessor(ctx));
                startProcessor(new DurableBackgroundTasksProcessor(ctx));

                startTimer.finishGlobalStage("Start processors");

                // Start plugins.
                for (PluginProvider provider : ctx.plugins().allProviders()) {
                    ctx.add(new GridPluginComponent(provider));

                    provider.start(ctx.plugins().pluginContextForProvider(provider));

                    startTimer.finishGlobalStage("Start '"+ provider.name() + "' plugin");
                }

                // Start platform plugins.
                if (ctx.config().getPlatformConfiguration() != null)
                    startProcessor(new PlatformPluginProcessor(ctx));

                ctx.cluster().initDiagnosticListeners();

                fillNodeAttributes(clusterProc.updateNotifierEnabled());

                ctx.cache().context().database().notifyMetaStorageSubscribersOnReadyForRead();

                ((DistributedMetaStorageImpl)ctx.distributedMetastorage()).inMemoryReadyForRead();

                startTimer.finishGlobalStage("Init metastore");

                ctx.cache().context().database().startMemoryRestore(ctx, startTimer);

                ctx.recoveryMode(false);

                startTimer.finishGlobalStage("Finish recovery");
            }
            catch (Throwable e) {
                U.error(
                    log, "Exception during start processors, node will be stopped and close connections", e);

                // Stop discovery spi to close tcp socket.
                ctx.discovery().stop(true);

                throw e;
            }

            gw.writeLock();

            try {
                gw.setState(STARTED);

                // Start discovery manager last to make sure that grid is fully initialized.
                startManager(discoMgr);
            }
            finally {
                gw.writeUnlock();
            }

            startTimer.finishGlobalStage("Join topology");

            // Check whether UTF-8 is the default character encoding.
            checkFileEncoding();

            // Check whether physical RAM is not exceeded.
            checkPhysicalRam();

            // Suggest configuration optimizations.
            suggestOptimizations(cfg);

            // Suggest JVM optimizations.
            ctx.performance().addAll(JvmConfigurationSuggestions.getSuggestions());

            // Suggest Operation System optimizations.
            ctx.performance().addAll(OsConfigurationSuggestions.getSuggestions());

            DiscoveryLocalJoinData joinData = ctx.discovery().localJoin();

            IgniteInternalFuture<Boolean> transitionWaitFut = joinData.transitionWaitFuture();

            // Notify discovery manager the first to make sure that topology is discovered.
            // Active flag is not used in managers, so it is safe to pass true.
            ctx.discovery().onKernalStart(true);

            // Notify IO manager the second so further components can send and receive messages.
            // Must notify the IO manager before transition state await to make sure IO connection can be established.
            ctx.io().onKernalStart(true);

            boolean active;

            if (transitionWaitFut != null) {
                if (log.isInfoEnabled()) {
                    log.info("Join cluster while cluster state transition is in progress, " +
                        "waiting when transition finish.");
                }

                active = transitionWaitFut.get();
            }
            else
                active = joinData.active();

            startTimer.finishGlobalStage("Await transition");

            boolean recon = false;

            // Callbacks.
            for (GridComponent comp : ctx) {
                // Skip discovery manager.
                if (comp instanceof GridDiscoveryManager)
                    continue;

                // Skip IO manager.
                if (comp instanceof GridIoManager)
                    continue;

                if (comp instanceof GridPluginComponent)
                    continue;

                if (!skipDaemon(comp)) {
                    try {
                        comp.onKernalStart(active);
                    }
                    catch (IgniteNeedReconnectException e) {
                        ClusterNode locNode = ctx.discovery().localNode();

                        assert locNode.isClient();

                        if (!ctx.discovery().reconnectSupported())
                            throw new IgniteCheckedException("Client node in forceServerMode " +
                                "is not allowed to reconnect to the cluster and will be stopped.");

                        if (log.isDebugEnabled())
                            log.debug("Failed to start node components on node start, will wait for reconnect: " + e);

                        recon = true;
                    }
                }
            }

            // Start plugins.
            for (PluginProvider provider : ctx.plugins().allProviders())
                provider.onIgniteStart();

            if (recon)
                reconnectState.waitFirstReconnect();

            ctx.metric().registerThreadPools(utilityCachePool, execSvc, svcExecSvc, sysExecSvc, stripedExecSvc,
                p2pExecSvc, mgmtExecSvc, igfsExecSvc, dataStreamExecSvc, restExecSvc, affExecSvc, idxExecSvc,
                callbackExecSvc, qryExecSvc, schemaExecSvc, rebalanceExecSvc, rebalanceStripedExecSvc, customExecSvcs);

            registerMetrics();

            // Register MBeans.
            mBeansMgr.registerAllMBeans(utilityCachePool, execSvc, svcExecSvc, sysExecSvc, stripedExecSvc, p2pExecSvc,
                mgmtExecSvc, igfsExecSvc, dataStreamExecSvc, restExecSvc, affExecSvc, idxExecSvc, callbackExecSvc,
                qryExecSvc, schemaExecSvc, rebalanceExecSvc, rebalanceStripedExecSvc, customExecSvcs, ctx.workersRegistry());

            ctx.systemView().registerThreadPools(stripedExecSvc, dataStreamExecSvc);

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

                /** Last completed task count. */
                private long lastCompletedCntQry;

                @Override public void run() {
                    if (execSvc instanceof ThreadPoolExecutor) {
                        ThreadPoolExecutor exec = (ThreadPoolExecutor)execSvc;

                        lastCompletedCntPub = checkPoolStarvation(exec, lastCompletedCntPub, "public");
                    }

                    if (sysExecSvc instanceof ThreadPoolExecutor) {
                        ThreadPoolExecutor exec = (ThreadPoolExecutor)sysExecSvc;

                        lastCompletedCntSys = checkPoolStarvation(exec, lastCompletedCntSys, "system");
                    }

                    if (qryExecSvc instanceof ThreadPoolExecutor) {
                        ThreadPoolExecutor exec = (ThreadPoolExecutor)qryExecSvc;

                        lastCompletedCntQry = checkPoolStarvation(exec, lastCompletedCntQry, "query");
                    }

                    if (stripedExecSvc != null)
                        stripedExecSvc.detectStarvation();
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
                private final DecimalFormat dblFmt = doubleFormat();

                @Override public void run() {
                    ackNodeMetrics(dblFmt, execSvc, sysExecSvc, customExecSvcs);
                }
            }, metricsLogFreq, metricsLogFreq);
        }

        scheduleLongOperationsDumpTask(ctx.cache().context().tm().longOperationsDumpTimeout());

        ctx.performance().add("Disable assertions (remove '-ea' from JVM options)", !U.assertionsEnabled());

        ctx.performance().logSuggestions(log, igniteInstanceName);

        U.quietAndInfo(log, "To start Console Management & Monitoring run ignitevisorcmd.{sh|bat}");

        if (!IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_QUIET, true))
            ackClassPathContent();

        ackStart(rtBean);

        if (!isDaemon())
            ctx.discovery().ackTopology(ctx.discovery().localJoin().joinTopologyVersion().topologyVersion(),
                EventType.EVT_NODE_JOINED, localNode());

        startTimer.finishGlobalStage("Await exchange");
    }

    /** */
    private static DecimalFormat doubleFormat() {
        return new DecimalFormat("#.##", DecimalFormatSymbols.getInstance(Locale.US));
    }

    /**
     * Scheduling tasks for dumping long operations. Closes current task
     * (if any) and if the {@code longOpDumpTimeout > 0} schedules a new task
     * with a new timeout, delay and start period equal to
     * {@code longOpDumpTimeout}, otherwise task is deleted.
     *
     * @param longOpDumpTimeout Long operations dump timeout.
     */
    public void scheduleLongOperationsDumpTask(long longOpDumpTimeout) {
        if (isStopping())
            return;

        synchronized (this) {
            GridTimeoutProcessor.CancelableTask task = longOpDumpTask;

            if (nonNull(task))
                task.close();

            if (longOpDumpTimeout > 0) {
                longOpDumpTask = ctx.timeout().schedule(
                    () -> ctx.cache().context().exchange().dumpLongRunningOperations(longOpDumpTimeout),
                    longOpDumpTimeout,
                    longOpDumpTimeout
                );
            }
            else
                longOpDumpTask = null;
        }
    }

    /**
     * @return Ignite security processor. See {@link IgniteSecurity} for details.
     */
    private GridProcessor securityProcessor() throws IgniteCheckedException {
        GridSecurityProcessor prc = createComponent(GridSecurityProcessor.class, ctx);

        return prc != null && prc.enabled()
            ? new IgniteSecurityProcessor(ctx, prc)
            : new NoOpIgniteSecurityProcessor(ctx);
    }

    /**
     * Create description of an executor service for logging.
     *
     * @param execSvcName Name of the service.
     * @param execSvc Service to create a description for.
     */
    private String createExecutorDescription(String execSvcName, ExecutorService execSvc) {
        int poolActiveThreads = 0;
        int poolIdleThreads = 0;
        int poolQSize = 0;

        if (execSvc instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor exec = (ThreadPoolExecutor)execSvc;

            int poolSize = exec.getPoolSize();

            poolActiveThreads = Math.min(poolSize, exec.getActiveCount());
            poolIdleThreads = poolSize - poolActiveThreads;
            poolQSize = exec.getQueue().size();
        }

        return execSvcName + " [active=" + poolActiveThreads + ", idle=" + poolIdleThreads + ", qSize=" + poolQSize + "]";
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
     * Creates service processor depend on {@link IgniteSystemProperties#IGNITE_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED}.
     *
     * @return The service processor. See {@link IgniteServiceProcessor} for details.
     */
    private GridProcessorAdapter createServiceProcessor() {
        final boolean srvcProcMode = getBoolean(IGNITE_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED, true);

        if (srvcProcMode)
            return new IgniteServiceProcessor(ctx);

        return new GridServiceProcessor(ctx);
    }

    /**
     * Validates common configuration parameters.
     *
     * @param cfg Ignite configuration to validate.
     */
    private void validateCommon(IgniteConfiguration cfg) {
        A.notNull(cfg.getNodeId(), "cfg.getNodeId()");

        if (!U.IGNITE_MBEANS_DISABLED)
            A.notNull(cfg.getMBeanServer(), "cfg.getMBeanServer()");

        A.notNull(cfg.getGridLogger(), "cfg.getGridLogger()");
        A.notNull(cfg.getMarshaller(), "cfg.getMarshaller()");
        A.notNull(cfg.getUserAttributes(), "cfg.getUserAttributes()");

        // All SPIs should be non-null.
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
     * Check whether UTF-8 is the default character encoding.
     * Differing character encodings across cluster may lead to erratic behavior.
     */
    private void checkFileEncoding() {
        String encodingDisplayName = Charset.defaultCharset().displayName(Locale.ENGLISH);

        if (!"UTF-8".equals(encodingDisplayName)) {
            U.quietAndWarn(log, "Default character encoding is " + encodingDisplayName +
                ". Specify UTF-8 character encoding by setting -Dfile.encoding=UTF-8 JVM parameter. " +
                "Differing character encodings across cluster may lead to erratic behavior.");
        }
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
            long totalOffheap = 0;

            for (ClusterNode node : ctx.discovery().allNodes()) {
                if (macs.equals(node.attribute(ATTR_MACS))) {
                    long heap = node.metrics().getHeapMemoryMaximum();
                    Long offheap = node.<Long>attribute(ATTR_OFFHEAP_SIZE);

                    if (heap != -1)
                        totalHeap += heap;

                    if (offheap != null)
                        totalOffheap += offheap;
                }
            }

            long total = totalHeap + totalOffheap;

            if (total < 0)
                total = Long.MAX_VALUE;

            // 4GB or 20% of available memory is expected to be used by OS and user applications
            long safeToUse = ram - Math.max(4L << 30, (long)(ram * 0.2));

            if (total > safeToUse) {
                U.quietAndWarn(log, "Nodes started on local machine require more than 80% of physical RAM what can " +
                    "lead to significant slowdown due to swapping (please decrease JVM heap size, data region " +
                    "size or checkpoint buffer size) [required=" + (total >> 20) + "MB, available=" +
                    (ram >> 20) + "MB]");
            }
        }
    }

    /**
     * @param cfg Ignite configuration to check for possible performance issues.
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
    private void fillNodeAttributes(boolean notifyEnabled) throws IgniteCheckedException {
        ctx.addNodeAttribute(ATTR_REBALANCE_POOL_SIZE, configuration().getRebalanceThreadPoolSize());
        ctx.addNodeAttribute(ATTR_DATA_STREAMER_POOL_SIZE, configuration().getDataStreamerThreadPoolSize());

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
                "computer can participate in topology.");

        // Stick in network context into attributes.
        add(ATTR_IPS, (ips.isEmpty() ? "" : ips));

        Map<String, ?> userAttrs = configuration().getUserAttributes();

        if (userAttrs != null && userAttrs.get(IgniteNodeAttributes.ATTR_MACS_OVERRIDE) != null)
            add(ATTR_MACS, (Serializable)userAttrs.get(IgniteNodeAttributes.ATTR_MACS_OVERRIDE));
        else
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
        add(ATTR_IGNITE_INSTANCE_NAME, igniteInstanceName);

        add(ATTR_PEER_CLASSLOADING, cfg.isPeerClassLoadingEnabled());
        add(ATTR_DEPLOYMENT_MODE, cfg.getDeploymentMode());
        add(ATTR_LANG_RUNTIME, getLanguage());

        add(ATTR_JVM_PID, U.jvmPid());

        add(ATTR_CLIENT_MODE, cfg.isClientMode());

        add(ATTR_CONSISTENCY_CHECK_SKIPPED, getBoolean(IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK));

        add(ATTR_VALIDATE_CACHE_REQUESTS, Boolean.TRUE);

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

        // Whether rollback of dynamic cache start is supported or not.
        // This property is added because of backward compatibility.
        add(ATTR_DYNAMIC_CACHE_START_ROLLBACK_SUPPORTED, Boolean.TRUE);

        // Save data storage configuration.
        addDataStorageConfigurationAttributes();

        // Save transactions configuration.
        add(ATTR_TX_CONFIG, cfg.getTransactionConfiguration());

        // Supported features.
        add(ATTR_IGNITE_FEATURES, IgniteFeatures.allFeatures());

        // Stick in SPI versions and classes attributes.
        addSpiAttributes(cfg.getCollisionSpi());
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

        ctx.addNodeAttribute(ATTR_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED,
            ctx.service() instanceof IgniteServiceProcessor);
    }

    /**
     * @throws IgniteCheckedException If duplicated SPI name found.
     */
    private void addDataStorageConfigurationAttributes() throws IgniteCheckedException {
        MemoryConfiguration memCfg = cfg.getMemoryConfiguration();

        // Save legacy memory configuration if it's present.
        if (memCfg != null) {
            // Page size initialization is suspended, see IgniteCacheDatabaseSharedManager#checkPageSize.
            // We should copy initialized value from new configuration.
            memCfg.setPageSize(cfg.getDataStorageConfiguration().getPageSize());

            add(ATTR_MEMORY_CONFIG, memCfg);
        }

        // Save data storage configuration.
        add(ATTR_DATA_STORAGE_CONFIG, new JdkMarshaller().marshal(cfg.getDataStorageConfiguration()));
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
            U.error(log, "Failed to start manager: " + mgr, e);

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
     * @param helper Helper to attach to kernal context.
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
     * @return {@code true} if the REST processor is enabled, {@code false} the otherwise.
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
     * @param cfg Ignite configuration to ack.
     */
    private void ackConfiguration(IgniteConfiguration cfg) {
        assert log != null;

        if (log.isInfoEnabled())
            log.info(cfg.toString());
    }

    /**
     * Acks Logger configuration.
     */
    private void ackLogger() {
        assert log != null;

        if (log.isInfoEnabled())
            log.info("Logger: " + log.getLoggerInfo());
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

                U.quiet(false, "  ^-- Logging by '" + log.getLoggerInfo() + '\'');

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
                (F.isEmpty(igniteInstanceName) ? "" : ", instance name=" + igniteInstanceName) + ')');
        }

        if (log.isInfoEnabled()) {
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
                    (igniteInstanceName == null ? "" : ">>> Ignite instance name: " + igniteInstanceName + NL) +
                    ">>> Local node [" +
                    "ID=" + locNode.id().toString().toUpperCase() +
                    ", order=" + locNode.order() + ", clientMode=" + ctx.clientNode() +
                    "]" + NL +
                    ">>> Local node addresses: " + U.addressesAsString(locNode) + NL +
                    ">>> Local ports: " + sb + NL;

            log.info(str);
        }

        if (!ctx.state().clusterState().active()) {
            U.quietAndInfo(log, ">>> Ignite cluster is not active (limited functionality available). " +
                "Use control.(sh|bat) script or IgniteCluster interface to activate.");
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
     * Logs out node metrics.
     *
     * @param dblFmt Decimal format.
     * @param execSvc Executor service.
     * @param sysExecSvc System executor service.
     * @param customExecSvcs Custom named executors.
     */
    private void ackNodeMetrics(DecimalFormat dblFmt,
        ExecutorService execSvc,
        ExecutorService sysExecSvc,
        Map<String, ? extends ExecutorService> customExecSvcs
    ) {
        if (!log.isInfoEnabled())
            return;

        try {
            ClusterMetrics m = cluster().localNode().metrics();

            int localCpus = m.getTotalCpus();
            double cpuLoadPct = m.getCurrentCpuLoad() * 100;
            double avgCpuLoadPct = m.getAverageCpuLoad() * 100;
            double gcPct = m.getCurrentGcCpuLoad() * 100;

            // Heap params.
            long heapUsed = m.getHeapMemoryUsed();
            long heapMax = m.getHeapMemoryMaximum();

            long heapUsedInMBytes = heapUsed / MEGABYTE;
            long heapCommInMBytes = m.getHeapMemoryCommitted() / MEGABYTE;

            double freeHeapPct = heapMax > 0 ? ((double)((heapMax - heapUsed) * 100)) / heapMax : -1;

            int hosts = 0;
            int servers = 0;
            int clients = 0;
            int cpus = 0;

            try {
                ClusterMetrics metrics = cluster().metrics();

                Collection<ClusterNode> nodes0 = cluster().nodes();

                hosts = U.neighborhood(nodes0).size();
                servers = cluster().forServers().nodes().size();
                clients = cluster().forClients().nodes().size();
                cpus = metrics.getTotalCpus();
            }
            catch (IgniteException ignore) {
                // No-op.
            }

            String dataStorageInfo = dataStorageReport(ctx.cache().context().database(), dblFmt, true);

            String id = U.id8(localNode().id());

            AffinityTopologyVersion topVer = ctx.discovery().topologyVersionEx();

            ClusterNode locNode = ctx.discovery().localNode();

            String networkDetails = "";

            if (!F.isEmpty(cfg.getLocalHost()))
                networkDetails += ", localHost=" + cfg.getLocalHost();

            if (locNode instanceof TcpDiscoveryNode)
                networkDetails += ", discoPort=" + ((TcpDiscoveryNode)locNode).discoveryPort();

            if (cfg.getCommunicationSpi() instanceof TcpCommunicationSpi)
                networkDetails += ", commPort=" + ((TcpCommunicationSpi)cfg.getCommunicationSpi()).boundPort();

            SB msg = new SB();

            msg.nl()
                .a("Metrics for local node (to disable set 'metricsLogFrequency' to 0)").nl()
                .a("    ^-- Node [id=").a(id).a(name() != null ? ", name=" + name() : "").a(", uptime=")
                .a(getUpTimeFormatted()).a("]").nl()
                .a("    ^-- Cluster [hosts=").a(hosts).a(", CPUs=").a(cpus).a(", servers=").a(servers)
                .a(", clients=").a(clients).a(", topVer=").a(topVer.topologyVersion())
                .a(", minorTopVer=").a(topVer.minorTopologyVersion()).a("]").nl()
                .a("    ^-- Network [addrs=").a(locNode.addresses()).a(networkDetails).a("]").nl()
                .a("    ^-- CPU [CPUs=").a(localCpus).a(", curLoad=").a(dblFmt.format(cpuLoadPct))
                .a("%, avgLoad=").a(dblFmt.format(avgCpuLoadPct)).a("%, GC=").a(dblFmt.format(gcPct)).a("%]").nl()
                .a("    ^-- Heap [used=").a(dblFmt.format(heapUsedInMBytes))
                .a("MB, free=").a(dblFmt.format(freeHeapPct))
                .a("%, comm=").a(dblFmt.format(heapCommInMBytes)).a("MB]").nl()
                .a(dataStorageInfo)
                .a("    ^-- Outbound messages queue [size=").a(m.getOutboundMessagesQueueSize()).a("]").nl()
                .a("    ^-- ").a(createExecutorDescription("Public thread pool", execSvc)).nl()
                .a("    ^-- ").a(createExecutorDescription("System thread pool", sysExecSvc));

            if (customExecSvcs != null) {
                for (Map.Entry<String, ? extends ExecutorService> entry : customExecSvcs.entrySet())
                    msg.nl().a("    ^-- ").a(createExecutorDescription(entry.getKey(), entry.getValue()));
            }

            log.info(msg.toString());

            ctx.cache().context().database().dumpStatistics(log);
        }
        catch (IgniteClientDisconnectedException ignore) {
            // No-op.
        }
    }

    /** */
    public static String dataStorageReport(IgniteCacheDatabaseSharedManager db, boolean includeMemoryStatistics) {
        return dataStorageReport(db, doubleFormat(), includeMemoryStatistics);
    }

    /** */
    private static String dataStorageReport(IgniteCacheDatabaseSharedManager db, DecimalFormat dblFmt,
        boolean includeMemoryStatistics) {
        // Off-heap params.
        Collection<DataRegion> regions = db.dataRegions();

        SB dataRegionsInfo = new SB();

        long loadedPages = 0;
        long offHeapUsedSummary = 0;
        long offHeapMaxSummary = 0;
        long offHeapCommSummary = 0;
        long pdsUsedSummary = 0;

        boolean persistenceEnabled = false;

        if (!F.isEmpty(regions)) {
            for (DataRegion region : regions) {
                DataRegionConfiguration regCfg = region.config();

                long pagesCnt = region.pageMemory().loadedPages();

                long offHeapUsed = region.pageMemory().systemPageSize() * pagesCnt;
                long offHeapInit = regCfg.getInitialSize();
                long offHeapMax = regCfg.getMaxSize();
                long offHeapComm = region.memoryMetrics().getOffHeapSize();

                long offHeapUsedInMBytes = offHeapUsed / MEGABYTE;
                long offHeapMaxInMBytes = offHeapMax / MEGABYTE;
                long offHeapCommInMBytes = offHeapComm / MEGABYTE;
                long offHeapInitInMBytes = offHeapInit / MEGABYTE;

                double freeOffHeapPct = offHeapMax > 0 ?
                    ((double)((offHeapMax - offHeapUsed) * 100)) / offHeapMax : -1;

                offHeapUsedSummary += offHeapUsed;
                offHeapMaxSummary += offHeapMax;
                offHeapCommSummary += offHeapComm;
                loadedPages += pagesCnt;

                String type = "user";

                try {
                    if (region == db.dataRegion(null))
                        type = "default";
                    else if (INTERNAL_DATA_REGION_NAMES.contains(regCfg.getName()))
                        type = "internal";
                }
                catch (IgniteCheckedException ice) {
                    // Should never happen
                    ice.printStackTrace();
                }

                dataRegionsInfo.a("    ^--   ")
                    .a(regCfg.getName()).a(" region [type=").a(type)
                    .a(", persistence=").a(regCfg.isPersistenceEnabled())
                    .a(", lazyAlloc=").a(regCfg.isLazyMemoryAllocation()).a(',').nl()
                    .a("      ...  ")
                    .a("initCfg=").a(dblFmt.format(offHeapInitInMBytes))
                    .a("MB, maxCfg=").a(dblFmt.format(offHeapMaxInMBytes))
                    .a("MB, usedRam=").a(dblFmt.format(offHeapUsedInMBytes))
                    .a("MB, freeRam=").a(dblFmt.format(freeOffHeapPct))
                    .a("%, allocRam=").a(dblFmt.format(offHeapCommInMBytes)).a("MB");

                if (regCfg.isPersistenceEnabled()) {
                    long pdsUsed = region.memoryMetrics().getTotalAllocatedSize();
                    long pdsUsedInMBytes = pdsUsed / MEGABYTE;

                    pdsUsedSummary += pdsUsed;

                    dataRegionsInfo.a(", allocTotal=").a(dblFmt.format(pdsUsedInMBytes)).a("MB");

                    persistenceEnabled = true;
                }

                dataRegionsInfo.a(']').nl();
            }
        }

        SB info = new SB();

        if (includeMemoryStatistics) {
            long offHeapUsedInMBytes = offHeapUsedSummary / MEGABYTE;
            long offHeapCommInMBytes = offHeapCommSummary / MEGABYTE;

            double freeOffHeapPct = offHeapMaxSummary > 0 ?
                ((double)((offHeapMaxSummary - offHeapUsedSummary) * 100)) / offHeapMaxSummary : -1;

            info.a("    ^-- Off-heap memory [used=").a(dblFmt.format(offHeapUsedInMBytes))
                .a("MB, free=").a(dblFmt.format(freeOffHeapPct))
                .a("%, allocated=").a(dblFmt.format(offHeapCommInMBytes)).a("MB]").nl()
                .a("    ^-- Page memory [pages=").a(loadedPages).a("]").nl();
        }

        info.a(dataRegionsInfo);

        if (persistenceEnabled) {
            long pdsUsedMBytes = pdsUsedSummary / MEGABYTE;

            info.a("    ^-- Ignite persistence [used=").a(dblFmt.format(pdsUsedMBytes)).a("MB]").nl();
        }

        return info.toString();
    }

    /**
     * @return Language runtime.
     */
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
     * Stops Ignite instance.
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
        gw.compareAndSet(null, new GridKernalGatewayImpl(igniteInstanceName));

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

            // Callback component in reverse order while kernal is still functional
            // if called in the same thread, at least.
            for (ListIterator<GridComponent> it = comps.listIterator(comps.size()); it.hasPrevious(); ) {
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

            if (ctx.hadoopHelper() != null)
                ctx.hadoopHelper().close();

            if (starveTask != null)
                starveTask.close();

            if (metricsLogTask != null)
                metricsLogTask.close();

            if (longOpDumpTask != null)
                longOpDumpTask.close();

            if (longJVMPauseDetector != null)
                longJVMPauseDetector.stop();

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
                    log.debug("Grid " + (igniteInstanceName == null ? "" : '\'' + igniteInstanceName + "' ") +
                        "is stopping.");
            }
            finally {
                gw.writeUnlock();
            }

            // Stopping cache operations.
            GridCacheProcessor cache = ctx.cache();

            if (cache != null)
                cache.blockGateways();

            // Unregister MBeans.
            if (!mBeansMgr.unregisterAllMBeans())
                errOnStop = true;

            // Stop components in reverse order.
            for (ListIterator<GridComponent> it = comps.listIterator(comps.size()); it.hasPrevious(); ) {
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
                String nodeName = igniteInstanceName == null ? "" : "name=" + igniteInstanceName + ", ";

                if (!errOnStop)
                    U.quiet(false, "Ignite node stopped OK [" + nodeName + "uptime=" +
                        X.timeSpan2DHMSM(U.currentTimeMillis() - startTime) + ']');
                else
                    U.quiet(true, "Ignite node stopped wih ERRORS [" + nodeName + "uptime=" +
                        X.timeSpan2DHMSM(U.currentTimeMillis() - startTime) + ']');
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
                        (igniteInstanceName == null ? "" : ">>> Ignite instance name: " + igniteInstanceName + NL) +
                        ">>> Grid uptime: " + X.timeSpan2DHMSM(U.currentTimeMillis() - startTime) +
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
                        (igniteInstanceName == null ? "" : ">>> Ignite instance name: " + igniteInstanceName + NL) +
                        ">>> Grid uptime: " + X.timeSpan2DHMSM(U.currentTimeMillis() - startTime) +
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
     * @param name Cache name.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Internal cache instance.
     */
    /*@java.test.only*/
    public <K, V> GridCacheAdapter<K, V> internalCache(String name) {
        CU.validateCacheName(name);
        checkClusterState();

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

        if (log.isDebugEnabled() && S.includeSensitive())
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
     * @return {@code True} is this node is daemon.
     */
    private boolean isDaemon() {
        assert cfg != null;

        return cfg.isDaemon() || IgniteSystemProperties.getBoolean(IGNITE_DAEMON);
    }

    /**
     * Whether or not remote JMX management is enabled for this node. Remote JMX management is enabled when the
     * following system property is set: <ul> <li>{@code com.sun.management.jmxremote}</li> </ul>
     *
     * @return {@code True} if remote JMX management is enabled - {@code false} otherwise.
     */
    @Override public boolean isJmxRemoteEnabled() {
        return System.getProperty("com.sun.management.jmxremote") != null;
    }

    /**
     * Whether or not node restart is enabled. Node restart us supported when this node was started with {@code
     * bin/ignite.{sh|bat}} script using {@code -r} argument. Node can be programmatically restarted using {@link
     * Ignition#restart(boolean)}} method.
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
        }
    }

    /**
     * Acknowledge that the rebalance configuration properties are setted correctly.
     *
     * @throws IgniteCheckedException If rebalance configuration validation fail.
     */
    private void ackRebalanceConfiguration() throws IgniteCheckedException {
        if (cfg.isClientMode()) {
            if (cfg.getRebalanceThreadPoolSize() != IgniteConfiguration.DFLT_REBALANCE_THREAD_POOL_SIZE)
                U.warn(log, "Setting the rebalance pool size has no effect on the client mode");
        }
        else {
            if (cfg.getSystemThreadPoolSize() <= cfg.getRebalanceThreadPoolSize())
                throw new IgniteCheckedException("Rebalance thread pool size exceed or equals System thread pool size. " +
                    "Change IgniteConfiguration.rebalanceThreadPoolSize property before next start.");

            if (cfg.getRebalanceThreadPoolSize() < 1)
                throw new IgniteCheckedException("Rebalance thread pool size minimal allowed value is 1. " +
                    "Change IgniteConfiguration.rebalanceThreadPoolSize property before next start.");

            if (cfg.getRebalanceBatchesPrefetchCount() < 1)
                throw new IgniteCheckedException("Rebalance batches prefetch count minimal allowed value is 1. " +
                    "Change IgniteConfiguration.rebalanceBatchesPrefetchCount property before next start.");

            if (cfg.getRebalanceBatchSize() <= 0)
                throw new IgniteCheckedException("Rebalance batch size must be greater than zero. " +
                    "Change IgniteConfiguration.rebalanceBatchSize property before next start.");

            if (cfg.getRebalanceThrottle() < 0)
                throw new IgniteCheckedException("Rebalance throttle can't have negative value. " +
                    "Change IgniteConfiguration.rebalanceThrottle property before next start.");

            if (cfg.getRebalanceTimeout() < 0)
                throw new IgniteCheckedException("Rebalance message timeout can't have negative value. " +
                    "Change IgniteConfiguration.rebalanceTimeout property before next start.");

            for (CacheConfiguration ccfg : cfg.getCacheConfiguration()) {
                if (ccfg.getRebalanceBatchesPrefetchCount() < 1)
                    throw new IgniteCheckedException("Rebalance batches prefetch count minimal allowed value is 1. " +
                        "Change CacheConfiguration.rebalanceBatchesPrefetchCount property before next start. " +
                        "[cache=" + ccfg.getName() + "]");
            }
        }
    }

    /**
     * Acknowledge the Ignite configuration related to the data storage.
     */
    private void ackMemoryConfiguration() {
        DataStorageConfiguration memCfg = cfg.getDataStorageConfiguration();

        if (memCfg == null)
            return;

        U.log(log, "System cache's DataRegion size is configured to " +
            (memCfg.getSystemRegionInitialSize() / (1024 * 1024)) + " MB. " +
            "Use DataStorageConfiguration.systemRegionInitialSize property to change the setting.");
    }

    /**
     * Acknowledge all caches configurations presented in the IgniteConfiguration.
     */
    private void ackCacheConfiguration() {
        CacheConfiguration[] cacheCfgs = cfg.getCacheConfiguration();

        if (cacheCfgs == null || cacheCfgs.length == 0)
            U.warn(log, "Cache is not configured - in-memory data grid is off.");
        else {
            SB sb = new SB();

            HashMap<String, ArrayList<String>> memPlcNamesMapping = new HashMap<>();

            for (CacheConfiguration c : cacheCfgs) {
                String cacheName = U.maskName(c.getName());

                String memPlcName = c.getDataRegionName();

                if (CU.isSystemCache(cacheName))
                    memPlcName = "sysMemPlc";
                else if (memPlcName == null && cfg.getDataStorageConfiguration() != null)
                    memPlcName = cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().getName();

                if (!memPlcNamesMapping.containsKey(memPlcName))
                    memPlcNamesMapping.put(memPlcName, new ArrayList<String>());

                ArrayList<String> cacheNames = memPlcNamesMapping.get(memPlcName);

                cacheNames.add(cacheName);
            }

            for (Map.Entry<String, ArrayList<String>> e : memPlcNamesMapping.entrySet()) {
                sb.a("in '").a(e.getKey()).a("' dataRegion: [");

                for (String s : e.getValue())
                    sb.a("'").a(s).a("', ");

                sb.d(sb.length() - 2, sb.length()).a("], ");
            }

            U.log(log, "Configured caches [" + sb.d(sb.length() - 2, sb.length()).toString() + ']');
        }
    }

    /**
     * Acknowledge configuration related to the peer class loading.
     */
    private void ackP2pConfiguration() {
        assert cfg != null;

        if (cfg.isPeerClassLoadingEnabled())
            U.warn(
                log,
                "Peer class loading is enabled (disable it in production for performance and " +
                    "deployment consistency reasons)");
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
        if (log.isInfoEnabled() && S.includeSensitive()) {
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
            try {
                log.debug("Boot class path: " + rtBean.getBootClassPath());
                log.debug("Class path: " + rtBean.getClassPath());
                log.debug("Library path: " + rtBean.getLibraryPath());
            }
            catch (Exception ignore) {
                // No-op: ignore for Java 9+ and non-standard JVMs.
            }
        }
    }

    /**
     * Prints warning if 'java.net.preferIPv4Stack=true' is not set.
     */
    private void ackIPv4StackFlagIsSet() {
        boolean preferIPv4 = Boolean.valueOf(System.getProperty("java.net.preferIPv4Stack"));

        if (!preferIPv4) {
            assert log != null;

            U.quietAndWarn(log, "Please set system property '-Djava.net.preferIPv4Stack=true' " +
                "to avoid possible problems in mixed environments.");
        }
    }

    /**
     * @param cfg Ignite configuration to use.
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

        if (cfg.getCommunicationFailureResolver() != null)
            objs.add(cfg.getCommunicationFailureResolver());

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
            checkClusterState();

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
            checkClusterState();

            return ctx.cache().transactions();
        }
        finally {
            unguard();
        }
    }

    /**
     * @param name Cache name.
     * @return Ignite internal cache instance related to the given name.
     */
    public <K, V> IgniteInternalCache<K, V> getCache(String name) {
        CU.validateCacheName(name);

        guard();

        try {
            checkClusterState();

            return ctx.cache().publicCache(name);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> cache(String name) {
        CU.validateCacheName(name);

        guard();

        try {
            checkClusterState();

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
        CU.validateNewCacheName(cacheCfg.getName());

        guard();

        try {
            checkClusterState();

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
        CU.validateConfigurationCacheNames(cacheCfgs);

        guard();

        try {
            checkClusterState();

            ctx.cache().dynamicStartCaches(cacheCfgs,
                true,
                true,
                false).get();

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
        CU.validateNewCacheName(cacheName);

        guard();

        try {
            checkClusterState();

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
        return getOrCreateCache0(cacheCfg, false).get1();
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteBiTuple<IgniteCache<K, V>, Boolean> getOrCreateCache0(
        CacheConfiguration<K, V> cacheCfg, boolean sql) {
        A.notNull(cacheCfg, "cacheCfg");
        String cacheName = cacheCfg.getName();

        CU.validateNewCacheName(cacheName);

        guard();

        try {
            checkClusterState();

            Boolean res = false;

            IgniteCacheProxy<K, V> cache = ctx.cache().publicJCache(cacheName, false, true);

            if (cache == null) {
                res =
                    sql ? ctx.cache().dynamicStartSqlCache(cacheCfg).get() :
                        ctx.cache().dynamicStartCache(cacheCfg,
                            cacheName,
                            null,
                            false,
                            true,
                            true).get();

                return new IgniteBiTuple<>(ctx.cache().publicJCache(cacheName), res);
            }
            else
                return new IgniteBiTuple<>(cache, res);
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
        CU.validateConfigurationCacheNames(cacheCfgs);

        guard();

        try {
            checkClusterState();

            ctx.cache().dynamicStartCaches(cacheCfgs,
                false,
                true,
                false).get();

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
        CU.validateNewCacheName(cacheCfg.getName());
        A.notNull(nearCfg, "nearCfg");

        guard();

        try {
            checkClusterState();

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
        CU.validateNewCacheName(cacheCfg.getName());
        A.notNull(nearCfg, "nearCfg");

        guard();

        try {
            checkClusterState();

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
        CU.validateNewCacheName(cacheName);
        A.notNull(nearCfg, "nearCfg");

        guard();

        try {
            checkClusterState();

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
    @Override public <K, V> IgniteCache<K, V> getOrCreateNearCache(String cacheName,
        NearCacheConfiguration<K, V> nearCfg) {
        CU.validateNewCacheName(cacheName);
        A.notNull(nearCfg, "nearCfg");

        guard();

        try {
            checkClusterState();

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
     * @param cache Ignite cache instance to check.
     * @throws IgniteCheckedException If cache without near cache was already started.
     */
    private void checkNearCacheStarted(IgniteCacheProxy<?, ?> cache) throws IgniteCheckedException {
        if (!cache.context().isNear())
            throw new IgniteCheckedException("Failed to start near cache " +
                "(a cache with the same name without near cache is already started)");
    }

    /** {@inheritDoc} */
    @Override public void destroyCache(String cacheName) {
        destroyCache0(cacheName, false);
    }

    /** {@inheritDoc} */
    @Override public boolean destroyCache0(String cacheName, boolean sql) throws CacheException {
        CU.validateCacheName(cacheName);

        IgniteInternalFuture<Boolean> stopFut = destroyCacheAsync(cacheName, sql, true);

        try {
            return stopFut.get();
        }
        catch (IgniteCheckedException e) {
            throw CU.convertToCacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void destroyCaches(Collection<String> cacheNames) {
        CU.validateCacheNames(cacheNames);

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
     * @param sql If the cache needs to be destroyed only if it was created by SQL {@code CREATE TABLE} command.
     * @param checkThreadTx If {@code true} checks that current thread does not have active transactions.
     * @return Ignite future.
     */
    public IgniteInternalFuture<Boolean> destroyCacheAsync(String cacheName, boolean sql, boolean checkThreadTx) {
        CU.validateCacheName(cacheName);

        guard();

        try {
            checkClusterState();

            return ctx.cache().dynamicDestroyCache(cacheName, sql, checkThreadTx, false, null);
        }
        finally {
            unguard();
        }
    }

    /**
     * @param cacheNames Collection of cache names.
     * @param checkThreadTx If {@code true} checks that current thread does not have active transactions.
     * @return Ignite future which will be completed when cache is destored.
     */
    public IgniteInternalFuture<?> destroyCachesAsync(Collection<String> cacheNames, boolean checkThreadTx) {
        CU.validateCacheNames(cacheNames);

        guard();

        try {
            checkClusterState();

            return ctx.cache().dynamicDestroyCaches(cacheNames, checkThreadTx);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(String cacheName) {
        CU.validateNewCacheName(cacheName);

        guard();

        try {
            checkClusterState();

            IgniteCacheProxy<K, V> cache = ctx.cache().publicJCache(cacheName, false, true);

            if (cache == null) {
                ctx.cache().getOrCreateFromTemplate(cacheName, true).get();

                return ctx.cache().publicJCache(cacheName);
            }

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
     * @param cacheName Cache name.
     * @param templateName Template name.
     * @param cfgOverride Cache config properties to override.
     * @param checkThreadTx If {@code true} checks that current thread does not have active transactions.
     * @return Future that will be completed when cache is deployed.
     */
    public IgniteInternalFuture<?> getOrCreateCacheAsync(String cacheName, String templateName,
        CacheConfigurationOverride cfgOverride, boolean checkThreadTx) {
        CU.validateNewCacheName(cacheName);

        guard();

        try {
            checkClusterState();

            if (ctx.cache().cache(cacheName) == null)
                return ctx.cache().getOrCreateFromTemplate(cacheName, templateName, cfgOverride, checkThreadTx);

            return new GridFinishedFuture<>();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> void addCacheConfiguration(CacheConfiguration<K, V> cacheCfg) {
        A.notNull(cacheCfg, "cacheCfg");
        CU.validateNewCacheName(cacheCfg.getName());

        guard();

        try {
            checkClusterState();

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
     * @return Collection of public cache instances.
     */
    public Collection<IgniteCacheProxy<?, ?>> caches() {
        guard();

        try {
            checkClusterState();

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
            checkClusterState();

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
            checkClusterState();

            return ctx.cache().utilityCache();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteInternalCache<K, V> cachex(String name) {
        CU.validateCacheName(name);

        guard();

        try {
            checkClusterState();

            return ctx.cache().cache(name);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteInternalCache<?, ?>> cachesx(
        IgnitePredicate<? super IgniteInternalCache<?, ?>>[] p) {
        guard();

        try {
            checkClusterState();

            return F.retain(ctx.cache().caches(), true, p);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteDataStreamer<K, V> dataStreamer(String cacheName) {
        CU.validateCacheName(cacheName);

        guard();

        try {
            checkClusterState();

            return ctx.<K, V>dataStream().dataStreamer(cacheName);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFileSystem fileSystem(String name) {
        if (name == null)
            throw new IllegalArgumentException("IGFS name cannot be null");

        guard();

        try {
            checkClusterState();

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
    @Nullable @Override public IgniteFileSystem igfsx(String name) {
        if (name == null)
            throw new IllegalArgumentException("IGFS name cannot be null");

        guard();

        try {
            checkClusterState();

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
            checkClusterState();

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
            checkClusterState();

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
            checkClusterState();

            return (T)ctx.pluginProvider(name).plugin();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteBinary binary() {
        checkClusterState();

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
        Ignition.stop(igniteInstanceName, true);
    }

    /** {@inheritDoc} */
    @Override public <K> Affinity<K> affinity(String cacheName) {
        CU.validateCacheName(cacheName);
        checkClusterState();

        GridCacheAdapter<K, ?> cache = ctx.cache().internalCache(cacheName);

        if (cache != null)
            return cache.affinity();

        return ctx.affinity().affinityProxy(cacheName);
    }

    /** {@inheritDoc} */
    @Override public boolean active() {
        guard();

        try {
            return context().state().publicApiActiveState(true);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void active(boolean active) {
        cluster().active(active);
    }

    /** */
    private Collection<BaselineNode> baselineNodes() {
        Collection<ClusterNode> srvNodes = cluster().forServers().nodes();

        ArrayList baselineNodes = new ArrayList(srvNodes.size());

        for (ClusterNode clN : srvNodes)
            baselineNodes.add(clN);

        return baselineNodes;
    }

    /** {@inheritDoc} */
    @Override public void resetLostPartitions(Collection<String> cacheNames) {
        CU.validateCacheNames(cacheNames);

        guard();

        try {
            ctx.cache().resetCacheState(cacheNames.isEmpty() ? ctx.cache().cacheNames() : cacheNames).get();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<DataRegionMetrics> dataRegionMetrics() {
        guard();

        try {
            return ctx.cache().context().database().memoryMetrics();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public DataRegionMetrics dataRegionMetrics(String memPlcName) {
        guard();

        try {
            return ctx.cache().context().database().memoryMetrics(memPlcName);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public DataStorageMetrics dataStorageMetrics() {
        guard();

        try {
            return ctx.cache().context().database().persistentStoreMetrics();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteEncryption encryption() {
        return ctx.encryption();
    }

    /** {@inheritDoc} */
    @Override public Collection<MemoryMetrics> memoryMetrics() {
        return DataRegionMetricsAdapter.collectionOf(dataRegionMetrics());
    }

    /** {@inheritDoc} */
    @Nullable @Override public MemoryMetrics memoryMetrics(String memPlcName) {
        return DataRegionMetricsAdapter.valueOf(dataRegionMetrics(memPlcName));
    }

    /** {@inheritDoc} */
    @Override public PersistenceMetrics persistentStoreMetrics() {
        return DataStorageMetricsAdapter.valueOf(dataStorageMetrics());
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteAtomicSequence atomicSequence(String name, long initVal, boolean create) {
        return atomicSequence(name, null, initVal, create);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteAtomicSequence atomicSequence(String name, AtomicConfiguration cfg, long initVal,
        boolean create) throws IgniteException {
        guard();

        try {
            checkClusterState();

            return ctx.dataStructures().sequence(name, cfg, initVal, create);
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
        return atomicLong(name, null, initVal, create);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteAtomicLong atomicLong(String name, AtomicConfiguration cfg, long initVal,
        boolean create) throws IgniteException {
        guard();

        try {
            checkClusterState();

            return ctx.dataStructures().atomicLong(name, cfg, initVal, create);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> IgniteAtomicReference<T> atomicReference(
        String name,
        @Nullable T initVal,
        boolean create
    ) {
        return atomicReference(name, null, initVal, create);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteAtomicReference<T> atomicReference(String name, AtomicConfiguration cfg,
        @Nullable T initVal, boolean create) throws IgniteException {
        guard();

        try {
            checkClusterState();

            return ctx.dataStructures().atomicReference(name, cfg, initVal, create);
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
        return atomicStamped(name, null, initVal, initStamp, create);
    }

    /** {@inheritDoc} */
    @Override public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name, AtomicConfiguration cfg,
        @Nullable T initVal, @Nullable S initStamp, boolean create) throws IgniteException {
        guard();

        try {
            checkClusterState();

            return ctx.dataStructures().atomicStamped(name, cfg, initVal, initStamp, create);
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
            checkClusterState();

            return ctx.dataStructures().countDownLatch(name, null, cnt, autoDel, create);
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
            checkClusterState();

            return ctx.dataStructures().semaphore(name, null, cnt, failoverSafe, create);
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
            checkClusterState();

            return ctx.dataStructures().reentrantLock(name, null, failoverSafe, fair, create);
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
        CollectionConfiguration cfg) {
        guard();

        try {
            checkClusterState();

            return ctx.dataStructures().queue(name, null, cap, cfg);
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
        CollectionConfiguration cfg) {
        guard();

        try {
            checkClusterState();

            return ctx.dataStructures().set(name, null, cfg);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /**
     * The {@code ctx.gateway().readLock()} is used underneath.
     */
    private void guard() {
        assert ctx != null;

        ctx.gateway().readLock();
    }

    /**
     * The {@code ctx.gateway().readUnlock()} is used underneath.
     */
    private void unguard() {
        assert ctx != null;

        ctx.gateway().readUnlock();
    }

    /**
     * Validate operation on cluster. Check current cluster state.
     *
     * @throws IgniteException If cluster in inActive state.
     */
    private void checkClusterState() throws IgniteException {
        if (!ctx.state().publicApiActiveState(true)) {
            throw new IgniteException("Can not perform the operation because the cluster is inactive. Note, that " +
                "the cluster is considered inactive by default if Ignite Persistent Store is used to let all the nodes " +
                "join the cluster. To activate the cluster call Ignite.active(true).");
        }
    }

    /**
     * Method is responsible for handling the {@link EventType#EVT_CLIENT_NODE_DISCONNECTED} event. Notify all the
     * GridComponents that the such even has been occurred (e.g. if the local client node disconnected from the cluster
     * components will be notified with a future which will be completed when the client is reconnected).
     */
    public void onDisconnected() {
        Throwable err = null;

        reconnectState.waitPreviousReconnect();

        GridFutureAdapter<?> reconnectFut = ctx.gateway().onDisconnected();

        if (reconnectFut == null) {
            assert ctx.gateway().getState() != STARTED : ctx.gateway().getState();

            return;
        }

        IgniteFutureImpl<?> curFut = (IgniteFutureImpl<?>)ctx.cluster().get().clientReconnectFuture();

        IgniteFuture<?> userFut;

        // In case of previous reconnect did not finish keep reconnect future.
        if (curFut != null && curFut.internalFuture() == reconnectFut)
            userFut = curFut;
        else {
            userFut = new IgniteFutureImpl<>(reconnectFut);

            ctx.cluster().get().clientReconnectFuture(userFut);
        }

        ctx.disconnected(true);

        List<GridComponent> comps = ctx.components();

        for (ListIterator<GridComponent> it = comps.listIterator(comps.size()); it.hasPrevious(); ) {
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

            GridCompoundFuture curReconnectFut = reconnectState.curReconnectFut = new GridCompoundFuture<>();

            reconnectState.reconnectDone = new GridFutureAdapter<>();

            for (GridComponent comp : ctx.components()) {
                IgniteInternalFuture<?> fut = comp.onReconnected(clusterRestarted);

                if (fut != null)
                    curReconnectFut.add(fut);
            }

            curReconnectFut.add(ctx.cache().context().exchange().reconnectExchangeFuture());

            curReconnectFut.markInitialized();

            final GridFutureAdapter reconnectDone = reconnectState.reconnectDone;

            curReconnectFut.listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> fut) {
                    try {
                        Object res = fut.get();

                        if (res == STOP_RECONNECT)
                            return;

                        ctx.gateway().onReconnected();

                        reconnectState.firstReconnectFut.onDone();
                    }
                    catch (IgniteCheckedException e) {
                        if (!X.hasCause(e, IgniteNeedReconnectException.class,
                            IgniteClientDisconnectedCheckedException.class,
                            IgniteInterruptedCheckedException.class)) {
                            U.error(log, "Failed to reconnect, will stop node.", e);

                            reconnectState.firstReconnectFut.onDone(e);

                            new Thread(() -> {
                                U.error(log, "Stopping the node after a failed reconnect attempt.");

                                close();
                            }, "node-stopper").start();
                        }
                        else {
                            assert ctx.discovery().reconnectSupported();

                            U.error(log, "Failed to finish reconnect, will retry [locNodeId=" + ctx.localNodeId() +
                                ", err=" + e.getMessage() + ']');
                        }
                    }
                    finally {
                        reconnectDone.onDone();
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

            if (!X.hasCause(err, NodeStoppingException.class))
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

        if (cls.equals(IGridClusterStateProcessor.class))
            return (T)new GridClusterStateProcessor(ctx);

        if(cls.equals(GridSecurityProcessor.class))
            return null;

        if (cls.equals(IgniteRestProcessor.class))
            return (T)new GridRestProcessor(ctx);

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
        igniteInstanceName = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, igniteInstanceName);
    }

    /**
     * @return IgniteKernal instance.
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
    @Override public void dumpDebugInfo() {
        try {
            GridKernalContextImpl ctx = this.ctx;

            GridDiscoveryManager discoMrg = ctx != null ? ctx.discovery() : null;

            ClusterNode locNode = discoMrg != null ? discoMrg.localNode() : null;

            if (ctx != null && discoMrg != null && locNode != null) {
                boolean client = ctx.clientNode();

                UUID routerId = locNode instanceof TcpDiscoveryNode ? ((TcpDiscoveryNode)locNode).clientRouterNodeId() : null;

                U.warn(ctx.cluster().diagnosticLog(), "Dumping debug info for node [id=" + locNode.id() +
                    ", name=" + ctx.igniteInstanceName() +
                    ", order=" + locNode.order() +
                    ", topVer=" + discoMrg.topologyVersion() +
                    ", client=" + client +
                    (client && routerId != null ? ", routerId=" + routerId : "") + ']');

                ctx.cache().context().exchange().dumpDebugInfo(null);
            }
            else
                U.warn(log, "Dumping debug info for node, context is not initialized [name=" + igniteInstanceName +
                    ']');
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

    /**
     * Registers metrics.
     */
    private void registerMetrics() {
        if (!ctx.metric().enabled())
            return;

        MetricRegistry reg = ctx.metric().registry(GridMetricManager.IGNITE_METRICS);

        reg.register("fullVersion", this::getFullVersion, String.class, FULL_VER_DESC);
        reg.register("copyright", this::getCopyright, String.class, COPYRIGHT_DESC);

        reg.register("startTimestampFormatted", this::getStartTimestampFormatted, String.class,
            START_TIMESTAMP_FORMATTED_DESC);

        reg.register("isRebalanceEnabled", this::isRebalanceEnabled, IS_REBALANCE_ENABLED_DESC);
        reg.register("uptimeFormatted", this::getUpTimeFormatted, String.class, UPTIME_FORMATTED_DESC);
        reg.register("startTimestamp", this::getStartTimestamp, START_TIMESTAMP_DESC);
        reg.register("uptime", this::getUpTime, UPTIME_DESC);
        reg.register("osInformation", this::getOsInformation, String.class, OS_INFO_DESC);
        reg.register("jdkInformation", this::getJdkInformation, String.class, JDK_INFO_DESC);
        reg.register("osUser", this::getOsUser, String.class, OS_USER_DESC);
        reg.register("vmName", this::getVmName, String.class, VM_NAME_DESC);
        reg.register("instanceName", this::getInstanceName, String.class, INSTANCE_NAME_DESC);

        reg.register("currentCoordinatorFormatted", this::getCurrentCoordinatorFormatted, String.class,
            CUR_COORDINATOR_FORMATTED_DESC);

        reg.register("isNodeInBaseline", this::isNodeInBaseline, IS_NODE_BASELINE_DESC);
        reg.register("longJVMPausesCount", this::getLongJVMPausesCount, LONG_JVM_PAUSES_CNT_DESC);

        reg.register("longJVMPausesTotalDuration", this::getLongJVMPausesTotalDuration,
            LONG_JVM_PAUSES_TOTAL_DURATION_DESC);

        reg.register("longJVMPauseLastEvents", this::getLongJVMPauseLastEvents, Map.class,
            LONG_JVM_PAUSE_LAST_EVENTS_DESC);

        reg.register("active", () -> ctx.state().clusterState().active()/*this::active*/, Boolean.class,
            ACTIVE_DESC);

        reg.register("clusterState", this::clusterState, String.class, CLUSTER_STATE_DESC);
        reg.register("lastClusterStateChangeTime", this::lastClusterStateChangeTime, LAST_CLUSTER_STATE_CHANGE_TIME_DESC);

        reg.register("userAttributesFormatted", this::getUserAttributesFormatted, List.class,
            USER_ATTRS_FORMATTED_DESC);

        reg.register("gridLoggerFormatted", this::getGridLoggerFormatted, String.class,
            GRID_LOG_FORMATTED_DESC);

        reg.register("executorServiceFormatted", this::getExecutorServiceFormatted, String.class,
            EXECUTOR_SRVC_FORMATTED_DESC);

        reg.register("igniteHome", this::getIgniteHome, String.class, IGNITE_HOME_DESC);

        reg.register("mBeanServerFormatted", this::getMBeanServerFormatted, String.class,
            MBEAN_SERVER_FORMATTED_DESC);

        reg.register("localNodeId", this::getLocalNodeId, UUID.class, LOC_NODE_ID_DESC);

        reg.register("isPeerClassLoadingEnabled", this::isPeerClassLoadingEnabled, Boolean.class,
            IS_PEER_CLS_LOADING_ENABLED_DESC);

        reg.register("lifecycleBeansFormatted", this::getLifecycleBeansFormatted, List.class,
            LIFECYCLE_BEANS_FORMATTED_DESC);

        reg.register("discoverySpiFormatted", this::getDiscoverySpiFormatted, String.class,
            DISCOVERY_SPI_FORMATTED_DESC);

        reg.register("communicationSpiFormatted", this::getCommunicationSpiFormatted, String.class,
            COMMUNICATION_SPI_FORMATTED_DESC);

        reg.register("deploymentSpiFormatted", this::getDeploymentSpiFormatted, String.class,
            DEPLOYMENT_SPI_FORMATTED_DESC);

        reg.register("checkpointSpiFormatted", this::getCheckpointSpiFormatted, String.class,
            CHECKPOINT_SPI_FORMATTED_DESC);

        reg.register("collisionSpiFormatted", this::getCollisionSpiFormatted, String.class,
            COLLISION_SPI_FORMATTED_DESC);

        reg.register("eventStorageSpiFormatted", this::getEventStorageSpiFormatted, String.class,
            EVT_STORAGE_SPI_FORMATTED_DESC);

        reg.register("failoverSpiFormatted", this::getFailoverSpiFormatted, String.class,
            FAILOVER_SPI_FORMATTED_DESC);

        reg.register("loadBalancingSpiFormatted", this::getLoadBalancingSpiFormatted, String.class,
            LOAD_BALANCING_SPI_FORMATTED_DESC);
    }

    /**
     * Class holds client reconnection event handling state.
     */
    private class ReconnectState {
        /** Future will be completed when the client node connected the first time. */
        private final GridFutureAdapter firstReconnectFut = new GridFutureAdapter();

        /**
         * Composed future of all {@link GridComponent#onReconnected(boolean)} callbacks.
         * The future completes when all Ignite components are finished handle given client reconnect event.
         */
        private GridCompoundFuture<?, Object> curReconnectFut;

        /** Future completes when reconnection handling is done (doesn't matter successfully or not). */
        private GridFutureAdapter<?> reconnectDone;

        /**
         * @throws IgniteCheckedException If failed.
         */
        void waitFirstReconnect() throws IgniteCheckedException {
            firstReconnectFut.get();
        }

        /**
         * Wait for the previous reconnection handling finished or force completion if not.
         */
        void waitPreviousReconnect() {
            if (curReconnectFut != null && !curReconnectFut.isDone()) {
                assert reconnectDone != null;

                curReconnectFut.onDone(STOP_RECONNECT);

                try {
                    reconnectDone.get();
                }
                catch (IgniteCheckedException ignore) {
                    // No-op.
                }
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ReconnectState.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override public void runIoTest(
        long warmup,
        long duration,
        int threads,
        long maxLatency,
        int rangesCnt,
        int payLoadSize,
        boolean procFromNioThread
    ) {
        ctx.io().runIoTest(warmup, duration, threads, maxLatency, rangesCnt, payLoadSize, procFromNioThread,
            new ArrayList(ctx.cluster().get().forServers().forRemotes().nodes()));
    }

    /** {@inheritDoc} */
    @Override public void clearNodeLocalMap() {
        ctx.cluster().get().clearNodeMap();
    }

    /** {@inheritDoc} */
    @Override public void clusterState(String state) {
        ClusterState newState = ClusterState.valueOf(state);

        cluster().state(newState);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteKernal.class, this);
    }
}
