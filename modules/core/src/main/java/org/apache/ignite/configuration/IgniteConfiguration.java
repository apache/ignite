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

package org.apache.ignite.configuration;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.UUID;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryListener;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.processor.EntryProcessor;
import javax.management.MBeanServer;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteAsyncCallback;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.segmentation.SegmentationPolicy;
import org.apache.ignite.plugin.segmentation.SegmentationResolver;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.checkpoint.CheckpointSpi;
import org.apache.ignite.spi.checkpoint.noop.NoopCheckpointSpi;
import org.apache.ignite.spi.collision.CollisionSpi;
import org.apache.ignite.spi.collision.noop.NoopCollisionSpi;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.deployment.DeploymentSpi;
import org.apache.ignite.spi.deployment.local.LocalDeploymentSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.eventstorage.EventStorageSpi;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;
import org.apache.ignite.spi.failover.FailoverSpi;
import org.apache.ignite.spi.failover.always.AlwaysFailoverSpi;
import org.apache.ignite.spi.indexing.IndexingSpi;
import org.apache.ignite.spi.loadbalancing.LoadBalancingSpi;
import org.apache.ignite.spi.loadbalancing.roundrobin.RoundRobinLoadBalancingSpi;
import org.apache.ignite.spi.swapspace.SwapSpaceSpi;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.ssl.SslContextFactory;

import static org.apache.ignite.plugin.segmentation.SegmentationPolicy.STOP;

/**
 * This class defines grid runtime configuration. This configuration is passed to
 * {@link Ignition#start(IgniteConfiguration)} method. It defines all configuration
 * parameters required to start a grid instance. Usually, a special
 * class called "loader" will create an instance of this interface and apply
 * {@link Ignition#start(IgniteConfiguration)} method to initialize Ignite instance.
 * <p>
 * Note that you should only set values that differ from defaults, as grid
 * will automatically pick default values for all values that are not set.
 * <p>
 * For more information about grid configuration and startup refer to {@link Ignition}
 * documentation.
 */
public class IgniteConfiguration {
    /** Courtesy notice log category. */
    public static final String COURTESY_LOGGER_NAME = "org.apache.ignite.CourtesyConfigNotice";

    /**
     * Default flag for peer class loading. By default the value is {@code false}
     * which means that peer class loading is disabled.
     */
    public static final boolean DFLT_P2P_ENABLED = false;

    /** Default metrics history size (value is {@code 10000}). */
    public static final int DFLT_METRICS_HISTORY_SIZE = 10000;

    /** Default metrics update frequency. */
    public static final long DFLT_METRICS_UPDATE_FREQ = 2000;

    /**
     * Default metrics expire time. The value is {@link Long#MAX_VALUE} which
     * means that metrics never expire.
     */
    public static final long DFLT_METRICS_EXPIRE_TIME = Long.MAX_VALUE;

    /** Default maximum timeout to wait for network responses in milliseconds (value is {@code 5,000ms}). */
    public static final long DFLT_NETWORK_TIMEOUT = 5000;

    /** Default interval between message send retries. */
    public static final long DFLT_SEND_RETRY_DELAY = 1000;

    /** Default message send retries count. */
    public static final int DFLT_SEND_RETRY_CNT = 3;

    /** Default number of clock sync samples. */
    public static final int DFLT_CLOCK_SYNC_SAMPLES = 8;

    /** Default clock synchronization frequency. */
    public static final int DFLT_CLOCK_SYNC_FREQUENCY = 120000;

    /** Default discovery startup delay in milliseconds (value is {@code 60,000ms}). */
    public static final long DFLT_DISCOVERY_STARTUP_DELAY = 60000;

    /** Default deployment mode (value is {@link DeploymentMode#SHARED}). */
    public static final DeploymentMode DFLT_DEPLOYMENT_MODE = DeploymentMode.SHARED;

    /** Default cache size for missed resources. */
    public static final int DFLT_P2P_MISSED_RESOURCES_CACHE_SIZE = 100;

    /** Default time server port base. */
    public static final int DFLT_TIME_SERVER_PORT_BASE = 31100;

    /** Default time server port range. */
    public static final int DFLT_TIME_SERVER_PORT_RANGE = 100;

    /** Default core size of public thread pool. */
    public static final int AVAILABLE_PROC_CNT = Runtime.getRuntime().availableProcessors();

    /** Default core size of public thread pool. */
    public static final int DFLT_PUBLIC_THREAD_CNT = Math.max(8, AVAILABLE_PROC_CNT);

    /** Default keep alive time for public thread pool. */
    @Deprecated
    public static final long DFLT_PUBLIC_KEEP_ALIVE_TIME = 0;

    /** Default limit of threads used for rebalance. */
    public static final int DFLT_REBALANCE_THREAD_POOL_SIZE = 1;

    /** Default max queue capacity of public thread pool. */
    @Deprecated
    public static final int DFLT_PUBLIC_THREADPOOL_QUEUE_CAP = Integer.MAX_VALUE;

    /** Default size of system thread pool. */
    public static final int DFLT_SYSTEM_CORE_THREAD_CNT = DFLT_PUBLIC_THREAD_CNT;

    /** Default max size of system thread pool. */
    @Deprecated
    public static final int DFLT_SYSTEM_MAX_THREAD_CNT = DFLT_PUBLIC_THREAD_CNT;

    /** Default keep alive time for system thread pool. */
    @Deprecated
    public static final long DFLT_SYSTEM_KEEP_ALIVE_TIME = 0;

    /** Default keep alive time for utility thread pool. */
    @Deprecated
    public static final long DFLT_UTILITY_KEEP_ALIVE_TIME = 10_000;

    /** Default max queue capacity of system thread pool. */
    @Deprecated
    public static final int DFLT_SYSTEM_THREADPOOL_QUEUE_CAP = Integer.MAX_VALUE;

    /** Default Ignite thread keep alive time. */
    public static final long DFLT_THREAD_KEEP_ALIVE_TIME = 60_000L;

    /** Default size of peer class loading thread pool. */
    public static final int DFLT_P2P_THREAD_CNT = 2;

    /** Default size of management thread pool. */
    public static final int DFLT_MGMT_THREAD_CNT = 4;

    /** Default segmentation policy. */
    public static final SegmentationPolicy DFLT_SEG_PLC = STOP;

    /** Default value for wait for segment on startup flag. */
    public static final boolean DFLT_WAIT_FOR_SEG_ON_START = true;

    /** Default value for all segmentation resolvers pass required. */
    public static final boolean DFLT_ALL_SEG_RESOLVERS_PASS_REQ = true;

    /** Default value segmentation resolve attempts count. */
    public static final int DFLT_SEG_RESOLVE_ATTEMPTS = 2;

    /** Default segment check frequency in discovery manager. */
    public static final long DFLT_SEG_CHK_FREQ = 10000;

    /** Default frequency of metrics log print out. */
    public static final long DFLT_METRICS_LOG_FREQ = 60000;

    /** Default TCP server port. */
    public static final int DFLT_TCP_PORT = 11211;

    /** Default marshal local jobs flag. */
    public static final boolean DFLT_MARSHAL_LOCAL_JOBS = false;

    /** Default value for cache sanity check enabled flag. */
    public static final boolean DFLT_CACHE_SANITY_CHECK_ENABLED = true;

    /** Default value for late affinity assignment flag. */
    public static final boolean DFLT_LATE_AFF_ASSIGNMENT = true;

    /** Default failure detection timeout in millis. */
    @SuppressWarnings("UnnecessaryBoxing")
    public static final Long DFLT_FAILURE_DETECTION_TIMEOUT = new Long(10_000);

    /** Optional grid name. */
    private String gridName;

    /** User attributes. */
    private Map<String, ?> userAttrs;

    /** Logger. */
    private IgniteLogger log;

    /** Public pool size. */
    private int pubPoolSize = DFLT_PUBLIC_THREAD_CNT;

    /** Async Callback pool size. */
    private int callbackPoolSize = DFLT_PUBLIC_THREAD_CNT;

    /**
     * Use striped pool for internal requests processing when possible
     * (e.g. cache requests per-partition striping).
     */
    private int stripedPoolSize = DFLT_PUBLIC_THREAD_CNT;

    /** System pool size. */
    private int sysPoolSize = DFLT_SYSTEM_CORE_THREAD_CNT;

    /** Management pool size. */
    private int mgmtPoolSize = DFLT_MGMT_THREAD_CNT;

    /** IGFS pool size. */
    private int igfsPoolSize = AVAILABLE_PROC_CNT;

    /** Utility cache pool size. */
    private int utilityCachePoolSize = DFLT_SYSTEM_CORE_THREAD_CNT;

    /** Utility cache pool keep alive time. */
    private long utilityCacheKeepAliveTime = DFLT_THREAD_KEEP_ALIVE_TIME;

    /** Marshaller pool size. */
    private int marshCachePoolSize = DFLT_SYSTEM_CORE_THREAD_CNT;

    /** Marshaller pool keep alive time. */
    private long marshCacheKeepAliveTime = DFLT_THREAD_KEEP_ALIVE_TIME;

    /** P2P pool size. */
    private int p2pPoolSize = DFLT_P2P_THREAD_CNT;

    /** Ignite installation folder. */
    private String igniteHome;

    /** Ignite work folder. */
    private String igniteWorkDir;

    /** MBean server. */
    private MBeanServer mbeanSrv;

    /** Local node ID. */
    private UUID nodeId;

    /** Marshaller. */
    private Marshaller marsh;

    /** Marshal local jobs. */
    private boolean marshLocJobs = DFLT_MARSHAL_LOCAL_JOBS;

    /** Daemon flag. */
    private boolean daemon;

    /** Whether or not peer class loading is enabled. */
    private boolean p2pEnabled = DFLT_P2P_ENABLED;

    /** List of package prefixes from the system class path that should be P2P loaded. */
    private String[] p2pLocClsPathExcl;

    /** Events of these types should be recorded. */
    private int[] inclEvtTypes;

    /** Maximum network requests timeout. */
    private long netTimeout = DFLT_NETWORK_TIMEOUT;

    /** Interval between message send retries. */
    private long sndRetryDelay = DFLT_SEND_RETRY_DELAY;

    /** Message send retries delay. */
    private int sndRetryCnt = DFLT_SEND_RETRY_CNT;

    /** Number of samples for clock synchronization. */
    private int clockSyncSamples = DFLT_CLOCK_SYNC_SAMPLES;

    /** Clock synchronization frequency. */
    private long clockSyncFreq = DFLT_CLOCK_SYNC_FREQUENCY;

    /** Metrics history time. */
    private int metricsHistSize = DFLT_METRICS_HISTORY_SIZE;

    /** Full metrics enabled flag. */
    private long metricsUpdateFreq = DFLT_METRICS_UPDATE_FREQ;

    /** Metrics expire time. */
    private long metricsExpTime = DFLT_METRICS_EXPIRE_TIME;

    /** Collection of life-cycle beans. */
    private LifecycleBean[] lifecycleBeans;

    /** Discovery SPI. */
    private DiscoverySpi discoSpi;

    /** Segmentation policy. */
    private SegmentationPolicy segPlc = DFLT_SEG_PLC;

    /** Segmentation resolvers. */
    private SegmentationResolver[] segResolvers;

    /** Segmentation resolve attempts count. */
    private int segResolveAttempts = DFLT_SEG_RESOLVE_ATTEMPTS;

    /** Wait for segment on startup flag. */
    private boolean waitForSegOnStart = DFLT_WAIT_FOR_SEG_ON_START;

    /** All segmentation resolvers pass required flag. */
    private boolean allResolversPassReq = DFLT_ALL_SEG_RESOLVERS_PASS_REQ;

    /** Segment check frequency. */
    private long segChkFreq = DFLT_SEG_CHK_FREQ;

    /** Communication SPI. */
    private CommunicationSpi commSpi;

    /** Event storage SPI. */
    private EventStorageSpi evtSpi;

    /** Collision SPI. */
    private CollisionSpi colSpi;

    /** Deployment SPI. */
    private DeploymentSpi deploySpi;

    /** Checkpoint SPI. */
    private CheckpointSpi[] cpSpi;

    /** Failover SPI. */
    private FailoverSpi[] failSpi;

    /** Load balancing SPI. */
    private LoadBalancingSpi[] loadBalancingSpi;

    /** Checkpoint SPI. */
    private SwapSpaceSpi swapSpaceSpi;

    /** Indexing SPI. */
    private IndexingSpi indexingSpi;

    /** Address resolver. */
    private AddressResolver addrRslvr;

    /** Cache configurations. */
    private CacheConfiguration[] cacheCfg;

    /** Client mode flag. */
    private Boolean clientMode;

    /** Rebalance thread pool size. */
    private int rebalanceThreadPoolSize = DFLT_REBALANCE_THREAD_POOL_SIZE;

    /** Transactions configuration. */
    private TransactionConfiguration txCfg = new TransactionConfiguration();

    /** */
    private PluginConfiguration[] pluginCfgs;

    /** Flag indicating whether cache sanity check is enabled. */
    private boolean cacheSanityCheckEnabled = DFLT_CACHE_SANITY_CHECK_ENABLED;

    /** Discovery startup delay. */
    private long discoStartupDelay = DFLT_DISCOVERY_STARTUP_DELAY;

    /** Tasks classes sharing mode. */
    private DeploymentMode deployMode = DFLT_DEPLOYMENT_MODE;

    /** Cache size of missed resources. */
    private int p2pMissedCacheSize = DFLT_P2P_MISSED_RESOURCES_CACHE_SIZE;

    /** Local host. */
    private String locHost;

    /** Base port number for time server. */
    private int timeSrvPortBase = DFLT_TIME_SERVER_PORT_BASE;

    /** Port number range for time server. */
    private int timeSrvPortRange = DFLT_TIME_SERVER_PORT_RANGE;

    /** Failure detection timeout. */
    private Long failureDetectionTimeout = DFLT_FAILURE_DETECTION_TIMEOUT;

    /** Property names to include into node attributes. */
    private String[] includeProps;

    /** Frequency of metrics log print out. */
    @SuppressWarnings("RedundantFieldInitialization")
    private long metricsLogFreq = DFLT_METRICS_LOG_FREQ;

    /** Local event listeners. */
    private Map<IgnitePredicate<? extends Event>, int[]> lsnrs;

    /** IGFS configuration. */
    private FileSystemConfiguration[] igfsCfg;

    /** Service configuration. */
    private ServiceConfiguration[] svcCfgs;

    /** Hadoop configuration. */
    private HadoopConfiguration hadoopCfg;

    /** Client access configuration. */
    private ConnectorConfiguration connectorCfg = new ConnectorConfiguration();

    /** ODBC configuration. */
    private OdbcConfiguration odbcCfg;

    /** Warmup closure. Will be invoked before actual grid start. */
    private IgniteInClosure<IgniteConfiguration> warmupClos;

    /** */
    private AtomicConfiguration atomicCfg = new AtomicConfiguration();

    /** User's class loader. */
    private ClassLoader classLdr;

    /** Cache store session listeners. */
    private Factory<CacheStoreSessionListener>[] storeSesLsnrs;

    /** Consistent globally unique node ID which survives node restarts. */
    private Serializable consistentId;

    /** SSL connection factory. */
    private Factory<SSLContext> sslCtxFactory;

    /** Platform configuration. */
    private PlatformConfiguration platformCfg;

    /** Cache key configuration. */
    private CacheKeyConfiguration[] cacheKeyCfg;

    /** */
    private BinaryConfiguration binaryCfg;

    /** */
    private boolean lateAffAssignment = DFLT_LATE_AFF_ASSIGNMENT;

    /**
     * Creates valid grid configuration with all default values.
     */
    public IgniteConfiguration() {
        // No-op.
    }

    /**
     * Creates grid configuration by coping all configuration properties from
     * given configuration.
     *
     * @param cfg Grid configuration to copy from.
     */
    public IgniteConfiguration(IgniteConfiguration cfg) {
        assert cfg != null;

        // SPIs.
        discoSpi = cfg.getDiscoverySpi();
        commSpi = cfg.getCommunicationSpi();
        deploySpi = cfg.getDeploymentSpi();
        evtSpi = cfg.getEventStorageSpi();
        cpSpi = cfg.getCheckpointSpi();
        colSpi = cfg.getCollisionSpi();
        failSpi = cfg.getFailoverSpi();
        loadBalancingSpi = cfg.getLoadBalancingSpi();
        indexingSpi = cfg.getIndexingSpi();
        swapSpaceSpi = cfg.getSwapSpaceSpi();

        /*
         * Order alphabetically for maintenance purposes.
         */
        addrRslvr = cfg.getAddressResolver();
        allResolversPassReq = cfg.isAllSegmentationResolversPassRequired();
        atomicCfg = cfg.getAtomicConfiguration();
        binaryCfg = cfg.getBinaryConfiguration();
        daemon = cfg.isDaemon();
        cacheCfg = cfg.getCacheConfiguration();
        cacheKeyCfg = cfg.getCacheKeyConfiguration();
        cacheSanityCheckEnabled = cfg.isCacheSanityCheckEnabled();
        callbackPoolSize = cfg.getAsyncCallbackPoolSize();
        connectorCfg = cfg.getConnectorConfiguration();
        classLdr = cfg.getClassLoader();
        clientMode = cfg.isClientMode();
        clockSyncFreq = cfg.getClockSyncFrequency();
        clockSyncSamples = cfg.getClockSyncSamples();
        consistentId = cfg.getConsistentId();
        deployMode = cfg.getDeploymentMode();
        discoStartupDelay = cfg.getDiscoveryStartupDelay();
        failureDetectionTimeout = cfg.getFailureDetectionTimeout();
        gridName = cfg.getGridName();
        hadoopCfg = cfg.getHadoopConfiguration();
        igfsCfg = cfg.getFileSystemConfiguration();
        igfsPoolSize = cfg.getIgfsThreadPoolSize();
        igniteHome = cfg.getIgniteHome();
        igniteWorkDir = cfg.getWorkDirectory();
        inclEvtTypes = cfg.getIncludeEventTypes();
        includeProps = cfg.getIncludeProperties();
        lateAffAssignment = cfg.isLateAffinityAssignment();
        lifecycleBeans = cfg.getLifecycleBeans();
        locHost = cfg.getLocalHost();
        log = cfg.getGridLogger();
        lsnrs = cfg.getLocalEventListeners();
        marsh = cfg.getMarshaller();
        marshLocJobs = cfg.isMarshalLocalJobs();
        marshCacheKeepAliveTime = cfg.getMarshallerCacheKeepAliveTime();
        marshCachePoolSize = cfg.getMarshallerCacheThreadPoolSize();
        mbeanSrv = cfg.getMBeanServer();
        metricsHistSize = cfg.getMetricsHistorySize();
        metricsExpTime = cfg.getMetricsExpireTime();
        metricsLogFreq = cfg.getMetricsLogFrequency();
        metricsUpdateFreq = cfg.getMetricsUpdateFrequency();
        mgmtPoolSize = cfg.getManagementThreadPoolSize();
        netTimeout = cfg.getNetworkTimeout();
        nodeId = cfg.getNodeId();
        odbcCfg = cfg.getOdbcConfiguration();
        p2pEnabled = cfg.isPeerClassLoadingEnabled();
        p2pLocClsPathExcl = cfg.getPeerClassLoadingLocalClassPathExclude();
        p2pMissedCacheSize = cfg.getPeerClassLoadingMissedResourcesCacheSize();
        p2pPoolSize = cfg.getPeerClassLoadingThreadPoolSize();
        platformCfg = cfg.getPlatformConfiguration();
        pluginCfgs = cfg.getPluginConfigurations();
        pubPoolSize = cfg.getPublicThreadPoolSize();
        rebalanceThreadPoolSize = cfg.getRebalanceThreadPoolSize();
        segChkFreq = cfg.getSegmentCheckFrequency();
        segPlc = cfg.getSegmentationPolicy();
        segResolveAttempts = cfg.getSegmentationResolveAttempts();
        segResolvers = cfg.getSegmentationResolvers();
        sndRetryCnt = cfg.getNetworkSendRetryCount();
        sndRetryDelay = cfg.getNetworkSendRetryDelay();
        sslCtxFactory = cfg.getSslContextFactory();
        storeSesLsnrs = cfg.getCacheStoreSessionListenerFactories();
        stripedPoolSize = cfg.getStripedPoolSize();
        svcCfgs = cfg.getServiceConfiguration();
        sysPoolSize = cfg.getSystemThreadPoolSize();
        timeSrvPortBase = cfg.getTimeServerPortBase();
        timeSrvPortRange = cfg.getTimeServerPortRange();
        txCfg = cfg.getTransactionConfiguration();
        userAttrs = cfg.getUserAttributes();
        utilityCacheKeepAliveTime = cfg.getUtilityCacheKeepAliveTime();
        utilityCachePoolSize = cfg.getUtilityCacheThreadPoolSize();
        waitForSegOnStart = cfg.isWaitForSegmentOnStart();
        warmupClos = cfg.getWarmupClosure();
    }

    /**
     * Gets optional grid name. Returns {@code null} if non-default grid name was not
     * provided.
     *
     * @return Optional grid name. Can be {@code null}, which is default grid name, if
     *      non-default grid name was not provided.
     */
    public String getGridName() {
        return gridName;
    }

    /**
     * Whether or not this node should be a daemon node.
     * <p>
     * Daemon nodes are the usual grid nodes that participate in topology but not
     * visible on the main APIs, i.e. they are not part of any cluster groups. The only
     * way to see daemon nodes is to use {@link ClusterGroup#forDaemons()} method.
     * <p>
     * Daemon nodes are used primarily for management and monitoring functionality that
     * is build on Ignite and needs to participate in the topology, but also needs to be
     * excluded from the "normal" topology, so that it won't participate in the task execution
     * or in-memory data grid storage.
     *
     * @return {@code True} if this node should be a daemon node, {@code false} otherwise.
     * @see ClusterGroup#forDaemons()
     */
    public boolean isDaemon() {
        return daemon;
    }

    /**
     * Sets daemon flag.
     * <p>
     * Daemon nodes are the usual grid nodes that participate in topology but not
     * visible on the main APIs, i.e. they are not part of any cluster group. The only
     * way to see daemon nodes is to use {@link ClusterGroup#forDaemons()} method.
     * <p>
     * Daemon nodes are used primarily for management and monitoring functionality that
     * is build on Ignite and needs to participate in the topology, but also needs to be
     * excluded from the "normal" topology, so that it won't participate in the task execution
     * or in-memory data grid storage.
     *
     * @param daemon Daemon flag.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setDaemon(boolean daemon) {
        this.daemon = daemon;

        return this;
    }

    /**
     * Sets grid name. Note that {@code null} is a default grid name.
     *
     * @param gridName Grid name to set. Can be {@code null}, which is default
     *      grid name.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setGridName(String gridName) {
        this.gridName = gridName;

        return this;
    }

    /**
     * Sets consistent globally unique node ID which survives node restarts.
     *
     * @param consistentId Node consistent ID.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setConsistentId(Serializable consistentId) {
        this.consistentId = consistentId;

        return this;
    }

    /**
     * Gets consistent globally unique node ID which survives node restarts.
     *
     * @return Node consistent ID.
     */
    public Serializable getConsistentId() {
        return consistentId;
    }

    /**
     * Should return any user-defined attributes to be added to this node. These attributes can
     * then be accessed on nodes by calling {@link ClusterNode#attribute(String)} or
     * {@link ClusterNode#attributes()} methods.
     * <p>
     * Note that system adds the following (among others) attributes automatically:
     * <ul>
     * <li>{@code {@link System#getProperties()}} - All system properties.</li>
     * <li>{@code {@link System#getenv(String)}} - All environment properties.</li>
     * </ul>
     * <p>
     * Note that grid will add all System properties and environment properties
     * to grid node attributes also. SPIs may also add node attributes that are
     * used for SPI implementation.
     * <p>
     * <b>NOTE:</b> attributes names starting with {@code org.apache.ignite} are reserved
     * for internal use.
     *
     * @return User defined attributes for this node.
     */
    public Map<String, ?> getUserAttributes() {
        return userAttrs;
    }

    /**
     * Sets user attributes for this node.
     *
     * @param userAttrs User attributes for this node.
     * @see IgniteConfiguration#getUserAttributes()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setUserAttributes(Map<String, ?> userAttrs) {
        this.userAttrs = userAttrs;

        return this;
    }

    /**
     * Should return an instance of logger to use in grid. If not provided,
     * {@ignitelink org.apache.ignite.logger.log4j.Log4JLogger}
     * will be used.
     *
     * @return Logger to use in grid.
     */
    public IgniteLogger getGridLogger() {
        return log;
    }

    /**
     * Sets logger to use within grid.
     *
     * @param log Logger to use within grid.
     * @see IgniteConfiguration#getGridLogger()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setGridLogger(IgniteLogger log) {
        this.log = log;

        return this;
    }

    /**
     * Returns striped pool size that should be used for cache requests
     * processing.
     * <p>
     * If set to non-positive value then requests get processed in system pool.
     * <p>
     * Striped pool is better for typical cache operations.
     *
     * @return Positive value if striped pool should be initialized
     *      with configured number of threads (stripes) and used for requests processing
     *      or non-positive value to process requests in system pool.
     *
     * @see #getPublicThreadPoolSize()
     * @see #getSystemThreadPoolSize()
     */
    public int getStripedPoolSize() {
        return stripedPoolSize;
    }

    /**
     * Sets striped pool size that should be used for cache requests
     * processing.
     * <p>
     * If set to non-positive value then requests get processed in system pool.
     * <p>
     * Striped pool is better for typical cache operations.
     *
     * @param stripedPoolSize Positive value if striped pool should be initialized
     *      with passed in number of threads (stripes) and used for requests processing
     *      or non-positive value to process requests in system pool.
     * @return {@code this} for chaining.
     *
     * @see #getPublicThreadPoolSize()
     * @see #getSystemThreadPoolSize()
     */
    public IgniteConfiguration setStripedPoolSize(int stripedPoolSize) {
        this.stripedPoolSize = stripedPoolSize;

        return this;
    }

    /**
     * Should return a thread pool size to be used in grid.
     * This executor service will be in charge of processing {@link ComputeJob GridJobs}
     * and user messages sent to node.
     * <p>
     * If not provided, executor service will have size {@link #DFLT_PUBLIC_THREAD_CNT}.
     *
     * @return Thread pool size to be used in grid to process job execution
     *      requests and user messages sent to the node.
     */
    public int getPublicThreadPoolSize() {
        return pubPoolSize;
    }

    /**
     * Size of thread pool that is in charge of processing internal system messages.
     * <p>
     * If not provided, executor service will have size {@link #DFLT_SYSTEM_CORE_THREAD_CNT}.
     *
     * @return Thread pool size to be used in grid for internal system messages.
     */
    public int getSystemThreadPoolSize() {
        return sysPoolSize;
    }

    /**
     * Size of thread pool that is in charge of processing asynchronous callbacks.
     * <p>
     * This pool is used for callbacks annotated with {@link IgniteAsyncCallback}.
     * <p>
     * If not provided, executor service will have size {@link #DFLT_PUBLIC_THREAD_CNT}.
     *
     * @return Thread pool size to be used.
     * @see IgniteAsyncCallback
     */
    public int getAsyncCallbackPoolSize() {
        return callbackPoolSize;
    }

    /**
     * Size of thread pool that is in charge of processing internal and Visor
     * {@link ComputeJob GridJobs}.
     * <p>
     * If not provided, executor service will have size {@link #DFLT_MGMT_THREAD_CNT}
     *
     * @return Thread pool size to be used in grid for internal and Visor
     *      jobs processing.
     */
    public int getManagementThreadPoolSize() {
        return mgmtPoolSize;
    }

    /**
     * Size of thread pool which  is in charge of peer class loading requests/responses. If you don't use
     * peer class loading and use GAR deployment only we would recommend to decrease
     * the value of total threads to {@code 1}.
     * <p>
     * If not provided, executor service will have size {@link #DFLT_P2P_THREAD_CNT}.
     *
     * @return Thread pool size to be used for peer class loading
     *      requests handling.
     */
    public int getPeerClassLoadingThreadPoolSize() {
        return p2pPoolSize;
    }

    /**
     * Size of thread pool that is in charge of processing outgoing IGFS messages.
     * <p>
     * If not provided, executor service will have size equals number of processors available in system.
     *
     * @return Thread pool size to be used for IGFS outgoing message sending.
     */
    public int getIgfsThreadPoolSize() {
        return igfsPoolSize;
    }

    /**
     * Default size of thread pool that is in charge of processing utility cache messages.
     * <p>
     * If not provided, executor service will have size {@link #DFLT_SYSTEM_CORE_THREAD_CNT}.
     *
     * @return Default thread pool size to be used in grid for utility cache messages.
     */
    public int getUtilityCacheThreadPoolSize() {
        return utilityCachePoolSize;
    }

    /**
     * Keep alive time of thread pool that is in charge of processing utility cache messages.
     * <p>
     * If not provided, executor service will have keep alive time {@link #DFLT_THREAD_KEEP_ALIVE_TIME}.
     *
     * @return Thread pool keep alive time (in milliseconds) to be used in grid for utility cache messages.
     */
    public long getUtilityCacheKeepAliveTime() {
        return utilityCacheKeepAliveTime;
    }

    /**
     * Default size of thread pool that is in charge of processing marshaller messages.
     * <p>
     * If not provided, executor service will have size {@link #DFLT_SYSTEM_CORE_THREAD_CNT}.
     *
     * @return Default thread pool size to be used in grid for marshaller messages.
     */
    public int getMarshallerCacheThreadPoolSize() {
        return marshCachePoolSize;
    }

    /**
     * Keep alive time of thread pool that is in charge of processing marshaller messages.
     * <p>
     * If not provided, executor service will have keep alive time {@link #DFLT_THREAD_KEEP_ALIVE_TIME}.
     *
     * @return Thread pool keep alive time (in milliseconds) to be used in grid for marshaller messages.
     */
    public long getMarshallerCacheKeepAliveTime() {
        return marshCacheKeepAliveTime;
    }

    /**
     * Sets thread pool size to use within grid.
     *
     * @param poolSize Thread pool size to use within grid.
     * @see IgniteConfiguration#getPublicThreadPoolSize()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setPublicThreadPoolSize(int poolSize) {
        pubPoolSize = poolSize;

        return this;
    }

    /**
     * Sets system thread pool size to use within grid.
     *
     * @param poolSize Thread pool size to use within grid.
     * @see IgniteConfiguration#getSystemThreadPoolSize()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setSystemThreadPoolSize(int poolSize) {
        sysPoolSize = poolSize;

        return this;
    }

    /**
     * Sets async callback thread pool size to use within grid.
     *
     * @param poolSize Thread pool size to use within grid.
     * @return {@code this} for chaining.
     * @see IgniteConfiguration#getAsyncCallbackPoolSize()
     * @see IgniteAsyncCallback
     */
    public IgniteConfiguration setAsyncCallbackPoolSize(int poolSize) {
        this.callbackPoolSize = poolSize;

        return this;
    }

    /**
     * Sets management thread pool size to use within grid.
     *
     * @param poolSize Thread pool size to use within grid.
     * @see IgniteConfiguration#getManagementThreadPoolSize()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setManagementThreadPoolSize(int poolSize) {
        mgmtPoolSize = poolSize;

        return this;
    }

    /**
     * Sets thread pool size to use for peer class loading.
     *
     * @param poolSize Thread pool size to use within grid.
     * @see IgniteConfiguration#getPeerClassLoadingThreadPoolSize()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setPeerClassLoadingThreadPoolSize(int poolSize) {
        p2pPoolSize = poolSize;

        return this;
    }

    /**
     * Set thread pool size that will be used to process outgoing IGFS messages.
     *
     * @param poolSize Executor service to use for outgoing IGFS messages.
     * @see IgniteConfiguration#getIgfsThreadPoolSize()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setIgfsThreadPoolSize(int poolSize) {
        igfsPoolSize = poolSize;

        return this;
    }

    /**
     * Sets default thread pool size that will be used to process utility cache messages.
     *
     * @param poolSize Default executor service size to use for utility cache messages.
     * @see IgniteConfiguration#getUtilityCacheThreadPoolSize()
     * @see IgniteConfiguration#getUtilityCacheKeepAliveTime()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setUtilityCachePoolSize(int poolSize) {
        utilityCachePoolSize = poolSize;

        return this;
    }

    /**
     * Sets keep alive time of thread pool size that will be used to process utility cache messages.
     *
     * @param keepAliveTime Keep alive time of executor service to use for utility cache messages.
     * @see IgniteConfiguration#getUtilityCacheThreadPoolSize()
     * @see IgniteConfiguration#getUtilityCacheKeepAliveTime()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setUtilityCacheKeepAliveTime(long keepAliveTime) {
        utilityCacheKeepAliveTime = keepAliveTime;

        return this;
    }

    /**
     * Sets default thread pool size that will be used to process marshaller messages.
     *
     * @param poolSize Default executor service size to use for marshaller messages.
     * @see IgniteConfiguration#getMarshallerCacheThreadPoolSize()
     * @see IgniteConfiguration#getMarshallerCacheKeepAliveTime()
     * @return {@code this} for chaining.
     * @deprecated Use {@link #setMarshallerCacheThreadPoolSize(int)} instead.
     */
    @Deprecated
    public IgniteConfiguration setMarshallerCachePoolSize(int poolSize) {
        return setMarshallerCacheThreadPoolSize(poolSize);
    }

    /**
     * Sets default thread pool size that will be used to process marshaller messages.
     *
     * @param poolSize Default executor service size to use for marshaller messages.
     * @see IgniteConfiguration#getMarshallerCacheThreadPoolSize()
     * @see IgniteConfiguration#getMarshallerCacheKeepAliveTime()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setMarshallerCacheThreadPoolSize(int poolSize) {
        marshCachePoolSize = poolSize;

        return this;
    }

    /**
     * Sets maximum thread pool size that will be used to process marshaller messages.
     *
     * @param keepAliveTime Keep alive time of executor service to use for marshaller messages.
     * @see IgniteConfiguration#getMarshallerCacheThreadPoolSize()
     * @see IgniteConfiguration#getMarshallerCacheKeepAliveTime()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setMarshallerCacheKeepAliveTime(long keepAliveTime) {
        marshCacheKeepAliveTime = keepAliveTime;

        return this;
    }

    /**
     * Should return Ignite installation home folder. If not provided, the system will check
     * {@code IGNITE_HOME} system property and environment variable in that order. If
     * {@code IGNITE_HOME} still could not be obtained, then grid will not start and exception
     * will be thrown.
     *
     * @return Ignite installation home or {@code null} to make the system attempt to
     *      infer it automatically.
     * @see IgniteSystemProperties#IGNITE_HOME
     */
    public String getIgniteHome() {
        return igniteHome;
    }

    /**
     * Sets Ignite installation folder.
     *
     * @param igniteHome {@code Ignition} installation folder.
     * @see IgniteConfiguration#getIgniteHome()
     * @see IgniteSystemProperties#IGNITE_HOME
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setIgniteHome(String igniteHome) {
        this.igniteHome = igniteHome;

        return this;
    }

    /**
     * Gets Ignite work directory. If not provided, the method will use work directory under
     * {@code IGNITE_HOME} specified by {@link IgniteConfiguration#setIgniteHome(String)} or
     * {@code IGNITE_HOME} environment variable or system property.
     * <p>
     * If {@code IGNITE_HOME} is not provided, then system temp directory is used.
     *
     * @return Ignite work directory or {@code null} to make the system attempt to infer it automatically.
     * @see IgniteConfiguration#getIgniteHome()
     * @see IgniteSystemProperties#IGNITE_HOME
     */
    public String getWorkDirectory() {
        return igniteWorkDir;
    }

    /**
     * Sets Ignite work folder.
     *
     * @param igniteWorkDir {@code Ignite} work directory.
     * @see IgniteConfiguration#getWorkDirectory()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setWorkDirectory(String igniteWorkDir) {
        this.igniteWorkDir = igniteWorkDir;

        return this;
    }

    /**
     * Should return MBean server instance. If not provided, the system will use default
     * platform MBean server.
     *
     * @return MBean server instance or {@code null} to make the system create a default one.
     * @see ManagementFactory#getPlatformMBeanServer()
     */
    public MBeanServer getMBeanServer() {
        return mbeanSrv;
    }

    /**
     * Sets initialized and started MBean server.
     *
     * @param mbeanSrv Initialized and started MBean server.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setMBeanServer(MBeanServer mbeanSrv) {
        this.mbeanSrv = mbeanSrv;

        return this;
    }

    /**
     * Unique identifier for this node within grid.
     *
     * @return Unique identifier for this node within grid.
     */
    @Deprecated
    public UUID getNodeId() {
        return nodeId;
    }

    /**
     * Sets unique identifier for local node.
     *
     * @param nodeId Unique identifier for local node.
     * @see IgniteConfiguration#getNodeId()
     * @return {@code this} for chaining.
     * @deprecated Use {@link #setConsistentId(Serializable)} instead.
     */
    @Deprecated
    public IgniteConfiguration setNodeId(UUID nodeId) {
        this.nodeId = nodeId;

        return this;
    }

    /**
     * Should return an instance of marshaller to use in grid. If not provided,
     * default marshaller implementation that allows to read object field values
     * without deserialization will be used.
     *
     * @return Marshaller to use in grid.
     */
    public Marshaller getMarshaller() {
        return marsh;
    }

    /**
     * Sets marshaller to use within grid.
     *
     * @param marsh Marshaller to use within grid.
     * @see IgniteConfiguration#getMarshaller()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setMarshaller(Marshaller marsh) {
        this.marsh = marsh;

        return this;
    }


    /**
     * Returns {@code true} if peer class loading is enabled, {@code false}
     * otherwise. Default value is {@code false} specified by {@link #DFLT_P2P_ENABLED}.
     * <p>
     * When peer class loading is enabled and task is not deployed on local node,
     * local node will try to load classes from the node that initiated task
     * execution. This way, a task can be physically deployed only on one node
     * and then internally penetrate to all other nodes.
     * <p>
     * See {@link ComputeTask} documentation for more information about task deployment.
     *
     * @return {@code true} if peer class loading is enabled, {@code false}
     *      otherwise.
     */
    public boolean isPeerClassLoadingEnabled() {
        return p2pEnabled;
    }

    /**
     * If this flag is set to {@code true}, jobs mapped to local node will be
     * marshalled as if it was remote node.
     * <p>
     * If not provided, default value is defined by {@link #DFLT_MARSHAL_LOCAL_JOBS}.
     *
     * @return {@code True} if local jobs should be marshalled.
     */
    public boolean isMarshalLocalJobs() {
        return marshLocJobs;
    }

    /**
     * Sets marshal local jobs flag.
     *
     * @param marshLocJobs {@code True} if local jobs should be marshalled.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setMarshalLocalJobs(boolean marshLocJobs) {
        this.marshLocJobs = marshLocJobs;

        return this;
    }

    /**
     * Enables/disables peer class loading.
     *
     * @param p2pEnabled {@code true} if peer class loading is
     *      enabled, {@code false} otherwise.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setPeerClassLoadingEnabled(boolean p2pEnabled) {
        this.p2pEnabled = p2pEnabled;

        return this;
    }

    /**
     * Should return list of packages from the system classpath that need to
     * be peer-to-peer loaded from task originating node.
     * '*' is supported at the end of the package name which means
     * that all sub-packages and their classes are included like in Java
     * package import clause.
     *
     * @return List of peer-to-peer loaded package names.
     */
    public String[] getPeerClassLoadingLocalClassPathExclude() {
        return p2pLocClsPathExcl;
    }

    /**
     * Sets list of packages in a system class path that should be P2P
     * loaded even if they exist locally.
     *
     * @param p2pLocClsPathExcl List of P2P loaded packages. Package
     *      name supports '*' at the end like in package import clause.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setPeerClassLoadingLocalClassPathExclude(String... p2pLocClsPathExcl) {
        this.p2pLocClsPathExcl = p2pLocClsPathExcl;

        return this;
    }

    /**
     * Number of node metrics to keep in memory to calculate totals and averages.
     * If not provided (value is {@code 0}), then default value
     * {@link #DFLT_METRICS_HISTORY_SIZE} is used.
     *
     * @return Metrics history size.
     * @see #DFLT_METRICS_HISTORY_SIZE
     */
    public int getMetricsHistorySize() {
        return metricsHistSize;
    }

    /**
     * Sets number of metrics kept in history to compute totals and averages.
     * If not explicitly set, then default value is {@code 10,000}.
     *
     * @param metricsHistSize Number of metrics kept in history to use for
     *      metric totals and averages calculations.
     * @see #DFLT_METRICS_HISTORY_SIZE
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setMetricsHistorySize(int metricsHistSize) {
        this.metricsHistSize = metricsHistSize;

        return this;
    }

    /**
     * Gets job metrics update frequency in milliseconds.
     * <p>
     * Updating metrics too frequently may have negative performance impact.
     * <p>
     * The following values are accepted:
     * <ul>
     *     <li>{@code -1} job metrics are never updated.</li>
     *     <li>{@code 0} job metrics are updated on each job start and finish.</li>
     *     <li>Positive value defines the actual update frequency. If not provided, then default value
     *     {@link #DFLT_METRICS_UPDATE_FREQ} is used.</li>
     * </ul>
     * If not provided, then default value {@link #DFLT_METRICS_UPDATE_FREQ} is used.
     *
     * @return Job metrics update frequency in milliseconds.
     * @see #DFLT_METRICS_UPDATE_FREQ
     */
    public long getMetricsUpdateFrequency() {
        return metricsUpdateFreq;
    }

    /**
     * Sets job metrics update frequency in milliseconds.
     * <p>
     * If set to {@code -1} job metrics are never updated.
     * If set to {@code 0} job metrics are updated on each job start and finish.
     * Positive value defines the actual update frequency.
     * If not provided, then default value
     * {@link #DFLT_METRICS_UPDATE_FREQ} is used.
     *
     * @param metricsUpdateFreq Job metrics update frequency in milliseconds.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setMetricsUpdateFrequency(long metricsUpdateFreq) {
        this.metricsUpdateFreq = metricsUpdateFreq;

        return this;
    }

    /**
     * Elapsed time in milliseconds after which node metrics are considered expired.
     * If not provided, then default value
     * {@link #DFLT_METRICS_EXPIRE_TIME} is used.
     *
     * @return Metrics expire time.
     * @see #DFLT_METRICS_EXPIRE_TIME
     */
    public long getMetricsExpireTime() {
        return metricsExpTime;
    }

    /**
     * Sets time in milliseconds after which a certain metric value is considered expired.
     * If not set explicitly, then default value is {@code 600,000} milliseconds (10 minutes).
     *
     * @param metricsExpTime The metricsExpTime to set.
     * @see #DFLT_METRICS_EXPIRE_TIME
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setMetricsExpireTime(long metricsExpTime) {
        this.metricsExpTime = metricsExpTime;

        return this;
    }

    /**
     * Maximum timeout in milliseconds for network requests.
     * <p>
     * If not provided, then default value
     * {@link #DFLT_NETWORK_TIMEOUT} is used.
     *
     * @return Maximum timeout for network requests.
     * @see #DFLT_NETWORK_TIMEOUT
     */
    public long getNetworkTimeout() {
        return netTimeout;
    }

    /**
     * Maximum timeout in milliseconds for network requests.
     * <p>
     * If not provided (value is {@code 0}), then default value
     * {@link #DFLT_NETWORK_TIMEOUT} is used.
     *
     * @param netTimeout Maximum timeout for network requests.
     * @see #DFLT_NETWORK_TIMEOUT
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setNetworkTimeout(long netTimeout) {
        this.netTimeout = netTimeout;

        return this;
    }

    /**
     * Interval in milliseconds between message send retries.
     * <p>
     * If not provided, then default value
     * {@link #DFLT_SEND_RETRY_DELAY} is used.
     *
     * @return Interval between message send retries.
     * @see #getNetworkSendRetryCount()
     * @see #DFLT_SEND_RETRY_DELAY
     */
    public long getNetworkSendRetryDelay() {
        return sndRetryDelay;
    }

    /**
     * Sets interval in milliseconds between message send retries.
     * <p>
     * If not provided, then default value
     * {@link #DFLT_SEND_RETRY_DELAY} is used.
     *
     * @param sndRetryDelay Interval between message send retries.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setNetworkSendRetryDelay(long sndRetryDelay) {
        this.sndRetryDelay = sndRetryDelay;

        return this;
    }

    /**
     * Message send retries count.
     * <p>
     * If not provided, then default value
     * {@link #DFLT_SEND_RETRY_CNT} is used.
     *
     * @return Message send retries count.
     * @see #getNetworkSendRetryDelay()
     * @see #DFLT_SEND_RETRY_CNT
     */
    public int getNetworkSendRetryCount() {
        return sndRetryCnt;
    }

    /**
     * Sets message send retries count.
     * <p>
     * If not provided, then default value
     * {@link #DFLT_SEND_RETRY_CNT} is used.
     *
     * @param sndRetryCnt Message send retries count.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setNetworkSendRetryCount(int sndRetryCnt) {
        this.sndRetryCnt = sndRetryCnt;

        return this;
    }

    /**
     * Gets number of samples used to synchronize clocks between different nodes.
     * <p>
     * Clock synchronization is used for cache version assignment in {@code CLOCK} order mode.
     *
     * @return Number of samples for one synchronization round.
     */
    public int getClockSyncSamples() {
        return clockSyncSamples;
    }

    /**
     * Sets number of samples used for clock synchronization.
     *
     * @param clockSyncSamples Number of samples.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setClockSyncSamples(int clockSyncSamples) {
        this.clockSyncSamples = clockSyncSamples;

        return this;
    }

    /**
     * Gets frequency at which clock is synchronized between nodes, in milliseconds.
     * <p>
     * Clock synchronization is used for cache version assignment in {@code CLOCK} order mode.
     *
     * @return Clock synchronization frequency, in milliseconds.
     */
    public long getClockSyncFrequency() {
        return clockSyncFreq;
    }

    /**
     * Sets clock synchronization frequency in milliseconds.
     *
     * @param clockSyncFreq Clock synchronization frequency.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setClockSyncFrequency(long clockSyncFreq) {
        this.clockSyncFreq = clockSyncFreq;

        return this;
    }

    /**
     * Gets Max count of threads can be used at rebalancing.
     * Minimum is 1.
     * @return count.
     */
    public int getRebalanceThreadPoolSize() {
        return rebalanceThreadPoolSize;
    }

    /**
     * Sets Max count of threads can be used at rebalancing.
     *
     * Default is {@code 1} which has minimal impact on the operation of the grid.
     *
     * @param rebalanceThreadPoolSize Number of system threads that will be assigned for partition transfer during
     *      rebalancing.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setRebalanceThreadPoolSize(int rebalanceThreadPoolSize) {
        this.rebalanceThreadPoolSize = rebalanceThreadPoolSize;

        return this;
    }

    /**
     * Returns a collection of life-cycle beans. These beans will be automatically
     * notified of grid life-cycle events. Use life-cycle beans whenever you
     * want to perform certain logic before and after grid startup and stopping
     * routines.
     *
     * @return Collection of life-cycle beans.
     * @see LifecycleBean
     * @see LifecycleEventType
     */
    public LifecycleBean[] getLifecycleBeans() {
        return lifecycleBeans;
    }

    /**
     * Sets a collection of lifecycle beans. These beans will be automatically
     * notified of grid lifecycle events. Use lifecycle beans whenever you
     * want to perform certain logic before and after grid startup and stopping
     * routines.
     *
     * @param lifecycleBeans Collection of lifecycle beans.
     * @see LifecycleEventType
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setLifecycleBeans(LifecycleBean... lifecycleBeans) {
        this.lifecycleBeans = lifecycleBeans;

        return this;
    }

    /**
     * Sets SSL context factory that will be used for creating a secure socket  layer.
     *
     * @param sslCtxFactory Ssl context factory.
     * @see SslContextFactory
     */
    public IgniteConfiguration setSslContextFactory(Factory<SSLContext> sslCtxFactory) {
        this.sslCtxFactory = sslCtxFactory;

        return this;
    }

    /**
     * Returns SSL context factory that will be used for creating a secure socket layer.
     *
     * @return SSL connection factory.
     * @see SslContextFactory
     */
    public Factory<SSLContext> getSslContextFactory() {
        return sslCtxFactory;
    }

    /**
     * Should return fully configured event SPI implementation. If not provided,
     * {@link MemoryEventStorageSpi} will be used.
     *
     * @return Grid event SPI implementation or {@code null} to use default implementation.
     */
    public EventStorageSpi getEventStorageSpi() {
        return evtSpi;
    }

    /**
     * Sets fully configured instance of {@link EventStorageSpi}.
     *
     * @param evtSpi Fully configured instance of {@link EventStorageSpi}.
     * @see IgniteConfiguration#getEventStorageSpi()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setEventStorageSpi(EventStorageSpi evtSpi) {
        this.evtSpi = evtSpi;

        return this;
    }

    /**
     * Should return fully configured discovery SPI implementation. If not provided,
     * {@link TcpDiscoverySpi} will be used by default.
     *
     * @return Grid discovery SPI implementation or {@code null} to use default implementation.
     */
    public DiscoverySpi getDiscoverySpi() {
        return discoSpi;
    }

    /**
     * Sets fully configured instance of {@link DiscoverySpi}.
     *
     * @param discoSpi Fully configured instance of {@link DiscoverySpi}.
     * @see IgniteConfiguration#getDiscoverySpi()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setDiscoverySpi(DiscoverySpi discoSpi) {
        this.discoSpi = discoSpi;

        return this;
    }

    /**
     * Returns segmentation policy. Default is {@link #DFLT_SEG_PLC}.
     *
     * @return Segmentation policy.
     */
    public SegmentationPolicy getSegmentationPolicy() {
        return segPlc;
    }

    /**
     * Sets segmentation policy.
     *
     * @param segPlc Segmentation policy.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setSegmentationPolicy(SegmentationPolicy segPlc) {
        this.segPlc = segPlc;

        return this;
    }

    /**
     * Gets wait for segment on startup flag. Default is {@link #DFLT_WAIT_FOR_SEG_ON_START}.
     * <p>
     * Returns {@code true} if node should wait for correct segment on start.
     * If node detects that segment is incorrect on startup and this method
     * returns {@code true}, node waits until segment becomes correct.
     * If segment is incorrect on startup and this method returns {@code false},
     * exception is thrown.
     *
     * @return {@code True} to wait for segment on startup, {@code false} otherwise.
     */
    public boolean isWaitForSegmentOnStart() {
        return waitForSegOnStart;
    }

    /**
     * Sets wait for segment on start flag.
     *
     * @param waitForSegOnStart {@code True} to wait for segment on start.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setWaitForSegmentOnStart(boolean waitForSegOnStart) {
        this.waitForSegOnStart = waitForSegOnStart;

        return this;
    }

    /**
     * Gets all segmentation resolvers pass required flag.
     * <p>
     * Returns {@code true} if all segmentation resolvers should succeed
     * for node to be in correct segment.
     * Returns {@code false} if at least one segmentation resolver should succeed
     * for node to be in correct segment.
     * <p>
     * Default is {@link #DFLT_ALL_SEG_RESOLVERS_PASS_REQ}.
     *
     * @return {@code True} if all segmentation resolvers should succeed,
     *      {@code false} if only one is enough.
     */
    public boolean isAllSegmentationResolversPassRequired() {
        return allResolversPassReq;
    }

    /**
     * Sets all segmentation resolvers pass required flag.
     *
     * @param allResolversPassReq {@code True} if all segmentation resolvers should
     *      succeed for node to be in the correct segment.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setAllSegmentationResolversPassRequired(boolean allResolversPassReq) {
        this.allResolversPassReq = allResolversPassReq;

        return this;
    }

    /**
     * Gets segmentation resolve attempts. Each configured resolver will have
     * this attempts number to pass segmentation check prior to check failure.
     *
     * Default is {@link #DFLT_SEG_RESOLVE_ATTEMPTS}.
     *
     * @return Segmentation resolve attempts.
     */
    public int getSegmentationResolveAttempts() {
        return segResolveAttempts;
    }

    /**
     * Sets segmentation resolve attempts count.
     *
     * @param segResolveAttempts Segmentation resolve attempts.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setSegmentationResolveAttempts(int segResolveAttempts) {
        this.segResolveAttempts = segResolveAttempts;

        return this;
    }

    /**
     * Returns a collection of segmentation resolvers.
     * <p>
     * If array is {@code null} or empty, periodical and on-start network
     * segment checks do not happen.
     *
     * @return Segmentation resolvers.
     */
    public SegmentationResolver[] getSegmentationResolvers() {
        return segResolvers;
    }

    /**
     * Sets segmentation resolvers.
     *
     * @param segResolvers Segmentation resolvers.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setSegmentationResolvers(SegmentationResolver... segResolvers) {
        this.segResolvers = segResolvers;

        return this;
    }

    /**
     * Returns frequency of network segment check by discovery manager.
     * <p>
     * if 0, periodic segment check is disabled and segment is checked only
     * on topology changes (if segmentation resolvers are configured).
     * <p>
     * Default is {@link #DFLT_SEG_CHK_FREQ}.
     *
     * @return Segment check frequency.
     */
    public long getSegmentCheckFrequency() {
        return segChkFreq;
    }

    /**
     * Sets network segment check frequency.
     *
     * @param segChkFreq Segment check frequency.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setSegmentCheckFrequency(long segChkFreq) {
        this.segChkFreq = segChkFreq;

        return this;
    }

    /**
     * Should return fully configured SPI communication  implementation. If not provided,
     * {@link TcpCommunicationSpi} will be used by default.
     *
     * @return Grid communication SPI implementation or {@code null} to use default implementation.
     */
    public CommunicationSpi getCommunicationSpi() {
        return commSpi;
    }

    /**
     * Sets fully configured instance of {@link CommunicationSpi}.
     *
     * @param commSpi Fully configured instance of {@link CommunicationSpi}.
     * @see IgniteConfiguration#getCommunicationSpi()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setCommunicationSpi(CommunicationSpi commSpi) {
        this.commSpi = commSpi;

        return this;
    }

    /**
     * Should return fully configured collision SPI implementation. If not provided,
     * {@link NoopCollisionSpi} is used and jobs get activated immediately
     * on arrive to mapped node. This approach suits well for large amount of small
     * jobs (which is a wide-spread use case). User still can control the number
     * of concurrent jobs by setting maximum thread pool size defined by
     * IgniteConfiguration.getPublicThreadPoolSize() configuration property.
     *
     * @return Grid collision SPI implementation or {@code null} to use default implementation.
     */
    public CollisionSpi getCollisionSpi() {
        return colSpi;
    }

    /**
     * Sets fully configured instance of {@link CollisionSpi}.
     *
     * @param colSpi Fully configured instance of {@link CollisionSpi} or
     *      {@code null} if no SPI provided.
     * @see IgniteConfiguration#getCollisionSpi()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setCollisionSpi(CollisionSpi colSpi) {
        this.colSpi = colSpi;

        return this;
    }

    /**
     * Should return fully configured deployment SPI implementation. If not provided,
     * {@link LocalDeploymentSpi} will be used.
     *
     * @return Grid deployment SPI implementation or {@code null} to use default implementation.
     */
    public DeploymentSpi getDeploymentSpi() {
        return deploySpi;
    }

    /**
     * Sets fully configured instance of {@link DeploymentSpi}.
     *
     * @param deploySpi Fully configured instance of {@link DeploymentSpi}.
     * @see IgniteConfiguration#getDeploymentSpi()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setDeploymentSpi(DeploymentSpi deploySpi) {
        this.deploySpi = deploySpi;

        return this;
    }

    /**
     * Should return fully configured checkpoint SPI implementation. If not provided,
     * {@link NoopCheckpointSpi} will be used.
     *
     * @return Grid checkpoint SPI implementation or {@code null} to use default implementation.
     */
    public CheckpointSpi[] getCheckpointSpi() {
        return cpSpi;
    }

    /**
     * Sets fully configured instance of {@link CheckpointSpi}.
     *
     * @param cpSpi Fully configured instance of {@link CheckpointSpi}.
     * @see IgniteConfiguration#getCheckpointSpi()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setCheckpointSpi(CheckpointSpi... cpSpi) {
        this.cpSpi = cpSpi;

        return this;
    }

    /**
     * Should return fully configured failover SPI implementation. If not provided,
     * {@link AlwaysFailoverSpi} will be used.
     *
     * @return Grid failover SPI implementation or {@code null} to use default implementation.
     */
    public FailoverSpi[] getFailoverSpi() {
        return failSpi;
    }

    /**
     * Sets fully configured instance of {@link FailoverSpi}.
     *
     * @param failSpi Fully configured instance of {@link FailoverSpi} or
     *      {@code null} if no SPI provided.
     * @see IgniteConfiguration#getFailoverSpi()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setFailoverSpi(FailoverSpi... failSpi) {
        this.failSpi = failSpi;

        return this;
    }

    /**
     * Returns failure detection timeout used by {@link TcpDiscoverySpi} and {@link TcpCommunicationSpi}.
     * <p>
     * Default is {@link #DFLT_FAILURE_DETECTION_TIMEOUT}.
     *
     * @see #setFailureDetectionTimeout(long)
     * @return Failure detection timeout in milliseconds.
     */
    public Long getFailureDetectionTimeout() {
        return failureDetectionTimeout;
    }

    /**
     * Sets failure detection timeout to use in {@link TcpDiscoverySpi} and {@link TcpCommunicationSpi}.
     * <p>
     * Failure detection timeout is used to determine how long the communication or discovery SPIs should wait before
     * considering a remote connection failed.
     *
     * @param failureDetectionTimeout Failure detection timeout in milliseconds.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setFailureDetectionTimeout(long failureDetectionTimeout) {
        this.failureDetectionTimeout = failureDetectionTimeout;

        return this;
    }

    /**
     * Should return fully configured load balancing SPI implementation. If not provided,
     * {@link RoundRobinLoadBalancingSpi} will be used.
     *
     * @return Grid load balancing SPI implementation or {@code null} to use default implementation.
     */
    public LoadBalancingSpi[] getLoadBalancingSpi() {
        return loadBalancingSpi;
    }

    /**
     * This value is used to expire messages from waiting list whenever node
     * discovery discrepancies happen.
     * <p>
     * During startup, it is possible for some SPIs to have a small time window when
     * <tt>Node A</tt> has discovered <tt>Node B</tt>, but <tt>Node B</tt>
     * has not discovered <tt>Node A</tt> yet. Such time window is usually very small,
     * a matter of milliseconds, but certain JMS providers, for example, may be very slow
     * and hence have larger discovery delay window.
     * <p>
     * The default value of this property is {@code 60,000} specified by
     * {@link #DFLT_DISCOVERY_STARTUP_DELAY}. This should be good enough for vast
     * majority of configurations. However, if you do anticipate an even larger
     * delay, you should increase this value.
     *
     * @return Time in milliseconds for when nodes can be out-of-sync.
     */
    public long getDiscoveryStartupDelay() {
        return discoStartupDelay;
    }

    /**
     * Sets time in milliseconds after which a certain metric value is considered expired.
     * If not set explicitly, then default value is {@code 600,000} milliseconds (10 minutes).
     *
     * @param discoStartupDelay Time in milliseconds for when nodes
     *      can be out-of-sync during startup.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setDiscoveryStartupDelay(long discoStartupDelay) {
        this.discoStartupDelay = discoStartupDelay;

        return this;
    }

    /**
     * Sets fully configured instance of {@link LoadBalancingSpi}.
     *
     * @param loadBalancingSpi Fully configured instance of {@link LoadBalancingSpi} or
     *      {@code null} if no SPI provided.
     * @see IgniteConfiguration#getLoadBalancingSpi()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setLoadBalancingSpi(LoadBalancingSpi... loadBalancingSpi) {
        this.loadBalancingSpi = loadBalancingSpi;

        return this;
    }

    /**
     * Sets fully configured instances of {@link SwapSpaceSpi}.
     *
     * @param swapSpaceSpi Fully configured instances of {@link SwapSpaceSpi} or
     *      <tt>null</tt> if no SPI provided.
     * @see IgniteConfiguration#getSwapSpaceSpi()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setSwapSpaceSpi(SwapSpaceSpi swapSpaceSpi) {
        this.swapSpaceSpi = swapSpaceSpi;

        return this;
    }

    /**
     * Should return fully configured swap space SPI implementation. If not provided,
     * {@link FileSwapSpaceSpi} will be used.
     * <p>
     * Note that user can provide one or multiple instances of this SPI (and select later which one
     * is used in a particular context).
     *
     * @return Grid swap space SPI implementation or <tt>null</tt> to use default implementation.
     */
    public SwapSpaceSpi getSwapSpaceSpi() {
        return swapSpaceSpi;
    }

    /**
     * Sets fully configured instances of {@link IndexingSpi}.
     *
     * @param indexingSpi Fully configured instance of {@link IndexingSpi}.
     * @see IgniteConfiguration#getIndexingSpi()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setIndexingSpi(IndexingSpi indexingSpi) {
        this.indexingSpi = indexingSpi;

        return this;
    }

    /**
     * Should return fully configured indexing SPI implementations.
     *
     * @return Indexing SPI implementation.
     */
    public IndexingSpi getIndexingSpi() {
        return indexingSpi;
    }

    /**
     * Gets address resolver for addresses mapping determination.
     *
     * @return Address resolver.
     */
    public AddressResolver getAddressResolver() {
        return addrRslvr;
    }

    /**
     * Sets address resolver for addresses mapping determination.
     *
     * @param addrRslvr Address resolver.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setAddressResolver(AddressResolver addrRslvr) {
        this.addrRslvr = addrRslvr;

        return this;
    }

    /**
     * Sets task classes and resources sharing mode.
     *
     * @param deployMode Task classes and resources sharing mode.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setDeploymentMode(DeploymentMode deployMode) {
        this.deployMode = deployMode;

        return this;
    }

    /**
     * Gets deployment mode for deploying tasks and other classes on this node.
     * Refer to {@link DeploymentMode} documentation for more information.
     *
     * @return Deployment mode.
     */
    public DeploymentMode getDeploymentMode() {
        return deployMode;
    }

    /**
     * Sets size of missed resources cache. Set 0 to avoid
     * missed resources caching.
     *
     * @param p2pMissedCacheSize Size of missed resources cache.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setPeerClassLoadingMissedResourcesCacheSize(int p2pMissedCacheSize) {
        this.p2pMissedCacheSize = p2pMissedCacheSize;

        return this;
    }

    /**
     * Returns missed resources cache size. If size greater than {@code 0}, missed
     * resources will be cached and next resource request ignored. If size is {@code 0},
     * then request for the resource will be sent to the remote node every time this
     * resource is requested.
     *
     * @return Missed resources cache size.
     */
    public int getPeerClassLoadingMissedResourcesCacheSize() {
        return p2pMissedCacheSize;
    }

    /**
     * Gets configuration (descriptors) for all caches.
     *
     * @return Array of fully initialized cache descriptors.
     */
    public CacheConfiguration[] getCacheConfiguration() {
        return cacheCfg;
    }

    /**
     * Sets cache configurations.
     *
     * @param cacheCfg Cache configurations.
     */
    @SuppressWarnings({"ZeroLengthArrayAllocation"})
    public IgniteConfiguration setCacheConfiguration(CacheConfiguration... cacheCfg) {
        this.cacheCfg = cacheCfg == null ? new CacheConfiguration[0] : cacheCfg;

        return this;
    }

    /**
     * Gets client mode flag. Client node cannot hold data in the caches. It's recommended to use
     * {@link DiscoverySpi} in client mode if this property is {@code true}.
     *
     * @return Client mode flag.
     * @see TcpDiscoverySpi#setForceServerMode(boolean)
     */
    public Boolean isClientMode() {
        return clientMode;
    }

    /**
     * Sets client mode flag.
     *
     * @param clientMode Client mode flag.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setClientMode(boolean clientMode) {
        this.clientMode = clientMode;

        return this;
    }

    /**
     * Gets cache key configuration.
     *
     * @return Cache key configuration.
     */
    public CacheKeyConfiguration[] getCacheKeyConfiguration() {
        return cacheKeyCfg;
    }

    /**
     * Sets cache key configuration.
     * Cache key configuration defines
     *
     * @param cacheKeyCfg Cache key configuration.
     */
    public IgniteConfiguration setCacheKeyConfiguration(CacheKeyConfiguration... cacheKeyCfg) {
        this.cacheKeyCfg = cacheKeyCfg;

        return this;
    }

    /**
     * Gets configuration for Ignite Binary objects.
     *
     * @return Binary configuration object.
     */
    public BinaryConfiguration getBinaryConfiguration() {
        return binaryCfg;
    }

    /**
     * Sets configuration for Ignite Binary objects.
     *
     * @param binaryCfg Binary configuration object.
     */
    public IgniteConfiguration setBinaryConfiguration(BinaryConfiguration binaryCfg) {
        this.binaryCfg = binaryCfg;

        return this;
    }

    /**
     * Gets flag indicating whether cache sanity check is enabled. If enabled, then Ignite
     * will perform the following checks and throw an exception if check fails:
     * <ul>
     *     <li>Cache entry is not externally locked with {@code lock(...)} or {@code lockAsync(...)}
     *     methods when entry is enlisted to transaction.</li>
     *     <li>Each entry in affinity group-lock transaction has the same affinity key as was specified on
     *     affinity transaction start.</li>
     *     <li>Each entry in partition group-lock transaction belongs to the same partition as was specified
     *     on partition transaction start.</li>
     * </ul>
     * <p>
     * These checks are not required for cache operation, but help to find subtle bugs. Disabling of this checks
     * usually yields a noticeable performance gain.
     * <p>
     * If not provided, default value is {@link #DFLT_CACHE_SANITY_CHECK_ENABLED}.
     *
     * @return {@code True} if group lock sanity check is enabled.
     */
    public boolean isCacheSanityCheckEnabled() {
        return cacheSanityCheckEnabled;
    }

    /**
     * Sets cache sanity check flag.
     *
     * @param cacheSanityCheckEnabled {@code True} if cache sanity check is enabled.
     * @see #isCacheSanityCheckEnabled()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setCacheSanityCheckEnabled(boolean cacheSanityCheckEnabled) {
        this.cacheSanityCheckEnabled = cacheSanityCheckEnabled;

        return this;
    }

    /**
     * Gets array of event types, which will be recorded.
     * <p>
     * Note that by default all events in Ignite are disabled. Ignite can and often does generate thousands
     * events per seconds under the load and therefore it creates a significant additional load on the system.
     * If these events are not needed by the application this load is unnecessary and leads to significant
     * performance degradation. So it is <b>highly recommended</b> to enable only those events that your
     * application logic requires. Note that certain events are required for Ignite's internal operations
     * and such events will still be generated but not stored by event storage SPI if they are disabled
     * in Ignite configuration.
     *
     * @return Include event types.
     */
    public int[] getIncludeEventTypes() {
        return inclEvtTypes;
    }

    /**
     * Sets array of event types, which will be recorded by {@link GridEventStorageManager#record(Event)}.
     * Note, that either the include event types or the exclude event types can be established.
     *
     * @param inclEvtTypes Include event types.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setIncludeEventTypes(int... inclEvtTypes) {
        this.inclEvtTypes = inclEvtTypes;

        return this;
    }

    /**
     * Sets system-wide local address or host for all Ignite components to bind to. If provided it will
     * override all default local bind settings within Ignite or any of its SPIs.
     *
     * @param locHost Local IP address or host to bind to.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setLocalHost(String locHost) {
        this.locHost = locHost;

        return this;
    }

    /**
     * Gets system-wide local address or host for all Ignite components to bind to. If provided it will
     * override all default local bind settings within Ignite or any of its SPIs.
     * <p>
     * If {@code null} then Ignite tries to use local wildcard address. That means that
     * all services will be available on all network interfaces of the host machine.
     * <p>
     * It is strongly recommended to set this parameter for all production environments.
     * <p>
     * If not provided, default is {@code null}.
     *
     * @return Local address or host to bind to.
     */
    public String getLocalHost() {
        return locHost;
    }

    /**
     * Gets base UPD port number for grid time server. Time server will be started on one of free ports in range
     * {@code [timeServerPortBase, timeServerPortBase + timeServerPortRange - 1]}.
     * <p>
     * Time server provides clock synchronization between nodes.
     *
     * @return Time
     */
    public int getTimeServerPortBase() {
        return timeSrvPortBase;
    }

    /**
     * Sets time server port base.
     *
     * @param timeSrvPortBase Time server port base.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setTimeServerPortBase(int timeSrvPortBase) {
        this.timeSrvPortBase = timeSrvPortBase;

        return this;
    }

    /**
     * Defines port range to try for time server start.
     *
     * If port range value is <tt>0</tt>, then implementation will try bind only to the port provided by
     * {@link #setTimeServerPortBase(int)} method and fail if binding to this port did not succeed.
     *
     * @return Number of ports to try before server initialization fails.
     */
    public int getTimeServerPortRange() {
        return timeSrvPortRange;
    }

    /**
     * Sets time server port range.
     *
     * @param timeSrvPortRange Time server port range.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setTimeServerPortRange(int timeSrvPortRange) {
        this.timeSrvPortRange = timeSrvPortRange;

        return this;
    }

    /**
     * Gets array of system or environment properties to include into node attributes.
     * If this array is {@code null}, which is default, then all system and environment
     * properties will be included. If this array is empty, then none will be included.
     * Otherwise, for every name provided, first a system property will be looked up,
     * and then, if it is not found, environment property will be looked up.
     *
     * @return Array of system or environment properties to include into node attributes.
     */
    public String[] getIncludeProperties() {
        return includeProps;
    }

    /**
     * Sets array of system or environment property names to include into node attributes.
     * See {@link #getIncludeProperties()} for more info.
     *
     * @param includeProps Array of system or environment property names to include into node attributes.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setIncludeProperties(String... includeProps) {
        this.includeProps = includeProps;

        return this;
    }

    /**
     * Gets frequency of metrics log print out.
     * <p>
     * If {@code 0}, metrics print out is disabled.
     * <p>
     * If not provided, then default value {@link #DFLT_METRICS_LOG_FREQ} is used.
     *
     * @return Frequency of metrics log print out.
     */
    public long getMetricsLogFrequency() {
        return metricsLogFreq;
    }

    /**
     * Sets frequency of metrics log print out.
     * <p>
     * If {@code 0}, metrics print out is disabled.
     * <p>
     * If not provided, then default value {@link #DFLT_METRICS_LOG_FREQ} is used.
     *
     * @param metricsLogFreq Frequency of metrics log print out.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setMetricsLogFrequency(long metricsLogFreq) {
        this.metricsLogFreq = metricsLogFreq;

        return this;
    }

    /**
     * Gets IGFS (Ignite In-Memory File System) configurations.
     *
     * @return IGFS configurations.
     */
    public FileSystemConfiguration[] getFileSystemConfiguration() {
        return igfsCfg;
    }

    /**
     * Sets IGFS (Ignite In-Memory File System) configurations.
     *
     * @param igfsCfg IGFS configurations.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setFileSystemConfiguration(FileSystemConfiguration... igfsCfg) {
        this.igfsCfg = igfsCfg;

        return this;
    }

    /**
     * Gets hadoop configuration.
     *
     * @return Hadoop configuration.
     */
    public HadoopConfiguration getHadoopConfiguration() {
        return hadoopCfg;
    }

    /**
     * Sets hadoop configuration.
     *
     * @param hadoopCfg Hadoop configuration.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setHadoopConfiguration(HadoopConfiguration hadoopCfg) {
        this.hadoopCfg = hadoopCfg;

        return this;
    }

    /**
     * @return Connector configuration.
     */
    public ConnectorConfiguration getConnectorConfiguration() {
        return connectorCfg;
    }

    /**
     * @param connectorCfg Connector configuration.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setConnectorConfiguration(ConnectorConfiguration connectorCfg) {
        this.connectorCfg = connectorCfg;

        return this;
    }

    /**
     * Gets configuration for ODBC.
     *
     * @return ODBC configuration.
     */
    public OdbcConfiguration getOdbcConfiguration() {
        return odbcCfg;
    }

    /**
     * Sets configuration for ODBC.
     *
     * @param odbcCfg ODBC configuration.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setOdbcConfiguration(OdbcConfiguration odbcCfg) {
        this.odbcCfg = odbcCfg;

        return this;
    }

    /**
     * Gets configurations for services to be deployed on the grid.
     *
     * @return Configurations for services to be deployed on the grid.
     */
    public ServiceConfiguration[] getServiceConfiguration() {
        return svcCfgs;
    }

    /**
     * Sets configurations for services to be deployed on the grid.
     *
     * @param svcCfgs Configurations for services to be deployed on the grid.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setServiceConfiguration(ServiceConfiguration... svcCfgs) {
        this.svcCfgs = svcCfgs;

        return this;
    }

    /**
     * Gets map of pre-configured local event listeners.
     * Each listener is mapped to array of event types.
     *
     * @return Pre-configured event listeners map.
     * @see EventType
     */
    public Map<IgnitePredicate<? extends Event>, int[]> getLocalEventListeners() {
        return lsnrs;
    }

    /**
     * Sets map of pre-configured local event listeners.
     * Each listener is mapped to array of event types.
     *
     * @param lsnrs Pre-configured event listeners map.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setLocalEventListeners(Map<IgnitePredicate<? extends Event>, int[]> lsnrs) {
        this.lsnrs = lsnrs;

        return this;
    }

    /**
     * Gets grid warmup closure. This closure will be executed before actual grid instance start. Configuration of
     * a starting instance will be passed to the closure so it can decide what operations to warm up.
     *
     * @return Warmup closure to execute.
     */
    public IgniteInClosure<IgniteConfiguration> getWarmupClosure() {
        return warmupClos;
    }

    /**
     * Sets warmup closure to execute before grid startup.
     *
     * @param warmupClos Warmup closure to execute.
     * @see #getWarmupClosure()
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setWarmupClosure(IgniteInClosure<IgniteConfiguration> warmupClos) {
        this.warmupClos = warmupClos;

        return this;
    }

    /**
     * Gets transactions configuration.
     *
     * @return Transactions configuration.
     */
    public TransactionConfiguration getTransactionConfiguration() {
        return txCfg;
    }

    /**
     * Sets transactions configuration.
     *
     * @param txCfg Transactions configuration.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setTransactionConfiguration(TransactionConfiguration txCfg) {
        this.txCfg = txCfg;

        return this;
    }

    /**
     * Gets plugin configurations.
     *
     * @return Plugin configurations.
     * @see PluginProvider
     */
    public PluginConfiguration[] getPluginConfigurations() {
        return pluginCfgs;
    }

    /**
     * Sets plugin configurations.
     *
     * @param pluginCfgs Plugin configurations.
     * @return {@code this} for chaining.
     * @see PluginProvider
     */
    public IgniteConfiguration setPluginConfigurations(PluginConfiguration... pluginCfgs) {
        this.pluginCfgs = pluginCfgs;

        return this;
    }

    /**
     * @return Atomic data structures configuration.
     */
    public AtomicConfiguration getAtomicConfiguration() {
        return atomicCfg;
    }

    /**
     * @param atomicCfg Atomic data structures configuration.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setAtomicConfiguration(AtomicConfiguration atomicCfg) {
        this.atomicCfg = atomicCfg;

        return this;
    }

    /**
     * Sets loader which will be used for instantiating execution context ({@link EntryProcessor EntryProcessors},
     * {@link CacheEntryListener CacheEntryListeners}, {@link CacheLoader CacheLoaders} and
     * {@link ExpiryPolicy ExpiryPolicys}).
     *
     * @param classLdr Class loader.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setClassLoader(ClassLoader classLdr) {
        this.classLdr = classLdr;

        return this;
    }

    /**
     * @return User's class loader.
     */
    public ClassLoader getClassLoader() {
        return classLdr;
    }

    /**
     * Gets cache store session listener factories.
     *
     * @return Cache store session listener factories.
     * @see CacheStoreSessionListener
     */
    public Factory<CacheStoreSessionListener>[] getCacheStoreSessionListenerFactories() {
        return storeSesLsnrs;
    }

    /**
     * Cache store session listener factories.
     * <p>
     * These are global store session listeners, so they are applied to
     * all caches. If you need to override listeners for a
     * particular cache, use {@link CacheConfiguration#setCacheStoreSessionListenerFactories(Factory[])}
     * configuration property.
     *
     * @param storeSesLsnrs Cache store session listener factories.
     * @return {@code this} for chaining.
     * @see CacheStoreSessionListener
     */
    public IgniteConfiguration setCacheStoreSessionListenerFactories(
        Factory<CacheStoreSessionListener>... storeSesLsnrs) {
        this.storeSesLsnrs = storeSesLsnrs;

        return this;
    }

    /**
     * Gets platform configuration.
     *
     * @return Platform configuration.
     */
    public PlatformConfiguration getPlatformConfiguration() {
        return platformCfg;
    }

    /**
     * Sets platform configuration.
     *
     * @param platformCfg Platform configuration.
     * @return  {@code this} for chaining.
     */
    public IgniteConfiguration setPlatformConfiguration(PlatformConfiguration platformCfg) {
        this.platformCfg = platformCfg;

        return this;
    }

    /**
     * Whether or not late affinity assignment mode should be used.
     * <p>
     * On each topology change, for each started cache partition-to-node mapping is
     * calculated using {@link AffinityFunction} configured for cache. When late
     * affinity assignment mode is disabled then new affinity mapping is applied immediately.
     * <p>
     * With late affinity assignment mode if primary node was changed for some partition, but data for this
     * partition is not rebalanced yet on this node, then current primary is not changed and new primary is temporary
     * assigned as backup. This nodes becomes primary only when rebalancing for all assigned primary partitions is
     * finished. This mode can show better performance for cache operations, since when cache primary node
     * executes some operation and data is not rebalanced yet, then it sends additional message to force rebalancing
     * from other nodes.
     * <p>
     * Note, that {@link Affinity} interface provides assignment information taking into account late assignment,
     * so while rebalancing for new primary nodes is not finished it can return assignment which differs
     * from assignment calculated by {@link AffinityFunction#assignPartitions}.
     * <p>
     * This property should have the same value for all nodes in cluster.
     * <p>
     * If not provided, default value is {@link #DFLT_LATE_AFF_ASSIGNMENT}.
     *
     * @return Late affinity assignment flag.
     * @see AffinityFunction
     */
    public boolean isLateAffinityAssignment() {
        return lateAffAssignment;
    }

    /**
     * Sets late affinity assignment flag.
     *
     * @param lateAffAssignment Late affinity assignment flag.
     * @return {@code this} for chaining.
     */
    public IgniteConfiguration setLateAffinityAssignment(boolean lateAffAssignment) {
        this.lateAffAssignment = lateAffAssignment;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteConfiguration.class, this);
    }
}
