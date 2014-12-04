/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.configuration;

import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.lifecycle.*;
import org.apache.ignite.managed.*;
import org.apache.ignite.plugin.*;
import org.gridgain.client.ssl.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.dotnet.*;
import org.gridgain.grid.dr.hub.receiver.*;
import org.gridgain.grid.dr.hub.sender.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.marshaller.jdk.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.portables.*;
import org.gridgain.grid.security.*;
import org.gridgain.grid.segmentation.*;
import org.gridgain.grid.spi.authentication.*;
import org.gridgain.grid.spi.authentication.noop.*;
import org.gridgain.grid.spi.checkpoint.*;
import org.gridgain.grid.spi.checkpoint.noop.*;
import org.gridgain.grid.spi.collision.*;
import org.gridgain.grid.spi.collision.noop.*;
import org.gridgain.grid.spi.communication.*;
import org.gridgain.grid.spi.communication.tcp.*;
import org.gridgain.grid.spi.deployment.*;
import org.gridgain.grid.spi.deployment.local.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.eventstorage.*;
import org.gridgain.grid.spi.eventstorage.memory.*;
import org.gridgain.grid.spi.failover.*;
import org.gridgain.grid.spi.failover.always.*;
import org.gridgain.grid.spi.indexing.*;
import org.gridgain.grid.spi.loadbalancing.*;
import org.gridgain.grid.spi.loadbalancing.roundrobin.*;
import org.gridgain.grid.spi.securesession.*;
import org.gridgain.grid.spi.securesession.noop.*;
import org.gridgain.grid.spi.swapspace.*;
import org.gridgain.grid.spi.swapspace.file.*;
import org.gridgain.grid.streamer.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.management.*;
import java.lang.management.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.segmentation.GridSegmentationPolicy.*;

/**
 * This class defines grid runtime configuration. This configuration is passed to
 * {@link org.apache.ignite.Ignition#start(IgniteConfiguration)} method. It defines all configuration
 * parameters required to start a grid instance. Usually, a special
 * class called "loader" will create an instance of this interface and apply
 * {@link org.apache.ignite.Ignition#start(IgniteConfiguration)} method to initialize GridGain instance.
 * <p>
 * Note that you should only set values that differ from defaults, as grid
 * will automatically pick default values for all values that are not set.
 * <p>
 * For more information about grid configuration and startup refer to {@link org.apache.ignite.Ignition}
 * documentation.
 */
public class IgniteConfiguration {
    /** Courtesy notice log category. */
    public static final String COURTESY_LOGGER_NAME = "org.gridgain.grid.CourtesyConfigNotice";

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

    /** Default deployment mode (value is {@link org.gridgain.grid.GridDeploymentMode#SHARED}). */
    public static final GridDeploymentMode DFLT_DEPLOYMENT_MODE = GridDeploymentMode.SHARED;

    /** Default cache size for missed resources. */
    public static final int DFLT_P2P_MISSED_RESOURCES_CACHE_SIZE = 100;

    /** Default SMTP port. */
    public static final int DFLT_SMTP_PORT = 25;

    /** Default SSL enabled flag. */
    public static final boolean DFLT_SMTP_SSL = false;

    /** Default STARTTLS enabled flag. */
    public static final boolean DFLT_SMTP_STARTTLS = false;

    /** Default FROM email address. */
    public static final String DFLT_SMTP_FROM_EMAIL = "info@gridgain.com";

    /** Default time server port base. */
    public static final int DFLT_TIME_SERVER_PORT_BASE = 31100;

    /** Default time server port range. */
    public static final int DFLT_TIME_SERVER_PORT_RANGE = 100;

    /** Default core size of public thread pool. */
    public static final int DFLT_PUBLIC_CORE_THREAD_CNT = Math.max(8, Runtime.getRuntime().availableProcessors()) * 2;

    /** Default max size of public thread pool. */
    public static final int DFLT_PUBLIC_MAX_THREAD_CNT = DFLT_PUBLIC_CORE_THREAD_CNT;

    /** Default keep alive time for public thread pool. */
    public static final long DFLT_PUBLIC_KEEP_ALIVE_TIME = 0;

    /** Default max queue capacity of public thread pool. */
    public static final int DFLT_PUBLIC_THREADPOOL_QUEUE_CAP = Integer.MAX_VALUE;

    /** Default size of system thread pool. */
    public static final int DFLT_SYSTEM_CORE_THREAD_CNT = DFLT_PUBLIC_CORE_THREAD_CNT;

    /** Default max size of system thread pool. */
    public static final int DFLT_SYSTEM_MAX_THREAD_CNT = DFLT_PUBLIC_CORE_THREAD_CNT;

    /** Default keep alive time for system thread pool. */
    public static final long DFLT_SYSTEM_KEEP_ALIVE_TIME = 0;

    /** Default max queue capacity of system thread pool. */
    public static final int DFLT_SYSTEM_THREADPOOL_QUEUE_CAP = Integer.MAX_VALUE;

    /** Default size of peer class loading thread pool. */
    public static final int DFLT_P2P_THREAD_CNT = 2;

    /** Default size of management thread pool. */
    public static final int DFLT_MGMT_THREAD_CNT = 4;

    /** Default size of REST thread pool. */
    public static final int DFLT_REST_CORE_THREAD_CNT = DFLT_PUBLIC_CORE_THREAD_CNT;

    /** Default max size of REST thread pool. */
    public static final int DFLT_REST_MAX_THREAD_CNT = DFLT_PUBLIC_CORE_THREAD_CNT;

    /** Default keep alive time for REST thread pool. */
    public static final long DFLT_REST_KEEP_ALIVE_TIME = 0;

    /** Default max queue capacity of REST thread pool. */
    public static final int DFLT_REST_THREADPOOL_QUEUE_CAP = Integer.MAX_VALUE;

    /** Default segmentation policy. */
    public static final GridSegmentationPolicy DFLT_SEG_PLC = STOP;

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

    /** Default TCP_NODELAY flag. */
    public static final boolean DFLT_TCP_NODELAY = true;

    /** Default TCP direct buffer flag. */
    public static final boolean DFLT_REST_TCP_DIRECT_BUF = false;

    /** Default REST idle timeout. */
    public static final int DFLT_REST_IDLE_TIMEOUT = 7000;

    /** Default rest port range. */
    public static final int DFLT_REST_PORT_RANGE = 100;

    /** Default marshal local jobs flag. */
    public static final boolean DFLT_MARSHAL_LOCAL_JOBS = false;

    /** Default value for cache sanity check enabled flag. */
    public static final boolean DFLT_CACHE_SANITY_CHECK_ENABLED = true;

    /** Optional grid name. */
    private String gridName;

    /** User attributes. */
    private Map<String, ?> userAttrs;

    /** Logger. */
    private GridLogger log;

    /** Executor service. */
    private ExecutorService execSvc;

    /** Executor service. */
    private ExecutorService sysSvc;

    /** Management executor service. */
    private ExecutorService mgmtSvc;

    /** GGFS executor service. */
    private ExecutorService ggfsSvc;

    /** REST requests executor service. */
    private ExecutorService restExecSvc;

    /** Peer class loading executor service shutdown flag. */
    private boolean p2pSvcShutdown = true;

    /** Executor service shutdown flag. */
    private boolean execSvcShutdown = true;

    /** System executor service shutdown flag. */
    private boolean sysSvcShutdown = true;

    /** Management executor service shutdown flag. */
    private boolean mgmtSvcShutdown = true;

    /** GGFS executor service shutdown flag. */
    private boolean ggfsSvcShutdown = true;

    /** REST executor service shutdown flag. */
    private boolean restSvcShutdown = true;

    /** Lifecycle email notification. */
    private boolean lifeCycleEmailNtf = true;

    /** Executor service. */
    private ExecutorService p2pSvc;

    /** Gridgain installation folder. */
    private String ggHome;

    /** Gridgain work folder. */
    private String ggWork;

    /** MBean server. */
    private MBeanServer mbeanSrv;

    /** Local node ID. */
    private UUID nodeId;

    /** Marshaller. */
    private GridMarshaller marsh;

    /** Marshal local jobs. */
    private boolean marshLocJobs = DFLT_MARSHAL_LOCAL_JOBS;

    /** Daemon flag. */
    private boolean daemon;

    /** Jetty XML configuration path. */
    private String jettyPath;

    /** {@code REST} flag. */
    private boolean restEnabled = true;

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
    private GridDiscoverySpi discoSpi;

    /** Segmentation policy. */
    private GridSegmentationPolicy segPlc = DFLT_SEG_PLC;

    /** Segmentation resolvers. */
    private GridSegmentationResolver[] segResolvers;

    /** Segmentation resolve attempts count. */
    private int segResolveAttempts = DFLT_SEG_RESOLVE_ATTEMPTS;

    /** Wait for segment on startup flag. */
    private boolean waitForSegOnStart = DFLT_WAIT_FOR_SEG_ON_START;

    /** All segmentation resolvers pass required flag. */
    private boolean allResolversPassReq = DFLT_ALL_SEG_RESOLVERS_PASS_REQ;

    /** Segment check frequency. */
    private long segChkFreq = DFLT_SEG_CHK_FREQ;

    /** Communication SPI. */
    private GridCommunicationSpi commSpi;

    /** Event storage SPI. */
    private GridEventStorageSpi evtSpi;

    /** Collision SPI. */
    private GridCollisionSpi colSpi;

    /** Authentication SPI. */
    private GridAuthenticationSpi authSpi;

    /** Secure session SPI. */
    private GridSecureSessionSpi sesSpi;

    /** Deployment SPI. */
    private GridDeploymentSpi deploySpi;

    /** Checkpoint SPI. */
    private GridCheckpointSpi[] cpSpi;

    /** Failover SPI. */
    private GridFailoverSpi[] failSpi;

    /** Load balancing SPI. */
    private GridLoadBalancingSpi[] loadBalancingSpi;

    /** Checkpoint SPI. */
    private GridSwapSpaceSpi swapSpaceSpi;

    /** Indexing SPI. */
    private GridIndexingSpi[] indexingSpi;

    /** Address resolver. */
    private GridAddressResolver addrRslvr;

    /** Cache configurations. */
    private GridCacheConfiguration[] cacheCfg;

    /** Transactions configuration. */
    private GridTransactionsConfiguration txCfg = new GridTransactionsConfiguration();

    /** Configuration for .Net nodes. */
    private GridDotNetConfiguration dotNetCfg;

    /** */
    private Collection<? extends PluginConfiguration> pluginCfgs;

    /** Flag indicating whether cache sanity check is enabled. */
    private boolean cacheSanityCheckEnabled = DFLT_CACHE_SANITY_CHECK_ENABLED;

    /** Discovery startup delay. */
    private long discoStartupDelay = DFLT_DISCOVERY_STARTUP_DELAY;

    /** Tasks classes sharing mode. */
    private GridDeploymentMode deployMode = DFLT_DEPLOYMENT_MODE;

    /** Cache size of missed resources. */
    private int p2pMissedCacheSize = DFLT_P2P_MISSED_RESOURCES_CACHE_SIZE;

    /** */
    private String smtpHost;

    /** */
    private int smtpPort = DFLT_SMTP_PORT;

    /** */
    private String smtpUsername;

    /** */
    private String smtpPwd;

    /** */
    private String[] adminEmails;

    /** */
    private String smtpFromEmail = DFLT_SMTP_FROM_EMAIL;

    /** */
    private boolean smtpSsl = DFLT_SMTP_SSL;

    /** */
    private boolean smtpStartTls = DFLT_SMTP_STARTTLS;

    /** Local host. */
    private String locHost;

    /** Base port number for time server. */
    private int timeSrvPortBase = DFLT_TIME_SERVER_PORT_BASE;

    /** Port number range for time server. */
    private int timeSrvPortRange = DFLT_TIME_SERVER_PORT_RANGE;

    /** REST secret key. */
    private String restSecretKey;

    /** Property names to include into node attributes. */
    private String[] includeProps;

    /** License custom URL. */
    private String licUrl;

    /** Frequency of metrics log print out. */
    @SuppressWarnings("RedundantFieldInitialization")
    private long metricsLogFreq = DFLT_METRICS_LOG_FREQ;

    /** Local event listeners. */
    private Map<IgnitePredicate<? extends GridEvent>, int[]> lsnrs;

    /** TCP host. */
    private String restTcpHost;

    /** TCP port. */
    private int restTcpPort = DFLT_TCP_PORT;

    /** TCP no delay flag. */
    private boolean restTcpNoDelay = DFLT_TCP_NODELAY;

    /** REST TCP direct buffer flag. */
    private boolean restTcpDirectBuf = DFLT_REST_TCP_DIRECT_BUF;

    /** REST TCP send buffer size. */
    private int restTcpSndBufSize;

    /** REST TCP receive buffer size. */
    private int restTcpRcvBufSize;

    /** REST TCP send queue limit. */
    private int restTcpSndQueueLimit;

    /** REST TCP selector count. */
    private int restTcpSelectorCnt = Math.min(4, Runtime.getRuntime().availableProcessors());

    /** Idle timeout. */
    private long restIdleTimeout = DFLT_REST_IDLE_TIMEOUT;

    /** SSL enable flag, default is disabled. */
    private boolean restTcpSslEnabled;

    /** SSL need client auth flag. */
    private boolean restTcpSslClientAuth;

    /** SSL context factory for rest binary server. */
    private GridSslContextFactory restTcpSslCtxFactory;

    /** Port range */
    private int restPortRange = DFLT_REST_PORT_RANGE;

    /** Folders accessible by REST. */
    private String[] restAccessibleFolders;

    /** GGFS configuration. */
    private GridGgfsConfiguration[] ggfsCfg;

    /** Client message interceptor. */
    private GridClientMessageInterceptor clientMsgInterceptor;

    /** Streamer configuration. */
    private GridStreamerConfiguration[] streamerCfg;

    /** Data center receiver hub configuration. */
    private GridDrReceiverHubConfiguration drRcvHubCfg;

    /** Data center sender hub configuration. */
    private GridDrSenderHubConfiguration drSndHubCfg;

    /** Data center ID. */
    private byte dataCenterId;

    /** Security credentials. */
    private GridSecurityCredentialsProvider securityCred;

    /** Service configuration. */
    private GridServiceConfiguration[] svcCfgs;

    /** Hadoop configuration. */
    private GridHadoopConfiguration hadoopCfg;

    /** Client access configuration. */
    private GridClientConnectionConfiguration clientCfg;

    /** Portable configuration. */
    private GridPortableConfiguration portableCfg;

    /** Warmup closure. Will be invoked before actual grid start. */
    private IgniteInClosure<IgniteConfiguration> warmupClos;

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
    @SuppressWarnings("deprecation")
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
        authSpi = cfg.getAuthenticationSpi();
        sesSpi = cfg.getSecureSessionSpi();
        loadBalancingSpi = cfg.getLoadBalancingSpi();
        swapSpaceSpi = cfg.getSwapSpaceSpi();
        indexingSpi = cfg.getIndexingSpi();

        /*
         * Order alphabetically for maintenance purposes.
         */
        addrRslvr = cfg.getAddressResolver();
        adminEmails = cfg.getAdminEmails();
        allResolversPassReq = cfg.isAllSegmentationResolversPassRequired();
        daemon = cfg.isDaemon();
        cacheCfg = cfg.getCacheConfiguration();
        cacheSanityCheckEnabled = cfg.isCacheSanityCheckEnabled();
        clientCfg = cfg.getClientConnectionConfiguration();
        clientMsgInterceptor = cfg.getClientMessageInterceptor();
        clockSyncFreq = cfg.getClockSyncFrequency();
        clockSyncSamples = cfg.getClockSyncSamples();
        dataCenterId = cfg.getDataCenterId();
        deployMode = cfg.getDeploymentMode();
        discoStartupDelay = cfg.getDiscoveryStartupDelay();
        drRcvHubCfg = cfg.getDrReceiverHubConfiguration() != null ?
            new GridDrReceiverHubConfiguration(cfg.getDrReceiverHubConfiguration()) : null;
        drSndHubCfg = cfg.getDrSenderHubConfiguration() != null ?
            new GridDrSenderHubConfiguration(cfg.getDrSenderHubConfiguration()) : null;
        execSvc = cfg.getExecutorService();
        execSvcShutdown = cfg.getExecutorServiceShutdown();
        ggHome = cfg.getGridGainHome();
        ggWork = cfg.getWorkDirectory();
        gridName = cfg.getGridName();
        ggfsCfg = cfg.getGgfsConfiguration();
        ggfsSvc = cfg.getGgfsExecutorService();
        ggfsSvcShutdown = cfg.getGgfsExecutorServiceShutdown();
        hadoopCfg = cfg.getHadoopConfiguration();
        inclEvtTypes = cfg.getIncludeEventTypes();
        includeProps = cfg.getIncludeProperties();
        jettyPath = cfg.getRestJettyPath();
        licUrl = cfg.getLicenseUrl();
        lifecycleBeans = cfg.getLifecycleBeans();
        lifeCycleEmailNtf = cfg.isLifeCycleEmailNotification();
        locHost = cfg.getLocalHost();
        log = cfg.getGridLogger();
        lsnrs = cfg.getLocalEventListeners();
        marsh = cfg.getMarshaller();
        marshLocJobs = cfg.isMarshalLocalJobs();
        mbeanSrv = cfg.getMBeanServer();
        metricsHistSize = cfg.getMetricsHistorySize();
        metricsExpTime = cfg.getMetricsExpireTime();
        metricsLogFreq = cfg.getMetricsLogFrequency();
        metricsUpdateFreq = cfg.getMetricsUpdateFrequency();
        mgmtSvc = cfg.getManagementExecutorService();
        mgmtSvcShutdown = cfg.getManagementExecutorServiceShutdown();
        netTimeout = cfg.getNetworkTimeout();
        nodeId = cfg.getNodeId();
        p2pEnabled = cfg.isPeerClassLoadingEnabled();
        p2pLocClsPathExcl = cfg.getPeerClassLoadingLocalClassPathExclude();
        p2pMissedCacheSize = cfg.getPeerClassLoadingMissedResourcesCacheSize();
        p2pSvc = cfg.getPeerClassLoadingExecutorService();
        p2pSvcShutdown = cfg.getPeerClassLoadingExecutorServiceShutdown();
        pluginCfgs = cfg.getPluginConfigurations();
        portableCfg = cfg.getPortableConfiguration();
        restAccessibleFolders = cfg.getRestAccessibleFolders();
        restEnabled = cfg.isRestEnabled();
        restIdleTimeout = cfg.getRestIdleTimeout();
        restPortRange = cfg.getRestPortRange();
        restSecretKey = cfg.getRestSecretKey();
        restTcpHost = cfg.getRestTcpHost();
        restTcpNoDelay = cfg.isRestTcpNoDelay();
        restTcpDirectBuf = cfg.isRestTcpDirectBuffer();
        restTcpSndBufSize = cfg.getRestTcpSendBufferSize();
        restTcpRcvBufSize = cfg.getRestTcpReceiveBufferSize();
        restTcpSndQueueLimit = cfg.getRestTcpSendQueueLimit();
        restTcpSelectorCnt = cfg.getRestTcpSelectorCount();
        restTcpPort = cfg.getRestTcpPort();
        restTcpSslCtxFactory = cfg.getRestTcpSslContextFactory();
        restTcpSslEnabled = cfg.isRestTcpSslEnabled();
        restTcpSslClientAuth = cfg.isRestTcpSslClientAuth();
        restExecSvc = cfg.getRestExecutorService();
        restSvcShutdown = cfg.getRestExecutorServiceShutdown();
        securityCred = cfg.getSecurityCredentialsProvider();
        segChkFreq = cfg.getSegmentCheckFrequency();
        segPlc = cfg.getSegmentationPolicy();
        segResolveAttempts = cfg.getSegmentationResolveAttempts();
        segResolvers = cfg.getSegmentationResolvers();
        sndRetryCnt = cfg.getNetworkSendRetryCount();
        sndRetryDelay = cfg.getNetworkSendRetryDelay();
        smtpHost = cfg.getSmtpHost();
        smtpPort = cfg.getSmtpPort();
        smtpUsername = cfg.getSmtpUsername();
        smtpPwd = cfg.getSmtpPassword();
        smtpFromEmail = cfg.getSmtpFromEmail();
        smtpSsl = cfg.isSmtpSsl();
        smtpStartTls = cfg.isSmtpStartTls();
        streamerCfg = cfg.getStreamerConfiguration();
        sysSvc = cfg.getSystemExecutorService();
        sysSvcShutdown = cfg.getSystemExecutorServiceShutdown();
        timeSrvPortBase = cfg.getTimeServerPortBase();
        timeSrvPortRange = cfg.getTimeServerPortRange();
        txCfg = cfg.getTransactionsConfiguration();
        userAttrs = cfg.getUserAttributes();
        waitForSegOnStart = cfg.isWaitForSegmentOnStart();
        warmupClos = cfg.getWarmupClosure();
        dotNetCfg = cfg.getDotNetConfiguration() == null ?
            null : new GridDotNetConfiguration(cfg.getDotNetConfiguration());
    }

    /**
     * Whether or not send email notifications on node start and stop. Note if enabled
     * email notifications will only be sent if SMTP is configured and at least one
     * admin email is provided.
     * <p>
     * By default - email notifications are enabled.
     *
     * @return {@code True} to enable lifecycle email notifications.
     * @see #getSmtpHost()
     * @see #getAdminEmails()
     */
    public boolean isLifeCycleEmailNotification() {
        return lifeCycleEmailNtf;
    }

    /**
     * Gets custom license file URL to be used instead of default license file location.
     *
     * @return Custom license file URL or {@code null} to use the default
     *      {@code $GRIDGAIN_HOME}-related location.
     */
    public String getLicenseUrl() {
        return licUrl;
    }

    /**
     * Whether or not to use SSL fot SMTP. Default is {@link #DFLT_SMTP_SSL}.
     * <p>
     * Note that GridGain uses SMTP to send emails in critical
     * situations such as license expiration or fatal system errors.
     * It is <b>highly</b> recommended to configure SMTP in production
     * environment.
     * <p>
     * Note that {@link #getSmtpHost()} is the only mandatory SMTP
     * configuration property.
     *
     * @return Whether or not to use SSL fot SMTP.
     * @see #DFLT_SMTP_SSL
     * @see GridSystemProperties#GG_SMTP_SSL
     */
    public boolean isSmtpSsl() {
        return smtpSsl;
    }

    /**
     * Whether or not to use STARTTLS fot SMTP. Default is {@link #DFLT_SMTP_STARTTLS}.
     * <p>
     * Note that GridGain uses SMTP to send emails in critical
     * situations such as license expiration or fatal system errors.
     * It is <b>highly</b> recommended to configure SMTP in production
     * environment.
     * <p>
     * Note that {@link #getSmtpHost()} is the only mandatory SMTP
     * configuration property.
     *
     * @return Whether or not to use STARTTLS fot SMTP.
     * @see #DFLT_SMTP_STARTTLS
     * @see GridSystemProperties#GG_SMTP_STARTTLS
     */
    public boolean isSmtpStartTls() {
        return smtpStartTls;
    }

    /**
     * Gets SMTP host name or {@code null} if SMTP is not configured.
     * <p>
     * Note that GridGain uses SMTP to send emails in critical
     * situations such as license expiration or fatal system errors.
     * It is <b>highly</b> recommended to configure SMTP in production
     * environment.
     * <p>
     * Note that {@code getSmtpHost()} is the only mandatory SMTP
     * configuration property.
     *
     * @return SMTP host name or {@code null} if SMTP is not configured.
     * @see GridSystemProperties#GG_SMTP_HOST
     */
    public String getSmtpHost() {
        return smtpHost;
    }

    /**
     * Gets SMTP port. Default value is {@link #DFLT_SMTP_PORT}.
     * <p>
     * Note that GridGain uses SMTP to send emails in critical
     * situations such as license expiration or fatal system errors.
     * It is <b>highly</b> recommended to configure SMTP in production
     * environment.
     * <p>
     * Note that {@link #getSmtpHost()} is the only mandatory SMTP
     * configuration property.
     *
     * @return SMTP port.
     * @see #DFLT_SMTP_PORT
     * @see GridSystemProperties#GG_SMTP_PORT
     */
    public int getSmtpPort() {
        return smtpPort;
    }

    /**
     * Gets SMTP username or {@code null} if not used.
     * <p>
     * Note that GridGain uses SMTP to send emails in critical
     * situations such as license expiration or fatal system errors.
     * It is <b>highly</b> recommended to configure SMTP in production
     * environment.
     * <p>
     * Note that {@link #getSmtpHost()} is the only mandatory SMTP
     * configuration property.
     *
     * @return SMTP username or {@code null}.
     * @see GridSystemProperties#GG_SMTP_USERNAME
     */
    public String getSmtpUsername() {
        return smtpUsername;
    }

    /**
     * SMTP password or {@code null} if not used.
     * <p>
     * Note that GridGain uses SMTP to send emails in critical
     * situations such as license expiration or fatal system errors.
     * It is <b>highly</b> recommended to configure SMTP in production
     * environment.
     * <p>
     * Note that {@link #getSmtpHost()} is the only mandatory SMTP
     * configuration property.
     *
     * @return SMTP password or {@code null}.
     * @see GridSystemProperties#GG_SMTP_PWD
     */
    public String getSmtpPassword() {
        return smtpPwd;
    }

    /**
     * Gets optional set of admin emails where email notifications will be set.
     * <p>
     * Note that GridGain uses SMTP to send emails in critical
     * situations such as license expiration or fatal system errors.
     * It is <b>highly</b> recommended to configure SMTP in production
     * environment.
     *
     * @return Optional set of admin emails where email notifications will be set.
     *      If {@code null} - emails will be sent only to the email in the license
     *      if one provided.
     * @see GridSystemProperties#GG_ADMIN_EMAILS
     */
    public String[] getAdminEmails() {
        return adminEmails;
    }

    /**
     * Gets optional FROM email address for email notifications. By default
     * {@link #DFLT_SMTP_FROM_EMAIL} will be used.
     *
     * @return Optional FROM email address for email notifications. If {@code null}
     *      - {@link #DFLT_SMTP_FROM_EMAIL} will be used by default.
     * @see #DFLT_SMTP_FROM_EMAIL
     * @see GridSystemProperties#GG_SMTP_FROM
     */
    public String getSmtpFromEmail() {
        return smtpFromEmail;
    }

    /**
     * Sets license URL different from the default location of the license file.
     *
     * @param licUrl License URl to set.
     */
    public void setLicenseUrl(String licUrl) {
        this.licUrl = licUrl;
    }

    /**
     * Sets whether or not to enable lifecycle email notifications.
     *
     * @param lifeCycleEmailNtf {@code True} to enable lifecycle email notifications.
     * @see GridSystemProperties#GG_LIFECYCLE_EMAIL_NOTIFY
     */
    public void setLifeCycleEmailNotification(boolean lifeCycleEmailNtf) {
        this.lifeCycleEmailNtf = lifeCycleEmailNtf;
    }

    /**
     * Sets whether or not SMTP uses SSL.
     * <p>
     * Note that GridGain uses SMTP to send emails in critical
     * situations such as license expiration or fatal system errors.
     * It is <b>highly</b> recommended to configure SMTP in production
     * environment.
     * <p>
     * Note that {@link #setSmtpHost(String)} is the only mandatory SMTP
     * configuration property.
     *
     * @param smtpSsl Whether or not SMTP uses SSL.
     * @see GridSystemProperties#GG_SMTP_SSL
     */
    public void setSmtpSsl(boolean smtpSsl) {
        this.smtpSsl = smtpSsl;
    }

    /**
     * Sets whether or not SMTP uses STARTTLS.
     * <p>
     * Note that GridGain uses SMTP to send emails in critical
     * situations such as license expiration or fatal system errors.
     * It is <b>highly</b> recommended to configure SMTP in production
     * environment.
     * <p>
     * Note that {@link #setSmtpHost(String)} is the only mandatory SMTP
     * configuration property.
     *
     * @param smtpStartTls Whether or not SMTP uses STARTTLS.
     * @see GridSystemProperties#GG_SMTP_STARTTLS
     */
    public void setSmtpStartTls(boolean smtpStartTls) {
        this.smtpStartTls = smtpStartTls;
    }

    /**
     * Sets SMTP host.
     * <p>
     * Note that GridGain uses SMTP to send emails in critical
     * situations such as license expiration or fatal system errors.
     * It is <b>highly</b> recommended to configure SMTP in production
     * environment.
     * <p>
     * Note that {@code #setSmtpHost(String)} is the only mandatory SMTP
     * configuration property.
     *
     * @param smtpHost SMTP host to set or {@code null} to disable sending emails.
     * @see GridSystemProperties#GG_SMTP_HOST
     */
    public void setSmtpHost(String smtpHost) {
        this.smtpHost = smtpHost;
    }

    /**
     * Sets SMTP port. Default value is {@link #DFLT_SMTP_PORT}.
     * <p>
     * Note that GridGain uses SMTP to send emails in critical
     * situations such as license expiration or fatal system errors.
     * It is <b>highly</b> recommended to configure SMTP in production
     * environment.
     * <p>
     * Note that {@link #setSmtpHost(String)} is the only mandatory SMTP
     * configuration property.
     *
     * @param smtpPort SMTP port to set.
     * @see #DFLT_SMTP_PORT
     * @see GridSystemProperties#GG_SMTP_PORT
     */
    public void setSmtpPort(int smtpPort) {
        this.smtpPort = smtpPort;
    }

    /**
     * Sets SMTP username or {@code null} if not used.
     * <p>
     * Note that GridGain uses SMTP to send emails in critical
     * situations such as license expiration or fatal system errors.
     * It is <b>highly</b> recommended to configure SMTP in production
     * environment.
     * <p>
     * Note that {@link #setSmtpHost(String)} is the only mandatory SMTP
     * configuration property.
     *
     * @param smtpUsername SMTP username or {@code null}.
     * @see GridSystemProperties#GG_SMTP_USERNAME
     */
    public void setSmtpUsername(String smtpUsername) {
        this.smtpUsername = smtpUsername;
    }

    /**
     * Sets SMTP password or {@code null} if not used.
     * <p>
     * Note that GridGain uses SMTP to send emails in critical
     * situations such as license expiration or fatal system errors.
     * It is <b>highly</b> recommended to configure SMTP in production
     * environment.
     * <p>
     * Note that {@link #setSmtpHost(String)} is the only mandatory SMTP
     * configuration property.
     *
     * @param smtpPwd SMTP password or {@code null}.
     * @see GridSystemProperties#GG_SMTP_PWD
     */
    public void setSmtpPassword(String smtpPwd) {
        this.smtpPwd = smtpPwd;
    }

    /**
     * Sets optional set of admin emails where email notifications will be set.
     * <p>
     * Note that GridGain uses SMTP to send emails in critical
     * situations such as license expiration or fatal system errors.
     * It is <b>highly</b> recommended to configure SMTP in production
     * environment.
     *
     * @param adminEmails Optional set of admin emails where email notifications will be set.
     *      If {@code null} - emails will be sent only to the email in the license
     *      if one provided.
     * @see GridSystemProperties#GG_ADMIN_EMAILS
     */
    public void setAdminEmails(String[] adminEmails) {
        this.adminEmails = adminEmails;
    }

    /**
     * Sets optional FROM email address for email notifications. By default
     * {@link #DFLT_SMTP_FROM_EMAIL} will be used.
     *
     * @param smtpFromEmail Optional FROM email address for email notifications. If {@code null}
     *      - {@link #DFLT_SMTP_FROM_EMAIL} will be used by default.
     * @see #DFLT_SMTP_FROM_EMAIL
     * @see GridSystemProperties#GG_SMTP_FROM
     */
    public void setSmtpFromEmail(String smtpFromEmail) {
        this.smtpFromEmail = smtpFromEmail;
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
     * visible on the main APIs, i.e. they are not part of any projections. The only
     * way to see daemon nodes is to use {@link org.apache.ignite.cluster.ClusterGroup#forDaemons()} method.
     * <p>
     * Daemon nodes are used primarily for management and monitoring functionality that
     * is build on GridGain and needs to participate in the topology but also needs to be
     * excluded from "normal" topology so that it won't participate in task execution
     * or in-memory data grid storage.
     *
     * @return {@code True} if this node should be a daemon node, {@code false} otherwise.
     * @see org.apache.ignite.cluster.ClusterGroup#forDaemons()
     */
    public boolean isDaemon() {
        return daemon;
    }

    /**
     * Sets daemon flag.
     * <p>
     * Daemon nodes are the usual grid nodes that participate in topology but not
     * visible on the main APIs, i.e. they are not part of any projections. The only
     * way to see daemon nodes is to use {@link org.apache.ignite.cluster.ClusterGroup#forDaemons()} method.
     * <p>
     * Daemon nodes are used primarily for management and monitoring functionality that
     * is build on GridGain and needs to participate in the topology but also needs to be
     * excluded from "normal" topology so that it won't participate in task execution
     * or in-memory data grid storage.
     *
     * @param daemon Daemon flag.
     */
    public void setDaemon(boolean daemon) {
        this.daemon = daemon;
    }

    /**
     * Sets grid name. Note that {@code null} is a default grid name.
     *
     * @param gridName Grid name to set. Can be {@code null}, which is default
     *      grid name.
     */
    public void setGridName(String gridName) {
        this.gridName = gridName;
    }

    /**
     * Should return any user-defined attributes to be added to this node. These attributes can
     * then be accessed on nodes by calling {@link org.apache.ignite.cluster.ClusterNode#attribute(String)} or
     * {@link org.apache.ignite.cluster.ClusterNode#attributes()} methods.
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
     * <b>NOTE:</b> attributes names starting with {@code org.gridgain} are reserved
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
     */
    public void setUserAttributes(Map<String, ?> userAttrs) {
        this.userAttrs = userAttrs;
    }

    /**
     * Should return an instance of logger to use in grid. If not provided,
     * {@gglink org.gridgain.grid.logger.log4j.GridLog4jLogger}
     * will be used.
     *
     * @return Logger to use in grid.
     */
    public GridLogger getGridLogger() {
        return log;
    }

    /**
     * Sets logger to use within grid.
     *
     * @param log Logger to use within grid.
     * @see IgniteConfiguration#getGridLogger()
     */
    public void setGridLogger(GridLogger log) {
        this.log = log;
    }

    /**
     * Should return an instance of fully configured thread pool to be used in grid.
     * This executor service will be in charge of processing {@link org.apache.ignite.compute.ComputeJob GridJobs}
     * and user messages sent to node.
     * <p>
     * If not provided, new executor service will be created using the following configuration:
     * <ul>
     *     <li>Core pool size - {@link #DFLT_PUBLIC_CORE_THREAD_CNT}</li>
     *     <li>Max pool size - {@link #DFLT_PUBLIC_MAX_THREAD_CNT}</li>
     *     <li>Queue capacity - {@link #DFLT_PUBLIC_THREADPOOL_QUEUE_CAP}</li>
     * </ul>
     *
     * @return Thread pool implementation to be used in grid to process job execution
     *      requests and user messages sent to the node.
     */
    public ExecutorService getExecutorService() {
        return execSvc;
    }

    /**
     * Executor service that is in charge of processing internal system messages.
     * <p>
     * If not provided, new executor service will be created using the following configuration:
     * <ul>
     *     <li>Core pool size - {@link #DFLT_SYSTEM_CORE_THREAD_CNT}</li>
     *     <li>Max pool size - {@link #DFLT_SYSTEM_MAX_THREAD_CNT}</li>
     *     <li>Queue capacity - {@link #DFLT_SYSTEM_THREADPOOL_QUEUE_CAP}</li>
     * </ul>
     *
     * @return Thread pool implementation to be used in grid for internal system messages.
     */
    public ExecutorService getSystemExecutorService() {
        return sysSvc;
    }

    /**
     * Executor service that is in charge of processing internal and Visor
     * {@link org.apache.ignite.compute.ComputeJob GridJobs}.
     * <p>
     * If not provided, new executor service will be created using the following configuration:
     * <ul>
     *     <li>Core pool size - {@link #DFLT_MGMT_THREAD_CNT}</li>
     *     <li>Max pool size - {@link #DFLT_MGMT_THREAD_CNT}</li>
     *     <li>Queue capacity - unbounded</li>
     * </ul>
     *
     * @return Thread pool implementation to be used in grid for internal and Visor
     *      jobs processing.
     */
    public ExecutorService getManagementExecutorService() {
        return mgmtSvc;
    }

    /**
     * Should return an instance of fully configured executor service which
     * is in charge of peer class loading requests/responses. If you don't use
     * peer class loading and use GAR deployment only we would recommend to decrease
     * the value of total threads to {@code 1}.
     * <p>
     * If not provided, new executor service will be created using the following configuration:
     * <ul>
     *     <li>Core pool size - {@link #DFLT_P2P_THREAD_CNT}</li>
     *     <li>Max pool size - {@link #DFLT_P2P_THREAD_CNT}</li>
     *     <li>Queue capacity - unbounded</li>
     * </ul>
     *
     * @return Thread pool implementation to be used for peer class loading
     *      requests handling.
     */
    public ExecutorService getPeerClassLoadingExecutorService() {
        return p2pSvc;
    }

    /**
     * Executor service that is in charge of processing outgoing GGFS messages. Note that this
     * executor must have limited task queue to avoid OutOfMemory errors when incoming data stream
     * is faster than network bandwidth.
     * <p>
     * If not provided, new executor service will be created using the following configuration:
     * <ul>
     *     <li>Core pool size - number of processors available in system</li>
     *     <li>Max pool size - number of processors available in system</li>
     * </ul>
     *
     * @return Thread pool implementation to be used for GGFS outgoing message sending.
     */
    public ExecutorService getGgfsExecutorService() {
        return ggfsSvc;
    }

    /**
     * Shutdown flag for executor service.
     * <p>
     * If not provided, default value {@code true} will be used which will shutdown
     * executor service when GridGain stops regardless of whether it was started before
     * GridGain or by GridGain.
     *
     * @return Executor service shutdown flag.
     */
    public boolean getExecutorServiceShutdown() {
        return execSvcShutdown;
    }

    /**
     * Shutdown flag for system executor service.
     * <p>
     * If not provided, default value {@code true} will be used which will shutdown
     * executor service when GridGain stops regardless of whether it was started before
     * GridGain or by GridGain.
     *
     * @return System executor service shutdown flag.
     */
    public boolean getSystemExecutorServiceShutdown() {
        return sysSvcShutdown;
    }

    /**
     * Shutdown flag for management executor service.
     * <p>
     * If not provided, default value {@code true} will be used which will shutdown
     * executor service when GridGain stops regardless of whether it was started before
     * GridGain or by GridGain.
     *
     * @return Management executor service shutdown flag.
     */
    public boolean getManagementExecutorServiceShutdown() {
        return mgmtSvcShutdown;
    }

    /**
     * Should return flag of peer class loading executor service shutdown when the grid stops.
     * <p>
     * If not provided, default value {@code true} will be used which means
     * that when grid will be stopped it will shut down peer class loading executor service.
     *
     * @return Peer class loading executor service shutdown flag.
     */
    public boolean getPeerClassLoadingExecutorServiceShutdown() {
        return p2pSvcShutdown;
    }

    /**
     * Shutdown flag for GGFS executor service.
     * <p>
     * If not provided, default value {@code true} will be used which will shutdown
     * executor service when GridGain stops regardless whether it was started before GridGain
     * or by GridGain.
     *
     * @return GGFS executor service shutdown flag.
     */
    public boolean getGgfsExecutorServiceShutdown() {
        return ggfsSvcShutdown;
    }

    /**
     * Sets thread pool to use within grid.
     *
     * @param execSvc Thread pool to use within grid.
     * @see IgniteConfiguration#getExecutorService()
     */
    public void setExecutorService(ExecutorService execSvc) {
        this.execSvc = execSvc;
    }

    /**
     * Sets executor service shutdown flag.
     *
     * @param execSvcShutdown Executor service shutdown flag.
     * @see IgniteConfiguration#getExecutorServiceShutdown()
     */
    public void setExecutorServiceShutdown(boolean execSvcShutdown) {
        this.execSvcShutdown = execSvcShutdown;
    }

    /**
     * Sets system thread pool to use within grid.
     *
     * @param sysSvc Thread pool to use within grid.
     * @see IgniteConfiguration#getSystemExecutorService()
     */
    public void setSystemExecutorService(ExecutorService sysSvc) {
        this.sysSvc = sysSvc;
    }

    /**
     * Sets system executor service shutdown flag.
     *
     * @param sysSvcShutdown System executor service shutdown flag.
     * @see IgniteConfiguration#getSystemExecutorServiceShutdown()
     */
    public void setSystemExecutorServiceShutdown(boolean sysSvcShutdown) {
        this.sysSvcShutdown = sysSvcShutdown;
    }

    /**
     * Sets management thread pool to use within grid.
     *
     * @param mgmtSvc Thread pool to use within grid.
     * @see IgniteConfiguration#getManagementExecutorService()
     */
    public void setManagementExecutorService(ExecutorService mgmtSvc) {
        this.mgmtSvc = mgmtSvc;
    }

    /**
     * Sets management executor service shutdown flag.
     *
     * @param mgmtSvcShutdown Management executor service shutdown flag.
     * @see IgniteConfiguration#getManagementExecutorServiceShutdown()
     */
    public void setManagementExecutorServiceShutdown(boolean mgmtSvcShutdown) {
        this.mgmtSvcShutdown = mgmtSvcShutdown;
    }

    /**
     * Sets thread pool to use for peer class loading.
     *
     * @param p2pSvc Thread pool to use within grid.
     * @see IgniteConfiguration#getPeerClassLoadingExecutorService()
     */
    public void setPeerClassLoadingExecutorService(ExecutorService p2pSvc) {
        this.p2pSvc = p2pSvc;
    }

    /**
     * Sets peer class loading executor service shutdown flag.
     *
     * @param p2pSvcShutdown Peer class loading executor service shutdown flag.
     * @see IgniteConfiguration#getPeerClassLoadingExecutorServiceShutdown()
     */
    public void setPeerClassLoadingExecutorServiceShutdown(boolean p2pSvcShutdown) {
        this.p2pSvcShutdown = p2pSvcShutdown;
    }

    /**
     * Set executor service that will be used to process outgoing GGFS messages.
     *
     * @param ggfsSvc Executor service to use for outgoing GGFS messages.
     * @see IgniteConfiguration#getGgfsExecutorService()
     */
    public void setGgfsExecutorService(ExecutorService ggfsSvc) {
        this.ggfsSvc = ggfsSvc;
    }

    /**
     * Sets GGFS executor service shutdown flag.
     *
     * @param ggfsSvcShutdown GGFS executor service shutdown flag.
     * @see IgniteConfiguration#getGgfsExecutorService()
     */
    public void setGgfsExecutorServiceShutdown(boolean ggfsSvcShutdown) {
        this.ggfsSvcShutdown = ggfsSvcShutdown;
    }

    /**
     * Should return GridGain installation home folder. If not provided, the system will check
     * {@code GRIDGAIN_HOME} system property and environment variable in that order. If
     * {@code GRIDGAIN_HOME} still could not be obtained, then grid will not start and exception
     * will be thrown.
     *
     * @return GridGain installation home or {@code null} to make the system attempt to
     *      infer it automatically.
     * @see GridSystemProperties#GG_HOME
     */
    public String getGridGainHome() {
        return ggHome;
    }

    /**
     * Sets GridGain installation folder.
     *
     * @param ggHome {@code GridGain} installation folder.
     * @see IgniteConfiguration#getGridGainHome()
     * @see GridSystemProperties#GG_HOME
     */
    public void setGridGainHome(String ggHome) {
        this.ggHome = ggHome;
    }

    /**
     * Gets GridGain work folder. If not provided, the method will use work folder under
     * {@code GRIDGAIN_HOME} specified by {@link IgniteConfiguration#setGridGainHome(String)} or
     * {@code GRIDGAIN_HOME} environment variable or system property.
     * <p>
     * If {@code GRIDGAIN_HOME} is not provided, then system temp folder is used.
     *
     * @return GridGain work folder or {@code null} to make the system attempt to infer it automatically.
     * @see IgniteConfiguration#getGridGainHome()
     * @see GridSystemProperties#GG_HOME
     */
    public String getWorkDirectory() {
        return ggWork;
    }

    /**
     * Sets GridGain work folder.
     *
     * @param ggWork {@code GridGain} work folder.
     * @see IgniteConfiguration#getWorkDirectory()
     */
    public void setWorkDirectory(String ggWork) {
        this.ggWork = ggWork;
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
     */
    public void setMBeanServer(MBeanServer mbeanSrv) {
        this.mbeanSrv = mbeanSrv;
    }

    /**
     * Unique identifier for this node within grid.
     *
     * @return Unique identifier for this node within grid.
     */
    public UUID getNodeId() {
        return nodeId;
    }

    /**
     * Sets unique identifier for local node.
     *
     * @param nodeId Unique identifier for local node.
     * @see IgniteConfiguration#getNodeId()
     */
    public void setNodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * Should return an instance of marshaller to use in grid. If not provided,
     * {@link GridOptimizedMarshaller} will be used on Java HotSpot VM, and
     * {@link GridJdkMarshaller} will be used on other VMs.
     *
     * @return Marshaller to use in grid.
     */
    public GridMarshaller getMarshaller() {
        return marsh;
    }

    /**
     * Sets marshaller to use within grid.
     *
     * @param marsh Marshaller to use within grid.
     * @see IgniteConfiguration#getMarshaller()
     */
    public void setMarshaller(GridMarshaller marsh) {
        this.marsh = marsh;
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
     * See {@link org.apache.ignite.compute.ComputeTask} documentation for more information about task deployment.
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
     */
    public void setMarshalLocalJobs(boolean marshLocJobs) {
        this.marshLocJobs = marshLocJobs;
    }

    /**
     * Enables/disables peer class loading.
     *
     * @param p2pEnabled {@code true} if peer class loading is
     *      enabled, {@code false} otherwise.
     */
    public void setPeerClassLoadingEnabled(boolean p2pEnabled) {
        this.p2pEnabled = p2pEnabled;
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
     */
    public void setPeerClassLoadingLocalClassPathExclude(String... p2pLocClsPathExcl) {
        this.p2pLocClsPathExcl = p2pLocClsPathExcl;
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
     */
    public void setMetricsHistorySize(int metricsHistSize) {
        this.metricsHistSize = metricsHistSize;
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
     */
    public void setMetricsUpdateFrequency(long metricsUpdateFreq) {
        this.metricsUpdateFreq = metricsUpdateFreq;
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
     */
    public void setMetricsExpireTime(long metricsExpTime) {
        this.metricsExpTime = metricsExpTime;
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
     */
    public void setNetworkTimeout(long netTimeout) {
        this.netTimeout = netTimeout;
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
     */
    public void setNetworkSendRetryDelay(long sndRetryDelay) {
        this.sndRetryDelay = sndRetryDelay;
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
     */
    public void setNetworkSendRetryCount(int sndRetryCnt) {
        this.sndRetryCnt = sndRetryCnt;
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
     */
    public void setClockSyncSamples(int clockSyncSamples) {
        this.clockSyncSamples = clockSyncSamples;
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
     */
    public void setClockSyncFrequency(long clockSyncFreq) {
        this.clockSyncFreq = clockSyncFreq;
    }

    /**
     * Returns a collection of life-cycle beans. These beans will be automatically
     * notified of grid life-cycle events. Use life-cycle beans whenever you
     * want to perform certain logic before and after grid startup and stopping
     * routines.
     *
     * @return Collection of life-cycle beans.
     * @see org.apache.ignite.lifecycle.LifecycleBean
     * @see org.apache.ignite.lifecycle.LifecycleEventType
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
     * @see org.apache.ignite.lifecycle.LifecycleEventType
     */
    public void setLifecycleBeans(LifecycleBean... lifecycleBeans) {
        this.lifecycleBeans = lifecycleBeans;
    }

    /**
     * Should return fully configured event SPI implementation. If not provided,
     * {@link GridMemoryEventStorageSpi} will be used.
     *
     * @return Grid event SPI implementation or {@code null} to use default implementation.
     */
    public GridEventStorageSpi getEventStorageSpi() {
        return evtSpi;
    }

    /**
     * Sets fully configured instance of {@link GridEventStorageSpi}.
     *
     * @param evtSpi Fully configured instance of {@link GridEventStorageSpi}.
     * @see IgniteConfiguration#getEventStorageSpi()
     */
    public void setEventStorageSpi(GridEventStorageSpi evtSpi) {
        this.evtSpi = evtSpi;
    }

    /**
     * Should return fully configured discovery SPI implementation. If not provided,
     * {@link GridTcpDiscoverySpi} will be used by default.
     *
     * @return Grid discovery SPI implementation or {@code null} to use default implementation.
     */
    public GridDiscoverySpi getDiscoverySpi() {
        return discoSpi;
    }

    /**
     * Sets fully configured instance of {@link GridDiscoverySpi}.
     *
     * @param discoSpi Fully configured instance of {@link GridDiscoverySpi}.
     * @see IgniteConfiguration#getDiscoverySpi()
     */
    public void setDiscoverySpi(GridDiscoverySpi discoSpi) {
        this.discoSpi = discoSpi;
    }

    /**
     * Returns segmentation policy. Default is {@link #DFLT_SEG_PLC}.
     *
     * @return Segmentation policy.
     */
    public GridSegmentationPolicy getSegmentationPolicy() {
        return segPlc;
    }

    /**
     * Sets segmentation policy.
     *
     * @param segPlc Segmentation policy.
     */
    public void setSegmentationPolicy(GridSegmentationPolicy segPlc) {
        this.segPlc = segPlc;
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
     */
    public void setWaitForSegmentOnStart(boolean waitForSegOnStart) {
        this.waitForSegOnStart = waitForSegOnStart;
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
     */
    public void setAllSegmentationResolversPassRequired(boolean allResolversPassReq) {
        this.allResolversPassReq = allResolversPassReq;
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
     */
    public void setSegmentationResolveAttempts(int segResolveAttempts) {
        this.segResolveAttempts = segResolveAttempts;
    }

    /**
     * Returns a collection of segmentation resolvers.
     * <p>
     * If array is {@code null} or empty, periodical and on-start network
     * segment checks do not happen.
     *
     * @return Segmentation resolvers.
     */
    public GridSegmentationResolver[] getSegmentationResolvers() {
        return segResolvers;
    }

    /**
     * Sets segmentation resolvers.
     *
     * @param segResolvers Segmentation resolvers.
     */
    public void setSegmentationResolvers(GridSegmentationResolver... segResolvers) {
        this.segResolvers = segResolvers;
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
     */
    public void setSegmentCheckFrequency(long segChkFreq) {
        this.segChkFreq = segChkFreq;
    }

    /**
     * Should return fully configured SPI communication  implementation. If not provided,
     * {@link GridTcpCommunicationSpi} will be used by default.
     *
     * @return Grid communication SPI implementation or {@code null} to use default implementation.
     */
    public GridCommunicationSpi getCommunicationSpi() {
        return commSpi;
    }

    /**
     * Sets fully configured instance of {@link GridCommunicationSpi}.
     *
     * @param commSpi Fully configured instance of {@link GridCommunicationSpi}.
     * @see IgniteConfiguration#getCommunicationSpi()
     */
    public void setCommunicationSpi(GridCommunicationSpi commSpi) {
        this.commSpi = commSpi;
    }

    /**
     * Should return fully configured collision SPI implementation. If not provided,
     * {@link GridNoopCollisionSpi} is used and jobs get activated immediately
     * on arrive to mapped node. This approach suits well for large amount of small
     * jobs (which is a wide-spread use case). User still can control the number
     * of concurrent jobs by setting maximum thread pool size defined by
     * GridConfiguration.getExecutorService() configuration property.
     *
     * @return Grid collision SPI implementation or {@code null} to use default implementation.
     */
    public GridCollisionSpi getCollisionSpi() {
        return colSpi;
    }

    /**
     * Sets fully configured instance of {@link GridCollisionSpi}.
     *
     * @param colSpi Fully configured instance of {@link GridCollisionSpi} or
     *      {@code null} if no SPI provided.
     * @see IgniteConfiguration#getCollisionSpi()
     */
    public void setCollisionSpi(GridCollisionSpi colSpi) {
        this.colSpi = colSpi;
    }

    /**
     * Should return fully configured authentication SPI implementation. If not provided,
     * {@link GridNoopAuthenticationSpi} will be used.
     *
     * @return Grid authentication SPI implementation or {@code null} to use default implementation.
     */
    public GridAuthenticationSpi getAuthenticationSpi() {
        return authSpi;
    }

    /**
     * Sets fully configured instance of {@link GridAuthenticationSpi}.
     *
     * @param authSpi Fully configured instance of {@link GridAuthenticationSpi} or
     * {@code null} if no SPI provided.
     * @see IgniteConfiguration#getAuthenticationSpi()
     */
    public void setAuthenticationSpi(GridAuthenticationSpi authSpi) {
        this.authSpi = authSpi;
    }

    /**
     * Should return fully configured secure session SPI implementation. If not provided,
     * {@link GridNoopSecureSessionSpi} will be used.
     *
     * @return Grid secure session SPI implementation or {@code null} to use default implementation.
     */
    public GridSecureSessionSpi getSecureSessionSpi() {
        return sesSpi;
    }

    /**
     * Sets fully configured instance of {@link GridSecureSessionSpi}.
     *
     * @param sesSpi Fully configured instance of {@link GridSecureSessionSpi} or
     * {@code null} if no SPI provided.
     * @see IgniteConfiguration#getSecureSessionSpi()
     */
    public void setSecureSessionSpi(GridSecureSessionSpi sesSpi) {
        this.sesSpi = sesSpi;
    }

    /**
     * Should return fully configured deployment SPI implementation. If not provided,
     * {@link GridLocalDeploymentSpi} will be used.
     *
     * @return Grid deployment SPI implementation or {@code null} to use default implementation.
     */
    public GridDeploymentSpi getDeploymentSpi() {
        return deploySpi;
    }

    /**
     * Sets fully configured instance of {@link GridDeploymentSpi}.
     *
     * @param deploySpi Fully configured instance of {@link GridDeploymentSpi}.
     * @see IgniteConfiguration#getDeploymentSpi()
     */
    public void setDeploymentSpi(GridDeploymentSpi deploySpi) {
        this.deploySpi = deploySpi;
    }

    /**
     * Should return fully configured checkpoint SPI implementation. If not provided,
     * {@link GridNoopCheckpointSpi} will be used.
     *
     * @return Grid checkpoint SPI implementation or {@code null} to use default implementation.
     */
    public GridCheckpointSpi[] getCheckpointSpi() {
        return cpSpi;
    }

    /**
     * Sets fully configured instance of {@link GridCheckpointSpi}.
     *
     * @param cpSpi Fully configured instance of {@link GridCheckpointSpi}.
     * @see IgniteConfiguration#getCheckpointSpi()
     */
    public void setCheckpointSpi(GridCheckpointSpi... cpSpi) {
        this.cpSpi = cpSpi;
    }

    /**
     * Should return fully configured failover SPI implementation. If not provided,
     * {@link GridAlwaysFailoverSpi} will be used.
     *
     * @return Grid failover SPI implementation or {@code null} to use default implementation.
     */
    public GridFailoverSpi[] getFailoverSpi() {
        return failSpi;
    }

    /**
     * Sets fully configured instance of {@link GridFailoverSpi}.
     *
     * @param failSpi Fully configured instance of {@link GridFailoverSpi} or
     *      {@code null} if no SPI provided.
     * @see IgniteConfiguration#getFailoverSpi()
     */
    public void setFailoverSpi(GridFailoverSpi... failSpi) {
        this.failSpi = failSpi;
    }

    /**
     * Should return fully configured load balancing SPI implementation. If not provided,
     * {@link GridRoundRobinLoadBalancingSpi} will be used.
     *
     * @return Grid load balancing SPI implementation or {@code null} to use default implementation.
     */
    public GridLoadBalancingSpi[] getLoadBalancingSpi() {
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
     */
    public void setDiscoveryStartupDelay(long discoStartupDelay) {
        this.discoStartupDelay = discoStartupDelay;
    }

    /**
     * Sets fully configured instance of {@link GridLoadBalancingSpi}.
     *
     * @param loadBalancingSpi Fully configured instance of {@link GridLoadBalancingSpi} or
     *      {@code null} if no SPI provided.
     * @see IgniteConfiguration#getLoadBalancingSpi()
     */
    public void setLoadBalancingSpi(GridLoadBalancingSpi... loadBalancingSpi) {
        this.loadBalancingSpi = loadBalancingSpi;
    }

    /**
     * Sets fully configured instances of {@link GridSwapSpaceSpi}.
     *
     * @param swapSpaceSpi Fully configured instances of {@link GridSwapSpaceSpi} or
     *      <tt>null</tt> if no SPI provided.
     * @see IgniteConfiguration#getSwapSpaceSpi()
     */
    public void setSwapSpaceSpi(GridSwapSpaceSpi swapSpaceSpi) {
        this.swapSpaceSpi = swapSpaceSpi;
    }

    /**
     * Should return fully configured swap space SPI implementation. If not provided,
     * {@link GridFileSwapSpaceSpi} will be used.
     * <p>
     * Note that user can provide one or multiple instances of this SPI (and select later which one
     * is used in a particular context).
     *
     * @return Grid swap space SPI implementation or <tt>null</tt> to use default implementation.
     */
    public GridSwapSpaceSpi getSwapSpaceSpi() {
        return swapSpaceSpi;
    }

    /**
     * Sets fully configured instances of {@link GridIndexingSpi}.
     *
     * @param indexingSpi Fully configured instances of {@link GridIndexingSpi}.
     * @see IgniteConfiguration#getIndexingSpi()
     */
    public void setIndexingSpi(GridIndexingSpi... indexingSpi) {
        this.indexingSpi = indexingSpi;
    }

    /**
     * Should return fully configured indexing SPI implementations. If not provided,
     * {@gglink org.gridgain.grid.spi.indexing.h2.GridH2IndexingSpi} will be used.
     * <p>
     * Note that user can provide one or multiple instances of this SPI (and select later which one
     * is used in a particular context).
     *
     * @return Indexing SPI implementation or <tt>null</tt> to use default implementation.
     */
    public GridIndexingSpi[] getIndexingSpi() {
        return indexingSpi;
    }

    /**
     * Gets address resolver for addresses mapping determination.
     *
     * @return Address resolver.
     */
    public GridAddressResolver getAddressResolver() {
        return addrRslvr;
    }

    /*
     * Sets address resolver for addresses mapping determination.
     *
     * @param addrRslvr Address resolver.
     */
    public void setAddressResolver(GridAddressResolver addrRslvr) {
        this.addrRslvr = addrRslvr;
    }

    /**
     * Sets task classes and resources sharing mode.
     *
     * @param deployMode Task classes and resources sharing mode.
     */
    public void setDeploymentMode(GridDeploymentMode deployMode) {
        this.deployMode = deployMode;
    }

    /**
     * Gets deployment mode for deploying tasks and other classes on this node.
     * Refer to {@link GridDeploymentMode} documentation for more information.
     *
     * @return Deployment mode.
     */
    public GridDeploymentMode getDeploymentMode() {
        return deployMode;
    }

    /**
     * Sets size of missed resources cache. Set 0 to avoid
     * missed resources caching.
     *
     * @param p2pMissedCacheSize Size of missed resources cache.
     */
    public void setPeerClassLoadingMissedResourcesCacheSize(int p2pMissedCacheSize) {
        this.p2pMissedCacheSize = p2pMissedCacheSize;
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
    public GridCacheConfiguration[] getCacheConfiguration() {
        return cacheCfg;
    }

    /**
     * Sets cache configurations.
     *
     * @param cacheCfg Cache configurations.
     */
    @SuppressWarnings({"ZeroLengthArrayAllocation"})
    public void setCacheConfiguration(GridCacheConfiguration... cacheCfg) {
        this.cacheCfg = cacheCfg == null ? new GridCacheConfiguration[0] : cacheCfg;
    }

    /**
     * Gets flag indicating whether cache sanity check is enabled. If enabled, then GridGain
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
     */
    public void setCacheSanityCheckEnabled(boolean cacheSanityCheckEnabled) {
        this.cacheSanityCheckEnabled = cacheSanityCheckEnabled;
    }

    /**
     * Gets array of event types, which will be recorded.
     * <p>
     * Note that by default all events in GridGain are disabled. GridGain can and often does generate thousands
     * events per seconds under the load and therefore it creates a significant additional load on the system.
     * If these events are not needed by the application this load is unnecessary and leads to significant
     * performance degradation. So it is <b>highly recommended</b> to enable only those events that your
     * application logic requires. Note that certain events are required for GridGain's internal operations
     * and such events will still be generated but not stored by event storage SPI if they are disabled
     * in GridGain configuration.
     *
     * @return Include event types.
     */
    public int[] getIncludeEventTypes() {
        return inclEvtTypes;
    }

    /**
     * Sets array of event types, which will be recorded by {@link GridEventStorageManager#record(GridEvent)}.
     * Note, that either the include event types or the exclude event types can be established.
     *
     * @param inclEvtTypes Include event types.
     */
    public void setIncludeEventTypes(int... inclEvtTypes) {
        this.inclEvtTypes = inclEvtTypes;
    }

    /**
     * Sets path, either absolute or relative to {@code GRIDGAIN_HOME}, to {@code JETTY}
     * XML configuration file. {@code JETTY} is used to support REST over HTTP protocol for
     * accessing GridGain APIs remotely.
     *
     * @param jettyPath Path to {@code JETTY} XML configuration file.
     * @deprecated Use {@link GridClientConnectionConfiguration#setRestJettyPath(String)}.
     */
    @Deprecated
    public void setRestJettyPath(String jettyPath) {
        this.jettyPath = jettyPath;
    }

    /**
     * Gets path, either absolute or relative to {@code GRIDGAIN_HOME}, to {@code Jetty}
     * XML configuration file. {@code Jetty} is used to support REST over HTTP protocol for
     * accessing GridGain APIs remotely.
     * <p>
     * If not provided, Jetty instance with default configuration will be started picking
     * {@link GridSystemProperties#GG_JETTY_HOST} and {@link GridSystemProperties#GG_JETTY_PORT}
     * as host and port respectively.
     *
     * @return Path to {@code JETTY} XML configuration file.
     * @see GridSystemProperties#GG_JETTY_HOST
     * @see GridSystemProperties#GG_JETTY_PORT
     * @deprecated Use {@link GridClientConnectionConfiguration#getRestJettyPath()}.
     */
    @Deprecated
    public String getRestJettyPath() {
        return jettyPath;
    }

    /**
     * Sets flag indicating whether external {@code REST} access is enabled or not.
     *
     * @param restEnabled Flag indicating whether external {@code REST} access is enabled or not.
     * @deprecated Use {@link GridClientConnectionConfiguration}.
     */
    @Deprecated
    public void setRestEnabled(boolean restEnabled) {
        this.restEnabled = restEnabled;
    }

    /**
     * Gets flag indicating whether external {@code REST} access is enabled or not. By default,
     * external {@code REST} access is turned on.
     *
     * @return Flag indicating whether external {@code REST} access is enabled or not.
     * @see GridSystemProperties#GG_JETTY_HOST
     * @see GridSystemProperties#GG_JETTY_PORT
     * @deprecated Use {@link GridClientConnectionConfiguration}.
     */
    @Deprecated
    public boolean isRestEnabled() {
        return restEnabled;
    }

    /**
     * Gets host for TCP binary protocol server. This can be either an
     * IP address or a domain name.
     * <p>
     * If not defined, system-wide local address will be used
     * (see {@link #getLocalHost()}.
     * <p>
     * You can also use {@code 0.0.0.0} value to bind to all
     * locally-available IP addresses.
     *
     * @return TCP host.
     * @deprecated Use {@link GridClientConnectionConfiguration#getRestTcpHost()}.
     */
    @Deprecated
    public String getRestTcpHost() {
        return restTcpHost;
    }

    /**
     * Sets host for TCP binary protocol server.
     *
     * @param restTcpHost TCP host.
     * @deprecated Use {@link GridClientConnectionConfiguration#setRestTcpHost(String)}.
     */
    @Deprecated
    public void setRestTcpHost(String restTcpHost) {
        this.restTcpHost = restTcpHost;
    }

    /**
     * Gets port for TCP binary protocol server.
     * <p>
     * Default is {@link #DFLT_TCP_PORT}.
     *
     * @return TCP port.
     * @deprecated Use {@link GridClientConnectionConfiguration#getRestTcpPort()}.
     */
    @Deprecated
    public int getRestTcpPort() {
        return restTcpPort;
    }

    /**
     * Sets port for TCP binary protocol server.
     *
     * @param restTcpPort TCP port.
     * @deprecated Use {@link GridClientConnectionConfiguration#setRestTcpPort(int)}.
     */
    @Deprecated
    public void setRestTcpPort(int restTcpPort) {
        this.restTcpPort = restTcpPort;
    }

    /**
     * Gets flag indicating whether {@code TCP_NODELAY} option should be set for accepted client connections.
     * Setting this option reduces network latency and should be set to {@code true} in majority of cases.
     * For more information, see {@link Socket#setTcpNoDelay(boolean)}
     * <p/>
     * If not specified, default value is {@link #DFLT_TCP_NODELAY}.
     *
     * @return Whether {@code TCP_NODELAY} option should be enabled.
     * @deprecated Use {@link GridClientConnectionConfiguration#isRestTcpNoDelay()}.
     */
    @Deprecated
    public boolean isRestTcpNoDelay() {
        return restTcpNoDelay;
    }

    /**
     * Sets whether {@code TCP_NODELAY} option should be set for all accepted client connections.
     *
     * @param restTcpNoDelay {@code True} if option should be enabled.
     * @see #isRestTcpNoDelay()
     * @deprecated Use {@link GridClientConnectionConfiguration#setRestTcpNoDelay(boolean)}.
     */
    @Deprecated
    public void setRestTcpNoDelay(boolean restTcpNoDelay) {
        this.restTcpNoDelay = restTcpNoDelay;
    }

    /**
     * Gets flag indicating whether REST TCP server should use direct buffers. A direct buffer is a buffer
     * that is allocated and accessed using native system calls, without using JVM heap. Enabling direct
     * buffer <em>may</em> improve performance and avoid memory issues (long GC pauses due to huge buffer
     * size).
     *
     * @return Whether direct buffer should be used.
     * @deprecated Use {@link GridClientConnectionConfiguration#isRestTcpDirectBuffer()}.
     */
    @Deprecated
    public boolean isRestTcpDirectBuffer() {
        return restTcpDirectBuf;
    }

    /**
     * Sets whether to use direct buffer for REST TCP server.
     *
     * @param restTcpDirectBuf {@code True} if option should be enabled.
     * @see #isRestTcpDirectBuffer()
     * @deprecated Use {@link GridClientConnectionConfiguration#setRestTcpDirectBuffer(boolean)}.
     */
    @Deprecated
    public void setRestTcpDirectBuffer(boolean restTcpDirectBuf) {
        this.restTcpDirectBuf = restTcpDirectBuf;
    }

    /**
     * Gets REST TCP server send buffer size.
     *
     * @return REST TCP server send buffer size (0 for default).
     * @deprecated Use {@link GridClientConnectionConfiguration#getRestTcpSendBufferSize()}.
     */
    @Deprecated
    public int getRestTcpSendBufferSize() {
        return restTcpSndBufSize;
    }

    /**
     * Sets REST TCP server send buffer size.
     *
     * @param restTcpSndBufSize Send buffer size.
     * @see #getRestTcpSendBufferSize()
     * @deprecated Use {@link GridClientConnectionConfiguration#setRestTcpSendBufferSize(int)}.
     */
    @Deprecated
    public void setRestTcpSendBufferSize(int restTcpSndBufSize) {
        this.restTcpSndBufSize = restTcpSndBufSize;
    }

    /**
     * Gets REST TCP server receive buffer size.
     *
     * @return REST TCP server receive buffer size (0 for default).
     * @deprecated Use {@link GridClientConnectionConfiguration#getRestTcpReceiveBufferSize()}.
     */
    @Deprecated
    public int getRestTcpReceiveBufferSize() {
        return restTcpRcvBufSize;
    }

    /**
     * Sets REST TCP server receive buffer size.
     *
     * @param restTcpRcvBufSize Receive buffer size.
     * @see #getRestTcpReceiveBufferSize()
     * @deprecated Use {@link GridClientConnectionConfiguration#setRestTcpReceiveBufferSize(int)}.
     */
    @Deprecated
    public void setRestTcpReceiveBufferSize(int restTcpRcvBufSize) {
        this.restTcpRcvBufSize = restTcpRcvBufSize;
    }

    /**
     * Gets REST TCP server send queue limit. If the limit exceeds, all successive writes will
     * block until the queue has enough capacity.
     *
     * @return REST TCP server send queue limit (0 for unlimited).
     * @deprecated Use {@link GridClientConnectionConfiguration#getRestTcpSendQueueLimit()}.
     */
    @Deprecated
    public int getRestTcpSendQueueLimit() {
        return restTcpSndQueueLimit;
    }

    /**
     * Sets REST TCP server send queue limit.
     *
     * @param restTcpSndQueueLimit REST TCP server send queue limit (0 for unlimited).
     * @see #getRestTcpSendQueueLimit()
     * @deprecated Use {@link GridClientConnectionConfiguration#setRestTcpSendQueueLimit(int)}.
     */
    @Deprecated
    public void setRestTcpSendQueueLimit(int restTcpSndQueueLimit) {
        this.restTcpSndQueueLimit = restTcpSndQueueLimit;
    }

    /**
     * Gets number of selector threads in REST TCP server. Higher value for this parameter
     * may increase throughput, but also increases context switching.
     *
     * @return Number of selector threads for REST TCP server.
     * @deprecated Use {@link GridClientConnectionConfiguration#getRestTcpSelectorCount()}.
     */
    @Deprecated
    public int getRestTcpSelectorCount() {
        return restTcpSelectorCnt;
    }

    /**
     * Sets number of selector threads for REST TCP server.
     *
     * @param restTcpSelectorCnt Number of selector threads for REST TCP server.
     * @see #getRestTcpSelectorCount()
     * @deprecated Use {@link GridClientConnectionConfiguration#setRestTcpSelectorCount(int)}.
     */
    @Deprecated
    public void setRestTcpSelectorCount(int restTcpSelectorCnt) {
        this.restTcpSelectorCnt = restTcpSelectorCnt;
    }

    /**
     * Gets idle timeout for REST server.
     * <p>
     * This setting is used to reject half-opened sockets. If no packets
     * come within idle timeout, the connection is closed.
     *
     * @return Idle timeout in milliseconds.
     * @deprecated Use {@link GridClientConnectionConfiguration#getRestIdleTimeout()}.
     */
    @Deprecated
    public long getRestIdleTimeout() {
        return restIdleTimeout;
    }

    /**
     * Sets idle timeout for REST server.
     *
     * @param restIdleTimeout Idle timeout in milliseconds.
     * @see #getRestIdleTimeout()
     * @deprecated Use {@link GridClientConnectionConfiguration#setRestIdleTimeout(long)}.
     */
    @Deprecated
    public void setRestIdleTimeout(long restIdleTimeout) {
        this.restIdleTimeout = restIdleTimeout;
    }

    /**
     * Whether secure socket layer should be enabled on binary rest server.
     * <p>
     * Note that if this flag is set to {@code true}, an instance of {@link GridSslContextFactory}
     * should be provided, otherwise binary rest protocol will fail to start.
     *
     * @return {@code True} if SSL should be enabled.
     * @deprecated Use {@link GridClientConnectionConfiguration#isRestTcpSslEnabled()}.
     */
    @Deprecated
    public boolean isRestTcpSslEnabled() {
        return restTcpSslEnabled;
    }

    /**
     * Sets whether Secure Socket Layer should be enabled for REST TCP binary protocol.
     * <p/>
     * Note that if this flag is set to {@code true}, then a valid instance of {@link GridSslContextFactory}
     * should be provided in {@code GridConfiguration}. Otherwise, TCP binary protocol will fail to start.
     *
     * @param restTcpSslEnabled {@code True} if SSL should be enabled.
     * @deprecated Use {@link GridClientConnectionConfiguration#setRestTcpSslEnabled(boolean)}.
     */
    @Deprecated
    public void setRestTcpSslEnabled(boolean restTcpSslEnabled) {
        this.restTcpSslEnabled = restTcpSslEnabled;
    }

    /**
     * Gets a flag indicating whether or not remote clients will be required to have a valid SSL certificate which
     * validity will be verified with trust manager.
     *
     * @return Whether or not client authentication is required.
     * @deprecated Use {@link GridClientConnectionConfiguration#isRestTcpSslClientAuth()}.
     */
    @Deprecated
    public boolean isRestTcpSslClientAuth() {
        return restTcpSslClientAuth;
    }

    /**
     * Sets flag indicating whether or not SSL client authentication is required.
     *
     * @param needClientAuth Whether or not client authentication is required.
     * @deprecated Use {@link GridClientConnectionConfiguration#setRestTcpSslClientAuth(boolean)}.
     */
    @Deprecated
    public void setRestTcpSslClientAuth(boolean needClientAuth) {
        restTcpSslClientAuth = needClientAuth;
    }

    /**
     * Gets context factory that will be used for creating a secure socket layer of rest binary server.
     *
     * @return SslContextFactory instance.
     * @see GridSslContextFactory
     * @deprecated Use {@link GridClientConnectionConfiguration#getRestTcpSslContextFactory()}.
     */
    @Deprecated
    public GridSslContextFactory getRestTcpSslContextFactory() {
        return restTcpSslCtxFactory;
    }

    /**
     * Sets instance of {@link GridSslContextFactory} that will be used to create an instance of {@code SSLContext}
     * for Secure Socket Layer on TCP binary protocol. This factory will only be used if
     * {@link #setRestTcpSslEnabled(boolean)} is set to {@code true}.
     *
     * @param restTcpSslCtxFactory Instance of {@link GridSslContextFactory}
     * @deprecated Use {@link GridClientConnectionConfiguration#setRestTcpSslContextFactory(GridSslContextFactory)}.
     */
    @Deprecated
    public void setRestTcpSslContextFactory(GridSslContextFactory restTcpSslCtxFactory) {
        this.restTcpSslCtxFactory = restTcpSslCtxFactory;
    }

    /**
     * Gets number of ports to try if configured port is already in use.
     *
     * @return Number of ports to try.
     * @deprecated Use {@link GridClientConnectionConfiguration#getRestPortRange()}.
     */
    @Deprecated
    public int getRestPortRange() {
        return restPortRange;
    }

    /**
     * Sets number of ports to try if configured one is in use.
     *
     * @param restPortRange Port range.
     * @deprecated Use {@link GridClientConnectionConfiguration#setRestPortRange(int)}.
     */
    @Deprecated
    public void setRestPortRange(int restPortRange) {
        this.restPortRange = restPortRange;
    }

    /**
     * Gets list of folders that are accessible for log reading command. When remote client requests
     * a log file, file path is checked against this list. If requested file is not located in any
     * sub-folder of these folders, request is not processed.
     * <p>
     * By default, list consists of a single {@code GRIDGAIN_HOME} folder. If {@code GRIDGAIN_HOME}
     * could not be detected and property is not specified, no restrictions applied.
     *
     * @return Array of folders that are allowed be read by remote clients.
     * @deprecated Use {@link GridClientConnectionConfiguration#getRestAccessibleFolders()}.
     */
    @Deprecated
    public String[] getRestAccessibleFolders() {
        return restAccessibleFolders;
    }

    /**
     * Sets array of folders accessible by REST processor for log reading command.
     *
     * @param restAccessibleFolders Array of folder paths.
     * @deprecated Use {@link GridClientConnectionConfiguration#setRestAccessibleFolders(String...)}.
     */
    @Deprecated
    public void setRestAccessibleFolders(String... restAccessibleFolders) {
        this.restAccessibleFolders = restAccessibleFolders;
    }

    /**
     * Should return an instance of fully configured thread pool to be used for
     * processing of client messages (REST requests).
     * <p>
     * If not provided, new executor service will be created using the following
     * configuration:
     * <ul>
     *     <li>Core pool size - {@link #DFLT_REST_CORE_THREAD_CNT}</li>
     *     <li>Max pool size - {@link #DFLT_REST_MAX_THREAD_CNT}</li>
     *     <li>Queue capacity - {@link #DFLT_REST_THREADPOOL_QUEUE_CAP}</li>
     * </ul>
     *
     * @return Thread pool implementation to be used for processing of client
     *      messages.
     * @deprecated Use {@link GridClientConnectionConfiguration#getRestExecutorService()}.
     */
    @Deprecated
    public ExecutorService getRestExecutorService() {
        return restExecSvc;
    }

    /**
     * Sets thread pool to use for processing of client messages (REST requests).
     *
     * @param restExecSvc Thread pool to use for processing of client messages.
     * @see IgniteConfiguration#getRestExecutorService()
     * @deprecated Use {@link GridClientConnectionConfiguration#setRestExecutorService(ExecutorService)}.
     */
    @Deprecated
    public void setRestExecutorService(ExecutorService restExecSvc) {
        this.restExecSvc = restExecSvc;
    }

    /**
     * Sets REST executor service shutdown flag.
     *
     * @param restSvcShutdown REST executor service shutdown flag.
     * @see IgniteConfiguration#getRestExecutorService()
     * @deprecated Use {@link GridClientConnectionConfiguration#setRestExecutorServiceShutdown(boolean)}.
     */
    @Deprecated
    public void setRestExecutorServiceShutdown(boolean restSvcShutdown) {
        this.restSvcShutdown = restSvcShutdown;
    }

    /**
     * Shutdown flag for REST executor service.
     * <p>
     * If not provided, default value {@code true} will be used which will shutdown
     * executor service when GridGain stops regardless whether it was started before GridGain
     * or by GridGain.
     *
     * @return REST executor service shutdown flag.
     * @deprecated Use {@link GridClientConnectionConfiguration#isRestExecutorServiceShutdown()}.
     */
    @Deprecated
    public boolean getRestExecutorServiceShutdown() {
        return restSvcShutdown;
    }

    /**
     * Sets system-wide local address or host for all GridGain components to bind to. If provided it will
     * override all default local bind settings within GridGain or any of its SPIs.
     *
     * @param locHost Local IP address or host to bind to.
     */
    public void setLocalHost(String locHost) {
        this.locHost = locHost;
    }

    /**
     * Gets system-wide local address or host for all GridGain components to bind to. If provided it will
     * override all default local bind settings within GridGain or any of its SPIs.
     * <p>
     * If {@code null} then GridGain tries to use local wildcard address. That means that
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
     */
    public void setTimeServerPortBase(int timeSrvPortBase) {
        this.timeSrvPortBase = timeSrvPortBase;
    }

    /**
     * Defines port range to try for time server start.
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
     */
    public void setTimeServerPortRange(int timeSrvPortRange) {
        this.timeSrvPortRange = timeSrvPortRange;
    }

    /**
     * Sets secret key to authenticate REST requests. If key is {@code null} or empty authentication is disabled.
     *
     * @param restSecretKey REST secret key.
     * @deprecated Use {@link GridClientConnectionConfiguration#setRestSecretKey(String)}.
     */
    @Deprecated
    public void setRestSecretKey(String restSecretKey) {
        this.restSecretKey = restSecretKey;
    }

    /**
     * Gets secret key to authenticate REST requests. If key is {@code null} or empty authentication is disabled.
     *
     * @return Secret key.
     * @see GridSystemProperties#GG_JETTY_HOST
     * @see GridSystemProperties#GG_JETTY_PORT
     * @deprecated Use {@link GridClientConnectionConfiguration#getRestSecretKey()}.
     */
    @Deprecated
    public String getRestSecretKey() {
        return restSecretKey;
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
     */
    public void setIncludeProperties(String... includeProps) {
        this.includeProps = includeProps;
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
     */
    public void setMetricsLogFrequency(long metricsLogFreq) {
        this.metricsLogFreq = metricsLogFreq;
    }

    /**
     * Gets interceptor for objects, moving to and from remote clients.
     * If this method returns {@code null} then no interception will be applied.
     * <p>
     * Setting interceptor allows to transform all objects exchanged via REST protocol.
     * For example if you use custom serialisation on client you can write interceptor
     * to transform binary representations received from client to Java objects and later
     * access them from java code directly.
     * <p>
     * Default value is {@code null}.
     *
     * @see GridClientMessageInterceptor
     * @return Interceptor.
     * @deprecated Use {@link GridClientConnectionConfiguration#getClientMessageInterceptor()}.
     */
    @Deprecated
    public GridClientMessageInterceptor getClientMessageInterceptor() {
        return clientMsgInterceptor;
    }

    /**
     * Sets client message interceptor.
     * <p>
     * Setting interceptor allows to transform all objects exchanged via REST protocol.
     * For example if you use custom serialisation on client you can write interceptor
     * to transform binary representations received from client to Java objects and later
     * access them from java code directly.
     *
     * @param interceptor Interceptor.
     * @deprecated Use {@link GridClientConnectionConfiguration#setClientMessageInterceptor(GridClientMessageInterceptor)}.
     */
    @Deprecated
    public void setClientMessageInterceptor(GridClientMessageInterceptor interceptor) {
        clientMsgInterceptor = interceptor;
    }

    /**
     * Gets GGFS configurations.
     *
     * @return GGFS configurations.
     */
    public GridGgfsConfiguration[] getGgfsConfiguration() {
        return ggfsCfg;
    }

    /**
     * Sets GGFS configurations.
     *
     * @param ggfsCfg GGFS configurations.
     */
    public void setGgfsConfiguration(GridGgfsConfiguration... ggfsCfg) {
        this.ggfsCfg = ggfsCfg;
    }

    /**
     * Gets streamers configurations.
     *
     * @return Streamers configurations.
     */
    public GridStreamerConfiguration[] getStreamerConfiguration() {
        return streamerCfg;
    }

    /**
     * Sets streamer configuration.
     *
     * @param streamerCfg Streamer configuration.
     */
    public void setStreamerConfiguration(GridStreamerConfiguration... streamerCfg) {
        this.streamerCfg = streamerCfg;
    }

    /**
     * Set data center receiver hub configuration.
     *
     * @return Data center receiver hub configuration.
     */
    public GridDrReceiverHubConfiguration getDrReceiverHubConfiguration() {
        return drRcvHubCfg;
    }

    /**
     * Set data center sender hub configuration.
     *
     * @param drRcvHubCfg Data center sender hub configuration.
     */
    public void setDrReceiverHubConfiguration(GridDrReceiverHubConfiguration drRcvHubCfg) {
        this.drRcvHubCfg = drRcvHubCfg;
    }

    /**
     * Get data center sender hub configuration.
     *
     * @return Data center sender hub configuration.
     */
    public GridDrSenderHubConfiguration getDrSenderHubConfiguration() {
        return drSndHubCfg;
    }

    /**
     * Set data center receiver hub configuration.
     *
     * @param drSndHubCfg Data center receiver hub configuration.
     */
    public void setDrSenderHubConfiguration(GridDrSenderHubConfiguration drSndHubCfg) {
        this.drSndHubCfg = drSndHubCfg;
    }

    /**
     * Gets data center ID of the grid.
     * <p>
     * It is expected that data center ID will be unique among all the topologies participating in data center
     * replication and the same for all the nodes that belong the given topology.
     *
     * @return Data center ID or {@code 0} if it is not set.
     */
    public byte getDataCenterId() {
        return dataCenterId;
    }

    /**
     * Sets data center ID of the grid.
     * <p>
     * It is expected that data center ID will be unique among all the topologies participating in data center
     * replication and the same for all the nodes that belong the given topology.
     *
     * @param dataCenterId Data center ID.
     */
    public void setDataCenterId(byte dataCenterId) {
        this.dataCenterId = dataCenterId;
    }

    /**
     * Gets hadoop configuration.
     *
     * @return Hadoop configuration.
     */
    public GridHadoopConfiguration getHadoopConfiguration() {
        return hadoopCfg;
    }

    /**
     * Sets hadoop configuration.
     *
     * @param hadoopCfg Hadoop configuration.
     */
    public void setHadoopConfiguration(GridHadoopConfiguration hadoopCfg) {
        this.hadoopCfg = hadoopCfg;
    }

    /**
     * Gets security credentials.
     *
     * @return Security credentials.
     */
    public GridSecurityCredentialsProvider getSecurityCredentialsProvider() {
        return securityCred;
    }

    /**
     * Sets security credentials.
     *
     * @param securityCred Security credentials.
     */
    public void setSecurityCredentialsProvider(GridSecurityCredentialsProvider securityCred) {
        this.securityCred = securityCred;
    }

    /**
     * @return Client connection configuration.
     */
    public GridClientConnectionConfiguration getClientConnectionConfiguration() {
        return clientCfg;
    }

    /**
     * @param clientCfg Client connection configuration.
     */
    public void setClientConnectionConfiguration(GridClientConnectionConfiguration clientCfg) {
        this.clientCfg = clientCfg;
    }

    /**
     * @return Portable configuration.
     */
    public GridPortableConfiguration getPortableConfiguration() {
        return portableCfg;
    }

    /**
     * @param portableCfg Portable configuration.
     */
    public void setPortableConfiguration(GridPortableConfiguration portableCfg) {
        this.portableCfg = portableCfg;
    }

    /**
     * Gets configurations for services to be deployed on the grid.
     *
     * @return Configurations for services to be deployed on the grid.
     */
    public GridServiceConfiguration[] getServiceConfiguration() {
        return svcCfgs;
    }

    /**
     * Sets configurations for services to be deployed on the grid.
     *
     * @param svcCfgs Configurations for services to be deployed on the grid.
     */
    public void setServiceConfiguration(GridServiceConfiguration... svcCfgs) {
        this.svcCfgs = svcCfgs;
    }

    /**
     * Gets map of pre-configured local event listeners.
     * Each listener is mapped to array of event types.
     *
     * @return Pre-configured event listeners map.
     * @see GridEventType
     */
    public Map<IgnitePredicate<? extends GridEvent>, int[]> getLocalEventListeners() {
        return lsnrs;
    }

    /**
     * Sets map of pre-configured local event listeners.
     * Each listener is mapped to array of event types.
     *
     * @param lsnrs Pre-configured event listeners map.
     */
    public void setLocalEventListeners(Map<IgnitePredicate<? extends GridEvent>, int[]> lsnrs) {
        this.lsnrs = lsnrs;
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
     */
    public void setWarmupClosure(IgniteInClosure<IgniteConfiguration> warmupClos) {
        this.warmupClos = warmupClos;
    }

    /**
     * Returns configuration for .Net nodes.
     * @return Configuration for .Net nodes.
     */
    @Nullable public GridDotNetConfiguration getDotNetConfiguration() {
        return dotNetCfg;
    }

    /**
     * Sets configuration for .Net nodes.
     * @param dotNetCfg Configuration for .Net nodes
     */
    public void setDotNetConfiguration(@Nullable GridDotNetConfiguration dotNetCfg) {
        this.dotNetCfg = dotNetCfg;
    }

    /**
     * Gets transactions configuration.
     *
     * @return Transactions configuration.
     */
    public GridTransactionsConfiguration getTransactionsConfiguration() {
        return txCfg;
    }

    /**
     * Sets transactions configuration.
     *
     * @param txCfg Transactions configuration.
     */
    public void setTransactionsConfiguration(GridTransactionsConfiguration txCfg) {
        this.txCfg = txCfg;
    }

    /**
     * @return Plugin configurations.
     */
    public Collection<? extends PluginConfiguration> getPluginConfigurations() {
        return pluginCfgs;
    }

    /**
     * @param pluginCfgs Plugin configurations.
     */
    public void setPluginConfigurations(Collection<? extends PluginConfiguration> pluginCfgs) {
        this.pluginCfgs = pluginCfgs;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteConfiguration.class, this);
    }
}
